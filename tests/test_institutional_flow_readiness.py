"""Regression checks for missing-data, PIT, session and voting semantics."""

import datetime as dt
import json
import sqlite3

from source_extension.institutional_flow import (
    FLOW_ASOF_SQL, TPEX_FLOW_SOURCE, assess_flow_readiness, format_flow_readiness,
)


AS_OF = dt.date(2026, 10, 7)
CUTOFF = dt.datetime(2026, 10, 7, 12, 2)
SESSIONS = [dt.date(2026, 10, day) for day in (1, 2, 5, 6, 7)]
MASTER = {"2330": {"market": "上市"}, "3105": {"market": "上櫃"},
          "9999": {"market": "上櫃"}}


def observation(day, code="3105", **overrides):
    row = {
        "entity_id": code, "effective_date": day, "quality_status": "ok",
        "unit": "shares", "captured_at": CUTOFF - dt.timedelta(seconds=1),
        "capture_basis": "source_feed", "dag_id": "source_extension_collect_after_close",
        "dag_run_id": "scheduled__test", "successful_source_run": 1,
        "value_text": json.dumps({
            "market": "上櫃", "three_institution_total_net_shares": 300000,
            "foreign_net_shares_excluding_foreign_dealers": 100000,
            "investment_trust_net_shares": 200000, "dealers_net_shares": 0,
            "foreign_dealers_net_shares": 90000,
        }),
    }
    return {**row, **overrides}


def assess(rows, **overrides):
    params = {
        "master": MASTER, "as_of_date": AS_OF, "cutoff_utc": CUTOFF,
        "expected_session_dates": SESSIONS, "calendar_verified": True,
        "price_rows_by_code": {
            code: [(day, 50.0, 1000000.0, False) for day in SESSIONS]
            for code in MASTER
        },
    }
    return assess_flow_readiness(rows, **{**params, **overrides})


def test_available_three_days_are_distinct_from_vote_and_missing_source():
    result = assess([observation(day) for day in SESSIONS[-3:]])
    otc = result["上櫃"]
    assert otc["same_day_count"] == otc["natural_pit_same_day_count"] == 1
    assert otc["five_session_ready_count"] == 0
    assert len(otc["observed_effective_dates"]) == 3
    assert otc["traded_without_source_count"] == 1
    assert not otc["vote_enabled"]
    assert result["上市"]["same_day_count"] is None
    assert result["上市"]["traded_without_source_count"] is None
    assert "已收 1/2" in format_flow_readiness("上櫃", otc, 2)
    assert "稽核未讀取既有股票表" in format_flow_readiness("上市", result["上市"], 1)


def test_complete_natural_panel_saves_shadow_facts_without_enabling_votes():
    otc = assess([observation(day) for day in SESSIONS])["上櫃"]
    assert otc["five_session_ready_count"] == 1
    shadow = otc["shadow_observations_5d"]["3105"]
    assert shadow["total_net_shares_5d"] == 1500000
    assert shadow["foreign_net_shares_5d"] == 500000
    assert shadow["dealer_net_shares_5d"] == 0  # Foreign dealers not added twice.
    assert not otc["vote_enabled"]
    assert otc["status"] == "official_feed_observed"


def test_missing_session_or_duplicate_day_cannot_be_replaced_by_row_offset():
    missing = [observation(day) for day in SESSIONS if day != SESSIONS[2]]
    missing.append(observation(SESSIONS[0] - dt.timedelta(days=1)))
    assert assess(missing)["上櫃"]["five_session_ready_count"] == 0
    duplicated = [observation(day) for day in SESSIONS] + [observation(AS_OF)]
    otc = assess(duplicated)["上櫃"]
    assert otc["same_day_count"] == otc["five_session_ready_count"] == 0
    assert otc["rejected_key_counts"]["duplicate_security_date"] == 1


def test_future_manual_current_only_and_failed_run_do_not_qualify_for_pit():
    for override in (
        {"captured_at": CUTOFF + dt.timedelta(seconds=1)},
        {"dag_run_id": "manual__test"}, {"capture_basis": "current_row_only"},
        {"successful_source_run": 0},
    ):
        rows = [observation(day) for day in SESSIONS[:-1]]
        rows.append(observation(AS_OF, **override))
        assert assess(rows)["上櫃"]["five_session_ready_count"] == 0


def test_signed_zero_facts_valid_but_bad_units_and_reconciliation_rejected():
    zero = observation(AS_OF)
    detail = json.loads(zero["value_text"])
    detail.update(three_institution_total_net_shares=0,
                  foreign_net_shares_excluding_foreign_dealers=-200000)
    zero["value_text"] = json.dumps(detail)
    assert assess([zero])["上櫃"]["same_day_count"] == 1
    assert assess([{**zero, "unit": "lots"}])["上櫃"]["same_day_count"] == 0
    detail["three_institution_total_net_shares"] = 1
    assert assess([{**zero, "value_text": json.dumps(detail)}])["上櫃"]["same_day_count"] == 0
    detail["three_institution_total_net_shares"] = None
    assert assess([{**zero, "value_text": json.dumps(detail)}])["上櫃"]["same_day_count"] == 0


def test_unmatched_codes_and_wrong_market_never_expand_stock_universe():
    result = assess([observation(AS_OF, code="00411A"), observation(AS_OF, code="2330")])
    assert result["上櫃"]["same_day_count"] == 0
    assert result["上櫃"]["source_unmatched_count"] == 2
    assert result["上市"]["status"] == "stored_source_not_assessed"


def test_price_gaps_no_trade_and_calendar_failure_prevent_complete_panel():
    rows = [observation(day) for day in SESSIONS]
    for price_rows in (
        [(day, 50.0, 1000.0, False) for day in SESSIONS[:-1]],
        [(day, 50.0, 0.0, True) for day in SESSIONS],
        [(day, None, 1000.0, False) for day in SESSIONS],
    ):
        assert assess(rows, price_rows_by_code={"3105": price_rows})["上櫃"]["five_session_ready_count"] == 0
    assert assess(rows, calendar_verified=False)["上櫃"]["five_session_ready_count"] == 0


def test_asof_sql_selects_pre_cutoff_revision_and_never_resurrects_rejected_value():
    db = sqlite3.connect(":memory:")
    fields = """record_hash TEXT,entity_id TEXT,effective_date TEXT,
        quality_status TEXT,unit TEXT,value_text TEXT"""
    db.execute(f"CREATE TABLE source_record_versions(id INTEGER,source_key TEXT,{fields},"
               "captured_at TEXT,capture_basis TEXT,dag_id TEXT,dag_run_id TEXT)")
    db.execute(f"CREATE TABLE source_records(source_key TEXT,{fields},updated_at TEXT)")
    db.execute("CREATE TABLE source_runs(source_key TEXT,dag_id TEXT,dag_run_id TEXT,"
               "status TEXT,finished_at TEXT)")
    db.execute("INSERT INTO source_runs VALUES(?,?,?,?,?)",
               (TPEX_FLOW_SOURCE, "source_extension_collect_after_close", "scheduled__test",
                "success", "2026-10-07 12:01:00"))
    for record_hash, rev, captured, quality in (
        ("a", 1, "2026-10-07 12:01:00", "ok"),
        ("a", 2, "2026-10-07 12:03:00", "ok"),
        ("b", 3, "2026-10-07 12:00:00", "ok"),
        ("b", 4, "2026-10-07 12:01:00", "quarantine"),
    ):
        db.execute("INSERT INTO source_record_versions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (
            rev, TPEX_FLOW_SOURCE, record_hash, "3105", "2026-10-07", quality,
            "shares", str(rev), captured, "source_feed",
            "source_extension_collect_after_close", "scheduled__test",
        ))
    # A current row written after the cutoff has no eligible fallback.
    db.execute("INSERT INTO source_records VALUES(?,?,?,?,?,?,?,?)", (
        TPEX_FLOW_SOURCE, "late", "9999", "2026-10-07", "ok", "shares", "99",
        "2026-10-07 12:03:00",
    ))
    cutoff = "2026-10-07 12:02:00"
    rows = db.execute(FLOW_ASOF_SQL.replace("%s", "?"), (
        cutoff, TPEX_FLOW_SOURCE, cutoff, cutoff, TPEX_FLOW_SOURCE, cutoff, cutoff,
        "2026-09-01", "2026-10-07",
    )).fetchall()
    assert {row[0]: (row[3], row[5]) for row in rows} == {
        "a": ("ok", "1"), "b": ("quarantine", "4"),
    }
    assert all(row[-1] == 1 for row in rows)
    db.close()
