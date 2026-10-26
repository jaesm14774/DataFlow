"""Trading-factor regressions: information cutoff, units, identity and abstention."""
import datetime as dt
import json

from source_extension.equity_order_flow import (
    select_institutional_observations, institutional_signal,
    select_broker_panels, broker_signal,
    should_wait_for_collection, COLLECTION_TASKS,
    load_equity_order_flow,
)
from source_extension.institutional_flow import format_flow_readiness

AS_OF = dt.date(2026, 10, 7)
CUTOFF = dt.datetime(2026, 10, 7, 13)
SESSIONS = [dt.date(2026, 10, d) for d in (1, 2, 5, 6, 7)]
MASTER = {"2330": {"market": "上市"}, "3105": {"market": "上櫃"}}
PRICES = [(d, 50, 1_000_000, False) for d in SESSIONS]


def legacy(day=AS_OF, code="2330", **updates):
    return dict(code=code, date=day, total=300000, foreign=100000, trust=200000,
                dealer=0, captured_at=CUTOFF - dt.timedelta(minutes=1), **updates)


def official(day=AS_OF, code="3105", *, total=300000, foreign=100000,
             trust=200000, dealer=0, **updates):
    row = dict(
        entity_id=code, effective_date=day, quality_status="ok", unit="shares",
        captured_at=CUTOFF - dt.timedelta(minutes=1), capture_basis="source_feed",
        successful_source_run=True, dag_id="source_extension_collect_after_close",
        dag_run_id="scheduled__test",
        value_text=json.dumps(dict(
            market="上櫃", three_institution_total_net_shares=total,
            foreign_net_shares_excluding_foreign_dealers=foreign,
            investment_trust_net_shares=trust, dealers_net_shares=dealer,
        )),
    )
    row.update(updates)
    return row


def select(official=(), rows=()):
    return select_institutional_observations(official, rows, master=MASTER,
                                            as_of_date=AS_OF, cutoff_utc=CUTOFF)


def inst_panel(days=SESSIONS, **updates):
    return {
        day: {
            "total": updates.get("total", 300000),
            "foreign": updates.get("foreign", 100000),
            "trust": updates.get("trust", 200000),
            "dealer": updates.get("dealer", 0),
            "source_key": "tpex_institutional_flow_daily",
            "capture_basis": "source_feed", "natural_pit": True,
            "captured_at": CUTOFF.isoformat(),
        }
        for day in days
    }


def test_institutional_direction_uses_available_data_with_existing_rules():
    result = institutional_signal(inst_panel(SESSIONS[-3:]), PRICES, SESSIONS, AS_OF)
    assert result["direction"] == 1
    assert result["vote_enabled"] is True
    assert result["status"] == "qualified"
    assert result["selected_window"]["days"] == 3
    assert result["selected_window"]["net_intensity_pct"] == 30
    assert "5" not in result["windows"]


def test_missing_session_never_becomes_three_or_five_day_row_offset():
    panel = inst_panel()
    del panel[SESSIONS[-2]]
    panel[SESSIONS[0] - dt.timedelta(days=1)] = inst_panel(
        [SESSIONS[0] - dt.timedelta(days=1)]
    )[SESSIONS[0] - dt.timedelta(days=1)]
    result = institutional_signal(panel, PRICES, SESSIONS, AS_OF)
    assert set(result["windows"]) == {"1"}
    assert result["selected_window"]["days"] == 1
    assert result["direction"] == 1


def test_opposite_qualified_windows_abstain_instead_of_cherry_picking():
    panel = inst_panel(total=500000, foreign=300000, trust=200000)
    panel[AS_OF].update(total=-500000, foreign=-300000, trust=-200000)
    result = institutional_signal(panel, PRICES, SESSIONS, AS_OF)
    assert result["windows"]["5"]["direction"] == 1
    assert result["windows"]["1"]["direction"] == -1
    assert result["status"] == "different_windows_disagree"
    assert result["direction"] == 0
    assert result["vote_enabled"] is True


def test_dealer_only_and_small_or_zero_flow_do_not_vote():
    for updates in (
        dict(total=500000, foreign=-100000, trust=0, dealer=600000),
        dict(total=1000, foreign=1000, trust=0),
        dict(total=0, foreign=-200000, trust=200000),
    ):
        result = institutional_signal(inst_panel(**updates), PRICES, SESSIONS, AS_OF)
        assert result["direction"] == 0


def test_official_preference_and_quarantine_do_not_revive_legacy_rows():
    source_row = official(total=-300000, foreign=-100000, trust=-200000)
    rows = [legacy(code="3105")]
    panel, rejected = select([source_row], rows)
    assert panel["3105"][AS_OF]["total"] == -300000
    assert panel["3105"][AS_OF]["natural_pit"]
    assert not rejected
    for changes in ({"quality_status": "quarantine"}, {"unit": "lots"}):
        panel, rejected = select([{**source_row, **changes}], rows)
        assert "3105" not in panel
        assert rejected["official_quality_or_duplicate"] == 1
    panel, rejected = select(rows=[{**legacy(), "foreign": 150000}])
    assert not panel
    assert rejected["stored_quality_or_duplicate"] == 1


def test_future_creation_effective_dates_missing_components_and_duplicates_excluded():
    for rows in (
        [{**official(), "captured_at": CUTOFF + dt.timedelta(seconds=1)}],
        [official(AS_OF + dt.timedelta(days=1))], [official(), official()],
        [{**official(), "value_text": json.dumps(dict(
            market="上櫃", three_institution_total_net_shares=300000,
            foreign_net_shares_excluding_foreign_dealers=100000,
            investment_trust_net_shares=None, dealers_net_shares=0,
        ))}],
        [{**official(), "value_text": json.dumps(dict(
            market="上櫃", three_institution_total_net_shares="Infinity",
            foreign_net_shares_excluding_foreign_dealers=100000,
            investment_trust_net_shares=200000, dealers_net_shares=0,
        ))}],
    ):
        assert not select(official=rows)[0]


def test_invalid_or_no_trade_prices_abstain_even_when_flow_exists():
    for prices in ([], [(AS_OF, 50, 0, True)], [(AS_OF, None, 1000000, False)],
                   [(AS_OF, 50, 1000000, False)] * 2):
        assert not institutional_signal(inst_panel(), prices, SESSIONS, AS_OF)["direction"]


def branch_rows(day=AS_OF, negative=False):
    values = [("989e", "中和", 800, 0)] + [("989E" if i == 0 else f"B{i}", "賣方", 0, 25) for i in range(32)]
    values.append(("N", "雙向", 200, 200))
    return [dict(code="2330", date=day, branch=branch, name=name,
                 buy_lots=sell if negative else buy, sell_lots=buy if negative else sell,
                 net_lots=(sell-buy) if negative else (buy-sell), type="分點", captured_at=CUTOFF)
            for branch, name, buy, sell in values]


def broker_panels(rows):
    return select_broker_panels(rows, master=MASTER, as_of_date=AS_OF, cutoff_utc=CUTOFF)


def test_broker_zero_sum_is_not_direction_and_case_sensitive_branches_survive():
    panels, rejected = broker_panels(branch_rows())
    panel = panels["2330"][AS_OF]
    assert not rejected
    assert "989e" in panel["branches"] and "989E" in panel["branches"]
    assert panel["branches"]["989e"]["net"] == 800000  # Lots converted to shares.
    assert sum(b["net"] for b in panel["branches"].values()) == 0
    result = broker_signal(panels["2330"], PRICES, SESSIONS, AS_OF)
    assert result["direction"] == 1
    assert result["selected_window"]["concentration_pct"] == 42.5
    assert result["selected_window"]["conservative_abs_pct"] == 40.985
    inverse, _ = broker_panels(branch_rows(negative=True))
    assert broker_signal(inverse["2330"], PRICES, SESSIONS, AS_OF)["direction"] == -1


def test_broker_true_duplicates_incomplete_future_and_otc_data_are_skipped():
    rows = branch_rows()
    assert not broker_panels(rows + [rows[0]])[0]
    assert not broker_panels([{**r, "captured_at": CUTOFF + dt.timedelta(seconds=1)} for r in rows])[0]
    assert not broker_panels([{**r, "code": "3105"} for r in rows])[0]
    assert not broker_panels([r for r in rows if r["branch"] == "989e"])[0]
    panels, _ = broker_panels([{**r, "buy_lots": r["buy_lots"] / 2, "sell_lots": r["sell_lots"] / 2,
                              "net_lots": r["net_lots"] / 2} for r in rows])
    result = broker_signal(panels["2330"], PRICES, SESSIONS, AS_OF)
    assert not result["direction"]
    assert result["rejected_date_counts"]["broker_quote_scope_or_coverage_mismatch"] == 1


def test_broker_missing_sessions_not_padded_and_small_volume_skipped():
    panels, _ = broker_panels(branch_rows() + branch_rows(SESSIONS[-2]))
    result = broker_signal(panels["2330"], PRICES, SESSIONS, AS_OF)
    assert set(result["windows"]) == {"1"}
    assert not broker_signal(panels["2330"], [(AS_OF, 50, 100000, False)], SESSIONS, AS_OF)["direction"]
    old, _ = broker_panels(branch_rows(SESSIONS[-2]))
    assert not broker_signal(old["2330"], PRICES, SESSIONS, AS_OF)["direction"]


def test_daily_waits_for_actual_collection_but_missing_or_failed_feed_has_bounded_delay():
    evening = dt.datetime(2026, 10, 7, 20, 2)
    assert should_wait_for_collection(scan_mode="daily", now_taipei=evening)
    partial = {COLLECTION_TASKS[0]: "success", COLLECTION_TASKS[1]: "running"}
    assert should_wait_for_collection(scan_mode="daily", now_taipei=evening, task_states=partial)
    for state in ("success", "failed", "skipped", "upstream_failed"):
        assert not should_wait_for_collection(scan_mode="daily", now_taipei=evening,
                                             task_states={COLLECTION_TASKS[0]: "success", COLLECTION_TASKS[1]: state})
    assert not should_wait_for_collection(scan_mode="daily", now_taipei=evening, run_state="failed")
    assert not should_wait_for_collection(scan_mode="daily", now_taipei=evening.replace(hour=21, minute=30))
    assert not should_wait_for_collection(scan_mode="intraday", now_taipei=evening)


def test_market_wide_quote_gap_cannot_shorten_actual_trading_calendar():
    class Sources:
        def execute(self, query, params): self.calendar = "holiday.taiwan" in query
        def fetchall(self):
            return [(AS_OF-dt.timedelta(days=i),) for i in range(45)
                    if (AS_OF-dt.timedelta(days=i)).weekday() >= 5] if self.calendar else []
    class Stocks:
        def execute(self, query, params): self.flow = "FROM 三大法人買賣" in query
        def fetchall(self):
            return [("2330", d, 300000, 100000, 200000, 0, CUTOFF) for d in SESSIONS] if self.flow else []
    missing_day = SESSIONS[-3]
    prices = {"2330": [r for r in PRICES if r[0] != missing_day]}
    context, readiness = load_equity_order_flow(
        Sources(), Stocks(), master=MASTER, as_of_date=AS_OF, cutoff_utc=CUTOFF,
        price_rows_by_code=prices, session_dates_by_market={"上市": {r[0] for r in prices["2330"]}, "上櫃": set()},
    )
    assert context["2330"]["institutions"]["direction"] == 1
    assert readiness["上市"]["status"] == "evaluated"
    assert readiness["上市"]["vote_enabled"] is True
    assert readiness["上市"]["three_session_ready_count"] == 0
    assert missing_day.isoformat() in readiness["上市"]["expected_session_dates"]
    assert readiness["上市"]["calendar_verified"]


def test_tpex_flow_is_evaluated_with_pit_diagnostics_and_listed_table_is_queried():
    official_rows = [official(day=day) for day in SESSIONS]
    rows = [
        (f"hash-{index}", row["entity_id"], row["effective_date"],
         row["quality_status"], row["unit"], row["value_text"], row["captured_at"],
         row["capture_basis"], row["dag_id"], row["dag_run_id"], row["successful_source_run"])
        for index, row in enumerate(official_rows)
    ]

    class Sources:
        def execute(self, query, params):
            self.query = query

        def fetchall(self):
            if "holiday.taiwan" in self.query:
                return [(AS_OF - dt.timedelta(days=i),) for i in range(45)
                        if (AS_OF - dt.timedelta(days=i)).weekday() >= 5]
            if "source_record_versions" in self.query:
                return rows
            return []

    class Stocks:
        def __init__(self): self.queries = []
        def execute(self, query, params): self.queries.append(query)
        def fetchall(self): return []

    stocks = Stocks()
    contexts, readiness = load_equity_order_flow(
        Sources(), stocks, master=MASTER, as_of_date=AS_OF, cutoff_utc=CUTOFF,
        price_rows_by_code={"2330": PRICES, "3105": PRICES},
        session_dates_by_market={"上市": set(SESSIONS), "上櫃": set(SESSIONS)},
    )

    otc = readiness["上櫃"]
    assert otc["same_day_count"] == otc["natural_pit_same_day_count"] == 1
    assert otc["five_session_ready_count"] == 1
    assert otc["vote_enabled"] is True
    assert otc["qualified_vote_count"] == 1
    assert contexts["3105"]["institutions"]["direction"] == 1
    assert readiness["上市"]["status"] == "same_day_data_unavailable"
    assert readiness["上市"]["same_day_count"] == 0
    assert any("三大法人買賣" in query for query in stocks.queries)
    assert "已納入判讀" in format_flow_readiness("上櫃", otc, 1)
    assert "同日已收 0/1" in format_flow_readiness("上市", readiness["上市"], 1)


def test_broker_raw_threshold_is_insufficient_after_rounding_bound():
    rows = branch_rows()
    rows[-1].update(buy_lots=1300, sell_lots=1300)
    panels, _ = broker_panels(rows)
    result = broker_signal(panels["2330"], [(AS_OF, 50, 2100000, False)], SESSIONS, AS_OF)
    assert result["windows"]["1"]["concentration_pct"] > 20
    assert result["windows"]["1"]["conservative_abs_pct"] < 20
    assert result["direction"] == 0


def test_broker_period_aggregates_branch_identity_and_reversal_abstains():
    panels, _ = broker_panels([r for d in SESSIONS[-3:] for r in branch_rows(d)])
    result = broker_signal(panels["2330"], PRICES, SESSIONS, AS_OF)
    assert result["selected_window"]["days"] == 3
    assert result["selected_window"]["top_buyers"][0]["net_shares"] == 2400000
    panels, _ = broker_panels([r for d in SESSIONS for r in branch_rows(d, negative=d == AS_OF)])
    result = broker_signal(panels["2330"], PRICES, SESSIONS, AS_OF)
    assert result["windows"]["5"]["direction"] == 1
    assert result["windows"]["1"]["direction"] == -1
    assert result["status"] == "different_windows_disagree"
    assert result["direction"] == 0
