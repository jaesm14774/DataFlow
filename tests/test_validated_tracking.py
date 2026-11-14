import datetime as dt

import pandas as pd
import pytest

from source_extension import validated_tracking

RULE = {
    "id": "breakout_60d_volume", "label": "突破 60 日高＋爆量", "status": "active", "horizon_sessions": 10,
    "params": {"max_names_shown": 5, "min_turnover_ntd": 50_000_000},
    "evidence": {"horizon": 10, "oos_dates": 886, "mean_excess_after_cost_pct": 0.17, "t_nw": 3.0,
                 "years_total": 11, "years_positive": 9},
}


def _panel(sessions=80):
    days = pd.bdate_range(end=pd.Timestamp("2026-11-13"), periods=sessions)
    rows = []
    for code in ("2330", "2317", "1101", "2603", "2002"):
        for day in days:
            rows.append([day, code, 100.0, 10_000.0])
    # an illiquid name that must not enter the benchmark
    for day in days:
        rows.append([day, "9999", 100.0, 1.0])
    frame = pd.DataFrame(rows, columns=["date", "code", "close", "vol"])
    return frame, days


def test_live_pick_is_scored_against_the_same_day_liquid_universe_not_zero():
    """A +10% pick when the liquid universe is flat except the pick itself.

    Universe (5 liquid names) mean = +2%, so the excess is +8% — not +10% — and the
    illiquid name never dilutes the benchmark."""
    frame, days = _panel()
    signal_pos = 60
    frame.loc[(frame["code"] == "2330") & (frame["date"] >= days[signal_pos + 1 + 10]), "close"] = 110.0
    picks = [{"id": 7, "as_of_date": days[signal_pos].date(), "entity_id": "2330"}]
    [outcome] = validated_tracking.compute_pick_outcomes(
        frame, picks, horizon=10, min_turnover_ntd=50_000_000
    )
    assert outcome["status"] == "settled"
    assert outcome["entry_date"] == days[signal_pos + 1].date()
    assert outcome["exit_date"] == days[signal_pos + 11].date()
    assert outcome["stock_return_pct"] == pytest.approx(10.0)
    assert outcome["universe_mean_return_pct"] == pytest.approx(2.0)
    assert outcome["excess_return_pct"] == pytest.approx(8.0)


def test_pick_stays_pending_until_the_holding_period_has_elapsed():
    frame, days = _panel()
    picks = [{"id": 1, "as_of_date": days[-5].date(), "entity_id": "2330"}]
    assert validated_tracking.compute_pick_outcomes(
        frame, picks, horizon=10, min_turnover_ntd=50_000_000
    ) == []


def test_delisted_pick_is_closed_as_no_price_instead_of_pending_forever():
    frame, days = _panel()
    signal_pos = 50
    frame = frame[~((frame["code"] == "2330") & (frame["date"] > days[signal_pos + 1]))]
    picks = [{"id": 3, "as_of_date": days[signal_pos].date(), "entity_id": "2330"}]
    [outcome] = validated_tracking.compute_pick_outcomes(
        frame, picks, horizon=10, min_turnover_ntd=50_000_000
    )
    assert outcome["status"] == "no_price"


class _Cursor:
    def __init__(self, fetchone=None):
        self.executed = []
        self.rowcount = 1
        self._fetchone = list(fetchone or [])

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetchone.pop(0)


def test_every_hit_is_recorded_with_rank_and_only_top_names_marked_shown():
    cursor = _Cursor()
    hits = [{"code": str(1000 + i), "close": 50.0, "metric": 5.0 - i * 0.1} for i in range(8)]
    inserted = validated_tracking.record_picks(
        cursor, rule=RULE, version="validated_v1", as_of_date=dt.date(2026, 11, 13),
        hits=hits, names={"1000": "甲"}, now=dt.datetime(2026, 11, 13, 20, 0),
    )
    assert inserted == 8
    limit = RULE["params"]["max_names_shown"]
    shown = [params[8] for _, params in cursor.executed]
    ranks = [params[7] for _, params in cursor.executed]
    assert ranks == list(range(1, 9))
    assert shown == [int(rank <= limit) for rank in ranks]
    assert all("INSERT IGNORE" in sql for sql, _ in cursor.executed)  # reruns cannot double count


def test_tracking_line_says_there_is_no_result_before_any_pick_settles():
    line = validated_tracking.tracking_line(validated_tracking.summarize_by_date([], 10, pending=12), RULE)
    assert "12" in line and "還沒有實際成績" in line


def _days(count, mean, per_day=3, start=dt.date(2026, 1, 5)):
    return [(start + dt.timedelta(days=i), mean, per_day) for i in range(count)]


def test_a_short_bad_live_record_never_recommends_stopping_a_rule():
    summary = validated_tracking.summarize_by_date(_days(8, -3.0, per_day=10), 10)
    line = validated_tracking.tracking_line(summary, RULE)
    assert summary["dates"] == 8 and summary["settled"] == 80
    assert "停用" not in line and "建議" not in line
    assert "-3.00%" in line  # the number is shown honestly, just never turned into a verdict
    assert "由每週的長期驗證決定" in validated_tracking.LIVE_REFERENCE_NOTE


def test_live_evidence_counts_signal_dates_not_picks():
    """One busy day with 50 winners must not outweigh a quiet losing day."""
    day = dt.date(2026, 3, 2)
    summary = validated_tracking.summarize_by_date(
        [(day, 10.0, 50), (day + dt.timedelta(days=30), -10.0, 1)], 10
    )
    assert summary["mean_excess_pct"] == pytest.approx(0.0)
    assert summary["settled"] == 51 and summary["dates"] == 2


def test_overlapping_holding_periods_are_not_counted_as_independent_observations():
    consecutive = validated_tracking.summarize_by_date(_days(20, 0.5), 10)
    assert consecutive["dates"] == 20
    assert consecutive["independent_dates"] < 5
    spaced = validated_tracking.summarize_by_date(
        [(dt.date(2026, 1, 5) + dt.timedelta(days=20 * i), 0.5, 3) for i in range(8)], 10
    )
    assert spaced["independent_dates"] == 8


def test_live_t_statistic_needs_enough_dates_and_the_recent_window_is_separate():
    few = validated_tracking.summarize_by_date(_days(5, 1.0), 10)
    assert few["t_nw"] is None
    long_run = _days(100, 0.2)
    summary = validated_tracking.summarize_by_date(long_run[:-63] + [(d, -2.0, n) for d, _, n in long_run[-63:]], 10)
    assert summary["t_nw"] is not None
    assert summary["recent_mean_excess_pct"] == pytest.approx(-2.0)
    assert summary["mean_excess_pct"] != summary["recent_mean_excess_pct"]


def test_candidates_record_only_their_strongest_hits_and_never_mark_any_as_shown():
    cursor = _Cursor()
    hits = [{"code": str(1000 + i), "close": 50.0, "metric": 1.0} for i in range(200)]
    rule = validated_tracking.tracking_rule({**RULE, "status": "candidate"})
    inserted = validated_tracking.record_picks(
        cursor, rule=rule, version="rules_v1", as_of_date=dt.date(2026, 11, 13),
        hits=hits, names={}, now=dt.datetime(2026, 11, 13, 20, 0),
    )
    assert inserted == validated_tracking.SHADOW_PICKS_PER_DAY
    assert all(params[8] == 0 for _, params in cursor.executed)


def test_only_an_active_rule_gets_a_shown_limit():
    assert validated_tracking.tracking_rule(RULE)["params"]["max_names_shown"] == 5
    assert validated_tracking.tracking_rule({**RULE, "status": "suspended"})["params"]["max_names_shown"] == 0


def test_summary_deducts_cost_and_counts_names_that_beat_it():
    summary = validated_tracking.summarize_excess([2.0, 0.5, -1.0, 1.0])
    assert summary["mean_excess_pct"] == pytest.approx(0.625)
    assert summary["mean_excess_after_cost_pct"] == pytest.approx(0.025)
    assert summary["beat_cost_share"] == pytest.approx(0.5)


def test_weekly_scorecard_compares_this_week_with_the_two_weeks_before_the_policy():
    rows = iter([
        (60, 56, 4, 1),   # 14 days before the policy
        (6, 6, 0, 0),     # this week
    ])

    class Cursor(_Cursor):
        def fetchone(self):
            try:
                return next(rows)
            except StopIteration:
                return (0, 0)

        def fetchall(self):
            return []

    card = validated_tracking.weekly_scorecard(
        Cursor(), today=dt.date(2026, 11, 20), policy_start=dt.date(2026, 11, 14), rules=[RULE]
    )
    assert card["before_per_week"]["messages"] == 30
    assert card["before_per_week"]["failed"] == 2
    assert card["after_week"]["messages"] == 6
    embed = validated_tracking.scorecard_embed(card, [RULE, {**RULE, "id": "x", "label": "觀察規則", "status": "candidate"}])
    assert "30 則 → 本週 6 則" in embed["description"]
    assert "未能產出" in embed["description"]
    assert "長期扣成本 +0.17%" in embed["description"]  # the long-term record is part of the scorecard
    assert "觀察中" in embed["description"] and "觀察規則" in embed["description"]


def test_suppressed_intraday_candidates_are_tracked_as_never_pushed_shadow_picks():
    cursor = _Cursor()
    rows = [
        {"entity_id": "2330", "security_name": "台積電", "as_of_date": dt.date(2026, 11, 14),
         "signal_keys": "insider_transfer_planned"},
        {"entity_id": "2317", "security_name": None, "as_of_date": "2026-11-13 09:30:00",
         "signal_keys": "company_announcement"},
    ]
    inserted = validated_tracking.record_shadow_candidates(
        cursor, rule=validated_tracking.SHADOW_INTRADAY_RULE, version="validated_v1",
        candidates=rows, now=dt.datetime(2026, 11, 14, 8, 0),
    )
    assert inserted == 2
    for sql, params in cursor.executed:
        assert "INSERT IGNORE" in sql and "0, %s, 'pending'" in sql  # shown=0: never pushed
        assert params[0] == validated_tracking.SHADOW_INTRADAY_RULE["id"]
    assert cursor.executed[1][1][2] == dt.date(2026, 11, 13)


def test_weekend_announcement_is_settled_from_the_next_trading_session():
    """An announcement dated on a Saturday enters at the next session's close, not never."""
    frame, days = _panel()
    friday_pos = next(i for i in range(50, 70) if days[i].weekday() == 4)
    saturday = days[friday_pos].date() + dt.timedelta(days=1)
    frame.loc[(frame["code"] == "2330") & (frame["date"] >= days[friday_pos + 11]), "close"] = 105.0
    picks = [{"id": 9, "as_of_date": saturday, "entity_id": "2330"}]
    [outcome] = validated_tracking.compute_pick_outcomes(
        frame, picks, horizon=10, min_turnover_ntd=50_000_000
    )
    assert outcome["status"] == "settled"
    assert outcome["entry_date"] == days[friday_pos + 1].date()
    assert outcome["stock_return_pct"] == pytest.approx(5.0)


def test_scorecard_shows_what_silencing_intraday_alerts_cost_or_saved():
    pending = validated_tracking.shadow_line(validated_tracking.summarize_excess([], pending=40))
    assert "40" in pending and "尚未結算" in pending
    settled = validated_tracking.shadow_line(validated_tracking.summarize_excess([-1.0, -2.0, 0.5]))
    assert "已結算 3 檔" in settled and "關掉沒有損失" in settled


def test_each_pick_settles_with_the_holding_period_it_was_recorded_under():
    """A candidate recorded at 5 sessions keeps that horizon even after the rule moves to 10."""
    frame, days = _panel()
    signal_day = days[50].date()
    updates = []

    class Ledger(_Cursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if sql.strip().startswith("UPDATE"):
                updates.append(params)

        def fetchall(self):
            return [(1, signal_day, "2330", 5), (2, signal_day, "2330", 10)]

    rule = {**RULE, "horizon_sessions": 10}
    counts = validated_tracking.settle_pending_picks(
        Ledger(), object(), rule=rule, today=days[-1].date(), now=dt.datetime(2026, 11, 14),
        fetch_prices=lambda cursor, today, span: frame,
    )
    assert counts["settled"] == 2
    exit_dates = {params[-1]: params[2] for params in updates}  # id -> exit_date
    assert exit_dates[1] == days[50 + 1 + 5].date() and exit_dates[2] == days[50 + 1 + 10].date()
