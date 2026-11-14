import datetime as dt
import json

from source_extension.backtest_diagnostics import summarize_daily_backtest

DATES = [dt.date(2026, 11, day) for day in (2, 3, 4)]
STOCKS = 300


class FakeCursor:
    """Answers the four queries summarize_daily_backtest issues, by shape."""

    connection = None

    def __init__(self, selected_family=False):
        self.description = []
        self._rows = []
        self.selected_family = selected_family

    def _evidence_details(self, index):
        top = index >= STOCKS * 0.9
        bottom = index < STOCKS * 0.1
        research = {"research:margin_utilization": 1} if top else (
            {"research:margin_utilization": -1} if bottom else {}
        )
        return {
            "family_directions": {"price": 1} if index % 50 == 0 else {},
            "family_status": {"price": "directional_up" if index % 50 == 0 else "neutral"},
            "research_family_directions": research,
            "policy_version": "p1",
            "candidate_selected": False,
        }

    def execute(self, query, params=None):
        if "COUNT(DISTINCT c.entity_id)" in query:
            self._set(["run_key", "expected_count", "actual_count"],
                      [("run-%d" % day.day, STOCKS, STOCKS) for day in DATES])
        elif "SELECT DISTINCT o.signal_date" in query:
            self._set(["signal_date"], [(day,) for day in DATES])
        elif "notification_status" in query:
            day = params[0]
            self._set(
                ["candidate_id", "entity_type", "details_text", "notification_status"],
                [
                    (day.day * 1000 + i, "stock_evidence",
                     json.dumps(self._evidence_details(i)), "unlinked_or_not_listed")
                    for i in range(STOCKS)
                ],
            )
        elif "o.horizon_sessions" in query:
            day = params[0]
            rows = []
            for i in range(STOCKS):
                stock_return = i / STOCKS * 2.0  # monotone in the index => top decile wins
                rows.append((
                    1, day.day * 1000 + i, "run-%d" % day.day, "%04d" % (1000 + i), "上市",
                    "ok", "twse_quotes", day, "訊號不足", "next_close", 1, stock_return,
                    0.0, 0, "settled", None, "stock_evidence", dt.datetime(2026, 11, day.day),
                    "dag", "run",
                ))
            self._set(
                ["id", "candidate_id", "run_key", "entity_id", "market", "source_status",
                 "outcome_price_source_key", "signal_date", "direction", "outcome_basis",
                 "horizon_sessions", "stock_return_pct", "excess_return_pp", "directional_hit",
                 "status", "error_reason", "entity_type", "started_at", "source_dag_id",
                 "source_dag_run_id"],
                rows,
            )
        else:  # pragma: no cover - guards against an unexpected query shape
            raise AssertionError(query)

    def _set(self, columns, rows):
        self.description = [(name,) for name in columns]
        self._rows = list(rows)

    def fetchmany(self, size):
        batch, self._rows = self._rows[:size], self._rows[size:]
        return batch


def test_research_family_is_scored_against_universe_mean_and_stays_non_voting():
    result = summarize_daily_backtest(FakeCursor(), as_of_date=dt.date(2026, 11, 5))

    # production family table is untouched by research families
    production = {row["family"] for row in result["family_counterfactuals"]}
    assert "price" in production
    assert not any(name.startswith("research:") for name in production)

    relative = [
        row for row in result["research_universe_relative"]
        if row["family"] == "research:margin_utilization"
    ]
    assert len(relative) == 1
    row = relative[0]
    assert row["signal_date_count"] == 3
    # top decile (+1) averages ~+0.9pp over the ~1.0 universe mean; bottom decile
    # (-1) earns the same after sign, so the directional excess is clearly positive
    assert 0.8 < row["mean_directional_excess_pp"] < 1.0
    assert row["market_relative_hit_rate"] == 1.0

    summary = [r for r in result["research_family_summary"] if r["horizon_sessions"] == 1]
    assert len(summary) == 1
    assert summary[0]["status"] == "insufficient_dates"  # only 3 independent dates
    assert summary[0]["signal_dates"] == 3


def test_missing_research_keys_leave_old_snapshots_working():
    class Legacy(FakeCursor):
        def _evidence_details(self, index):
            details = super()._evidence_details(index)
            details.pop("research_family_directions")
            return details

    result = summarize_daily_backtest(Legacy(), as_of_date=dt.date(2026, 11, 5))
    assert result["research_universe_relative"] == []
    assert result["research_family_summary"] == []
    assert result["deduplicated_outcome_rows"] == STOCKS * len(DATES)


def test_research_summary_spaces_independent_dates_for_dates_and_iso_strings():
    from source_extension.backtest_diagnostics import _new_group, _research_summary

    def groups(key_of):
        group = _new_group()
        for offset in range(60):
            day = dt.date(2026, 1, 5) + dt.timedelta(days=offset)
            if day.weekday() < 5:
                group["by_date_excess"][key_of(day)] = [0.5, 1]
        key = ("p1", "natural_parent_run", "上市", "ok", "src", "next_close", 5, "research:x")
        return {key: group}

    by_date = _research_summary(groups(lambda day: day))[0]
    by_text = _research_summary(groups(lambda day: day.isoformat()))[0]
    assert by_date["independent_dates"] == by_text["independent_dates"]
    assert by_date["independent_dates"] < by_date["signal_dates"] / 3
