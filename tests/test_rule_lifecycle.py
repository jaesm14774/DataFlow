import datetime as dt

import numpy as np
import pandas as pd
import pytest

from source_extension import rule_lifecycle as lifecycle
from source_extension import rule_revalidation, signal_rules

RULE_IDS = [rule.id for rule in signal_rules.RULES]
TODAY = dt.date(2026, 11, 14)


def synthetic_daily(profiles, *, dates=None, seed=11):
    """Date-level evidence for all rules x horizons.

    ``profiles`` maps rule id -> (mean excess %, win probability); others are noise around 0.
    ``recent`` entries may override the last 63 dates: id -> mean.
    """
    rng = np.random.default_rng(seed)
    dates = dates if dates is not None else pd.bdate_range("2016-01-04", "2026-10-30")
    parts = []
    for rule_id in RULE_IDS:
        mean, win_p = profiles.get(rule_id, (0.0, 0.38))
        for horizon in signal_rules.HORIZONS:
            excess = rng.normal(mean, 1.2, len(dates))
            n = np.full(len(dates), 5)
            parts.append(pd.DataFrame({
                "rule_id": rule_id, "horizon": horizon, "signal_date": dates, "n_names": n,
                "mean_excess_pct": excess, "n_win": rng.binomial(5, 0.5, len(dates)),
                "n_win_after_cost": rng.binomial(5, win_p, len(dates)),
                "base_n": 1000, "base_win_after_cost": 380,
            }))
    return pd.concat(parts, ignore_index=True)


def evidence(**overrides):
    base = {"horizon": 10, "oos_dates": 900, "mean_excess_after_cost_pct": 0.2, "gate_pass": True,
            "recent_reference": {"dates": 63, "mean_excess_after_cost_pct": -1.0}}
    return {**base, **overrides}


# --- the policy -----------------------------------------------------------------------------

def test_a_bad_recent_quarter_does_not_demote_a_rule_with_a_good_long_record():
    decision = lifecycle.decide(
        lifecycle.ACTIVE, evidence(), days_in_status=400, new_dates_since_change=120
    )
    assert decision.status == lifecycle.ACTIVE and not decision.changed


def test_sustained_long_term_negative_demotes_only_after_the_minimum_time_and_new_dates():
    bad = evidence(mean_excess_after_cost_pct=-0.15, gate_pass=False)
    too_soon = lifecycle.decide(lifecycle.ACTIVE, bad, days_in_status=20, new_dates_since_change=200)
    too_few = lifecycle.decide(lifecycle.ACTIVE, bad, days_in_status=400, new_dates_since_change=5)
    enough = lifecycle.decide(lifecycle.ACTIVE, bad, days_in_status=400, new_dates_since_change=200)
    assert not too_soon.changed and not too_few.changed
    assert enough.status == lifecycle.SUSPENDED and enough.changed


def test_failing_the_win_rate_gate_alone_is_not_a_reason_to_demote():
    """Promotion needs the full gate; demotion needs the long-term edge to be actually negative."""
    barely = evidence(mean_excess_after_cost_pct=0.02, gate_pass=False)
    decision = lifecycle.decide(lifecycle.ACTIVE, barely, days_in_status=400, new_dates_since_change=200)
    assert decision.status == lifecycle.ACTIVE


def test_promotion_needs_the_full_gate_and_a_seasoned_status():
    promising = evidence()
    assert not lifecycle.decide(lifecycle.CANDIDATE, promising, days_in_status=10, new_dates_since_change=5).changed
    failing = evidence(gate_pass=False)
    assert not lifecycle.decide(lifecycle.CANDIDATE, failing, days_in_status=400, new_dates_since_change=200).changed
    promoted = lifecycle.decide(lifecycle.SUSPENDED, promising, days_in_status=400, new_dates_since_change=200)
    assert promoted.status == lifecycle.ACTIVE


def test_a_borderline_rule_does_not_flip_back_and_forth():
    """Evidence that hovers on the gate every week must not make the status flicker."""
    status, since = lifecycle.ACTIVE, TODAY - dt.timedelta(days=400)
    transitions = 0
    for week in range(52):
        today = TODAY + dt.timedelta(weeks=week)
        wobble = 0.03 if week % 2 == 0 else -0.03
        ev = evidence(mean_excess_after_cost_pct=wobble, gate_pass=wobble > 0)
        new_dates = (today - since).days * 5 // 7
        decision = lifecycle.decide(
            status, ev, days_in_status=(today - since).days, new_dates_since_change=new_dates
        )
        if decision.changed:
            status, since, transitions = decision.status, today, transitions + 1
    assert transitions <= 4  # a naive weekly pass/fail would change status ~50 times


def test_missing_evidence_keeps_the_current_status():
    decision = lifecycle.decide(
        lifecycle.ACTIVE, {"insufficient_data": True}, days_in_status=999, new_dates_since_change=999
    )
    assert decision.status == lifecycle.ACTIVE and not decision.changed


# --- the long-window gate -------------------------------------------------------------------

def _gate(daily):
    return lifecycle.verdicts(lifecycle.summarize(daily))


def test_gate_accepts_a_consistent_edge_and_rejects_noise():
    daily = synthetic_daily({"breakout_60d_volume": (1.6, 0.62)})
    table = _gate(daily)
    assert set(table[table["passes"]]["signal"]) == {"breakout_60d_volume"}


def test_the_recent_quarter_is_reference_only_and_never_enters_the_gate():
    daily = synthetic_daily({"breakout_60d_volume": (1.6, 0.62)})
    last = daily[daily["rule_id"] == "breakout_60d_volume"]["signal_date"].drop_duplicates().nlargest(63)
    crash = (daily["rule_id"] == "breakout_60d_volume") & daily["signal_date"].isin(last)
    daily.loc[crash, "mean_excess_pct"] = -4.0
    table = _gate(daily)
    ev = lifecycle.long_term_evidence(daily, table, "breakout_60d_volume", 10)
    assert ev["gate_pass"] is True
    assert ev["recent_reference"]["mean_excess_after_cost_pct"] < -3  # shown, but not decisive


def test_one_rules_movement_cannot_change_another_rules_numbers_only_the_shared_cutoff():
    """The family-wise cutoff is shared, so the verdict is a snapshot: it is evidence, not status."""
    base = synthetic_daily({"breakout_60d_volume": (1.6, 0.62)})
    other = synthetic_daily({"breakout_60d_volume": (1.6, 0.62), "trust_buy_5d_top": (0.9, 0.5)})
    a = lifecycle.long_term_evidence(base, _gate(base), "breakout_60d_volume", 10)
    b = lifecycle.long_term_evidence(other, _gate(other), "breakout_60d_volume", 10)
    assert a["mean_excess_pct"] == b["mean_excess_pct"] and a["t_nw"] == b["t_nw"]


# --- status bookkeeping against the ledger --------------------------------------------------

class Ledger:
    """In-memory stand-in for the three rule tables."""

    def __init__(self, monkeypatch, statuses=None):
        self.statuses = dict(statuses or {})
        self.history = []
        monkeypatch.setattr(rule_revalidation, "read_statuses", lambda cursor: dict(self.statuses))
        monkeypatch.setattr(rule_revalidation, "_write_status", self._write_status)
        monkeypatch.setattr(rule_revalidation, "_write_history", self._write_history)

    def _write_status(self, cursor, rule_id, version, status, horizon, since, evidence, today, now):
        self.statuses[rule_id] = {
            "status": status, "horizon_sessions": horizon, "status_since": since,
            "evidence": evidence, "last_evaluated_on": today,
        }

    def _write_history(self, cursor, rule_id, from_status, to_status, reason, evidence, today, now):
        self.history.append((rule_id, from_status, to_status, today))


def registry(seed_active=()):
    return {"ruleset": "rules_v1", "rules": [
        {"id": rid, "seed_status": "active" if rid in seed_active else "candidate", "horizon_sessions": 10}
        for rid in RULE_IDS
    ]}


def _evaluate(ledger, daily, reg, today=TODAY):
    return rule_revalidation.evaluate_statuses(
        object(), daily, reg, today=today, now=dt.datetime(2026, 11, 14, 9)
    )


def test_first_run_seeds_every_rule_and_records_each_in_the_history(monkeypatch):
    ledger = Ledger(monkeypatch)
    daily = synthetic_daily({"breakout_60d_volume": (1.6, 0.62)})
    _evaluate(ledger, daily, registry(seed_active={"breakout_60d_volume"}))
    assert set(ledger.statuses) == set(RULE_IDS)
    assert ledger.statuses["breakout_60d_volume"]["status"] == lifecycle.ACTIVE
    assert {h[0] for h in ledger.history} == set(RULE_IDS)


def test_a_control_group_is_tracked_but_never_pushed_even_with_a_great_record(monkeypatch):
    ledger = Ledger(monkeypatch)
    daily = synthetic_daily({"foreign_sell_5d_bottom": (1.6, 0.62)})
    _evaluate(ledger, daily, registry(seed_active={"foreign_sell_5d_bottom"}))
    assert ledger.statuses["foreign_sell_5d_bottom"]["status"] == lifecycle.CANDIDATE
    later = TODAY + dt.timedelta(days=400)
    _evaluate(ledger, daily, registry(), today=later)
    assert ledger.statuses["foreign_sell_5d_bottom"]["status"] == lifecycle.CANDIDATE


def test_a_candidate_is_promoted_only_once_its_long_record_has_matured_in_that_status(monkeypatch):
    daily = synthetic_daily({"breakout_60d_volume": (1.6, 0.62)})
    ledger = Ledger(monkeypatch)
    _evaluate(ledger, daily, registry())  # seeded as candidate today
    _evaluate(ledger, daily, registry(), today=TODAY + dt.timedelta(days=30))
    assert ledger.statuses["breakout_60d_volume"]["status"] == lifecycle.CANDIDATE
    changes = _evaluate(ledger, daily, registry(), today=TODAY + dt.timedelta(days=120))
    assert ledger.statuses["breakout_60d_volume"]["status"] == lifecycle.ACTIVE
    assert [c["rule_id"] for c in changes] == ["breakout_60d_volume"]


def test_an_active_rule_survives_a_terrible_quarter_but_not_a_negative_long_record(monkeypatch):
    since = TODAY - dt.timedelta(days=200)
    active = {"status": "active", "horizon_sessions": 10, "status_since": since, "evidence": {}}

    strong = synthetic_daily({"breakout_60d_volume": (1.6, 0.62)})
    last = strong[strong["rule_id"] == "breakout_60d_volume"]["signal_date"].drop_duplicates().nlargest(63)
    strong.loc[(strong["rule_id"] == "breakout_60d_volume") & strong["signal_date"].isin(last), "mean_excess_pct"] = -4.0
    ledger = Ledger(monkeypatch, {"breakout_60d_volume": dict(active)})
    _evaluate(ledger, strong, registry(seed_active={"breakout_60d_volume"}))
    assert ledger.statuses["breakout_60d_volume"]["status"] == lifecycle.ACTIVE

    weak = synthetic_daily({"breakout_60d_volume": (0.2, 0.40)})  # below the 0.6% cost for years
    ledger = Ledger(monkeypatch, {"breakout_60d_volume": dict(active)})
    changes = _evaluate(ledger, weak, registry(seed_active={"breakout_60d_volume"}))
    assert ledger.statuses["breakout_60d_volume"]["status"] == lifecycle.SUSPENDED
    assert ("breakout_60d_volume", "active", "suspended", TODAY) in ledger.history
    moved = [c for c in changes if c["rule_id"] == "breakout_60d_volume"]
    assert [(c["from"], c["to"]) for c in moved] == [("active", "suspended")]


def test_status_falls_back_to_the_registry_seed_when_the_ledger_is_unreachable():
    class Broken:
        def execute(self, *args, **kwargs):
            raise ConnectionError("ledger down")

    reg = registry(seed_active={"breakout_60d_volume"})
    states = rule_revalidation.load_rule_states(Broken(), reg)
    by_id = {s["id"]: s for s in states}
    assert by_id["breakout_60d_volume"]["status"] == lifecycle.ACTIVE
    assert by_id["trust_buy_5d_top"]["status"] == lifecycle.CANDIDATE
    assert not by_id["breakout_60d_volume"]["from_database"]


def test_database_status_wins_over_the_registry_seed(monkeypatch):
    Ledger(monkeypatch, {"breakout_60d_volume": {
        "status": "suspended", "horizon_sessions": 10, "status_since": TODAY,
        "evidence": {}, "last_evaluated_on": TODAY,
    }})
    states = rule_revalidation.load_rule_states(object(), registry(seed_active={"breakout_60d_volume"}))
    assert {s["id"]: s for s in states}["breakout_60d_volume"]["status"] == lifecycle.SUSPENDED


def test_the_multiple_testing_family_is_fixed_even_if_only_one_rule_has_data():
    """A rule must not get an easier threshold just because the others have no rows yet."""
    rows = []
    for window, t in (("IS", 1.0), ("OOS", 2.6)):
        rows.append({
            "signal": "breakout_60d_volume", "horizon": 10, "window": window, "dates": 900,
            "mean_excess_pct": 1.0, "mean_excess_after_cost_pct": 0.4, "t_nw": t,
            "stock_win_rate_after_cost": 0.45, "base_win_rate_after_cost": 0.39,
        })
    table = lifecycle.verdicts(pd.DataFrame(rows))
    assert not table.iloc[0]["bh_pass"]  # one-sided p~0.005 is above 0.10 / 36, though below 0.10 / 1


def test_a_registry_seed_cannot_bypass_the_gate(monkeypatch):
    ledger = Ledger(monkeypatch)
    daily = synthetic_daily({})  # nothing has an edge
    _evaluate(ledger, daily, registry(seed_active={"breakout_60d_volume"}))
    assert ledger.statuses["breakout_60d_volume"]["status"] == lifecycle.CANDIDATE


def test_a_status_change_is_announced_once_the_next_trading_day():
    seen = []

    class Cursor:
        def execute(self, sql, params=None):
            seen.append(params)

        def fetchall(self):
            return []

    monday, tuesday = dt.date(2026, 11, 16), dt.date(2026, 11, 17)
    rule_revalidation.recent_transitions(Cursor(), monday)
    rule_revalidation.recent_transitions(Cursor(), tuesday)
    assert seen[0] == (dt.date(2026, 11, 13), monday)  # Saturday's decision is first shown on Monday
    assert seen[1] == (monday, tuesday)  # ...and no longer on Tuesday
