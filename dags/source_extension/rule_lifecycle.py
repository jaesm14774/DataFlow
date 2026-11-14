"""Long-horizon validation and lifecycle of candidate signal rules (pure functions).

Nothing here is a one-off verdict. Every rule is re-measured on all matured history
each week; the *status* only moves under hysteresis so that a few good or bad weeks
cannot flip it:

* evidence unit is the signal *date* (mean excess return of that day's hits over the
  same-day liquid-universe mean), scored with Newey-West t, Benjamini-Hochberg across the
  fixed pre-registered family (rules x holding periods), after a 0.6% round-trip cost;
* ``candidate``  - tracked in shadow only, never pushed;
* ``active``     - pushed. Promotion needs the full gate; a rule is demoted only when its
  long-window excess *after cost* is negative AND it has spent the minimum time in its
  status AND enough new signal dates arrived since the last change;
* ``suspended``  - back to shadow tracking; it can be re-promoted through the same gate
  after the same minimum time.

A recent short window (about one quarter) is computed for display only and is never an
input to a status decision.
"""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .signal_rules import HORIZONS, ROUND_TRIP_COST_PCT, RULES, Panel

SPLIT_DATE = pd.Timestamp("2023-01-01")  # fixed: the untouched window keeps growing with live data
MIN_NAMES_PER_DATE = 3
FAMILY_ALPHA = 0.10
WIN_RATE_EDGE = 0.02
MIN_DATES_FOR_STATS = 10
MIN_STATUS_DAYS = 90
MIN_NEW_DATES_FOR_DEMOTION = 60  # new signal dates (about one quarter) since the last status change
REFERENCE_WINDOW_DATES = 63
MIN_DATES_FOR_YEAR = 20

CANDIDATE, ACTIVE, SUSPENDED = "candidate", "active", "suspended"
STATUSES = (CANDIDATE, ACTIVE, SUSPENDED)
STATUS_LABELS = {CANDIDATE: "觀察中", ACTIVE: "推播中", SUSPENDED: "暫停推播（持續追蹤）"}

DAILY_COLUMNS = [
    "rule_id", "horizon", "signal_date", "n_names", "mean_excess_pct",
    "n_win", "n_win_after_cost", "base_n", "base_win_after_cost",
]


# --- statistics -------------------------------------------------------------------

def newey_west_t(x: np.ndarray, lag: int) -> float:
    n = len(x)
    if n < MIN_DATES_FOR_STATS:
        return float("nan")
    mu = x.mean()
    e = x - mu
    var = (e @ e) / n
    for k in range(1, min(lag, n - 1) + 1):
        weight = 1 - k / (lag + 1)
        var += 2 * weight * (e[k:] @ e[:-k]) / n
    if var <= 0:
        return float("nan")
    return float(mu / math.sqrt(var / n))


def p_from_t(t: float | None) -> float:
    if t is None or (isinstance(t, float) and math.isnan(t)):
        return 1.0
    return math.erfc(abs(t) / math.sqrt(2))  # two-sided normal approximation


def bh_flags(pvals: pd.Series, alpha: float) -> pd.Series:
    order = pvals.sort_values().index
    m = len(pvals)
    thresh = pd.Series({idx: alpha * (i + 1) / m for i, idx in enumerate(order)})
    passed = pvals.loc[order] <= thresh
    if passed.any():
        cutoff = pvals.loc[passed[passed].index[-1]]
        return pvals <= cutoff
    return pd.Series(False, index=pvals.index)


# --- date-level evidence ------------------------------------------------------------

def daily_excess_table(panel: Panel, *, rule_ids: list[str] | None = None) -> pd.DataFrame:
    """One row per rule, horizon and signal date with everything needed to score it later.

    Only dates whose forward return has matured are present, so the table can simply be
    appended to as time passes. Rows exist for every such date (``n_names`` may be 0)
    because the same-day universe counts are the win-rate baseline.
    """

    wanted = rule_ids or [rule.id for rule in RULES]
    parts: list[pd.DataFrame] = []
    cost = ROUND_TRIP_COST_PCT
    for horizon in HORIZONS:
        forward = panel.forward[horizon]
        valid_universe = panel.liquid & forward.notna()
        base = forward.where(panel.liquid).mean(axis=1)  # mean, not median: skewed returns bias a median test
        excess = forward.sub(base, axis=0)
        base_n = valid_universe.sum(axis=1)
        base_win = ((excess > cost) & valid_universe).sum(axis=1)
        keep = base_n > 0
        for rule_id in wanted:
            hit = panel.signals[rule_id] & forward.notna()
            n = hit.sum(axis=1)
            frame = pd.DataFrame({
                "rule_id": rule_id,
                "horizon": horizon,
                "signal_date": excess.index,
                "n_names": n.values,
                "mean_excess_pct": excess.where(hit).mean(axis=1).values,
                "n_win": ((excess > 0) & hit).sum(axis=1).values,
                "n_win_after_cost": ((excess > cost) & hit).sum(axis=1).values,
                "base_n": base_n.values,
                "base_win_after_cost": base_win.values,
            })
            parts.append(frame[keep.values])
    if not parts:
        return pd.DataFrame(columns=DAILY_COLUMNS)
    out = pd.concat(parts, ignore_index=True)
    out["signal_date"] = pd.to_datetime(out["signal_date"])
    return out[DAILY_COLUMNS]


def _window_stats(frame: pd.DataFrame, horizon: int) -> dict[str, Any] | None:
    days = frame[(frame["n_names"] >= MIN_NAMES_PER_DATE) & frame["mean_excess_pct"].notna()]
    x = days["mean_excess_pct"].to_numpy(dtype=float)
    if len(x) < MIN_DATES_FOR_STATS:
        return {"dates": int(len(x))}
    names = int(days["n_names"].sum())
    span = frame[(frame["signal_date"] >= days["signal_date"].min()) & (frame["signal_date"] <= days["signal_date"].max())]
    base_n = float(span["base_n"].sum())
    mean = float(x.mean())
    return {
        "dates": int(len(x)),
        "names": names,
        "mean_excess_pct": mean,
        "mean_excess_after_cost_pct": mean - ROUND_TRIP_COST_PCT,
        "t_nw": newey_west_t(x, horizon),
        "stock_win_rate": float(days["n_win"].sum() / names),
        "date_win_rate": float((x > 0).mean()),
        "stock_win_rate_after_cost": float(days["n_win_after_cost"].sum() / names),
        "base_win_rate_after_cost": float(span["base_win_after_cost"].sum() / base_n) if base_n else float("nan"),
        "avg_names_per_date": float(days["n_names"].mean()),
    }


def summarize(daily: pd.DataFrame, split_date: pd.Timestamp = SPLIT_DATE) -> pd.DataFrame:
    """Per rule, horizon and window (IS / OOS) statistics from the date-level table."""

    rows = []
    for (rule_id, horizon), group in daily.groupby(["rule_id", "horizon"], sort=False):
        group = group.sort_values("signal_date")
        for label, window in (
            ("IS", group[group["signal_date"] < split_date]),
            ("OOS", group[group["signal_date"] >= split_date]),
        ):
            rows.append({"signal": rule_id, "horizon": int(horizon), "window": label, **_window_stats(window, int(horizon))})
    return pd.DataFrame(rows)


def verdicts(summary: pd.DataFrame, *, alpha: float = FAMILY_ALPHA) -> pd.DataFrame:
    """The full gate, applied to the whole pre-registered family at once."""

    oos = summary[summary["window"] == "OOS"].dropna(subset=["t_nw"]).copy()
    ins = summary[summary["window"] == "IS"].set_index(["signal", "horizon"])
    oos["p"] = oos["t_nw"].map(p_from_t)
    oos = oos.set_index(["signal", "horizon"])
    oos["p_one"] = np.where(oos["t_nw"] > 0, oos["p"] / 2, 1.0)  # only positive edges count as discoveries
    # the family is fixed (every rule x horizon); a missing cell counts as a non-discovery
    family = pd.MultiIndex.from_product([[rule.id for rule in RULES], HORIZONS], names=["signal", "horizon"])
    family_p = oos["p_one"].reindex(family.union(oos.index), fill_value=1.0)
    oos["bh_pass"] = bh_flags(family_p, alpha).reindex(oos.index)
    oos["is_mean"] = ins["mean_excess_pct"].reindex(oos.index)
    oos["is_t"] = ins["t_nw"].reindex(oos.index)
    oos["passes"] = (
        oos["bh_pass"] & (oos["mean_excess_after_cost_pct"] > 0) & (oos["is_mean"] > 0)
        & (oos["stock_win_rate_after_cost"] >= oos["base_win_rate_after_cost"] + WIN_RATE_EDGE)
    )
    return oos.reset_index()


def year_breakdown(daily: pd.DataFrame, rule_id: str, horizon: int) -> dict[str, int]:
    """In how many calendar years the rule's mean excess was positive (stability, informational)."""

    group = daily[(daily["rule_id"] == rule_id) & (daily["horizon"] == horizon)]
    group = group[(group["n_names"] >= MIN_NAMES_PER_DATE) & group["mean_excess_pct"].notna()]
    if group.empty:
        return {"years_total": 0, "years_positive": 0}
    by_year = group.groupby(group["signal_date"].dt.year)["mean_excess_pct"].agg(["mean", "count"])
    by_year = by_year[by_year["count"] >= MIN_DATES_FOR_YEAR]
    return {"years_total": int(len(by_year)), "years_positive": int((by_year["mean"] > 0).sum())}


def recent_reference(daily: pd.DataFrame, rule_id: str, horizon: int, dates: int = REFERENCE_WINDOW_DATES) -> dict[str, Any]:
    """Short-window result. Display only: never an input to a status decision."""

    group = daily[(daily["rule_id"] == rule_id) & (daily["horizon"] == horizon)]
    group = group[(group["n_names"] >= MIN_NAMES_PER_DATE) & group["mean_excess_pct"].notna()].sort_values("signal_date")
    tail = group.tail(dates)
    if tail.empty:
        return {"dates": 0}
    mean = float(tail["mean_excess_pct"].mean())
    return {
        "dates": int(len(tail)),
        "from": str(tail["signal_date"].min().date()),
        "to": str(tail["signal_date"].max().date()),
        "mean_excess_after_cost_pct": mean - ROUND_TRIP_COST_PCT,
    }


def long_term_evidence(
    daily: pd.DataFrame, verdict_table: pd.DataFrame, rule_id: str, horizon: int | None = None
) -> dict[str, Any]:
    """Snapshot of the long-window evidence for one rule (its best horizon unless given)."""

    rows = verdict_table[verdict_table["signal"] == rule_id]
    if rows.empty:
        return {"rule_id": rule_id, "insufficient_data": True}
    if horizon is None:
        passing = rows[rows["passes"]]
        pick = (passing if not passing.empty else rows).sort_values("p_one").iloc[0]
    else:
        match = rows[rows["horizon"] == horizon]
        if match.empty:
            return {"rule_id": rule_id, "horizon": horizon, "insufficient_data": True}
        pick = match.iloc[0]
    h = int(pick["horizon"])
    evidence = {
        "rule_id": rule_id,
        "horizon": h,
        "oos_dates": int(pick["dates"]),
        "mean_excess_pct": round(float(pick["mean_excess_pct"]), 4),
        "mean_excess_after_cost_pct": round(float(pick["mean_excess_after_cost_pct"]), 4),
        "t_nw": round(float(pick["t_nw"]), 3),
        "win_rate_pct": round(float(pick["stock_win_rate_after_cost"]) * 100, 2),
        "base_win_rate_pct": round(float(pick["base_win_rate_after_cost"]) * 100, 2),
        "is_mean_excess_pct": None if pd.isna(pick["is_mean"]) else round(float(pick["is_mean"]), 4),
        "bh_pass": bool(pick["bh_pass"]),
        "gate_pass": bool(pick["passes"]),
        "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
        **year_breakdown(daily, rule_id, h),
        "recent_reference": recent_reference(daily, rule_id, h),
    }
    return evidence


# --- lifecycle ------------------------------------------------------------------------

@dataclass(frozen=True)
class Decision:
    status: str
    changed: bool
    reason: str


def seed_status(evidence: dict[str, Any]) -> Decision:
    """Initial status when a rule is first registered: the gate alone decides."""

    if evidence.get("gate_pass"):
        return Decision(ACTIVE, True, "初次登錄：通過完整長期驗證門檻")
    return Decision(CANDIDATE, True, "初次登錄：未通過完整長期門檻，只做觀察追蹤")


def decide(
    status: str,
    evidence: dict[str, Any],
    *,
    days_in_status: int,
    new_dates_since_change: int,
) -> Decision:
    """Hysteresis: promote only on the full gate, demote only on sustained negative evidence."""

    if evidence.get("insufficient_data"):
        return Decision(status, False, "資料不足，維持原狀態")
    seasoned = days_in_status >= MIN_STATUS_DAYS
    if status in (CANDIDATE, SUSPENDED):
        if evidence.get("gate_pass") and seasoned:
            return Decision(ACTIVE, True, "長期證據通過完整門檻，且已觀察滿最短期間")
        return Decision(status, False, "尚未滿足升級條件")
    if status == ACTIVE:
        negative = evidence.get("mean_excess_after_cost_pct", 0.0) < 0
        enough_new = new_dates_since_change >= MIN_NEW_DATES_FOR_DEMOTION
        if negative and seasoned and enough_new:
            return Decision(SUSPENDED, True, "長期扣成本後超額報酬轉為負，且已滿最短期間與足夠新樣本")
        return Decision(ACTIVE, False, "維持推播（短期波動不構成降級理由）")
    raise ValueError(f"unknown status {status!r}")


def days_between(start: dt.date | dt.datetime | str | None, today: dt.date) -> int:
    if start is None:
        return 0
    if isinstance(start, str):
        start = dt.date.fromisoformat(start[:10])
    if isinstance(start, dt.datetime):
        start = start.date()
    return (today - start).days
