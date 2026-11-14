"""Live tracking for signal rules and for every push-policy change.

Backtests are only a promise. This module records every name a rule produced (shown or
not), settles each one after the rule's holding period against the same-day
liquid-universe mean (the yardstick used in the research), and builds a weekly scorecard
so each change to the push policy can be judged by evidence:

* 洗版   : Discord messages per week, before vs after the policy started
* 送達   : messages whose delivery failed (for example HTTP 413)
* 誤報   : 「未能產出」 notices
* 訊號品質: settled live excess return, counted per signal *date* (one observation per
            day, Newey-West t) and labelled reference-only. A short live record never
            decides a rule's fate; the weekly long-horizon revalidation does.
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import numpy as np
import pandas as pd

from .rule_lifecycle import (
    MIN_DATES_FOR_STATS, REFERENCE_WINDOW_DATES, STATUS_LABELS, newey_west_t,
)

RETURN_CLIP = 0.5
ROUND_TRIP_COST_PCT = 0.6
NO_PRICE_GRACE_SESSIONS = 3
SHADOW_PICKS_PER_DAY = 30  # candidates can hit hundreds of names a day; track the strongest

# Intraday announcement alerts were switched off because nothing showed they help.
# Their candidates are still tracked here as never-pushed "shadow picks" so the
# scorecard can show whether silencing them cost anything.
SHADOW_INTRADAY_RULE = {
    "id": "intraday_announcement_shadow",
    "horizon_sessions": 10,
    "params": {"min_turnover_ntd": 50_000_000, "max_names_shown": 0},
}


def tracking_rule(state: dict[str, Any]) -> dict[str, Any]:
    """The slice of a rule state the ledger needs: only active rules ever mark names as shown."""

    shown = int(state["params"]["max_names_shown"]) if state["status"] == "active" else 0
    return {
        "id": state["id"],
        "label": state.get("label"),
        "horizon_sessions": int(state["horizon_sessions"]),
        "params": {"min_turnover_ntd": state["params"]["min_turnover_ntd"], "max_names_shown": shown},
    }


def record_picks(
    cursor, *, rule: dict[str, Any], version: str, as_of_date: dt.date,
    hits: list[dict[str, Any]], names: dict[str, str], now: dt.datetime,
) -> int:
    """Persist hits (rank order) idempotently; returns rows newly inserted.

    Active rules record every hit; candidates (``max_names_shown`` 0) record only the
    strongest ``SHADOW_PICKS_PER_DAY`` and never mark anything as shown.
    """

    shown_limit = int(rule["params"]["max_names_shown"])
    if shown_limit == 0:
        hits = hits[:SHADOW_PICKS_PER_DAY]
    inserted = 0
    for rank, hit in enumerate(hits, start=1):
        cursor.execute(
            """
            INSERT IGNORE INTO validated_signal_picks
                (rule_id, rule_version, as_of_date, entity_id, security_name,
                 signal_close, metric_value, rank_in_digest, shown,
                 horizon_sessions, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
            """,
            (
                rule["id"], version, as_of_date, hit["code"],
                (names.get(hit["code"]) or None), hit["close"], hit.get("metric"),
                rank, int(rank <= shown_limit), int(rule["horizon_sessions"]), now,
            ),
        )
        inserted += int(getattr(cursor, "rowcount", 0) or 0)
    return inserted


def _as_date(value) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def record_shadow_candidates(
    cursor, *, rule: dict[str, Any], version: str, candidates: list[dict[str, Any]],
    now: dt.datetime,
) -> int:
    """Store suppressed intraday candidates (never pushed) for later settlement.

    Direction is not scored (many announcements are direction-neutral), so the
    result answers "did announced stocks beat the market at all", nothing more.
    """

    inserted = 0
    for rank, row in enumerate(candidates, start=1):
        cursor.execute(
            """
            INSERT IGNORE INTO validated_signal_picks
                (rule_id, rule_version, as_of_date, entity_id, security_name,
                 signal_close, metric_value, signal_keys, rank_in_digest, shown,
                 horizon_sessions, status, created_at)
            VALUES (%s, %s, %s, %s, %s, NULL, NULL, %s, %s, 0, %s, 'pending', %s)
            """,
            (
                rule["id"], version, _as_date(row["as_of_date"]), str(row["entity_id"]),
                (row.get("security_name") or None), str(row.get("signal_keys") or "")[:255],
                rank, int(rule["horizon_sessions"]), now,
            ),
        )
        inserted += int(getattr(cursor, "rowcount", 0) or 0)
    return inserted


def compute_pick_outcomes(
    prices: pd.DataFrame, picks: list[dict[str, Any]], *, horizon: int, min_turnover_ntd: float,
) -> list[dict[str, Any]]:
    """Settle picks whose exit session exists.

    Entry is the close of the session after ``as_of_date`` (the last session on or
    before it when the date is a weekend/holiday, as for announcements) and exit is
    ``horizon`` sessions later, exactly as in the backtest. The benchmark is the mean return of
    the 4-digit names that were liquid on the signal date over the same window.
    """

    if prices.empty or not picks:
        return []
    frame = prices.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["code"] = frame["code"].astype(str)
    frame = frame[frame["code"].str.fullmatch(r"[1-9]\d{3}")].drop_duplicates(["date", "code"])
    sessions = pd.Index(sorted(frame["date"].unique()))
    close = frame.pivot(index="date", columns="code", values="close").reindex(sessions)
    vol = frame.pivot(index="date", columns="code", values="vol").reindex(sessions)
    liquid = (close * vol * 1000.0).rolling(20, min_periods=15).median() >= min_turnover_ntd
    outcomes: list[dict[str, Any]] = []
    for pick in picks:
        signal_ts = pd.Timestamp(pick["as_of_date"])
        pos = int(sessions.searchsorted(signal_ts, side="right")) - 1
        if pos < 0:
            continue
        entry_pos, exit_pos = pos + 1, pos + 1 + horizon
        if exit_pos >= len(sessions):
            continue
        code = str(pick["entity_id"])
        entry = close.iloc[entry_pos].get(code)
        exit_ = close.iloc[exit_pos].get(code)
        base = {
            "id": pick["id"],
            "entry_date": sessions[entry_pos].date(),
            "exit_date": sessions[exit_pos].date(),
        }
        if pd.isna(entry) or pd.isna(exit_) or entry <= 0:
            if exit_pos + NO_PRICE_GRACE_SESSIONS < len(sessions):
                outcomes.append({**base, "status": "no_price"})
            continue
        universe = (close.iloc[exit_pos] / close.iloc[entry_pos] - 1).clip(-RETURN_CLIP, RETURN_CLIP)
        universe = universe[liquid.iloc[pos] & universe.notna()]
        if universe.empty:
            continue
        stock_return = float(min(max(exit_ / entry - 1, -RETURN_CLIP), RETURN_CLIP)) * 100.0
        universe_mean = float(universe.mean()) * 100.0
        outcomes.append({
            **base, "status": "settled",
            "entry_close": float(entry), "exit_close": float(exit_),
            "stock_return_pct": stock_return,
            "universe_mean_return_pct": universe_mean,
            "excess_return_pct": stock_return - universe_mean,
        })
    return outcomes


def settle_pending_picks(
    ledger_cursor, stock_cursor, *, rule: dict[str, Any], today: dt.date, now: dt.datetime,
    fetch_prices,
) -> dict[str, int]:
    """Settle every pending pick of ``rule`` whose holding period has elapsed."""

    ledger_cursor.execute(
        "SELECT id, as_of_date, entity_id, horizon_sessions FROM validated_signal_picks "
        "WHERE rule_id=%s AND status='pending' ORDER BY as_of_date",
        (rule["id"],),
    )
    picks = [
        {"id": r[0], "as_of_date": r[1], "entity_id": r[2],
         "horizon": int(r[3]) if r[3] else int(rule["horizon_sessions"])}
        for r in ledger_cursor.fetchall()
    ]
    if not picks:
        return {"pending": 0, "settled": 0, "no_price": 0}
    earliest = min(pick["as_of_date"] for pick in picks)
    start = earliest - dt.timedelta(days=45)
    prices = fetch_prices(stock_cursor, today, (today - start).days)
    outcomes = []
    for horizon in sorted({pick["horizon"] for pick in picks}):
        # each pick settles with the holding period it was recorded under, even if the rule's
        # preferred horizon changed afterwards
        outcomes += compute_pick_outcomes(
            prices, [pick for pick in picks if pick["horizon"] == horizon], horizon=horizon,
            min_turnover_ntd=float(rule["params"]["min_turnover_ntd"]),
        )
    counts = {"settled": 0, "no_price": 0}
    for outcome in outcomes:
        if outcome["status"] == "settled":
            ledger_cursor.execute(
                """
                UPDATE validated_signal_picks
                SET status='settled', entry_date=%s, entry_close=%s, exit_date=%s,
                    exit_close=%s, stock_return_pct=%s, universe_mean_return_pct=%s,
                    excess_return_pct=%s, settled_at=%s
                WHERE id=%s AND status='pending'
                """,
                (outcome["entry_date"], outcome["entry_close"], outcome["exit_date"],
                 outcome["exit_close"], outcome["stock_return_pct"],
                 outcome["universe_mean_return_pct"], outcome["excess_return_pct"],
                 now, outcome["id"]),
            )
            counts["settled"] += 1
        else:
            ledger_cursor.execute(
                "UPDATE validated_signal_picks SET status='no_price', entry_date=%s, "
                "exit_date=%s, settled_at=%s WHERE id=%s AND status='pending'",
                (outcome["entry_date"], outcome["exit_date"], now, outcome["id"]),
            )
            counts["no_price"] += 1
    counts["pending"] = len(picks) - counts["settled"] - counts["no_price"]
    return counts


def live_summary(ledger_cursor, rule_id: str, horizon: int | None = None) -> dict[str, Any]:
    """Live record of one rule, one observation per signal date (mean excess of that day's picks)."""

    ledger_cursor.execute(
        "SELECT as_of_date, AVG(excess_return_pct), COUNT(*), MAX(horizon_sessions) "
        "FROM validated_signal_picks WHERE rule_id=%s AND status='settled' GROUP BY as_of_date "
        "ORDER BY as_of_date",
        (rule_id,),
    )
    days = [(_as_date(r[0]), float(r[1]), int(r[2]), int(r[3])) for r in ledger_cursor.fetchall()]
    ledger_cursor.execute(
        "SELECT COUNT(*), COUNT(DISTINCT as_of_date) FROM validated_signal_picks "
        "WHERE rule_id=%s AND status='pending'",
        (rule_id,),
    )
    pending, pending_days = ledger_cursor.fetchone()
    if horizon is None:
        horizon = days[-1][3] if days else 10
    return summarize_by_date(
        [(day, mean, n) for day, mean, n, _ in days], horizon, int(pending or 0), int(pending_days or 0)
    )


def summarize_by_date(
    days: list[tuple[dt.date, float, int]], horizon: int, pending: int = 0, pending_days: int = 0,
) -> dict[str, Any]:
    """Per-date summary: dates (not picks) are the unit; overlapping holds are not independent."""

    days = sorted(days)
    means = np.array([mean for _, mean, _ in days], dtype=float)
    picks = int(sum(n for _, _, n in days))
    independent = 0
    last_kept: dt.date | None = None
    min_gap = dt.timedelta(days=int(np.ceil(horizon * 7 / 5)))
    for day, _, _ in days:  # greedy: keep a date only once the previous kept hold has ended
        if last_kept is None or day - last_kept >= min_gap:
            independent += 1
            last_kept = day
    recent = means[-REFERENCE_WINDOW_DATES:] if len(means) else means
    mean = float(means.mean()) if len(means) else None
    return {
        "dates": len(days),
        "independent_dates": independent,
        "settled": picks,
        "pending": pending,
        "pending_days": pending_days,
        "mean_excess_pct": mean,
        "mean_excess_after_cost_pct": None if mean is None else mean - ROUND_TRIP_COST_PCT,
        "t_nw": newey_west_t(means, horizon) if len(means) >= MIN_DATES_FOR_STATS else None,
        "recent_mean_excess_pct": float(recent.mean()) if len(recent) else None,
        "recent_dates": int(len(recent)),
    }


def summarize_excess(excess: list[float], pending: int = 0, pending_days: int = 0) -> dict[str, Any]:
    """Pick-level summary, used only for the never-pushed intraday shadow group."""

    n = len(excess)
    mean = sum(excess) / n if n else None
    return {
        "settled": n,
        "pending": pending,
        "pending_days": pending_days,
        "mean_excess_pct": mean,
        "mean_excess_after_cost_pct": None if mean is None else mean - ROUND_TRIP_COST_PCT,
        "beat_cost_share": (
            sum(1 for value in excess if value > ROUND_TRIP_COST_PCT) / n if n else None
        ),
    }


LIVE_REFERENCE_NOTE = (
    "上線成績只是短期參考，樣本少時好壞都不代表什麼；規則是否繼續推播，"
    "由每週的長期驗證決定（看全部歷史、扣成本，長期為負且滿最短期間才會暫停）。"
)


def tracking_line(summary: dict[str, Any], rule: dict[str, Any]) -> str:
    """Live record of one rule. Short-term, reference only: it never recommends a status change."""

    label = rule.get("label") or rule["id"]
    if not summary["settled"]:
        return (
            f"・{label}：已記錄 {summary['pending']} 檔，持有滿 {rule['horizon_sessions']} 個交易日後結算；"
            "目前還沒有實際成績。"
        )
    text = (
        f"・{label}：上線至今 {summary['dates']} 個訊號日、{summary['settled']} 檔，"
        f"平均比同期全市場 {summary['mean_excess_pct']:+.2f}%（扣成本 {summary['mean_excess_after_cost_pct']:+.2f}%）"
    )
    if summary.get("t_nw") is not None:
        text += f"，t={summary['t_nw']:.1f}"
    return text + "。"


def shadow_line(summary: dict[str, Any]) -> str:
    if not summary["settled"]:
        return (
            f"被關掉的盤中公告快訊：已記錄 {summary['pending']} 檔，尚未結算；"
            "結算後會在這裡顯示它們有沒有跑贏大盤，用來判斷關掉是否正確。"
        )
    return (
        f"被關掉的盤中公告快訊（不分多空）：已結算 {summary['settled']} 檔，平均比同期全市場 "
        f"{summary['mean_excess_pct']:+.2f}%（扣成本 {summary['mean_excess_after_cost_pct']:+.2f}%）。"
        "接近 0 或為負代表關掉沒有損失。"
    )


def weekly_scorecard(
    ledger_cursor, *, today: dt.date, policy_start: dt.date, rules: list[dict[str, Any]],
) -> dict[str, Any]:
    """Messages/failures/false alarms (7 days vs the 14 before the policy) plus each rule's live record."""

    def window(start: dt.date, end: dt.date) -> dict[str, int]:
        ledger_cursor.execute(
            """
            SELECT COUNT(*), COALESCE(SUM(status='sent'), 0), COALESCE(SUM(status='failed'), 0),
                   COALESCE(SUM(message_text LIKE %s), 0)
            FROM market_signal_notifications
            WHERE created_at >= %s AND created_at < %s
            """,
            ("%未能產出%", start, end),
        )
        total, sent, failed, false_alarm = ledger_cursor.fetchone()
        return {"messages": int(total), "sent": int(sent), "failed": int(failed),
                "failure_notices": int(false_alarm)}

    after_start = max(policy_start, today - dt.timedelta(days=7))
    before = window(policy_start - dt.timedelta(days=14), policy_start)
    after = window(after_start, today + dt.timedelta(days=1))
    return {
        "before_per_week": {k: v / 2 for k, v in before.items()},
        "after_week": after,
        "after_days": (today - after_start).days + 1,
        "live": {
            rule["id"]: live_summary(ledger_cursor, rule["id"], int(rule["horizon_sessions"]))
            for rule in rules
        },
        "shadow": live_summary(ledger_cursor, SHADOW_INTRADAY_RULE["id"]),
    }


def long_term_line(rule: dict[str, Any]) -> str:
    """One compact line of a rule's long-horizon evidence and status (the part that decides)."""

    evidence = rule.get("evidence") or {}
    status = STATUS_LABELS.get(rule["status"], rule["status"])
    head = f"・{rule['label']}｜{status}"
    if evidence.get("oos_dates") and evidence.get("mean_excess_after_cost_pct") is not None:
        years = ""
        if evidence.get("years_total"):
            years = f"｜{evidence['years_total']} 年中 {evidence['years_positive']} 年為正"
        return (
            f"{head}｜持有 {evidence.get('horizon', rule['horizon_sessions'])} 日，"
            f"長期扣成本 {evidence['mean_excess_after_cost_pct']:+.2f}%（{evidence['oos_dates']} 個訊號日"
            f"，t={evidence['t_nw']:.1f}）{years}"
        )
    return f"{head}｜長期證據累積中"


def _grouped_status_lines(rules: list[dict[str, Any]]) -> list[str]:
    """Rules that are not pushed, one compact line per status (keeps the digest inside Discord's limit)."""

    lines = []
    for status in ("suspended", "candidate"):
        labels = [rule["label"] for rule in rules if rule["status"] == status]
        if labels:
            lines.append(f"・{STATUS_LABELS[status]}（長期證據尚未達推播標準，持續追蹤）：" + "、".join(labels))
    return lines


def scorecard_embed(card: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any]:
    before, after = card["before_per_week"], card["after_week"]
    lines = [
        f"洗版：每週推播從 {before['messages']:.0f} 則 → 本週 {after['messages']} 則",
        f"送不出去：每週 {before['failed']:.0f} 則 → 本週 {after['failed']} 則",
        f"「未能產出」通知：每週 {before['failure_notices']:.0f} 則 → 本週 {after['failure_notices']} 則",
        "",
        "**各規則長期證據（所有規則持續重新驗證）**",
        *[long_term_line(rule) for rule in rules if rule["status"] == "active"],
        *_grouped_status_lines(rules),
        "",
        "**上線追蹤（短期，僅供參考）**",
        *[tracking_line(card["live"][rule["id"]], rule) for rule in rules if rule["status"] == "active"],
        shadow_line(card["shadow"]),
        LIVE_REFERENCE_NOTE,
    ]
    return {"title": "本週追蹤：這些改動有沒有變好", "description": "\n".join(lines), "color": 0x8E44AD}
