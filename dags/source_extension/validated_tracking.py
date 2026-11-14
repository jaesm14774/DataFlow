"""Live tracking for validated signals and for every push-policy change.

Backtests are only a promise. This module records every name a validated rule
produced (shown or not), settles each one after the rule's holding period against
the same-day liquid-universe mean (the yardstick used in the research), and builds
a weekly scorecard so each change to the push policy can be judged by evidence:

* 洗版   : Discord messages per week, before vs after the policy started
* 送達   : messages whose delivery failed (for example HTTP 413)
* 誤報   : 「未能產出」 notices
* 訊號品質: settled live excess return of the pushed names vs the backtest promise
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

RETURN_CLIP = 0.5
ROUND_TRIP_COST_PCT = 0.6
MIN_SETTLED_FOR_VERDICT = 60
NO_PRICE_GRACE_SESSIONS = 3


def record_picks(
    cursor, *, rule: dict[str, Any], version: str, as_of_date: dt.date,
    hits: list[dict[str, Any]], names: dict[str, str], now: dt.datetime,
) -> int:
    """Persist every hit (rank order) idempotently; returns rows newly inserted."""

    shown_limit = int(rule["params"]["max_names_shown"])
    inserted = 0
    for rank, hit in enumerate(hits, start=1):
        cursor.execute(
            """
            INSERT IGNORE INTO validated_signal_picks
                (rule_id, rule_version, as_of_date, entity_id, security_name,
                 signal_close, volume_ratio, rank_in_digest, shown,
                 horizon_sessions, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', %s)
            """,
            (
                rule["id"], version, as_of_date, hit["code"],
                (names.get(hit["code"]) or None), hit["close"], hit["volume_ratio"],
                rank, int(rank <= shown_limit), int(rule["horizon_sessions"]), now,
            ),
        )
        inserted += int(getattr(cursor, "rowcount", 0) or 0)
    return inserted


def compute_pick_outcomes(
    prices: pd.DataFrame, picks: list[dict[str, Any]], *, horizon: int, min_turnover_ntd: float,
) -> list[dict[str, Any]]:
    """Settle picks whose exit session exists.

    Entry is the close of the session after ``as_of_date`` and exit is ``horizon``
    sessions later, exactly as in the backtest. The benchmark is the mean return of
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
        pos = int(sessions.searchsorted(signal_ts))
        if pos >= len(sessions) or sessions[pos] != signal_ts:
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
        "SELECT id, as_of_date, entity_id FROM validated_signal_picks "
        "WHERE rule_id=%s AND status='pending' ORDER BY as_of_date",
        (rule["id"],),
    )
    picks = [{"id": r[0], "as_of_date": r[1], "entity_id": r[2]} for r in ledger_cursor.fetchall()]
    if not picks:
        return {"pending": 0, "settled": 0, "no_price": 0}
    earliest = min(pick["as_of_date"] for pick in picks)
    start = earliest - dt.timedelta(days=45)
    prices = fetch_prices(stock_cursor, today, (today - start).days)
    outcomes = compute_pick_outcomes(
        prices, picks, horizon=int(rule["horizon_sessions"]),
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


def live_summary(ledger_cursor, rule_id: str) -> dict[str, Any]:
    ledger_cursor.execute(
        "SELECT excess_return_pct, as_of_date FROM validated_signal_picks "
        "WHERE rule_id=%s AND status='settled'",
        (rule_id,),
    )
    rows = ledger_cursor.fetchall()
    ledger_cursor.execute(
        "SELECT COUNT(*), COUNT(DISTINCT as_of_date) FROM validated_signal_picks "
        "WHERE rule_id=%s AND status='pending'",
        (rule_id,),
    )
    pending, pending_days = ledger_cursor.fetchone()
    return summarize_excess([float(r[0]) for r in rows], int(pending or 0), int(pending_days or 0))


def summarize_excess(excess: list[float], pending: int = 0, pending_days: int = 0) -> dict[str, Any]:
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
        "enough_for_verdict": n >= MIN_SETTLED_FOR_VERDICT,
    }


def tracking_line(summary: dict[str, Any], rule: dict[str, Any]) -> str:
    promise = rule["evidence"]["mean_excess_pct"]
    if not summary["settled"]:
        return (
            f"上線追蹤：已記錄 {summary['pending']} 檔，等持有滿 {rule['horizon_sessions']} 個交易日後結算；"
            "目前還沒有實際成績，不能下結論。"
        )
    text = (
        f"上線追蹤：已結算 {summary['settled']} 檔，平均比同期全市場 "
        f"{summary['mean_excess_pct']:+.2f}%（扣成本 {summary['mean_excess_after_cost_pct']:+.2f}%），"
        f"賺過成本的比例 {summary['beat_cost_share'] * 100:.0f}%；回測預期 {promise:+.2f}%。"
    )
    if not summary["enough_for_verdict"]:
        text += f"樣本還少（要 {MIN_SETTLED_FOR_VERDICT} 檔以上才看得出好壞）。"
    elif summary["mean_excess_after_cost_pct"] < 0:
        text += "⚠️ 上線成績扣成本後為負，未達回測預期，建議停用或重新驗證。"
    return text


def weekly_scorecard(
    ledger_cursor, *, today: dt.date, policy_start: dt.date, rule: dict[str, Any],
) -> dict[str, Any]:
    """Messages/failures/false alarms: last 7 days vs the 14 days before the policy."""

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
        "live": live_summary(ledger_cursor, rule["id"]),
    }


def scorecard_embed(card: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
    before, after = card["before_per_week"], card["after_week"]
    lines = [
        f"洗版：每週推播從 {before['messages']:.0f} 則 → 本週 {after['messages']} 則",
        f"送不出去：每週 {before['failed']:.0f} 則 → 本週 {after['failed']} 則",
        f"「未能產出」通知：每週 {before['failure_notices']:.0f} 則 → 本週 {after['failure_notices']} 則",
        tracking_line(card["live"], rule),
    ]
    return {"title": "本週追蹤：這些改動有沒有變好", "description": "\n".join(lines), "color": 0x8E44AD}
