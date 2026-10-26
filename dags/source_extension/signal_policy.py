"""Pure qualification rules; scores count input groups, never probabilities."""

from __future__ import annotations

import datetime as dt
import math
import re
from collections import Counter, defaultdict

POLICY_VERSION = "group_calendar_risk_order_flow_v4"
FAMILY_GROUPS = {
    "price": "market_tape", "volume": "market_tape",
    "institutions": "equity_order_flow", "broker_branches": "equity_order_flow",
    "profitability": "operating_margin",
    "insiders": "insider_holdings",
}


def merge_stored_quote_history(current, stored_rows, *, master, cutoff_utc, known_quote_keys=()):
    """Use stored history where the current feed has no security/date record.

    Stock-table volume is in lots; scanner volume is in shares. Collection-time
    availability is retained separately from immutable source-feed versions.
    """
    result = {code: list(rows) for code, rows in current.items()}
    boundaries = {}
    known = set(known_quote_keys) | {(code, row[0]) for code, rows in current.items() for row in rows}
    for code, rows in current.items():
        market = master.get(code, {}).get("market")
        if rows and market in ("上市", "上櫃"):
            day = max(row[0] for row in rows)
            boundaries[market] = max(day, boundaries.get(market, day))
    grouped = defaultdict(list)
    for row in stored_rows:
        code = str(row.get("code") or "").strip()
        market = master.get(code, {}).get("market")
        day, captured = row.get("date"), row.get("captured_at")
        if (not isinstance(day, dt.date) or isinstance(day, dt.datetime)
                or market not in boundaries or day >= boundaries[market]
                or (code, day) in known
                or not isinstance(captured, dt.datetime) or captured > cutoff_utc):
            continue
        grouped[(code, day)].append(row)
    provenance = Counter()
    for (code, day), rows in grouped.items():
        if len(rows) != 1:
            continue
        try:
            close, volume = float(rows[0]["close"]), float(rows[0]["volume_lots"]) * 1000
        except (KeyError, TypeError, ValueError):
            continue
        if not all(math.isfinite(value) and value > 0 for value in (close, volume)):
            continue
        result.setdefault(code, []).append((day, close, volume, False))
        provenance[code] += 1
    return {code: sorted(rows, key=lambda row: row[0]) for code, rows in result.items()}, dict(provenance)


def trading_sessions(as_of_date, closed_dates, lookback_days=60):
    days = [as_of_date - dt.timedelta(days=i) for i in range(lookback_days)]
    # An incomplete holiday table cannot certify consecutive sessions.
    if not all(day in closed_dates for day in days if day.weekday() >= 5):
        return []
    return sorted(day for day in days if day.weekday() < 5 and day not in closed_dates)


def is_observed_market_session(effective_date, market, session_dates_by_market):
    """Require a source snapshot date to match an observed official quote session."""
    return effective_date in session_dates_by_market.get(market, set())


def quote_window(rows, sessions, length):
    dates = list(sessions)[-length:]
    if len(dates) != length:
        return []
    counts = Counter(row[0] for row in rows)
    panel = {row[0]: row for row in rows}
    if any(counts[day] != 1 for day in dates):
        return []
    selected = [panel[day] for day in dates]
    if any(row[1] is None or row[1] <= 0 or row[3] for row in selected):
        return []
    return selected


def evidence_balance(families):
    groups = {}
    for family, direction in families.items():
        if direction in (-1, 1):
            groups.setdefault(FAMILY_GROUPS.get(family, family), set()).add(direction)
    votes = {group: next(iter(values)) if len(values) == 1 else 0
             for group, values in groups.items()}
    bullish = sum(value == 1 for value in votes.values())
    bearish = sum(value == -1 for value in votes.values())
    total = bullish - bearish
    return {
        "policy_version": POLICY_VERSION, "group_directions": votes,
        "bullish_groups": bullish, "bearish_groups": bearish,
        "aligned_groups": max(bullish, bearish),
        "direction": "偏多" if total > 0 else "偏空" if total < 0 else "訊號分歧",
    }


def revenue_period_current(period, today):
    """A refreshed export cannot make an old reporting period current."""
    age = (today.year - period.year) * 12 + today.month - period.month
    return age in (1, 2)


def strip_negated_terms(text, terms):
    for term in sorted(terms, key=len, reverse=True):
        text = re.sub(r"(?:尚未|並未|未曾|未|沒有|尚無|並無|無法|無|取消)\s*"
                      r"(?:發生|收到|出現)?\s*" + re.escape(term), "", text)
    return text


def profitability_vote(margins):
    if not margins:
        return {}
    current_key = max(margins)
    prior_key = (current_key[0] - 1, current_key[1])
    if prior_key not in margins:
        return {}  # QoQ is seasonal context, not a directional substitute.
    current, prior = margins[current_key], margins[prior_key]
    change = current - prior
    direction = (1 if change > 0 else -1) if (
        abs(change) >= 3 or current >= 0 > prior or current < 0 <= prior
    ) else 0
    return {"direction": direction, "change_pp": change, "margin_pct": current,
            "period": f"{current_key[0]}Q{current_key[1]}", "comparison": "same_quarter_yoy"}


def trading_risk_action(contexts):
    flags = set()
    for item in contexts or []:
        if item.get("event_type") != "trading_status_snapshot":
            continue
        details = item.get("details") or {}
        flags.update(key for key in ("suspended", "altered", "periodic", "managed")
                     if details.get(key) is True)
    if "suspended" in flags:
        return "官方狀態為停止交易；暫停操作建議，僅追蹤恢復交易公告。"
    if flags:
        return "官方列為變更交易、分盤或管理股票；暫緩新增部位，先確認收足款券與撮合限制。"
    return None
