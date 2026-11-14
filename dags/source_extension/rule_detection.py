"""Daily detection for every registered rule, built on the shared rule definitions.

Detection uses exactly the panel the backtest and the weekly revalidation use
(``signal_rules.compute_panel``). Each input feed is checked for "has the data really
landed" before a rule may report anything, because a feed that is merely late must
show up as 「資料未到」, never as "no hit".
"""
from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from . import signal_rules

MIN_LANDED_RATIO = 0.9
HISTORY_CALENDAR_DAYS = 220
TDCC_MAX_STALE_DAYS = 12  # weekly data: older than this means the feed stopped


def fetch_inputs(stock_cursor, as_of_date: dt.date, calendar_days: int = HISTORY_CALENDAR_DAYS) -> dict[str, pd.DataFrame]:
    start = as_of_date - dt.timedelta(days=calendar_days)

    def frame(sql: str, columns: list[str], params: tuple) -> pd.DataFrame:
        stock_cursor.execute(sql, params)
        data = pd.DataFrame(stock_cursor.fetchall(), columns=columns)
        for column in columns[2:]:  # MySQL DECIMAL arrives as Decimal objects; the maths needs floats
            data[column] = pd.to_numeric(data[column], errors="coerce")
        return data

    window = (start, as_of_date)
    price = frame(
        "SELECT 資料日期, 證券代號, 收盤, 成交量 FROM 個股成交資訊 WHERE 資料日期 BETWEEN %s AND %s",
        ["date", "code", "close", "vol"], window,
    )
    inst = frame(
        "SELECT 資料日期, 證券代號, 外資買賣超股數, 投信買賣超股數, 三大法人買賣超股數 "
        "FROM 三大法人買賣 WHERE 資料日期 BETWEEN %s AND %s",
        ["date", "code", "foreign", "trust", "total"], window,
    )
    margin = frame(
        "SELECT 資料日期, 證券代號, 融資剩餘額, 券資比 FROM 融資融券 WHERE 資料日期 BETWEEN %s AND %s",
        ["date", "code", "margin_bal", "short_ratio"], window,
    )
    tdcc = frame(
        # this legacy table stores the effective date as YYYYMMDD text: compare text with text
        "SELECT 資料日期, 證券代號, `>400張大股東持有百分比` FROM 大戶比例 WHERE 資料日期 BETWEEN %s AND %s",
        ["date", "code", "big_pct"], (start.strftime("%Y%m%d"), as_of_date.strftime("%Y%m%d")),
    )
    return {"price": price, "inst": inst, "margin": margin, "tdcc": normalize_tdcc(tdcc)}


def normalize_tdcc(tdcc: pd.DataFrame) -> pd.DataFrame:
    """TDCC dates arrive as YYYYMMDD text in some rows; coerce to datetimes and numbers."""

    if tdcc.empty:
        return tdcc
    frame = tdcc.copy()
    text = frame["date"].astype(str).str.replace("-", "", regex=False).str.slice(0, 8)
    frame = frame[text.str.fullmatch(r"\d{8}")]
    frame["date"] = pd.to_datetime(text[frame.index], format="%Y%m%d")
    frame["big_pct"] = pd.to_numeric(frame["big_pct"], errors="coerce")
    return frame


def _daily_counts(frame: pd.DataFrame) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(dtype=int)
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"])
    data["code"] = data["code"].astype(str)
    data = data[data["code"].str.fullmatch(r"[1-9]\d{3}")].drop_duplicates(["date", "code"])
    return data.groupby("date").size()


def feed_landed(counts: pd.Series, session: pd.Timestamp) -> bool:
    """A daily feed has landed when its row count on ``session`` is within 10% of its recent norm."""

    if counts.empty or session not in counts.index:
        return False
    earlier = counts[counts.index < session].tail(5)
    if earlier.empty:
        return bool(counts[session] > 0)
    return bool(counts[session] >= MIN_LANDED_RATIO * earlier.median())


def source_landing(inputs: dict[str, pd.DataFrame], sessions: pd.Index, as_of: pd.Timestamp) -> dict[str, bool]:
    """Per feed: is the data this session's rules depend on actually in the database?"""

    price_counts = _daily_counts(inputs["price"])
    landing = {"price": feed_landed(price_counts, as_of)}
    landing["inst"] = feed_landed(_daily_counts(inputs.get("inst")), as_of)
    position = sessions.get_loc(as_of) if as_of in sessions else None
    margin_lag = signal_rules.LAG_BY_SOURCE["margin"]
    if position is None or position - margin_lag < 0:
        landing["margin"] = False
    else:
        landing["margin"] = feed_landed(_daily_counts(inputs.get("margin")), sessions[position - margin_lag])
    tdcc = inputs.get("tdcc")
    if tdcc is None or tdcc.empty:
        landing["tdcc"] = False
    else:
        tdcc_dates = pd.to_datetime(tdcc["date"]).drop_duplicates()
        newest = tdcc_dates.max()
        fresh = (as_of - newest).days <= TDCC_MAX_STALE_DAYS
        landing["tdcc"] = bool(fresh and tdcc_week_present(tdcc_dates, sessions, position))
    return landing


def tdcc_week_present(tdcc_dates: pd.Series, sessions: pd.Index, position: int | None) -> bool:
    """The weekly snapshot whose publication lag ends on this session must be in the database.

    A week that was published (a Friday data date maps to the session ``tdcc_lag`` sessions
    before ``as_of``) but never landed would otherwise look like "no big-holder change".
    Holiday Fridays are tolerated by accepting a data date within three days.
    """

    lag = signal_rules.LAG_BY_SOURCE["tdcc"]
    if position is None or position - lag - 1 < 0:
        return False
    lo, hi = sessions[position - lag - 1], sessions[position - lag]
    for day in pd.date_range(lo + pd.Timedelta(days=1), hi):
        if day.weekday() == 4:
            near = tdcc_dates[(tdcc_dates >= day - pd.Timedelta(days=3)) & (tdcc_dates <= day + pd.Timedelta(days=3))]
            if near.empty:
                return False
    return True


def detect_rules(
    inputs: dict[str, pd.DataFrame], as_of_date: dt.date, rule_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Hits per rule for the as-of session, with per-rule data availability.

    ``status`` is ``data_not_ready`` when prices for the as-of session are missing or
    partial. A rule whose own feeds have not landed reports ``data_not_ready`` while the
    other rules still report normally.
    """

    empty = {"status": "data_not_ready", "rules": {}, "breadth": {}, "landing": {}}
    price = inputs.get("price")
    if price is None or price.empty:
        return empty
    as_of = pd.Timestamp(as_of_date)
    cleaned = signal_rules.clean_price(price)
    sessions = pd.Index(sorted(cleaned["date"].unique()))
    if len(sessions) == 0 or sessions[-1] != as_of:
        return empty
    landing = source_landing(inputs, sessions, as_of)
    if not landing["price"]:
        return {**empty, "landing": landing}
    panel = signal_rules.compute_panel(
        price, inputs.get("inst"), inputs.get("margin"), inputs.get("tdcc"), with_forward=False
    )
    wanted = rule_ids or [rule.id for rule in signal_rules.RULES]
    rules: dict[str, dict[str, Any]] = {}
    for rule_id in wanted:
        rule = signal_rules.RULES_BY_ID[rule_id]
        ready = all(landing.get(source, True) for source in rule.sources)
        rules[rule_id] = {
            "status": "ok" if ready else "data_not_ready",
            "missing_feeds": [source for source in rule.sources if not landing.get(source, True)],
            "hits": signal_rules.rule_hits(panel, rule_id, as_of) if ready else [],
        }
    previous = panel.close.shift(1).loc[as_of]
    day_return = (panel.close.loc[as_of] / previous - 1) * 100
    tradable = panel.liquid.loc[as_of] & day_return.notna()
    returns = day_return[tradable]
    breadth = {
        "stocks": int(len(returns)),
        "up": int((returns > 0).sum()),
        "down": int((returns < 0).sum()),
        "median_return_pct": float(returns.median()) if len(returns) else None,
    }
    return {"status": "ok", "rules": rules, "breadth": breadth, "landing": landing}
