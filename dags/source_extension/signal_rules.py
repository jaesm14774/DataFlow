"""Single source of truth for every candidate signal rule.

The research backtest, the weekly revalidation job and the daily detection all import
this module, so a rule can never mean one thing in the backtest and another in
production. Changing a threshold means registering a *new* rule version: its forward
evidence starts on its registration date.

Inputs are long-form frames (one row per date and stock):

* price : date, code, close, vol          (vol in lots of 1,000 shares)
* inst  : date, code, foreign, trust, total   (net buy shares)
* margin: date, code, margin_bal, short_ratio
* tdcc  : date, code, big_pct             (holders of >400 lots, weekly)

Publication lags are part of the rule definition. A signal is attributed to the first
session on which its inputs are really available, and entry is the close of the session
after that:

* price / institutional flows: available the evening of the same day (lag 0)
* margin balances: collected the next morning, so usable on the next session (lag 1)
* TDCC weekly holders: usable three sessions after the data date (lag 3)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

RULESET_VERSION = "rules_v1"
HORIZONS = (5, 10, 20)
ROUND_TRIP_COST_PCT = 0.6
RET_CLIP = 0.5
MIN_TURNOVER_NTD = 50_000_000

LAG_BY_SOURCE = {"price": 0, "inst": 0, "margin": 1, "tdcc": 3}


@dataclass(frozen=True)
class Rule:
    id: str
    label: str
    meaning: str
    sources: tuple[str, ...]
    metric_label: str
    metric_format: str
    registered_on: str = "2026-11-14"
    # Bearish comparison groups stay in the tested family (so multiple-testing is honest)
    # and are tracked forever, but they are never promoted to a push.
    is_control: bool = False

    @property
    def publish_lag_sessions(self) -> int:
        return max(LAG_BY_SOURCE[source] for source in self.sources)


RULES: tuple[Rule, ...] = (
    Rule("foreign_buy_5d_top", "外資 5 日買超強度居前 5%",
         "外資近 5 日買超股數占 20 日均量達 5% 以上，且在全市場居前 5%。",
         ("price", "inst"), "外資 5 日買超占 20 日均量", "{:.1f}%"),
    Rule("trust_buy_5d_top", "投信 5 日買超強度居前 5%",
         "投信近 5 日買超股數占 20 日均量達 2% 以上，且在全市場居前 5%。",
         ("price", "inst"), "投信 5 日買超占 20 日均量", "{:.1f}%"),
    Rule("inst_buy_streak_5d", "三大法人連 5 日買超",
         "三大法人連續 5 日買超，且外資 5 日買超強度為正。",
         ("price", "inst"), "外資 5 日買超占 20 日均量", "{:.1f}%"),
    Rule("foreign_trust_both_buy_3d", "外資＋投信同步連 3 日買超",
         "外資與投信同時連 3 日買超，且外資 5 日買超強度為正。",
         ("price", "inst"), "外資 5 日買超占 20 日均量", "{:.1f}%"),
    Rule("breakout_60d_volume", "突破 60 日高＋爆量",
         "收盤創 60 個交易日新高，且成交量達前 20 日均量 2 倍以上，代表有人在積極追價。",
         ("price",), "量是近 20 日均量", "{:.1f} 倍"),
    Rule("volume_surge_3x_up", "爆量 3 倍且 5 日漲逾 3%",
         "成交量達前 20 日均量 3 倍以上，且近 5 日上漲超過 3%。",
         ("price",), "量是近 20 日均量", "{:.1f} 倍"),
    Rule("margin_cleanup_up", "融資減少且股價上漲",
         "融資餘額 5 日減少 5% 以上，且股價近 5 日上漲超過 2%（散戶退場、股價仍漲）。",
         ("price", "margin"), "融資 5 日變化", "{:+.1f}%"),
    Rule("short_squeeze_setup", "券資比偏高且上漲",
         "券資比達 30% 以上，且股價近 5 日上漲超過 3%。",
         ("price", "margin"), "券資比", "{:.1f}%"),
    Rule("tdcc_big_holder_up", "千張大戶持股週增",
         "持股超過 400 張的大股東占比單週增加 1 個百分點以上。",
         ("price", "tdcc"), "大戶持股週變化", "{:+.1f} 個百分點"),
    Rule("oversold_foreign_buy", "大跌後外資買超",
         "近 20 日下跌超過 20%，但外資近 5 日買超占 20 日均量達 3% 以上。",
         ("price", "inst"), "近 20 日漲跌", "{:+.1f}%"),
    Rule("foreign_sell_5d_bottom", "外資 5 日賣超強度居後 5%",
         "外資近 5 日賣超股數占 20 日均量達 5% 以上，且在全市場居後 5%（偏空對照組）。",
         ("price", "inst"), "外資 5 日買賣超占 20 日均量", "{:+.1f}%", is_control=True),
    Rule("tdcc_big_holder_down", "千張大戶持股週減",
         "持股超過 400 張的大股東占比單週減少 1 個百分點以上（偏空對照組）。",
         ("price", "tdcc"), "大戶持股週變化", "{:+.1f} 個百分點", is_control=True),
)
RULES_BY_ID = {rule.id: rule for rule in RULES}


@dataclass
class Panel:
    sessions: pd.Index
    close: pd.DataFrame
    liquid: pd.DataFrame
    signals: dict[str, pd.DataFrame]
    metrics: dict[str, pd.DataFrame]
    forward: dict[int, pd.DataFrame]


def clean_price(price: pd.DataFrame) -> pd.DataFrame:
    frame = price.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["code"] = frame["code"].astype(str)
    frame = frame[frame["code"].str.fullmatch(r"[1-9]\d{3}")]
    return frame.drop_duplicates(["date", "code"]).sort_values(["code", "date"])


def _wide(frame: pd.DataFrame | None, column: str, sessions: pd.Index, columns: pd.Index) -> pd.DataFrame:
    if frame is None or frame.empty or column not in frame:
        return pd.DataFrame(np.nan, index=sessions, columns=columns)
    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"])
    data["code"] = data["code"].astype(str)
    data = data.drop_duplicates(["date", "code"])
    return data.pivot(index="date", columns="code", values=column).reindex(index=sessions, columns=columns)


def compute_panel(
    price: pd.DataFrame,
    inst: pd.DataFrame | None = None,
    margin: pd.DataFrame | None = None,
    tdcc: pd.DataFrame | None = None,
    *,
    margin_lag: int | None = None,
    tdcc_lag: int | None = None,
    with_forward: bool = True,
) -> Panel:
    """Build every rule's boolean signal panel (attributed to its availability date)."""

    margin_lag = LAG_BY_SOURCE["margin"] if margin_lag is None else margin_lag
    tdcc_lag = LAG_BY_SOURCE["tdcc"] if tdcc_lag is None else tdcc_lag
    price = clean_price(price)
    sessions = pd.Index(sorted(price["date"].unique()))
    p = price.set_index(["date", "code"])
    close = p["close"].unstack("code").reindex(sessions)
    vol = p["vol"].unstack("code").reindex(sessions)
    columns = close.columns
    shares = vol * 1000.0
    turnover = close * shares
    liquid = turnover.rolling(20, min_periods=15).median() >= MIN_TURNOVER_NTD
    avg_vol20 = shares.rolling(20, min_periods=15).mean()

    foreign = _wide(inst, "foreign", sessions, columns)
    trust = _wide(inst, "trust", sessions, columns)
    total = _wide(inst, "total", sessions, columns)
    f5 = foreign.rolling(5, min_periods=4).sum() / avg_vol20
    t5 = trust.rolling(5, min_periods=4).sum() / avg_vol20
    ret20 = close / close.shift(20) - 1
    ret5 = close / close.shift(5) - 1
    prior_avg_vol = avg_vol20.shift(1)
    vr = shares / prior_avg_vol.where(prior_avg_vol > 0)  # a zero prior average must not become an infinite ratio
    hi60 = close.rolling(60, min_periods=50).max().shift(1)
    margin_bal = _wide(margin, "margin_bal", sessions, columns)
    short_ratio = _wide(margin, "short_ratio", sessions, columns)
    margin_chg5 = margin_bal / margin_bal.shift(5) - 1

    raw: dict[str, pd.DataFrame] = {
        "foreign_buy_5d_top": (f5 >= 0.05) & (f5.rank(axis=1, pct=True) >= 0.95),
        "trust_buy_5d_top": (t5 >= 0.02) & (t5.rank(axis=1, pct=True) >= 0.95),
        "inst_buy_streak_5d": (total.gt(0).rolling(5).sum() == 5) & (f5 > 0.02),
        "foreign_trust_both_buy_3d": (
            (foreign.gt(0).rolling(3).sum() == 3) & (trust.gt(0).rolling(3).sum() == 3) & (f5 > 0.02)
        ),
        "breakout_60d_volume": (close > hi60) & (vr >= 2.0),
        "volume_surge_3x_up": (vr >= 3.0) & (ret5 > 0.03),
        "oversold_foreign_buy": (ret20 <= -0.20) & (f5 > 0.03),
        "foreign_sell_5d_bottom": (f5 <= -0.05) & (f5.rank(axis=1, pct=True) <= 0.05),
    }
    margin_raw = {
        "margin_cleanup_up": (margin_chg5 <= -0.05) & (ret5 > 0.02),
        "short_squeeze_setup": (short_ratio >= 30) & (ret5 > 0.03),
    }
    signals: dict[str, pd.DataFrame] = {}
    for name, frame in raw.items():
        signals[name] = frame.fillna(False)
    for name, frame in margin_raw.items():
        # margin data for day D is only usable from the next session
        signals[name] = frame.fillna(False).shift(margin_lag, fill_value=False).astype(bool)

    d_big = pd.DataFrame(np.nan, index=sessions, columns=columns)
    if tdcc is not None and not tdcc.empty:
        weekly = tdcc.copy()
        weekly["date"] = pd.to_datetime(weekly["date"])
        weekly["code"] = weekly["code"].astype(str)
        weekly["big_pct"] = pd.to_numeric(weekly["big_pct"], errors="coerce")
        big = weekly.drop_duplicates(["date", "code"]).set_index(["date", "code"])["big_pct"].unstack("code").sort_index()
        change = big - big.shift(1)
        for date, row in change.iterrows():
            pos = sessions.searchsorted(date)
            available = pos + tdcc_lag
            if available < len(sessions):
                d_big.iloc[available] = row.reindex(columns).values
    signals["tdcc_big_holder_up"] = (d_big >= 1.0).fillna(False)
    signals["tdcc_big_holder_down"] = (d_big <= -1.0).fillna(False)

    for name in list(signals):
        signals[name] = signals[name] & liquid

    metrics = {
        "foreign_buy_5d_top": f5 * 100, "trust_buy_5d_top": t5 * 100,
        "inst_buy_streak_5d": f5 * 100, "foreign_trust_both_buy_3d": f5 * 100,
        "breakout_60d_volume": vr, "volume_surge_3x_up": vr,
        # margin metrics are shifted with their signals so the ranking value is what was knowable
        "margin_cleanup_up": (margin_chg5 * 100).shift(margin_lag), "short_squeeze_setup": short_ratio.shift(margin_lag),
        "tdcc_big_holder_up": d_big, "tdcc_big_holder_down": d_big,
        "oversold_foreign_buy": ret20 * 100, "foreign_sell_5d_bottom": f5 * 100,
    }
    forward: dict[int, pd.DataFrame] = {}
    if with_forward:
        entry = close.shift(-1)
        for horizon in HORIZONS:
            forward[horizon] = ((close.shift(-1 - horizon) / entry - 1).clip(-RET_CLIP, RET_CLIP)) * 100.0
    return Panel(sessions, close, liquid, signals, metrics, forward)


def rule_hits(panel: Panel, rule_id: str, session: pd.Timestamp) -> list[dict[str, Any]]:
    """Hits of one rule on one session, strongest metric first."""

    signal = panel.signals[rule_id].loc[session]
    codes = signal[signal].index
    metric = panel.metrics[rule_id].loc[session]
    previous = panel.close.shift(1).loc[session]
    day_return = (panel.close.loc[session] / previous - 1) * 100
    hits = []
    for code in codes:
        value = metric.get(code)
        hits.append({
            "code": code,
            "close": float(panel.close.loc[session, code]),
            "day_return_pct": float(day_return[code]) if pd.notna(day_return[code]) else None,
            "metric": float(value) if pd.notna(value) else None,
        })
    hits.sort(key=lambda row: (-abs(row["metric"] or 0.0), row["code"]))
    return hits
