"""Validated-signal gate and plain-language daily digest.

Only rules listed as ``active`` in ``validated_signals.json`` may reach Discord.
The rules and their evidence come from ``docs/signal_validation_research.py``
(point-in-time, out-of-sample, cost-adjusted); see
``docs/signal_validation_report_20261114.md``.

The rule definition here must stay identical to the research script:
* close above the highest close of the previous ``lookback_sessions`` sessions
* volume at least ``volume_ratio_min`` times the mean of the previous
  ``volume_avg_sessions`` sessions
* 20-session median turnover (close * volume * 1000) at least ``min_turnover_ntd``
* 4-digit common stocks only
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd

REGISTRY_PATH = Path(__file__).with_name("validated_signals.json")
BREAKOUT_RULE_ID = "breakout_60d_volume"
MIN_HISTORY_SESSIONS = 50
MIN_LANDED_RATIO = 0.9


def load_registry(path: Path | str = REGISTRY_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        registry = json.load(handle)
    if not isinstance(registry, dict) or not isinstance(registry.get("rules"), list):
        raise ValueError("validated_signals.json must contain a 'rules' list")
    return registry


def active_rules(registry: dict[str, Any]) -> list[dict[str, Any]]:
    return [rule for rule in registry["rules"] if rule.get("status") == "active"]


def fetch_price_history(stock_cursor, as_of_date: dt.date, calendar_days: int = 130) -> pd.DataFrame:
    start = as_of_date - dt.timedelta(days=calendar_days)
    stock_cursor.execute(
        "SELECT 資料日期, 證券代號, 收盤, 成交量 FROM 個股成交資訊 "
        "WHERE 資料日期 BETWEEN %s AND %s",
        (start, as_of_date),
    )
    frame = pd.DataFrame(stock_cursor.fetchall(), columns=["date", "code", "close", "vol"])
    return frame


def fetch_names(stock_cursor, as_of_date: dt.date) -> dict[str, str]:
    stock_cursor.execute(
        "SELECT 證券代號, 證券名稱 FROM 個股基本指標 WHERE 資料日期 = "
        "(SELECT MAX(資料日期) FROM 個股基本指標 WHERE 資料日期 <= %s)",
        (as_of_date,),
    )
    return {str(code): str(name or "").strip() for code, name in stock_cursor.fetchall()}


def detect_breakouts(
    prices: pd.DataFrame, as_of_date: dt.date, params: dict[str, Any],
) -> dict[str, Any]:
    """Return the as-of session's rule hits plus a small market breadth summary.

    ``status`` is ``data_not_ready`` when the as-of session has no prices, so the
    digest never claims "no signal" when the data simply has not landed.
    """

    frame = prices.copy()
    if frame.empty:
        return {"status": "data_not_ready", "hits": [], "breadth": {}}
    frame["date"] = pd.to_datetime(frame["date"])
    frame["code"] = frame["code"].astype(str)
    frame = frame[frame["code"].str.fullmatch(r"[1-9]\d{3}")]
    frame = frame.drop_duplicates(["date", "code"])
    as_of = pd.Timestamp(as_of_date)
    sessions = pd.Index(sorted(frame["date"].unique()))
    if len(sessions) == 0 or sessions[-1] != as_of:
        return {"status": "data_not_ready", "hits": [], "breadth": {}}
    close = frame.pivot(index="date", columns="code", values="close").reindex(sessions)
    vol = frame.pivot(index="date", columns="code", values="vol").reindex(sessions)
    if len(sessions) >= 2:
        # One exchange landing before the other would shrink the universe and turn a
        # real signal day into a false "no signal"; treat a big drop as not ready.
        landed = int(close.iloc[-1].notna().sum())
        previous = int(close.iloc[-2].notna().sum())
        if previous and landed < MIN_LANDED_RATIO * previous:
            return {"status": "data_not_ready", "hits": [], "breadth": {}}
    lookback = int(params["lookback_sessions"])
    avg_n = int(params["volume_avg_sessions"])
    shares = vol * 1000.0
    turnover = close * shares
    liquid = turnover.rolling(20, min_periods=15).median() >= float(params["min_turnover_ntd"])
    prior_high = close.rolling(lookback, min_periods=MIN_HISTORY_SESSIONS).max().shift(1)
    prior_avg_volume = shares.rolling(avg_n, min_periods=15).mean().shift(1)
    ratio = (shares / prior_avg_volume.where(prior_avg_volume > 0)).replace(
        [float("inf"), float("-inf")], float("nan")
    )
    last = sessions[-1]
    hit_mask = (
        (close.loc[last] > prior_high.loc[last])
        & (ratio.loc[last] >= float(params["volume_ratio_min"]))
        & liquid.loc[last]
    ).fillna(False)
    previous_close = close.shift(1).loc[last]
    day_return = (close.loc[last] / previous_close - 1) * 100
    hits = [
        {
            "code": code,
            "close": float(close.loc[last, code]),
            "volume_ratio": float(ratio.loc[last, code]),
            "day_return_pct": (
                float(day_return[code]) if pd.notna(day_return[code]) else None
            ),
        }
        for code in hit_mask[hit_mask].index
    ]
    hits.sort(key=lambda row: (-row["volume_ratio"], row["code"]))
    tradable = liquid.loc[last] & day_return.notna()
    returns = day_return[tradable]
    breadth = {
        "stocks": int(len(returns)),
        "up": int((returns > 0).sum()),
        "down": int((returns < 0).sum()),
        "median_return_pct": float(returns.median()) if len(returns) else None,
    }
    return {"status": "ok", "hits": hits, "breadth": breadth}


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{value:+.1f}%"


def build_validated_digest(
    as_of_date: dt.date,
    registry: dict[str, Any],
    detection: dict[str, Any],
    names: dict[str, str] | None = None,
    tracking_embeds: list[dict[str, Any]] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Plain-language after-close digest: market snapshot + validated hits only."""

    names = names or {}
    rules = {rule["id"]: rule for rule in active_rules(registry)}
    rule = rules.get(BREAKOUT_RULE_ID)
    date_text = as_of_date.strftime("%Y/%m/%d")
    hits = detection.get("hits") or []
    shown_limit = int(rule["params"]["max_names_shown"]) if rule else 0
    shown = hits[:shown_limit] if rule else []
    if detection.get("status") != "ok":
        message = f"📊 台股收盤摘要｜{date_text}｜價格資料未到齊"
    else:
        message = f"📊 台股收盤摘要｜{date_text}｜通過驗證的訊號 {len(hits) if rule else 0} 檔"
    embeds: list[dict[str, Any]] = []

    breadth = detection.get("breadth") or {}
    if detection.get("status") == "ok" and breadth.get("stocks"):
        embeds.append({
            "title": "今天市場",
            "description": (
                f"主要上市櫃股票 {breadth['stocks']} 檔：上漲 {breadth['up']} 檔、"
                f"下跌 {breadth['down']} 檔，中位數漲跌 {_pct(breadth.get('median_return_pct'))}。"
            ),
            "color": 0x34495E,
        })

    if detection.get("status") != "ok":
        embeds.append({
            "title": "價格資料尚未更新",
            "description": (
                "今天的收盤價資料還沒進資料庫，**這不代表沒有訊號**。"
                "今天不判斷訊號，請以下一個交易日的摘要為準。"
            ),
            "color": 0xE67E22,
        })
    elif rule is None:
        embeds.append({
            "title": "目前沒有啟用的驗證規則",
            "description": "所有規則都已停用，今天只提供市場概況。",
            "color": 0x7F8C8D,
        })
    elif not hits:
        embeds.append({
            "title": "今日無通過驗證的訊號",
            "description": (
                "不是系統壞掉，是今天沒有股票符合「突破 60 日高＋爆量」。"
                "沒訊號就不用做任何事。"
            ),
            "color": 0x7F8C8D,
        })
    else:
        evidence = rule["evidence"]
        lines = []
        for row in shown:
            name = names.get(row["code"], "")
            lines.append(
                f"**{row['code']} {name}**｜收 {row['close']:g}｜今日 {_pct(row['day_return_pct'])}"
                f"｜量是近 20 日均量 {row['volume_ratio']:.1f} 倍"
            )
        if len(hits) > len(shown):
            lines.append(
                f"另有 {len(hits) - len(shown)} 檔符合條件未列出（只依爆量倍數排序，這個排序沒有被驗證過）。"
            )
        embeds.append({
            "title": f"通過驗證：{rule['label']}",
            "description": "\n".join(lines),
            "color": 0x27AE60,
        })
        embeds.append({
            "title": "這個訊號有多可靠？",
            "description": (
                f"意思：股價創 60 個交易日新高，而且成交量暴增到平常 2 倍以上，代表有人在積極追價。\n"
                f"歷史檢驗（{evidence['history']}，2023 年後為沒調整過的樣本外，"
                f"共 {evidence['oos_dates']} 個交易日）：買進後持有 {rule['horizon_sessions']} 個交易日，"
                f"平均比同期全市場多 {evidence['mean_excess_pct']:+.2f}%，扣掉交易成本後約 "
                f"{evidence['mean_excess_after_cost_pct']:+.2f}%；"
                f"賺過成本的比例 {evidence['win_rate_pct']:.0f}%（隨機挑一檔是 "
                f"{evidence['base_win_rate_pct']:.0f}%）。\n"
                "**白話：優勢很小，不是明牌。單一檔仍有超過一半的機率沒跑贏市場，"
                "請當作「值得看一下的名單」，不要當成買進指令。**\n"
                "提醒：漲停鎖死的股票隔天常常買不到，回測沒有扣掉這個因素。"
            ),
            "color": 0x2980B9,
        })
    embeds.extend(tracking_embeds or [])
    embeds.append({
        "title": "說明",
        "description": (
            f"只推播通過歷史驗證的訊號（規則版本 {registry.get('version')}，"
            f"驗證日 {registry.get('validated_on')}）；其他訊號只留在系統紀錄，不再打擾你。"
            "以上不構成投資建議。"
        ),
        "color": 0x95A5A6,
    })
    return message, embeds
