"""Plain-language daily digest built from the rules that currently have long-term evidence.

Only rules whose status is ``active`` may push names to Discord. A rule's status is not
a one-off verdict: it comes from the weekly long-horizon revalidation
(``rule_revalidation`` / ``rule_lifecycle``), with ``validated_signals.json`` supplying
seed statuses and configuration. The wording therefore says "目前長期證據（截至 …）", never
"通過驗證", and short-term live results are labelled reference-only.

Rule definitions live in ``signal_rules`` and are shared with the research backtest and
the weekly revalidation, so a rule cannot mean one thing in testing and another here.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .rule_lifecycle import ACTIVE, STATUS_LABELS

REGISTRY_PATH = Path(__file__).with_name("validated_signals.json")
LOGGER = logging.getLogger(__name__)
# Discord rejects a request body above ~10,000 bytes (HTTP 413). The wire format escapes every
# CJK character (about 6 bytes), so the budget is measured the way the sender measures it.
DIGEST_BYTE_BUDGET = 9_000
EMBED_MAX_CHARS = 1_400  # one embed must always fit a request on its own; long ones are split


def load_registry(path: Path | str = REGISTRY_PATH) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        registry = json.load(handle)
    if not isinstance(registry, dict) or not isinstance(registry.get("rules"), list):
        raise ValueError("validated_signals.json must contain a 'rules' list")
    return registry


def active_rules(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [rule for rule in states if rule.get("status") == ACTIVE]


def fetch_price_history(stock_cursor, as_of_date: dt.date, calendar_days: int = 130) -> pd.DataFrame:
    start = as_of_date - dt.timedelta(days=calendar_days)
    stock_cursor.execute(
        "SELECT 資料日期, 證券代號, 收盤, 成交量 FROM 個股成交資訊 "
        "WHERE 資料日期 BETWEEN %s AND %s",
        (start, as_of_date),
    )
    frame = pd.DataFrame(stock_cursor.fetchall(), columns=["date", "code", "close", "vol"])
    for column in ("close", "vol"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def fetch_names(stock_cursor, as_of_date: dt.date) -> dict[str, str]:
    stock_cursor.execute(
        "SELECT 證券代號, 證券名稱 FROM 個股基本指標 WHERE 資料日期 = "
        "(SELECT MAX(資料日期) FROM 個股基本指標 WHERE 資料日期 <= %s)",
        (as_of_date,),
    )
    return {str(code): str(name or "").strip() for code, name in stock_cursor.fetchall()}


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{value:+.1f}%"


def request_bytes(message: str, embeds: list[dict[str, Any]]) -> int:
    return len(json.dumps({"content": message, "embeds": embeds}).encode("utf-8"))


def split_long_embeds(embeds: list[dict[str, Any]], max_chars: int = EMBED_MAX_CHARS) -> list[dict[str, Any]]:
    """Split embeds whose description is long on line boundaries, so none can exceed a request alone."""

    out: list[dict[str, Any]] = []
    for embed in embeds:
        text = embed.get("description") or ""
        if len(text) <= max_chars:
            out.append(embed)
            continue
        chunks, current = [], ""
        for line in text.split("\n"):
            while len(line) > max_chars:  # a single pathological line
                if current:
                    chunks.append(current)
                    current = ""
                chunks.append(line[:max_chars])
                line = line[max_chars:]
            if current and len(current) + 1 + len(line) > max_chars:
                chunks.append(current)
                current = line
            else:
                current = f"{current}\n{line}" if current else line
        if current:
            chunks.append(current)
        for index, chunk in enumerate(chunks):
            title = embed["title"] if index == 0 else f"{embed['title']}（續 {index}）"
            out.append({**embed, "title": title, "description": chunk})
    return out


def _format_metric(rule: dict[str, Any], value: float | None) -> str:
    if value is None:
        return ""
    return f"｜{rule['metric_label']} {rule['metric_format'].format(value)}"


def long_term_summary(rule: dict[str, Any]) -> str:
    """One sentence of a rule's long-horizon evidence, in plain language."""

    evidence = rule.get("evidence") or {}
    if not evidence.get("oos_dates"):
        return "長期證據還在累積中。"
    text = (
        f"持有 {evidence.get('horizon', rule['horizon_sessions'])} 個交易日，全部歷史（2023 年後為沒調整過的樣本外，"
        f"{evidence['oos_dates']} 個訊號日）平均比同期全市場多 {evidence['mean_excess_pct']:+.2f}%，"
        f"扣掉交易成本後約 {evidence['mean_excess_after_cost_pct']:+.2f}%；"
        f"賺過成本的比例 {evidence['win_rate_pct']:.0f}%（隨機挑一檔 {evidence['base_win_rate_pct']:.0f}%）"
    )
    if evidence.get("years_total"):
        text += f"；{evidence['years_total']} 個年度中有 {evidence['years_positive']} 年為正"
    text += "。"
    recent = evidence.get("recent_reference") or {}
    if recent.get("dates"):
        text += (
            f"最近一季（{recent['dates']} 個訊號日，只是參考、不影響規則去留）扣成本 "
            f"{recent['mean_excess_after_cost_pct']:+.2f}%。"
        )
    return text


def build_validated_digest(
    as_of_date: dt.date,
    registry: dict[str, Any],
    detection: dict[str, Any],
    states: list[dict[str, Any]],
    names: dict[str, str] | None = None,
    tracking_embeds: list[dict[str, Any]] | None = None,
    transitions: list[dict[str, Any]] | None = None,
    evidence_as_of: dt.date | str | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """After-close digest: market snapshot + hits of the rules that are currently active."""

    names = names or {}
    transitions = transitions or []
    label_of = {rule["id"]: rule["label"] for rule in states}
    active = active_rules(states)
    date_text = as_of_date.strftime("%Y/%m/%d")
    as_of_text = str(evidence_as_of or registry.get("validated_on") or "")[:10]
    status = detection.get("status")
    rule_results = detection.get("rules") or {}

    def assemble(names_cap: int | None, compact: bool) -> tuple[str, list[dict[str, Any]]]:
        hit_rules = [r for r in active if rule_results.get(r["id"], {}).get("hits")]
        total_hits = sum(len(rule_results[r["id"]]["hits"]) for r in hit_rules)
        if status != "ok":
            message = f"📊 台股收盤摘要｜{date_text}｜價格資料未到齊"
        else:
            message = f"📊 台股收盤摘要｜{date_text}｜推播中規則的訊號 {total_hits} 檔"
        embeds: list[dict[str, Any]] = []
        breadth = detection.get("breadth") or {}
        if status == "ok" and breadth.get("stocks"):
            embeds.append({
                "title": "今天市場",
                "description": (
                    f"流動性足夠的上市櫃股票 {breadth['stocks']} 檔：上漲 {breadth['up']} 檔、"
                    f"下跌 {breadth['down']} 檔，中位數漲跌 {_pct(breadth.get('median_return_pct'))}。"
                ),
                "color": 0x34495E,
            })
        if status != "ok":
            embeds.append({
                "title": "價格資料尚未更新",
                "description": (
                    "今天的收盤價資料還沒進資料庫，**這不代表沒有訊號**。"
                    "今天不判斷訊號，請以下一個交易日的摘要為準。"
                ),
                "color": 0xE67E22,
            })
        elif not active:
            embeds.append({
                "title": "目前沒有推播中的規則",
                "description": "所有規則的長期證據都不足以推播，今天只提供市場概況；規則仍在持續追蹤驗證。",
                "color": 0x7F8C8D,
            })
        else:
            waiting = [r for r in active if rule_results.get(r["id"], {}).get("status") == "data_not_ready"]
            for rule in hit_rules:
                hits = rule_results[rule["id"]]["hits"]
                limit = int(rule["params"]["max_names_shown"])
                if names_cap is not None:
                    limit = min(limit, names_cap)
                lines = []
                for row in hits[:limit]:
                    lines.append(
                        f"**{row['code']} {names.get(row['code'], '')}**｜收 {row['close']:g}"
                        f"｜今日 {_pct(row['day_return_pct'])}{_format_metric(rule, row['metric'])}"
                    )
                if len(hits) > limit:
                    lines.append(f"另有 {len(hits) - limit} 檔符合條件未列出（排序沒有被驗證過）。")
                embeds.append({"title": f"訊號：{rule['label']}", "description": "\n".join(lines), "color": 0x27AE60})
            if not hit_rules and not waiting:
                labels = "、".join(r["label"] for r in active)
                embeds.append({
                    "title": "今日沒有符合推播中規則的股票",
                    "description": f"不是系統壞掉，是今天沒有股票符合「{labels}」。沒訊號就不用做任何事。",
                    "color": 0x7F8C8D,
                })
            if waiting:
                embeds.append({
                    "title": "部分資料尚未更新",
                    "description": (
                        "以下規則所需的資料還沒到齊，**今天不判斷，不代表沒有訊號**：\n"
                        + "\n".join(f"・{r['label']}" for r in waiting)
                    ),
                    "color": 0xE67E22,
                })
            if hit_rules:
                parts = []
                for rule in hit_rules:
                    parts.append(f"**{rule['label']}**" + ("" if compact else f"\n意思：{rule['meaning']}"))
                    parts.append(long_term_summary(rule))
                parts.append(
                    "**白話：優勢都很小，不是明牌。單一檔仍有超過一半的機率沒跑贏市場，請當作「值得看一下的名單」，"
                    "不要當成買進指令。** 漲停鎖死的股票隔天常買不到，回測沒有扣掉這個因素。"
                )
                embeds.append({
                    "title": f"這些訊號有多可靠？目前長期證據（截至 {as_of_text}）",
                    "description": "\n".join(parts),
                    "color": 0x2980B9,
                })
        if any(rule.get("status_source") == "fallback_error" for rule in states):
            embeds.append({
                "title": "規則狀態讀取失敗",
                "description": "資料庫裡的規則狀態暫時讀不到，今天暫用登錄檔的初始狀態；若有規則已被暫停，今天可能仍會列出，請以下次摘要為準。",
                "color": 0xE67E22,
            })
        recent = [t for t in transitions if t["to"] != t["from"]]
        if recent:
            embeds.append({
                "title": "規則狀態變更",
                "description": "\n".join(
                    f"・{label_of.get(t['rule_id'], t['rule_id'])}：{STATUS_LABELS.get(t['from'], t['from'])}"
                    f" → {STATUS_LABELS.get(t['to'], t['to'])}（{t['reason']}）"
                    for t in recent
                ),
                "color": 0x8E44AD,
            })
        embeds.extend(tracking_embeds or [])
        embeds.append({
            "title": "說明",
            "description": (
                f"所有規則每週用全部歷史重新驗證，只有長期證據站得住腳的規則才推播；短期成績只是參考，不會單獨決定規則去留。"
                f"其他規則只在系統內追蹤，不打擾你（規則集 {registry.get('ruleset')}）。以上不構成投資建議。"
            ),
            "color": 0x95A5A6,
        })
        return message, split_long_embeds(embeds)

    attempts: list[tuple[int | None, bool]] = [(None, False), (None, True)]
    attempts += [(cap, True) for cap in (3, 2, 1)]
    message, embeds = assemble(*attempts[0])
    for names_cap, compact in attempts:
        message, embeds = assemble(names_cap, compact)
        if request_bytes(message, embeds) <= DIGEST_BYTE_BUDGET:
            break
    else:
        LOGGER.warning(
            "digest is %s bytes on the wire (> %s); the sender will split it into several messages",
            request_bytes(message, embeds), DIGEST_BYTE_BUDGET,
        )
    return message, embeds
