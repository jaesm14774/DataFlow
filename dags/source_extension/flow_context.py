"""Supplementary chip-flow context (三大法人 + 券商分點) shown under each digest hit.

This is *annotation only*: it never changes which stocks a rule selects or how they are
ordered, and none of the thresholds below has been validated against forward returns.
The digest labels it that way. It exists because a single "外資 5 日買超" number hides
things a reader needs to see:

* 投信 and 自營商 may be selling while 外資 buys (or the reverse);
* a branch that looks like a big buyer may be offset by the *same firm's* other branches
  (左手買、右手賣), so branches are also netted at the firm level;
* the buy side may be one firm while the sell side is thousands of small lots spread over
  many branches (or the reverse), which reads very differently from a balanced book;
* a large part of the volume may be the same branches buying and selling (對沖／當沖);
* foreign-broker branches can be compared with the 三大法人 外資 figure.

Broker data is incomplete (the TWSE feed is collected the next morning, only for 上市, and
roughly half the stocks on any day), so every broker line states which dates it covers and
nothing is interpreted when no valid day exists.
"""
from __future__ import annotations

import datetime as dt
import logging
from collections import defaultdict
from typing import Any, Iterable

import pandas as pd

LOGGER = logging.getLogger(__name__)

WINDOW_SESSIONS = 5
DIRECTIONAL_PCT = 1.0          # |net| below 1% of the window volume is treated as flat
BROKER_MIN_VOLUME_LOTS = 100   # below this, per-branch integer rounding dominates
BROKER_VOLUME_RATIO = (0.90, 1.10)  # panel buy/sell totals must match the day's volume
TOP_N = 3
CONCENTRATED_SHARE = 0.45      # top-3 firms hold at least this share of one side
DISPERSED_SHARE = 0.30         # ... while the other side's top 3 hold at most this
OFFSET_RATIO = 0.50            # firm net below half of the branch net => offset
WASH_RATIO = 0.50              # firm sold at least half of what it bought
FOREIGN_NAME_HINTS = (
    "美商", "港商", "新加坡商", "英商", "瑞士", "日商", "法商", "德商", "荷商", "澳商",
    "摩根", "高盛", "瑞銀", "花旗", "匯豐", "麥格理", "野村", "美林", "法銀", "巴黎",
    "德意志", "里昂", "巴克萊", "星展", "渣打",
)  # heuristic name match; 大和國泰 is a local joint venture and is deliberately excluded


def _to_date(value: Any) -> dt.date | None:
    """Any date-like value to a ``date``; None for NULL / NaT / unparseable (never NaT itself)."""

    if value is None or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    return None if parsed is pd.NaT else parsed.date()


def _num(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(number) else number


def firm_key(name: Any, branch: Any) -> str:
    """Parent broker of a branch: the text before '-', or a code label when the name is missing."""

    text = str(name or "").strip()
    if not text or text.lower() in {"none", "nan"}:
        return f"分點{str(branch or '').strip()}"
    return text.split("-")[0].strip() or text


def is_foreign_broker(name: Any) -> bool:
    text = str(name or "")
    return any(hint in text for hint in FOREIGN_NAME_HINTS)


# ---------------------------------------------------------------- institutions


def institution_context(
    code: str,
    sessions: list[dt.date],
    inst: dict[tuple[str, dt.date], tuple[float | None, float | None, float | None]],
    volume: dict[tuple[str, dt.date], float],
) -> dict[str, Any] | None:
    """Foreign / trust / dealer net over the window, in lots and as a share of the window volume.

    ``inst`` values are shares, ``volume`` is in lots (張), exactly as the source tables store them.
    """

    days = [day for day in sessions if (code, day) in inst]
    if not days:
        return None
    window_volume = sum(volume.get((code, day), 0.0) for day in days)
    totals = {"foreign": 0.0, "trust": 0.0, "dealer": 0.0}
    for day in days:
        foreign, trust, dealer = inst[(code, day)]
        totals["foreign"] += foreign or 0.0
        totals["trust"] += trust or 0.0
        totals["dealer"] += dealer or 0.0
    result: dict[str, Any] = {"days": len(days), "window": len(sessions), "volume_lots": window_volume}
    for key, shares in totals.items():
        lots = shares / 1000.0
        result[f"{key}_lots"] = lots
        result[f"{key}_pct"] = (lots / window_volume * 100.0) if window_volume > 0 else None
    last = sessions[-1] if sessions else None
    if last in days:
        today = inst[(code, last)]
        result["today_foreign_lots"] = (today[0] or 0.0) / 1000.0
        result["today_trust_lots"] = (today[1] or 0.0) / 1000.0
    result["alignment"] = _alignment(result.get("foreign_pct"), result.get("trust_pct"))
    return result


def _alignment(foreign_pct: float | None, trust_pct: float | None) -> str:
    if foreign_pct is None or trust_pct is None:
        return ""
    foreign = 1 if foreign_pct >= DIRECTIONAL_PCT else -1 if foreign_pct <= -DIRECTIONAL_PCT else 0
    trust = 1 if trust_pct >= DIRECTIONAL_PCT else -1 if trust_pct <= -DIRECTIONAL_PCT else 0
    if foreign > 0 and trust > 0:
        return "外資、投信同買"
    if foreign < 0 and trust < 0:
        return "外資、投信同賣"
    if foreign > 0 and trust < 0:
        return "外資買、投信賣（分歧）"
    if foreign < 0 and trust > 0:
        return "投信買、外資賣（分歧）"
    return ""


# --------------------------------------------------------------------- brokers


def _latest_unique(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """One row per branch code for a (code, day): a re-collection must not be counted twice."""

    chosen: dict[str, dict[str, Any]] = {}
    for row in rows:
        branch = str(row.get("branch") or "").strip()
        if not branch:
            continue
        previous = chosen.get(branch)
        created = row.get("created")
        if previous is None or (created is not None and (previous.get("created") is None or created >= previous["created"])):
            chosen[branch] = row
    return chosen


def broker_context(
    code: str,
    sessions: list[dt.date],
    rows: list[dict[str, Any]],
    volume: dict[tuple[str, dt.date], float],
    foreign_inst_shares: dict[dt.date, float] | None = None,
) -> dict[str, Any]:
    """Firm-level view of the branch panels that are valid within the window.

    Never raises on bad data: a day that fails the volume cross-check is dropped and counted.
    """

    by_day: dict[dt.date, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = _to_date(row.get("day"))
        if day is not None:
            by_day[day].append(row)
    latest_any = max((day for day in by_day if not sessions or day <= sessions[-1]), default=None)

    valid: dict[dt.date, dict[str, dict[str, Any]]] = {}
    rejected = 0
    for day in sessions:
        if day not in by_day:
            continue
        unique = _latest_unique(by_day[day])
        buy = sum(_num(r.get("buy")) or 0.0 for r in unique.values())
        sell = sum(_num(r.get("sell")) or 0.0 for r in unique.values())
        day_volume = volume.get((code, day))
        low, high = BROKER_VOLUME_RATIO
        # Same checks as equity_order_flow: both sides match the day's volume AND each other
        # (every buy lot has a sell lot; per-branch integer rounding allows about one lot per branch).
        if (not day_volume or day_volume < BROKER_MIN_VOLUME_LOTS or buy <= 0 or sell <= 0
                or not (low <= buy / day_volume <= high and low <= sell / day_volume <= high)
                or abs(buy - sell) > max(2.0, float(len(unique)))):
            rejected += 1
            continue
        valid[day] = unique
    if not valid:
        return {"status": "no_valid_day", "days": [], "window": len(sessions),
                "latest_data_day": latest_any, "rejected_days": rejected}

    days = sorted(valid)
    window_volume = sum(volume[(code, day)] for day in days)
    branch_totals: dict[str, dict[str, Any]] = {}
    firm_totals: dict[str, dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
    churn_lots = 0.0
    gross_buy = 0.0
    foreign_buy = foreign_sell = 0.0
    for day in days:
        for branch, row in valid[day].items():
            buy, sell = _num(row.get("buy")) or 0.0, _num(row.get("sell")) or 0.0
            name = str(row.get("name") or "").strip()
            firm = firm_key(name, branch)
            entry = branch_totals.setdefault(branch, {"name": name or branch, "firm": firm, "buy": 0.0, "sell": 0.0})
            entry["buy"] += buy
            entry["sell"] += sell
            firm_totals[firm]["buy"] += buy
            firm_totals[firm]["sell"] += sell
            churn_lots += min(buy, sell)
            gross_buy += buy
            if is_foreign_broker(name):
                foreign_buy += buy
                foreign_sell += sell
    for entry in branch_totals.values():
        entry["net"] = entry["buy"] - entry["sell"]
    for entry in firm_totals.values():
        entry["net"] = entry["buy"] - entry["sell"]

    firm_buyers = sorted(((f, v) for f, v in firm_totals.items() if v["net"] > 0), key=lambda r: (-r[1]["net"], r[0]))
    firm_sellers = sorted(((f, v) for f, v in firm_totals.items() if v["net"] < 0), key=lambda r: (r[1]["net"], r[0]))
    buy_total = sum(v["net"] for _, v in firm_buyers)
    sell_total = -sum(v["net"] for _, v in firm_sellers)
    top_buyers = firm_buyers[:TOP_N]
    top_sellers = firm_sellers[:TOP_N]
    buy_share = sum(v["net"] for _, v in top_buyers) / buy_total if buy_total > 0 else None
    sell_share = -sum(v["net"] for _, v in top_sellers) / sell_total if sell_total > 0 else None
    pattern = ""
    if buy_share is not None and sell_share is not None:
        if buy_share >= CONCENTRATED_SHARE and sell_share <= DISPERSED_SHARE:
            pattern = "買方集中、賣方分散"
        elif sell_share >= CONCENTRATED_SHARE and buy_share <= DISPERSED_SHARE:
            pattern = "賣方集中、買方分散"

    # Foreign houses trade both ways for many clients, so a high gross sell is normal for them
    # and would flag almost every stock; only domestic top buyers are checked.
    wash = [
        {"firm": firm, "buy": v["buy"], "sell": v["sell"]}
        for firm, v in top_buyers
        if v["buy"] > 0 and v["sell"] >= WASH_RATIO * v["buy"] and not is_foreign_broker(firm)
    ]
    branch_buyers = sorted(((b, v) for b, v in branch_totals.items() if v["net"] > 0), key=lambda r: (-r[1]["net"], r[0]))
    offsets = []
    for branch, entry in branch_buyers[:TOP_N]:
        firm_net = firm_totals[entry["firm"]]["net"]
        if firm_net < OFFSET_RATIO * entry["net"] and firm_totals[entry["firm"]]["sell"] > entry["sell"]:
            offsets.append({"branch": entry["name"], "branch_net": entry["net"], "firm": entry["firm"], "firm_net": firm_net})

    foreign_net = foreign_buy - foreign_sell
    inst_foreign_lots = None
    # Compare only when 三大法人 has a figure for every broker day: a missing day is unknown, not zero.
    if foreign_inst_shares is not None and all(day in foreign_inst_shares for day in days):
        inst_foreign_lots = sum(foreign_inst_shares[day] for day in days) / 1000.0
    return {
        "status": "ok", "days": days, "window": len(sessions), "latest_data_day": latest_any,
        "rejected_days": rejected, "volume_lots": window_volume,
        "top_buyers": [{"firm": f, "net": v["net"], "buy": v["buy"], "sell": v["sell"]} for f, v in top_buyers],
        "top_sellers": [{"firm": f, "net": v["net"], "buy": v["buy"], "sell": v["sell"]} for f, v in top_sellers],
        "buy_top_share": buy_share, "sell_top_share": sell_share,
        "sell_branch_count": sum(1 for v in branch_totals.values() if v["net"] < 0),
        "pattern": pattern, "wash": wash, "offsets": offsets,
        "churn_pct": churn_lots / gross_buy * 100.0 if gross_buy > 0 else None,
        "foreign_broker_net_lots": foreign_net, "foreign_inst_lots": inst_foreign_lots,
    }


# ------------------------------------------------------------------ formatting


def _lots(value: float) -> str:
    return f"{value:+,.0f}"


def _md(day: dt.date) -> str:
    return f"{day.month}/{day.day}"


def institution_line(info: dict[str, Any]) -> str:
    def part(label: str, key: str) -> str:
        lots, pct = info[f"{key}_lots"], info.get(f"{key}_pct")
        share = "" if pct is None else (f"（量 {pct:+.0f}%）" if abs(pct) >= 0.5 else "（量 ~0%）")
        return f"{label} {_lots(lots)} 張{share}"

    coverage = "" if info["days"] >= info["window"] else f"（{info['days']}/{info['window']} 日）"
    text = f"法人 {info['window']} 日{coverage}：{part('外資', 'foreign')}｜{part('投信', 'trust')}｜{part('自營', 'dealer')}"
    if info.get("alignment"):
        text += f"｜{info['alignment']}"
    return text


def broker_lines(info: dict[str, Any], market: str) -> list[str]:
    if market == "上櫃":
        return ["分點：上櫃沒有分點資料"]
    if market != "上市":
        return ["分點：市場別不明，未查詢"]
    if info.get("status") != "ok":
        latest = info.get("latest_data_day")
        tail = f"，最新只到 {_md(latest)}" if latest else ""
        return [f"分點：近 {info.get('window', WINDOW_SESSIONS)} 日沒有可用資料{tail}"]
    days = "、".join(_md(day) for day in info["days"])
    head = f"分點（{days}，{len(info['days'])}/{info['window']} 日）"
    buyers = " ".join(f"{b['firm']} {_lots(b['net'])}" for b in info["top_buyers"]) or "無"
    sellers = " ".join(f"{s['firm']} {_lots(s['net'])}" for s in info["top_sellers"]) or "無"
    lines = [f"{head}：大買 {buyers}｜大賣 {sellers}"]
    notes: list[str] = []
    if info["pattern"]:
        sell_share = info.get("sell_top_share")
        detail = f"，賣超前 3 家僅占 {sell_share * 100:.0f}%、共 {info['sell_branch_count']} 個分點" if (
            info["pattern"] == "買方集中、賣方分散" and sell_share is not None) else ""
        notes.append(f"{info['pattern']}{detail}")
    for item in info["offsets"]:
        notes.append(f"{item['branch']} {_lots(item['branch_net'])} 被{item['firm']}其他分點抵銷（全家 {_lots(item['firm_net'])}）")
    for item in info["wash"]:
        notes.append(
            f"{item['firm']} 毛買 {item['buy']:,.0f}／毛賣 {item['sell']:,.0f} 張"
            f"（賣出占買進 {item['sell'] / item['buy'] * 100:.0f}%）"
        )
    foreign = info.get("foreign_broker_net_lots")
    inst = info.get("foreign_inst_lots")
    if foreign is not None and inst is not None and (abs(foreign) >= 1 or abs(inst) >= 1):
        mismatch = "（兩者方向相反）" if foreign * inst < 0 else ""
        notes.append(f"外資券商分點 {_lots(foreign)}／法人外資 {_lots(inst)} 張{mismatch}")
    if notes:
        lines.append("　".join(notes))
    return lines


def brief_institution_line(info: dict[str, Any]) -> str:
    """The shortest form that still shows all three institutions (used when the digest is tight)."""

    return (f"法人 {info['window']} 日：外資 {_lots(info['foreign_lots'])}／投信 {_lots(info['trust_lots'])}"
            f"／自營 {_lots(info['dealer_lots'])} 張")


def context_lines(ctx: dict[str, Any] | None, detail: str) -> list[str]:
    """Sub-lines (already prefixed) for one stock; ``detail`` is full | inst | brief | none."""

    if not ctx or detail == "none":
        return []
    if detail == "brief":
        return [f"{SUBLINE_PREFIX}{brief_institution_line(ctx['institution'])}"] if ctx.get("institution") else []
    lines: list[str] = []
    if ctx.get("institution"):
        lines.append(institution_line(ctx["institution"]))
    if detail == "full" and ctx.get("broker") is not None:
        lines.extend(broker_lines(ctx["broker"], ctx.get("market", "")))
    return [f"{SUBLINE_PREFIX}{line}" for line in lines]


SUBLINE_PREFIX = "　└ "
DISCLAIMER = (
    "「└」開頭的法人與券商分點附註只是輔助資訊：沒有經過長期驗證，不影響規則命中與排序；"
    "分點資料只有上市、隔天才收、不是每檔每天都有，每行都標明涵蓋哪幾天。"
)
SAME_AS_ABOVE = f"{SUBLINE_PREFIX}籌碼附註同上"
DISCLAIMER_INST_ONLY ="「└」開頭的法人附註只是輔助資訊：沒有經過長期驗證，不影響規則命中與排序。"


# -------------------------------------------------------------------- loading


def _placeholders(items: list[str]) -> str:
    return ",".join(["%s"] * len(items))


def select_session_window(price: pd.DataFrame, as_of: dt.date, size: int = WINDOW_SESSIONS) -> list[dt.date]:
    """Last ``size`` sessions up to ``as_of`` that actually have a normal number of price rows."""

    frame = price[["date", "code"]].copy()
    frame["date"] = frame["date"].map(_to_date)
    counts = frame.dropna(subset=["date"]).groupby("date").size().sort_index()
    counts = counts[[day <= as_of for day in counts.index]]
    if counts.empty:
        return []
    floor = 0.5 * float(counts.tail(10).median())
    return [day for day in counts.index if counts[day] >= floor][-size:]


def _volume_map(price: pd.DataFrame, codes: set[str], sessions: list[dt.date]) -> dict[tuple[str, dt.date], float]:
    wanted = set(sessions)
    frame = price[price["code"].astype(str).isin(codes)]
    volume: dict[tuple[str, dt.date], float] = {}
    for date, code, vol in zip(frame["date"], frame["code"], frame["vol"]):
        day = _to_date(date)
        number = _num(vol)
        if day in wanted and number is not None:
            volume[(str(code), day)] = number
    return volume


def pick_codes(states: list[dict[str, Any]], detection: dict[str, Any]) -> list[str]:
    """Codes the digest can show: the first ``max_names_shown`` hits of every active rule."""

    codes: list[str] = []
    for rule in states:
        if rule.get("status") != "active":
            continue
        hits = (detection.get("rules") or {}).get(rule["id"], {}).get("hits") or []
        limit = int((rule.get("params") or {}).get("max_names_shown", 5))
        for hit in hits[:limit]:
            if hit["code"] not in codes:
                codes.append(hit["code"])
    return codes


def build_flow_context(
    stock_cursor, inputs: dict[str, pd.DataFrame], states: list[dict[str, Any]],
    detection: dict[str, Any], as_of_date: dt.date,
) -> dict[str, Any]:
    """Load and compute the context for the shown codes. Never raises: failure becomes ``error``."""

    try:
        codes = pick_codes(states, detection)
        if not codes or detection.get("status") != "ok":
            return {"codes": {}}
        price = inputs["price"]
        sessions = select_session_window(price, as_of_date)
        if not sessions:
            return {"codes": {}}
        volume = _volume_map(price, set(codes), sessions)
        first, last = sessions[0], sessions[-1]

        stock_cursor.execute(
            f"SELECT TRIM(證券代號), TRIM(市場別) FROM 全證券代碼相關資料表 WHERE TRIM(證券代號) IN ({_placeholders(codes)})",
            tuple(codes),
        )
        market = {str(c): str(m or "").strip() for c, m in stock_cursor.fetchall()}

        stock_cursor.execute(
            "SELECT 資料日期, 證券代號, 外資買賣超股數, 投信買賣超股數, 自營商買賣超股數 FROM 三大法人買賣 "
            f"WHERE 資料日期 BETWEEN %s AND %s AND 證券代號 IN ({_placeholders(codes)})",
            (first, last, *codes),
        )
        inst: dict[tuple[str, dt.date], tuple[float | None, float | None, float | None]] = {}
        for day, code, foreign, trust, dealer in stock_cursor.fetchall():
            parsed = _to_date(day)
            if parsed is not None:
                inst[(str(code).strip(), parsed)] = (_num(foreign), _num(trust), _num(dealer))

        listed = [code for code in codes if market.get(code) == "上市"]
        broker_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if listed:
            # (資料日期, 證券代號) lead the unique index; never wrap 證券代號 in TRIM here.
            stock_cursor.execute(
                "SELECT 資料日期, 證券代號, 券商代號, 券商名稱, 買量, 賣量, create_time FROM 券商進出總表 "
                f"WHERE 資料類型='分點' AND 資料日期 BETWEEN %s AND %s AND 證券代號 IN ({_placeholders(listed)})",
                (first - dt.timedelta(days=10), last, *listed),
            )
            for day, code, branch, name, buy, sell, created in stock_cursor.fetchall():
                broker_rows[str(code).strip()].append(
                    {"day": day, "branch": branch, "name": name, "buy": buy, "sell": sell, "created": created}
                )

        result: dict[str, Any] = {}
        for code in codes:
            kind = market.get(code, "")
            foreign_by_day = {day: inst[(code, day)][0] for day in sessions
                              if (code, day) in inst and inst[(code, day)][0] is not None}
            result[code] = {
                "market": kind,
                "institution": institution_context(code, sessions, inst, volume),
                "broker": (broker_context(code, sessions, broker_rows.get(code, []), volume, foreign_by_day)
                           if kind == "上市" else {"status": "not_listed"}),
            }
        return {"codes": result, "sessions": [day.isoformat() for day in sessions]}
    except Exception as exc:  # the digest must still go out without this supplement
        LOGGER.exception("flow context failed; the digest will be sent without it")
        return {"codes": {}, "error": f"{type(exc).__name__}: {exc}"[:200]}
