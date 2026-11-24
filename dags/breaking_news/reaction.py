"""事後檢驗：用加權指數（TAIEX）日收盤衡量每則警示後的市場反應。

重要限制（刻意誠實標示）：專案目前只有 TAIEX 的「日」收盤，沒有盤中資料，
所以這裡只能算「警示前最後一個收盤 → 警示後第 1/2 個收盤」的報酬，
其中包含警示發生前就已發生的走勢，是粗估，只適合做門檻是否有鑑別力的長期檢驗，
不是精確的事件研究。
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Sequence

from breaking_news.store import TABLE

TAIEX_CLOSE_TIME = time(13, 30)
TAIPEI_OFFSET = timedelta(hours=8)
SETTLE_DECISIONS = ("push", "watch", "capped")
GIVE_UP_AFTER_DAYS = 10
BATCH_LIMIT = 500


def compute_reaction(
    alert_utc: datetime, closes: Sequence[tuple[date, float]]
) -> dict | None:
    """closes: 依日期遞增的 (交易日, 收盤指數)。資料不足回傳 None。"""
    alert_taipei = alert_utc + TAIPEI_OFFSET
    before = [(d, c) for d, c in closes if datetime.combine(d, TAIEX_CLOSE_TIME) <= alert_taipei]
    after = [(d, c) for d, c in closes if datetime.combine(d, TAIEX_CLOSE_TIME) > alert_taipei]
    if not before or not after:
        return None
    ref_date, ref_close = before[-1]
    if not ref_close:
        return None
    result = {
        "ref_date": ref_date,
        "ref_close": ref_close,
        "post1_date": after[0][0],
        "post1_return_pct": round((after[0][1] / ref_close - 1) * 100, 4),
        "post2_date": None,
        "post2_return_pct": None,
    }
    if len(after) >= 2:
        result["post2_date"] = after[1][0]
        result["post2_return_pct"] = round((after[1][1] / ref_close - 1) * 100, 4)
    return result


def _load_closes(cursor, start: date, end: date) -> list[tuple[date, float]]:
    cursor.execute(
        "SELECT effective_date, metric_value FROM source_records "
        "WHERE event_type = 'market_index_daily' AND entity_id = 'TAIEX' "
        "AND quality_status = 'ok' AND effective_date BETWEEN %s AND %s "
        "ORDER BY effective_date",
        (start, end),
    )
    return [(row[0], float(row[1])) for row in cursor.fetchall() if row[1] is not None]


def settle_reactions(cursor, now_utc: datetime) -> dict:
    placeholders = ",".join(["%s"] * len(SETTLE_DECISIONS))
    cursor.execute(
        f"SELECT item_key, published_at FROM {TABLE} "
        f"WHERE decision IN ({placeholders}) AND reaction_settled_at IS NULL "
        f"AND published_at < %s ORDER BY published_at LIMIT {BATCH_LIMIT}",
        (*SETTLE_DECISIONS, now_utc - timedelta(days=1)),
    )
    pending = cursor.fetchall()
    if not pending:
        return {"pending": 0, "settled": 0}

    first = min(row[1] for row in pending)
    last = max(row[1] for row in pending)
    closes = _load_closes(
        cursor,
        (first + TAIPEI_OFFSET).date() - timedelta(days=10),
        (last + TAIPEI_OFFSET).date() + timedelta(days=10),
    )

    settled = 0
    for key, published_at in pending:
        reaction = compute_reaction(published_at, closes)
        too_old = published_at < now_utc - timedelta(days=GIVE_UP_AFTER_DAYS)
        if reaction is None:
            if too_old:
                # 永遠算不出來（指數資料缺口）：標記已處理，避免卡住後面的批次
                cursor.execute(
                    f"UPDATE {TABLE} SET reaction_settled_at = %s WHERE item_key = %s",
                    (now_utc, key),
                )
            continue
        # 第 2 個收盤還沒出來就先等，除非已經等太久
        if reaction["post2_date"] is None and not too_old:
            continue
        cursor.execute(
            f"UPDATE {TABLE} SET taiex_ref_date = %s, taiex_ref_close = %s, "
            f"taiex_post1_date = %s, taiex_post1_return_pct = %s, "
            f"taiex_post2_date = %s, taiex_post2_return_pct = %s, reaction_settled_at = %s "
            f"WHERE item_key = %s",
            (
                reaction["ref_date"], reaction["ref_close"],
                reaction["post1_date"], reaction["post1_return_pct"],
                reaction["post2_date"], reaction["post2_return_pct"], now_utc, key,
            ),
        )
        settled += 1
    return {"pending": len(pending), "settled": settled}


def reaction_summary(cursor) -> list[tuple]:
    """各決策別的樣本數與平均絕對報酬，用來看 push 門檻是否真的比 watch 更能動盤。"""
    cursor.execute(
        f"SELECT decision, COUNT(*), AVG(ABS(taiex_post1_return_pct)), "
        f"AVG(taiex_post1_return_pct), AVG(ABS(taiex_post2_return_pct)) "
        f"FROM {TABLE} WHERE reaction_settled_at IS NOT NULL "
        f"AND taiex_post1_return_pct IS NOT NULL GROUP BY decision"
    )
    return list(cursor.fetchall())


def mean_abs_daily_return(closes: Sequence[tuple[date, float]]) -> float | None:
    """全部交易日的平均絕對日報酬(%)，當作「隨便一天」的基準線。"""
    moves = [
        abs(curr / prev - 1) * 100
        for (_, prev), (_, curr) in zip(closes, closes[1:])
        if prev
    ]
    return round(sum(moves) / len(moves), 4) if moves else None


def baseline_abs_return(cursor, now_utc: datetime, days: int = 400) -> float | None:
    end = (now_utc + TAIPEI_OFFSET).date()
    return mean_abs_daily_return(_load_closes(cursor, end - timedelta(days=days), end))
