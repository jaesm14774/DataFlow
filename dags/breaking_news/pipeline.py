"""重磅消息掃描主流程：抓取 → 評分 → 決策 → 寫帳本 → 推播。

依賴（連線、抓取、通知器、時間）都可注入，測試不需要網路與資料庫。
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Callable

from breaking_news import store
from breaking_news.policy import DAILY_PUSH_CAP, HOURLY_PUSH_CAP, decide, item_key
from breaking_news.scoring import CRITICAL_THRESHOLD, excerpt, score_item
from breaking_news.sources import SOURCES, SOURCES_BY_KEY, fetch_all

# 預設沿用既有頻道；若要獨立頻道，先在 Discord token CSV 新增一列並設定此環境變數
ALERT_CHANNEL_LABEL = os.getenv("BREAKING_NEWS_DISCORD_LABEL", "新聞即時通")
TAIPEI_OFFSET = timedelta(hours=8)

CATEGORY_LABELS = {
    "monetary": "貨幣政策",
    "trade": "貿易/制裁",
    "war": "戰爭/軍事",
    "black_swan": "市場急跌/黑天鵝",
    "energy": "能源",
    "macro_data": "總經數據",
    "leadership": "關鍵人物異動",
    "taiwan_conflict": "台海",
    "taiwan": "台灣",
    "china": "中國",
    "semis_ai": "半導體/AI",
}
_HEADLINE_CATEGORIES = (
    "taiwan_conflict", "black_swan", "war", "monetary", "trade",
    "leadership", "energy", "macro_data",
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def format_alert(
    *, source_key: str, title: str, url: str, published_at: datetime,
    score: int, categories: str, corroborating_sources: str,
) -> str:
    spec = SOURCES_BY_KEY.get(source_key)
    label = spec.label if spec else source_key
    names = [name for name in categories.split(",") if name]
    headline = [CATEGORY_LABELS[n] for n in _HEADLINE_CATEGORIES if n in names]
    tags = "、".join(headline) or "重要消息"
    taipei = (published_at + TAIPEI_OFFSET).strftime("%m/%d %H:%M")
    sources = [s for s in corroborating_sources.split(",") if s]
    corroboration = f"｜{len(sources)} 家來源印證" if len(sources) >= 2 else ""
    lines = [
        f"🚨 重磅快訊｜{tags}",
        excerpt(title, 140),
        f"來源：{label}｜{taipei}（台北）｜分數 {score}{corroboration}",
    ]
    if url:
        lines.append(url)
    return "\n".join(lines)


def _send_one(cursor, connection, notifier, key: str, message: str) -> str:
    try:
        result = notifier.notify_with_receipt(msg=message)
    except Exception as exc:  # 通知器自己的未預期錯誤：結果未知，不自動重送
        print(f"[breaking_news] 推播例外：{type(exc).__name__}")
        result = {"success": False, "accepted": None, "outcome_unknown": True,
                  "http_status": None, "receipt_id": None}
    status = store.mark_push(cursor, key, result, _now_utc())
    connection.commit()
    return status


def run_settle(*, now_utc: datetime | None = None, open_connection: Callable | None = None) -> dict:
    """每日一次：回填警示後的 TAIEX 反應，並印出各決策別的彙總。"""
    from breaking_news import reaction

    if open_connection is None:
        from lib.get_sql import get_sql_from_config

        def open_connection():
            return get_sql_from_config("crawler")

    connection, cursor, engine = open_connection()
    try:
        store.create_schema(cursor)
        result = reaction.settle_reactions(cursor, now_utc or _now_utc())
        connection.commit()
        result["summary"] = reaction.reaction_summary(cursor)
        # 各決策別的平均絕對報酬必須跟這條基準線比，才看得出門檻有沒有鑑別力
        result["baseline_mean_abs_daily_return_pct"] = reaction.baseline_abs_return(
            cursor, now_utc or _now_utc()
        )
        print(f"[breaking_news] 反應回填：{result}")
        return result
    except Exception as exc:
        connection.rollback()
        print(f"[breaking_news] 反應回填失敗：{type(exc).__name__}")
        raise
    finally:
        connection.close()
        engine.dispose()


def run_scan(
    *,
    now_utc: datetime | None = None,
    open_connection: Callable | None = None,
    fetch: Callable = fetch_all,
    notifier_factory: Callable | None = None,
) -> dict:
    now = now_utc or _now_utc()
    items, errors, attempted = fetch(SOURCES, now)
    if attempted and len(errors) == len(attempted):
        raise RuntimeError(f"[breaking_news] 所有來源皆失敗：{sorted(errors)}")

    if open_connection is None:
        from lib.get_sql import get_sql_from_config

        def open_connection():
            return get_sql_from_config("crawler")

    if notifier_factory is None:
        from lib.notify import discord_notifier

        def notifier_factory():
            return discord_notifier(ALERT_CHANNEL_LABEL)

    connection, cursor, engine = open_connection()
    summary = {"fetched": len(items), "new": 0, "pushed": 0, "retried": 0,
               "errors": dict(errors), "decisions": {}}
    try:
        store.create_schema(cursor)
        connection.commit()
        store.recover_stale_pending(cursor, now)
        connection.commit()

        scored = {}
        for item in items:
            scored.setdefault(item_key(item), (item, score_item(item)))
        known = store.known_item_keys(cursor, scored.keys())
        new_items = [pair for key, pair in scored.items() if key not in known]
        summary["new"] = len(new_items)

        notifier = None
        if new_items:
            fresh_sources = {item.source_key for item, _ in new_items}
            seeded_sources = fresh_sources - store.sources_with_history(cursor, fresh_sources)
            decisions = decide(
                new_items,
                now_utc=now,
                seeded_sources=seeded_sources,
                pool=store.load_pool(cursor, now),
                pushed_last_hour=store.count_pushes_since(cursor, now - timedelta(hours=1)),
                pushed_last_day=store.count_pushes_since(cursor, now - timedelta(days=1)),
            )
            for decision in decisions:
                summary["decisions"][decision.decision] = summary["decisions"].get(decision.decision, 0) + 1
            # 先確認通知器能建立（缺 token 等）再寫帳本；失敗時整輪回滾，下一輪還能重推
            if any(d.decision == "push" for d in decisions):
                notifier = notifier_factory()
            to_send = store.insert_decisions(cursor, decisions, now)
            connection.commit()

            for decision in to_send:
                message = format_alert(
                    source_key=decision.item.source_key,
                    title=decision.item.title,
                    url=decision.item.url,
                    published_at=decision.item.published_at,
                    score=decision.final_score,
                    categories=",".join(decision.scored.categories),
                    corroborating_sources=",".join(decision.corroborating_sources),
                )
                status = _send_one(cursor, connection, notifier, decision.key, message)
                if status == store.PUSH_STATUS_SENT:
                    summary["pushed"] += 1

        for row in store.failed_pushes_to_retry(cursor, now):
            key, source_key, title, url, published_at, score, categories, sources = row
            # 重試也要遵守頻率上限（極重大事件除外）
            if int(score) < CRITICAL_THRESHOLD and (
                store.count_pushes_since(cursor, now - timedelta(hours=1)) >= HOURLY_PUSH_CAP
                or store.count_pushes_since(cursor, now - timedelta(days=1)) >= DAILY_PUSH_CAP
            ):
                continue
            notifier = notifier or notifier_factory()  # 先建好通知器，再認領（避免認領後建不起來而丟警示）
            if not store.claim_retry(cursor, key):
                continue
            connection.commit()
            message = format_alert(
                source_key=source_key, title=title, url=url, published_at=published_at,
                score=int(score), categories=categories, corroborating_sources=sources,
            )
            if _send_one(cursor, connection, notifier, key, message) == store.PUSH_STATUS_SENT:
                summary["retried"] += 1

        print(f"[breaking_news] 掃描完成：{summary}")
        return summary
    except Exception as exc:
        connection.rollback()
        print(f"[breaking_news] 掃描失敗：{type(exc).__name__}")
        raise
    finally:
        connection.close()
        engine.dispose()
