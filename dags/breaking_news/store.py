"""帳本（crawler DB）：每一則被評分的消息都留一筆，推播與否都記錄，
供日後用市場反應回頭檢驗門檻。建表沿用專案慣例：CREATE TABLE IF NOT EXISTS + GET_LOCK。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

from breaking_news.policy import (
    DECISION_PUSH,
    MAX_AGE_MINUTES,
    CLUSTER_WINDOW,
    Decision,
    PoolEvent,
)

TABLE = "breaking_news_events"
SCHEMA_LOCK = "dataflow_breaking_news_schema"

PUSH_STATUS_PENDING = "pending"
PUSH_STATUS_SENT = "sent"
PUSH_STATUS_ACCEPTED = "accepted_unverified"
PUSH_STATUS_UNKNOWN = "delivery_unknown"
PUSH_STATUS_FAILED = "failed"
# 已經（可能）通知到使用者的狀態，計入頻率上限；failed 沒送到所以不計
COUNTED_PUSH_STATUSES = (
    PUSH_STATUS_PENDING, PUSH_STATUS_SENT, PUSH_STATUS_ACCEPTED, PUSH_STATUS_UNKNOWN,
)
PENDING_RECOVERY_MINUTES = 10

SCHEMA_STATEMENTS = (
    f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        item_key CHAR(64) NOT NULL,
        source_key VARCHAR(48) NOT NULL,
        title TEXT NOT NULL,
        url VARCHAR(1000) NOT NULL,
        published_at DATETIME NOT NULL COMMENT 'UTC',
        first_seen_at DATETIME NOT NULL COMMENT 'UTC',
        base_score SMALLINT NOT NULL,
        score SMALLINT NOT NULL,
        tier VARCHAR(8) NOT NULL,
        decision VARCHAR(12) NOT NULL,
        triggered TINYINT(1) NOT NULL,
        covered TINYINT(1) NOT NULL,
        categories VARCHAR(255) NOT NULL,
        reasons TEXT NOT NULL,
        tokens TEXT NOT NULL,
        corroborating_sources VARCHAR(255) NOT NULL,
        push_status VARCHAR(32) NULL,
        push_receipt_id VARCHAR(64) NULL,
        push_http_status SMALLINT NULL,
        pushed_at DATETIME NULL COMMENT 'UTC',
        taiex_ref_date DATE NULL,
        taiex_ref_close DECIMAL(30,8) NULL,
        taiex_post1_date DATE NULL,
        taiex_post1_return_pct DECIMAL(10,4) NULL,
        taiex_post2_date DATE NULL,
        taiex_post2_return_pct DECIMAL(10,4) NULL,
        reaction_settled_at DATETIME NULL COMMENT 'UTC',
        PRIMARY KEY (id),
        UNIQUE KEY uq_breaking_news_item (item_key),
        KEY ix_breaking_news_published (published_at),
        KEY ix_breaking_news_source (source_key, published_at),
        KEY ix_breaking_news_push (decision, push_status, published_at),
        KEY ix_breaking_news_reaction (decision, reaction_settled_at, published_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)


def create_schema(cursor) -> None:
    cursor.execute("SELECT GET_LOCK(%s, 60)", (SCHEMA_LOCK,))
    if cursor.fetchone()[0] != 1:
        raise RuntimeError("could not acquire breaking news schema lock")
    try:
        for statement in SCHEMA_STATEMENTS:
            cursor.execute(statement)
    finally:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (SCHEMA_LOCK,))


def known_item_keys(cursor, keys: Iterable[str]) -> set[str]:
    keys = list(keys)
    found: set[str] = set()
    for start in range(0, len(keys), 500):
        chunk = keys[start:start + 500]
        placeholders = ",".join(["%s"] * len(chunk))
        cursor.execute(f"SELECT item_key FROM {TABLE} WHERE item_key IN ({placeholders})", tuple(chunk))
        found.update(row[0] for row in cursor.fetchall())
    return found


def sources_with_history(cursor, source_keys: Iterable[str]) -> set[str]:
    source_keys = list(source_keys)
    if not source_keys:
        return set()
    placeholders = ",".join(["%s"] * len(source_keys))
    cursor.execute(
        f"SELECT DISTINCT source_key FROM {TABLE} WHERE source_key IN ({placeholders})",
        tuple(source_keys),
    )
    return {row[0] for row in cursor.fetchall()}


def load_pool(cursor, now_utc: datetime) -> list[PoolEvent]:
    since = now_utc - CLUSTER_WINDOW - timedelta(minutes=MAX_AGE_MINUTES)
    fresh_after = now_utc - timedelta(minutes=MAX_AGE_MINUTES)
    # duplicate / stale 不當錨點（避免群集跨輪次接力、也避免舊聞誤殺新消息）；
    # 首次執行留下的 seeded 只有「當時還新鮮」的才有錨點效力
    cursor.execute(
        f"SELECT item_key, source_key, tokens, base_score, triggered, covered, published_at "
        f"FROM {TABLE} WHERE published_at >= %s AND (triggered = 1 OR covered = 1) "
        f"AND decision NOT IN ('duplicate', 'stale') "
        f"AND (decision <> 'seeded' OR published_at >= %s)",
        (since, fresh_after),
    )
    return [
        PoolEvent(
            item_key=row[0],
            source_key=row[1],
            tokens=frozenset(str(row[2]).split()),
            base_score=int(row[3]),
            triggered=bool(row[4]),
            covered=bool(row[5]),
            published_at=row[6],
        )
        for row in cursor.fetchall()
    ]


def count_pushes_since(cursor, since: datetime) -> int:
    placeholders = ",".join(["%s"] * len(COUNTED_PUSH_STATUSES))
    cursor.execute(
        f"SELECT COUNT(*) FROM {TABLE} WHERE decision = %s AND first_seen_at >= %s "
        f"AND push_status IN ({placeholders})",
        (DECISION_PUSH, since, *COUNTED_PUSH_STATUSES),
    )
    return int(cursor.fetchone()[0])


def recover_stale_pending(cursor, now_utc: datetime) -> int:
    """送出前就中斷的 pending：超過一段時間標成 delivery_unknown，不自動重送避免重複。"""
    cursor.execute(
        f"UPDATE {TABLE} SET push_status = %s WHERE push_status = %s AND first_seen_at < %s",
        (PUSH_STATUS_UNKNOWN, PUSH_STATUS_PENDING,
         now_utc - timedelta(minutes=PENDING_RECOVERY_MINUTES)),
    )
    return cursor.rowcount


def insert_decisions(cursor, decisions: list[Decision], now_utc: datetime) -> list[Decision]:
    """寫入帳本；回傳「確實由本次寫入、且需要推播」的決策（防止並行重複）。"""
    to_send: list[Decision] = []
    for decision in decisions:
        scored = decision.scored
        item = decision.item
        is_push = decision.decision == DECISION_PUSH
        cursor.execute(
            f"INSERT IGNORE INTO {TABLE} (item_key, source_key, title, url, published_at, "
            f"first_seen_at, base_score, score, tier, decision, triggered, covered, categories, "
            f"reasons, tokens, corroborating_sources, push_status) "
            f"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                decision.key, item.source_key, item.title, item.url[:1000], item.published_at,
                now_utc, scored.score, decision.final_score, decision.final_tier,
                decision.decision, int(scored.triggered), int(_is_covered(decision)),
                ",".join(scored.categories)[:255], "; ".join(scored.reasons),
                " ".join(sorted(scored.tokens)), ",".join(decision.corroborating_sources)[:255],
                PUSH_STATUS_PENDING if is_push else None,
            ),
        )
        if is_push and cursor.rowcount == 1:
            to_send.append(decision)
    return to_send


def _is_covered(decision: Decision) -> bool:
    return decision.decision in {"push", "seeded", "stale", "duplicate"}  # 已處理；是否為錨點見 policy/load_pool


def delivery_status(result: dict) -> str:
    if result.get("success") is True and result.get("receipt_id"):
        return PUSH_STATUS_SENT
    if result.get("accepted") is True:
        return PUSH_STATUS_ACCEPTED
    if result.get("outcome_unknown") is True:
        return PUSH_STATUS_UNKNOWN
    return PUSH_STATUS_FAILED


def mark_push(cursor, key: str, result: dict, now_utc: datetime) -> str:
    status = delivery_status(result)
    cursor.execute(
        f"UPDATE {TABLE} SET push_status = %s, push_receipt_id = %s, push_http_status = %s, "
        f"pushed_at = %s WHERE item_key = %s AND push_status = '{PUSH_STATUS_PENDING}'",
        (
            status,
            result.get("receipt_id") if status == PUSH_STATUS_SENT else None,
            result.get("http_status"),
            now_utc if status != PUSH_STATUS_FAILED else None,
            key,
        ),
    )
    return status


def claim_retry(cursor, key: str) -> bool:
    cursor.execute(
        f"UPDATE {TABLE} SET push_status = %s WHERE item_key = %s AND push_status = %s",
        (PUSH_STATUS_PENDING, key, PUSH_STATUS_FAILED),
    )
    return cursor.rowcount == 1


def failed_pushes_to_retry(cursor, now_utc: datetime) -> list[tuple]:
    """送出明確失敗（非未知結果）且仍在有效期內的推播，下一輪再試一次。"""
    cursor.execute(
        f"SELECT item_key, source_key, title, url, published_at, score, categories, "
        f"corroborating_sources FROM {TABLE} "
        f"WHERE decision = %s AND push_status = %s AND published_at >= %s",
        (DECISION_PUSH, PUSH_STATUS_FAILED, now_utc - timedelta(minutes=MAX_AGE_MINUTES)),
    )
    return list(cursor.fetchall())
