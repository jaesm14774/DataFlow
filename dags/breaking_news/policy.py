"""推播決策：把「評分」變成「推不推」。純函式，所有狀態由呼叫端傳入。

處理的失敗模式：
- 首次執行洪水：該來源在帳本裡還沒有任何資料 → 只記錄（seeded），不推。
- 過期新聞：發布時間距今超過 MAX_AGE_MINUTES → 只記錄（stale）。
- 同一事件多家報導：以 token 相似度分群，推過就不再推（duplicate）；
  不同來源獨立印證會加分，讓單一媒體的快訊在第二、三家跟進後升級成 push。
- 頻率上限：每小時、每日硬上限，超過只記錄（capped）；極重大事件（≥CRITICAL）可繞過。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta

from breaking_news.scoring import (
    CRITICAL_THRESHOLD,
    TIER_IGNORE,
    TIER_PUSH,
    TIER_WATCH,
    PUSH_THRESHOLD,
    NewsItem,
    Scored,
    publisher,
    same_event,
    source_traits,
    tier_for_score,
)

MAX_AGE_MINUTES = 90
CLUSTER_WINDOW = timedelta(hours=12)
# 已推過的事件，只有後來的單則分數明顯更高（升級）才允許再推
ESCALATION_MARGIN = 2
HOURLY_PUSH_CAP = 3
DAILY_PUSH_CAP = 10
CORROBORATION_BONUS_PER_SOURCE = 2
CORROBORATION_BONUS_CAP = 4

DECISION_PUSH = "push"
DECISION_WATCH = "watch"
DECISION_IGNORE = "ignore"
DECISION_SEEDED = "seeded"
DECISION_STALE = "stale"
DECISION_DUPLICATE = "duplicate"
DECISION_CAPPED = "capped"


def item_key(item: NewsItem) -> str:
    return hashlib.sha256(f"{item.source_key}|{item.guid}".encode("utf-8")).hexdigest()


@dataclass
class PoolEvent:
    """帳本中近期事件的最小資訊，用來做分群與印證。"""

    item_key: str
    source_key: str
    tokens: frozenset[str]
    base_score: int
    triggered: bool
    covered: bool  # 這個事件已經推過、或被判定為已處理（seeded/stale/duplicate）
    published_at: datetime


@dataclass(frozen=True)
class Decision:
    item: NewsItem
    scored: Scored
    key: str
    decision: str
    final_score: int
    final_tier: str
    corroborating_sources: tuple[str, ...] = ()


def _corroborators(members: list[PoolEvent], own_source: str) -> set[str]:
    sources = {publisher(own_source)} if source_traits(own_source).corroborates else set()
    sources.update(publisher(m.source_key) for m in members if source_traits(m.source_key).corroborates)
    return sources


def decide(
    new_items: list[tuple[NewsItem, Scored]],
    *,
    now_utc: datetime,
    seeded_sources: set[str],
    pool: list[PoolEvent],
    pushed_last_hour: int,
    pushed_last_day: int,
) -> list[Decision]:
    """對本輪新出現的項目逐一決策（依發布時間由舊到新）。"""
    decisions: list[Decision] = []
    pool = list(pool)
    hourly = pushed_last_hour
    daily = pushed_last_day
    stale_before = now_utc - timedelta(minutes=MAX_AGE_MINUTES)

    for item, scored in sorted(new_items, key=lambda pair: pair[0].published_at):
        key = item_key(item)

        def finish(decision: str, final_score: int, final_tier: str, *, covered: bool,
                   sources: tuple[str, ...] = ()) -> None:
            decisions.append(Decision(item, scored, key, decision, final_score, final_tier, sources))
            # duplicate 不當新的錨點：否則同題材的新稿一路接力，群集會無限延長
            anchor = decision not in (DECISION_DUPLICATE, DECISION_STALE)
            if decision == DECISION_SEEDED and not covered:
                anchor = False  # 首次執行留下的過期項目不當印證或錨點
            if anchor and (scored.triggered or covered):
                pool.append(PoolEvent(
                    item_key=key,
                    source_key=item.source_key,
                    tokens=scored.tokens,
                    base_score=scored.score,
                    triggered=scored.triggered,
                    covered=covered,
                    published_at=item.published_at,
                ))

        is_stale = item.published_at < stale_before
        if item.source_key in seeded_sources:
            # 首次執行的新鮮項目要當錨點（擋住之後的轉載）；過期的不當，避免誤殺新消息
            finish(DECISION_SEEDED, scored.score, scored.tier, covered=not is_stale)
            continue
        if is_stale:
            finish(DECISION_STALE, scored.score, scored.tier, covered=False)
            continue

        matches = [
            member for member in pool
            if abs(member.published_at - item.published_at) <= CLUSTER_WINDOW
            and same_event(scored.tokens, member.tokens)
        ]
        covered_matches = [member for member in matches if member.covered]
        if covered_matches:
            anchor_score = max(member.base_score for member in covered_matches)
            escalated = (
                scored.score >= PUSH_THRESHOLD
                and scored.score >= anchor_score + ESCALATION_MARGIN
                and source_traits(item.source_key).corroborates
            )
            if not escalated:
                finish(DECISION_DUPLICATE, scored.score, TIER_IGNORE, covered=True)
                continue

        sources = _corroborators(matches, item.source_key)
        bonus = 0
        if len(sources) >= 2:
            bonus = min(
                CORROBORATION_BONUS_CAP,
                CORROBORATION_BONUS_PER_SOURCE * (len(sources) - 1),
            )
        cluster_base = max([scored.score, *(m.base_score for m in matches)])
        triggered = scored.triggered or any(m.triggered for m in matches)
        final_score = cluster_base + bonus
        final_tier = tier_for_score(final_score, triggered)
        source_tuple = tuple(sorted(sources))

        if final_tier == TIER_PUSH and not source_traits(item.source_key).corroborates:
            # 聚合來源（Google News）只當印證，自己不觸發推播
            final_tier = TIER_WATCH
        if final_tier == TIER_PUSH:
            # 繞過上限需要分數夠高「且」至少兩家不同媒體印證，避免單一來源連發洗版
            critical = final_score >= CRITICAL_THRESHOLD and len(sources) >= 2
            if not critical and (hourly >= HOURLY_PUSH_CAP or daily >= DAILY_PUSH_CAP):
                finish(DECISION_CAPPED, final_score, final_tier, covered=False, sources=source_tuple)
                continue
            hourly += 1
            daily += 1
            finish(DECISION_PUSH, final_score, final_tier, covered=True, sources=source_tuple)
        elif final_tier == TIER_WATCH:
            finish(DECISION_WATCH, final_score, final_tier, covered=False, sources=source_tuple)
        else:
            finish(DECISION_IGNORE, final_score, final_tier, covered=False, sources=source_tuple)

    return decisions
