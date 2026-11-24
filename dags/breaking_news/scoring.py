"""重磅消息評分：純函式，不碰網路與資料庫。

score_item(item) -> Scored，內容是 (tier, score, categories, reasons, tokens)。
之後若要換成 LLM 分類器，只需要替換這一層，其餘流程不用動。

設計原則（對應「只推真正會動搖整個市場的消息」）：
- 一定要命中至少一個「觸發類別」(trigger) 才可能進 push / watch；
  只有「背景類別」（例如只提到輝達或中國）不算。
- 官方來源（Fed）與美國總統貼文有較高的基礎分，一般媒體基礎分低，
  需要多家媒體交叉印證（見 policy.py 的 corroboration）才會升級成 push。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

TIER_PUSH = "push"
TIER_WATCH = "watch"
TIER_IGNORE = "ignore"

PUSH_THRESHOLD = 6
WATCH_THRESHOLD = 3
CRITICAL_THRESHOLD = 8  # 分數達此值且 ≥2 家不同媒體印證，才可繞過頻率上限
EXTRA_CATEGORY_BONUS_CAP = 2  # 主類別之外，每多命中一個類別 +1，最多 +2
TITLE_SCORING_LIMIT = 280  # 只看標題（長貼文只看前 280 字），摘要太容易誤判
NOISE_PENALTY = 4
COMMENTARY_PENALTY = 2  # 評論/解說/直播稿降一級


@dataclass(frozen=True)
class NewsItem:
    source_key: str
    guid: str
    title: str
    url: str
    published_at: datetime  # UTC, naive
    summary: str = ""


@dataclass(frozen=True)
class SourceTraits:
    base_score: int
    require_trigger_hit: bool = True
    # 聚合型來源（Google News）只是轉載別家報導，不能算獨立印證
    corroborates: bool = True
    # 若設定：標題沒命中就最高只到 watch（例如 Fed 的例行公告不推）
    push_title_pattern: re.Pattern | None = None


@dataclass(frozen=True)
class Scored:
    tier: str
    score: int
    categories: tuple[str, ...]
    reasons: tuple[str, ...]
    tokens: frozenset[str] = field(default_factory=frozenset)
    triggered: bool = False


@dataclass(frozen=True)
class Category:
    name: str
    weight: int
    trigger: bool
    patterns: tuple[str, ...]


def _compile(patterns: tuple[str, ...]) -> re.Pattern:
    return re.compile("|".join(patterns), re.IGNORECASE)


# --- 類別定義 ---------------------------------------------------------------
# 英文用 \b 邊界避免誤判；中文直接子字串比對。
_CATEGORIES: tuple[Category, ...] = (
    Category("monetary", 3, True, (
        r"\bfederal reserve\b", r"\bfomc\b", r"\brate (?:cut|hike|decision)s?\b",
        r"\binterest rates?\b", r"\bpowell\b", r"\bemergency (?:rate|meeting)\b",
        r"\bquantitative (?:easing|tightening)\b", r"\bbalance sheet\b",
        r"聯準會", r"升息", r"降息", r"鮑爾", r"緊急(?:降息|會議)", r"縮表", r"量化寬鬆",
    )),
    Category("trade", 3, True, (
        r"\btariffs?\b", r"\bsanctions?\b", r"\bexport controls?\b", r"\btrade war\b",
        r"\bembargo\b", r"\bentity list\b", r"\bchip (?:ban|curbs?|restrictions?)\b",
        r"關稅", r"制裁", r"出口管制", r"貿易戰", r"禁運", r"實體清單",
    )),
    Category("war", 3, True, (
        r"\bdeclares? war\b", r"\bwar (?:with|on|against|breaks? out)\b",
        r"\binvasion\b", r"\binvad(?:e|es|ed|ing)\b", r"\bmissiles?\b",
        r"\bair ?strikes?\b", r"\bmilitary strikes?\b", r"\bcease-?fire\b",
        r"\bnuclear\b", r"\bblockade\b", r"\bmilitary drills?\b",
        r"戰爭", r"開戰", r"入侵", r"飛彈", r"空襲", r"停火", r"封鎖", r"核武", r"軍演",
    )),
    Category("black_swan", 3, True, (
        r"\bmarket crash\b", r"\bflash crash\b", r"\bstocks? (?:crash|plunge|plummet|tumble)",
        r"\bsell-?off\b", r"\bcircuit breaker\b", r"\bbank run\b", r"\bsovereign default\b",
        r"\bbankruptcy\b", r"\bchapter 11\b", r"\bmeltdown\b",
        r"崩盤", r"熔斷", r"暴跌", r"股災", r"擠兌", r"破產", r"違約",
    )),
    Category("energy", 2, True, (
        r"\bopec\b", r"\boil prices?\b", r"\bcrude\b", r"\bstrait of hormuz\b",
        r"原油", r"油價", r"荷莫茲",
    )),
    Category("macro_data", 2, True, (
        r"\bcpi\b", r"\bnonfarm\b", r"\bpayrolls?\b", r"\bjobs report\b",
        r"\bunemployment rate\b", r"\brecession\b", r"\binflation\b", r"\bgdp\b",
        r"非農", r"通膨", r"衰退", r"失業率",
    )),
    # 背景類別：只加分，不能單獨觸發
    Category("taiwan", 1, False, (
        r"\btaiwan\b", r"\btaipei\b", r"台海", r"台灣", r"兩岸", r"共軍", r"解放軍",
    )),
    Category("china", 1, False, (r"\bchina\b", r"\bchinese\b", r"\bbeijing\b", r"中國", r"北京")),
    Category("semis_ai", 1, False, (
        r"\bnvidia\b", r"\btsmc\b", r"\bsemiconductors?\b", r"\bchips?\b",
        r"台積電", r"輝達", r"晶片", r"半導體",
    )),
)

_COMPILED_CATEGORIES = tuple((category, _compile(category.patterns)) for category in _CATEGORIES)

# 領導人事異動：需要「動作」+「角色或人名」+「超大型標的/關鍵人物」同時出現
_LEADERSHIP_ACTION = _compile((
    r"\bresign(?:s|ed|ation)?\b", r"\bsteps? down\b", r"\bstepping down\b",
    r"\boust(?:s|ed|ing)?\b", r"\bfired\b", r"\bsacked\b", r"\bdies\b", r"\bdeath of\b",
    r"\breinstat(?:e|es|ed|ement)\b", r"\breturn(?:s|ed)? as\b", r"\bcomeback\b",
    r"\bsuccession\b", r"\bto replace\b", r"\bnamed (?:new )?ceo\b",
    r"\bre-?hir(?:e|es|ed|ing)\b", r"\breappoint(?:s|ed|ment)?\b",
    r"下台", r"辭職", r"請辭", r"解任", r"換帥", r"重返", r"回歸", r"復出", r"重回", r"接班", r"逝世",
    r"復職", r"復任", r"重新任命", r"回鍋",
))
_LEADERSHIP_ROLE = _compile((
    r"\bceo\b", r"\bchairman\b", r"\bchair\b", r"\bpresident\b", r"\bfounder\b",
    r"執行長", r"董事長", r"總裁", r"創辦人", r"總統", r"主席",
))
_MEGA_ENTITIES = _compile((
    r"\bnvidia\b", r"\bapple\b", r"\bmicrosoft\b", r"\btsmc\b", r"\btesla\b", r"\bmusk\b",
    r"\balphabet\b", r"\bgoogle\b", r"\bamazon\b", r"\bmeta\b", r"\bopenai\b",
    r"\bsamsung\b", r"\bintel\b", r"\bbroadcom\b", r"\bhuang\b", r"\bpowell\b",
    r"\bfed\b", r"\btrump\b", r"\bxi jinping\b", r"\bputin\b",
    r"輝達", r"黃仁勳", r"台積電", r"魏哲家", r"蘋果", r"特斯拉", r"馬斯克", r"微軟",
    r"鴻海", r"聯發科", r"鮑爾", r"川普", r"習近平", r"普丁", r"聯準會",
))
_LEADERSHIP_PERSON = _compile((
    r"\bhuang\b", r"\bmusk\b", r"\bpowell\b", r"\btrump\b", r"\bxi jinping\b", r"\bputin\b",
    r"黃仁勳", r"魏哲家", r"馬斯克", r"鮑爾", r"川普", r"習近平", r"普丁",
))

# 例行公告：Fed 的行程、年報、問卷等不是行情消息
_NOISE = _compile((
    r"\btentative\b", r"\bschedule\b", r"\bcalendar\b", r"\bannual report\b",
    r"\bsurvey\b", r"\bconference\b", r"\bpublication\b", r"\bregistrations?\b",
))

# 評論、解說、直播稿：不是新事件本身
_COMMENTARY = _compile((
    r"\bwhy\b", r"\bhow\b", r"\bexplained\b", r"\banalysis\b", r"\bopinion\b",
    r"\bwhat to know\b", r"\bwhat it means\b", r"\blive\b", r"\bhere'?s\b",
    r"解析", r"分析", r"評論", r"懶人包", r"一次看",
))

# 別名 → 標準 token，讓中英文報導能被判成同一事件
_ALIASES: dict[str, str] = {
    "輝達": "nvidia", "黃仁勳": "huang", "jensen": "huang", "台積電": "tsmc",
    "川普": "trump", "聯準會": "fed", "鮑爾": "powell", "台灣": "taiwan",
    "關稅": "tariff", "tariffs": "tariff", "中國": "china", "習近平": "xi",
    "馬斯克": "musk", "普丁": "putin", "蘋果": "apple", "特斯拉": "tesla",
    "微軟": "microsoft", "聯發科": "mediatek", "鴻海": "foxconn",
    "加拿大": "canada", "墨西哥": "mexico", "日本": "japan", "韓國": "korea",
    "俄羅斯": "russia", "伊朗": "iran", "以色列": "israel", "烏克蘭": "ukraine",
    "印度": "india", "歐盟": "europe", "越南": "vietnam",
}
# 短詞（不足 4 個字母）無法被一般的英文詞規則抓到，另外處理
_SHORT_TOKENS = {"fed": "fed", "eu": "europe", "xi": "xi", "uk": "britain"}
# 方向詞：兩則報導的方向詞完全不同（降息 vs 維持不變）就不是同一事件
_DIRECTION_GROUPS = (
    frozenset({"cut", "cuts", "lowers", "lowered", "slashes", "降息", "下調"}),
    frozenset({"hike", "hikes", "raises", "raised", "升息", "上調"}),
    frozenset({"hold", "holds", "steady", "unchanged", "keeps", "維持不變", "按兵不動"}),
    frozenset({"ceasefire", "truce", "停火", "休戰"}),
    frozenset({"strike", "strikes", "attack", "attacks", "空襲", "攻擊"}),
)
# 地名：兩則報導各自提到的地名完全不同，就不是同一事件（加拿大關稅 vs 墨西哥關稅）
_PLACE_TOKENS = frozenset({
    "canada", "mexico", "japan", "korea", "russia", "iran", "israel", "ukraine",
    "india", "europe", "vietnam", "china", "taiwan", "germany", "france", "brazil",
    "turkey", "venezuela", "gaza", "syria", "pakistan", "australia", "britain",
})
_ENTITY_TOKENS = frozenset({
    "nvidia", "huang", "tsmc", "trump", "fed", "powell", "taiwan", "china", "xi",
    "musk", "putin", "apple", "tesla", "microsoft", "mediatek", "foxconn", "openai",
    "alphabet", "google", "amazon", "meta", "samsung", "intel", "broadcom", "europe",
})
_STOPWORDS = frozenset({
    "this", "that", "with", "from", "have", "will", "after", "about", "over", "into",
    "says", "said", "amid", "more", "than", "what", "when", "your", "their", "market",
    "stock", "stocks", "today", "live", "news", "update", "updates",
    "announces", "announced", "announce", "reports", "report", "reportedly", "just",
    "breaking", "says", "following", "could", "would", "should", "first", "new",
})

_FED_MARKET_MOVING = re.compile(
    r"fomc statement|implementation note|minutes of the federal open market|"
    r"economic projections|discount rate|emergency|rate (?:cut|hike)|"
    r"balance sheet|asset purchases|swap lines?",
    re.IGNORECASE,
)

_SOURCE_TRAITS: dict[str, SourceTraits] = {
    "fed_monetary": SourceTraits(base_score=3, push_title_pattern=_FED_MARKET_MOVING),
    "trump_truth": SourceTraits(base_score=3),
}
_DEFAULT_TRAITS = SourceTraits(base_score=1)
_AGGREGATOR_TRAITS = SourceTraits(base_score=0, corroborates=False)


def source_traits(source_key: str) -> SourceTraits:
    if source_key in _SOURCE_TRAITS:
        return _SOURCE_TRAITS[source_key]
    if source_key.startswith("google_news"):
        return _AGGREGATOR_TRAITS
    return _DEFAULT_TRAITS


def publisher(source_key: str) -> str:
    """同一家媒體的多個 feed（cnbc_top / cnbc_finance）只算一家印證。"""
    return source_key.split("_", 1)[0]


def build_tokens(text: str) -> frozenset[str]:
    """事件比對用的 token：別名正規化後的實體 + 英文關鍵詞 + 數字。"""
    lowered = text.lower()
    tokens: set[str] = set()
    for alias, canonical in _ALIASES.items():
        if alias in lowered:
            tokens.add(canonical)
    for index, group in enumerate(_DIRECTION_GROUPS):
        if any(word in lowered for word in group if not word.isascii()) or any(
            re.search(rf"\b{word}\b", lowered) for word in group if word.isascii()
        ):
            tokens.add(f"dir:{index}")
    for word in re.findall(r"[a-z][a-z'\-]{3,}", lowered):
        word = word.strip("'-")
        word = _ALIASES.get(word, word)
        if word not in _STOPWORDS and len(word) >= 4:
            tokens.add(word)
    for word in re.findall(r"\b[a-z]{2,3}\b", lowered):
        if word in _SHORT_TOKENS:
            tokens.add(_SHORT_TOKENS[word])
    for number in re.findall(r"\d+(?:\.\d+)?%?", lowered):
        if len(number) >= 2:
            tokens.add(number)
    return frozenset(tokens)


def _leadership_hit(text: str) -> bool:
    return bool(
        _LEADERSHIP_ACTION.search(text)
        and _MEGA_ENTITIES.search(text)
        and (_LEADERSHIP_ROLE.search(text) or _LEADERSHIP_PERSON.search(text))
    )


def score_item(item: NewsItem) -> Scored:
    # 只用標題：RSS 摘要常夾帶不相干字眼（例如指數行情稿提到 Fed），會大量誤判
    text = item.title[:TITLE_SCORING_LIMIT]
    traits = source_traits(item.source_key)

    categories: list[str] = []
    reasons: list[str] = []
    weights: list[int] = []
    triggered = False

    for category, pattern in _COMPILED_CATEGORIES:
        match = pattern.search(text)
        if not match:
            continue
        categories.append(category.name)
        weights.append(category.weight)
        triggered = triggered or category.trigger
        reasons.append(f"{category.name}:{match.group(0).lower()}")

    if _leadership_hit(text):
        categories.append("leadership")
        weights.append(3)
        triggered = True
        reasons.append("leadership:key-person-change")

    if "taiwan" in categories and "war" in categories:
        categories.append("taiwan_conflict")
        weights.append(3)
        reasons.append("taiwan_conflict:taiwan+war")

    # 遞減計分：最重的類別全額計入，其餘每個 +1（最多 +2），
    # 避免「利率 + 通膨」這種例行財經稿因多類別疊加就過門檻
    category_total = 0
    if weights:
        category_total = max(weights) + min(EXTRA_CATEGORY_BONUS_CAP, len(weights) - 1)

    score = traits.base_score + category_total
    if _NOISE.search(text):
        score -= NOISE_PENALTY
        reasons.append("noise:routine-announcement")
    if _COMMENTARY.search(text):
        score -= COMMENTARY_PENALTY
        reasons.append("commentary:explainer-or-live")
    if traits.push_title_pattern and not traits.push_title_pattern.search(text):
        score = min(score, PUSH_THRESHOLD - 1)
        reasons.append("capped:not-market-moving-title")

    return Scored(
        tier=_tier(score, triggered, traits.require_trigger_hit),
        score=score,
        categories=tuple(categories),
        reasons=tuple(reasons),
        tokens=build_tokens(text) | frozenset(f"cat:{name}" for name in categories),
        triggered=triggered,
    )


def _tier(score: int, triggered: bool, require_trigger: bool = True) -> str:
    if require_trigger and not triggered:
        return TIER_IGNORE
    if score >= PUSH_THRESHOLD:
        return TIER_PUSH
    if score >= WATCH_THRESHOLD:
        return TIER_WATCH
    return TIER_IGNORE


def tier_for_score(score: int, triggered: bool) -> str:
    """corroboration 加分後重新判定 tier。"""
    return _tier(score, triggered)


def same_event(left: frozenset[str], right: frozenset[str]) -> bool:
    """兩則報導是否為同一事件。

    - 地名各自出現且完全不重疊 → 不同事件；
    - 至少共享 2 個非類別 token（其中含 1 個實體）且共享 1 個類別 → 同一事件（可跨中英文）；
    - 或整體 Jaccard ≥ 0.5。
    """
    if not left or not right:
        return False
    left_places, right_places = left & _PLACE_TOKENS, right & _PLACE_TOKENS
    if left_places and right_places and left_places.isdisjoint(right_places):
        return False
    left_dirs = {t for t in left if t.startswith("dir:")}
    right_dirs = {t for t in right if t.startswith("dir:")}
    if left_dirs and right_dirs and left_dirs.isdisjoint(right_dirs):
        return False
    shared = left & right
    informative = {
        token for token in shared if not token.startswith(("cat:", "dir:"))
    }
    cat_shared = {token for token in shared if token.startswith("cat:")}
    if len(informative) >= 2 and (informative & _ENTITY_TOKENS) and cat_shared:
        return True
    union = left | right
    return bool(union) and len(shared) / len(union) >= 0.5


def excerpt(text: str, limit: int = 140) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"
