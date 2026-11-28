"""Research-only signal families and their validation statistics.

Nothing here votes in the production notification policy. The module has two
jobs:

1. ``FAMILIES`` is a pre-registered table (data source, hypothesised sign,
   point-in-time rule). The sign is fixed *before* results are looked at, so a
   family that works in the opposite direction shows up as a negative IC
   instead of being silently flipped.
2. Pure, dependency-light statistics used by both the historical study
   and the forward shadow evaluation in ``backtest_diagnostics``: rank IC, decile legs, a within-date
   shuffle control, Benjamini-Hochberg adjustment and a family verdict.

Ideas adopted from the three reviewed projects:

* Vibe-Trading ``bench_runner_strict`` / ``multipletesting``: random-control IC
  by shuffling scores inside each date, non-overlapping forward-return
  sampling for horizons > 1, Benjamini-Hochberg across every family x horizon.
* freqtrade ``lookahead-analysis``: the historical study has a truncation test
  that recomputes features from data cut at a date and diffs them.
* TradingAgents ``memory/settlement``: enter at the first close after the
  signal is publishable and settle only after the full window has traded.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import math
import re
from typing import Any, Iterable, Mapping, Sequence

RESEARCH_POLICY_VERSION = "research-families-2026-11-28-v4"
RESEARCH_FAMILY_PREFIX = "research:"

# Taiwan cash-equity round trip: 0.1425% commission each way (no broker
# discount assumed) plus the 0.3% securities transaction tax on the sell leg.
ROUND_TRIP_COST_PCT = 0.1425 * 2 + 0.3
DEFAULT_QUANTILE = 0.10
MIN_CROSS_SECTION = 200


@dataclasses.dataclass(frozen=True)
class FamilySpec:
    """One pre-registered hypothesis.

    ``sign`` is +1 when a high raw score is hypothesised to be bullish and -1
    when a high raw score is hypothesised to be bearish. ``publish_lag`` states
    when the data is actually known; the study enters one session later.
    """

    name: str
    source: str
    sign: int
    hypothesis: str
    publish_lag: str
    horizons: tuple[int, ...] = (1, 5, 10, 20)
    forward_scoring: bool = False


FAMILIES: tuple[FamilySpec, ...] = (
    FamilySpec(
        "foreign_flow_5d", "三大法人買賣", +1,
        "外資 5 日淨買超占 20 日均量，買超延續",
        "T 日盤後公布",
    ),
    FamilySpec(
        "foreign_flow_20d", "三大法人買賣", +1,
        "外資 20 日淨買超占 20 日均量，較慢的累積部位",
        "T 日盤後公布", horizons=(5, 10, 20, 60),
    ),
    FamilySpec(
        "trust_flow_5d", "三大法人買賣", +1,
        "投信 5 日淨買超占 20 日均量；投信偏小型股、有作帳與認養行為",
        "T 日盤後公布",
    ),
    FamilySpec(
        "dealer_flow_5d", "三大法人買賣", +1,
        "自營商 5 日淨買超占 20 日均量；含避險部位，預期較弱",
        "T 日盤後公布",
    ),
    FamilySpec(
        "margin_balance_change_5d", "融資融券", -1,
        "融資餘額 5 日增幅大者籌碼偏散戶槓桿，之後相對落後",
        "T 日盤後公布", forward_scoring=True,
    ),
    FamilySpec(
        "margin_utilization", "融資融券", -1,
        "融資使用率高者追價槓桿擁擠",
        "T 日盤後公布", forward_scoring=True,
    ),
    FamilySpec(
        "short_balance_change_5d", "融資融券", -1,
        "融券餘額 5 日增幅大者代表放空壓力上升",
        "T 日盤後公布", forward_scoring=True,
    ),
    FamilySpec(
        "short_to_margin_ratio", "融資融券", +1,
        "券資比高者有軋空動能",
        "T 日盤後公布", forward_scoring=True,
    ),
    FamilySpec(
        "daytrade_heat_5d", "當沖資訊", -1,
        "當沖成交量占比 5 日均值高者投機過熱，之後相對落後",
        "T 日盤後公布",
    ),
    FamilySpec(
        "tdcc_large_holder_change_4w", "大戶比例", +1,
        ">400 張大股東持股比 4 週變化，籌碼集中",
        "資料日週五，保守延後 4 個日曆日", horizons=(5, 10, 20, 60),
    ),
    FamilySpec(
        "tdcc_holder_count_change_4w", "大戶比例", -1,
        "總股東人數 4 週增幅，籌碼分散",
        "資料日週五，保守延後 4 個日曆日", horizons=(5, 10, 20, 60),
    ),
    FamilySpec(
        "revenue_yoy", "月營收資訊", +1,
        "月營收年增率橫斷面排名",
        "次月 10 日法定期限後一個交易日", horizons=(5, 10, 20, 60),
        forward_scoring=True,
    ),
    FamilySpec(
        "revenue_yoy_acceleration", "月營收資訊", +1,
        "月營收年增率較上月年增率的加速度",
        "次月 10 日法定期限後一個交易日", horizons=(5, 10, 20, 60),
        forward_scoring=True,
    ),
    FamilySpec(
        "earnings_yield", "個股基本指標", +1,
        "盈利殖利率（100/本益比，僅本益比>0）高者價值面便宜，長期相對較強",
        "T 日盤後公布（BWIBBU/peQry）", horizons=(5, 10, 20, 60), forward_scoring=True,
    ),
    FamilySpec(
        "book_to_market", "個股基本指標", +1,
        "淨值市價比（1/股價淨值比，僅 PB>0）高者價值面便宜",
        "T 日盤後公布（BWIBBU/peQry）", horizons=(5, 10, 20, 60), forward_scoring=True,
    ),
    FamilySpec(
        "dividend_yield", "個股基本指標", +1,
        "現金殖利率高者具評價支撐；僅比較有配息（>0）者，未配息視為未評分",
        "T 日盤後公布（BWIBBU/peQry）", horizons=(5, 10, 20, 60), forward_scoring=True,
    ),
    FamilySpec(
        "short_term_reversal_5d", "個股成交資訊", -1,
        "5 日報酬反轉（Vibe-Trading strev）",
        "T 日收盤", forward_scoring=True,
    ),
    FamilySpec(
        "high_52w_proximity", "個股成交資訊", +1,
        "收盤相對 252 日高點的距離（Vibe-Trading high52w）",
        "T 日收盤", horizons=(5, 10, 20, 60),
    ),
    FamilySpec(
        "amihud_illiquidity_20d", "個股成交資訊", +1,
        "Amihud 非流動性溢酬（Vibe-Trading illiq）",
        "T 日收盤", horizons=(5, 10, 20, 60),
    ),
    FamilySpec(
        "volatility_20d", "個股成交資訊", -1,
        "20 日波動度高者（低波動異常現象）",
        "T 日收盤", horizons=(5, 10, 20, 60), forward_scoring=True,
    ),
    FamilySpec(
        "tdcc_holder_count_change_4w_resid20", "大戶比例", -1,
        "總股東人數 4 週增幅，扣除近 20 日報酬（股價下跌後散戶人數上升的機械關聯）",
        "資料日週五，保守延後 4 個日曆日", horizons=(5, 10, 20, 60),
    ),
    FamilySpec(
        "prod_institutions_rule", "正式規則重現：三大法人", +1,
        "1/3/5 日淨買超強度 10/5/5%、持續性、外資+投信同向（equity_order_flow 常數）",
        "T 日盤後公布", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "prod_price_rule", "正式規則重現：價格", +1,
        "5 日報酬 |r|≥8% 且與同日母體平均超額同向（以母體平均代替加權指數）",
        "T 日收盤", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "prod_volume_rule", "正式規則重現：量能", +1,
        "成交量 ≥ 前 20 日均量 2 倍且當日 |漲跌|≥3%，方向依漲跌",
        "T 日收盤", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "prod_two_of_three_agree", "正式規則重現：價/量/法人", +1,
        "上述三條正式規則至少兩條同向且無反向",
        "T 日盤後公布", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "news_attention_shock_5d", "news_ch.news", +1,
        "個股新聞標題 5 日則數相對過去 60 日均值的衝擊（新聞驅動的延續）",
        "發布後的下一個收盤（13:30 後算次一交易日）", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "news_polarity_5d", "news_ch.news", +1,
        "個股新聞標題 5 日多空詞彙極性（預先登記的詞表）",
        "發布後的下一個收盤（13:30 後算次一交易日）", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "ptt_attention_shock_5d", "ptt_article", -1,
        "PTT 股版標題提及的 5 日則數衝擊（散戶關注過熱，之後相對落後）",
        "發文後的下一個收盤（13:30 後算次一交易日）", horizons=(1, 5, 10, 20),
    ),
    FamilySpec(
        "ptt_label_event", "ptt_article", +1,
        "PTT 股版 [標的] 文章分類為多/空的事件（散戶情緒）；符號 +1 代表跟隨文章方向",
        "發文後的下一個收盤", horizons=(1, 5, 10, 20),
    ),
)

FAMILY_BY_NAME = {spec.name: spec for spec in FAMILIES}
EVENT_FAMILIES = frozenset({
    "prod_institutions_rule", "prod_price_rule", "prod_volume_rule",
    "prod_two_of_three_agree", "ptt_label_event",
    "news_attention_shock_5d", "news_polarity_5d", "ptt_attention_shock_5d",
})


def research_family_key(name: str) -> str:
    return name if name.startswith(RESEARCH_FAMILY_PREFIX) else RESEARCH_FAMILY_PREFIX + name


# ---------------------------------------------------------------------------
# News-title text signals (shared by the historical study and the daily scan)
# ---------------------------------------------------------------------------

# Pre-registered title lexicon; fixed before looking at any result.
BULL_WORDS = (
    "利多", "大漲", "漲停", "創高", "創新高", "看好", "調升", "上修", "營收創", "獲利創",
    "轉盈", "搶單", "大單", "買超", "喊進", "翻紅", "受惠", "擴產",
)
BEAR_WORDS = (
    "利空", "重挫", "大跌", "跌停", "下修", "調降", "虧損", "轉虧", "看空", "裁員",
    "衰退", "警示", "處置", "掏空", "違約", "下市", "賣超", "示警", "崩", "新低",
)
NEWS_POLARITY_FAMILY = "news_polarity_5d"
NEWS_POLARITY_MIN_LABELLED = 2
NEWS_POLARITY_MIN_ABS = 0.5
NEWS_WINDOW_SESSIONS = 5
_CODE_IN_TITLE = re.compile(r"[\(（](\d{4})[\)）]|(?<!\d)(\d{4})(?![\d年月日點元%])")


def title_polarity_flags(title: str) -> tuple[bool, bool]:
    """(bullish, bearish) once; a word negated by a leading 不 does not count."""

    text = str(title or "")
    bull = any(word in text and ("不" + word) not in text for word in BULL_WORDS)
    bear = any(word in text and ("不" + word) not in text for word in BEAR_WORDS)
    return bull and not bear, bear and not bull


def build_name_trie(pairs: Iterable[tuple[Any, Any]]) -> tuple[dict, set[str]]:
    """Trie of unique security names (>=3 characters) plus the set of listed codes."""

    names: dict[str, set[str]] = {}
    codes: set[str] = set()
    for code, name in pairs:
        code = str(code or "").strip()
        if not re.fullmatch(r"[1-9]\d{3}", code):
            continue
        codes.add(code)
        text = str(name or "").strip()
        if len(text) >= 3:  # 2-character names collide with ordinary words
            names.setdefault(text, set()).add(code)
    trie: dict = {}
    for text, owners in names.items():
        if len(owners) != 1:
            continue
        node = trie
        for char in text:
            node = node.setdefault(char, {})
        node["$"] = next(iter(owners))
    return trie, codes


def match_codes(title: str, trie: dict, codes: set[str], *, bare_codes: bool) -> set[str]:
    found: set[str] = set()
    length = len(title)
    for start in range(length):
        node = trie.get(title[start])
        if node is None:
            continue
        position = start + 1
        while True:
            if "$" in node:
                found.add(node["$"])
            if position >= length:
                break
            node = node.get(title[position])
            if node is None:
                break
            position += 1
    for match in _CODE_IN_TITLE.finditer(title):
        code = match.group(1) or (match.group(2) if bare_codes else None)
        if code in codes:
            found.add(code)
    return found


def after_cutoff(created_at: dt.datetime) -> bool:
    """Text published after the 13:30 close belongs to the next session.

    A date-only timestamp (exactly midnight) carries no publication time, so it is
    treated conservatively as published after the close.
    """

    clock = (created_at.hour, created_at.minute, created_at.second, created_at.microsecond)
    return clock == (0, 0, 0, 0) or clock > (13, 30, 0, 0)


def news_window_start(sessions: Sequence[dt.date]) -> dt.date | None:
    """Lowest publication date that can still map into the five-session window.

    One session before the window is included: a title published after that
    session's close (or on the weekend after it) belongs to the first window session.
    """

    full = sorted(set(sessions))
    if len(full) < NEWS_WINDOW_SESSIONS:
        return None
    if len(full) > NEWS_WINDOW_SESSIONS:
        return full[-NEWS_WINDOW_SESSIONS - 1]
    return full[0] - dt.timedelta(days=4)


def news_polarity_by_code(
    titles: Iterable[tuple[Any, Any]], sessions: Sequence[dt.date],
    trie: dict, codes: set[str],
) -> dict[str, dict[str, Any]]:
    """Per-stock 5-session title counts and polarity, point-in-time safe.

    `sessions` are the last trading sessions up to the as-of date. Each title is
    assigned to the first session on or after its publication day (the day after
    when published after 13:30); titles assigned beyond the last session are
    dropped, so nothing published after the as-of close can leak in.
    """

    full = sorted(set(sessions))
    ordered = full[-NEWS_WINDOW_SESSIONS:]
    if len(ordered) < NEWS_WINDOW_SESSIONS:
        return {}
    window = set(ordered)
    # a title published on or before the session preceding the window is too old
    previous_session = full[-NEWS_WINDOW_SESSIONS - 1] if len(full) > NEWS_WINDOW_SESSIONS else None
    stats: dict[str, dict[str, Any]] = {}
    seen: set[tuple[str, str]] = set()  # syndicated copies of one headline count once
    for title, created_at in titles:
        if not title or not isinstance(created_at, dt.datetime):
            continue
        day = created_at.date() + dt.timedelta(days=int(after_cutoff(created_at)))
        if previous_session is not None:
            if day <= previous_session:
                continue
        elif day < ordered[0] - dt.timedelta(days=4):
            continue  # only five sessions known: allow just the weekend/holiday run-up
        assigned = next((session for session in ordered if session >= day), None)
        if assigned is None or assigned not in window:
            continue
        matched = match_codes(str(title), trie, codes, bare_codes=False)
        if not matched:
            continue
        bull, bear = title_polarity_flags(str(title))
        key_title = "".join(str(title).split())
        for code in matched:
            if (code, key_title) in seen:
                continue
            seen.add((code, key_title))
            row = stats.setdefault(code, {"news_n": 0, "bull": 0, "bear": 0})
            row["news_n"] += 1
            row["bull"] += int(bull)
            row["bear"] += int(bear)
    for row in stats.values():
        labelled = row["bull"] + row["bear"]
        row["labelled"] = labelled
        row["polarity"] = round((row["bull"] - row["bear"]) / (labelled + 1), 4)
    return stats


def news_polarity_direction(row: Mapping[str, Any]) -> int:
    """Event direction with the same fixed thresholds as the historical study."""

    if row.get("news_n", 0) < NEWS_POLARITY_MIN_LABELLED or row.get("labelled", 0) < NEWS_POLARITY_MIN_LABELLED:
        return 0
    polarity = float(row.get("polarity") or 0.0)
    if abs(polarity) < NEWS_POLARITY_MIN_ABS:
        return 0
    return 1 if polarity > 0 else -1


# ---------------------------------------------------------------------------
# Cross-sectional direction assignment (forward shadow evaluation)
# ---------------------------------------------------------------------------

def cross_sectional_directions(
    scores: Mapping[str, float | None],
    *,
    sign: int,
    quantile: float = DEFAULT_QUANTILE,
    min_names: int = MIN_CROSS_SECTION,
) -> dict[str, int]:
    """Top/bottom ``quantile`` of the hypothesis-signed score get +1/-1.

    Returns an empty mapping when the universe is too small, so a thin or
    partially collected day never fabricates directions. A tie group that
    straddles a quantile boundary is left neutral as a whole instead of being
    split arbitrarily.
    """

    items = [
        (code, float(value) * sign) for code, value in scores.items()
        if value is not None and isinstance(value, (int, float))
        and not isinstance(value, bool) and math.isfinite(float(value))
    ]
    count = len(items)
    if count < min_names:
        return {}
    items.sort(key=lambda item: item[1])
    low_cut = count * quantile
    high_cut = count * (1 - quantile)
    directions: dict[str, int] = {}
    position = 0
    while position < count:
        end = position
        while end + 1 < count and items[end + 1][1] == items[position][1]:
            end += 1
        first_rank, last_rank = position + 1, end + 1
        if last_rank <= low_cut:
            direction = -1
        elif first_rank > high_cut:
            direction = 1
        else:
            direction = 0
        if direction:
            for code, _ in items[position:end + 1]:
                directions[code] = direction
        position = end + 1
    return directions


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _realised_volatility(closes: Sequence[Any] | None) -> float | None:
    values = [_finite(value) for value in (closes or [])]
    if len(values) < 16 or any(value is None or value <= 0 for value in values):
        return None
    returns = [values[i] / values[i - 1] - 1 for i in range(1, len(values))]
    mean = sum(returns) / len(returns)
    variance = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    return math.sqrt(variance)


def collect_forward_scores(
    positioning_context: Mapping[str, Any] | None,
    revenue_context: Mapping[str, Any] | None,
    *,
    return_5d_pct: Any = None,
    closes: Sequence[Any] | None = None,
    valuation_context: Mapping[str, Any] | None = None,
) -> dict[str, float]:
    """Raw research scores derivable from the daily scan's existing context.

    Only the ``forward_scoring`` families are produced here; flow, day-trade,
    TDCC and the remaining price-structure families need panels the daily loop
    does not hold and are covered by the historical study instead. Returns and
    closes must already be corporate-action guarded by the caller.
    """

    position = positioning_context or {}
    scores: dict[str, float] = {}
    margin_change = _finite(position.get("margin_balance_change_5d_pct"))
    if margin_change is not None:
        scores["margin_balance_change_5d"] = margin_change
    short_change = _finite(position.get("short_balance_change_5d_pct"))
    if short_change is not None:
        scores["short_balance_change_5d"] = short_change
    utilization = _finite(position.get("margin_utilization_pct"))
    if utilization is not None:
        scores["margin_utilization"] = utilization
    margin_balance = _finite(position.get("margin_balance"))
    short_balance = _finite(position.get("short_balance"))
    if margin_balance and margin_balance > 0 and short_balance is not None:
        scores["short_to_margin_ratio"] = short_balance / margin_balance * 100
    revenue_yoy = _finite((revenue_context or {}).get("revenue_yoy_pct"))
    if revenue_yoy is not None:
        scores["revenue_yoy"] = revenue_yoy
    prior_yoy = _finite((revenue_context or {}).get("revenue_yoy_prior_month_pct"))
    if revenue_yoy is not None and prior_yoy is not None:
        scores["revenue_yoy_acceleration"] = revenue_yoy - prior_yoy
    valuation = valuation_context or {}
    pe_ratio = _finite(valuation.get("pe_ratio"))
    if pe_ratio is not None and pe_ratio > 0:
        scores["earnings_yield"] = 100.0 / pe_ratio
    price_to_book = _finite(valuation.get("price_to_book"))
    if price_to_book is not None and price_to_book > 0:
        scores["book_to_market"] = 1.0 / price_to_book
    dividend_yield = _finite(valuation.get("dividend_yield_pct"))
    # Non-payers (0) would form one giant tie block that straddles the tail
    # cut and neutralises the whole bearish side, so they stay unscored.
    if dividend_yield is not None and dividend_yield > 0:
        scores["dividend_yield"] = dividend_yield
    five_day = _finite(return_5d_pct)
    if five_day is not None:
        scores["short_term_reversal_5d"] = five_day
    volatility = _realised_volatility(closes)
    if volatility is not None:
        scores["volatility_20d"] = volatility
    return scores


def assign_forward_directions(
    scores_by_code: Mapping[str, Mapping[str, float]],
    market_by_code: Mapping[str, str] | None = None,
) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    """Decile directions per family, assigned within each market.

    Returns ``(directions, scored_counts)``. ``directions`` is keyed by stock
    code then family key. ``scored_counts`` maps family key to ``{market:
    names_with_a_score}`` so a market whose source is stale or quarantined
    shows up as missing coverage instead of silently turning a family into a
    one-market signal. A market with fewer than ``MIN_CROSS_SECTION`` scored
    names gets no directions for that family.
    """

    result: dict[str, dict[str, int]] = {}
    counts: dict[str, dict[str, int]] = {}
    for spec in FAMILIES:
        if not spec.forward_scoring:
            continue
        key = research_family_key(spec.name)
        by_market: dict[str, dict[str, float | None]] = {}
        for code, scores in scores_by_code.items():
            value = scores.get(spec.name)
            if value is None:
                continue
            market = (market_by_code or {}).get(code) or "unknown"
            by_market.setdefault(market, {})[code] = value
        for market, family_scores in by_market.items():
            counts.setdefault(key, {})[market] = len(family_scores)
            for code, direction in cross_sectional_directions(
                family_scores, sign=spec.sign,
            ).items():
                result.setdefault(code, {})[key] = direction
    return result, counts


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def normal_two_sided_p(t_stat: float | None) -> float | None:
    if t_stat is None or not math.isfinite(t_stat):
        return None
    return math.erfc(abs(t_stat) / math.sqrt(2))


def _betacf(a: float, b: float, x: float) -> float:
    tiny = 1e-30
    qab, qap, qam = a + b, a + 1, a - 1
    c, d = 1.0, 1 - qab * x / qap
    d = 1 / (d if abs(d) > tiny else tiny)
    h = d
    for m in range(1, 300):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > tiny else tiny)
        c = 1 + aa / c
        c = c if abs(c) > tiny else tiny
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > tiny else tiny)
        c = 1 + aa / c
        c = c if abs(c) > tiny else tiny
        delta = d * c
        h *= delta
        if abs(delta - 1) < 3e-12:
            break
    return h


def t_two_sided_p(t_stat: float | None, df: int) -> float | None:
    """Student-t two-sided p-value via the regularised incomplete beta."""

    if t_stat is None or not math.isfinite(t_stat) or df < 1:
        return None
    x = df / (df + t_stat * t_stat)
    a, b = df / 2, 0.5
    front = math.exp(
        math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
        + a * math.log(x) + b * math.log(1 - x)
    ) if 0 < x < 1 else 0.0
    if x < (a + 1) / (a + b + 2):
        return min(1.0, front * _betacf(a, b, x) / a)
    return min(1.0, 1 - front * _betacf(b, a, 1 - x) / b)


def mean_t_stat(values: Iterable[float]) -> dict[str, Any]:
    data = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    count = len(data)
    if count < 2:
        return {"n": count, "mean": data[0] if data else None, "t": None, "p": None}
    mean = sum(data) / count
    variance = sum((value - mean) ** 2 for value in data) / (count - 1)
    if variance <= 0:
        return {"n": count, "mean": mean, "t": None, "p": None}
    t_stat = mean / math.sqrt(variance / count)
    return {"n": count, "mean": mean, "t": t_stat, "p": t_two_sided_p(t_stat, count - 1)}


def benjamini_hochberg(p_values: Sequence[float | None]) -> list[float | None]:
    """BH step-up adjusted q-values; ``None`` p-values stay ``None``."""

    indexed = [(p, i) for i, p in enumerate(p_values) if p is not None]
    indexed.sort()
    total = len(indexed)
    adjusted: list[float | None] = [None] * len(p_values)
    running = 1.0
    for rank in range(total, 0, -1):
        p, index = indexed[rank - 1]
        running = min(running, p * total / rank)
        adjusted[index] = running
    return adjusted


def bootstrap_mean_ci(
    values: Sequence[float], *, draws: int = 2000, seed: int = 7, level: float = 0.95,
) -> tuple[float, float] | None:
    import random

    data = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    if len(data) < 5:
        return None
    rng = random.Random(seed)
    count = len(data)
    means = sorted(
        sum(data[rng.randrange(count)] for _ in range(count)) / count
        for _ in range(draws)
    )
    low = means[int((1 - level) / 2 * draws)]
    high = means[min(draws - 1, int((1 + level) / 2 * draws))]
    return low, high


def family_verdict(
    *,
    discovery: Mapping[str, Any],
    holdout: Mapping[str, Any],
    q_value: float | None,
    shuffle_ok: bool,
    min_dates: int = 20,
    spread_discovery: Mapping[str, Any] | None = None,
    spread_holdout: Mapping[str, Any] | None = None,
) -> str:
    """Classify one family x horizon.

    ``confirmed``: BH q < 0.05 on the full sample, the discovery and holdout
    IC share a sign, holdout one-sided p < 0.05, the shuffle control stayed
    near zero, and (when given) the winsorised decile spread has the same sign
    as the IC in both periods. ``reversed`` is the same test in the opposite
    sign of the pre-registered hypothesis. ``rank_only_skew`` means the rank IC
    passes but the decile spread disagrees: the typical stock behaves as
    hypothesised while the mean is driven by a few right-tail winners (and, in
    an unadjusted survivor-only panel, by missing delisted losers), so an
    equal-weight trade would not capture it.
    """

    d_n, h_n = discovery.get("n") or 0, holdout.get("n") or 0
    if d_n < min_dates or h_n < min_dates:
        return "insufficient_dates"
    d_mean, h_mean = discovery.get("mean"), holdout.get("mean")
    d_p, h_p = discovery.get("p"), holdout.get("p")
    if None in (d_mean, h_mean, d_p, h_p):
        return "insufficient_dates"
    if not shuffle_ok:
        return "control_failed"
    same_sign = d_mean * h_mean > 0
    holdout_one_sided = h_p / 2 < 0.05
    if q_value is not None and q_value < 0.05 and same_sign and holdout_one_sided:
        if spread_discovery is not None or spread_holdout is not None:
            for spread in (spread_discovery, spread_holdout):
                mean = (spread or {}).get("mean")
                if mean is None or mean * h_mean <= 0:
                    return "rank_only_skew"
        return "confirmed" if h_mean > 0 else "reversed"
    if d_p < 0.05 and not same_sign:
        return "unstable_sign"
    if d_p < 0.05:
        return "discovery_only"
    return "noise"
