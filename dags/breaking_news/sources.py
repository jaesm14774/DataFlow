"""重磅消息來源：官方、通訊社與社群貼文的 RSS 設定、抓取與解析。

這些 feed 在 2026-11 開發時實測過；Google News 會限流（曾回 HTTPError），
trumpstruth.org 是非官方存檔站，其延遲未實測，可由帳本的
first_seen_at − published_at 事後量測。解析函式是純函式，
抓取失敗只丟出例外給呼叫端記錄，不會中斷其他來源。
"""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable
from urllib.parse import quote_plus

import requests

from breaking_news.scoring import NewsItem

USER_AGENT = "Mozilla/5.0 (compatible; DataFlowBreakingNews/1.0)"
FETCH_TIMEOUT_SECONDS = 10
SUMMARY_LIMIT = 400


@dataclass(frozen=True)
class SourceSpec:
    key: str
    label: str
    url: str
    poll_every_minutes: int = 5


def _google_news(query: str, *, hl: str, gl: str, ceid: str) -> str:
    return (
        f"https://news.google.com/rss/search?q={quote_plus(query)}"
        f"&hl={hl}&gl={gl}&ceid={ceid}"
    )


SOURCES: tuple[SourceSpec, ...] = (
    # 官方：只用貨幣政策類（FOMC 聲明/會議紀要）；press_all 多為銀行執法公告，雜訊太多
    SourceSpec("fed_monetary", "Fed 貨幣政策", "https://www.federalreserve.gov/feeds/press_monetary.xml"),
    # 美國總統貼文（Truth Social 非官方存檔站；延遲未實測）
    SourceSpec("trump_truth", "川普 Truth Social", "https://www.trumpstruth.org/feed"),
    # 台灣在地
    SourceSpec("cna_finance", "中央社財經", "https://feeds.feedburner.com/rsscna/finance"),
    # 國際通訊/財經媒體
    SourceSpec("wsj_markets", "WSJ Markets", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain"),
    SourceSpec("cnbc_top", "CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    SourceSpec("cnbc_finance", "CNBC Finance", "https://www.cnbc.com/id/10000664/device/rss/rss.html"),
    SourceSpec("bbc_world", "BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    SourceSpec("bbc_business", "BBC Business", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    SourceSpec("aljazeera", "Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    # 聚合：Google News 容易限流，只用 2 個查詢，每 15 分鐘一次
    SourceSpec(
        "google_news_en", "Google News (EN)",
        _google_news(
            'tariff OR sanctions OR "Federal Reserve" OR "Taiwan Strait" OR "circuit breaker" when:1h',
            hl="en-US", gl="US", ceid="US:en",
        ),
        poll_every_minutes=15,
    ),
    SourceSpec(
        "google_news_zh", "Google News (中文)",
        _google_news("台股 OR 關稅 OR 台海 OR 聯準會 OR 熔斷 when:1h", hl="zh-TW", gl="TW", ceid="TW:zh-Hant"),
        poll_every_minutes=15,
    ),
)

SOURCES_BY_KEY = {spec.key: spec for spec in SOURCES}


def is_due(spec: SourceSpec, now_utc: datetime) -> bool:
    """無狀態的輪詢節奏：以 5 分鐘為一格，慢速來源每 N 格才抓一次。"""
    slot = (now_utc.hour * 60 + now_utc.minute) // 5
    every = max(1, spec.poll_every_minutes // 5)
    return slot % every == 0


def _clean(text: str | None) -> str:
    if not text:
        return ""
    without_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def parse_pub_date(raw: str | None) -> datetime | None:
    """RFC822 日期 → UTC naive datetime；解析不了回傳 None。"""
    if not raw or not raw.strip():
        return None
    try:
        parsed = parsedate_to_datetime(raw.strip())
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def parse_feed(xml_text: str | bytes, source_key: str) -> list[NewsItem]:
    """解析 RSS 2.0。缺標題、缺日期或缺識別的項目直接略過。"""
    root = ET.fromstring(xml_text)
    items: list[NewsItem] = []
    for node in root.iter("item"):
        title = _clean(node.findtext("title"))
        link = _clean(node.findtext("link"))
        guid = _clean(node.findtext("guid")) or link
        published = parse_pub_date(node.findtext("pubDate"))
        if not title or not guid or published is None:
            continue
        items.append(
            NewsItem(
                source_key=source_key,
                guid=guid,
                title=title,
                url=link,
                published_at=published,
                summary=_clean(node.findtext("description"))[:SUMMARY_LIMIT],
            )
        )
    return items


def fetch_source(spec: SourceSpec, session: requests.Session | None = None) -> list[NewsItem]:
    http = session or requests
    response = http.get(
        spec.url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/xml, text/xml"},
        timeout=FETCH_TIMEOUT_SECONDS,
        allow_redirects=True,
    )
    response.raise_for_status()
    return parse_feed(response.content, spec.key)


def fetch_all(
    specs: Iterable[SourceSpec], now_utc: datetime
) -> tuple[list[NewsItem], dict[str, str], list[str]]:
    """逐一抓取到期的來源。回傳 (items, {來源: 錯誤型別}, 本輪嘗試過的來源)。

    單一來源失敗只記錄、不拋出；是否整體失敗交由呼叫端判斷。
    """
    items: list[NewsItem] = []
    errors: dict[str, str] = {}
    attempted: list[str] = []
    with requests.Session() as session:
        for spec in specs:
            if not is_due(spec, now_utc):
                continue
            attempted.append(spec.key)
            try:
                items.extend(fetch_source(spec, session))
            except (requests.RequestException, ET.ParseError) as exc:
                errors[spec.key] = type(exc).__name__
                print(f"[breaking_news] 來源 {spec.key} 抓取失敗：{type(exc).__name__}")
    return items, errors, attempted
