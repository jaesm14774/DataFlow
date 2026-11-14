"""Permission-gated discovery and collection for Bahamut game boards."""

from __future__ import annotations

import json
import hashlib
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import parse_qs, urljoin, urlsplit
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from game.knowledge_ingest import _get_database_connection, _now_utc, _upsert_record


SOURCE_KEY = "bahamut"
BASE_URL = "https://forum.gamer.com.tw"
USER_AGENT = "DataFlowPersonalCrawler/1.0"
MIN_REQUEST_INTERVAL_SECONDS = 5.0
MAX_REQUESTS_PER_RUN = 50
MAX_BOARD_INDEX_PAGES_PER_RUN = 40
MAX_THREAD_PAGES_PER_RUN = 10
SITEMAP_REFRESH_DAYS = 30
DIRECTORY_REFRESH_DAYS = 7
BOARD_ID_RE = re.compile(r"(?:[?&]bsn=)(\d+)")
THREAD_ID_RE = re.compile(r"(?:[?&]snA=)(\d+)")
RELEVANT_GUIDE_TERMS = ("攻略", "心得", "教學", "分析", "機制", "玩法")
GAME_DIRECTORY_ROOTS = {"手機遊戲", "PC", "TV 掌機"}
ROOT_NAVIGATION_LABELS = {
    "手機": ("手機遊戲",),
    "PC": ("PC",),
    "TV 掌機": ("TV 掌機",),
    "動漫畫": ("動漫板",),
    "宅生活": ("宅生活",),
    "站務": ("站務討論板",),
}
NON_GAME_BOARD_TITLE = re.compile(
    r"(?:綜合討論|主機平台|電腦應用|RPG製作大師|遊戲開發|遊戲製作)"
)


class CrawlBudgetExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class BoardCandidate:
    board_id: str
    board_url: str
    board_name: str | None = None
    game_title: str | None = None
    taxonomy_path: tuple[str, ...] = ()
    method: str = "sitemap"


@dataclass(frozen=True)
class ThreadIndexEntry:
    article_id: str
    article_url: str
    title: str
    article_type: str
    tags: tuple[str, ...]


@dataclass(frozen=True)
class ThreadPost:
    post_id: str
    post_number: int
    post_type: str
    body_html: str
    body_text: str
    media_refs: tuple[dict, ...]


def _normal_text(node) -> str:
    return " ".join(node.stripped_strings) if node else ""


def _safe_site_url(href: str, base: str = BASE_URL) -> str | None:
    absolute = urljoin(base, href)
    parts = urlsplit(absolute)
    try:
        port = parts.port
    except ValueError:
        return None
    if (
        parts.scheme != "https"
        or parts.hostname != "forum.gamer.com.tw"
        or parts.username is not None
        or parts.password is not None
        or port not in {None, 443}
    ):
        return None
    return absolute


def _taxonomy_path(anchor) -> tuple[str, ...]:
    category_box = anchor.find_parent("div", class_="BH-lbox")
    if not category_box or "FORUM-master_box1" not in category_box.get("class", []):
        return ()
    root_heading = category_box.find_previous_sibling("h4")
    group = anchor.parent
    group_heading = group.find("strong", recursive=False) if group else None
    return tuple(
        value for value in (_normal_text(root_heading), _normal_text(group_heading))
        if value
    )


def parse_guide_boards(
    html: str, taxonomy_path: tuple[str, ...] = ()
) -> list[BoardCandidate]:
    soup = BeautifulSoup(html, "lxml")
    candidates = {}
    for anchor in soup.select('a[href*="bsn="]'):
        href = _safe_site_url(anchor.get("href", ""))
        if not href or urlsplit(href).path.lower() != "/b.php":
            continue
        match = BOARD_ID_RE.search(href)
        if not match:
            continue
        board_id = match.group(1)
        name = _normal_text(anchor).removesuffix("哈啦板").strip() or None
        local_path = _taxonomy_path(anchor)
        candidate_path = taxonomy_path
        if taxonomy_path and len(local_path) > 1 and local_path[-1] != taxonomy_path[-1]:
            candidate_path = (*taxonomy_path, local_path[-1])
        if not candidate_path:
            candidate_path = local_path
        candidates[board_id] = BoardCandidate(
            board_id=board_id,
            board_url=f"{BASE_URL}/B.php?bsn={board_id}",
            board_name=name,
            game_title=name,
            taxonomy_path=candidate_path,
            method="game_directory",
        )
    return list(candidates.values())


def parse_guide_category_pages(
    html: str,
    parent_category_id: str | None = None,
    parent_taxonomy_path: tuple[str, ...] = (),
) -> list[tuple[str, str, str | None, tuple[str, ...]]]:
    soup = BeautifulSoup(html, "lxml")
    categories = {}
    category_boxes = soup.select(".BH-lbox.FORUM-master_box1")
    for category_box in category_boxes:
        root_heading = (
            parent_taxonomy_path[0]
            if parent_taxonomy_path
            else _normal_text(category_box.find_previous_sibling("h4"))
        )
        for group in category_box.find_all("div", recursive=False):
            group_heading = _normal_text(group.find("strong", recursive=False))
            path = tuple(value for value in (root_heading, group_heading) if value)
            if parent_taxonomy_path and parent_taxonomy_path[-1] != group_heading:
                path = (*parent_taxonomy_path, group_heading) if group_heading else parent_taxonomy_path
            for anchor in group.select('a[href*="c="]'):
                if "BH-tcolor1" not in anchor.get("class", []):
                    continue
                href = _safe_site_url(anchor.get("href", ""))
                if not href:
                    continue
                parts = urlsplit(href)
                if parts.path.lower() not in {"/", "/index.php"}:
                    continue
                category_ids = parse_qs(parts.query).get("c", [])
                if not category_ids or not category_ids[0].isdigit():
                    continue
                category_id = category_ids[0]
                categories[category_id] = (
                    category_id, f"{BASE_URL}/?c={category_id}", None, path,
                )

    if not category_boxes:
        for anchor in soup.select('a[href*="c="]'):
            if "BH-tcolor1" not in anchor.get("class", []):
                continue
            href = _safe_site_url(anchor.get("href", ""))
            if not href:
                continue
            parts = urlsplit(href)
            if parts.path.lower() not in {"/", "/index.php"}:
                continue
            category_ids = parse_qs(parts.query).get("c", [])
            if not category_ids or not category_ids[0].isdigit():
                continue
            category_id = category_ids[0]
            group = anchor.parent
            group_heading = _normal_text(
                group.find("strong", recursive=False) if group else None
            )
            path = parent_taxonomy_path
            if group_heading and (not path or path[-1] != group_heading):
                path = (*path, group_heading)
            categories[category_id] = (
                category_id, f"{BASE_URL}/?c={category_id}",
                parent_category_id, path,
            )
    for navigation in soup.select("nav"):
        for anchor in navigation.select('a[href*="c="]'):
            root_path = ROOT_NAVIGATION_LABELS.get(_normal_text(anchor))
            if not root_path:
                continue
            href = _safe_site_url(anchor.get("href", ""))
            if not href:
                continue
            category_ids = parse_qs(urlsplit(href).query).get("c", [])
            if not category_ids or not category_ids[0].isdigit():
                continue
            category_id = category_ids[0]
            categories[category_id] = (
                category_id, f"{BASE_URL}/?c={category_id}", None, root_path,
            )
    return list(categories.values())


def parse_sitemap_index(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    if root.tag.rsplit("}", 1)[-1] != "sitemapindex":
        return []
    return [
        loc.text.strip()
        for loc in root.iter()
        if loc.tag.rsplit("}", 1)[-1] == "loc" and loc.text
    ]


def parse_sitemap_boards(xml_text: str) -> list[BoardCandidate]:
    root = ET.fromstring(xml_text)
    candidates = {}
    for loc in root.iter():
        if loc.tag.rsplit("}", 1)[-1] != "loc" or not loc.text:
            continue
        url = _safe_site_url(loc.text.strip())
        if not url:
            continue
        match = BOARD_ID_RE.search(url)
        if not match:
            continue
        board_id = match.group(1)
        candidates[board_id] = BoardCandidate(
            board_id=board_id,
            board_url=f"{BASE_URL}/B.php?bsn={board_id}",
        )
    return list(candidates.values())


def parse_board_threads(html: str, board_id: str) -> tuple[list[ThreadIndexEntry], int]:
    soup = BeautifulSoup(html, "lxml")
    if "哈啦板" not in _normal_text(soup.title):
        raise ValueError(f"board {board_id} did not return a recognizable Bahamut board page")
    page_numbers = [1]
    for anchor in soup.select('a[href*="page="]'):
        query = parse_qs(urlsplit(anchor.get("href", "")).query)
        if query.get("bsn") != [str(board_id)]:
            continue
        page = query.get("page", [None])[0]
        if page and page.isdigit():
            page_numbers.append(int(page))
    last_page = max(page_numbers)

    entries = {}
    for row in soup.select(".b-list__row"):
        article_anchor = row.select_one("a.b-list__main__title[href*='snA=']")
        if not article_anchor:
            continue
        article_url = _safe_site_url(article_anchor.get("href", ""))
        match = THREAD_ID_RE.search(article_url or "")
        title = _normal_text(article_anchor)
        if not article_url or not match or not title:
            continue
        tag_text = _normal_text(row.select_one(".b-list__summary__sort"))
        tags = tuple(term for term in RELEVANT_GUIDE_TERMS if term in tag_text)
        if not tags:
            continue
        entries[match.group(1)] = ThreadIndexEntry(
            article_id=match.group(1),
            article_url=article_url,
            title=title,
            article_type="guide" if any(t in tags for t in ("攻略", "教學", "玩法", "機制")) else "experience",
            tags=tags,
        )
    return list(entries.values()), last_page


def _clean_post_html(article) -> tuple[str, str, tuple[dict, ...]]:
    body = BeautifulSoup(str(article), "lxml")
    root = body.find("article", class_="c-article") or body
    for unwanted in root.select("script, style, noscript, .c-post__footer, .c-reply"):
        unwanted.decompose()

    media_refs = []
    for image in root.select("img"):
        source = image.get("data-src") or image.get("src")
        if source:
            media_url = urljoin(BASE_URL, source)
            if urlsplit(media_url).scheme not in {"http", "https"}:
                continue
            media_refs.append({
                "kind": "image",
                "url": media_url,
                "alt": image.get("alt", ""),
            })
    for frame in root.select("iframe, video, source"):
        source = frame.get("src") or frame.get("data-src")
        if source:
            media_url = urljoin(BASE_URL, source)
            if urlsplit(media_url).scheme in {"http", "https"}:
                media_refs.append({"kind": frame.name, "url": media_url})

    clean = BeautifulSoup("<div></div>", "lxml").div
    for child in list(root.contents):
        clean.append(child)

    allowed_tags = {
        "a", "b", "blockquote", "br", "code", "dd", "del", "details", "div",
        "dl", "dt", "em", "figcaption", "figure", "h1", "h2", "h3", "h4",
        "h5", "h6", "hr", "i", "img", "li", "ol", "p", "pre", "s",
        "small", "span", "strong", "sub", "summary", "sup", "table", "tbody",
        "td", "th", "thead", "tr", "u", "ul",
    }
    for tag in list(clean.find_all(True)):
        if tag.name not in allowed_tags:
            tag.unwrap()
            continue
        allowed_attributes = {
            "a": {"href", "title"},
            "img": {"src", "alt", "title", "width", "height"},
            "td": {"colspan", "rowspan"},
            "th": {"colspan", "rowspan"},
            "ol": {"start"},
        }.get(tag.name, set())
        if tag.name == "img" and not tag.get("src") and tag.get("data-src"):
            tag["src"] = tag.get("data-src")
        for attribute in list(tag.attrs):
            value = tag.get(attribute)
            if attribute not in allowed_attributes:
                del tag[attribute]
            elif attribute == "href" or attribute == "src":
                value = urljoin(BASE_URL, str(value))
                if urlsplit(value).scheme not in {"http", "https"}:
                    del tag[attribute]
                else:
                    tag[attribute] = value
    body_html = clean.decode_contents(formatter="html")
    text_root = BeautifulSoup(body_html, "lxml")
    body_text = re.sub(r"\s+", " ", text_root.get_text(" ", strip=True)).strip()
    return body_html, body_text, tuple(media_refs)


def parse_thread_page(
    html: str, board_id: str, article_id: str, page_number: int
) -> tuple[list[ThreadPost], int]:
    soup = BeautifulSoup(html, "lxml")
    if "哈啦板" not in _normal_text(soup.title):
        raise ValueError(f"thread {board_id}/{article_id} did not return a recognizable page")
    page_numbers = [page_number]
    for anchor in soup.select('a[href*="page="]'):
        query = parse_qs(urlsplit(anchor.get("href", "")).query)
        if query.get("bsn") != [str(board_id)] or query.get("snA") != [str(article_id)]:
            continue
        page = query.get("page", [None])[0]
        if page and page.isdigit():
            page_numbers.append(int(page))
    last_page = max(page_numbers)

    posts = []
    for post_container in soup.select(".c-section__main.c-post"):
        article = post_container.select_one("article.c-article[id]")
        if not article:
            raise ValueError("top-level post container is missing its article body")
        post_id = str(article.get("id", "")).strip()
        floor_node = post_container.select_one(".floor")
        floor_text = _normal_text(floor_node)
        if "樓主" in floor_text:
            post_number = 1
        else:
            floor_match = re.search(r"\d+", floor_text)
            if not floor_match:
                raise ValueError("top-level post is missing its floor number")
            post_number = int(floor_match.group(0))
        if not post_id:
            raise ValueError("top-level post is missing its source ID")
        body_html, body_text, media_refs = _clean_post_html(article)
        if not body_html.strip() or (not body_text.strip() and not media_refs):
            raise ValueError(f"top-level post {post_id} has no readable text or media body")
        posts.append(ThreadPost(
            post_id=post_id,
            post_number=post_number,
            post_type="original" if post_number == 1 else "reply",
            body_html=body_html,
            body_text=body_text,
            media_refs=media_refs,
        ))
    return posts, last_page


class BahamutClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.robot_parser: RobotFileParser | None = None
        self.robots_text = ""
        self.last_request_at: float | None = None
        self.request_count = 0

    def _get(self, url: str, *, parse_robots: bool = True) -> requests.Response:
        if self.request_count >= MAX_REQUESTS_PER_RUN:
            raise CrawlBudgetExceeded("per-run request budget reached")
        safe_url = _safe_site_url(url)
        if not safe_url:
            raise ValueError("Bahamut collector only follows HTTPS forum.gamer.com.tw URLs")
        for redirect_count in range(4):
            if self.request_count >= MAX_REQUESTS_PER_RUN:
                raise CrawlBudgetExceeded("per-run request budget reached")
            if (
                parse_robots and self.robot_parser
                and not self.robot_parser.can_fetch(USER_AGENT, safe_url)
            ):
                raise PermissionError(f"robots.txt disallows {safe_url}")

            interval = MIN_REQUEST_INTERVAL_SECONDS
            if self.robot_parser:
                crawl_delay = self.robot_parser.crawl_delay(USER_AGENT) or 0
                request_rate = self.robot_parser.request_rate(USER_AGENT)
                if request_rate and request_rate.requests:
                    crawl_delay = max(crawl_delay, request_rate.seconds / request_rate.requests)
                interval = max(interval, crawl_delay)
            if self.last_request_at is None:
                time.sleep(interval)
            else:
                wait = interval - (time.monotonic() - self.last_request_at)
                if wait > 0:
                    time.sleep(wait)
            response = self.session.get(
                safe_url, timeout=(5, 15), allow_redirects=False
            )
            self.last_request_at = time.monotonic()
            self.request_count += 1
            if response.status_code in (401, 403, 429):
                raise PermissionError(
                    f"Bahamut returned HTTP {response.status_code}; stopping this run"
                )
            if response.is_redirect or response.is_permanent_redirect:
                if redirect_count == 3:
                    raise ValueError("Bahamut redirect limit exceeded")
                target = _safe_site_url(response.headers.get("Location", ""), safe_url)
                if not target:
                    raise ValueError("Bahamut redirected outside its HTTPS forum host")
                safe_url = target
                continue
            response.raise_for_status()
            return response
        raise ValueError("Bahamut redirect limit exceeded")

    def load_robots(self) -> None:
        response = self._get(f"{BASE_URL}/robots.txt", parse_robots=False)
        self.robots_text = response.text
        parser = RobotFileParser()
        parser.set_url(f"{BASE_URL}/robots.txt")
        parser.parse(response.text.splitlines())
        self.robot_parser = parser

    def get_text(self, url: str) -> str:
        response = self._get(url)
        content_type = response.headers.get("Content-Type", "").lower()
        path = urlsplit(url).path.lower()
        if "html" in content_type or path in {"/", "/guide.php", "/b.php", "/c.php"}:
            return str(BeautifulSoup(response.content, "lxml"))
        if "xml" in content_type or path.endswith(".xml"):
            return response.content.decode("utf-8-sig", errors="replace")
        return response.content.decode(response.encoding or "utf-8", errors="replace")


def _active_source_grant(cursor, source_key: str):
    cursor.execute(
        """
        SELECT id, can_store_metadata, can_store_full_text,
               can_run_llm_analysis, can_use_commercial_derivatives
        FROM game_knowledge_rights_grants
        WHERE source_key = %s AND scope_type = 'source' AND scope_key = '*'
          AND valid_from <= UTC_TIMESTAMP()
          AND (valid_until IS NULL OR valid_until >= UTC_TIMESTAMP())
        ORDER BY valid_from DESC, id DESC LIMIT 1
        """,
        (source_key,),
    )
    return cursor.fetchone()


def _board_grant(cursor, source_key: str, board_id: str):
    cursor.execute(
        """
        SELECT id, can_store_metadata, can_store_full_text,
               can_run_llm_analysis, can_use_commercial_derivatives
        FROM game_knowledge_rights_grants
        WHERE source_key = %s AND scope_type IN ('board', 'source')
          AND scope_key IN (%s, '*')
          AND valid_from <= UTC_TIMESTAMP()
          AND (valid_until IS NULL OR valid_until >= UTC_TIMESTAMP())
        ORDER BY CASE scope_type WHEN 'board' THEN 1 ELSE 2 END, valid_from DESC, id DESC
        LIMIT 1
        """,
        (source_key, board_id),
    )
    return cursor.fetchone()


def _thread_grant(cursor, source_key: str, board_id: str, article_id: str):
    cursor.execute(
        """
        SELECT id, can_store_metadata, can_store_full_text,
               can_run_llm_analysis, can_use_commercial_derivatives
        FROM game_knowledge_rights_grants
        WHERE source_key = %s
          AND valid_from <= UTC_TIMESTAMP()
          AND (valid_until IS NULL OR valid_until >= UTC_TIMESTAMP())
          AND (
              (scope_type = 'article' AND scope_key = %s)
              OR (scope_type = 'board' AND scope_key = %s)
              OR (scope_type = 'source' AND scope_key = '*')
          )
        ORDER BY CASE scope_type WHEN 'article' THEN 1 WHEN 'board' THEN 2 ELSE 3 END,
                 valid_from DESC, id DESC LIMIT 1
        """,
        (source_key, f"{board_id}/{article_id}", board_id),
    )
    return cursor.fetchone()


def _store_candidates(conn, cursor, candidates: list[BoardCandidate]) -> int:
    now = _now_utc()
    rows = [
        (
            SOURCE_KEY, candidate.board_id, candidate.board_name,
            candidate.board_url, candidate.game_title,
            json.dumps(candidate.taxonomy_path, ensure_ascii=False)
            if candidate.taxonomy_path else None,
            candidate.method, now, now,
        )
        for candidate in candidates
    ]
    if rows:
        upsert_sql = """
            INSERT INTO game_knowledge_board_candidates
                (source_key, board_external_id, board_name, board_url,
                 candidate_game_title, taxonomy_path_json, discovery_method,
                 review_status, discovered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s, %s)
            ON DUPLICATE KEY UPDATE
                board_name = COALESCE(VALUES(board_name), board_name),
                candidate_game_title = COALESCE(VALUES(candidate_game_title), candidate_game_title),
                taxonomy_path_json = CASE
                    WHEN VALUES(discovery_method) = 'game_directory'
                     AND (
                        taxonomy_path_json IS NULL
                        OR JSON_VALID(taxonomy_path_json) = 0
                        OR JSON_UNQUOTE(JSON_EXTRACT(VALUES(taxonomy_path_json), '$[0]'))
                           <> JSON_UNQUOTE(JSON_EXTRACT(taxonomy_path_json, '$[0]'))
                        OR JSON_LENGTH(VALUES(taxonomy_path_json))
                           >= JSON_LENGTH(taxonomy_path_json)
                        OR JSON_UNQUOTE(JSON_EXTRACT(taxonomy_path_json, '$[0]'))
                           NOT IN ('手機遊戲', 'PC', 'TV 掌機')
                     )
                        THEN VALUES(taxonomy_path_json)
                    ELSE taxonomy_path_json END,
                discovery_method = CASE
                    WHEN VALUES(discovery_method) = 'game_directory'
                        THEN 'game_directory'
                    WHEN discovery_method = 'game_directory'
                        THEN discovery_method ELSE 'sitemap' END,
                updated_at = VALUES(updated_at)
            """
        for offset in range(0, len(rows), 1000):
            cursor.executemany(upsert_sql, rows[offset:offset + 1000])
    conn.commit()
    return len(candidates)


def _queue_directory_pages(conn, cursor, categories) -> None:
    now = _now_utc()
    rows = [
        (SOURCE_KEY, category_id, page_url, parent_id, now)
        for category_id, page_url, parent_id, _ in categories
    ]
    if rows:
        cursor.executemany(
            """
            INSERT INTO game_knowledge_directory_pages
                (source_key, category_external_id, category_url,
                 parent_category_id, discovered_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                category_url = VALUES(category_url),
                parent_category_id = VALUES(parent_category_id)
            """,
            rows,
        )
    for category_id, _, _, taxonomy_path in categories:
        taxonomy_json = json.dumps(taxonomy_path, ensure_ascii=False)
        cursor.execute(
            """
            SELECT taxonomy_path_json
            FROM game_knowledge_directory_taxonomy
            WHERE source_key = %s AND category_external_id = %s
            """,
            (SOURCE_KEY, category_id),
        )
        existing = cursor.fetchone()
        if not existing or existing[0] != taxonomy_json:
            previous_path = []
            if existing and existing[0]:
                try:
                    previous_path = json.loads(existing[0])
                except (TypeError, json.JSONDecodeError):
                    previous_path = []
            cursor.execute(
                """
                INSERT INTO game_knowledge_directory_taxonomy
                    (source_key, category_external_id, taxonomy_path_json, updated_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    taxonomy_path_json = VALUES(taxonomy_path_json),
                    updated_at = VALUES(updated_at)
                """,
                (SOURCE_KEY, category_id, taxonomy_json, now),
            )
            cursor.execute(
                """
                UPDATE game_knowledge_directory_pages SET scanned_at = NULL
                WHERE source_key = %s AND category_external_id = %s
                """,
                (SOURCE_KEY, category_id),
            )
            if len(taxonomy_path) == 1 and taxonomy_path[0] in GAME_DIRECTORY_ROOTS:
                roots_to_refresh = {taxonomy_path[0]}
                if previous_path and previous_path[0] in GAME_DIRECTORY_ROOTS:
                    roots_to_refresh.add(previous_path[0])
                cursor.execute(
                    """
                    SELECT category_external_id, taxonomy_path_json
                    FROM game_knowledge_directory_taxonomy
                    WHERE source_key = %s
                    """,
                    (SOURCE_KEY,),
                )
                for child_id, child_path_json in cursor.fetchall():
                    if str(child_id) == str(category_id):
                        continue
                    try:
                        child_path = json.loads(child_path_json or "[]")
                    except (TypeError, json.JSONDecodeError):
                        continue
                    if len(child_path) > 1 and child_path[0] in roots_to_refresh:
                        cursor.execute(
                            """
                            UPDATE game_knowledge_directory_pages SET scanned_at = NULL
                            WHERE source_key = %s AND category_external_id = %s
                            """,
                            (SOURCE_KEY, child_id),
                        )
    conn.commit()


def _ensure_root_navigation_seed(cursor) -> None:
    expected_paths = tuple(
        json.dumps(path, ensure_ascii=False)
        for path in ROOT_NAVIGATION_LABELS.values()
    )
    placeholders = ", ".join(["%s"] * len(expected_paths))
    cursor.execute(
        f"""
        SELECT COUNT(DISTINCT taxonomy_path_json)
        FROM game_knowledge_directory_taxonomy
        WHERE source_key = %s AND taxonomy_path_json IN ({placeholders})
        """,
        (SOURCE_KEY, *expected_paths),
    )
    if int(cursor.fetchone()[0] or 0) == len(expected_paths):
        return
    cursor.execute(
        """
        UPDATE game_knowledge_directory_pages SET scanned_at = NULL
        WHERE source_key = %s AND category_external_id = '101'
        """,
        (SOURCE_KEY,),
    )


def _refresh_stale_game_directory_pages(cursor) -> None:
    roots = tuple(sorted(GAME_DIRECTORY_ROOTS))
    placeholders = ", ".join(["%s"] * len(roots))
    cursor.execute(
        f"""
        UPDATE game_knowledge_directory_pages d
        JOIN game_knowledge_directory_taxonomy t
          ON t.source_key = d.source_key
         AND t.category_external_id = d.category_external_id
        SET d.scanned_at = NULL
        WHERE d.source_key = %s AND t.source_key = %s
          AND d.scanned_at IS NOT NULL
          AND d.scanned_at <= %s
          AND JSON_UNQUOTE(JSON_EXTRACT(t.taxonomy_path_json, '$[0]'))
              IN ({placeholders})
        """,
        (
            SOURCE_KEY, SOURCE_KEY,
            _now_utc() - timedelta(days=DIRECTORY_REFRESH_DAYS),
            *roots,
        ),
    )


def _discover_candidates(conn, cursor, client: BahamutClient) -> int:
    source_grant = _active_source_grant(cursor, SOURCE_KEY)
    if not source_grant or not source_grant[1]:
        return 0

    client.load_robots()
    discovered_ids = set()
    guide_html = client.get_text(f"{BASE_URL}/guide.php")
    guide_candidates = parse_guide_boards(guide_html)
    _store_candidates(conn, cursor, guide_candidates)
    discovered_ids.update(candidate.board_id for candidate in guide_candidates)
    _queue_directory_pages(conn, cursor, parse_guide_category_pages(guide_html))
    _ensure_root_navigation_seed(cursor)
    _refresh_stale_game_directory_pages(cursor)
    conn.commit()

    now = _now_utc()
    cursor.execute(
        """
        INSERT IGNORE INTO game_knowledge_discovery_state
            (source_key, state_key, checked_at, updated_at)
        VALUES (%s, 'sitemap', NULL, %s)
        """,
        (SOURCE_KEY, now),
    )
    cursor.execute(
        """
        SELECT checked_at FROM game_knowledge_discovery_state
        WHERE source_key = %s AND state_key = 'sitemap'
        """,
        (SOURCE_KEY,),
    )
    sitemap_checked_at = cursor.fetchone()[0]
    refresh_sitemap = (
        sitemap_checked_at is None
        or sitemap_checked_at <= now - timedelta(days=SITEMAP_REFRESH_DAYS)
    )
    if refresh_sitemap:
        sitemap_urls = [
            line.split(":", 1)[1].strip()
            for line in client.robots_text.splitlines()
            if line.lower().startswith("sitemap:")
        ]
        sitemap_candidates = {}
        for sitemap_url in sitemap_urls:
            sitemap_xml = client.get_text(sitemap_url)
            shard_urls = parse_sitemap_index(sitemap_xml)
            if not shard_urls:
                for candidate in parse_sitemap_boards(sitemap_xml):
                    sitemap_candidates.setdefault(candidate.board_id, candidate)
            for shard_url in shard_urls:
                shard_xml = client.get_text(shard_url)
                for candidate in parse_sitemap_boards(shard_xml):
                    sitemap_candidates.setdefault(candidate.board_id, candidate)
        _store_candidates(conn, cursor, list(sitemap_candidates.values()))
        discovered_ids.update(sitemap_candidates)
        cursor.execute(
            """
            UPDATE game_knowledge_discovery_state
            SET checked_at = %s, updated_at = %s
            WHERE source_key = %s AND state_key = 'sitemap'
            """,
            (now, now, SOURCE_KEY),
        )
        conn.commit()

    while client.request_count < MAX_REQUESTS_PER_RUN:
        cursor.execute(
            """
            SELECT d.category_external_id, d.category_url,
                   t.taxonomy_path_json
            FROM game_knowledge_directory_pages d
            LEFT JOIN game_knowledge_directory_taxonomy t
              ON t.source_key = d.source_key
             AND t.category_external_id = d.category_external_id
            WHERE d.source_key = %s AND d.scanned_at IS NULL
            ORDER BY CASE WHEN d.category_external_id = '101' THEN 0 ELSE 1 END,
                     d.category_external_id
            LIMIT 1
            """,
            (SOURCE_KEY,),
        )
        page = cursor.fetchone()
        if not page:
            break
        category_id, category_url, taxonomy_path_json = page
        taxonomy_path = tuple(json.loads(taxonomy_path_json or "[]"))
        category_html = client.get_text(category_url)
        candidates = parse_guide_boards(category_html, taxonomy_path)
        _store_candidates(conn, cursor, candidates)
        discovered_ids.update(candidate.board_id for candidate in candidates)
        children = parse_guide_category_pages(
            category_html, category_id, taxonomy_path
        )
        _queue_directory_pages(conn, cursor, children)
        cursor.execute(
            """
            UPDATE game_knowledge_directory_pages SET scanned_at = %s
            WHERE source_key = %s AND category_external_id = %s
            """,
            (_now_utc(), SOURCE_KEY, category_id),
        )
        conn.commit()
    return len(discovered_ids)


def _game_key_for_title(game_title: str, board_id: str) -> str:
    normalized = unicodedata.normalize("NFKC", game_title).casefold()
    slug = re.sub(r"[^\w]+", "-", normalized, flags=re.UNICODE).strip("-")
    return f"game-{slug}"[:160] if slug else (
        "game-" + hashlib.sha256(board_id.encode("utf-8")).hexdigest()[:32]
    )


def _promote_reviewed_candidates(conn, cursor) -> None:
    now = _now_utc()
    cursor.execute(
        """
        SELECT board_external_id, candidate_game_title, mapped_game_key,
               mapping_source, taxonomy_path_json
        FROM game_knowledge_board_candidates
        WHERE source_key = %s AND discovery_method = 'game_directory'
          AND review_status <> 'rejected' AND mapping_source <> 'manual'
          AND (mapping_source = 'directory_title'
               OR (review_status = 'pending' AND mapped_game_key IS NULL))
          AND candidate_game_title IS NOT NULL AND candidate_game_title <> ''
        """,
        (SOURCE_KEY,),
    )
    candidates = cursor.fetchall()
    replaced_game_keys = set()
    for board_id, game_title, previous_game_key, mapping_source, taxonomy_path_json in candidates:
        taxonomy_path = json.loads(taxonomy_path_json or "[]")
        is_game_board = (
            len(taxonomy_path) >= 1
            and taxonomy_path[0] in GAME_DIRECTORY_ROOTS
            and (len(taxonomy_path) == 1 or taxonomy_path[1] != "主機平台")
            and not NON_GAME_BOARD_TITLE.search(game_title)
        )
        if not is_game_board:
            if mapping_source == "directory_title":
                cursor.execute(
                    """
                    UPDATE game_knowledge_board_candidates
                    SET review_status = 'pending', mapped_game_key = NULL,
                        mapping_source = 'unmapped', updated_at = %s
                    WHERE source_key = %s AND board_external_id = %s
                    """,
                    (now, SOURCE_KEY, board_id),
                )
            continue
        game_key = _game_key_for_title(game_title, board_id)
        cursor.execute(
            """
            INSERT INTO game_knowledge_games (game_key, title, aliases_json, created_at, updated_at)
            VALUES (%s, %s, '[]', %s, %s)
            ON DUPLICATE KEY UPDATE title = VALUES(title), updated_at = VALUES(updated_at)
            """,
            (game_key, game_title, now, now),
        )
        if previous_game_key and previous_game_key != game_key:
            replaced_game_keys.add(previous_game_key)
        cursor.execute(
            """
            UPDATE game_knowledge_board_candidates
            SET review_status = 'approved', mapped_game_key = %s,
                mapping_source = 'directory_title', updated_at = %s
            WHERE source_key = %s AND board_external_id = %s
              AND review_status <> 'rejected' AND mapping_source <> 'manual'
            """,
            (game_key, now, SOURCE_KEY, board_id),
        )
    cursor.execute(
        """
        UPDATE game_knowledge_articles a
        JOIN game_knowledge_board_candidates c
          ON c.source_key = a.source_key AND c.board_external_id = a.board_external_id
        SET a.game_key = c.mapped_game_key
        WHERE c.source_key = %s AND c.mapping_source = 'directory_title'
          AND c.mapped_game_key IS NOT NULL
        """,
        (SOURCE_KEY,),
    )
    cursor.execute(
        """
        INSERT INTO game_knowledge_boards
            (source_key, board_external_id, game_key, board_name, board_url,
             platform_name, created_at, updated_at)
        SELECT c.source_key, c.board_external_id, c.mapped_game_key,
               COALESCE(c.board_name, c.candidate_game_title, c.board_external_id),
               c.board_url, 'Bahamut', %s, %s
        FROM game_knowledge_board_candidates c
        JOIN game_knowledge_games g ON g.game_key = c.mapped_game_key
        WHERE c.source_key = %s AND c.review_status = 'approved'
          AND c.mapped_game_key IS NOT NULL
        ON DUPLICATE KEY UPDATE
            game_key = VALUES(game_key), board_name = VALUES(board_name),
            board_url = VALUES(board_url), is_enabled = 1, updated_at = VALUES(updated_at)
        """,
        (now, now, SOURCE_KEY),
    )
    cursor.execute(
        """
        UPDATE game_knowledge_boards b
        JOIN game_knowledge_board_candidates c
          ON c.source_key = b.source_key AND c.board_external_id = b.board_external_id
        SET b.is_enabled = 0, b.updated_at = %s
        WHERE b.source_key = %s
          AND (c.review_status <> 'approved' OR c.mapped_game_key IS NULL)
        """,
        (now, SOURCE_KEY),
    )
    for game_key in replaced_game_keys:
        cursor.execute(
            """
            DELETE FROM game_knowledge_games
            WHERE game_key = %s
              AND NOT EXISTS (
                  SELECT 1 FROM game_knowledge_board_candidates
                  WHERE mapped_game_key = %s
              )
              AND NOT EXISTS (
                  SELECT 1 FROM game_knowledge_boards WHERE game_key = %s
              )
              AND NOT EXISTS (
                  SELECT 1 FROM game_knowledge_articles WHERE game_key = %s
              )
            """,
            (game_key, game_key, game_key, game_key),
        )
    conn.commit()


def _ensure_board_scan_state(conn, cursor, board_id: str) -> tuple[int, bool]:
    now = _now_utc()
    cursor.execute(
        """
        INSERT IGNORE INTO game_knowledge_board_scan_state
            (source_key, board_external_id, next_listing_page, listing_complete, updated_at)
        VALUES (%s, %s, 1, 0, %s)
        """,
        (SOURCE_KEY, board_id, now),
    )
    cursor.execute(
        """
        SELECT next_listing_page, listing_complete
        FROM game_knowledge_board_scan_state
        WHERE source_key = %s AND board_external_id = %s
        """,
        (SOURCE_KEY, board_id),
    )
    return cursor.fetchone()


def _store_thread_index(conn, cursor, board, entry: ThreadIndexEntry) -> None:
    record = {
        "source_key": SOURCE_KEY,
        "game_key": board[0],
        "game_title": board[1],
        "board_external_id": board[2],
        "board_name": board[3],
        "board_url": board[4],
        "article_external_id": entry.article_id,
        "article_url": entry.article_url,
        "title": entry.title,
        "article_type": entry.article_type,
        "tags": list(entry.tags),
        "posts_complete": False,
        "expected_post_count": None,
        "posts": [],
    }
    _upsert_record(conn, cursor, record)


def _collect_board_indexes(conn, cursor, client: BahamutClient) -> int:
    cursor.execute(
        """
        SELECT b.game_key, g.title, b.board_external_id, b.board_name, b.board_url
        FROM game_knowledge_boards b
        JOIN game_knowledge_games g ON g.game_key = b.game_key
        LEFT JOIN game_knowledge_board_scan_state s
          ON s.source_key = b.source_key AND s.board_external_id = b.board_external_id
        WHERE b.source_key = %s AND b.is_enabled = 1
        ORDER BY s.last_scanned_at IS NOT NULL, s.last_scanned_at, b.board_external_id
        """,
        (SOURCE_KEY,),
    )
    boards = cursor.fetchall()
    page_count = 0
    for board in boards:
        if (
            page_count >= MAX_BOARD_INDEX_PAGES_PER_RUN
            or client.request_count >= MAX_REQUESTS_PER_RUN
        ):
            break
        grant = _board_grant(cursor, SOURCE_KEY, board[2])
        if not grant or not grant[1]:
            continue
        next_page, listing_complete = _ensure_board_scan_state(conn, cursor, board[2])
        backfill_in_progress = not bool(listing_complete)
        page = next_page if backfill_in_progress else 1
        html = client.get_text(f"{board[4]}&page={page}")
        entries, last_page = parse_board_threads(html, board[2])
        for entry in entries:
            _store_thread_index(conn, cursor, board, entry)
        is_complete = page >= last_page
        now = _now_utc()
        cursor.execute(
            """
            UPDATE game_knowledge_board_scan_state
            SET next_listing_page = IF(%s, IF(%s, %s, %s), 1),
                listing_complete = IF(%s, %s, 1),
                last_scanned_at = %s, updated_at = %s
            WHERE source_key = %s AND board_external_id = %s
            """,
            (
                int(backfill_in_progress), int(is_complete), page, page + 1,
                int(backfill_in_progress), int(is_complete),
                now, now, SOURCE_KEY, board[2],
            ),
        )
        conn.commit()
        page_count += 1
    return page_count


def _collect_thread_pages(conn, cursor, client: BahamutClient) -> int:
    page_count = 0
    while page_count < MAX_THREAD_PAGES_PER_RUN and client.request_count < MAX_REQUESTS_PER_RUN:
        cursor.execute(
            """
            SELECT a.game_key, g.title, a.board_external_id, b.board_name, b.board_url,
                   a.article_external_id, a.article_url, a.title, a.article_type,
                   a.tags_json, a.next_post_page, a.thread_page_count
            FROM game_knowledge_articles a
            JOIN game_knowledge_games g ON g.game_key = a.game_key
            JOIN game_knowledge_boards b
              ON b.source_key = a.source_key AND b.board_external_id = a.board_external_id
            WHERE a.source_key = %s AND a.article_type IN ('guide', 'experience')
              AND a.posts_complete = 0 AND b.is_enabled = 1
            ORDER BY a.last_post_page_at IS NOT NULL, a.last_post_page_at,
                     a.board_external_id, a.article_external_id
            """,
            (SOURCE_KEY,),
        )
        articles = cursor.fetchall()
        if not articles:
            break
        processed_this_pass = set()
        crawled_this_pass = 0
        for article in articles:
            if page_count >= MAX_THREAD_PAGES_PER_RUN or client.request_count >= MAX_REQUESTS_PER_RUN:
                break
            board_id, article_id = article[2], article[5]
            article_key = (board_id, article_id)
            if article_key in processed_this_pass:
                continue
            processed_this_pass.add(article_key)
            grant = _thread_grant(cursor, SOURCE_KEY, board_id, article_id)
            full_text_allowed = bool(grant and grant[1] and grant[2])
            if not full_text_allowed:
                continue
            page = int(article[10] or 1)
            separator = "&" if "?" in article[6] else "?"
            page_url = f"{article[6]}{separator}page={page}"
            html = client.get_text(page_url)
            posts, discovered_last_page = parse_thread_page(html, board_id, article_id, page)
            if not posts:
                raise ValueError(f"thread {board_id}/{article_id} page {page} had no top-level posts")
            final_page = page >= discovered_last_page
            record = {
                "source_key": SOURCE_KEY,
                "game_key": article[0],
                "game_title": article[1],
                "board_external_id": board_id,
                "board_name": article[3],
                "board_url": article[4],
                "article_external_id": article_id,
                "article_url": article[6],
                "title": article[7],
                "article_type": article[8],
                "tags": json.loads(article[9] or "[]"),
                "posts_complete": final_page,
                "expected_post_count": None,
                "posts": [
                    {
                        "post_external_id": post.post_id,
                        "post_number": post.post_number,
                        "post_type": post.post_type,
                        "content_format": "text/html",
                        "body_content": post.body_html,
                        "body_text": post.body_text,
                        "media_refs": list(post.media_refs),
                    }
                    for post in posts
                ],
            }
            _upsert_record(conn, cursor, record)
            next_page = page if final_page else page + 1
            now = _now_utc()
            cursor.execute(
                """
                UPDATE game_knowledge_articles
                SET next_post_page = %s, thread_page_count = %s,
                    last_post_page_at = %s, updated_at = %s
                WHERE source_key = %s AND board_external_id = %s AND article_external_id = %s
                """,
                (
                    next_page, discovered_last_page, now, now,
                    SOURCE_KEY, board_id, article_id,
                ),
            )
            conn.commit()
            page_count += 1
            crawled_this_pass += 1
        if not crawled_this_pass:
            break
    return page_count


def _completed_article_count(cursor) -> int:
    cursor.execute(
        """
        SELECT COUNT(*) FROM game_knowledge_articles
        WHERE source_key = %s AND posts_complete = 1
          AND content_status = 'full_text_stored'
        """,
        (SOURCE_KEY,),
    )
    return int(cursor.fetchone()[0])


def collect_bahamut_game_knowledge(dag_run_id: str) -> dict:
    conn, cursor, engine = _get_database_connection()
    started_at = _now_utc()
    client = BahamutClient()
    candidates = board_pages = thread_pages = 0
    completed_before = completed_articles = 0
    status = "running"
    error = None
    try:
        cursor.execute(
            "INSERT INTO game_knowledge_crawl_runs (dag_run_id, started_at, status) "
            "VALUES (%s, %s, 'running') ON DUPLICATE KEY UPDATE "
            "started_at = VALUES(started_at), finished_at = NULL, status = 'running', "
            "error_summary = NULL",
            (dag_run_id, started_at),
        )
        conn.commit()
        completed_before = _completed_article_count(cursor)
        cursor.execute(
            "SELECT is_enabled FROM game_knowledge_sources WHERE source_key = %s",
            (SOURCE_KEY,),
        )
        source = cursor.fetchone()
        if not source or not source[0]:
            status = "skipped_disabled"
            return {"status": status, "request_count": 0}

        source_grant = _active_source_grant(cursor, SOURCE_KEY)
        if not source_grant or not source_grant[1]:
            cursor.execute(
                """
                SELECT b.board_external_id
                FROM game_knowledge_boards b
                JOIN game_knowledge_rights_grants r
                  ON r.source_key = b.source_key
                 AND ((r.scope_type = 'board' AND r.scope_key = b.board_external_id)
                      OR (r.scope_type = 'source' AND r.scope_key = '*'))
                WHERE b.source_key = %s AND r.can_store_metadata = 1
                  AND r.valid_from <= UTC_TIMESTAMP()
                  AND (r.valid_until IS NULL OR r.valid_until >= UTC_TIMESTAMP())
                LIMIT 1
                """,
                (SOURCE_KEY,),
            )
            if not cursor.fetchone():
                status = "skipped_no_metadata_grant"
                return {"status": status, "request_count": 0}
        if source_grant and source_grant[1]:
            candidates = _discover_candidates(conn, cursor, client)
        _promote_reviewed_candidates(conn, cursor)
        if client.robot_parser is None:
            client.load_robots()
        board_pages = _collect_board_indexes(conn, cursor, client)
        thread_pages = _collect_thread_pages(conn, cursor, client)
        completed_articles = max(
            0, _completed_article_count(cursor) - completed_before
        )
        status = "success"
        return {
            "status": status,
            "request_count": client.request_count,
            "board_candidate_count": candidates,
            "board_page_count": board_pages,
            "thread_page_count": thread_pages,
            "completed_article_count": completed_articles,
        }
    except Exception as exc:
        conn.rollback()
        status = "failed"
        error = str(exc)
        raise
    finally:
        completed_articles = max(
            completed_articles,
            max(0, _completed_article_count(cursor) - completed_before),
        )
        cursor.execute(
            """
            UPDATE game_knowledge_crawl_runs
            SET finished_at = %s, request_count = %s, board_candidate_count = %s,
                board_page_count = %s, thread_page_count = %s,
                completed_article_count = %s, status = %s,
                error_summary = %s
            WHERE dag_run_id = %s
            """,
            (
                _now_utc(), client.request_count, candidates, board_pages,
                thread_pages, completed_articles, status,
                error[:60000] if error else None, dag_run_id,
            ),
        )
        conn.commit()
        conn.close()
        engine.dispose()
