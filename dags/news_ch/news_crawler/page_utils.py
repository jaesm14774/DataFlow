"""Publisher page helpers; publication timestamps are always Taipei wall time."""
import datetime as dt
import json
import re
from urllib.parse import urljoin, urlsplit

import requests

TAIPEI = dt.timezone(dt.timedelta(hours=8))
URL_COLUMNS = ['source', 'article_url', 'category', 'created_at', 'img', 'keyword']


def published_time(value):
    if not value:
        return None
    text = str(value).strip().replace('/', '-')
    try:
        parsed = dt.datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError:
        return None
    if parsed.tzinfo:
        parsed = parsed.astimezone(TAIPEI).replace(tzinfo=None)
    return parsed.strftime('%Y-%m-%d %H:%M:%S')


def meta(soup, name):
    tag = soup.find('meta', attrs={'property': name}) or soup.find('meta', attrs={'name': name})
    return str(tag.get('content') or '').strip() if tag else ''


def jsonld_nodes(soup):
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            pending = [json.loads(script.string or script.get_text())]
        except (ValueError, TypeError):
            continue
        while pending:
            node = pending.pop()
            if isinstance(node, dict):
                yield node
                pending.extend(x for x in node.values() if isinstance(x, (dict, list)))
            elif isinstance(node, list):
                pending.extend(node)


def same_article(left, right):
    a, b = urlsplit(str(left)), urlsplit(str(right))
    return (a.hostname, a.path.rstrip('/')) == (b.hostname, b.path.rstrip('/'))


def validate_publisher_url(url, *, allowed_hosts, article_path):
    """Reject publisher URLs outside their HTTPS host and article-path contract."""
    text = str(url or '').strip()
    parsed = urlsplit(text)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError('Publisher URL has an invalid port') from exc
    if (
        parsed.scheme != 'https'
        or (parsed.hostname or '').casefold() not in {
            str(host).casefold() for host in allowed_hosts
        }
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or article_path.fullmatch(parsed.path) is None
    ):
        raise ValueError('Publisher URL is outside the approved host and article path')
    return text


def safe_publisher_get(
    url, *, allowed_hosts, article_path, headers, timeout, max_redirects=3
):
    """Fetch an approved publisher page, validating each redirect before use."""
    current_url = validate_publisher_url(
        url, allowed_hosts=allowed_hosts, article_path=article_path
    )
    redirect_statuses = {301, 302, 303, 307, 308}
    for redirect_count in range(max_redirects + 1):
        response = requests.get(
            current_url, headers=headers, timeout=timeout, allow_redirects=False
        )
        if response.status_code in redirect_statuses:
            location = response.headers.get('Location')
            response.close()
            if not location or redirect_count >= max_redirects:
                raise ValueError('Publisher redirect is missing or exceeds the limit')
            current_url = validate_publisher_url(
                urljoin(current_url, location),
                allowed_hosts=allowed_hosts,
                article_path=article_path,
            )
            continue
        if 300 <= response.status_code < 400:
            response.close()
            raise ValueError('Publisher returned an unsupported redirect status')
        response.raise_for_status()
        return response
    raise ValueError('Publisher redirect limit exceeded')


def article_metadata(soup, article_url):
    for node in jsonld_nodes(soup):
        types = node.get('@type', [])
        types = [types] if isinstance(types, str) else types
        if not isinstance(types, list) or not {'Article', 'NewsArticle'} & set(types):
            continue
        entity = node.get('mainEntityOfPage', '')
        if isinstance(entity, dict):
            entity = entity.get('@id') or entity.get('url') or ''
        identity = node.get('url') or entity or node.get('@id')
        if identity and same_article(identity, article_url):
            return node
    return {}


def scoped_text(root):
    if root is None:
        return ''
    for node in root.select('script, style, figure, iframe, button, .ad_in_news, .adsbygoogle'):
        node.decompose()
    # Rich editors also use div/span paragraphs. Selecting only p loses their body.
    lines = root.get_text('\n', strip=True).splitlines()
    return '\n'.join(x for x in lines if x and not re.match(r'^(▲|▼|►|請繼續往下閱讀|不用抽 不用搶|-+廣告，請繼續往下閱讀)', x)).strip()


def validate_article(frame, observed_at=None):
    """Return a reason rather than silently accepting a nonempty error page."""
    if frame is None or len(frame) != 1:
        return 'not_one_article'
    row = frame.iloc[0]
    title = str(row.get('title') or '').strip()
    body = str(row.get('content') or '').strip()
    if not title or re.fullmatch(r'[.\s…]+', title):
        return 'placeholder_title'
    if title.casefold() in {'access denied', 'just a moment...', '403 forbidden', '404 not found'}:
        return 'error_page_title'
    if re.search(r'[\u0080-\u009f\ufffd]', title + body):
        return 'invalid_text_encoding'
    if not body:
        return 'empty_body'
    stamp = published_time(row.get('created_at'))
    if not stamp:
        return 'invalid_published_time'
    now = observed_at or dt.datetime.now(TAIPEI).replace(tzinfo=None)
    if dt.datetime.fromisoformat(stamp) > now + dt.timedelta(minutes=5):
        return 'future_published_time'
    return None
