"""Official RSS discovery plus scoped public article pages."""
import datetime as dt
import hashlib
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urljoin

import pandas as pd
from bs4 import BeautifulSoup
from news_ch.news_crawler.base_process import NewsLogic
from news_ch.news_crawler.page_utils import (
    URL_COLUMNS, meta, published_time, safe_publisher_get, scoped_text,
    same_article,
)


class RSSNews(NewsLogic):
    _feed_url = ''
    _host = ''
    _article_path = ''

    def article_id(self, path):
        return path.rstrip('/').split('/')[-1]

    def get_article_url_from(self):
        feed_path = re.compile(re.escape(urlsplit(self._feed_url).path))
        response = safe_publisher_get(
            self._feed_url, allowed_hosts={self._host}, article_path=feed_path,
            headers=self._headers, timeout=self._timeout,
        )
        root = ET.fromstring(response.content)
        rows = []
        for item in root.findall('./channel/item'):
            url = (item.findtext('link') or '').strip()
            parsed = urlsplit(url)
            if parsed.scheme != 'https' or parsed.hostname != self._host or not re.fullmatch(self._article_path, parsed.path):
                continue
            try:
                timestamp = published_time(parsedate_to_datetime(item.findtext('pubDate') or '').isoformat())
            except (ValueError, TypeError):
                timestamp = None
            categories = [x.text.strip() for x in item.findall('category') if x.text and x.text.strip()]
            enclosure = item.find('enclosure')
            rows.append({'source': self._source, 'article_url': url,
                         'category': categories[0] if categories else '',
                         'created_at': timestamp, 'keyword': ';'.join(categories),
                         'img': enclosure.get('url', '') if enclosure is not None else ''})
        if not rows:
            raise ValueError(f'{self._source} official feed contains no recognized article URLs')
        return pd.DataFrame(rows, columns=URL_COLUMNS).drop_duplicates('article_url').reset_index(drop=True)

    def get_article_info(self, article_url, tim, img, keyword, category):
        parsed = urlsplit(article_url)
        if parsed.scheme != 'https' or parsed.hostname != self._host or not re.fullmatch(self._article_path, parsed.path):
            raise ValueError('Article URL is outside publisher allowlist')
        response = safe_publisher_get(
            article_url, allowed_hosts={self._host},
            article_path=re.compile(self._article_path),
            headers=self._headers, timeout=self._timeout,
        )
        if not same_article(response.url, article_url):
            raise ValueError('Article redirected to a different article or publisher')
        soup = BeautifulSoup(response.content, 'lxml')
        canonical=soup.select_one('link[rel="canonical"]')
        if canonical and canonical.get('href') and not same_article(urljoin(response.url,canonical['href']),article_url):
            raise ValueError('Article canonical URL does not match the feed item')
        title, body, author, section = self.parse_page(soup)
        timestamp = published_time(meta(soup, 'article:published_time')) or published_time(tim)
        if not title or not body or not timestamp:
            raise ValueError('Article lacks title, public body or publication date')
        return self.build_article_dataframe(article_id=self.article_id(parsed.path),
            title=title, created_at=timestamp, content=body, author=author,
            category=section or category, keyword=keyword, article_url=article_url,
            img=meta(soup, 'og:image') or img)


class PanSci(RSSNews):
    _source = '泛科學'
    _feed_url = 'https://pansci.asia/feed'
    _host = 'pansci.asia'
    _article_path = r'/archives/\d+/?'

    def parse_page(self, soup):
        # Later load_post wrappers are recommendations and must never enter body.
        article = soup.select_one('#load_post_1')
        if article is None:
            raise ValueError('PanSci public article wrapper is missing')
        title = article.select_one('h1')
        author = article.select_one('a[href*="/archives/author/"]')
        categories=article.select('.single_top_cat')
        return (title.get_text(' ', strip=True) if title else '',
                scoped_text(article.select_one('.post-content-container')),
                author.get_text(' ', strip=True) if author else '',
                ';'.join(x.get_text(' ',strip=True) for x in categories))


class Reporter(RSSNews):
    _source = '報導者'
    _feed_url = 'https://www.twreporter.org/a/rss2.xml'
    _host = 'www.twreporter.org'
    _article_path = r'/a/[a-z0-9-]+/?'

    def article_id(self, path):
        # Article slugs exceed the existing varchar(50) key in all three tables.
        # Keep the complete source URL; hash its canonical path for a stable key.
        return 'reporter_' + hashlib.sha256(path.rstrip('/').encode('utf-8')).hexdigest()[:40]

    def parse_page(self, soup):
        # Public SSR contains the full report, not just the RSS description.
        body = soup.select_one('#article-body [class*="article-page__ContentBlock"]')
        title = meta(soup, 'og:title').removesuffix(' - 報導者 The Reporter')
        authors = soup.select('#article-body a[href*="/authors/"]')
        names = list(dict.fromkeys(x.get_text(' ', strip=True) for x in authors))
        return title, scoped_text(body), ';'.join(names), meta(soup, 'article:section')
