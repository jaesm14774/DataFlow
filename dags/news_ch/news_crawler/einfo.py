"""Environmental Information Center's current public SSR pages."""
import re
from urllib.parse import urljoin, urlsplit
import pandas as pd
from bs4 import BeautifulSoup
from news_ch.news_crawler.base_process import NewsLogic
from news_ch.news_crawler.page_utils import (
    URL_COLUMNS, meta, published_time, safe_publisher_get, scoped_text,
)


class EInfo(NewsLogic):
    _source = '環境資訊中心'
    _domain_url = 'https://e-info.org.tw'
    _sections = ('news', 'opinion', 'column', 'supplement')
    _section_limit = 12

    def get_article_url_from(self):
        rows, seen = [], set()
        for section in self._sections:
            response = safe_publisher_get(
                f'{self._domain_url}/section/{section}',
                allowed_hosts={urlsplit(self._domain_url).hostname},
                article_path=re.compile(rf'/section/{re.escape(section)}'),
                headers=self._headers, timeout=self._timeout,
            )
            soup = BeautifulSoup(response.content, 'lxml')
            count = 0
            for link in soup.select('main a[href]'):
                url = urljoin(self._domain_url, link.get('href', ''))
                if not re.fullmatch(r'https://e-info\.org\.tw/node/\d+', url) or url in seen:
                    continue
                seen.add(url)
                rows.append({'source': self._source, 'article_url': url, 'category': section,
                             'created_at': '', 'img': '', 'keyword': ''})
                count += 1
                if count >= self._section_limit:
                    break
        if not rows:
            raise ValueError('EInfo section pages contain no article URLs')
        return pd.DataFrame(rows, columns=URL_COLUMNS)

    def get_article_info(self, article_url, tim, img, keyword, category):
        if not re.fullmatch(r'https://e-info\.org\.tw/node/\d+', article_url):
            raise ValueError('Article URL is outside EInfo allowlist')
        response = safe_publisher_get(
            article_url, allowed_hosts={'e-info.org.tw'},
            article_path=re.compile(r'/node/\d+'),
            headers=self._headers, timeout=self._timeout,
        )
        soup = BeautifulSoup(response.content, 'lxml')
        root = soup.select_one('main article[class*="post-content__Content-"]')
        title = soup.select_one('main h1')
        timestamp = published_time(meta(soup, 'article:published_time'))
        # Draft editor paragraphs are divs, not p tags.
        content = scoped_text(root)
        if not title or not timestamp or not content:
            raise ValueError('EInfo article lacks scoped body, title or publication time')
        categories = soup.select('main aside a[href*="/section/"], main aside a[href*="/category/"]')
        authors = soup.select('main aside a[href*="/author/"]')
        tags = soup.select('main aside a[href*="/tag/"]')
        return self.build_article_dataframe(article_id=article_url.rsplit('/', 1)[-1],
            title=title.get_text(' ', strip=True), created_at=timestamp, content=content,
            author=';'.join(x.get_text(' ', strip=True) for x in authors),
            category=';'.join(x.get_text(' ', strip=True) for x in categories) or category,
            keyword=';'.join(x.get_text(' ', strip=True) for x in tags),
            article_url=article_url, img=meta(soup, 'og:image') or img)
