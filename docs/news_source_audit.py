"""Read-only live crawler audit. Never imports all_news or writes to the DB.

python docs/news_source_audit.py --output docs/news_source_before.json --samples 6
"""
import argparse
import contextlib
import datetime as dt
import hashlib
import io
import json
import os
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dags'))
import common.config as config
config.user_agent_path = str(ROOT / 'dags/common/user_agent.csv')
import requests
from news_ch.news_crawler import (
    Anue, ChinaTimes, CNA, ETtoday, iThome, LibertyTimes, NewTalk,
    PTS, SETN, TheNewsLens, United, TVBS, PanSci, Reporter, EInfo,
)
from news_ch.news_crawler.page_utils import validate_article

CRAWLERS = [Anue, ChinaTimes, CNA, ETtoday, iThome, LibertyTimes,
            NewTalk, PTS, SETN, TheNewsLens, United, TVBS, PanSci, Reporter, EInfo]


def stratified(frame, count):
    """Round robin publisher categories/URL sections rather than head(count)."""
    groups = {}
    for _, row in frame.drop_duplicates('article_url').iterrows():
        key = str(row.get('category', '')).strip()
        if not key:
            parts = urlsplit(row['article_url']).path.strip('/').split('/')
            key = '/'.join(parts[:2])
        groups.setdefault(key, []).append(row)
    result = []
    while groups and len(result) < count:
        for key in list(groups):
            result.append(groups[key].pop(0))
            if not groups[key]:
                del groups[key]
            if len(result) == count:
                break
    return result


def audit(cls, samples, pages):
    crawler = cls()
    crawler._page = pages
    crawler._timeout = 15
    result = {'source': crawler._source, 'crawler': cls.__name__, 'articles': []}
    try:
        frame = crawler.get_article_url_from()
        result['url_count'] = len(frame)
        result['listing_categories'] = frame['category'].fillna('').value_counts().to_dict() if 'category' in frame else {}
        for row in stratified(frame, samples) if len(frame) else []:
            item = {'url': row['article_url'], 'listing_category': str(row.get('category', ''))}
            try:
                value = crawler.get_article_info(article_url=row['article_url'], tim=row.get('created_at', ''),
                    img=row.get('img', ''), keyword=row.get('keyword', ''), category=row.get('category', ''))
                data = value.iloc[0].to_dict()
                item['resolved_article_url'] = str(data.get('article_url') or '')
                for key in ('title', 'article_id', 'category', 'created_at', 'author'):
                    item[key] = str(data.get(key, '') or '').strip()
                content = str(data.get('content', '') or '').strip()
                item['body_chars'] = len(content)
                item['body_sha256'] = hashlib.sha256(content.encode()).hexdigest()
                item['body_lead'] = content[:100]
                item['validation_error'] = validate_article(value)
                item['valid'] = item['validation_error'] is None
            except Exception as exc:
                item['error'] = f'{type(exc).__name__}: {exc}'
                item['valid'] = False
            result['articles'].append(item)
    except Exception as exc:
        result['listing_error'] = f'{type(exc).__name__}: {exc}'
    result['valid_count'] = sum(x['valid'] for x in result['articles'])
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--samples', type=int, default=6)
    parser.add_argument('--pages', type=int, default=1)
    parser.add_argument('--sources', nargs='*')
    args = parser.parse_args()
    selected = [cls for cls in CRAWLERS if not args.sources or cls.__name__ in args.sources]
    results = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(audit, cls, args.samples, args.pages) for cls in selected]
        for future in as_completed(futures):
            value = future.result()
            results.append(value)
            print(json.dumps({k: v for k, v in value.items() if k != 'articles'}, ensure_ascii=False), flush=True)
    payload = {'observed_at': dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(),
               'db_writes': False, 'sources': sorted(results, key=lambda x: x['crawler'])}
    Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
