"""
Automated test for every news_ch crawler source.
Tests get_article_url_from() and get_article_info() for at least 10 articles per source.
Reports missing fields and errors.
"""
import sys
import os
import time
import json
import traceback
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

os.environ.setdefault('NEWS_CH_TEST_MODE', '1')

import pandas as pd
import numpy as np

local_ua_path = os.path.join(os.path.dirname(__file__), '..', 'dags', 'common', 'user_agent.csv')

import common.config as cfg
cfg.user_agent_path = os.path.abspath(local_ua_path)

import news_ch.news_crawler.base_process as bp
bp.user_agent = pd.read_csv(cfg.user_agent_path, sep=r'\n').useragent.tolist()

from news_ch.news_crawler import (
    Anue, ChinaTimes, CNA, ETtoday, iThome,
    LibertyTimes, NewTalk, PTS, SETN,
    TheNewsLens, United, TVBS,
)

REQUIRED_FIELDS = ['article_id', 'title', 'created_at', 'content', 'author',
                   'category', 'keyword', 'article_url', 'img', 'source']
MIN_ARTICLES = 10

CRAWLERS = {
    '鉅亨網': Anue,
    '中時日報': ChinaTimes,
    '中央社': CNA,
    'ETtoday': ETtoday,
    'iThome': iThome,
    '自由時報': LibertyTimes,
    'newtalk': NewTalk,
    '公視新聞': PTS,
    '三立新聞': SETN,
    'thenewslens': TheNewsLens,
    '聯合報': United,
    'TVBS': TVBS,
}


def is_empty(val):
    if val is None:
        return True
    if isinstance(val, float) and np.isnan(val):
        return True
    s = str(val).strip()
    return s == '' or s == ' ' or s == 'None'


def test_source(name, crawler_cls):
    result = {
        'source': name,
        'url_fetch_ok': False,
        'url_count': 0,
        'url_error': None,
        'articles_tested': 0,
        'articles_ok': 0,
        'articles_failed': 0,
        'field_missing': {},
        'errors': [],
    }

    for f in REQUIRED_FIELDS:
        result['field_missing'][f] = 0

    crawler = crawler_cls()
    print(f'\n{"="*70}')
    print(f'[TEST] {name} - get_article_url_from()')
    print(f'{"="*70}')

    try:
        url_df = crawler.get_article_url_from()
        if url_df is None or len(url_df) == 0:
            result['url_error'] = 'empty result'
            print(f'  [WARN] 取得 0 筆 URL')
            return result
        result['url_fetch_ok'] = True
        result['url_count'] = len(url_df)
        print(f'  [OK] 取得 {len(url_df)} 筆 URL')
    except Exception as e:
        result['url_error'] = f'{type(e).__name__}: {e}'
        print(f'  [FAIL] {result["url_error"]}')
        traceback.print_exc()
        return result

    test_count = min(MIN_ARTICLES, len(url_df))
    sample = url_df.head(test_count)

    for idx, row in sample.iterrows():
        url = row.get('article_url', '')
        print(f'\n  [{result["articles_tested"]+1}/{test_count}] {url[:80]}...')

        try:
            crawler_fresh = crawler_cls()
            article_df = crawler_fresh.get_article_info(
                article_url=row.get('article_url', ''),
                tim=row.get('created_at', ''),
                img=row.get('img', ''),
                keyword=row.get('keyword', ''),
                category=row.get('category', ''),
            )
            result['articles_tested'] += 1

            if article_df is None or len(article_df) == 0:
                result['articles_failed'] += 1
                result['errors'].append({'url': url, 'error': 'empty dataframe'})
                print(f'    [FAIL] empty dataframe')
                continue

            row_data = article_df.iloc[0]
            missing_in_this = []
            for f in REQUIRED_FIELDS:
                val = row_data.get(f, None)
                if is_empty(val):
                    result['field_missing'][f] += 1
                    missing_in_this.append(f)

            if missing_in_this:
                print(f'    [WARN] missing: {", ".join(missing_in_this)}')
            else:
                print(f'    [OK] all fields present')
                result['articles_ok'] += 1

            time.sleep(0.5)

        except Exception as e:
            result['articles_tested'] += 1
            result['articles_failed'] += 1
            err_msg = f'{type(e).__name__}: {e}'
            result['errors'].append({'url': url, 'error': err_msg})
            print(f'    [FAIL] {err_msg}')
            traceback.print_exc()
            time.sleep(0.5)

    return result


def main():
    print(f'News Crawler Test - {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Testing {len(CRAWLERS)} sources, min {MIN_ARTICLES} articles each\n')

    all_results = []

    for name, cls in CRAWLERS.items():
        try:
            r = test_source(name, cls)
            all_results.append(r)
        except Exception as e:
            print(f'\n[FATAL] {name}: {type(e).__name__}: {e}')
            traceback.print_exc()
            all_results.append({
                'source': name,
                'url_fetch_ok': False,
                'url_count': 0,
                'url_error': f'FATAL: {e}',
                'articles_tested': 0,
                'articles_ok': 0,
                'articles_failed': 0,
                'field_missing': {f: 0 for f in REQUIRED_FIELDS},
                'errors': [{'url': '', 'error': f'FATAL: {e}'}],
            })

    print(f'\n\n{"="*70}')
    print(f'SUMMARY REPORT')
    print(f'{"="*70}')

    for r in all_results:
        status = 'OK' if r['url_fetch_ok'] and r['articles_failed'] == 0 else 'ISSUE'
        print(f'\n[{status}] {r["source"]}')
        print(f'  URL fetch: {"OK" if r["url_fetch_ok"] else "FAIL"} ({r["url_count"]} URLs)')
        if r['url_error']:
            print(f'  URL error: {r["url_error"]}')
        print(f'  Articles: tested={r["articles_tested"]}, ok={r["articles_ok"]}, failed={r["articles_failed"]}')

        missing_fields = {k: v for k, v in r['field_missing'].items() if v > 0}
        if missing_fields:
            print(f'  Missing fields:')
            for f, count in missing_fields.items():
                pct = count / max(r['articles_tested'], 1) * 100
                print(f'    - {f}: {count}/{r["articles_tested"]} ({pct:.0f}%)')

        if r['errors']:
            print(f'  Errors ({len(r["errors"])}):')
            for e in r['errors'][:3]:
                print(f'    - {e["error"][:100]}')

    output_path = os.path.join(os.path.dirname(__file__), 'news_crawlers_test_results.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f'\nResults saved to {output_path}')

    problem_sources = [r for r in all_results if not r['url_fetch_ok'] or r['articles_failed'] > 0 or
                       any(v > r['articles_tested'] * 0.3 for v in r['field_missing'].values() if r['articles_tested'] > 0)]
    if problem_sources:
        print(f'\n[ATTENTION] Sources with issues:')
        for r in problem_sources:
            print(f'  - {r["source"]}')
    else:
        print(f'\n[ALL PASS] All sources are working correctly!')

    return all_results


if __name__ == '__main__':
    main()
