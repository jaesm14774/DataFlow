"""Live new-source storage check using connection-local TEMPORARY tables only.

Run inside the existing Airflow worker. It never changes news or staging tables.
"""
import datetime as dt
import json
import sys
import pandas as pd
import pymysql
from common.config import sql_configure_path
from news_ch.news_crawler import PanSci, Reporter, EInfo
from news_ch.news_crawler.page_utils import validate_article


def main():
    config=pd.read_csv(sql_configure_path,index_col='name')['value']
    connection=pymysql.connect(host=str(config['host']),port=int(config['port']),
        user=str(config['user']),password=str(config['password']),database='news_ch',charset='utf8mb4')
    results=[]
    try:
        with connection.cursor() as cursor:
            cursor.execute('CREATE TEMPORARY TABLE news_source_preflight LIKE news')
            cursor.execute('CREATE TEMPORARY TABLE news_diff_preflight LIKE news_diff')
            cursor.execute("SELECT column_name,character_maximum_length FROM information_schema.columns WHERE table_schema='news_ch' AND table_name='news' AND data_type='varchar'")
            limits=dict(cursor.fetchall())
            for cls in (PanSci,Reporter,EInfo):
                crawler=cls()
                urls=crawler.get_article_url_from()
                accepted=[]
                for _,row in urls.iterrows():
                    article=crawler.get_article_info(article_url=row['article_url'],tim=row['created_at'],img=row['img'],keyword=row['keyword'],category=row['category'])
                    reason=validate_article(article)
                    if reason:
                        raise ValueError(f'{crawler._source}: {reason}')
                    data=article.iloc[0].to_dict()
                    for field,limit in limits.items():
                        if field in data and data[field] is not None and len(str(data[field]))>limit:
                            raise ValueError(f'{crawler._source}: {field} exceeds {limit}')
                    columns=list(data)
                    names=','.join('`'+name+'`' for name in columns)
                    placeholders=','.join(['%s']*len(columns))
                    cursor.execute(f'INSERT INTO news_source_preflight ({names}) VALUES ({placeholders})',[data[name] for name in columns])
                    cursor.execute('INSERT INTO news_diff_preflight (article_id,source) VALUES (%s,%s)',(data['article_id'],data['source']))
                    accepted.append(data)
                cursor.execute('SELECT COUNT(*) FROM news_source_preflight WHERE source=%s',(crawler._source,))
                stored=cursor.fetchone()[0]
                results.append({'source':crawler._source,'urls':len(urls),'validated':len(accepted),
                    'temporary_stored':stored,'categories':sorted(set(str(a['category']) for a in accepted)),
                    'max_article_id_chars':max(len(a['article_id']) for a in accepted),
                    'max_body_chars':max(len(a['content']) for a in accepted)})
        receipt={'observed_at':dt.datetime.now(dt.timezone.utc).isoformat(),
            'persistent_writes':False,'tables':'connection-local TEMPORARY clones of news and news_diff',
            'sources':results,'all_matched':all(x['validated']==x['temporary_stored']==x['urls'] for x in results)}
        print('NEWS_STORAGE_PREFLIGHT '+json.dumps(receipt,ensure_ascii=False),flush=True)
        return receipt
    finally:
        connection.rollback()
        connection.close()


if __name__=='__main__':
    main()
