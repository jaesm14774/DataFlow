"""One-off read-only verification of a named natural news DAG run.

It does not trigger, clear or rerun tasks and never writes news/notification data.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import time

PROBE = r'''
import datetime as dt,json,sys
from airflow.models import DagRun,TaskInstance,XCom
from airflow.utils.session import create_session
from sqlalchemy import select
target=sys.argv[1]
with create_session() as session:
    run=session.execute(select(DagRun).where(DagRun.dag_id=='news_ch_collect_process',DagRun.run_id==target)).scalar_one_or_none()
    result={'run_id':target,'state':str(run.state) if run else 'not_created','tasks':{}}
    if run:
        tasks=session.execute(select(TaskInstance).where(TaskInstance.dag_id==run.dag_id,TaskInstance.run_id==target)).scalars().all()
        result['tasks']={t.task_id:str(t.state) for t in tasks}
    if run and str(run.state)=='success':
        parsed=XCom.get_one(dag_id=run.dag_id,run_id=target,task_id='collect_news',key='return_value',session=session)
        ingested=XCom.get_one(dag_id=run.dag_id,run_id=target,task_id='delete_and_insert',key='return_value',session=session)
        result.update({'collection':parsed,'ingestion':ingested})
if result['state']=='success':
    import pandas as pd,pymysql
    from common.config import sql_configure_path
    from news_ch.news_crawler.page_utils import validate_article
    cfg=pd.read_csv(sql_configure_path,index_col='name')['value']
    connection=pymysql.connect(host=str(cfg['host']),port=int(cfg['port']),user=str(cfg['user']),password=str(cfg['password']),database='news_ch',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor,connect_timeout=10,read_timeout=25)
    stamp=result['ingestion']['create_time_tpe']
    with connection.cursor() as cursor:
        # news has a unique (source, article_id) index, but no create_time index.
        # Drive indexed lookups from the completed run's small staging key ledger.
        cursor.execute('SELECT COUNT(*) AS count FROM news_diff')
        ledger_count=cursor.fetchone()['count']
        cursor.execute('SELECT n.article_id,n.source,n.article_url,n.title,n.content,n.category,n.created_at,n.create_time FROM news_diff d STRAIGHT_JOIN news n FORCE INDEX (unique_index) ON n.source=d.source AND n.article_id=d.article_id WHERE n.create_time=%s',(stamp,))
        rows=cursor.fetchall()
    connection.close()
    counts,categories,bad,keys={}, {}, [],set()
    duplicates=0
    for row in rows:
        source=row['source']; counts[source]=counts.get(source,0)+1
        category=str(row.get('category') or '')
        source_categories=categories.setdefault(source,{})
        source_categories[category]=source_categories.get(category,0)+1
        reason=validate_article(pd.DataFrame([row]))
        if reason: bad.append({'source':source,'article_id':row['article_id'],'reason':reason})
        key=(source,row['article_id'])
        if key in keys: duplicates+=1
        keys.add(key)
    expected={source['source']:source['accepted_count'] for source in result['collection']['sources']}
    result['persisted']={'create_time_tpe':stamp,'rows':len(rows),'staging_key_count':ledger_count,'sources':counts,'categories':categories,'invalid':bad,'duplicates':duplicates,'lookup':'news_diff key ledger joined through unique_index(source,article_id)'}
    result['checks']={'all_three_tasks_succeeded':all(result['tasks'].get(t)=='success' for t in ('collect_article_url','collect_news','delete_and_insert')),
        'source_counts_match':counts=={k:v for k,v in expected.items() if v},
        'all_15_sources_nonempty':len(counts)==15 and all(counts.values()),
        'row_count_matches_receipts':len(rows)==ledger_count==result['ingestion']['inserted_count']==result['collection']['accepted_count'],
        'article_fields_valid':not bad,'no_duplicate_keys':duplicates==0}
    result['verified']=all(result['checks'].values())
print('NEWS_RUNTIME_PROBE '+json.dumps(result,ensure_ascii=False,default=str))
'''


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--deadline',required=True)
    args=parser.parse_args()
    deadline=dt.datetime.fromisoformat(args.deadline.replace('Z','+00:00'))
    previous=None
    failures=0
    while dt.datetime.now(dt.timezone.utc)<deadline:
        try:
            completed=subprocess.run(['docker','exec','-e','PYTHONPATH=/opt/airflow/dags',
                'dataflow-airflow-worker-1','python','-c',PROBE,args.run_id],capture_output=True,text=True,encoding='utf-8',timeout=45)
        except subprocess.TimeoutExpired:
            failures+=1
            if failures>=3:
                raise RuntimeError('Three metadata probe timeouts')
            time.sleep(45)
            continue
        line=next((line for line in completed.stdout.splitlines() if line.startswith('NEWS_RUNTIME_PROBE ')),None)
        if completed.returncode or line is None:
            failures+=1
            if failures>=3:
                raise RuntimeError('Three metadata probe failures: '+completed.stderr[-1200:])
            time.sleep(45)
            continue
        failures=0
        result=json.loads(line.removeprefix('NEWS_RUNTIME_PROBE '))
        result['observed_at']=dt.datetime.now(dt.timezone.utc).isoformat()
        result['read_only']=True
        Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        state=(result['state'],tuple(sorted(result['tasks'].items())))
        if state!=previous:
            print(json.dumps({'run_id':args.run_id,'state':result['state'],'tasks':result['tasks'],'verified':result.get('verified')},ensure_ascii=False),flush=True)
            previous=state
        if result['state'] in ('success','failed'):
            return 0 if result.get('verified') else 1
        time.sleep(45)
    raise TimeoutError('Named natural DAG run did not finish before deadline')


if __name__=='__main__':
    raise SystemExit(main())
