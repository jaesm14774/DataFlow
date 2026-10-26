import re
import datetime
import pandas as pd
import numpy as np
from lib.get_sql import *
from lib.log_process_execution import BaseLogRecord
from common.config import *
from lib.common_tool import task_wrapper
from news_ch.news_crawler import (
    Anue, ChinaTimes, CNA, ETtoday, iThome,
    LibertyTimes, NewTalk, PTS, SETN,
    TheNewsLens, United, TVBS, PanSci, Reporter, EInfo,
)
from news_ch.utils import is_valid_url, clean_text, clean_time_format
from news_ch.news_crawler.page_utils import validate_article

app_name = 'news_ch'
database_name = 'news_ch'
table_name = 'news'
_date_only_news_sources = frozenset({'ithome'})

#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)
_default_log_record = log_record

task_dict={
    1: Anue(),
    # 2: Apple(), #蘋果日報已結束營業
    3: ChinaTimes(),
    4: CNA(),
    5: ETtoday(),
    6: iThome(),
    7: LibertyTimes(),
    8: NewTalk(),
    9: PTS(),
    10: SETN(),
    11: TheNewsLens(), 
    12: United(),
    13: TVBS(),
    14: PanSci(),
    15: Reporter(),
    16: EInfo(),
}

source_dict={}
for k in task_dict.keys():
    source_dict[task_dict[k]._source]=task_dict[k]

def _source_row_counts(frame):
    if frame is None or frame.empty or 'source' not in frame.columns:
        return {}
    counts=frame['source'].fillna('未知來源').astype(str).value_counts(dropna=False)
    return {source:int(count) for source,count in counts.items()}

def _source_removed_counts(before, after):
    before_counts=_source_row_counts(before)
    after_counts=_source_row_counts(after)
    return {
        source:max(0,count-after_counts.get(source,0))
        for source,count in before_counts.items()
    }

def _non_blank_news_text(series):
    return series.fillna('').astype(str).str.strip().ne('')

def _new_connection():
    connection=pd.read_csv(sql_configure_path,index_col='name')
    return get_sql(connection.loc['host','value'],
                   int(connection.loc['port','value']),
                   connection.loc['user','value'],
                   connection.loc['password','value'],database_name)

conn=cursor=engine=None

def _ensure_runtime(count_before=False,record=None):
    global conn,cursor,engine
    if conn is None or not getattr(conn,'open',False):
        conn,cursor,engine=_new_connection()
    if count_before:
        before_count=pd.read_sql_query(
            f'select count(id) as N from {table_name}',engine
        )['N'].iloc[0]
        (record or _default_log_record).set_before_count(before_count)
        print(f'[中文新聞] DB 既有筆數={before_count}')
    return conn,cursor,engine

#收集所有文章網址
@task_wrapper
def collect_article_url(conn=None,cursor=None,engine=None,log_record=None):
    conn,cursor,engine=_ensure_runtime()
    log_record=log_record or _default_log_record
    try:
        frames=[]
        for k in task_dict.keys():
            crawler=task_dict[k]
            try:
                frame=crawler.get_article_url_from()
                if frame is not None and not frame.empty:
                    frames.append(frame)
            except Exception as source_error:
                log_record.raise_error(
                    f'{crawler._source} article URL collection failed: {source_error!r}'
                )
                print(
                    f'[中文新聞] 來源失敗，略過並繼續 | source={crawler._source} | '
                    f'{type(source_error).__name__}: {source_error}'
                )

        if not frames:
            raise RuntimeError('All enabled news sources returned no article URLs')

        TEMP=pd.concat(frames, ignore_index=True)
        #刪除無效網址
        TEMP=TEMP[TEMP['article_url'].map(is_valid_url)]
        #刪除重複的網址
        TEMP=TEMP[~TEMP.duplicated(subset=['article_url'])]
        if TEMP.empty:
            raise RuntimeError('Enabled news sources returned no valid article URLs')

        # TRUNCATE needs an exclusive metadata lock and can stall behind long readers.
        cursor.execute('delete from article_url')
        conn.commit()
        #打亂順序
        TEMP=TEMP.iloc[np.random.choice(range(0,TEMP.shape[0]),size=TEMP.shape[0],replace=False),:]
        TEMP=TEMP.reset_index(drop=True)
        
        TEMP.to_sql('article_url',engine,index=False,if_exists='append')
        print('[中文新聞] 各站列表網址已寫入 article_url')
    except Exception as e:
        log_record.raise_error(repr(e))
        log_record.insert_to_log_record()
        print(f'[中文新聞] 收網址失敗 | {type(e).__name__}: {e}')
        raise e

    # collect_news(conn,cursor,engine,log_record)


#解析文章函數匯集
def get_info(article_url,source,category,tim,img,keyword):
    return source_dict[source].get_article_info(article_url=article_url,
                                                category=category,
                                                tim=tim,
                                                img=img,
                                                keyword=keyword)

@task_wrapper
def collect_news(conn=None,cursor=None,engine=None,log_record=None):
    conn,cursor,engine=_ensure_runtime()
    log_record=log_record or _default_log_record
    _key_columns=['article_id','source']
    
    try:
        TEMP=pd.read_sql_query('select * from article_url',engine)
        
        #收集所有文章的網址
        if TEMP.shape[0] == 0:
            # Do not let a successful empty fetch replay a previous run's staging rows.
            cursor.execute('truncate table news_diff')
            cursor.execute('truncate table news_temp')
            conn.commit()
            conn.close()
            engine.dispose()
            return {
                'status':'no_article_urls',
                'source_count':0,
                'url_count':0,
                'parsed_count':0,
                'valid_time_count':0,
                'accepted_count':0,
                'title_blank_or_whitespace_rejected':0,
                'content_blank_or_whitespace_rejected':0,
                'duplicate_source_article_id_rejected':0,
                'other_post_time_filter_rejected':0,
                'post_time_filter_rejection_count_delta':0,
                'post_time_filter_rejections_reconciled':True,
                'sources':[],
            }
        url_counts=_source_row_counts(TEMP)
        
        #得到所有文章的資訊
        D_temp=[]
        validation_rejections={}

        for index, row in TEMP.iterrows():
            try:
                print(f'[中文新聞] 解析文章 | url={row["article_url"]}')
                article=get_info(article_url=row["article_url"], category=row["category"], tim=row["created_at"], source=row["source"], img=row["img"], keyword=row["keyword"])
                reason=validate_article(article)
                if reason:
                    source_rejections=validation_rejections.setdefault(row['source'],{})
                    source_rejections[reason]=source_rejections.get(reason,0)+1
                    print(f'[中文新聞] 文章驗證拒收 | source={row["source"]} reason={reason} url={row["article_url"]}')
                    if reason=='not_one_article':
                        continue
                    # Retain raw parsed counts and the established quarantine ledger.
                    if reason in {'invalid_published_time','future_published_time'}:
                        article.loc[:,'created_at']=None
                article['_quality_rejected']=reason in {
                    'placeholder_title','error_page_title','invalid_text_encoding'
                }
                D_temp.append(article)
            except Exception as e:
                print(f'[中文新聞] 單篇略過 | url={row["article_url"]} | {type(e).__name__}: {e}')
                continue
        
        if not D_temp:
            raise RuntimeError('No source returned a valid article; refusing to replay staging rows')
        D_temp=pd.concat(D_temp,axis=0)
        D_temp['article_id']=D_temp['article_id'].astype('str')
        parsed_counts=_source_row_counts(D_temp)
        
        #檢查異常時間
        D_temp=D_temp[~D_temp.created_at.isnull()]
        D_temp['created_at']=D_temp.created_at.map(clean_time_format)
        D_temp=D_temp[~D_temp.created_at.isnull()]
        D_temp['created_at']=D_temp['created_at'].astype('datetime64[ns]')   
        valid_time_counts=_source_row_counts(D_temp)
        
        #刪除無法收集到的標題與內文的文章
        quality_filtered=D_temp.loc[~D_temp['_quality_rejected']].copy()
        quality_rejected_counts=_source_removed_counts(D_temp,quality_filtered)
        quality_filtered=quality_filtered.drop(columns=['_quality_rejected'])
        title_filtered=quality_filtered.loc[_non_blank_news_text(quality_filtered['title'])]
        title_rejected_counts=_source_removed_counts(quality_filtered,title_filtered)
        content_filtered=title_filtered.loc[
            _non_blank_news_text(title_filtered['content'])
        ]
        content_rejected_counts=_source_removed_counts(title_filtered,content_filtered)
        content_filtered=content_filtered.sort_values(['created_at'],ascending=False)
        duplicate_mask=content_filtered.duplicated(subset=['source','article_id'])
        duplicate_rejected_counts=_source_row_counts(content_filtered.loc[duplicate_mask])
        D_temp=content_filtered.loc[~duplicate_mask].copy()
        D_temp.loc[D_temp.created_at == ' ','created_at']=None
        D_temp.content=D_temp.content.apply(clean_text,special_sign='\"')
        D_temp.title=D_temp.title.apply(clean_text,special_sign='\"')
        D_temp=D_temp.reset_index(drop=True)
        accepted_counts=_source_row_counts(D_temp)
        freshness_observed_at=datetime.datetime.now()+datetime.timedelta(hours=8)
        freshness_cutoff=freshness_observed_at-datetime.timedelta(hours=48)
        recorded_times=D_temp['created_at']
        source_keys=D_temp['source'].fillna('未知來源').astype(str).str.strip().str.casefold()
        date_only_mask=(
            source_keys.isin(_date_only_news_sources)
            | (recorded_times.dt.time == datetime.time.min)
        )
        recorded_dates=recorded_times.dt.date
        recorded_datetime_recent_mask=(
            ~date_only_mask
            & (recorded_times >= freshness_cutoff)
            & (recorded_times <= freshness_observed_at)
        )
        date_only_recent_mask=(
            date_only_mask
            & (recorded_dates > freshness_cutoff.date())
            & (recorded_dates <= freshness_observed_at.date())
        )
        date_only_boundary_mask=(
            date_only_mask & (recorded_dates == freshness_cutoff.date())
        )
        older_by_recorded_precision_mask=(
            (~date_only_mask & (recorded_times < freshness_cutoff))
            | (date_only_mask & (recorded_dates < freshness_cutoff.date()))
        )
        future_recorded_time_mask=recorded_times > freshness_observed_at
        recorded_datetime_recent_counts=_source_row_counts(
            D_temp.loc[recorded_datetime_recent_mask]
        )
        date_only_recent_counts=_source_row_counts(D_temp.loc[date_only_recent_mask])
        date_only_boundary_counts=_source_row_counts(
            D_temp.loc[date_only_boundary_mask]
        )
        older_by_recorded_precision_counts=_source_row_counts(
            D_temp.loc[older_by_recorded_precision_mask]
        )
        future_recorded_time_counts=_source_row_counts(
            D_temp.loc[future_recorded_time_mask]
        )
        configured_sources={crawler._source for crawler in task_dict.values()}
        observed_sources=(
            set(url_counts) | set(parsed_counts) | set(valid_time_counts)
            | set(accepted_counts)
        )
        source_coverage=[]
        for source in sorted(configured_sources | observed_sources):
            url_count=url_counts.get(source,0)
            parsed_count=parsed_counts.get(source,0)
            valid_time_count=valid_time_counts.get(source,0)
            accepted_count=accepted_counts.get(source,0)
            fetch_or_parse_rejected=max(0,url_count-parsed_count)
            invalid_time_quarantined=max(0,parsed_count-valid_time_count)
            content_or_duplicate_rejected=max(0,valid_time_count-accepted_count)
            title_rejected=title_rejected_counts.get(source,0)
            content_rejected=content_rejected_counts.get(source,0)
            duplicate_rejected=duplicate_rejected_counts.get(source,0)
            quality_rejected=quality_rejected_counts.get(source,0)
            other_post_time_filter_rejected=max(
                0,
                content_or_duplicate_rejected
                - title_rejected
                - content_rejected
                - duplicate_rejected
                - quality_rejected,
            )
            post_time_filter_rejection_count_delta=(
                content_or_duplicate_rejected
                - title_rejected
                - content_rejected
                - duplicate_rejected
                - quality_rejected
                - other_post_time_filter_rejected
            )
            post_time_filter_rejections_reconciled=(
                post_time_filter_rejection_count_delta==0
            )
            recorded_datetime_recent=recorded_datetime_recent_counts.get(source,0)
            date_only_recent=date_only_recent_counts.get(source,0)
            date_only_boundary=date_only_boundary_counts.get(source,0)
            older_by_recorded_precision=older_by_recorded_precision_counts.get(source,0)
            future_recorded_time=future_recorded_time_counts.get(source,0)
            recent_recorded_count=recorded_datetime_recent+date_only_recent
            if not accepted_count:
                freshness_status='no_accepted_articles'
            elif recent_recorded_count and older_by_recorded_precision:
                freshness_status='mixed_recent_and_old'
            elif recent_recorded_count:
                freshness_status='recent_by_recorded_time_or_date'
            elif date_only_boundary:
                freshness_status='cutoff_date_time_unverified'
            elif older_by_recorded_precision:
                freshness_status='stale_by_recorded_time_or_date'
            else:
                freshness_status='unclassified'
            if not url_count:
                source_status='no_urls'
            elif not parsed_count:
                source_status='no_parsed_articles'
            elif not valid_time_count:
                source_status='no_valid_time'
            elif not accepted_count:
                source_status='empty_after_filter'
            elif accepted_count < url_count:
                source_status='partial'
            else:
                source_status='complete'
            source_coverage.append({
                'source':str(source),
                'status':source_status,
                'url_count':url_count,
                'parsed_count':parsed_count,
                'valid_time_count':valid_time_count,
                'accepted_count':accepted_count,
                'fetch_or_parse_rejected':fetch_or_parse_rejected,
                'invalid_time_quarantined':invalid_time_quarantined,
                'content_or_duplicate_rejected':content_or_duplicate_rejected,
                'title_blank_or_whitespace_rejected':title_rejected,
                'content_blank_or_whitespace_rejected':content_rejected,
                'duplicate_source_article_id_rejected':duplicate_rejected,
                'other_post_time_filter_rejected':other_post_time_filter_rejected,
                'post_time_filter_rejection_count_delta':
                    post_time_filter_rejection_count_delta,
                'post_time_filter_rejections_reconciled':
                    post_time_filter_rejections_reconciled,
                'freshness_status':freshness_status,
                'recorded_datetime_within_48h':recorded_datetime_recent,
                'date_only_after_cutoff_date':date_only_recent,
                'date_only_cutoff_date_time_unverified':date_only_boundary,
                'older_by_recorded_precision':older_by_recorded_precision,
                'future_recorded_time':future_recorded_time,
                'validation_rejections':validation_rejections.get(source,{}),
                'article_quality_rejected':quality_rejected,
                'accepted_categories':{
                    str(category):int(count) for category,count in
                    D_temp.loc[D_temp['source']==source,'category'].fillna('').value_counts().items()
                },
            })
            print(
                '[中文新聞][source_coverage] '
                f'source={source} urls={url_count} parsed={parsed_count} '
                f'valid_time={valid_time_count} accepted={accepted_count} '
                f'fetch_or_parse_rejected={fetch_or_parse_rejected} '
                f'invalid_time_quarantined={invalid_time_quarantined} '
                f'content_or_duplicate_rejected={content_or_duplicate_rejected} '
                f'title_blank_or_whitespace_rejected={title_rejected} '
                f'content_blank_or_whitespace_rejected={content_rejected} '
                f'duplicate_source_article_id_rejected={duplicate_rejected} '
                f'article_quality_rejected={quality_rejected} '
                f'other_post_time_filter_rejected={other_post_time_filter_rejected} '
                f'post_time_filter_rejection_count_delta='
                f'{post_time_filter_rejection_count_delta} '
                f'freshness_status={freshness_status} '
                f'recorded_datetime_within_48h={recorded_datetime_recent} '
                f'date_only_after_cutoff_date={date_only_recent} '
                f'date_only_cutoff_date_time_unverified={date_only_boundary} '
                f'older_by_recorded_precision={older_by_recorded_precision} '
                f'future_recorded_time={future_recorded_time} '
                f'status={source_status}'
            )

        reconciliation_failures=[
            (
                row['source'],
                row['post_time_filter_rejection_count_delta'],
            )
            for row in source_coverage
            if not row['post_time_filter_rejections_reconciled']
        ]
        if reconciliation_failures:
            failure_summary=', '.join(
                f'{source}:delta={delta}'
                for source,delta in reconciliation_failures
            )
            raise ValueError(
                'news post-time-filter rejection counts do not reconcile: '
                + failure_summary
            )
        
        cursor.execute('truncate table news_diff')
        cursor.execute('truncate table news_temp')
        conn.commit()
        
        D_temp.loc[:,_key_columns].to_sql('news_diff',engine,index=0,if_exists='append')
        D_temp.to_sql('news_temp',engine,index=False,if_exists='append')
    except Exception as e:
        log_record.raise_error(repr(e))
        log_record.insert_to_log_record()
        print(f'[中文新聞] 解析批次失敗 | {type(e).__name__}: {e}')
        raise e   
    
    conn.close()
    engine.dispose()

    # PythonOperator stores this content-free receipt in Airflow XCom so that
    # source coverage remains queryable without relying on worker log files.
    return {
        'status':'success',
        'source_count':len(source_coverage),
        'url_count':sum(row['url_count'] for row in source_coverage),
        'parsed_count':sum(row['parsed_count'] for row in source_coverage),
        'valid_time_count':sum(row['valid_time_count'] for row in source_coverage),
        'accepted_count':sum(row['accepted_count'] for row in source_coverage),
        'fetch_or_parse_rejected':sum(
            row['fetch_or_parse_rejected'] for row in source_coverage
        ),
        'invalid_time_quarantined':sum(
            row['invalid_time_quarantined'] for row in source_coverage
        ),
        'content_or_duplicate_rejected':sum(
            row['content_or_duplicate_rejected'] for row in source_coverage
        ),
        'article_quality_rejected':sum(row['article_quality_rejected'] for row in source_coverage),
        'title_blank_or_whitespace_rejected':sum(
            row['title_blank_or_whitespace_rejected'] for row in source_coverage
        ),
        'content_blank_or_whitespace_rejected':sum(
            row['content_blank_or_whitespace_rejected'] for row in source_coverage
        ),
        'duplicate_source_article_id_rejected':sum(
            row['duplicate_source_article_id_rejected'] for row in source_coverage
        ),
        'other_post_time_filter_rejected':sum(
            row['other_post_time_filter_rejected'] for row in source_coverage
        ),
        'post_time_filter_rejection_count_delta':sum(
            row['post_time_filter_rejection_count_delta']
            for row in source_coverage
        ),
        'post_time_filter_reconciliation_failed_source_count':sum(
            not row['post_time_filter_rejections_reconciled']
            for row in source_coverage
        ),
        'post_time_filter_rejections_reconciled':all(
            row['post_time_filter_rejections_reconciled']
            for row in source_coverage
        ),
        'freshness_observed_at_tpe':freshness_observed_at.isoformat(),
        'freshness_cutoff_tpe':freshness_cutoff.isoformat(),
        'timezone':'Asia/Taipei',
        'sources':source_coverage,
    }
    
    # delete_and_insert(conn,cursor,engine,log_record)
         
@task_wrapper
def delete_and_insert(conn=None,cursor=None,engine=None,log_record=None):
    conn,cursor,engine=_ensure_runtime(count_before=True,record=log_record)
    log_record=log_record or _default_log_record
    try:
        delete_sql=f"""
        delete tb from news as tb
        where exists (
        select *
        from news_diff tb2
        where tb.article_id=tb2.article_id and tb.source=tb2.source
        )
        """
        delete_count=cursor.execute(delete_sql)
        conn.commit()

        log_record.set_delete_count(delete_count)
        print(f'[中文新聞] 刪除筆數={delete_count}（舊文重寫）')
        
        D_temp=pd.read_sql_query('select * from news_temp',engine)
        # `NewsLogic` instances are created at module import, so their original
        # create_time can predate this batch. Stamp the rows at persistence time
        # because market-signal PIT reads use this field as the ingestion cutoff.
        ingested_at=datetime.datetime.now()+datetime.timedelta(hours=8)
        D_temp['create_time']=ingested_at.strftime('%Y-%m-%d %H:%M:%S')
        D_temp.to_sql('news',engine,index=False,if_exists='append')
        print(
            '[中文新聞][ingest_receipt] '
            f'rows={D_temp.shape[0]} '
            f'create_time_tpe={ingested_at.strftime("%Y-%m-%d %H:%M:%S")}'
        )
        
        log_record.set_insert_count(D_temp.shape[0])
        print(f'[中文新聞] 新增筆數={D_temp.shape[0]}')
        
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        
        print(f'[中文新聞] 寫入後總筆數={after_count}')
        print('[中文新聞] 成功')
        return {
            'status':'success',
            'inserted_count':int(D_temp.shape[0]),
            'create_time_tpe':ingested_at.strftime('%Y-%m-%d %H:%M:%S'),
            'timezone':'Asia/Taipei',
        }
    except Exception as e:
        log_record.raise_error(repr(e))
        print(f'[中文新聞] 失敗 | {type(e).__name__}: {e}')
        raise e        
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()

