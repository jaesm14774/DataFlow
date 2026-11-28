import pandas as pd

from lib.get_sql import *
from lib.log_process_execution import BaseLogRecord
from lib.notify import discord_notifier
from common.config import *

from news_ch.news_crawler import CP, InfoTalk, TVBS_SaveMoney
from news_ch.relevance import BUCKET_DEAL, Classification, format_alert, select_for_push, deal_score

app_name = 'save_money'
database_name = 'news_ch'
table_name = 'save_money'

#初始化log
log_record=BaseLogRecord(app_name=app_name)
_default_log_record = log_record

task_dict={
    1: CP(),
    2: InfoTalk(),
    3: TVBS_SaveMoney()
}

source_dict={}
for i in range(1,len(task_dict)+1):
    source_dict[task_dict[i]._source]=task_dict[i]

conn=cursor=engine=None

def _ensure_runtime(count_before=False,record=None):
    global conn,cursor,engine
    if conn is None or not getattr(conn,'open',False):
        conn,cursor,engine=get_sql_from_config(database_name)
    if count_before:
        before_count=pd.read_sql_query(
            f'select count(1) as N from {table_name}',engine
        )['N'].iloc[0]
        (record or _default_log_record).set_before_count(before_count)
        print(f'[優惠情報] DB 既有筆數={before_count}')
    return conn,cursor,engine


#news ch notify
save_money_ch_notify=discord_notifier('好康報')

def collect_news(conn=None,cursor=None,engine=None,log_record=None):
    conn,cursor,engine=_ensure_runtime()
    log_record=log_record or _default_log_record
    _key_columns=['article_id','source']
    
    try:
        #得到所有文章的資訊
        D_temp=[]
        for i in range(1,len(task_dict)+1):
            D_temp.append(task_dict[i].get_article_url_from())
        
        D_temp=pd.concat(D_temp)
        #刪除重複的網址
        D_temp=D_temp[~D_temp.duplicated(subset=_key_columns)]
        #刪除無法收集到的標題與內文的文章
        D_temp=D_temp[D_temp.title != ' ']
        D_temp=D_temp[D_temp.brief_content!=' ']
        D_temp=D_temp[D_temp.brief_content!='']
        D_temp=D_temp.reset_index(drop=True)
        
        cursor.execute(f'truncate table {table_name}_diff')
        cursor.execute(f'truncate table {table_name}_temp')
        conn.commit()
        
        D_temp.loc[:,_key_columns].to_sql(f'{table_name}_diff',engine,index=0,if_exists='append')
        D_temp.to_sql(f'{table_name}_temp',engine,index=False,if_exists='append')
    except Exception as e:
        log_record.raise_error(repr(e))
        log_record.insert_to_log_record()
        print(f'[優惠情報] 收集失敗 | {type(e).__name__}: {e}')
        raise e        

def delete_and_insert(conn=None,cursor=None,engine=None,log_record=None):
    conn,cursor,engine=_ensure_runtime(count_before=True,record=log_record)
    log_record=log_record or _default_log_record
    try:
        delete_sql=f"""
        delete tb from {table_name} as tb
        where exists (
        select *
        from {table_name}_diff tb2
        where tb.article_id=tb2.article_id and tb.source=tb2.source
        )
        """
        delete_count=cursor.execute(delete_sql)
        conn.commit()

        log_record.set_delete_count(delete_count)
        print(f'[優惠情報] 刪除筆數={delete_count}')
        
        D_temp=pd.read_sql_query(f'select * from {table_name}_temp',engine)
        D_temp.to_sql(table_name,engine,index=False,if_exists='append')
        
        log_record.set_insert_count(D_temp.shape[0])
        print(f'[優惠情報] 新增筆數={D_temp.shape[0]}')
        
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        
        print(f'[優惠情報] 寫入後總筆數={after_count}')
        print('[優惠情報] 成功')
    except Exception as e:
        log_record.raise_error(repr(e))
        print(f'[優惠情報] 失敗 | {type(e).__name__}: {e}')
        raise e        
    finally:
        log_record.insert_to_log_record()

def notification(conn=None,cursor=None,engine=None):
    conn,cursor,engine=_ensure_runtime()
    _key_columns=['article_id','source']
    notification_table_name = 'notification_record'
    
    #read all data not scan before
    select_sql_query=f"""
    select * 
    from {table_name} tb
    where not exists(
        select *
        from notification_record tb2
        where tb.article_id=tb2.article_id and tb.source=tb2.source
    )
    """.strip()
    
    select_df=pd.read_sql_query(select_sql_query,engine).reset_index(drop=True)
    
    print(f'[優惠情報] 待通知篇數={select_df.shape[0]}')
    
    select_df.loc[:,_key_columns].to_sql(notification_table_name,engine,index=0,if_exists='append')
        
    # 只推「真的有折扣力道」的好康：5折以下 / 買一送一 / 免費 / 補助 / 黑五…；
    # 演唱會、跨年、開幕等活動頁與一般優惠碼整理視為雜訊（已寫入 notification_record，不會重評）。
    candidates=[]
    for _,row in select_df.iterrows():
        score,reason=deal_score(row['title'],(row['brief_content'] if isinstance(row['brief_content'],str) else '')[:120],promo_source=True)
        if score>=2:
            candidates.append((row.to_dict(),Classification(BUCKET_DEAL,reason,score)))
    picked=select_for_push(candidates,caps={BUCKET_DEAL:2}).get(BUCKET_DEAL,[])
    print(f'[優惠情報] 達標 {len(candidates)} 篇，本次推播 {len(picked)} 篇')

    for row,cls in picked:
        save_money_ch_notify.notify(msg=format_alert(row,cls))
    
    print('[優惠情報] Discord 通知已送出')
