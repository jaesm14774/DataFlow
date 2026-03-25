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
    TheNewsLens, United, TVBS,
)
from news_ch.utils import is_valid_url, clean_text, clean_time_format

app_name = 'news_ch'
database_name = 'news_ch'
table_name = 'news'

#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)

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
}

source_dict={}
for k in task_dict.keys():
    source_dict[task_dict[k]._source]=task_dict[k]

connection=pd.read_csv(sql_configure_path,index_col='name')

def _new_connection():
    return get_sql(connection.loc['host','value'],
                   int(connection.loc['port','value']),
                   connection.loc['user','value'],
                   connection.loc['password','value'],database_name)

conn,cursor,engine=_new_connection()

before_count=pd.read_sql_query(f'select count(id) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)
print(f'[中文新聞] DB 既有筆數={before_count}')

#收集所有文章網址
@task_wrapper
def collect_article_url(conn,cursor,engine,log_record):
    try:
        cursor.execute('truncate table article_url')
        conn.commit()
        
        TEMP=pd.concat([task_dict[k].get_article_url_from() for k in task_dict.keys()])
        #刪除無效網址
        TEMP=TEMP[TEMP['article_url'].map(is_valid_url)]
        #刪除重複的網址
        TEMP=TEMP[~TEMP.duplicated(subset=['article_url'])]
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
def collect_news(conn,cursor,engine,log_record):
    _key_columns=['article_id','source']
    
    try:
        TEMP=pd.read_sql_query('select * from article_url',engine)
        
        #收集所有文章的網址
        if TEMP.shape[0] == 0:
            return 'Done all process'
        
        #得到所有文章的資訊
        D_temp=[]

        for index, row in TEMP.iterrows():
            try:
                print(f'[中文新聞] 解析文章 | url={row["article_url"]}')
                D_temp.append(get_info(article_url=row["article_url"], category=row["category"], tim=row["created_at"], source=row["source"], img=row["img"], keyword=row["keyword"]))
            except Exception as e:
                print(f'[中文新聞] 單篇略過 | url={row["article_url"]} | {type(e).__name__}: {e}')
                continue
        
        D_temp=pd.concat(D_temp,axis=0)
        D_temp['article_id']=D_temp['article_id'].astype('str')
        
        #檢查異常時間
        D_temp=D_temp[~D_temp.created_at.isnull()]
        D_temp['created_at']=D_temp.created_at.map(clean_time_format)
        D_temp['created_at']=D_temp['created_at'].astype('datetime64[ns]')   
        
        #刪除無法收集到的標題與內文的文章
        D_temp=D_temp[D_temp.title != ' ']
        D_temp=D_temp[D_temp.content!=' ']
        D_temp=D_temp[D_temp.content!='']
        D_temp=D_temp.sort_values(['created_at'],ascending=False)
        D_temp=D_temp[~D_temp.duplicated(subset=['source','article_id'])]
        D_temp.loc[D_temp.created_at == ' ','created_at']=None
        D_temp.content=D_temp.content.apply(clean_text,special_sign='\"')
        D_temp.title=D_temp.title.apply(clean_text,special_sign='\"')
        D_temp=D_temp.reset_index(drop=True)
        
        conn,cursor,engine=_new_connection()
        
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
    
    # delete_and_insert(conn,cursor,engine,log_record)
         
@task_wrapper
def delete_and_insert(conn,cursor,engine,log_record):
    try:
        conn,cursor,engine=_new_connection()
        
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
        D_temp.to_sql('news',engine,index=False,if_exists='append')
        
        log_record.set_insert_count(D_temp.shape[0])
        print(f'[中文新聞] 新增筆數={D_temp.shape[0]}')
        
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        
        print(f'[中文新聞] 寫入後總筆數={after_count}')
        print('[中文新聞] 成功')
    except Exception as e:
        log_record.raise_error(repr(e))
        print(f'[中文新聞] 失敗 | {type(e).__name__}: {e}')
        raise e        
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()

