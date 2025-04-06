import re
import datetime
import pandas as pd
import numpy as np
from urllib.parse import urlparse
from lib.get_sql import *
from lib.log_process_execution import BaseLogRecord
from common.config import *
from lib.common_tool import task_wrapper
from news_ch.news_crawler.module import *

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

#連接mysql
#read sql config
connection=pd.read_csv(sql_configure_path,index_col='name')
conn,cursor,engine=get_sql(connection.loc['host','value'],
                           int(connection.loc['port','value']),
                           connection.loc['user','value'],
                           connection.loc['password','value'],database_name)

before_count=pd.read_sql_query(f'select count(id) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)
print(f'中文新聞，處理前總數為 : {before_count}')

def is_valid_url(url):
    """
    判斷一個URL是否為有效的正常URL
    
    參數:
    url (str): 需要驗證的URL字符串
    
    返回:
    bool: 如果URL有效則返回True，否則返回False
    """
    # 檢查URL是否為空
    if not url:
        return False
    
    # 基本URL格式的正則表達式
    regex = re.compile(
        r'^(?:http|https)://' # http:// 或 https:// 開頭
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # 域名
        r'localhost|' # localhost
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # 或IP地址
        r'(?::\d+)?' # 可選的端口
        r'(?:/?|[/?]\S+)$', re.IGNORECASE) # 路徑和查詢參數
    
    if re.match(regex, url) is not None:
        try:
            # 使用urlparse進一步驗證URL結構
            result = urlparse(url)
            # 檢查協議、網絡位置和路徑
            return all([result.scheme, result.netloc])
        except:
            return False
    return False

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
        print('成功收集中文新聞網址')
    except Exception as e:
        log_record.raise_error(repr(e))
        log_record.insert_to_log_record()
        print('任務失敗')
        raise e

    # collect_news(conn,cursor,engine,log_record)


#解析文章函數匯集
def get_info(article_url,source,category,tim,img,keyword):
    return source_dict[source].get_article_info(article_url=article_url,
                                                category=category,
                                                tim=tim,
                                                img=img,
                                                keyword=keyword)

#清理特殊符號
def clean_text(text,special_sign='\"'):
    return re.sub(string=text,pattern=special_sign,repl='')

def clean_time_format(time_format):
    """
    Clean and standardize time format to '%Y-%m-%d %H:%M:%S'
    - Replace '/' with '-'
    - If format is '%Y-%m-%d %H:%M', add ':00' seconds
    - Return None for invalid formats
    
    Args:
        time_format (str): Input time string
        
    Returns:
        str or None: Standardized time string in '%Y-%m-%d %H:%M:%S' format or None if invalid
    """
    # Convert to string and standardize separators
    time_format = str(time_format).replace('/', '-')
    
    # Remove double spaces
    time_format = re.sub(string=time_format, pattern='  ', repl=' ')
    
    # Check if format starts with '0' (error case)
    if time_format[0] == '0':
        return None
    
    # Try standard format first
    try:
        datetime.datetime.strptime(time_format, '%Y-%m-%d %H:%M:%S')
        return time_format
    except ValueError:
        # Try format without seconds
        try:
            dt = datetime.datetime.strptime(time_format, '%Y-%m-%d %H:%M')
            # Add seconds to the format
            return dt.strftime('%Y-%m-%d %H:%M') + ':00'
        except ValueError:
            # Invalid format
            return None

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
                print(f'collecting url : {row["article_url"]}')
                D_temp.append(get_info(article_url=row["article_url"], category=row["category"], tim=row["created_at"], source=row["source"], img=row["img"], keyword=row["keyword"]))
            except Exception as e:
                print(e)
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
        
        #連接mysql(避免程式處理時間過久，已中斷連線)
        conn,cursor,engine=get_sql(connection.loc['host','value'],
                                int(connection.loc['port','value']),
                                connection.loc['user','value'],
                                connection.loc['password','value'],database_name)
        
        cursor.execute('truncate table news_diff')
        cursor.execute('truncate table news_temp')
        conn.commit()
        
        D_temp.loc[:,_key_columns].to_sql('news_diff',engine,index=0,if_exists='append')
        D_temp.to_sql('news_temp',engine,index=False,if_exists='append')
    except Exception as e:
        log_record.raise_error(repr(e))
        log_record.insert_to_log_record()
        print('任務失敗')
        raise e   
    
    conn.close()
    engine.dispose()
    
    # delete_and_insert(conn,cursor,engine,log_record)
         
@task_wrapper
def delete_and_insert(conn,cursor,engine,log_record):
    try:
        #連接mysql(避免程式處理時間過久，已中斷連線)
        conn,cursor,engine=get_sql(connection.loc['host','value'],
                                int(connection.loc['port','value']),
                                connection.loc['user','value'],
                                connection.loc['password','value'],database_name)
        
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
        print(f'中文新聞 刪除資料筆數 : {delete_count}')
        
        D_temp=pd.read_sql_query('select * from news_temp',engine)
        D_temp.to_sql('news',engine,index=False,if_exists='append')
        
        log_record.set_insert_count(D_temp.shape[0])
        print(f'中文新聞 新增資料筆數 : {D_temp.shape[0]}')
        
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        
        print(f'中文新聞，爬蟲後筆數 : {after_count}')
        print('收集成功!')
    except Exception as e:
        log_record.raise_error(repr(e))
        print('任務失敗')
        raise e        
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()

