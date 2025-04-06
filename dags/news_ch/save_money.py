import datetime
import pandas as pd

from lib.get_sql import *
from lib.log_process_execution import BaseLogRecord
from lib.notify import DiscordNotify
from common.config import *

from news_ch.news_crawler.module import *

app_name = 'save_money'
database_name = 'news_ch'
table_name = 'save_money'

#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)

task_dict={
    1: CP(),
    2: InfoTalk(),
    3: TVBS_SaveMoney()
}

source_dict={}
for i in range(1,len(task_dict)+1):
    source_dict[task_dict[i]._source]=task_dict[i]

#連接mysql
#read sql config
connection=pd.read_csv(sql_configure_path,index_col='name')
conn,cursor,engine=get_sql(connection.loc['host','value'],
                           int(connection.loc['port','value']),
                           connection.loc['user','value'],
                           connection.loc['password','value'],database_name)

before_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)
print(f'中文優惠情報，處理前總數為 : {before_count}')


#news ch notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['好康報','token']
save_money_ch_notify=DiscordNotify()
save_money_ch_notify.webhook_url=token

def collect_news(conn,cursor,engine,log_record):
    _key_columns=['article_id','source']
    global table_name
    
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
        print('任務失敗')
        raise e        

def delete_and_insert(conn,cursor,engine,log_record):
    global table_name
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
        print(f'中文優惠情報 刪除資料筆數 : {delete_count}')
        
        D_temp=pd.read_sql_query(f'select * from {table_name}_temp',engine)
        D_temp.to_sql(table_name,engine,index=False,if_exists='append')
        
        log_record.set_insert_count(D_temp.shape[0])
        print(f'中文優惠情報 新增資料筆數 : {D_temp.shape[0]}')
        
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        
        print(f'中文優惠情報，爬蟲後筆數 : {after_count}')
        print('收集成功!')
    except Exception as e:
        log_record.raise_error(repr(e))
        print('任務失敗')
        raise e        
    finally:
        log_record.insert_to_log_record()

def notification(conn,cursor,engine):
    global table_name
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
    
    print(f'這次的中文優惠情報通知共有 {select_df.shape[0]}篇')
    
    select_df.loc[:,_key_columns].to_sql(notification_table_name,engine,index=0,if_exists='append')
        
    for i in range(0,len(select_df)):
        save_money_ch_notify.notify(msg=str(select_df.source.iloc[i])+'\n'+str(select_df.created_at.iloc[i])+':'+select_df.title.iloc[i]+'\n'+select_df.brief_content.iloc[i])
        save_money_ch_notify.notify(msg=select_df.article_url.iloc[i])
    
    print('中文優惠情報 通知完成')
