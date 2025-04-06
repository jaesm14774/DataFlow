import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import random
import datetime
from dateutil.relativedelta import *

from lib.get_sql import *
from lib.log_process_execution import BaseLogRecord
from lib.notify import DiscordNotify
from common.config import *

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,12,21),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'execution_timeout': datetime.timedelta(seconds=3600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'mojim song of collection'
    }

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

app_name = 'song'
database_name = 'crawler'
table_name = 'song'

user_agent=["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"]
#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)

#連接mysql
connection=pd.read_csv(sql_configure_path,index_col='name')
conn,cursor,engine=get_sql(connection.loc['host','value'],
                           int(connection.loc['port','value']),
                           connection.loc['user','value'],
                           connection.loc['password','value'],database_name)


before_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)


def get_song_from_month(date):
    """
    抓取當月歌曲
    """
    a=requests.get(f'https://mojim.com/twzlist{date}.htm',
                  headers={
                      'Host':'mojim.com',
                      'User-Agent':random.choice(user_agent),
                  },timeout=60)
    soup=BeautifulSoup(a.text,'lxml')
    
    df=[]
    for dd in soup.find_all('dd',class_=re.compile('hb\d')):
        if dd.find('h1') is None:
            continue
        
        temp=[re.sub(string=s,pattern=r'^\s+|\s+$',repl='') for s in dd.find('h1').text.split('＞')]
        singer=temp[0].strip()
        singer=re.sub(string=singer,pattern=r'^\d{4}-\d{2}-\d{2}',repl='').strip()
        album=temp[1]
        
        singer_u='https://mojim.com'+dd.find('h1').find('a').get('href')
        album_u='https://mojim.com'+dd.find('h1').find('a').find_next('a').get('href')
        
        for song in dd.find_all('span',class_='m0a'):
            song_u='https://mojim.com'+song.find(class_=re.compile('t\d')).get('href')
            song_name=re.sub(string=song.find(class_=re.compile('t\d')).text,pattern=r'^\s+|\s+$',repl='')
            
            df.append(pd.DataFrame({
                'album':album,
                'singer':singer,
                'song_name':song_name,
                'album_url':album_u,
                'singer_url':singer_u,
                'song_url':song_u,
                'publishDate':date
            },index=[0]))
            
    return pd.concat(df)

def grab_id(url):
    return re.search(string=url,pattern=r'(tw[A-z0-9]+)\.htm').group(1)

def get_song_url_data():
    deter=pd.read_sql_query(f'select * from {table_name} order by publishDate desc limit 8000',engine)
    df=[]
    tt=deter.publishDate.iloc[0]
    end_month=format(datetime.datetime.strptime(deter.publishDate.iloc[0],'%Y-%m')+relativedelta(months=2),'%Y-%m')
    while tt != end_month:
        df.append(get_song_from_month(tt))
        
        print(f'Done crawler of song --- {tt}')
        tt=format(datetime.datetime.strptime(tt,'%Y-%m')+relativedelta(months=1),'%Y-%m')

    df=pd.concat(df)
    df['album_id']=df['album_url'].map(grab_id)
    df['singer_id']=df['singer_url'].map(grab_id)
    df['song_id']=df['song_url'].map(grab_id)

    df=df.loc[:,['album_id', 'singer_id','song_id','song_name','album', 'singer',
                'album_url', 'singer_url', 'song_url','publishDate']]

    df=df[~df.duplicated(subset=['song_id'])]
    
    cursor.execute(f'truncate table {table_name}_diff')
    cursor.execute('truncate table song_url_data')
    conn.commit()
    
    df.loc[:,['song_id']].to_sql(f'{table_name}_diff',engine,index=0,if_exists='append')
    df.to_sql('song_url_data',engine,index=0,if_exists='append')

def get_song_info(song_url):
    a=requests.get(song_url,
                  headers={
                      'Host':'mojim.com',
                      'User-Agent':random.choice(user_agent),
                  },timeout=60)
    soup=BeautifulSoup(a.text,'lxml')
    #抓取較為精準的歌詞內容
    content=str(soup.find('dd',class_='fsZx3')).split('<br/>')
    last_index=len(content)
    content=content[1:(last_index-1)]
    if len(content) == 0:
        return pd.DataFrame({
            'song_url':song_url,
            'content':' '
        },index=[1]) 
    #刪除[00:01.00]這種加上歌詞出現時間，又在重寫一次歌詞的內容
    ij=[i for i,s in enumerate(content) if bool(re.search(string=s,pattern=r'^\[\d+:\d+\.\d+\]'))]
    if len(ij)>0:
        n=min(ij)
        temp=content+[' ',' ']
        temp=[i for i,s in enumerate(content) if s=='' and temp[i+1] =='' and temp[i+2]=='']
        if len(temp) !=0:
            n=min(min(temp),n)            
    else:
        n=len(content)
    content=content[0:n]    
    #有更改的，又再一次副寫內容
    
    #有極少數歌曲，會用-----符號，上下各一個包住歌詞，避免誤刪，如果第一個-----出現在前5行內，且剛好有兩個-----則取第二個的位置
    ij=[i for i,s in enumerate(content) if bool(re.search(string=s,pattern=r'^-----'))]
    if len(ij) == 2 and min(ij)<=5:
        n=[s for s in ij if s>5][0]
    elif len(ij) == 1 or len(ij)>2:
        n=min(ij)
    else:
        n=len(content)
    content=content[0:n]    
    
    #刪除上面關於歌曲的雜訊(不統一，不爬取)
    ii=[i for i,s in enumerate(content) if bool(re.search(string=s,pattern=r'^(作詞|作曲|編曲|監製|女聲|主唱|Lyrics|Music)：'))]
    if len(ii)>0:
        n=max(ii)
    else:
        n=-1
    content=content[(n+1):]     
    try:
        if bool(re.search(string=content[0],pattern=r'^Sung by')):
            content=content[1:]
    except:
        temp=pd.DataFrame({
                'song_url':song_url,
                'content':'  '
            },index=[1])
        return temp
    #刪除頭尾空格符號
    content=[re.sub(string=s,pattern=r'^\s+|\s+$',repl='') for s in content]
    content=[re.sub(string=s,pattern=r'\u3000',repl='    ') for s in content]
    #刪除魔鏡網的廣告詞
    content=[s for s in content if not bool(re.search(string=s,pattern=r'更多更詳盡歌詞 在'))]
    #刪除對唱版的畫蛇添足(<唱這句的人>:<歌詞>)
    for i,s in enumerate(content):
        if s.find('：') != -1:
            content[i]=s[(s.find('：')+1):]
        else:
            pass
    #刪除多保留的 -------
    content=[s for s in content if not bool(re.search(string=s,pattern=r'^-----'))]
    #將歌詞組成一個段落
    content_r=[]
    temp=[]
    ii=(np.array(content) != '').tolist()
    for b,s in zip(ii,content):
        if b:
            temp.append(s)
        else:
            if len(temp)>0:
                content_r.append('\n'.join(temp))
            content_r.append(s)
            temp=[]
    if ii[-1]:
        content_r.append(s)    
    #解決repeat歌詞問題
    sign='\*|@|#|＊|＃|△'
    temp=[s for s in content_r if bool(re.search(string=s,pattern=f'^Repeat |^{sign}'))]
    if len(temp)<=1:
        result=re.sub(string='\n'.join(content_r),pattern=r'^\s+|\s+$',repl='')
    else:
        temp=[s for s in temp if not bool(re.search(string=s,pattern=f'^Repeat '))]
        sign_map_text={}
        for s in [re.split(string=s,pattern=f'({sign})') for s in temp]:
            if s[0] == '':
                s=s[1:]
            sign_map_text[s[0]]=s[1]    
        for i,s in enumerate(content_r):
            if bool(re.search(string=s,pattern=f'^{sign}')):
                content_r[i]=re.sub(string=s,pattern=f'{sign}',repl='')
            if bool(re.search(string=s,pattern=r'^Repeat ')):
                for si,txt in sign_map_text.items():
                    s=re.sub(string=s,pattern=re.escape(si),repl=re.escape(txt))            
                s=re.sub(r'\\(.)', r'\1',s)
                s=re.sub(string=s,pattern=r'\\\\\n',repl='\n')
                s=re.sub(string=s,pattern=r'^Repeat ',repl='')
                content_r[i]=s
        result=re.sub(string='\n'.join(content_r),pattern=r'^\s+|\s+$',repl='')
    #清理髒符號
    result=re.sub(string=result,pattern=r'▲|▼',repl='')
    if len(result)<100:
        result=' '

    return pd.DataFrame({
        'song_url':song_url,
        'content':result
    },index=[1])

def get_song_content_and_delete_insert():
    df=pd.read_sql_query('select * from song_url_data',engine)
    
    rs=[]
    for i in range(0,df.shape[0]):
        rs.append(get_song_info(df.song_url.iloc[i]))
        print(f'{i}th song collect.')

    rs=pd.concat(rs)
    rs=rs[rs.content != ' ']
    rs=rs[rs.content != '  ']

    final=pd.merge(left=rs,right=df,on=['song_url'],how='left')

    try:
        delete_sql=f"""
            delete tb from {table_name} as tb
            where exists (
            select *
            from {table_name}_diff tb2
            where tb.song_id=tb2.song_id
            )
            """
        delete_count=cursor.execute(delete_sql)
        conn.commit()
        
        log_record.set_delete_count(delete_count)
        final.to_sql(table_name,engine,index=0,if_exists='append')
        log_record.set_insert_count(final.shape[0])
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        print('收集成功!')
    except Exception as e:
        log_record.raise_error(repr(e))
        print('任務失敗')
        raise e        
    finally:
        log_record.insert_to_log_record()
        

with DAG(
    dag_id='mojim_song_collection',
    default_args=default_args,
    schedule='1 7 1,16 * *',
    catchup=False
) as dag:
    get_song_url = PythonOperator(
        task_id='get_mojim_song_url',
        python_callable=get_song_url_data,
        on_failure_callback=notify.task_custom_failure_function,
        dag=dag
    )

    get_song_content = PythonOperator(
        task_id='get_mojim_song_content_and_delete_insert',
        python_callable=get_song_content_and_delete_insert,
        on_failure_callback=notify.task_custom_failure_function,
        dag=dag
    )

    get_song_url >> get_song_content

if __name__ == "__main__":
    dag.cli()
