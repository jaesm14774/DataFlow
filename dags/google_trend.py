import requests
import re
import pandas as pd
import datetime
import time
import random
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from common.config import *
from lib.log_process_execution import BaseLogRecord
from lib.get_sql import get_sql

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,10,8),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'execution_timeout': datetime.timedelta(seconds=600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=2),
        'tags':'Google daily trend'
    }


connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')

headers={
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'referer': 'https://trends.google.com/trends/trendingsearches/daily?geo=TW',
}

app_name = 'google_daily_trend'
database_name = 'google'
table_name = 'hot_search'

#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)

conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],
                            connection.loc['user','value'],connection.loc['password','value'],database_name)

before_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)


# #get google daily trend all country available
# def country_crawler(engine):
#     a=requests.get('https://trends.google.com/trends/trendingsearches/daily',headers={
#                         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
#                         'referer': 'https://trends.google.com/trends/trendingsearches/daily?geo=TW',
#                     }, timeout=20)
#     soup=BeautifulSoup(a.text,'lxml')

#     str_soup=str(soup)
#     str_soup=str_soup[(str_soup.find('var geoPicker = ')+16):]
#     str_soup=str_soup[:str_soup.find(';')]

#     country=json.loads(str_soup)

#     pd.DataFrame(country).rename(columns={'id':'country_abbrev_en_name','name':'country_ch_name'}).to_sql('country',
#                                                                                                         engine,
#                                                                                                         index=False,
#                                                                                                         if_exists='append')

def get_board(engine):
    board_table_name='country'
      
    return pd.read_sql_query(f'select id as cid,country_abbrev_en_name,country_ch_name from {board_table_name} where crawler_status=1',engine)

def search(geo,board_id,date):
    a=requests.get(f'https://trends.google.com/trends/api/dailytrends?hl=zh-TW&tz=-480&ed={date}&geo={geo}&ns=20',
                  headers={
                      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
                      'referer': 'https://trends.google.com/trends/trendingsearches/daily?geo=TW',
                  }, timeout=20)
    temp=json.loads(re.sub(r'\)\]\}\',\n', '', a.text))
    
    title=[s['title']['query'] for s in temp['default']['trendingSearchesDays'][0]['trendingSearches']]
    hot_level=[s['formattedTraffic'] for s in temp['default']['trendingSearchesDays'][0]['trendingSearches']]
    related_keyword=[';'.join([j['query'] for j in s['relatedQueries']]) for s in temp['default']['trendingSearchesDays'][0]['trendingSearches']]
    related_news_title=[';\n'.join([j['title'] for j in s['articles']]) for s in temp['default']['trendingSearchesDays'][0]['trendingSearches']]    
    related_news_url=[';\n'.join([j['url'] for j in s['articles']]) for s in temp['default']['trendingSearchesDays'][0]['trendingSearches']]    
    related_news_source=[';\n'.join([j['source'] for j in s['articles']]) for s in temp['default']['trendingSearchesDays'][0]['trendingSearches']]    
    
    return pd.DataFrame({
        'cid':board_id,
        'create_time':format(datetime.datetime.now(),'%Y-%m-%d %H:%M:%S'),
        'created_at':date,
        'keyword':title,
        'hot_level':hot_level,
        'related_keyword':related_keyword,
        'related_news_title':related_news_title,
        'related_news_url':related_news_url,
        'related_news_source':related_news_source
    })

def crawler(date=datetime.datetime.now().strftime('%Y%m%d')):
    _select_col=['cid','keyword','hot_level','created_at']
    
    print('Connected with MySQL')

    board=get_board(engine)

    print('Get google trend board : country')
    
    delete_count=0;insert_count=0
    try:
        for i in range(len(board)):
            cid=board['cid'].iloc[i]
            country_abbrev_en_name=board['country_abbrev_en_name'].iloc[i]
            country_ch_name=board['country_ch_name'].iloc[i]
            print(f'Now collect country is {country_abbrev_en_name}-{country_ch_name}')

            df=search(geo=country_abbrev_en_name,board_id=cid,date=date)  
        
            #first truncate diff table
            cursor.execute(f'truncate table {table_name}_diff')
            conn.commit()

            #insert data to _diff table
            df.loc[:,_select_col].to_sql(f'{table_name}_diff',engine,index=False,if_exists='append')

            #delete old data
            filter_sql=' and '.join([f'a.{s}=b.{s}' for s in _select_col])

            sql=f"""
            delete a
            from {table_name} as a inner join {table_name}_diff as b 
            on {filter_sql};          
            """
            delete_count+=cursor.execute(sql)
            conn.commit()

            #update by delete insert data
            df.to_sql(f'{table_name}',engine,index=False,if_exists='append')
            insert_count+=df.shape[0]
            
            print(f'Done {country_abbrev_en_name}-{country_ch_name}')

            time.sleep(random.randint(10,20)/10)
            
        log_record.set_delete_count(delete_count)
        log_record.set_insert_count(insert_count)
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
        conn.close() #關閉資料庫


with DAG(
    dag_id='google_daily_trend',
    default_args=default_args,
    schedule='41 2,6,10,14,18,22 * * *',
    catchup=False
) as dag:
    first_step = PythonOperator(
        task_id='crawler',
        python_callable=crawler,
        dag=dag
    )

    # [START howto_operator_bash]
    second_step = BashOperator(
        task_id='run_after_crawler',
        bash_command='echo Done'
    )

    first_step >> second_step

if __name__ == "__main__":
    dag.cli()

