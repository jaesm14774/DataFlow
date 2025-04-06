import requests
import datetime
import pandas as pd
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from common.config import *
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_NAME='holiday'

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式bug權杖','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,11,17),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'collect_taiwan_holiday'
    }

def collect_holiday_taiwan():
    rename_dict={
        'date':'now_date',
        'isHoliday':'isholiday',
        'chinese':'chinese_name',
        'name': 'chinese_name'
    }
    #308DCD75-6434-45BC-A95F-584DA4FED251 = 政府行政機關辦公日曆表
    start_page=0
    threshold_N=100
    now_date=datetime.datetime.now()+datetime.timedelta(hours=8)
    data=[]
    
    while True:
        print(f'Now collect start_page = {start_page}')
        url=f'https://data.ntpc.gov.tw/api/datasets/308DCD75-6434-45BC-A95F-584DA4FED251/json?page={start_page}&size=1000'
        response=requests.get(url, timeout=20).json()
        
        data.append(pd.DataFrame(response).rename(columns=rename_dict))
        
        last_date=datetime.datetime.strptime(response[-1]['date'],'%Y%m%d')
        if last_date>=now_date:
            break
        if start_page >= threshold_N:
            break
        
        start_page+=1

    result=pd.concat(data)
    result=result[~result.duplicated(subset=['now_date'])].drop(['year'], axis=1)
    
    #get sql
    connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')
    conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],connection.loc['user','value'],
                                connection.loc['password','value'],DB_NAME)
    
    cursor.execute('truncate table taiwan')
    conn.commit()
    
    result.to_sql('taiwan',engine,index=0,if_exists='append')
    
    conn.close()
    engine.dispose()


with DAG(
    dag_id='taiwan_holiday_collect_process',
    default_args=default_args,
    schedule='0 1 18,1 * *',
    catchup=False
) as dag:
    
    collect_holiday_taiwan_process = PythonOperator(
        task_id='collect_holiday_taiwan',
        python_callable=collect_holiday_taiwan,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

if __name__ == '__main__':
    dag.cli()