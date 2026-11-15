import requests
import datetime
import pandas as pd
from lib.get_sql import get_sql_from_config
from lib.notify import discord_notifier
from lib.taipei_time import taipei_now
from sqlalchemy import text
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_NAME='holiday'

# 證交所公告的臨時休市日（政府辦公日曆沒有），新增時需註明依據
MARKET_ONLY_CLOSURES=[
    ('2026-10-23','颱風休市','薔琵颱風，證交所/櫃買中心公告休市；官方指數、個股行情與融資券當日皆無資料'),
]

#Discord notify
notify=discord_notifier('程式bug權杖')

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
    now_date=taipei_now()
    data=[]
    
    while True:
        print(f'[假日API] 抓取頁碼 start_page={start_page}')
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
    conn,cursor,engine=get_sql_from_config(DB_NAME)
    
    # 清空、重載、補臨時休市放在同一個 transaction：任何一步失敗都會 rollback，不會留下空的行事曆
    # （用 DELETE 而非 TRUNCATE，因為 TRUNCATE 會 implicit commit）
    try:
        with engine.begin() as tx:
            tx.execute(text('delete from taiwan'))
            result.to_sql('taiwan',tx,index=0,if_exists='append')

            # 政府辦公日曆不含「颱風休市」這類臨時休市；重載後要補回，否則會被清掉
            for closure_date, closure_name, closure_note in MARKET_ONLY_CLOSURES:
                tx.execute(
                    text(
                        'INSERT INTO taiwan (now_date, chinese_name, isholiday, holidayCategory, description) '
                        'VALUES (:d, :name, :holiday, :category, :note) '
                        'ON DUPLICATE KEY UPDATE chinese_name=VALUES(chinese_name), isholiday=VALUES(isholiday), '
                        'holidayCategory=VALUES(holidayCategory), description=VALUES(description)'
                    ),
                    {'d': closure_date, 'name': closure_name, 'holiday': '是',
                     'category': '市場臨時休市', 'note': closure_note},
                )
    finally:
        conn.close()
        engine.dispose()


with DAG(
    dag_id='taiwan_holiday_collect_process',
    description='定期收集台灣市場休市日並更新 holiday.taiwan 行事曆。',
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
