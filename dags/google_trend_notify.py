import requests
import pandas as pd
import datetime
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from common.config import *
from lib.get_sql import *
from lib.notify import DiscordNotify

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,10,8),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'execution_timeout': datetime.timedelta(seconds=600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=2),
        'tags':['Google daily trend notify']
    }

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['新聞即時通','token']
news_notify=DiscordNotify()
news_notify.webhook_url=token


#discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token
#SQL connection
connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')

def get_board_map(engine):
    board_table_name='country'
      
    b=pd.read_sql_query(f'select id as cid,country_abbrev_en_name,country_ch_name from {board_table_name} where notify_status=1',engine)
    
    return {k:v for k,v in zip(b['cid'],b['country_ch_name'])}

def Notify():
    conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],connection.loc['user','value'],
                            connection.loc['password','value'],'google')

    board_map_dict=get_board_map(engine)

    print('Get active status of board.')

    google=pd.read_sql_query('select * from hot_search where notify=0',engine)

    all_id=google['id'].tolist()

    google=google[~google.duplicated(subset=['keyword'])]

    print('Get unnotified article.')

    for b in board_map_dict:
        temp=google[google.cid == b]
        news_notify.notify(msg=f'Now is board : {board_map_dict[b]}')
        for i in range(0,len(temp)):
            source_1=temp.related_news_source.iloc[i].split(';\n')[0]
            title_1=temp.related_news_title.iloc[i].split(';\n')[0].strip().replace('"', "'")
            url_1=random.choice(temp.related_news_url.iloc[i].split(';\n')).replace('"', "'")
            created_time=str(temp.create_time.iloc[i]+datetime.timedelta(hours=8))
            keyword=temp.keyword.iloc[i].replace('"', "'")
            hot_level=temp.hot_level.iloc[i]
            related_word=temp.related_keyword.iloc[i]
            
            message=(
                f"\n{created_time} : {keyword} {hot_level}"
                f"\n\n{related_word}\n"
                f"source: {source_1} title: "
                f"{title_1}\n\n"
                f"{url_1}\n\n\n"
            )
            
            news_notify.notify(msg=message)

        print(f'Done board notification - {board_map_dict[b]}.')
    
    cursor.execute('update hot_search set notify=1 where id in %s' % str(tuple(all_id)))
    
    print('Article of notification is updated.')
    
    conn.commit()
    conn.close()

    print('All done!')

with DAG(
    dag_id='google_daily_trend_notify',
    default_args=default_args,
    schedule='5 22 * * *',
    catchup=False
) as dag:
    first_step = PythonOperator(
        task_id='google_notify',
        python_callable=Notify,
        on_failure_callback=notify.task_custom_failure_function,
        dag=dag
    )

    # [START howto_operator_bash]
    second_step = BashOperator(
        task_id='run_after_google_hotword_notification',
        bash_command='echo "Notify done!"'
    )

    first_step >> second_step

if __name__ == "__main__":
    dag.cli()