from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import pandas as pd
from common.config import *
from lib.notify import DiscordNotify
from game.pre_registration import (
    fetch_search_results,
    parse_search_results,
    fetch_game_details,
    write_to_sql,
    notify_new_games
)

# DAG 預設參數
default_args = {
    'owner': 'jaesm14774',
    'depends_on_past': False, 
    'start_date': datetime.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
}

#discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

# 建立 DAG
with DAG(
    'game_pre_registration_collect',
    default_args=default_args,
    description='收集 Google Play 預約註冊遊戲資訊',
    schedule_interval='0 9 * * *',  # 每天上午9點執行
    catchup=False,
    tags=['game']
) as dag:

    # 定義任務
    fetch_results = PythonOperator(
        task_id='fetch_search_results',
        python_callable=fetch_search_results,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    parse_results = PythonOperator(
        task_id='parse_search_results',
        python_callable=parse_search_results,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    fetch_details = PythonOperator(
        task_id='fetch_game_details',
        python_callable=fetch_game_details,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    write_to_database = PythonOperator(
        task_id='write_to_sql',
        python_callable=write_to_sql,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    send_notifications = PythonOperator(
        task_id='notify_new_games',
        python_callable=notify_new_games,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    # 設定任務順序
    fetch_results >> parse_results >> fetch_details >> write_to_database >> send_notifications

if __name__ == "__main__":
    dag.cli()
