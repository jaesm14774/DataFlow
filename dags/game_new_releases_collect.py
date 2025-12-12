from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import pandas as pd
from common.config import *
from lib.notify import DiscordNotify
from game.new_releases import (
    fetch_new_releases_details,
    write_new_releases_to_sql,
    notify_new_releases
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
    dag_id='game_new_releases_collect',
    default_args=default_args,
    description='收集 Google Play、巴哈姆特、4Gamers 新上架遊戲資訊',
    schedule_interval='0 7 * * *',  # 每天上午7點執行
    catchup=False,
    tags=['game']
) as dag:

    # 定義任務
    fetch_details = PythonOperator(
        task_id='fetch_new_releases_details',
        python_callable=fetch_new_releases_details,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    write_to_database = PythonOperator(
        task_id='write_new_releases_to_sql',
        python_callable=write_new_releases_to_sql,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    send_notifications = PythonOperator(
        task_id='notify_new_releases',
        python_callable=notify_new_releases,
        on_failure_callback=notify.task_custom_failure_function,
        provide_context=True
    )

    # 設定任務順序
    fetch_details >> write_to_database >> send_notifications

if __name__ == "__main__":
    dag.cli()

