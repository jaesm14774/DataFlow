from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import pandas as pd
from common.config import *
from lib.notify import DiscordNotify
from credit_cards.money101_crawler import fetch_card_data, write_to_sql, notify_new_card  # 引用主程式中的函數

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
    'money101_collect',
    default_args=default_args,
    description='收集 Money101 信用卡資訊',
    schedule_interval='0 18 */3 * *',  # 每天上午9點執行
    catchup=False,
    tags=['money101']
) as dag:

    # 定義任務
    fetch_results = PythonOperator(
        task_id='fetch_card_data',
        python_callable=fetch_card_data,
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
        task_id='notify_new_card',
        python_callable=notify_new_card,
        on_failure_callback=notify.task_custom_failure_function
    )

    # 設定任務依賴關係
    fetch_results >> write_to_database >> send_notifications


if __name__ == "__main__":
    dag.cli() 