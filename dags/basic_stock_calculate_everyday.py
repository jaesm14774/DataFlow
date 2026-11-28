from airflow import DAG
from airflow.operators.python import PythonOperator

from stock.calculate_process import *
from lib.notify import discord_notifier

#Discord notify
notify=discord_notifier('程式執行狀態', discord_token_path)
    
default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,11,14),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=7200),
        'retries': 0,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'stock calculate everyday',
        'do_xcom_push': True
    }

with DAG(
    dag_id='stock_calculate_everyday_process',
    description='舊版地緣券商推薦清單（手動）；主力買賣超已移到 stock_collect_everyday_process 每日排程。',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    # 此 DAG 只剩地緣券商推薦鏈，維持手動：2026-11 的回測顯示
    # 地緣券商沒有買進訊號，不是因為授權。主力買賣超已搬到
    # stock_collect_everyday_process（僅上市，上櫃分點抓不到已放棄）。

    stock_calculate_broker_recommendation_process = PythonOperator(
        task_id='calculate_broker_recommendation',
        python_callable=calculate_broker_recommendation,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_calculate_broker_ten_days_recommendation_process = PythonOperator(
        task_id='calculate_broker_ten_days_recommendation',
        python_callable=calculate_broker_ten_days_recommendation,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_make_broker_inout_recommendation_list_process = PythonOperator(
        task_id='make_broker_inout_recommendation_list',
        python_callable=make_broker_inout_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )   

    stock_calculate_broker_recommendation_process >> stock_calculate_broker_ten_days_recommendation_process
    
    stock_calculate_broker_ten_days_recommendation_process >> stock_make_broker_inout_recommendation_list_process
if __name__ == "__main__":
    dag.cli()
