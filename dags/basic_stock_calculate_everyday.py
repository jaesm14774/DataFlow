from airflow import DAG
from airflow.operators.python import PythonOperator

from stock.calculate_process import *
from lib.notify import DiscordNotify

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token
    
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
    description='舊版券商與主力推薦清單；未確認逐股資料授權前不由收集 DAG 觸發。',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    # This DAG is intentionally manual-only while its per-security broker
    # query inputs remain outside the approved market-wide signal feeds.
    # The legacy institutional-flow branches used a website-query feed. Keep
    # them out of scheduled signals until listed-market batch authorization,
    # TPEx coverage, PIT history, and outcome checks are established.

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

    stock_calculate_mainforce_buy_sell_process = PythonOperator(
        task_id='calculate_mainforce_buy_sell',
        python_callable=calculate_mainforce_buy_sell,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )   
    
    stock_make_mainforce_buy_sell_recommendation_list_process = PythonOperator(
        task_id='make_mainforce_buy_sell_recommendation_list',
        python_callable=make_mainforce_buy_sell_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )   
     
    
    stock_calculate_mainforce_buy_sell_process >> stock_make_mainforce_buy_sell_recommendation_list_process
    stock_calculate_broker_recommendation_process >> stock_calculate_broker_ten_days_recommendation_process
    
    stock_calculate_broker_ten_days_recommendation_process >> stock_make_broker_inout_recommendation_list_process
if __name__ == "__main__":
    dag.cli()
