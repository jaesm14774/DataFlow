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
        'start_date': datetime.datetime(2022,11,26),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=14400),
        'retries': 0,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'stock calculate',
        'do_xcom_push': True
    }

with DAG(
    dag_id='stock_calculate_weekday_process',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    stock_make_major_holder_recommendation_list_process = PythonOperator(
        task_id='make_major_holder_recommendation_list',
        python_callable=make_major_holder_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_make_margin_trade_recommendation_list_process = PythonOperator(
        task_id='make_margin_trade_recommendation_list',
        python_callable=make_margin_trade_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_make_month_revenue_recommendation_list_process = PythonOperator(
        task_id='make_month_revenue_recommendation_list',
        python_callable=make_month_revenue_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    ) 
    
    stock_make_major_holder_recommendation_list_process >> stock_make_month_revenue_recommendation_list_process
    stock_make_margin_trade_recommendation_list_process
    
if __name__ == "__main__":
    dag.cli()

