from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.edgemodifier import Label

from stock.collect_process import *
from stock.earning_call_notify import *
from lib.notify import DiscordNotify

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,11,18),
        # 'end_date': datetime(),
        # 'depends_on_past': False,
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=7200),
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=40),
        'tags':'stock collect weekday',
        'do_xcom_push': True
    }

with DAG(
    dag_id='stock_collect_weekday_process',
    default_args=default_args,
    schedule='0 1 * * 6',
    catchup=False
) as dag:
    calculate_trigger_process = TriggerDagRunOperator(
        task_id="calculate_trigger_weekday_dagrun",
        trigger_dag_id="stock_calculate_weekday_process",
        execution_date='{{ ts }}',
        reset_dag_run=True,
        wait_for_completion=False,
        allowed_states=['success'],
        poke_interval =300,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_is_weekend_ornot_process = BranchPythonOperator(
        task_id='is_weekend_ornot',
        python_callable=is_weekend,
        dag=dag,
        provide_context=True,
        on_failure_callback=notify.task_custom_failure_function
    )
    stock_done = DummyOperator(task_id='stock_collect_process_is_done',dag=dag)
    
    stock_major_holder_collect_process = PythonOperator(
        task_id='stock_major_holder_collect',
        python_callable=major_holder_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )    

    stock_margin_trade_collect_process = PythonOperator(
        task_id='stock_margin_trade_collect',
        python_callable=margin_trade_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )    
    
    stock_month_revenue_collect_process = PythonOperator(
        task_id='stock_month_revenue_collect',
        python_callable=month_revenue_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )    
    
    stock_earning_call_collect_process = PythonOperator(
        task_id='earning_call_collect',
        python_callable=earning_call_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_is_weekend_ornot_process >> Label('是星期六') >> [stock_major_holder_collect_process,
                                                            stock_margin_trade_collect_process,
                                                            stock_month_revenue_collect_process] >> stock_earning_call_collect_process
    
    stock_earning_call_collect_process >> calculate_trigger_process
    
    
if __name__ == "__main__":
    dag.cli()

