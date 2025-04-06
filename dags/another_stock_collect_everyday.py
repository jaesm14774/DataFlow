from stock.treasury_stock import *
from stock.lottery_notify import *
from stock.collect_process import is_taiwan_stock_close

from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.edgemodifier import Label


#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token
    
default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2023,1,1),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=900),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'stock calculate everyday'
    }

with DAG(
    dag_id='another_stock_calculate_everyday_process',
    default_args=default_args,
    schedule='0 22 * * 1,2,3,4,5',
    catchup=False
) as dag:
    stock_lottery_collect_process = PythonOperator(
        task_id='lottery_collect',
        python_callable=lottery_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_treasury_history_collect_process = PythonOperator(
        task_id='treasury_history_collect',
        python_callable=treasury_history_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_deter_open_or_not_process = BranchPythonOperator(
        task_id='stock_deter_open_or_not',
        python_callable=is_taiwan_stock_close,
        dag=dag,
        op_kwargs={'task_name':'treasury_history_collect'},
        provide_context=True,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_done = DummyOperator(task_id='stock_collect_process_is_done',dag=dag)

    stock_deter_open_or_not_process >> Label("今天沒有開盤") >> stock_done
    
    stock_deter_open_or_not_process >> Label("今天有開盤") >> stock_treasury_history_collect_process >> stock_lottery_collect_process

if __name__ == '__main__':
    dag.cli()