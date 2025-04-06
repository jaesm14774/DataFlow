from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.edgemodifier import Label

from stock.collect_process import *
from lib.notify import DiscordNotify

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,11,24),
        # 'end_date': datetime(),
        # 'depends_on_past': False,
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=21600),
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        'retries': 2,
        'retry_delay': datetime.timedelta(minutes=15),
        'tags':'stock collect everyday',
        'do_xcom_push': True
    }

with DAG(
    dag_id='stock_collect_everyday_process',
    default_args=default_args,
    schedule='10 8 * * 1,2,3,4,5',
    catchup=False
) as dag:
    stock_all_code_info_collect_process = PythonOperator(
        task_id='stock_all_code_info_collect',
        python_callable=all_code_info_collect,
        dag=dag,
        provide_context=True,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_back_up_process = PythonOperator(
        task_id='stock_back_up_delete_code',
        python_callable=back_up,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_renew_info_process = PythonOperator(
        task_id='stock_renew_info_new_code',
        python_callable=renew_info,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_three_main_investor_collect_process = PythonOperator(
        task_id='stock_three_main_investor_collect',
        python_callable=three_main_inverstor_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_twse_all_code_daily_deal_info_collect_process = PythonOperator(
        task_id='stock_twse_all_code_daily_deal_info_collect',
        python_callable=all_code_daily_deal_info_collect,
        op_kwargs={'_type':'twse'},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_tpex_all_code_daily_deal_info_collect_process = PythonOperator(
        task_id='stock_tpex_all_code_daily_deal_info_collect',
        python_callable=all_code_daily_deal_info_collect,
        op_kwargs={'_type':'tpex'},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_broker_inout_tpex_trigger_insert_process = PythonOperator(
        task_id='stock_broker_inout_tpex_trigger_insert',
        python_callable=broker_inout_tpex_trigger_insert,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_all_code_standard_metric_collect_process = PythonOperator(
        task_id='stock_all_code_standard_metric_collect',
        python_callable=all_code_standard_metric_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )    

    stock_day_trade_collect_process = PythonOperator(
        task_id='stock_day_trade_collect',
        python_callable=day_trade_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_broker_inout_twse_collect_process = PythonOperator(
        task_id='stock_broker_inout_twse_collect',
        python_callable=broker_inout_twse_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_broker_inout_twse_collect_second_time_process = PythonOperator(
        task_id='stock_broker_inout_twse_collect_second_time',
        python_callable=broker_inout_twse_collect,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    sleep_process = PythonOperator(
        task_id='time_sleep_function',
        python_callable=time_sleep,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )    

    calculate_trigger_process = TriggerDagRunOperator(
        task_id="calculate_trigger_everyday_dagrun",
        trigger_dag_id="stock_calculate_everyday_process",
        execution_date='{{ ts }}',
        reset_dag_run=True,
        wait_for_completion=False,
        allowed_states=['success'],
        poke_interval =300,
        on_failure_callback=notify.task_custom_failure_function,
        on_success_callback=notify.task_custom_success_function
    )
    
    stock_deter_open_or_not_process = BranchPythonOperator(
        task_id='stock_deter_open_or_not',
        python_callable=is_taiwan_stock_close,
        dag=dag,
        provide_context=True,
        on_failure_callback=notify.task_custom_failure_function
    )

    stock_done = DummyOperator(task_id='stock_collect_process_is_done',dag=dag)

    stock_deter_open_or_not_process >> Label("今天沒有開盤") >> stock_done
    
    stock_deter_open_or_not_process >> Label("今天有開盤") >> stock_all_code_info_collect_process
    
    stock_all_code_info_collect_process >> [stock_back_up_process,stock_renew_info_process]
    
    [stock_back_up_process,stock_renew_info_process] >> stock_three_main_investor_collect_process
    
    stock_three_main_investor_collect_process >> [stock_twse_all_code_daily_deal_info_collect_process,
                                                  stock_tpex_all_code_daily_deal_info_collect_process]
    
    stock_tpex_all_code_daily_deal_info_collect_process >> stock_broker_inout_tpex_trigger_insert_process
    
    stock_broker_inout_tpex_trigger_insert_process >> stock_day_trade_collect_process
    
    stock_twse_all_code_daily_deal_info_collect_process >> stock_broker_inout_twse_collect_process
    
    stock_broker_inout_twse_collect_process >> stock_broker_inout_twse_collect_second_time_process 
    stock_broker_inout_twse_collect_second_time_process >> sleep_process >> stock_all_code_standard_metric_collect_process
    stock_all_code_standard_metric_collect_process >> calculate_trigger_process
    
if __name__ == "__main__":
    dag.cli()
