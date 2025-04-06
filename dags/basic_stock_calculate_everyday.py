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
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:
    stock_calculate_outer_and_investment_ratio_process = PythonOperator(
        task_id='calculate_outer_and_investment_ratio',
        python_callable=calculate_outer_and_investment_ratio,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_calculate_outer_and_investment_recommendation_process = PythonOperator(
        task_id='calculate_outer_and_investment_recommendation',
        python_callable=calculate_outer_and_investment_recommendation,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
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
    
    stock_make_outer_and_investment_recommendation_list_process = PythonOperator(
        task_id='make_outer_and_investment_recommendation_list',
        python_callable=make_outer_and_investment_recommendation_list,
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
     
    stock_make_another_broker_inout_recommendation_list_process = PythonOperator(
        task_id='make_another_broker_inout_recommendation_list',
        python_callable=make_another_broker_inout_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function,
        on_success_callback=notify.task_custom_success_function
    ) 

    stock_make_outer_and_investment_cumulate_recommendation_list_process = PythonOperator(
        task_id='make_outer_and_investment_cumulate_recommendation_list',
        python_callable=make_outer_and_investment_cumulate_recommendation_list,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    
    stock_calculate_mainforce_buy_sell_process >> stock_make_mainforce_buy_sell_recommendation_list_process
    stock_calculate_broker_recommendation_process >> stock_calculate_broker_ten_days_recommendation_process
    
    stock_calculate_broker_ten_days_recommendation_process >> stock_make_broker_inout_recommendation_list_process
    stock_calculate_broker_ten_days_recommendation_process >> stock_make_another_broker_inout_recommendation_list_process
    
    stock_calculate_outer_and_investment_ratio_process >> stock_calculate_outer_and_investment_recommendation_process
    stock_calculate_outer_and_investment_recommendation_process >> stock_make_outer_and_investment_recommendation_list_process
    stock_calculate_outer_and_investment_recommendation_process >> stock_make_outer_and_investment_cumulate_recommendation_list_process
    
    [stock_make_outer_and_investment_cumulate_recommendation_list_process,stock_make_outer_and_investment_recommendation_list_process]

if __name__ == "__main__":
    dag.cli()

