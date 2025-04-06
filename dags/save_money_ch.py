from airflow import DAG
from airflow.operators.python import PythonOperator

from news_ch.save_money import *
from lib.notify import DiscordNotify

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2023,1,19),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'chinese save money crawler'
    }

with DAG(
    dag_id='save_money_ch_collect_process',
    default_args=default_args,
    schedule='40 1,4,10 * * *',
    catchup=False
) as dag:
    save_money_ch_collect_news = PythonOperator(
        task_id='collect_news',
        python_callable=collect_news,
        op_kwargs={'engine':engine,'conn':conn,'cursor':cursor,'log_record':log_record},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    save_money_ch_delete_and_insert = PythonOperator(
        task_id='delete_and_insert',
        python_callable=delete_and_insert,
        op_kwargs={'engine':engine,'conn':conn,'cursor':cursor,'log_record':log_record},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    save_money_ch_notify = PythonOperator(
        task_id='notification',
        python_callable=notification,
        op_kwargs={'engine':engine,'conn':conn,'cursor':cursor},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )    
    
    save_money_ch_collect_news >> save_money_ch_delete_and_insert >> save_money_ch_notify
    
    
if __name__ == "__main__":
    dag.cli()

