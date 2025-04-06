from airflow import DAG
from airflow.operators.python import PythonOperator

from news_ch.all_news import *
from lib.notify import DiscordNotify

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,12,14),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=3000),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'chinese news crawler'
    }


with DAG(
    dag_id='news_ch_collect_process',
    default_args=default_args,
    schedule='30 0,2,4,6,8,10,15,21 * * *',
    catchup=False
) as dag:
    news_ch_collect_article_url = PythonOperator(
        task_id='collect_article_url',
        python_callable=collect_article_url,
        op_kwargs={'engine':engine,'conn':conn,'cursor':cursor,'log_record':log_record},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )

    news_ch_collect_news = PythonOperator(
        task_id='collect_news',
        python_callable=collect_news,
        op_kwargs={'engine':engine,'conn':conn,'cursor':cursor,'log_record':log_record},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    news_ch_delete_and_insert = PythonOperator(
        task_id='delete_and_insert',
        python_callable=delete_and_insert,
        op_kwargs={'engine':engine,'conn':conn,'cursor':cursor,'log_record':log_record},
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    news_ch_collect_article_url >> news_ch_collect_news >> news_ch_delete_and_insert
    
    
if __name__ == "__main__":
    dag.cli()

