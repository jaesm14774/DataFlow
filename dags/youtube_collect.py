from airflow import DAG
from airflow.operators.python import PythonOperator

from youtube.collect_process import *
from lib.notify import DiscordNotify

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2024,1,20),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=3000),
        'retries': 0,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'youtube crawler'
    }


with DAG(
    dag_id='youtube_collect_process',
    default_args=default_args,
    schedule='25 1 * * *',
    catchup=False
) as dag:
    youtube_collect = PythonOperator(
        task_id='collect_youtube',
        python_callable=collect_all_youtube,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    youtube_collect
    
    
if __name__ == "__main__":
    dag.cli()

