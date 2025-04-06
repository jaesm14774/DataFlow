from airflow import DAG
from airflow.operators.python import PythonOperator

from ptt.collect_process import *
from lib.notify import DiscordNotify

#discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2024,1,11),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=1200),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'ptt crawler'
    }


with DAG(
    dag_id='ptt_collect_process',
    default_args=default_args,
    schedule='5 0,2,4,6,10,16 * * *',
    catchup=False
) as dag:
    ptt_collect = PythonOperator(
        task_id='collect_ptt',
        python_callable=collect_all_ptt,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    ptt_collect
    
    
if __name__ == "__main__":
    dag.cli()

