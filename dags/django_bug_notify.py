import pandas as pd
import datetime

from lib.get_sql import *
from lib.notify import DiscordNotify
from common.config import *

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2023,1,7),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'execution_timeout': datetime.timedelta(seconds=300),
        'retries': 0,
        'tags':'Django bug notify'
    }

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

#連接mysql
database_name='logs'
connection=pd.read_csv(sql_configure_path,index_col='name')
conn,cursor,engine=get_sql(connection.loc['host','value'],
                           int(connection.loc['port','value']),
                           connection.loc['user','value'],
                           connection.loc['password','value'],database_name)

#discord notify for bug
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式bug權杖','token']
notify_bug=DiscordNotify()
notify_bug.webhook_url=token

def extract_error_type(s):
    return s.split(',')[0].replace('(','').strip()

def notification():
    bug=pd.read_sql_query('select * from log where notify=0',engine)

    if len(bug) == 0:
        return

    bug['exc_info']=bug['exc_info'].map(extract_error_type)

    for i in range(0,bug.shape[0]):
        notify_bug.notify(msg='\n'+str(bug.create_time.iloc[i])+':'+bug.app_name.iloc[i])
        notify_bug.notify(msg='\n'+'Error type :'+bug.exc_info.iloc[i])
        notify_bug.notify(msg='\n'+bug.trace.iloc[i])

    #傳送過的Bug，更新資料庫notify變數為1
    if bug.shape[0] == 1:
        cursor.execute(f'update log set notify=1 where id={bug["id"].iloc[0]}')
    else:
        cursor.execute('update log set notify=1 where id in %s' % str(tuple(bug["id"].tolist())))
    
    conn.commit()

    #關閉連線
    conn.close()
    engine.dispose()
    print('Bug notify is all done!')


with DAG(
    dag_id='django_bug_notify',
    default_args=default_args,
    schedule='30 3,6,9,12,15,18,21 * * *',
    catchup=False
) as dag:
    notification_process = PythonOperator(
        task_id='bug_notify',
        python_callable=notification,
        on_failure_callback=notify.task_custom_failure_function,
        dag=dag
    )

    notification_process
    

if __name__ == "__main__":
    dag.cli()