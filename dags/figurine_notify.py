import requests
import re
import datetime
import pandas as pd
import numpy as np
import glob
import random

from lib.get_sql import *
from lib.log_process_execution import BaseLogRecord
from lib.notify import DiscordNotify
from common.config import *

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,12,21),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'execution_timeout': datetime.timedelta(seconds=600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
        'tags':'shopee figurine collection'
    }

image_path='/opt/airflow/plugins/figurine_notify/image/'
#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token

#公仔通知 notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['公仔通知','token']
figurine_notify=DiscordNotify()
figurine_notify.webhook_url=token

app_name = 'figurine'
database_name = 'crawler'
table_name = 'figurine_item'

#初始化log
log_record=BaseLogRecord(process_date=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                         app_name=app_name)

user_agent = [
"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
]

#連接mysql
connection=pd.read_csv(sql_configure_path,index_col='name')
conn,cursor,engine=get_sql(connection.loc['host','value'],
                           int(connection.loc['port','value']),
                           connection.loc['user','value'],
                           connection.loc['password','value'],database_name)


before_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]
log_record.set_before_count(before_count)

# session=requests.session()
# session.headers={'user-agent':random.choice(user_agent),
#                 'x-api-source': 'pc','x-requested-with': 'XMLHttpRequest',
#                 'referer': 'https://shopee.tw'
#                 }

# session.get('https://shopee.tw/', timeout=20)

def get_shopee_product(session,shop_id,n_product=100,sort_type=2):
    """
    sort_type:[1,2] (1: 熱門 2: 依時間排序),
    shop_id:蝦皮店家的代號,
    n_product:獲得商品資訊的數量上限，最多100筆 (offset 忽略前n筆，可以透過這個參數獲取歷史資料)
    """
    if n_product>100:
        n_product=100
        
    url=(
        f'https://shopee.tw/api/v4/shop/rcmd_items?'
        f'bundle=shop_page_category_tab_main&limit={n_product}&shop_id={shop_id}&'
        f'sort_type={sort_type}&offset=0&page_type=shop'
    )  
    
    a=session.get(url,timeout=60)    
    data=a.json()['data']
    rs=[]
    
    if 'items' not in data:
        return pd.DataFrame()
    
    for part in data['items']:    
        rs.append(pd.DataFrame({
            'itemid':part['itemid'],
            'image':part['image'],
            'shopid':part['shopid'],
            'price_min':int(part['price_min']/100000),
            'price_max':int(part['price_max']/100000),
            'currency':part['currency'],
            'show_free_shipping':part['show_free_shipping'],
            'brand':part['brand'],
            'liked_count':part['liked_count'],
            'name':part['name'],
            'historical_sold':part['historical_sold'],
            'ctime':datetime.datetime.fromtimestamp(part['ctime']).strftime('%Y-%m-%d %H:%M:%S')
        },index=[0])) 
    
    if len(rs)==0:
        return pd.DataFrame()
    
    data=pd.concat(rs)
    data=data.reset_index(drop=True)
    return data

def search_key(x,key_word=['星之卡比','卡比','耿鬼']):
    if isinstance(key_word,list) or isinstance(key_word,np.array):
        if bool(re.search(string=x,pattern='|'.join(key_word))):
            return True
        else:
            return False
    elif isinstance(key_word,str):
        if bool(re.search(string=x,pattern=key_word)):
            return True
        else:
            return False        
    else:
        print('Key_word only uses in list, array, or string.')
        raise RuntimeError('')

#truncate insert (overwrite)
def collect_data(session, shop_id=11664018):
    try:
        if isinstance(shop_id,list):
            data=[]
            for shop in shop_id:
                data.append(get_shopee_product(shop_id=shop))
            data=pd.concat(data)
        else: 
            data=get_shopee_product(shop_id=shop_id)
        
        D_old=pd.read_sql_query(f'select * from {table_name}',engine).drop(['id','create_time'],axis=1)
        
        D=pd.concat([D_old,data])
        D=D[~D.duplicated(subset=['itemid'],keep='last')]
        
        cursor.execute(f'truncate table {table_name}')
        conn.commit()
        
        delete_count=-1
        log_record.set_delete_count(delete_count)
        
        D.to_sql(table_name,engine,index=0,if_exists='append')

        log_record.set_insert_count(D.shape[0])
        after_count=pd.read_sql_query(f'select count(1) as N from {table_name}',engine)['N'].iloc[0]  
        log_record.set_after_count(after_count)
        log_record.success = 1
        print('收集成功!')
    except Exception as e:
        log_record.raise_error(repr(e))
        print('任務失敗')
        raise e        
    finally:
        log_record.insert_to_log_record()

def notification(session):
    _key_col=['itemid','image']
    
    D=pd.read_sql_query(f'select * from {table_name}',engine)
    notify_old=pd.read_sql_query('select * from figurine_notified',engine)    

    data=D[~D.itemid.isin(notify_old.itemid.tolist())]
    
    if data.shape[0] !=0:
        temp=data[data.name.apply(search_key)]
        
        if temp.shape[0] ==0:
            print('All Done!')
        else:
            last_index=glob.glob(image_path+'*.jpg')
            
            if len(last_index) == 0:
                last_index=0
            else:
                last_index=len(last_index)    
            
            for i in range(0,temp.shape[0]):
                msg='\n商品名稱: \n'+temp.name.iloc[i]+'\n\n'
                if temp.price_min.iloc[i] == temp.price_max.iloc[i]:
                    price=str(temp.price_min.iloc[i])+' '+temp.currency.iloc[i]
                else:
                    price=str(temp.price_min.iloc[i])+'~'+str(temp.price_max.iloc[i])+' '+temp.currency.iloc[i]
                msg=msg+'價格: '+price+'\n\n'
                msg=msg+'歷史售出數量: '+str(temp.historical_sold.iloc[i])+'\n\n'
                msg=msg+'網址: \n'+'https://shopee.tw/'+temp.name.iloc[i]+'-i.'+str(temp.shopid.iloc[i])+'.'+str(temp.itemid.iloc[i])
                #下載圖片
                a=session.get('https://cf.shopee.tw/file/{}_tn'.format(temp.image.iloc[i]))
                with open(image_path+str(last_index+i)+'.jpg','wb') as f:
                    f.write(a.content) 
                               
                figurine_notify.notify(msg=msg,file_path=image_path+str(last_index+i)+'.jpg')
            
            temp.loc[:,_key_col].to_sql('figurine_notified',engine,index=0,if_exists='append')
            
            print('All Done!')
            
            
with DAG(
    dag_id='figurine_notification',
    default_args=default_args,
    schedule='30 2,14 * * *',
    catchup=False
) as dag:
    collect_data = PythonOperator(
        task_id='get_shopee_figurine_data',
        python_callable=collect_data,
        op_kwargs={'shop_id':[11664018,342018,4278882,2532889]},
        on_failure_callback=notify.task_custom_failure_function,
        dag=dag
    )

    notification = PythonOperator(
        task_id='shopee_figurine_notification',
        python_callable=notification,
        on_failure_callback=notify.task_custom_failure_function,
        dag=dag
    )

    collect_data >> notification

if __name__ == "__main__":
    dag.cli()