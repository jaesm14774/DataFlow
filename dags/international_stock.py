from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.notify import DiscordNotify
from stock.collect_logic import *

default_args = {
        'owner': 'jaesm14774',    
        'start_date': datetime.datetime(2022,12,22),
        'email': ['jaesm14774@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'execution_timeout': datetime.timedelta(seconds=600),
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=10),
        'tags':'stock collect everyday',
        'do_xcom_push': True
    }

#Discord notify
token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
token=token.loc['程式執行狀態','token']
notify=DiscordNotify()
notify.webhook_url=token


class StockInternationalCollectProcess(StockCollectLogic):
    table_name='international_stock'
    _diff_table_name='international_stock_diff'
    _select_columns=['stock_name','create_date']
    app_name='stock_market_index'
    
    def transform_time(self,x,now_year):
        x=now_year+'/'+x
        
        return datetime.datetime.strptime(x,'%Y/%m/%d %H:%M').strftime('%Y-%m-%d %H:%M:%S')
    
    def collect(self):
        """
        爬蟲程式
        """
        #抓取yahoo國際股市
        a=requests.get('https://tw.stock.yahoo.com/world-indices',headers=self.headers, timeout=20)
        soup=BeautifulSoup(a.text,'lxml')
                
        #yahoo網頁幾乎class都是很怪的標籤(可能隨時間變動)，因此採取笨方法，易錯
        col_name=[s.text.strip() for s in soup.find(class_='table-header-wrapper').find_all('div') if s.text.strip() != '']
        col_name=col_name[1:]

        data=[[sub_part.text.strip() for sub_part in part.find_all('span') if sub_part.text.strip() != ''] \
            for part in soup.find(class_='table-body-wrapper').find_all('li',class_='List(n)')]
        
        data=[[s[0]]+s[2:] for s in data] #捨棄股票代碼
        data=pd.DataFrame(data,columns=col_name)
        data.drop(['買進','賣出'],axis=1,inplace=True)
        data=data.rename(columns={
            '股名/股號':'stock_name',
            '股價':'close',
            '漲跌':'diff', '漲跌幅(%)':'variety', '開盤':'open', '昨收':'yesterday_close', 
            '最高':'max', '最低':'min',
            '成交量(股)':'volume',
            '時間(CST)':'created_at'
        })
        data['create_date']=self.now_time.strftime('%Y-%m-%d')

        for s in ['close','diff','variety','open','yesterday_close','max','min']:
            data[s]=data[s].map(self.clean_comma)

        #修改時間格式
        y=format(self.now_time,'%Y')
        data['created_at']=data['created_at'].apply(self.transform_time,now_year=y)
                
        #因為yahoo符號用顏色代表，無法表現正負值，更改成正確數值
        change_index=data[(data['close']<data['yesterday_close'])].index.tolist()
        if len(change_index)>0:
            data.loc[change_index,'diff']=data.loc[change_index,'diff'].astype('float')*(-1)
            data.loc[change_index,'variety']='-'+data.loc[change_index,'variety']
    
        self.data=data
    
    def save(self):
        """
        儲存資料進資料庫(或其他)
        """
        self.cursor.execute(f'truncate table {self._diff_table_name}')
        self.conn.commit()
        self.data.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,index=0,if_exists='append')
        delete_sql=f"""
        delete tb from {self.table_name} as tb
        where exists (
        select *
        from {self._diff_table_name} tb2
        where tb.stock_name=tb2.stock_name and tb.create_date=tb2.create_date
        )
        """
        self.delete_count=self.cursor.execute(delete_sql)
        self.conn.commit()
        
        self.data.to_sql(self.table_name,self.engine,if_exists='append',index=0)
        self.insert_count=len(self.data)
        
    def notification(self):
        token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
        token=token.loc['新聞即時通','token']
        news_notify=DiscordNotify()
        news_notify.webhook_url=token
        #開始一天一次的通知
        select_sql=f"""
        select * from {self.table_name} as tb
        where not exists (
        select *
        from international_stock_notification tb2
        where tb.stock_name=tb2.stock_name and tb.create_date=tb2.create_date
        )
        """
        
        df=pd.read_sql_query(select_sql,self.engine)

        if len(df) != 0:
            #更新不通知的國家
            temp=df[~df.stock_name.isin(['道瓊工業指數', 'S&P 500指數', 'NASDAQ指數', '費城半導體指數',\
                                '日經225指數','印度孟買指數', '馬來西亞股市'])]

            #更新資料庫notify變數為1
            if len(temp)>0:
                temp.loc[:,self._select_columns].to_sql('international_stock_notification',self.engine,
                                                        index=0,if_exists='append')
            #通知想要的國家
            df=df[df.stock_name.isin(['道瓊工業指數', 'S&P 500指數', 'NASDAQ指數', '費城半導體指數',\
                        '日經225指數','印度孟買指數', '馬來西亞股市'])]
            
            if len(df)>0:
                for i in range(0,df.shape[0]):
                    news_notify.notify(msg='\n名稱:'+df.stock_name.iloc[i]+'\n指數:'+str(df.close.iloc[i])+\
                                    '\n漲跌幅:'+df.variety.iloc[i]+'\n時間:'+str(df.created_at.iloc[i]))
                    news_notify.notify(msg='\n\n'+'*'*20)
                
                df.loc[:,self._select_columns].to_sql('international_stock_notification',self.engine,
                                                                            index=0,if_exists='append')
    
    def process(self):
        """
        主要的資料流程
        """
        try:
            self.collect()
            self.save()
            self.notification()
            self.log_process_record()
            
            self.log_record.success = 1
            print('收集成功!')
        except Exception as e:
            self.log_record.raise_error(repr(e))
            print('任務失敗')
            raise e
        finally:
            self.log_record.insert_to_log_record()
            self.end()

#國際指數通知
def interantional_stock():
    main_etl=StockInternationalCollectProcess()
    main_etl.process()
    
    print(f'Done of international stock process')


with DAG(
    dag_id='stock_international_stock_process',
    default_args=default_args,
    schedule='20 22 * * 1,2,3,4,5,0',
    catchup=False
) as dag:
    stock_international_stock_process = PythonOperator(
        task_id='international_stock_process',
        python_callable=interantional_stock,
        dag=dag,
        on_failure_callback=notify.task_custom_failure_function
    )
    
    stock_international_stock_process

if __name__ == '__main__':
    dag.cli()