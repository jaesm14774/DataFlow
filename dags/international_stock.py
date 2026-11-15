from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.notify import discord_notifier
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
notify=discord_notifier('程式執行狀態', discord_token_path)


class StockInternationalCollectProcess(StockCollectLogic):
    table_name='international_stock'
    _diff_table_name='international_stock_diff'
    _select_columns=['stock_name','create_date']
    app_name='stock_market_index'
    
    def transform_time(self,x,now_year):
        raw_time=str(x).strip()
        if raw_time in {'-','--'}:
            return None
        candidates=[
            (raw_time,'%Y/%m/%d %H:%M'),
            (raw_time,'%Y-%m-%d %H:%M'),
            (f'{now_year}/{raw_time}','%Y/%m/%d %H:%M'),
            (f'{self.now_time.strftime("%Y/%m/%d")} {raw_time}','%Y/%m/%d %H:%M'),
        ]
        for value,time_format in candidates:
            try:
                return datetime.datetime.strptime(value,time_format).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        raise ValueError(f'Unsupported Yahoo index timestamp: {raw_time!r}')
    
    def collect(self):
        """
        爬蟲程式
        """
        #抓取yahoo國際股市
        response=requests.get('https://tw.stock.yahoo.com/world-indices',headers=self.headers, timeout=20)
        response.raise_for_status()
        soup=BeautifulSoup(response.text,'lxml')

        required_columns={
            '股名/股號','股價','漲跌','漲跌幅(%)','買進','賣出','開盤','昨收',
            '最高','最低','成交量(股)','時間(CST)'
        }
        table=None
        col_name=None
        for candidate in soup.find_all('table'):
            header_row=candidate.find('tr')
            if header_row is None:
                continue
            candidate_columns=[cell.get_text(strip=True)
                               for cell in header_row.find_all(['th','td'],recursive=False)]
            if required_columns.issubset(candidate_columns):
                table=candidate
                col_name=candidate_columns
                break
        if table is None:
            raise ValueError('Yahoo world indices table is missing required columns')

        data=[]
        for row in table.find_all('tr')[1:]:
            cells=row.find_all(['th','td'],recursive=False)
            if not cells:
                continue
            if len(cells) != len(col_name):
                raise ValueError(
                    f'Unexpected Yahoo world indices row width: '
                    f'expected {len(col_name)}, got {len(cells)}'
                )
            stock_name=cells[0].find('span')
            if stock_name is None:
                raise ValueError('Yahoo world indices row is missing its index name')
            data.append([stock_name.get_text(strip=True)]+
                        [cell.get_text(strip=True) for cell in cells[1:]])
        if not data:
            raise ValueError('Yahoo world indices table contains no index rows')

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

        numeric_columns=['close','diff','open','yesterday_close','max','min']
        for s in numeric_columns:
            data[s]=data[s].map(lambda value: self.clean_comma(value,output_not_string=True))
        data['variety']=data['variety'].map(self.clean_comma)

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
        self.delete_count,self.insert_count=self.replace_by_keys(self.data,self.table_name,self._diff_table_name,
            ['stock_name','create_date'],self._select_columns)
        
    def notification(self):
        news_notify=discord_notifier('新聞即時通', discord_token_path)
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
            print('[國際指數] 成功')
        except Exception as e:
            self.log_record.raise_error(repr(e))
            print(f'[國際指數] 失敗 | {type(e).__name__}: {e}')
            raise e
        finally:
            self.log_record.insert_to_log_record()
            self.end()

#國際指數通知
def interantional_stock():
    main_etl=StockInternationalCollectProcess()
    main_etl.process()
    
    print('[國際指數] DAG 任務結束')


with DAG(
    dag_id='stock_international_stock_process',
    description='每日收集並更新國際市場指數與行情資料。',
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
