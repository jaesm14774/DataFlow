from stock.collect_logic import *
from lib.notify import DiscordNotify
from dateutil.relativedelta import relativedelta

class StockEarningCallCollectProcess(StockCollectLogic):
    """
    法說會資訊收集
    """
    table_name='法說會'
    history_notify_table_name='法說會_歷史'
    _diff_table_name='法說會_diff'
    app_name='stock_earning_call'
    domain_url='https://mopsov.twse.com.tw/nas/STR/'

    def revise_time_ch_to_en(self,t):
        n=re.search(string=t,pattern=r'^(\d+)/').group(1)
        return re.sub(string=t,pattern=n,repl=str(int(n)+1911))

    def revise_time_en_to_ch(self,t):
        n=re.search(string=t,pattern=r'^(\d+)/').group(1)
        return re.sub(string=t,pattern=n,repl=str(int(n)-1911))

    def extract_correct_time_format(self,t):
        if '至' in t:
            return t.split('至')[0].strip()
        else:
            assert len(t) == 10
            return t
        
    def get_earnings_call(self,roc_year,roc_month):
        url='https://mopsov.twse.com.tw/mops/web/t100sb02_1'
        data={
            'encodeURIComponent':'1',
            'step':'1',
            'firstin':'1',
            'off':'1',
            'TYPEK':'sii',
            'year':roc_year,
            'month':roc_month,
            'co_id':''
        }
        a=self.simulate_request(event={'url':url,'headers':self.headers,
                                       'data':data,
                                       'method':'post'})
        soup=BeautifulSoup(a['text'],'lxml')
        df=pd.read_html(str(soup))[8]
        df.columns=[s[1] for s in df.columns]
        df=df.loc[:,['公司代號', '公司名稱', '召開法人說明會日期','中文檔案', '英文檔案']]
        df=df.rename(columns={
            '公司代號':'證券代號',
            '公司名稱':'證券名稱',
            '召開法人說明會日期':'資料日期',
            '中文檔案':'file_url'
        })
        df['資料日期']=df['資料日期'].map(self.revise_time_ch_to_en).str.replace('/','-')
        df['資料日期']=df['資料日期'].map(self.extract_correct_time_format)

        df.loc[df.file_url == '','file_url']=df['英文檔案'].tolist()
        df.loc[df.file_url.isnull(),'file_url']=df['英文檔案'].tolist()
        df['file_url']=[u if u.find('http') != -1 or u.find('當日會後') != -1 else self.domain_url+u for u in df['file_url']]
        df.loc[df.file_url == '內容檔案於當日會後公告於公開資訊觀測站','file_url']=None
        df.drop(['英文檔案'],axis=1,inplace=True)
        
        return df    
    
    def collect(self):
        all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
        
        df=[]
        for i in range(0,3):
            temp_time=(self.now_time-relativedelta(months=i)).strftime('%Y/%m/%d')

            roc_time=self.revise_time_en_to_ch(temp_time)
            roc_year=re.search(string=roc_time,pattern=r'^(\d+)/').group(1)
            roc_month=re.search(string=roc_time,pattern=r'/(\d+)/').group(1)
            df.append(self.get_earnings_call(roc_year,roc_month))
            time.sleep(5)
            
            print(f'完成 {roc_time} 法說會收集')
            
        result=pd.concat(df)
        result=result[~result.duplicated(subset=['資料日期','證券代號'])]
        result['證券代號']=result['證券代號'].astype('str')
        result=pd.merge(left=result,right=all_code_info.loc[:,['證券代號','市場別']],how='left',on=['證券代號'])
        
        self.result=result
        
    def save(self):
        if len(self.result)>0:
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            index=0,if_exists='append')
            
            #step2 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期
            )
            """
            self.delete_count=self.cursor.execute(delete_sql)
            self.conn.commit()
            
            self.result.to_sql(self.table_name,self.engine,index=0,
                               if_exists='append')
            self.insert_count=len(self.result)
            
    def notification(self):
        df=self.result
        
        old_notify=pd.read_sql_query(f'select * from {self.history_notify_table_name}',self.engine)
        
        df=df[~(df.資料日期+df.證券代號.astype('str')).isin(old_notify.資料日期.astype('str')+old_notify.證券代號.astype('str'))]
        
        #沒有法說會檔案，通知也沒有用，可能是還沒有更新
        df=df[~df.file_url.isnull()]

        if df.shape[0] == 0:
            return('法說會順利執行')
        
        df=df.reset_index(drop=True)
        # #下載新的法說檔案
        # for ii in range(0,df.shape[0]):
        #     date=df.資料日期.iloc[ii]
        #     name=df.證券名稱.iloc[ii].replace('*','')
        #     file_url=df.file_url.iloc[ii]
        #     file_type=re.search(string=file_url,pattern=r'\..{1,5}$').group(0).replace('.','')
            
        #     a=self.simulate_request(event={'url':file_url,'headers':self.headers,'method':'get'})
        #     try:
        #         with open(earning_call_path+'file/'+date.replace('-','')+name+f'.{file_type}','wb') as f:
        #             f.write(base64.b64decode(a['content'].encode()))
        #     except:
        #         continue    
        #新增已通知紀錄
        df.loc[:,self._select_columns].to_sql(self.history_notify_table_name,self.engine,index=0,
                                         if_exists='append')
        
        #Discord notify
        token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
        token=token.loc['新聞即時通','token']
        notify=DiscordNotify()
        notify.webhook_url=token
        
        for i in range(0,df.shape[0]):
            #法說會
            m1=(
                f'\n{df.資料日期.iloc[i]}\n{df.市場別.iloc[i]}\n{df.證券代號.iloc[i]}'
                f'\n{df.證券名稱.iloc[i]}\n\n{df.file_url.iloc[i]}\n'
            )
            
            notify.notify(msg='\n'+m1)
                        
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
        
def earning_call_collect(): 
    """
    法說會資料收集
    """
    main_etl=StockEarningCallCollectProcess()
    main_etl.process()
    
    print('Done collection of stock earnning call process')