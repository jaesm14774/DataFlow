from stock.collect_logic import *
from lib.notify import DiscordNotify
from dateutil.relativedelta import relativedelta
import time


class StockTreasuryHistoryInfoCollectProcess(StockCollectLogic):
    """
    收集庫藏股資訊
    """
    table_name='庫藏股_歷史'
    app_name='stock_treasury_history'
    numeric_col=['預定買回股數','買回價格區間最低','買回價格區間最高','本次已買回股數(空白為尚在執行中)',
                 '本次執行完畢已註銷或轉讓股數','本次已買回總金額(空白為尚在執行中)',
                 '本次平均每股買回價格(空白為尚在執行中)','本次買回股數佔公司已發行股份總數比例(%)(空白為尚在執行中)',
                 '買回股份總金額上限(依最新財報計算之法定上限)']
    tmp_table_name='庫藏股_歷史_temp'
    delete_table_name='delete_庫藏股'
    notify_table_name='庫藏股通知'
    
    _select_columns=['證券代號','預定買回期間起']
    
    def __init__(self):
        super().__init__()
        start_date='2000/01/01'
        self.start_date=datetime.datetime.strptime(start_date,'%Y/%m/%d')
        
    def revise_time_ch_to_en(self,t):
        n=re.search(string=t,pattern=r'^(\d+)/').group(1)
        return re.sub(string=t,pattern=n,repl=str(int(n)+1911))

    def revise_time_en_to_ch(self,t):
        n=re.search(string=t,pattern=r'^(\d+)/').group(1)
        return re.sub(string=t,pattern=n,repl=str(int(n)-1911))
    
    def clean_time_format(self,t):
        return re.sub(string=t,pattern='/',repl='')
    
    def collect(self): 
        result=[]
        R=[]
        while self.start_date < self.now_time:  
            st=self.clean_time_format(self.revise_time_en_to_ch(self.start_date.strftime('%Y/%m/%d')))
            ed=self.clean_time_format(self.revise_time_en_to_ch((self.start_date+relativedelta(years=10)).strftime('%Y/%m/%d')))     
            #上市
            url='https://mopsov.twse.com.tw/mops/web/ajax_t35sc09'
            data={
                'encodeURIComponent':1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'TYPEK': 'sii',
                'd1': st,
                'd2': ed,
                'RD': 1
            }
            a=self.simulate_request(event={'url':url,'data':data,
                                    'method':'post'})
            print(a)
            twse_soup=BeautifulSoup(a['text'],'lxml')
            
            #上櫃
            url='https://mopsov.twse.com.tw/mops/web/ajax_t35sc09'
            data={
                'encodeURIComponent':1,
                'step': 1,
                'firstin': 1,
                'off': 1,
                'TYPEK': 'otc',
                'd1': st,
                'd2': ed,
                'RD': 1
            }
            b=self.simulate_request(event={'url':url,'data':data,
                                    'method':'post'})
            tpex_soup=BeautifulSoup(b['text'],'lxml')
            
            col_name=['序號','證券代號','公司名稱','董事會決議日期','買回目的','買回股份總金額上限(依最新財報計算之法定上限)',
            '預定買回股數','買回價格區間最低','買回價格區間最高','預定買回期間起','預定買回期間迄','是否執行完畢',
            '買回達一定標準資料','本次已買回股數(空白為尚在執行中)','本次執行完畢已註銷或轉讓股數',
            '本次已買回股數佔預定買回股數比例(%)(空白為尚在執行中)','本次已買回總金額(空白為尚在執行中)',
            '本次平均每股買回價格(空白為尚在執行中)','本次買回股數佔公司已發行股份總數比例(%)(空白為尚在執行中)',
            '本次未執行完畢之原因']
            
            """
            買回目的統計件數：
            (1)轉讓股份予員工
            (2)股權轉換
            (3)維護公司信用及股東權益
            """
            dat=[]
            tb=twse_soup.find('form',{'action':'/mops/web/ajax_t35sb01'}).find('table',class_='hasBorder')
            for r in tb.find_all(class_=re.compile('even|odd')):
                dat.append(pd.DataFrame([[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in r.find_all('td')]],
                                        columns=col_name,index=[1]))
            """
            買回目的統計件數：
            (1)轉讓股份予員工
            (2)股權轉換
            (3)維護公司信用及股東權益
            """
            dat2=[]
            tb=tpex_soup.find('form',{'action':'/mops/web/ajax_t35sb01'}).find('table',class_='hasBorder')
            for r in tb.find_all(class_=re.compile('even|odd')):
                dat2.append(pd.DataFrame([[re.sub(string=s.text,pattern=r'^\s+|\s+$',repl='') for s in r.find_all('td')]],
                                        columns=col_name,index=[1]))

            twse=pd.concat(dat)
            tpex=pd.concat(dat2)
            R=pd.concat([twse,tpex])
            result.append(R)
            self.start_date=self.start_date+relativedelta(years=10)
            print(self.start_date.strftime('%Y/%m/%d'),'--- ',self.now_time.strftime('%Y/%m/%d'),'庫藏股收集完成')
            time.sleep(5)
        
        result=pd.concat(result)
        #刪除還在執行的資料(對現有想要的數據沒有幫助，未來更新時，再加入)
        result['預定買回期間迄']=result.預定買回期間迄.map(self.revise_time_ch_to_en).astype('datetime64[ns]')
        result['預定買回期間起']=result.預定買回期間起.map(self.revise_time_ch_to_en).astype('datetime64[ns]')
        result=result.drop(['買回達一定標準資料'],axis=1) #不太好抓，且沒有好記的方式，忽略
        for s in self.numeric_col:
            result[s]=result[s].apply(self.clean_comma,output_not_string=True)
            
        result=result.replace('',np.nan)
        result=result.drop(['序號'],axis=1)
        result=result[~result.duplicated(subset=self._select_columns)].reset_index(drop=True)
        
        self.notify_result=result[result.是否執行完畢 == 'N']
        self.history_result=result[result.是否執行完畢 != 'N']
        
        self.cursor.execute(f'truncate table {self.tmp_table_name}')
        self.conn.commit()
        self.history_result.loc[:,self._select_columns].to_sql(self.tmp_table_name,self.engine,index=0,
                                                               if_exists='append')
        
        find_delete_record_sql=f"""
        select *
        from {self.table_name} tb
        where not exists (
            select *
            from {self.tmp_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.預定買回期間起=tb2.預定買回期間起
            
        )
        """
        self.delete_df=pd.read_sql_query(find_delete_record_sql,self.engine)
    
    
    def calculate_old_record_mean(self,df):
        """
        計算庫藏股過往的 回收記錄
        """
        temp=pd.DataFrame({
            'n':df.shape[0],
            'confidence(%)':np.round(df['本次已買回股數佔預定買回股數比例(%)(空白為尚在執行中)'].mean(),decimals=2),
            'every_record':' ; '.join(df['本次已買回股數佔預定買回股數比例(%)(空白為尚在執行中)'].astype('str').tolist())
        },index=[1])  
        return temp
    
    def notification(self):
        old_notify=pd.read_sql_query(f'select * from {self.notify_table_name}',self.engine).loc[:,self._select_columns]
        old_notify['預定買回期間起']=old_notify['預定買回期間起'].astype('str')

        self.notify_result['預定買回期間起']=self.notify_result.預定買回期間起.astype('str')
        
        self.notify_result=self.notify_result[~((self.notify_result.證券代號+self.notify_result.預定買回期間起).isin(old_notify.證券代號+old_notify.預定買回期間起))]
        
        if self.notify_result.shape[0] == 0:
            return('庫藏股順利執行')
        
        history_record=pd.read_sql_query(f'select * from {self.table_name}',self.engine)
        
        summary=history_record.groupby(['公司名稱','證券代號']).apply(self.calculate_old_record_mean) \
            .reset_index(drop=False).drop(['level_2'],axis=1)
        
        temp_info=summary[summary.證券代號.isin(self.notify_result.證券代號)]
        #存入已通知紀錄
        self.notify_result.loc[:,['證券代號','公司名稱','預定買回期間起']].to_sql(self.notify_table_name,
                                                                   self.engine,index=0,if_exists='append')
        
        #Discord notify
        token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
        token=token.loc['新聞即時通','token']
        notify=DiscordNotify()
        notify.webhook_url=token
        
        temp_info=pd.merge(left=temp_info,right=self.notify_result,
                           on=['公司名稱','證券代號'],how='left')
        
        for col_name in temp_info.columns:
            temp_info[col_name]=temp_info[col_name].astype('str')
        
        for i in range(0,temp_info.shape[0]):
            #庫藏股資訊
            m1='\n'+temp_info['公司名稱'].iloc[i]+'('+temp_info['證券代號'].iloc[i]+')'+'庫藏股:'+\
            '\n'+'價格低->高:'+temp_info['買回價格區間最低'].iloc[i]+'~'+temp_info['買回價格區間最高'].iloc[i]+\
            '\n'+'區間:'+temp_info['預定買回期間起'].iloc[i]+'~'+temp_info['預定買回期間迄'].iloc[i]
            
            notify.notify(msg='\n'+m1)
            
            #庫藏股歷史訊息
            m2='\n\n'+temp_info['公司名稱'].iloc[i]+'('+temp_info['證券代號'].iloc[i]+')'+'歷史紀錄:'+\
            '\n'+'歷史發行次數:'+temp_info['n'].iloc[i]+\
            '\n'+'歷史執行百分比(%):'+temp_info['confidence(%)'].iloc[i]+\
            '\n'+'歷史各別紀錄:'+temp_info['every_record'].iloc[i]

            notify.notify(msg='\n'+m2)
    
    def save(self):
        if len(self.delete_df)>0:
            self.delete_df=self.delete_df[~self.delete_df.duplicated(subset=['證券代號','預定買回期間起'])]
            
            self.cursor.execute(f'truncate table {self.delete_table_name}_diff')
            self.conn.commit()
            self.delete_df.loc[:,['證券代號','預定買回期間起']].to_sql(
                self.delete_table_name+'_diff',self.engine,index=0,if_exists='append'
            )

            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.delete_table_name} as tb
            where exists (
            select *
            from {self.delete_table_name}_diff tb2
            where tb.證券代號=tb2.證券代號 and tb.預定買回期間起=tb2.預定買回期間起
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.delete_df.drop(['id'],axis=1).to_sql(self.delete_table_name,self.engine,
                                                      index=0,if_exists='append')   
        
        if len(self.history_result)>0:
            self.cursor.execute(f'truncate table {self.table_name}')
            self.conn.commit()
            self.delete_count=-1
            self.history_result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.history_result)
            
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

#收集庫藏股資訊
def treasury_history_collect(): 
    main_etl=StockTreasuryHistoryInfoCollectProcess()
    main_etl.process()
    
    print('Done collection of treasury history record process')