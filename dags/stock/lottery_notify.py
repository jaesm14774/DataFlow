from stock.collect_logic import *
from lib.notify import DiscordNotify


class StockLotteryCollectProcess(StockCollectLogic):
    """
    收集股票申購資訊
    """
    table_name='股票申購'
    _diff_table_name='股票申購_diff'
    notify_table_name='股票申購_notification'
    _select_columns=['證券代號','抽籤日期']
    numeric_col=['承銷股數','申購股數','承銷價(元)','總承銷金額(元)','總合格件']
    republic_china_of_year_col=['抽籤日期','申購開始日','申購結束日','撥券日期(上市、上櫃日期)']
    error=[]
    column_rename_dict={
        '發行市場':'市場別',
        '申購開始日':'開始日期',
        '申購結束日':'截止日期',
        '承銷股數':'承銷股數(張)',
        '撥券日期(上市、上櫃日期)':'撥券日期',
        '申購股數':'可申購股數(張)',
        '總合格件':'申購筆數',
        '中籤率(%)':'中籤率',
        '收盤':'參考價格',
        '承銷價(元)':'申購價格'
    }
    app_name='stock_lottery'
    
    def republic_of_china_date_transform(self,dt):
        """
        時間處理函數
        """
        dt=dt.replace('/','-')
        dt=dt.split('-')
        dt[0]=str(int(dt[0])+1911)
        
        return '-'.join(dt)

    def collect_lottery(self,year):
        url=f'https://www.twse.com.tw/rwd/zh/announcement/publicForm?date={year}0101&response=json'
        a=requests.post(url,headers=self.headers, timeout=20)
        df=a.json()
        df=pd.DataFrame(df['data'],columns=[col_name.strip() for col_name in df['fields']]).reset_index(drop=True)
        
        #deal error format
        t=df[df['取消公開抽籤'] != '']
        t=t[t.取消公開抽籤 != '取消申購']
        
        if len(t)>0:
            self.error.append(t)
            df=df.drop(t.index,axis=0)
        
        return df
     
    def collect(self):
        df=self.collect_lottery(year=self.now_time.year)
        df.drop(['序號'],axis=1,inplace=True)
        df=df.reset_index(drop=True)
        
        if len(self.error)>0:
            self.error=pd.concat(self.error)
            self.error.drop(['序號'],axis=1,inplace=True)
            self.error=self.error.reset_index(drop=True)
            self.error['證券名稱']+=self.error['證券代號']
            col_position=self.error.columns.tolist().index('證券代號')
            for i in range(len(self.error)):
                self.error.iloc[i,col_position:]=self.error.iloc[i,(col_position+1):].tolist()+[None]
        
            df=df.append(self.error)
        
        df=df.replace('---',None)
        
        for col_name in self.republic_china_of_year_col:
            df[col_name]=df[col_name].map(self.republic_of_china_date_transform)
            
        for col_name in self.numeric_col:
            df[col_name]=df[col_name].apply(self.clean_comma,output_not_string=True)
            
        df.drop(['實際承銷股數','實際承銷價(元)'],axis=1,inplace=True)
        df['承銷股數']=df['承銷股數']/1000
        df['申購股數']=df['申購股數']/1000
        df=df.rename(columns=self.column_rename_dict)
        df=df[~df.duplicated(subset=self._select_columns,keep='last')]

        #新增參考價格、申購獲利(讀取個股資訊)
        all_data=pd.read_sql_query('select 資料日期 as 開始日期,收盤,證券代號 from 個股成交資訊',self.engine)
        all_data['開始日期']=all_data['開始日期'].astype('str')
        
        df=pd.merge(left=df,right=all_data,
                    how='left',on=['證券代號','開始日期']).sort_values(['開始日期','證券代號'])
        df=df.rename(columns=self.column_rename_dict)
        df['抽中獲利']=(df['參考價格'].astype('float')-df['申購價格'].astype('float'))*df['可申購股數(張)']*1000
        df=df.reset_index(drop=True)
        
        #!!!
        #exception(Now not good method to handle error condition)
        df=df[df['總承銷金額(元)'] != '未訂出']

        #!!!

        self.result=df
        
        old_df=pd.read_sql_query(f'select * from {self.notify_table_name}',self.engine)
        old_df['抽籤日期']=old_df['抽籤日期'].astype('str')
        
        notify_df=df[~(df.證券代號+'-'+df.抽籤日期).isin(old_df.證券代號+'-'+old_df.抽籤日期)]
        company_info=pd.read_sql_query('select 證券代號,主要經營業務 from 公司資訊',self.engine)
        self.notify_df=pd.merge(left=notify_df,right=company_info,how='left',on=['證券代號'])
    
    def save(self):
        if len(self.result)>0:
            print('save step start')
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                      if_exists='append',index=0)
            #step2 delete already exist data in 券商進出總表
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.抽籤日期=tb2.抽籤日期
            )
            """
            self.delete_count=self.cursor.execute(delete_sql)
            self.conn.commit()
            #step3 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)

            
    def notification(self):        
        if self.notify_df.shape[0] == 0:
            return('股票申購順利執行')
        
        #Discord notify
        token=pd.read_csv(discord_token_path,encoding='utf_8_sig',index_col='name')
        token=token.loc['新聞即時通','token']
        notify=DiscordNotify()
        notify.webhook_url=token
        
        for i in range(0,self.notify_df.shape[0]):
            #申購通知
            self.notify_df=self.notify_df.where(~self.notify_df.isnull(),'')
            # self.notify_df.抽中獲利=self.notify_df.抽中獲利.round(2)
            
            m1=(
                f"\n日期:{self.notify_df.開始日期.iloc[i]}~{self.notify_df.截止日期.iloc[i]}\n市場別:"
                f"{self.notify_df.市場別.iloc[i]}\n證券代號:{self.notify_df.證券代號.iloc[i]}"
                f"\n證券名稱:{self.notify_df.證券名稱.iloc[i]}\n抽中獲利:{str(self.notify_df.抽中獲利.iloc[i])}"
                f"\n中籤率:{str(self.notify_df.中籤率.iloc[i])}"
                f"\n可申購股數(張):{str(self.notify_df['可申購股數(張)'].iloc[i])}"
                f"\n申購價格:{str(self.notify_df.申購價格.iloc[i])}"
                f"\n抽籤日期:{self.notify_df.抽籤日期.iloc[i]}"
                f"\n撥券日期:{self.notify_df.撥券日期.iloc[i]}"
                f"\n承銷股數(張):{str(self.notify_df['承銷股數(張)'].iloc[i])}\n主要經營業務:"
                f"{self.notify_df.主要經營業務.iloc[i]}\n\n"
            )
            
            notify.notify(msg='\n'+m1)
            
            self.notify_df.loc[:,self._select_columns].to_sql(self.notify_table_name,self.engine,index=0,
                                                              if_exists='append')

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


def lottery_collect(): 
    """
    收集庫藏股資訊
    """
    main_etl=StockLotteryCollectProcess()
    main_etl.process()
    
    print('Done collection of stock lottery collect process')