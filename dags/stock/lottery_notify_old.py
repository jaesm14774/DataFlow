from stock.collect_logic import *
from lib.notify import DiscordNotify


class StockLotteryCollectProcess(StockCollectLogic):
    """
    收集股票申購資訊
    """
    
    table_name='股票申購_過去'
    app_name='stock_lottery'
    notify_table_name='股票申購_新'
    numeric_col=['抽中獲利','申購筆數','申購股數','預扣費用','承銷股數(張)','總承銷金額(元)']
    
    def clean_sign(self,txt,sign):
        txt=re.sub(string=txt,pattern=sign,repl='')
        if txt == '':
            return np.nan
        else:
            return int(txt)
    
    def deal_start_end_date(self,s):
        r=[]
        for i in re.finditer(string=s,pattern=r'\d{4}-\d{2}-\d{2}'):
            r.append(i.group(0))
            
        st=r[0]
        ed=r[1]
        return st,ed

    def deal_stock_code_name(self,s):
        code=s.split('-')[0]
        name=s.split('-')[1].replace(code,'')
        
        return code,name
        
    def parse_soup(self,soup):
        total_dat=[]
        for ta in soup.find_all('table',{'border':1,'width':'98%'})[1:]:
            col_name=[s.text.strip() for s in ta.find('tr').find_all('td')]
            row_data=[[(subpart.find('a').text.strip()+'-'+subpart.text.strip()) if subpart.find('a') is not None else subpart.text.strip() for subpart in part.find_all('td')] for part in ta.find_all('tr')[1:]]    

            total_dat.append(pd.DataFrame(row_data,columns=col_name))
        
        if len(total_dat) == 0:
            return pd.DataFrame()
        
        total_dat=pd.concat(total_dat)
        total_dat=total_dat.rename(columns={'承銷股數(千股)':'承銷股數(張)'})
        #string to int type
        for s in self.numeric_col:
            total_dat[s]=total_dat[s].apply(self.clean_sign,sign=',')
        
        #開始日期截止日期
        r=[self.deal_start_end_date(s) for s in total_dat['開始日期截止日期']]
        total_dat['開始日期']=[s[0] for s in r]
        total_dat['截止日期']=[s[1] for s in r]
        
        #股票代號股票名稱
        CODE=[];NAME=[]
        for s in total_dat.股票代號股票名稱:
            code,name=self.deal_stock_code_name(s)
            
            CODE.append(code)
            NAME.append(name)
        
        total_dat['證券代號']=CODE
        total_dat['證券名稱']=NAME
        
        total_dat=total_dat.loc[:,['證券代號', '證券名稱','市場別', '抽中獲利', '獲利率', '中籤率',
                                '申購筆數', '申購價格', '參考價格', '申購股數', '開始日期',
                                '截止日期','預扣費用', '預扣款日', '抽籤日期', '還款日期', 
                                '撥券日期', '承銷股數(張)', '總承銷金額(元)', '主辦券商']]
        return total_dat

    def collect_lottery_subscription_lastest(self):
        """
        收集最新股票申購紀錄
        """
        a=requests.get('https://stockluckydraw.com/StockInfoTable.htm',
                    headers=self.headers,timeout=self.timeout)
        a.encoding='utf8'
        soup=BeautifulSoup(a.text,'lxml') 
        #備註 : 與parse soup函數有些微差異
        total_dat=[]
        for ta in soup.find_all('table',{'border':1,'width':'98%'}):
            col_name=[s.text.strip() for s in ta.find('tr').find_all('td')]
            row_data=[[(subpart.find('a').text.strip()+'-'+subpart.text.strip()) if subpart.find('a') is not None else subpart.text.strip() for subpart in part.find_all('td')] for part in ta.find_all('tr')[1:]]    

            total_dat.append(pd.DataFrame(row_data,columns=col_name))
            
        total_dat=pd.concat(total_dat)
        total_dat=total_dat.where(~total_dat.isnull(),'')
        #string to int type
        for s in ['抽中獲利','申購筆數','申購股數','預扣費用','承銷股數(張)','總承銷金額(元)']:
            total_dat[s]=total_dat[s].apply(self.clean_sign,sign=',')

        #股票代號股票名稱
        CODE=[];NAME=[]
        for s in total_dat.股票代號股票名稱:
            code,name=self.deal_stock_code_name(s)
            CODE.append(code)
            NAME.append(name)
        
        total_dat['證券代號']=CODE
        total_dat['證券名稱']=NAME
        total_dat=total_dat.drop(['股票代號股票名稱'],axis=1)
        total_dat=total_dat.loc[:,['證券代號', '證券名稱','市場別', '抽中獲利', '獲利率', '中籤率', 
                                '申購筆數', '申購價格', '參考價格', '申購股數', '開始日期',
                                '截止日期','預扣費用', '預扣款日', '抽籤日期', '還款日期','撥券日期',
                                '承銷股數(張)', '總承銷金額(元)', '主辦券商']]
        return total_dat

    def collect_lottery_subscription(self,YEAR=None):
        """
        股票申購，歷史紀錄
        """
        if YEAR is None:
            a=requests.get('https://stockluckydraw.com/stock/StockInfoOld.php',
                        headers=self.headers,timeout=self.timeout)
        else:
            a=requests.get(f'https://stockluckydraw.com/stock/StockInfoOld.php?YEAR={YEAR}',
                        headers=self.headers,timeout=self.timeout)

        soup=BeautifulSoup(a.text,'lxml')
        return self.parse_soup(soup)
     
    def collect(self):
        #舊資料
        old=pd.read_sql_query(f'select * from {self.table_name}',self.engine)

        #最新的歷史資料
        history_new=self.collect_lottery_subscription(YEAR=None)
        if len(history_new)>0:
            history_new=history_new[~history_new.duplicated(subset=['證券代號','開始日期','截止日期'])]
            history_new=history_new.sort_values(['抽籤日期'],ascending=False)

        #還未結束的資料(discord通知)
        new=self.collect_lottery_subscription_lastest()
        new=new[~new.duplicated(subset=['證券代號','開始日期','截止日期'])]
        new=new.sort_values(['抽籤日期'],ascending=False)


        #更新過去的歷史資料
        history=pd.concat([history_new,old])
        history=history[~history.duplicated(subset=['證券代號','開始日期','截止日期'])].sort_values(['開始日期'],
                                                                                ascending=False)
        history=history.replace('',np.nan)
        
        self.cursor.execute(f'truncate table {self.table_name}')
        self.conn.commit()
        
        self.history=history

        #通知新資料
        #讀取舊有新資料，判斷哪些股票尚未通知
        latest=pd.read_sql_query(f'select * from {self.notify_table_name}',self.engine)
        latest['開始日期']=latest['開始日期'].astype('str')
        #notify
        notify_df=new[~((new.證券代號+new.開始日期).isin(latest.證券代號+latest.開始日期))]
        #新增公司業務資訊
        info=pd.read_sql_query('select * from 公司資訊',self.engine)
        notify_df=pd.merge(left=notify_df,right=info.loc[:,['證券代號','主要經營業務']],
                           on=['證券代號'],how='left')

        notify_df=notify_df.loc[:,['證券代號', '證券名稱','市場別','抽中獲利','獲利率','中籤率',
            '申購股數', '開始日期', '截止日期', '預扣費用', '抽籤日期', '還款日期', '撥券日期',
            '承銷股數(張)', '主要經營業務'],]

        self.notify_df=notify_df.reset_index(drop=True)
    
    def save(self):
        if len(self.history)>0:
            self.history.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.history)
            
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
            ['證券代號', '證券名稱','市場別','抽中獲利','獲利率','中籤率',
            '申購股數', '開始日期', '截止日期', '預扣費用', '抽籤日期', '還款日期', '撥券日期',
            '承銷股數(張)', '主要經營業務']
            
            self.notify_df=self.notify_df.where(~self.notify_df.isnull(),'')
            
            m1=(
                f"\n日期:{self.notify_df.開始日期.iloc[i]}~{self.notify_df.截止日期.iloc[i]}\n市場別:"
                f"{self.notify_df.市場別.iloc[i]}\n證券代號:{self.notify_df.證券代號.iloc[i]}"
                f"\n證券名稱:{self.notify_df.證券名稱.iloc[i]}\n抽中獲利:{str(self.notify_df.抽中獲利.iloc[i])}"
                f"\n獲利率:{str(self.notify_df.獲利率.iloc[i])}\n中籤率:{str(self.notify_df.中籤率.iloc[i])}"
                f"\n申購股數:{str(self.notify_df.申購股數.iloc[i])}\n預扣費用:"
                f"{str(self.notify_df.預扣費用.iloc[i])}\n抽籤日期:{self.notify_df.抽籤日期.iloc[i]}"
                f"\n還款日期:{self.notify_df.還款日期.iloc[i]}\n撥券日期:{self.notify_df.撥券日期.iloc[i]}"
                f"\n承銷股數(張):{str(self.notify_df['承銷股數(張)'].iloc[i])}\n主要經營業務:"
                f"{self.notify_df.主要經營業務.iloc[i]}\n\n"
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
        

class StockInitialLotteryPastRecordProcess(StockCollectLogic):
    table_name='股票申購_過去'
    app_name='stock_init_lottery'
    numeric_col=['抽中獲利','申購筆數','申購股數','預扣費用','承銷股數(張)','總承銷金額(元)']
    
    def clean_sign(self,txt,sign):
        txt=re.sub(string=txt,pattern=sign,repl='')
        if txt == '':
            return np.nan
        else:
            return int(txt)
    
    def deal_start_end_date(self,s):
        r=[]
        for i in re.finditer(string=s,pattern=r'\d{4}-\d{2}-\d{2}'):
            r.append(i.group(0))
            
        st=r[0]
        ed=r[1]
        return st,ed

    def deal_stock_code_name(self,s):
        code=s.split('-')[0]
        name=s.split('-')[1].replace(code,'')
        
        return code,name
        
    def parse_soup(self,soup):
        total_dat=[]
        for ta in soup.find_all('table',{'border':1,'width':'98%'})[1:]:
            col_name=[s.text.strip() for s in ta.find('tr').find_all('td')]
            row_data=[[(subpart.find('a').text.strip()+'-'+subpart.text.strip()) if subpart.find('a') is not None else subpart.text.strip() for subpart in part.find_all('td')] for part in ta.find_all('tr')[1:]]    

            total_dat.append(pd.DataFrame(row_data,columns=col_name))
        
        if len(total_dat) == 0:
            return pd.DataFrame()
        
        total_dat=pd.concat(total_dat)
        total_dat=total_dat.rename(columns={'承銷股數(千股)':'承銷股數(張)'})
        #string to int type
        for s in self.numeric_col:
            total_dat[s]=total_dat[s].apply(self.clean_sign,sign=',')
        
        #開始日期截止日期
        r=[self.deal_start_end_date(s) for s in total_dat['開始日期截止日期']]
        total_dat['開始日期']=[s[0] for s in r]
        total_dat['截止日期']=[s[1] for s in r]
        
        #股票代號股票名稱
        CODE=[];NAME=[]
        for s in total_dat.股票代號股票名稱:
            code,name=self.deal_stock_code_name(s)
            
            CODE.append(code)
            NAME.append(name)
        
        total_dat['證券代號']=CODE
        total_dat['證券名稱']=NAME
        
        total_dat=total_dat.loc[:,['證券代號', '證券名稱','市場別', '抽中獲利', '獲利率', '中籤率',
                                '申購筆數', '申購價格', '參考價格', '申購股數', '開始日期',
                                '截止日期','預扣費用', '預扣款日', '抽籤日期', '還款日期', 
                                '撥券日期', '承銷股數(張)', '總承銷金額(元)', '主辦券商']]
        
        return total_dat

    def collect_lottery_subscription(self,YEAR=None):
        """
        股票申購，歷史紀錄
        """
        if YEAR is None:
            a=requests.get('https://stockluckydraw.com/stock/StockInfoOld.php',
                        headers=self.headers,timeout=self.timeout)
        else:
            a=requests.get(f'https://stockluckydraw.com/stock/StockInfoOld.php?YEAR={YEAR}',
                        headers=self.headers,timeout=self.timeout)

        soup=BeautifulSoup(a.text,'lxml')
        return self.parse_soup(soup)
     
    def collect(self):
        a=requests.get('https://stockluckydraw.com/stock/StockInfoOld.php',
                    headers=self.headers,timeout=self.timeout)    
        soup=BeautifulSoup(a.text,'lxml')
        
        target_year=[s.text.strip('年').strip() for s in soup.find('h1').find_all('a') if bool(re.search(string=s.text.strip(),
                                                                                            pattern=r'年$'))] 
        D=[]
        for yy in target_year:
            D.append(self.collect_lottery_subscription(YEAR=yy))
            print(f'完成 股票申購歷史紀錄 {yy}年')
            time.sleep(3)

        D=pd.concat(D)
        D=D[~D.duplicated(subset=['證券代號','開始日期','截止日期'])]
        D=D.replace('',np.nan)
        self.result=D.sort_values(['抽籤日期'],ascending=False) 
    
    def save(self):
        if len(self.result)>0:
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)


def lottery_collect(): 
    """
    收集庫藏股資訊
    """
    main_etl=StockLotteryCollectProcess()
    main_etl.process()
    
    print('Done collection of stock lottery collect process')

def lottery_init_collect(): 
    """
    一開始初始化庫藏股歷史資訊
    """
    main_etl=StockInitialLotteryPastRecordProcess()
    main_etl.process()
    
    print('Done collection of stock init history lottery record collect process')