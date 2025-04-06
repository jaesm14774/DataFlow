import numpy as np
import re
import pandas as pd
import time
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pymysql
from stock.config.config import *
from itertools import chain
import MySQLdb

class MySQLConnection:
    db_name=DB_NAME
    
    def __init__(self,sql_configure_path):
        self.connection=pd.read_csv(sql_configure_path,index_col='name')
        """
        host:server ip of sql
        port:port sql
        user:user name of sql
        password:password of sql user
        db_name:name of database
        """
        host=self.connection.loc['host','value']
        port=int(self.connection.loc['port','value'])
        user=self.connection.loc['user','value']
        password=self.connection.loc['password','value']
        
        self.conn=pymysql.connect(host=host,
                                 port=int(port),
                                 user=user,
                                 password=password,
                                 db=self.db_name)
        
        self.cursor=self.conn.cursor()
        
        self.engine = create_engine('mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8mb4' % 
                                   (user,quote_plus(password),host,port,self.db_name))
        
    def end(self):
        self.conn.close()
        
        self.engine.dispose()


class StockCalculateLogic(MySQLConnection):
    """
    股票收集基本邏輯
    """
    now_time=datetime.datetime.now()+datetime.timedelta(hours=8) #將時間變台灣時間
    union_column_name_dict={
        '日期':'資料日期',
        '有價證券代號':'證券代號',
        '有價證券名稱':'證券名稱',
        'code':'證券代號',
        '觀察清單':'證券代號'
    }
    sql_configure_path=sql_configure_path
    _select_columns=['證券代號','資料日期'] #delete insert時，選的key pair
    _diff_table_name='股票模板_diff'
    
    def __init__(self):
        super().__init__(self.sql_configure_path)
        self.really_date_sql=f"""
        SELECT DISTINCT 資料日期
        FROM {self.db_name}.三大法人買賣
        ORDER BY 資料日期 DESC
        """

    #清除逗號
    def clean_comma(self,txt,output_not_string=False):
        try:
            txt=str(txt).strip()
        except:
            pass

        if isinstance(txt,str):
            txt=txt.replace(',','')

            if output_not_string:
                try:
                    return int(txt)
                except:
                    try:
                        return float(txt)
                    except:
                        return txt
            else:
                return txt
        else:
            return txt

    #時間處理函數
    def make_regular_date_format(self,dt):
        if isinstance(dt,datetime.datetime):
            dt=str(dt)

        #判別各情況
        if len(dt) == 9: #民國轉西元
            dt=f'{int(dt[:3])+1911}{dt[3:]}'
        elif len(dt) == 10: #西元年
            dt=dt
        else:
            assert f'沒看過的日期格式:{dt}'

        dt=dt.replace('/','-')
        return dt

    #清理已發行普通股數
    def clean_company_info(self,s):
        if '股' in s:
            s=re.sub(string=s,pattern=r'股|\s+',repl='')
        return int(re.sub(string=s,pattern=',',repl=''))

    def decimal_format(self,v,n=3):
        return np.round(v,decimals=n)*100

    def load(self):
        """
        讀取必要資料
        """
        #各股票基本資料
        company_info=pd.read_sql_query('select * from 公司資訊',self.engine)
        company_info=company_info.loc[:,['證券代號','公司名稱','已發行普通股數','主要經營業務','公司地址']]
        company_info=company_info[~company_info['已發行普通股數'].isnull()]
        company_info['已發行普通股數']=company_info['已發行普通股數'].map(self.clean_company_info)
        company_info['證券代號']=[s.strip() for s in company_info.證券代號]
        self.company_info=company_info
        all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
        all_code_info['證券代號']=all_code_info['證券代號'].map(str.strip)
        all_code_info['證券名稱']=all_code_info['證券名稱'].map(str.strip)
        self.all_code_info=all_code_info
        self.stock_map_dict={code.strip():name.strip() for code,name in zip(all_code_info['證券代號'],
                                                                            all_code_info['證券名稱'])}

        #實收資本額
        self.aggregate_info=pd.merge(left=company_info,right=all_code_info,on=['證券代號'],
                                     how='left').loc[:,['證券代號','證券名稱','已發行普通股數',
                                                        '產業別','主要經營業務','市場別']]    
    def calculate(self):
        """
        計算程式
        """
        pass
    
    def save(self):
        """
        儲存資料進資料庫(或其他)
        """
        pass
    
    def process(self):
        """
        主要的資料流程
        """
        self.load()
        self.calculate()
        self.save()
        self.end()


class StockCalculateOuterAndInvestmentRationProcess(StockCalculateLogic):
    """
    計算外本比、投本比
    """
    negative=np.append(np.array([-100]),np.arange(-3,-0.24,0.25))
    negative_last=np.array([-0.25,-0.05])
    zero=np.array([-0.05,0.05])
    positive_first=np.array([0.05,0.25])
    positive=np.append(np.arange(0.25,3.01,0.25),100)
    bin=np.append(np.append(np.append(np.append(negative,negative_last),zero),positive_first),positive)
    bin=np.unique(bin)
    score_label=[-25,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1,0,1,2,3,4,5,6,7,8,9,10,11,12,25]
    
    def __init__(self,start_date=None,end_date=None):
        super().__init__()
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]
        if end_date and start_date:
            self.end_date=end_date
            self.start_date=start_date
        else:
            #default value
            self.start_date=self.really_date.strftime('%Y-%m-%d')
            self.end_date=self.really_date.strftime('%Y-%m-%d')

    def load(self):
        super().load()
        
        query_sql=(
            f"select * from 三大法人買賣 where 資料日期 >= '{self.start_date}' and 資料日期 <= '{self.end_date}'"
        )
        three_mainforce_data=pd.read_sql_query(query_sql,
                            self.engine)
        three_mainforce_data['證券名稱']=three_mainforce_data['證券名稱'].map(str.strip)
        self.three_mainforce_data=three_mainforce_data
        
        self.really_end_date=self.three_mainforce_data['資料日期'].iloc[-1]
        self.really_start_date=self.three_mainforce_data['資料日期'].iloc[0]
        
        #殖利率、本益比表格(取最後一天做合併，因為這樣才可以更好的找出明天的標的
        today=self.really_end_date.strftime('%Y-%m-%d')
        metric_info=pd.read_sql_query(f"select * from 個股基本指標 where 資料日期 = '{today}'",self.engine)
        metric_info=metric_info.drop(['股利年度'],axis=1)
        self.metric_info=metric_info
        
        deal_info=pd.read_sql_query(f"select * from 個股成交資訊 where 資料日期 = '{today}'",self.engine)
        self.deal_info=deal_info

    def calculate(self):
        self.three_mainforce_data=self.three_mainforce_data.loc[:,['資料日期','證券代號','證券名稱','外資買進股數',
                                                                   '外資賣出股數','外資買賣超股數', '投信買進股數',
                                                                   '投信賣出股數', '投信買賣超股數','自營商買賣超股數',
                                                                   '自營商買進股數', '自營商賣出股數','三大法人買賣超股數'
                                                                   ]]
        
        numeric_col=[col for col in self.three_mainforce_data.columns if col not in ['證券代號','證券名稱','資料日期']]
        self.three_mainforce_data=self.three_mainforce_data[self.three_mainforce_data['證券代號'].isin(self.all_code_info['證券代號'].tolist())]
        
        for col in numeric_col:
            self.three_mainforce_data=self.three_mainforce_data[~self.three_mainforce_data[col].isnull()]
        
        #清理空格
        self.three_mainforce_data['證券代號']=[s.strip() for s in self.three_mainforce_data.證券代號]
        self.three_mainforce_data['證券名稱']=[s.strip() for s in self.three_mainforce_data.證券名稱]
        
        D=pd.merge(left=self.three_mainforce_data,right=self.aggregate_info,on=['證券代號','證券名稱'],how='left')    
        
        D['外本比_%']=(D['外資買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D['投本比_%']=(D['投信買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D['三大本比_%']=(D['三大法人買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D=D.drop(['外資買賣超股數','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數','已發行普通股數'],axis=1)
        D=pd.merge(left=D,right=self.metric_info,on=['資料日期','證券代號','證券名稱'],how='left')
        D=pd.merge(left=D,right=self.deal_info,on=['資料日期','證券代號'],how='left')
        
        temp=D.groupby(['證券代號','證券名稱'])
        
        Total_View=pd.DataFrame(columns=['資料日期', '證券代號', '證券名稱', '產業別', '主要經營業務', '市場別', '外本比_%', '投本比_%',
            '三大本比_%','外資買超天數','投信買超天數','外資賣超天數','投信賣超天數','殖利率(%)', '本益比', '股價淨值比','收盤','成交量'])
        
        for i,j in temp.groups:
            info=dict()
            tt=temp.get_group((i,j))
            info['外本比_%']=tt['外本比_%'].sum()
            info['投本比_%']=tt['投本比_%'].sum()
            info['三大本比_%']=tt['三大本比_%'].sum()
            
            if tt['本益比'].isnull().iloc[-1]:
                continue
            
            info['外資買超天數']=(tt['外本比_%']>0).sum()
            info['投信買超天數']=(tt['投本比_%']>0).sum()
            info['外資賣超天數']=(tt['外本比_%']<0).sum()
            info['投信賣超天數']=(tt['投本比_%']<0).sum()
            u=tt['資料日期'].astype('str').values
            info['資料日期']=u[0]+'~'+u[-1]
            info['證券代號']=i
            info['證券名稱']=j
            info['產業別']=tt['產業別'].iloc[0]
            info['主要經營業務']=tt['主要經營業務'].iloc[0]
            info['市場別']=tt['市場別'].iloc[0]
            info['殖利率(%)']=tt['殖利率(%)'].iloc[-1]
            info['本益比']=tt['本益比'].iloc[-1]
            info['股價淨值比']=tt['股價淨值比'].iloc[-1]
            info['收盤']=tt['收盤'].iloc[-1]
            info['成交量']=tt['成交量'].iloc[-1]
            info=pd.DataFrame(info,index=[1])
            
            Total_View=pd.concat([Total_View,info],ignore_index=True)
            
        Total_View=Total_View.loc[:,['資料日期', '證券代號', '證券名稱', '產業別', '主要經營業務',
                                    '市場別','外本比_%','投本比_%','三大本比_%','外資買超天數',
                                    '投信買超天數','外資賣超天數','投信賣超天數',
                                    '殖利率(%)', '本益比', '股價淨值比','收盤','成交量']]
        
        Total_View['外資買超天數']=Total_View['外資買超天數'].astype('float')
        Total_View['投信買超天數']=Total_View['投信買超天數'].astype('float')
        Total_View['外資賣超天數']=Total_View['外資賣超天數'].astype('float')
        Total_View['投信賣超天數']=Total_View['投信賣超天數'].astype('float')
        
        #更新整體外本比與投本比得分表格
        temp=pd.cut(x=Total_View['外本比_%'].values,bins=self.bin,
                    labels=self.score_label)
        temp[temp.isnull()]=0
        temp=temp.astype('int')

        Total_View['外本比分數']=temp
        Total_View['資料日期']=self.really_end_date
        
        temp=pd.cut(x=Total_View['投本比_%'].values,bins=self.bin,
                    labels=self.score_label)
        temp[temp.isnull()]=0
        temp=temp.astype('int')
        Total_View['投本比分數']=temp
        
        self.result=Total_View
    
    def save(self):
        if len(self.result)>0:
            #step1 truncate delete temp table
            self.cursor.execute('TRUNCATE TABLE 外本比_diff')
            self.cursor.execute('TRUNCATE TABLE 投本比_diff')
            self.conn.commit()

            self.result.loc[:,self._select_columns].to_sql('外本比_diff',self.engine,if_exists='append',index=0)
            self.result.loc[:,self._select_columns].to_sql('投本比_diff',self.engine,if_exists='append',index=0)

            #step1 delete already exist data
            delete_sql1=f"""
            delete tb from 外本比 as tb
            where exists (
            select *
            from 外本比_diff tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期
            )
            """
            delete_sql2=f"""
            delete tb from 投本比 as tb
            where exists (
            select *
            from 投本比_diff tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期
            )
            """ 
            
            self.cursor.execute(delete_sql1)
            self.cursor.execute(delete_sql2)
            self.conn.commit()
            
            #step2 insert data
            self.result.loc[:,['證券代號','證券名稱','外本比分數','資料日期']].to_sql('外本比',
                                                                     self.engine,if_exists='append',index=0)
            self.result.loc[:,['證券代號','證券名稱','投本比分數','資料日期']].to_sql('投本比',
                                                                     self.engine,if_exists='append',index=0)


class StockCalculateOuterAndInvestmentRecommendationProcess(StockCalculateLogic):
    """
    外投本比回測計算
    """
    table_name = '法人比推薦'
    _diff_table_name = '法人比推薦_diff'
    _select_columns = ['證券代號','資料日期','推薦方式']
    
    def __init__(self):
        super().__init__()
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]

    def load_and_view(self,start_date,end_date):
        query_sql=(
            f"select * from 三大法人買賣 where 資料日期 >= '{start_date}' and 資料日期 <= '{end_date}'"
            f"order by 資料日期"
        )
        three_mainforce_data=pd.read_sql_query(query_sql,self.engine)
        if len(three_mainforce_data) == 0:
            return ['','']
        
        three_mainforce_data=three_mainforce_data.loc[:,['資料日期','證券代號','證券名稱','外資買賣超股數','投信買賣超股數',
                                                        '自營商買賣超股數','三大法人買賣超股數']]
        three_mainforce_data=three_mainforce_data[three_mainforce_data['證券代號'].isin(self.all_code_info['證券代號'])]
        three_mainforce_data['證券名稱']=three_mainforce_data['證券名稱'].map(str.strip)
        three_mainforce_data=three_mainforce_data.reset_index(drop=True)
        
        really_end_date=three_mainforce_data['資料日期'].iloc[-1]
        really_start_date=three_mainforce_data['資料日期'].iloc[0]
        
        D=pd.merge(left=three_mainforce_data,right=self.aggregate_info,on=['證券代號','證券名稱'],how='left')    
        D['外本比_%']=(D['外資買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D['投本比_%']=(D['投信買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D['三大本比_%']=(D['三大法人買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D=D[~D.市場別.isnull()].reset_index(drop=True)
        
        select_column=['證券代號', '證券名稱', '產業別', '市場別', '外本比_%', '投本比_%', '三大本比_%']
        #外本比排序
        r1=D.sort_values(['外本比_%','投本比_%','三大本比_%'],ascending=False).reset_index().loc[:20,select_column]
        #投本比排序
        r2=D.sort_values(['投本比_%','外本比_%','三大本比_%'],ascending=False).reset_index().loc[:20,select_column]
        r1['資料日期']=really_end_date
        r2['資料日期']=really_end_date
        
        return [r1,r2]
    
    def calculate(self):
        #已記錄 外本比
        dealt_date=pd.read_sql_query('select distinct 資料日期 from 法人比推薦 order by 資料日期',
                                     self.engine)['資料日期'].astype('str').tolist()
        if len(dealt_date)>1:
            dealt_date=dealt_date[:(len(dealt_date)-1)]
            
        #外資
        out=[]
        #投信
        investment=[]
        
        List=pd.read_sql_query(self.really_date_sql,self.engine)['資料日期'].astype('str').tolist()
        List=[t for t in List if t not in dealt_date]
        if len(List) == 0:
            self.out=[]
            self.investment=[]
            return
        
        for i in range(len(List)):
            tt=List[i]
            
            o,inv=self.load_and_view(tt,tt)
            out.append(o)
            investment.append(inv)
            
            print(f'外投本比回測-{tt} is done and remaining {len(List)}')
        
        self.out=[s for s in out if isinstance(s,pd.DataFrame)]
        self.investment=[s for s in investment if isinstance(s,pd.DataFrame)]
        
    def save(self):
        #step1 truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()
        
        if len(self.out)>0:
            self.out=pd.concat(self.out,axis=0)
            self.out=self.out.reset_index(drop=True)
            self.out=self.out.replace(np.inf,np.nan)
            self.out=self.out.replace(-np.inf,np.nan)
            self.out=self.out[~self.out['外本比_%'].isnull()]
            self.out['推薦方式']='外本比推薦'
            self.out.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                        if_exists='append',index=0)

        if len(self.investment)>0:
            self.investment=pd.concat(self.investment,axis=0)
            self.investment=self.investment.reset_index(drop=True)
            self.investment=self.investment.replace(np.inf,np.nan)
            self.investment=self.investment.replace(-np.inf,np.nan)
            self.investment=self.investment[~self.investment['投本比_%'].isnull()]
            self.investment['推薦方式']='投本比推薦'
            self.investment.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                           if_exists='append',index=0)
            
        #step1 delete already exist data
        delete_sql=f"""
        delete tb from {self.table_name} as tb
        where exists (
        select *
        from {self._diff_table_name} tb2
        where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
        )
        """
        self.cursor.execute(delete_sql)
        self.conn.commit()
        #step2 insert data
        if len(self.out)>0:
            self.out.to_sql(self.table_name,self.engine,index=0,if_exists='append')
        if len(self.investment)>0:
            self.investment.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockCalculateBrokerRecommendationProcess(StockCalculateLogic):
    """
    地緣券商回測更新
    """
    table_name='地緣券商推薦'
    _diff_table_name='地緣券商推薦_diff'
    _select_columns = ['證券代號','資料日期','券商名稱','推薦方式','買賣']
    buy_sell_ratio_threshold=0.003
    amount_threshold=100
    
    def find_near_broker(self,b):
        """
        篩選每家公司對應的10公里內券商，如果沒有任一家10公里的，則找最近兩家券商作代表
        """
        temp=b[b.距離<10]
        if temp.shape[0]>0:
            return temp.drop(['證券代號'],axis=1)
        else:
            if b.距離.min()<25:
                return b[b.距離 < (b.距離.min()+1)].drop(['證券代號'],axis=1)
            else:
                return None
    
    def load(self):
        super().load()
        
        #公司與券商距離
        geo_broker=pd.read_sql_query('select * from 地緣券商資訊',self.engine).drop(['id','create_time'],axis=1)
        geo_broker=geo_broker.groupby(['證券代號']).apply(self.find_near_broker).reset_index().drop(['level_1'],axis=1)

        self.company_info['已發行普通股數']=(self.company_info['已發行普通股數']/1000).round(0)
        geo_broker_info=pd.merge(left=geo_broker,right=self.company_info,on=['證券代號','公司名稱'],how='left')
        self.geo_broker_info=geo_broker_info
        #扣除台北市、新北市，因為券商太過密集，無法判斷
        except_pat='台北市|臺北市|新北市|台北縣|臺北縣|新北巿|台北巿|台北巿|北市'
        keep_code=[i for i,s in zip(geo_broker_info.證券代號,geo_broker_info.公司地址) if not bool(re.search(string=s,
                                                                                                pattern=except_pat))]
        self.keep_code=keep_code
        #扣除外資券商
        foreign_broker=['港商麥格理','東方匯理','美林','台灣摩根士丹利','美商高盛','瑞士信貸','港商德意志',
            '港商野村','港商法國興業','花旗環球','新加坡商瑞銀','摩根大通','大和國泰','法銀巴黎','台灣巴克萊',
            '香港上海匯豐']
        self.foreign_broker=foreign_broker
        
    def calculate_geo_broker(self,geo_broker_df,code,threshold):
        """
        計算是否為地緣券商
        """
        buy_r=geo_broker_df[np.logical_and(geo_broker_df.資料類型 == '分點',geo_broker_df.證券代號 == code)]. \
            sort_values(['買賣超'],ascending=False).iloc[0:5,:]
        sell_r=geo_broker_df[np.logical_and(geo_broker_df.資料類型 == '分點',geo_broker_df.證券代號 == code)]. \
            sort_values(['買賣超'],ascending=False).iloc[-5:,:]
        
        if buy_r.shape[0] == 0:
            return          
        if ((buy_r.買賣超<0).sum()>0) or ((sell_r.買賣超>0).sum()<0):
            return
        
        try:#當天未交易或倒閉股票
            already_publish=self.geo_broker_info[self.geo_broker_info.證券代號 == code].已發行普通股數.iloc[0]
        except:
            return
        
        if (buy_r.買賣超.iloc[0]/already_publish) <  self.buy_sell_ratio_threshold:
            return
        
        temp_broker_name=self.geo_broker_info[self.geo_broker_info.證券代號 == code].券商名稱.values
        
        rs1=[broker_name for amount,broker_name in zip(buy_r.買賣超,buy_r.券商名稱) \
            if broker_name in temp_broker_name and broker_name not in self.foreign_broker and \
                amount>self.amount_threshold]
        rs2=[broker_name for amount,broker_name in zip(sell_r.買賣超,sell_r.券商名稱) \
            if broker_name in temp_broker_name and broker_name not in self.foreign_broker and \
                abs(amount)>self.amount_threshold]
        
        if len(rs1)>0:
            for broker_name in rs1:
                if self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                       self.geo_broker_info['證券代號'] == code)].距離.iloc[0]<threshold:
                    print(code,broker_name,self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                     self.geo_broker_info['證券代號'] == code)].距離.iloc[0],' 買')
                                       
                    self.recommend.append(pd.DataFrame({
                        '資料日期':buy_r.資料日期.iloc[0],
                        '證券代號':code,
                        '券商名稱':broker_name,
                        '距離':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].距離.iloc[0],
                        '買量':buy_r[buy_r.券商名稱 == broker_name].買賣超.iloc[0],
                        '地址':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].公司地址.iloc[0],
                        '推薦方式':'當日',
                        '買賣':'買'
                    },index=[1]))
                else:
                    continue
        if len(rs2)>0:
            for broker_name in rs2:
                if self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                       self.geo_broker_info['證券代號'] == code)].距離.iloc[0]<threshold:
                    print(code,broker_name,self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                               self.geo_broker_info['證券代號'] == code)].距離.iloc[0],' 賣')
                    self.recommend.append(pd.DataFrame({
                        '資料日期':sell_r.資料日期.iloc[0],
                        '證券代號':code,
                        '券商名稱':broker_name,
                        '距離':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].距離.iloc[0],
                        '賣量':sell_r[sell_r.券商名稱 == broker_name].買賣超.iloc[0],
                        '地址':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].公司地址.iloc[0],
                        '推薦方式':'當日',
                        '買賣':'賣'
                    },index=[1]))
                else:
                    continue 
    
    def load_and_view(self,today):
        """
        計算當日地緣券商名單
        """
        d=pd.read_sql_query(f"select * from 券商進出總表 where 資料日期 = '{today}'",self.engine)
        d['資料日期']=d['資料日期'].astype('str')
        self.recommend=[]

        for cc in d.證券代號.unique():
            if cc in self.keep_code: #不在新北、台北市
                self.calculate_geo_broker(geo_broker_df=d,code=cc,threshold=9999999)
            else:#在台北或新北市
                self.calculate_geo_broker(geo_broker_df=d,code=cc,threshold=2)
        
        if len(self.recommend) >0:
            return pd.concat(self.recommend,axis=0)
        else:
            return pd.DataFrame()
        
    def calculate(self):
        dealt_date=pd.read_sql_query(f'select distinct 資料日期 from {self.table_name}',self.engine)['資料日期'].astype('str').tolist()
        if len(dealt_date)>1:
            dealt_date=dealt_date[:(len(dealt_date)-1)]

        List=pd.read_sql_query(self.really_date_sql,self.engine)['資料日期'].astype('str').tolist()
        List=[t for t in List if t not in dealt_date]
        
        #地緣推薦回測
        RS=[]

        for tt in List:
            RS.append(self.load_and_view(today=tt))
        
        RS=[part for part in RS if len(part)>0]

        if len(RS)>0:
            RS=pd.concat(RS,axis=0)
            if '買量' not in RS.columns:
                RS['買量']=np.nan

            if '賣量' not in RS.columns:
                RS['賣量']=np.nan
            
            RS=RS.loc[:,['資料日期', '證券代號', '券商名稱','地址','距離','買量','賣量','推薦方式', '買賣']]
            self.result=RS
        else:
            self.result=[]
    
    def save(self):
        if len(self.result)>0:
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.券商名稱=tb2.券商名稱 and
                tb.推薦方式=tb2.推薦方式 and tb.買賣=tb2.買賣
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')       


class StockCalculateBrokerTenDaysRecommendationProcess(StockCalculateLogic):
    """
    地緣券商十日回測更新
    """
    table_name='地緣券商推薦'
    _diff_table_name='地緣券商推薦_diff'
    _select_columns = ['證券代號','資料日期','券商名稱','推薦方式','買賣']
    N=10
    buy_sell_ratio_threshold=0.01
    amount_threshold=300
    
    def find_near_broker(self,b):
        """
        篩選每家公司對應的10公里內券商，如果沒有任一家10公里的，則找最近兩家券商作代表
        """
        temp=b[b.距離<10]
        if temp.shape[0]>0:
            return temp.drop(['證券代號'],axis=1)
        else:
            if b.距離.min()<25:
                return b[b.距離 < (b.距離.min()+1)].drop(['證券代號'],axis=1)
            else:
                return None
    
    def load(self):
        super().load()
        
        #公司與券商距離
        geo_broker=pd.read_sql_query('select * from 地緣券商資訊',self.engine).drop(['id','create_time'],axis=1)
        geo_broker=geo_broker.groupby(['證券代號']).apply(self.find_near_broker).reset_index().drop(['level_1'],axis=1)

        self.company_info['已發行普通股數']=(self.company_info['已發行普通股數']/1000).round(0)
        geo_broker_info=pd.merge(left=geo_broker,right=self.company_info,on=['證券代號','公司名稱'],how='left')
        self.geo_broker_info=geo_broker_info
        #扣除台北市、新北市，因為券商太過密集，無法判斷
        except_pat='台北市|臺北市|新北市|台北縣|臺北縣|新北巿|台北巿|台北巿|北市'
        keep_code=[i for i,s in zip(geo_broker_info.證券代號,geo_broker_info.公司地址) if not bool(re.search(string=s,
                                                                                                pattern=except_pat))]
        self.keep_code=keep_code
        #也扣除外資券商
        self.foreign_broker=['港商麥格理','東方匯理','美林','台灣摩根士丹利','美商高盛','瑞士信貸','港商德意志',
            '港商野村','港商法國興業','花旗環球','新加坡商瑞銀','摩根大通','大和國泰','法銀巴黎','台灣巴克萊',
            '香港上海匯豐']
    
    def combine_N_days_into_one(self,geo_broker_df):
        """
        多天合併一天 地緣多日
        """
        geo_broker_df=geo_broker_df.sort_values(['資料日期'])
        tt=geo_broker_df.資料日期.iloc[0]+'~'+geo_broker_df.資料日期.iloc[-1]
        
        buy_sum=geo_broker_df['買量'].sum()
        if buy_sum != 0:
            buy_price=np.round((geo_broker_df['買量']*geo_broker_df['買價']).sum()/buy_sum,2)
        else:
            buy_price=0
        
        sell_sum=geo_broker_df['賣量'].sum()
        if sell_sum != 0:
            sell_price=np.round((geo_broker_df['賣量']*geo_broker_df['賣價']).sum()/sell_sum,2)
        else:
            sell_price=0
        
        total_sum=geo_broker_df['買量'].sum()-geo_broker_df['賣量'].sum()
        if total_sum != 0 :
            total_price=(geo_broker_df['買量']*geo_broker_df['買價']).sum()-(geo_broker_df['賣量']*geo_broker_df['賣價']).sum()
            total_price=np.round(total_price/total_sum,2)
        else:
            total_price=0
            
        return pd.DataFrame({
            '資料區間':tt,
            '證券代號':geo_broker_df['證券代號'].iloc[0],
            '券商名稱':geo_broker_df['券商名稱'].iloc[0],
            '買量':buy_sum,
            '買價':buy_price,
            '賣量':sell_sum,
            '賣價':sell_price,
            '買賣超':total_sum,
            '均價':total_price,
            '資料類型':'分點'
        },index=[0])
        
    def calculate_geo_broker(self,geo_broker_df,code,threshold):
        """
        計算地緣10日累積推薦
        """
        buy_r=geo_broker_df[np.logical_and(geo_broker_df.資料類型 == '分點',geo_broker_df.證券代號 == code)]. \
            sort_values(['買賣超'],ascending=False).iloc[0:5,:]
        sell_r=geo_broker_df[np.logical_and(geo_broker_df.資料類型 == '分點',geo_broker_df.證券代號 == code)]. \
            sort_values(['買賣超'],ascending=False).iloc[-5:,:]
        
        if buy_r.shape[0] == 0:
            return          
        if ((buy_r.買賣超<0).sum()>0) or ((sell_r.買賣超>0).sum()<0):
            return
        
        try:#當天未交易或倒閉股票
            already_publish=self.geo_broker_info[self.geo_broker_info.證券代號 == code].已發行普通股數.iloc[0]
        except:
            return
        
        if (buy_r.買賣超.iloc[0]/already_publish) <  self.buy_sell_ratio_threshold:
            return
        
        temp_broker_name=self.geo_broker_info[self.geo_broker_info.證券代號 == code].券商名稱.values
        
        rs1=[broker_name for amount,broker_name in zip(buy_r.買賣超,buy_r.券商名稱) \
            if broker_name in temp_broker_name and broker_name not in self.foreign_broker and \
                amount>self.amount_threshold]
        rs2=[broker_name for amount,broker_name in zip(sell_r.買賣超,sell_r.券商名稱) \
            if broker_name in temp_broker_name and broker_name not in self.foreign_broker and \
                abs(amount)>self.amount_threshold]
        
        if len(rs1)>0:
            for broker_name in rs1:
                if self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                       self.geo_broker_info['證券代號'] == code)].距離.iloc[0]<threshold:
                    print(code,broker_name,self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                self.geo_broker_info['證券代號'] == code)].距離.iloc[0],
                          ' 買')
                    self.recommend.append(pd.DataFrame({
                        '資料日期':buy_r.資料日期.iloc[0],
                        '證券代號':code,
                        '券商名稱':broker_name,
                        '距離':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].距離.iloc[0],
                        '買量':buy_r[buy_r.券商名稱 == broker_name].買賣超.iloc[0],
                        '地址':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].公司地址.iloc[0],
                        '推薦方式':f'{self.N}日',
                        '買賣':'買'
                    },index=[1]))
                else:
                    continue
        if len(rs2)>0:
            for broker_name in rs2:
                if self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                       self.geo_broker_info['證券代號'] == code)].距離.iloc[0]<threshold:
                    print(code,broker_name,self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                               self.geo_broker_info['證券代號'] == code)].距離.iloc[0],
                          ' 賣')
                    self.recommend.append(pd.DataFrame({
                        '資料日期':sell_r.資料日期.iloc[0],
                        '證券代號':code,
                        '券商名稱':broker_name,
                        '距離':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].距離.iloc[0],
                        '賣量':sell_r[sell_r.券商名稱 == broker_name].買賣超.iloc[0],
                        '地址':self.geo_broker_info[np.logical_and(self.geo_broker_info['券商名稱']==broker_name,
                                                                 self.geo_broker_info['證券代號'] == code)].公司地址.iloc[0],
                        '推薦方式':f'{self.N}日',
                        '買賣':'賣'
                    },index=[1]))
                else:
                    continue 
    
    def another_calculate_and_save(self):
        """
        整合N天數據成唯一檔案，且N天都有買賣的股票，儲存地緣10
        """
        if len(self.List_Interval) == 0:
            return
        d=pd.read_sql_query(f"select * from 券商進出總表 where 資料日期 >= '{self.List_Interval[-1]}'"
                             f"and 資料日期 <= '{self.List_Interval[0]}'",
                            self.engine)
        d['資料日期']=d['資料日期'].astype('str')

        temp=d[~d.duplicated(subset=['證券代號','資料日期'])].copy()
        temp=temp.groupby(['證券代號'])['資料日期'].count().reset_index(drop=False)
        choose=temp[temp['資料日期'] == self.N]['證券代號'].tolist()
        d=d[d.證券代號.isin(choose)].reset_index(drop=True)
        d.drop(['券商代號'],axis=1,inplace=True)
        
        st=time.time()
        rs=d.groupby(['證券代號','券商名稱']).apply(self.combine_N_days_into_one).reset_index(drop=True)
        rs['資料日期']=self.List_Interval[0]
        print(f'地緣券商多日計算花費時間  : {time.time()-st} seconds')
        rs.to_sql('券商進出_十日',self.engine,if_exists='append',index=0)
        
    def load_and_view(self,today):
        """
        計算十日地緣券商名單
        """
        d=pd.read_sql_query(f"select * from 券商進出_十日 where 資料日期 = '{today}'",self.engine)
        d['資料日期']=d['資料日期'].astype('str')
        self.recommend=[]

        for cc in d.證券代號.unique():
            if cc in self.keep_code: #不在新北、台北市
                self.calculate_geo_broker(geo_broker_df=d,code=cc,threshold=9999999)
            else:#在台北或新北市
                self.calculate_geo_broker(geo_broker_df=d,code=cc,threshold=2)
        
        if len(self.recommend) >0:
            return pd.concat(self.recommend,axis=0)
        else:
            return pd.DataFrame()
    
    def calculate(self):
        """
        計算未處理的地緣10日累積推薦
        """
        dealt_date=pd.read_sql_query(f'select distinct 資料日期 from 地緣券商推薦',self.engine)['資料日期'].astype('str').tolist()
        
        List=pd.read_sql_query(self.really_date_sql,self.engine)['資料日期'].astype('str').tolist()
        List=[t for t in List if t not in dealt_date]
        
        self.List_Interval=List[:self.N]
        
        print('List:',List)
        #地緣10推薦回測
        RS=[]

        for tt in List:
            RS.append(self.load_and_view(today=tt))
        
        RS=[tmp for tmp in RS if len(tmp)>0]
        
        if len(RS)>0:
            RS=pd.concat(RS,axis=0)
            if '買量' not in RS.columns:
                RS['買量']=np.nan

            if '賣量' not in RS.columns:
                RS['賣量']=np.nan
            RS=RS.loc[:,['資料日期', '證券代號', '券商名稱','地址','距離','買量','賣量','推薦方式', '買賣']]
            self.result=RS
        else:
            self.result=[]
    
    def save(self):
        if len(self.result)>0:
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.券商名稱=tb2.券商名稱 and
                tb.推薦方式=tb2.推薦方式 and tb.買賣=tb2.買賣
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')       
    
    def process(self):
        self.load()
        self.calculate()
        self.save()
        self.another_calculate_and_save() #儲存最新數據進 券商進出_十日
        self.end()      


class StockCalculateMainForceBuySellProcess(StockCalculateLogic):
    """
    計算 主力買賣超
    """
    table_name = '主力買賣超'
    _diff_table_name = '股票模板_diff'
    
    def __init__(self):
        super().__init__()
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]

    def load(self):
        super().load()
        mainforce_old_df=pd.read_sql_query(f'select * from {self.table_name}',self.engine)
        mainforce_old_df['資料日期']=mainforce_old_df['資料日期'].astype('str')

        self.already_exist_mainforce_date=sorted(mainforce_old_df['資料日期'].unique().tolist())
        if len(self.already_exist_mainforce_date)>1:
            self.already_exist_mainforce_date=self.already_exist_mainforce_date[:len(self.already_exist_mainforce_date)-1]
        
        self.broker_inout_date=pd.read_sql_query('select distinct 資料日期 from 券商進出總表',self.engine)['資料日期'].astype('str').tolist()
        
    def calculate_nth_broker_inout(self,df,n=20):
        """
        計算前20大買賣券商
        """
        df=df.sort_values(['買賣超'])
        main_sell=df.買賣超.iloc[-n:].sum()+df.買賣超.iloc[:n].sum()
        return main_sell
    
    def calculate(self):
        List=list(set(self.broker_inout_date) - set(self.already_exist_mainforce_date))
        
        if len(List) == 0:
            List=[self.already_exist_mainforce_date[-1]]
        
        rs=[]
        for tt in List:
            broker_inout_df=pd.read_sql_query(f"select * from 券商進出總表 where 資料日期='{tt}' and 資料類型='分點'",self.engine)
            broker_inout_df=broker_inout_df.groupby(['證券代號']).apply(self.calculate_nth_broker_inout).reset_index(drop=False)
            #讓index為證券代號，到時候統一組合
            broker_inout_df.index=broker_inout_df.證券代號.tolist()
            broker_inout_df.drop(['證券代號'],axis=1,inplace=True)
            
            rs.append(broker_inout_df)
        
        if len(rs)>0:
            rs=pd.concat(rs,axis=1,ignore_index=False).reset_index(drop=False)
            rs=rs.rename(columns={'index':'證券代號'})
            rs['證券代號']=rs['證券代號'].astype('str')
            if rs.shape[1] <=2:
                rs.columns=['證券代號']+List
            else:
                rs.columns=['證券代號']+List
            
            self.result=pd.melt(rs,id_vars=['證券代號']).rename(columns={'variable':'資料日期','value':'主力買賣超'})
        else:
            self.result=[]
        
    def save(self):
        #step1 truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()

        if len(self.result)>0:
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                           if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMakeOuterAndInvestmentRecommendationListProcess(StockCalculateLogic):
    """
    外投本比推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff2'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    def load(self):
        super().load()
        temp=pd.read_sql_query("select * from 法人比推薦 order by 資料日期",self.engine)
        self.outer_df=temp[temp.推薦方式 == '外本比推薦']
        self.investment_df=temp[temp.推薦方式 == '投本比推薦']  
    
    def combine_code(self,df):
        return ';'.join(df.證券代號.tolist())
    
    def make_table(self,df,today,n=10):
        df=df.copy()
        df=df.groupby(['資料日期']).apply(self.combine_code).reset_index(drop=False)
        df.資料日期=df.資料日期.astype('datetime64[ns]')
        
        before_today_df=df[df.資料日期<=today]
        if before_today_df.shape[0] <(n+1):
            return ' '
        else:
            appear=[]
            today_code=before_today_df[0].iloc[-1].split(';')
            for i in range(2,n+2):
                appear.append(before_today_df[0].iloc[-i].split(';'))
            
            appear=list(chain.from_iterable(appear))
            
            rs=[]
            for code in today_code:
                if code not in appear:
                    rs.append(code)
                else:
                    continue
            
            if len(rs)>0:
                print('新鮮麵包出爐囉~~~')
                if format(datetime.datetime.now(),'%Y-%m-%d') != before_today_df.資料日期.astype('str').iloc[-1]:
                    print('\n\n\n','強烈注意，這不是今天的結果')
                
                return pd.DataFrame({
                    '資料日期':before_today_df.資料日期.iloc[-1],
                    '證券代號':rs
                })
    
    def calculate(self):
        self.result1=self.make_table(df=self.outer_df,today=format(datetime.datetime.now(),'%Y-%m-%d'),n=1)
        self.result2=self.make_table(df=self.investment_df,today=format(datetime.datetime.now(),'%Y-%m-%d'),n=1)
    
    def save(self):
        if self.result1 is not None:
            self.result1['證券名稱']=self.result1['證券代號'].map(self.stock_map_dict)
            self.result1['推薦方式']='外本比'
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result1.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                              if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result1.to_sql(self.table_name,self.engine,index=0,if_exists='append')
           
        if self.result2 is not None:
            self.result2['證券名稱']=self.result2['證券代號'].map(self.stock_map_dict)
            self.result2['推薦方式']='投本比'
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result2.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result2.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMakeBrokerInOutRecommendationListProcess(StockCalculateLogic):
    """
    地緣券商當日推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    def exclude_broker_function(self,df):
        """
        #刪除買賣相抵的地緣券商
        """
        df=df.reset_index(drop=True)
        df.loc[df.買量.isnull(),'買量']=0
        df.loc[df.賣量.isnull(),'賣量']=0
        if df.推薦方式.iloc[0] == '10日':
            if df.買量.sum()+df.賣量.sum()<300:
                return None
            else:
                index_pos=df[df['買量'] == df['買量'].max()].index[0]
                df.loc[index_pos,'買量']=df['買量'].sum()+df['賣量'].sum()
                df=df.loc[[index_pos],:]
                return df.loc[:,['券商名稱', '地址', '買量', '距離','推薦方式']]        
        else:
            if df.買量.sum()+df.賣量.sum()<100:
                return None
            else:
                index_pos=df[df['買量'] == df['買量'].max()].index[0]
                df.loc[index_pos,'買量']=df['買量'].sum()+df['賣量'].sum()
                df=df.loc[[index_pos],:]
                return df.loc[:,['券商名稱', '地址', '買量', '距離','推薦方式']]        
    
    def load(self):
        super().load()
        temp=pd.read_sql_query("select * from 地緣券商推薦 order by 資料日期",self.engine)
        self.broker_today_df=temp[temp.推薦方式 == '當日']
        self.broker_today_df=self.broker_today_df.groupby(['資料日期','證券代號']). \
            apply(self.exclude_broker_function).reset_index(drop=False).drop(['level_2'],axis=1)
        
        self.broker_ten_days_df=temp[temp.推薦方式 == '10日']
        self.broker_ten_days_df=self.broker_ten_days_df.groupby(['資料日期','證券代號']). \
            apply(self.exclude_broker_function).reset_index(drop=False).drop(['level_2'],axis=1)
        
    def combine_code(self,df):
        return ';'.join(df.證券代號.tolist())
    
    def make_table(self,df,today,n=10):
        df=df.copy()
        df=df.groupby(['資料日期']).apply(self.combine_code).reset_index(drop=False)
        df.資料日期=df.資料日期.astype('datetime64[ns]')
        
        before_today_df=df[df.資料日期<=today]
        if before_today_df.shape[0] <(n+1):
            return ' '
        else:
            appear=[]
            today_code=before_today_df[0].iloc[-1].split(';')
            for i in range(2,n+2):
                appear.append(before_today_df[0].iloc[-i].split(';'))
            
            appear=list(chain.from_iterable(appear))
            
            rs=[]
            for code in today_code:
                if code not in appear:
                    rs.append(code)
                else:
                    continue
            
            if len(rs)>0:
                print('新鮮麵包出爐囉~~~')
                if format(datetime.datetime.now(),'%Y-%m-%d') != before_today_df.資料日期.astype('str').iloc[-1]:
                    print('\n\n\n','強烈注意，這不是今天的結果')
                
                return pd.DataFrame({
                    '資料日期':before_today_df.資料日期.iloc[-1],
                    '證券代號':rs
                })
    
    def calculate(self):
        self.result1=self.make_table(df=self.broker_today_df,today=format(datetime.datetime.now(),'%Y-%m-%d'),n=6)
        self.result2=self.make_table(df=self.broker_ten_days_df,today=format(datetime.datetime.now(),'%Y-%m-%d'),n=6)
    
    def save(self):
        if self.result1 is not None:
            self.result1['證券名稱']=self.result1['證券代號'].map(self.stock_map_dict)
            self.result1['推薦方式']='地緣今日'
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result1.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                              if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result1.to_sql(self.table_name,self.engine,index=0,if_exists='append')
           
        if self.result2 is not None:
            self.result2['證券名稱']=self.result2['證券代號'].map(self.stock_map_dict)
            self.result2['推薦方式']='地緣10日'
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result2.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result2.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMakeMainForceBuySellRecommendationListProcess(StockCalculateLogic):
    """
    主力買賣超推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    def load(self):
        super().load()
        mainforce_inout=pd.read_sql_query(f'select * from 主力買賣超',self.engine)
        mainforce_inout['資料日期']=mainforce_inout['資料日期'].astype('str')
        self.mainforce_inout=mainforce_inout
        
    def condition1(self,v,divide=1/8):
        """
        條件，賣超的量不能大於買超的量*divide
        """
        v1=v[v>0]
        v2=v[v<0]
        if abs(np.sum(v2))<np.sum(v1)*divide:
            return True
        else:
            return False

    def condition2(self,df,threshold):
        """
        計算標的主力符合買量的天數
        """
        return df[df.主力買賣超>threshold].shape[0]

    def find_candidate(self,df,total_day,end_date,threshold,tolerate_day=1):
        if end_date not in df['資料日期'].unique():
            print((
                f'主力買賣超推薦-Warning:\n'
                f'Sorry for not found {end_date},so we change end_date to {df["資料日期"].iloc[-1]}'
                )
            )
            end_date=df['資料日期'].iloc[-1]
        
        before_day=df[df.資料日期 <= end_date]['資料日期'].unique()
        
        if len(before_day)<total_day:
            print('主力買賣超推薦 total_day is larger than amount of data: error!')
            return None
        else:
            choose_date_interval=np.sort(before_day)[-total_day:]
            
            df=df[df.資料日期.isin(choose_date_interval)]
        
        #刪除有資料筆數還很少，沒有足夠天數的資料，也就是有nan的資料
        df=df[~df.主力買賣超.isnull()]
        #刪除主力買賣超幾天合起來連total_day*50張都沒買到的標的
        amount_threshold=total_day*50
        code=df.groupby(['證券代號'])['主力買賣超'].sum() 
        df=df[df['證券代號'].isin(code[code>=amount_threshold].index)]
        #計算每隻剩下的標的，主力買了幾天，留下大於等於total_day-tolerate_day
        temp=df.groupby(['證券代號']).apply(self.condition2,threshold=threshold)
        code=temp[temp>=(total_day-tolerate_day)]
        df=df[df['證券代號'].isin(code.index)]
        #計算各股票買的量，賣不能超過買的量的1/8
        temp=df.groupby(['證券代號'])['主力買賣超'].apply(self.condition1)
        code=temp[temp]
        df=df[df['證券代號'].isin(code.index)]
        
        if df.shape[0] != 0:
            df=df[df.資料日期 == end_date].reset_index(drop=True)
            df.drop(['主力買賣超','id','create_time'],axis=1,inplace=True)
            return df
        else:
            return pd.DataFrame()
        
    def calculate(self):
        #看天數
        result1=self.find_candidate(df=self.mainforce_inout,end_date=format(datetime.datetime.now(),'%Y-%m-%d'),
                 threshold=100,total_day=3,tolerate_day=0)
        #可容忍
        result2=self.find_candidate(df=self.mainforce_inout,end_date=format(datetime.datetime.now(),'%Y-%m-%d'),
                 threshold=50,total_day=4,tolerate_day=1)
        
        result=pd.concat([result1,result2],axis=0)
        self.result=result[~result.duplicated(subset=['證券代號','資料日期'])]
    
    def save(self):
        if len(self.result)>0:
            self.result['證券名稱']=self.result['證券代號'].map(self.stock_map_dict)
            self.result['推薦方式']='主力買賣超'
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            if_exists='append',index=0)
            #step1 delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMakeAnotherBrokerInOutRecommendationListProcess(StockCalculateLogic):
    """
    十日差，法人買、當日某券商大買、地緣券商多日
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    def weighted_average(self,df):
        return np.round((df['買賣超']*df['買價']).sum()/df['買賣超'].sum(),decimals=4)

    def net_income_count(self,df):
        return ((df['賣量']*(df['賣價']-df['買價'])).sum())*1000

    def remaining_count(self,df):
        return np.round(df['買賣超'].sum())
    
    def find_near_broker(self,b):
        """
        篩選每家公司對應的10公里內券商，如果沒有任一家10公里的，則找最近兩家券商作代表
        """
        temp=b[b.距離<10]
        if temp.shape[0]>0:
            return temp.drop(['證券代號'],axis=1)
        else:
            if b.距離.min()<25:
                return b[b.距離 < (b.距離.min()+1)].drop(['證券代號'],axis=1)
            else:
                return None

    def load(self):
        super().load()
        
        #加一個限制條件，前15名至少要有3個以上的外資券商，否則偏向散戶買(國內券商多散戶QQ)
        self.foregin_broker=['港商麥格理','東方匯理','美林','台灣摩根士丹利','美商高盛','瑞士信貸','港商德意志',
            '港商野村','港商法國興業','花旗環球','新加坡商瑞銀','摩根大通','大和國泰','法銀巴黎','台灣巴克萊',
            '香港上海匯豐']
        
        List=pd.read_sql_query('select distinct 資料日期 from 券商進出總表 order by 資料日期',self.engine)['資料日期'].tolist()
        #轉為datetime格式 datetime.date還無法比較= =
        self.List=[datetime.datetime.strptime(t.strftime('%Y-%m-%d'),'%Y-%m-%d') for t in List]
        self.datelist=List
        
        #公司與券商距離
        geo_broker=pd.read_sql_query('select * from 地緣券商資訊',self.engine).drop(['id','create_time'],axis=1)
        geo_broker=geo_broker.groupby(['證券代號']).apply(self.find_near_broker).reset_index().drop(['level_1'],axis=1)
        self.company_info['已發行普通股數']=(self.company_info['已發行普通股數']/1000).round(0)
        geo_broker_info=pd.merge(left=geo_broker,right=self.company_info,on=['證券代號','公司名稱'],how='left')
        #扣除台北市、新北市，因為券商太過密集，無法判斷
        except_pat='台北市|臺北市|新北市|台北縣|臺北縣|新北巿|台北巿|台北巿|北市'
        keep_code=[i for i,s in zip(geo_broker_info.證券代號,geo_broker_info.公司地址) if not bool(re.search(string=s,
                                                                                                     pattern=except_pat))]
        #在台北新北市，需要嚴守地緣券商距離(<2 KM)                                                                                pattern=except_pat))]
        temp=geo_broker_info[geo_broker_info.證券代號.isin(keep_code)]
        code_list=temp[temp.距離>2].證券代號.tolist()
        self.geo_broker_info=geo_broker_info[geo_broker_info.證券代號.isin(code_list)]
        
    def prob_underestimate(self,target_table_name,end_date=datetime.datetime.now(),
                           foreign_broker_threshold=3):
        """
        十日差、券商買 & 當日某券商大買
        在加一個，如果top1>8*top2的數量，列為重要指標
        在加一個，如果前15名有買賣超為負的券商，刪除
        """
        date_list=[t for t in self.List if t <= end_date]
        end_date=date_list[-1].strftime('%Y-%m-%d')
        
        ten_days_broker_inout_df=pd.read_sql_query(f"select * from {target_table_name} where 資料日期 = '{end_date}' and 資料類型='分點'",
                                                   self.engine)
        deal_info=pd.read_sql_query(f"select * from 個股成交資訊 where 資料日期 ='{end_date}' order by 證券代號",self.engine)
        deal_info['資料日期']=deal_info['資料日期'].astype('str')
        
        #各股票殖利率、本益比表格
        metric_info=pd.read_sql_query(f"select * from 個股基本指標 where 資料日期='{end_date}'",self.engine)
        
        temp=ten_days_broker_inout_df.groupby(['證券代號'])
        #各股票對應券商進出的資訊
        broker_info=[]
        
        for gg in temp.groups:
            d=temp.get_group(gg)
            account_v=d.sort_values(['買賣超'],ascending=False)
            d_temp=account_v.iloc[0:15,:]
            account_v=np.sum(account_v.iloc[0:7].買賣超.values)+np.sum(account_v.iloc[-7:].買賣超.values)
            #限制條件3
            num=(d_temp['買賣超']<0).sum()
            if num >0:
                continue
            #限制條件1
            if d_temp.券商名稱.isin(self.foregin_broker).sum() < foreign_broker_threshold:
                continue
            
            if len(d_temp) < 10:
                continue
            
            #限制條件2
            d=pd.DataFrame({
                '證券代號':gg,
                '安全價位':self.weighted_average(d_temp),
                'top15大券商已獲利金額':self.net_income_count(d_temp),
                'top15大券商剩餘獲量':self.remaining_count(d_temp),
                'importance':(d_temp.iloc[0,:].買賣超/d_temp.iloc[1,:].買賣超)>=8,
                'top15大券商買賣超大於0數量':15-num,
                '主力買賣超':account_v
            },index=[1])
            d=pd.merge(left=d,right=deal_info,on=['證券代號'],how='left').loc[:,['證券代號','收盤','成交量','安全價位',
                                                                             'top15大券商已獲利金額',
                                                                             'top15大券商剩餘獲量','importance',
                                                                             'top15大券商買賣超大於0數量','主力買賣超']]
            d=pd.merge(left=d,right=metric_info,on=['證券代號'],how='left')
            d=d.loc[:,['證券代號','證券名稱','收盤','成交量','安全價位','top15大券商已獲利金額','top15大券商剩餘獲量',
                       'top15大券商買賣超大於0數量','主力買賣超','importance','殖利率(%)','本益比','股價淨值比']]
            broker_info.append(d)
        
        if len(broker_info) == 0:
            return
            
        broker_info=pd.concat(broker_info).reset_index(drop=True)
        
        #合併資訊
        broker_info=pd.merge(left=broker_info,right=self.aggregate_info,on=['證券代號','證券名稱'],how='left') 
        broker_info.收盤=broker_info.收盤.astype('float')
        broker_info['價位差(%)']=(broker_info['收盤']-broker_info['安全價位'])*100/broker_info['安全價位']
        broker_info=broker_info.drop(['已發行普通股數'],axis=1).loc[:,['證券代號','證券名稱','收盤','成交量',
                                                                '安全價位','價位差(%)','top15大券商已獲利金額',
                                                                'top15大券商剩餘獲量','top15大券商買賣超大於0數量',
                                                                '主力買賣超','importance','殖利率(%)','本益比',
                                                                '股價淨值比','產業別','主要經營業務','市場別']]
        broker_info=broker_info[~broker_info.本益比.isnull()]
        #三大法人
        query_sql=(
            f"select * from 三大法人買賣 where 資料日期 = '{end_date}'"
        )
        three_mainforce_data=pd.read_sql_query(query_sql,self.engine)
        three_mainforce_data['證券名稱']=three_mainforce_data['證券名稱'].map(str.strip)
        three_mainforce_data['外資買賣超']=three_mainforce_data['外資買賣超股數'].map(self.decimal_format)
        three_mainforce_data['投信買賣超']=three_mainforce_data['投信買賣超股數'].map(self.decimal_format)
        three_mainforce_data['自營商買賣超']=three_mainforce_data['自營商買賣超股數'].map(self.decimal_format)
        three_mainforce_data['三大法人買賣超']=three_mainforce_data['三大法人買賣超股數'].map(self.decimal_format)
        
        broker_info=pd.merge(left=broker_info,right=three_mainforce_data,on=['證券代號','證券名稱'],how='left')
        broker_info['資料日期']=end_date
        
        return broker_info
        
    def combine_function(self,df,restricted_num):
        """
        地緣多日
        """
        rs=pd.DataFrame(columns=['資料日期','買量', '買價'],index=[0])
        rs['資料日期']=df.資料日期.iloc[0]+'~'+df.資料日期.iloc[-1]
        
        if abs(df.買賣超.sum())<restricted_num:
            return None
        rs['買量']=df.買賣超.sum() 
        
        if df.買量.sum()-df.賣量.sum() ==0:
            return None
        
        if df.買量.sum()-df.賣量.sum()>0:
            rs['買價']=np.round(a=(((df.買量*df.買價).sum()-(df.賣量*df.賣價).sum())/(df.買量.sum()-df.賣量.sum()))
                              ,decimals=3)
        elif df.買量.sum()-df.賣量.sum()<0:
            rs['買價']=np.round(a=(((df.買量*df.買價).sum()-(df.賣量*df.賣價).sum())/abs(df.買量.sum()-df.賣量.sum())),
                              decimals=3)
        else:
            return None
        
        return rs
    
    def in_near(self,df):
        """
        地緣多日
        """
        temp=self.geo_broker_info[self.geo_broker_info.證券代號 == df.證券代號.iloc[0]]
        df=df[df.券商名稱.isin(temp.券商名稱)]
        if df.shape[0] == 0:
            return None
        else:
            return df.drop(['證券代號'],axis=1) 
    
    
    def broker_interval_estimate(self,start_date,end_date):
        """
        地緣多日
        """
        date_interval=[t.strftime('%Y-%m-%d') for t in self.datelist if t>= start_date and t<=end_date]
        
        if len(date_interval) == 0:
            return
        else:
            #每天至少平均要有25以上的量，不然這券商根本不重要
            restricted_num=25*len(date_interval) 
        
        temp=[]
        for t in date_interval:
            f=pd.read_sql_query(f"select * from 券商進出總表 where 資料日期 = '{t}' and 資料類型='分點'",self.engine)
            temp.append(f)
        
        temp=pd.concat(temp)
        temp['資料日期']=temp['資料日期'].astype('str')
        rs=temp.groupby(['證券代號','券商名稱']).apply(self.combine_function,restricted_num=restricted_num).reset_index(drop=False)
        rs=rs.drop(['level_2'],axis=1) 
        rs=rs[~rs.券商名稱.isin(self.foregin_broker)]
        rs=rs.groupby(['證券代號']).apply(self.in_near)
        rs=rs.reset_index(drop=False).drop(['level_1'],axis=1)
        rs=pd.merge(left=rs,right=self.geo_broker_info,
                    on=['證券代號','券商名稱'],how='left').loc[:,['證券代號','券商名稱',
                                                          '距離','資料日期', '買量', '買價','已發行普通股數']]
        rs['比例(%)']=(rs['買量']*100/rs['已發行普通股數']).apply(self.decimal_format,n=3)
        rs=rs.drop(['買量','已發行普通股數'],axis=1)  
        
        #比例須達到一定程度的券商，據參考價值
        bound=len(date_interval)*0.14
        return rs[rs['比例(%)'].map(abs)>bound].sort_values(['證券代號','比例(%)'])
    
    def combine_percentage(self,df):
        return df['比例(%)'].sum()
    
    def calculate(self):
        result1=self.prob_underestimate(target_table_name='券商進出_十日')
        if result1 is not None:
            result1=result1[np.logical_and(result1['價位差(%)']<-5,result1['三大法人買賣超']>100)].sort_values(['價位差(%)'])
            self.result1=result1.loc[:,['資料日期','證券代號']]
        else:
            self.result1=[]
        
        result2=self.prob_underestimate(target_table_name='券商進出總表',foreign_broker_threshold=0)
        if result2 is not None:
            result2=result2[np.logical_and(result2.importance,result2['三大法人買賣超']>50)].sort_values(['價位差(%)'])
            self.result2=result2.loc[:,['資料日期','證券代號']]
        else:
            self.result2=[]
        
        result3=self.broker_interval_estimate(start_date=self.datelist[-3],end_date=self.datelist[-1])
        result3=result3[result3.買價>0].groupby(['證券代號']).apply(self.combine_percentage).reset_index(drop=False)
        result3.columns=['證券代號','比例(%)']
        result3=result3[result3['比例(%)']>0.4]
        result3['資料日期']=self.List[-1]
        self.result3=result3.loc[:,['資料日期','證券代號']]
        
    def save(self):
        super().__init__()
        insert_data=[]
        
        if len(self.result1)>0:
            self.result1['證券名稱']=self.result1['證券代號'].map(self.stock_map_dict)
            self.result1['推薦方式']='十日差，法人買'
            insert_data.append(self.result1)

        if len(self.result2)>0:
            self.result2['證券名稱']=self.result2['證券代號'].map(self.stock_map_dict)
            self.result2['推薦方式']='當日某券商大買'
            insert_data.append(self.result2)
        
        if self.result3 is not None or len(self.result3)>0:
            self.result3['證券名稱']=self.result3['證券代號'].map(self.stock_map_dict)
            self.result3['推薦方式']='地緣多日'
            insert_data.append(self.result3)         
        
        
        #step truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()
        
        for d in insert_data:
            d.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,if_exists='append',index=0)
        
        #step delete already exist data
        delete_sql=f"""
        delete tb from {self.table_name} as tb
        where exists (
        select *
        from {self._diff_table_name} tb2
        where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
        )
        """
        self.cursor.execute(delete_sql)
        self.conn.commit()
        #step insert data
        for d in insert_data:
            d.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMakeOuterAndInvestmentCumulateRecommendationListProcess(StockCalculateLogic):
    """
    外投本比五日累積推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff2'
    _select_columns=['證券代號','資料日期','推薦方式']
    N=5
    
    def __init__(self):
        super().__init__()
        List=pd.read_sql_query(self.really_date_sql,self.engine)['資料日期'].astype('str').tolist()
        
        self.end_date=List[-1]
        self.start_date=List[-self.N]
            
    def load(self):
        super().load()
        
        query_sql=(
            f"select * from 三大法人買賣 where 資料日期 >= '{self.start_date}' and 資料日期 <= '{self.end_date}'"
        )
        three_mainforce_data=pd.read_sql_query(query_sql,
                            self.engine)
        self.three_mainforce_data=three_mainforce_data
        
        #殖利率、本益比表格(取最後一天做合併，因為這樣才可以更好的找出明天的標的
        today=self.end_date
        metric_info=pd.read_sql_query(f"select * from 個股基本指標 where 資料日期 = '{today}'",self.engine)
        metric_info=metric_info.drop(['股利年度'],axis=1)
        self.metric_info=metric_info.drop(['id', 'create_time'], axis=1)
        
        deal_info=pd.read_sql_query(f"select * from 個股成交資訊 where 資料日期 = '{today}'",self.engine)
        self.deal_info=deal_info.drop(['id', 'create_time'], axis=1)

    def calculate(self):
        self.three_mainforce_data=self.three_mainforce_data.loc[:,['資料日期','證券代號','證券名稱','外資買進股數',
                                                                   '外資賣出股數','外資買賣超股數', '投信買進股數',
                                                                   '投信賣出股數', '投信買賣超股數','自營商買賣超股數',
                                                                   '自營商買進股數', '自營商賣出股數','三大法人買賣超股數'
                                                                   ]]
        
        numeric_col=[col for col in self.three_mainforce_data.columns if col not in ['證券代號','證券名稱','資料日期']]
        self.three_mainforce_data=self.three_mainforce_data[self.three_mainforce_data['證券代號'].isin(self.all_code_info['證券代號'].tolist())]
        
        for col in numeric_col:
            self.three_mainforce_data=self.three_mainforce_data[~self.three_mainforce_data[col].isnull()]
        
        #清理空格
        self.three_mainforce_data['證券代號']=[s.strip() for s in self.three_mainforce_data.證券代號]
        self.three_mainforce_data['證券名稱']=[s.strip() for s in self.three_mainforce_data.證券名稱]
        
        D=pd.merge(left=self.three_mainforce_data,right=self.aggregate_info,on=['證券代號','證券名稱'],how='left')    
        D['外本比_%']=(D['外資買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D['投本比_%']=(D['投信買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D['三大本比_%']=(D['三大法人買賣超股數']/D['已發行普通股數']).apply(self.decimal_format,n=5)
        D=D.drop(['外資買賣超股數','投信買賣超股數','自營商買賣超股數','三大法人買賣超股數','已發行普通股數'],axis=1)

        D=pd.merge(left=D,right=self.metric_info,on=['資料日期','證券代號','證券名稱'],how='left')
        D=pd.merge(left=D,right=self.deal_info,on=['資料日期','證券代號'],how='left')
        #各股票殖利率、本益比表格
        metric_info=pd.read_sql_query(f"select * from 個股基本指標 where 資料日期='{self.end_date}'",self.engine)
        metric_info=metric_info.drop(['股利年度'],axis=1)
        metric_info['資料日期']=metric_info['資料日期'].astype('str')
        
        deal_info=pd.read_sql_query(f"select * from 個股成交資訊 where 資料日期='{self.end_date}'",self.engine)
        deal_info['資料日期']=deal_info['資料日期'].astype('str')
        D=pd.merge(left=D,right=metric_info,on=['資料日期','證券代號','證券名稱'],how='left')
        D=pd.merge(left=D,right=deal_info,on=['資料日期','證券代號'],how='left')

        temp=D.groupby(['證券代號','證券名稱'])
        
        Total_View=pd.DataFrame(columns=['資料區間', '證券代號', '證券名稱', '產業別', '主要經營業務',
                                         '市場別', '外本比_%', '投本比_%','三大本比_%','外資買超天數',
                                         '投信買超天數','外資賣超天數','投信賣超天數','殖利率(%)', '本益比',
                                         '股價淨值比','收盤','成交量'])
        
        for i,j in temp.groups:
            info=dict()
            tt=temp.get_group((i,j))
            tt=tt[~tt['本益比'].isnull()]
            if len(tt) == 0:
                continue
            
            info['外本比_%']=tt['外本比_%'].sum()
            info['投本比_%']=tt['投本比_%'].sum()
            info['三大本比_%']=tt['三大本比_%'].sum()
            info['外資買超天數']=(tt['外本比_%']>0).sum()
            info['投信買超天數']=(tt['投本比_%']>0).sum()
            info['外資賣超天數']=(tt['外本比_%']<0).sum()
            info['投信賣超天數']=(tt['投本比_%']<0).sum()
            info['資料區間']=tt['資料日期'].iloc[0]+'~'+tt['資料日期'].iloc[-1]
            info['證券代號']=i
            info['證券名稱']=j
            info['產業別']=tt['產業別'].iloc[0]
            info['主要經營業務']=tt['主要經營業務'].iloc[0]
            info['市場別']=tt['市場別'].iloc[0]
            info['殖利率(%)']=tt['殖利率(%)'].iloc[-1]
            info['本益比']=tt['本益比'].iloc[-1]
            info['股價淨值比']=tt['股價淨值比'].iloc[-1]
            info['收盤']=tt['收盤'].iloc[-1]
            info['成交量']=tt['成交量'].iloc[-1]
            info=pd.DataFrame(info,index=[1])
            
            Total_View=pd.concat([Total_View,info],ignore_index=True)
            
        Total_View=Total_View.loc[:,['資料區間', '證券代號', '證券名稱', '產業別', '主要經營業務',
                                    '市場別','外本比_%','投本比_%','三大本比_%','外資買超天數',
                                    '投信買超天數','外資賣超天數','投信賣超天數',
                                    '殖利率(%)', '本益比', '股價淨值比','收盤','成交量']]
        Total_View['資料日期']=self.end_date
        Total_View['外資買超天數']=Total_View['外資買超天數'].astype('float')
        Total_View['投信買超天數']=Total_View['投信買超天數'].astype('float')
        Total_View['外資賣超天數']=Total_View['外資賣超天數'].astype('float')
        Total_View['投信賣超天數']=Total_View['投信賣超天數'].astype('float')
        Total_View=Total_View[np.logical_or((Total_View['外本比_%']>0).values,(Total_View['投本比_%']>0).values)]
        Total_View=Total_View[Total_View['三大本比_%']>0]
        Total_View=Total_View[np.logical_or((Total_View.外資買超天數>=(0.8*self.N)).values,
                                            (Total_View.投信買超天數>=(0.8*self.N)).values)]
        Total_View=Total_View[Total_View.成交量>100]
        Total_View=Total_View.loc[:,['資料日期','資料區間','證券代號','證券名稱','產業別','主要經營業務',
                                     '市場別','外本比_%','投本比_%','三大本比_%','外資買超天數','投信買超天數',
                                     '外資賣超天數','投信賣超天數','殖利率(%)', '本益比', '股價淨值比']].sort_values(['外資買超天數','投信買超天數'],ascending=False)
        
        self.result1=Total_View[np.logical_and(Total_View['外本比_%']>0.4,Total_View['外本比_%']<1)]
        self.result2=Total_View[np.logical_and(Total_View['投本比_%']>0.4,Total_View['投本比_%']<1)]
    
    def save(self):
        insert_data=[]
        
        if len(self.result1)>0:
            self.result1['證券名稱']=self.result1['證券代號'].map(self.stock_map_dict)
            self.result1['推薦方式']='外本比累積'
            insert_data.append(self.result1)

        if len(self.result2)>0:
            self.result2['證券名稱']=self.result2['證券代號'].map(self.stock_map_dict)
            self.result2['推薦方式']='投本比累積'
            insert_data.append(self.result2)
            
        #step truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()
        
        for d in insert_data:
            d.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,if_exists='append',index=0)
        
        #step delete already exist data
        delete_sql=f"""
        delete tb from {self.table_name} as tb
        where exists (
        select *
        from {self._diff_table_name} tb2
        where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
        )
        """
        self.cursor.execute(delete_sql)
        self.conn.commit()
        #step insert data
        for d in insert_data:
            d.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMajorHolderRecommendationListProcess(StockCalculateLogic):
    """
    大戶比推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    def __init__(self):
        super().__init__()
        lastest_date_sql=f"""
            SELECT DISTINCT 資料日期
            FROM 大戶比例
            ORDER BY 資料日期 DESC
            LIMIT 2
        """
        self.lastest_date=pd.read_sql_query(lastest_date_sql,self.engine)['資料日期'].astype('str').tolist()
    
    def load(self):
        super().load()
        major_df=pd.read_sql_query('select * from 大戶比例 where 資料日期 in %s' % str(tuple(self.lastest_date)),
                                   self.engine)
        major_df['資料日期']=major_df['資料日期'].astype('str')
        self.major_df=major_df.sort_values(['證券代號','資料日期'])
        
    def calculate_recommend_major(self,df):
        if df.shape[0] != 2:
            return pd.DataFrame()
        else:
            temp1=np.round(a=(df['>400張大股東持有百分比'].iloc[-1]-df['>400張大股東持有百分比'].iloc[0]),
                        decimals=2)
            
            temp2=np.round(df['>1000張大股東持有百分比'].iloc[-1]-df['>1000張大股東持有百分比'].iloc[0],decimals=2)
            
            if abs(temp1) > 2 or abs(temp2) >2:
                return pd.DataFrame({
                    '資料日期':df.資料日期.iloc[-1],
                    '證券代號':df.證券代號.iloc[0],
                    '400大戶比變化':temp1,
                    '1000大戶比變化':temp2
                },index=[1])
            else:
                return pd.DataFrame()
    
    def calculate(self):
        #最新大戶比潛在名單
        result=self.major_df.groupby(['證券代號']).apply(self.calculate_recommend_major).reset_index(drop=True)
        if len(result)>0:
            result['證券名稱']=result['證券代號'].map(str.strip).map(self.stock_map_dict)
            
            r_temp=[]
            for v1,v2 in zip(result['400大戶比變化'],result['1000大戶比變化']):
                if (v1>2 or v2>2) and (v1<-2 or v2<-2):
                    r_temp.append('大戶衝、大戶空')
                elif (v1>2 or v2>2) and not (v1<-2 or v2<-2):
                    r_temp.append('大戶衝')
                elif not (v1>2 or v2>2) and (v1<-2 or v2<-2):
                    r_temp.append('大戶空')
            
            result['推薦方式']=r_temp
            self.result=result.loc[:,['資料日期','證券代號','證券名稱','推薦方式']]
        else:
            self.result=[]
        
    def save(self):
        if len(self.result)>0:
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            if_exists='append',index=0)
            #delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')


class StockMarginTradeRecommendationListProcess(StockCalculateLogic):
    """
    融資融券推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff2'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    threshold_5=50
    threshold_20=100
    threshold_5_times=10
    
    def load(self):
        super().load()
        margin_trade_df=pd.read_sql_query('select * from 融資融券 order by 證券代號,資料日期 desc',self.engine)
        margin_trade_df['資料日期']=margin_trade_df['資料日期'].astype('str')
        self.margin_trade_df=margin_trade_df
        
    def calculate_recommend_margin(self,df):
        if df.shape[0]<23:
            return pd.DataFrame()
        
        #融資
        five_mean_margin=df.當日融資變化.map(abs).rolling(window=5,).mean()
        five_mean_margin=five_mean_margin[~five_mean_margin.isnull()].iloc[1]

        month_mean_margin=df.當日融資變化.map(abs).rolling(window=22,).mean()
        month_mean_margin=month_mean_margin[~month_mean_margin.isnull()].iloc[1]

        margin=df.當日融資變化.iloc[0]
        
        if five_mean_margin>0 and margin>50 and abs(five_mean_margin)<=self.threshold_5 and \
            abs(month_mean_margin)<self.threshold_20 and margin>10*five_mean_margin:
            dat1=pd.DataFrame({
                '資料日期':df['資料日期'].iloc[0],
                '證券代號':df.證券代號.iloc[0],
                '當日融資變化':df.當日融資變化.iloc[0],
                '融資餘額':df.融資剩餘額.iloc[0],
                '五日平均':five_mean_margin,
                '五日融資值':';'.join([str(s) for s in df.當日融資變化.iloc[1:6]]),
                '推薦方式':'融資暴增'
            },index=[1])
        else:
            dat1=pd.DataFrame()

        #融券
        five_mean_margin=df.當日融券變化.map(abs).rolling(window=5,).mean()
        five_mean_margin=five_mean_margin[~five_mean_margin.isnull()].iloc[1]

        month_mean_margin=df.當日融券變化.map(abs).rolling(window=22,).mean()
        month_mean_margin=month_mean_margin[~month_mean_margin.isnull()].iloc[1]

        margin=df.當日融券變化.iloc[0]
        
        if five_mean_margin>0 and margin>50 and abs(five_mean_margin)<=self.threshold_5 and \
            abs(month_mean_margin)<self.threshold_20 and margin>10*five_mean_margin:
            dat2=pd.DataFrame({
                '資料日期':df['資料日期'].iloc[0],
                '證券代號':df.證券代號.iloc[0],
                '當日融券變化':df.當日融券變化.iloc[0],
                '融券餘額':df.融券剩餘額.iloc[0],
                '五日平均':five_mean_margin,
                '五日融券值':';'.join([str(s) for s in df.當日融券變化.iloc[1:6]]),
                '推薦方式':'融券暴增'
            },index=[1])
        else:
            dat2=pd.DataFrame()
        
        dat=pd.concat([dat1,dat2])
        
        if len(dat) == 2:
            dat=dat.reset_index(drop=True)
            dat.loc[0,'推薦方式']='融資暴增,融券暴增'
            dat=dat.loc[[0],:]
        
        return dat
    
    def calculate(self):
        #最新融資券名單
        self.result=self.margin_trade_df.groupby(['證券代號']).apply(self.calculate_recommend_margin).reset_index(drop=True)
            
    def save(self):
        if len(self.result)>0:
            self.result['證券名稱']=self.result['證券代號'].map(str.strip).map(self.stock_map_dict)
            self.result=self.result.loc[:,['資料日期','證券代號','證券名稱','推薦方式']]
            
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            if_exists='append',index=0)
            #delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
    
    
class StockMonthRevenueRecommendationListProcess(StockCalculateLogic):
    """
    月營收資訊推薦名單
    """
    table_name='觀察清單'
    _diff_table_name='觀察清單_diff'
    _select_columns=['證券代號','資料日期','推薦方式']
    
    threshold=100
    
    def load(self):
        super().load()
        month_revenue_df=pd.read_sql_query('select * from 月營收資訊',self.engine)
        month_revenue_df['資料日期']=month_revenue_df['資料日期'].astype('str')
        month_revenue_df=month_revenue_df.sort_values(['證券代號','資料日期'],ascending=False)
        self.month_revenue_df=month_revenue_df
    
    def calculate_recommend_month_revenue(self,df):  
        """
        連續月營收暴增名單
        """
        if df.shape[0] == 0:
            return pd.DataFrame()
        elif df.shape[0] ==1:
            temp=df['去年同月增減(%)'].iloc[0]>self.threshold
            temp2=False
            temp3=False
        elif df.shape[0] ==2:
            temp=df['去年同月增減(%)'].iloc[0]>self.threshold
            temp2=df['去年同月增減(%)'].iloc[1]>self.threshold        
            temp3=False
        else:
            temp=df['去年同月增減(%)'].iloc[0]>self.threshold
            temp2=df['去年同月增減(%)'].iloc[1]>self.threshold
            temp3=df['去年同月增減(%)'].iloc[2]>self.threshold

        if temp and temp2 and temp3:
            T=[df['去年同月增減(%)'].iloc[0],\
            df['去年同月增減(%)'].iloc[1],\
            df['去年同月增減(%)'].iloc[2]]
        
            return pd.DataFrame({
                '資料日期':df['資料日期'].iloc[0],
                '證券代號':df['證券代號'].iloc[0],
                '去年同月增減%':';'.join([str(s) for s in T])
            },index=[1])
        else:
            return pd.DataFrame()
    
    def calculate(self):
        #最新融資券名單
        self.result=self.month_revenue_df.groupby(['證券代號']).apply(self.calculate_recommend_month_revenue).reset_index(drop=True) 
            
    def save(self):
        if len(self.result)>0:
            self.result['證券名稱']=self.result['證券代號'].map(str.strip).map(self.stock_map_dict)
            self.result['推薦方式']='前3個月營收亮眼'
            self.result=self.result.loc[:,['資料日期','證券代號','證券名稱','推薦方式']]

            
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                            if_exists='append',index=0)
            #delete already exist data
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.推薦方式=tb2.推薦方式
            )
            """
            self.cursor.execute(delete_sql)
            self.conn.commit()
            #insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')