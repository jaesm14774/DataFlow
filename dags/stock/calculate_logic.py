import numpy as np
import re
import pandas as pd
import time
from stock.config.config import *
from itertools import chain
from lib.db_connection import MySQLConnection as _BaseMySQLConnection
from lib.taipei_time import taipei_now


class MySQLConnection(_BaseMySQLConnection):
    """stock 專用：繼承統一版 MySQLConnection，預設使用 stock config 的 DB_NAME。"""

    def __init__(self, sql_configure_path=None):
        super().__init__(db_name=DB_NAME, sql_configure_path=sql_configure_path or globals().get('sql_configure_path'))


class StockCalculateLogic(MySQLConnection):
    """
    股票收集基本邏輯
    """
    now_time=taipei_now() #將時間變台灣時間
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

    #清理已發行普通股數
    def clean_company_info(self,s):
        if '股' in s:
            s=re.sub(string=s,pattern=r'股|\s+',repl='')
        return int(re.sub(string=s,pattern=',',repl=''))

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
        except Exception:
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
                    print(f'[地緣券商-當日] 買 | 代號={code} 券商={broker_name} 距離={self.geo_broker_info[np.logical_and(self.geo_broker_info["券商名稱"]==broker_name,self.geo_broker_info["證券代號"] == code)].距離.iloc[0]}')
                                       
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
                    print(f'[地緣券商-當日] 賣 | 代號={code} 券商={broker_name} 距離={self.geo_broker_info[np.logical_and(self.geo_broker_info["券商名稱"]==broker_name,self.geo_broker_info["證券代號"] == code)].距離.iloc[0]}')
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
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','券商名稱','推薦方式','買賣'],self._select_columns)


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
        except Exception:
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
                    print(f'[地緣券商-N日] 買 | 代號={code} 券商={broker_name} 距離={self.geo_broker_info[np.logical_and(self.geo_broker_info["券商名稱"]==broker_name,self.geo_broker_info["證券代號"] == code)].距離.iloc[0]}')
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
                    print(f'[地緣券商-N日] 賣 | 代號={code} 券商={broker_name} 距離={self.geo_broker_info[np.logical_and(self.geo_broker_info["券商名稱"]==broker_name,self.geo_broker_info["證券代號"] == code)].距離.iloc[0]}')
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
        print(f'[地緣券商-十日] 聚合計算耗時={time.time()-st:.1f}s')
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
        
        print(f'[地緣券商-十日] 待回測日期列表（共 {len(List)} 個）={List[:5]}{"…" if len(List)>5 else ""}')
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
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','券商名稱','推薦方式','買賣'],self._select_columns)
    
    def process(self):
        self.load()
        self.calculate()
        self.save()
        # self.another_calculate_and_save() #儲存最新數據進 券商進出_十日
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
        if len(self.result)>0:
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)


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
                print(f'[觀察清單-地緣] 新進榜 | 代號={rs} | 資料日={before_today_df.資料日期.iloc[-1]}')
                if format(datetime.datetime.now(),'%Y-%m-%d') != before_today_df.資料日期.astype('str').iloc[-1]:
                    print('[觀察清單-地緣] 警告：最新一筆資料日非今日（回測或 DB 延遲）')
                
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
            self.replace_by_keys(self.result1,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','推薦方式'],self._select_columns)
           
        if self.result2 is not None:
            self.result2['證券名稱']=self.result2['證券代號'].map(self.stock_map_dict)
            self.result2['推薦方式']='地緣10日'
            self.replace_by_keys(self.result2,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','推薦方式'],self._select_columns)


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
            print(f'[主力買賣超推薦] 無資料日 {end_date}，改用最後可用日 {df["資料日期"].max()}')
            end_date=df['資料日期'].max()
        
        before_day=df[df.資料日期 <= end_date]['資料日期'].unique()
        
        if len(before_day)<total_day:
            print(f'[主力買賣超推薦] 資料天數不足 | 需要={total_day} 實際={len(before_day)}（無法計算窗口）')
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
        
        frames=[r for r in (result1,result2) if r is not None]
        if len(frames)==0:
            self.result=pd.DataFrame()
            return
        result=pd.concat(frames,axis=0)
        self.result=result[~result.duplicated(subset=['證券代號','資料日期'])]
    
    def save(self):
        if len(self.result)>0:
            self.result['證券名稱']=self.result['證券代號'].map(self.stock_map_dict)
            self.result['推薦方式']='主力買賣超'
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','推薦方式'],self._select_columns)


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
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','推薦方式'],self._select_columns)


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
            
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','推薦方式'],self._select_columns)
    
    
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

            
            self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期','推薦方式'],self._select_columns)