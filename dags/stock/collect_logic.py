import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import pandas as pd
import time
import csv
import json
import shutil
import random
from math import sin, asin, cos, radians, fabs, sqrt
from ast import literal_eval #避免geometry那欄變成str
import googlemaps #google api 查詢經緯度
from stock.config.config import *
import cv2
from lib.log_process_execution import BaseLogRecord
from lib.taipei_time import taipei_now
from lib.db_connection import MySQLConnection as _BaseMySQLConnection
import boto3
from io import StringIO
from stock import margin_daily
from stock.parsing import clean_comma as _clean_comma, make_regular_date_format as _make_regular_date_format

import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"]="python"


class MySQLConnection(_BaseMySQLConnection):
    """stock 專用：繼承統一版 MySQLConnection，預設使用 stock config 的 DB_NAME。"""

    def __init__(self, sql_configure_path=None):
        super().__init__(db_name=DB_NAME, sql_configure_path=sql_configure_path or globals().get('sql_configure_path'))


class StockCollectLogic(MySQLConnection):
    """
    股票收集基本邏輯
    """
    now_time=(datetime.datetime.strptime(BACKFILL_DATE, '%Y-%m-%d')
              if BACKFILL_DATE
              else taipei_now())
    union_column_name_dict={
        '日期':'資料日期',
        '有價證券代號':'證券代號',
        '有價證券名稱':'證券名稱',
        'code':'證券代號',
        '觀察清單':'證券代號'
    }
    sql_configure_path=sql_configure_path
    timeout=60
    headers={'User-Agent':random.choice(user_agent),'Referer':'https://www.google.com/'}
    _select_columns=['證券代號','資料日期'] #delete insert時，選的key pair
    _diff_table_name='股票模板_diff'
    app_name = ''
    table_name = ''
    insert_count = 0
    delete_count = 0
    
    def __init__(self):
        super().__init__(self.sql_configure_path)
        self.really_date_sql=f"""
        SELECT DISTINCT 資料日期
        FROM {self.db_name}.三大法人買賣
        ORDER BY 資料日期 DESC
        LIMIT 1
        """
        #初始化log
        self.log_record=BaseLogRecord(process_date=self.now_time.strftime('%Y-%m-%d'),app_name=self.app_name)
        self.log_record.set_before_count(self.table_row_count(self.table_name))
        self.aws_client=boto3.client('lambda',aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_key,region_name='us-east-1')

    # 超過這個列數的表，exact COUNT(*) 要掃整個索引（券商進出總表 3.5 億列約 5 分鐘，而且每個任務開始與結束各一次），
    # 改用 information_schema 的估計值；before/after 只用在執行紀錄，不影響任何邏輯。
    APPROX_COUNT_THRESHOLD=2_000_000

    def table_row_count(self,table_name):
        try:
            estimate=pd.read_sql_query(
                f"select table_rows as N from information_schema.tables "
                f"where table_schema = '{self.db_name}' and table_name = '{table_name}'",self.engine)['N'].iloc[0]
            if estimate is not None and estimate == estimate and int(estimate) > self.APPROX_COUNT_THRESHOLD:
                return int(estimate)
        except Exception as e:
            print(f'[{self.app_name}] 讀取列數估計失敗，改用 COUNT(*) | {type(e).__name__}: {e}')
        return pd.read_sql_query(f'select count(1) as N from {table_name}',self.engine)['N'].iloc[0]

    def end(self):
        try:
            http_session = getattr(self, 'http_session', None)
            if http_session is not None:
                http_session.close()
        finally:
            super().end()
    
    def simulate_request(self,event):
        """
        被公開觀測站鎖了，放大絕招 aws lambda
        """
        response=self.aws_client.invoke(FunctionName='similar_proxy',
                                        Payload=json.dumps(event))
        
        a=response['Payload'].read()
        a=a.decode('utf8')
        
        return json.loads(a)
        
    def clean_comma(self,txt,output_not_string=False):
        """
        清除逗號，output_not_string=True 時強制轉數字，無法轉則回傳 np.nan
        """
        return _clean_comma(txt,output_not_string)

    make_regular_date_format=staticmethod(_make_regular_date_format)
    
    def collect(self):
        """
        爬蟲程式
        """
        pass
    
    def save(self):
        """
        儲存資料進資料庫(或其他)
        """
        pass
    
    def log_process_record(self):
        """
        紀錄執行過程中的基本資訊
        """
        self.log_record.set_delete_count(self.delete_count)
        self.log_record.set_insert_count(self.insert_count)
        self.log_record.set_after_count(self.table_row_count(self.table_name))
    
    def process(self):
        """
        主要的資料流程
        """
        print(f'[{self.app_name}] 開始 | 基準日={self.now_time.date()} | BACKFILL_DATE={BACKFILL_DATE}')
        try:
            self.collect()
            self.save()
            self.log_process_record()
            
            self.log_record.success = 1
            print(f'[{self.app_name}] 成功 | insert={self.insert_count} delete={self.delete_count}')
        except Exception as e:
            self.log_record.raise_error(repr(e))
            print(f'[{self.app_name}] 失敗 | {type(e).__name__}: {e}')
            raise e
        finally:
            self.log_record.insert_to_log_record()
            self.end()


class StockBackUpProcess(MySQLConnection):
    """
    股票已下市，不應在紀錄資料，且把資料移到delete_<>資料表，
    確保已刪除不會與上市上櫃股票混在一起，且有機會被還原
    """
    def __init__(self,sql_configure_path):
        super().__init__(sql_configure_path)

    def make_backup_insert_sql(self,source_table_name,delete_table_name,select_column):
        insert_sql=f"""
        insert into {delete_table_name}{str(tuple(select_column)).replace("'",'`')}
        select `{','.join(select_column).replace(',','`,`')}`
        from {source_table_name}
        where 證券代號 = '{self.code}'
        """.strip()

        return insert_sql

    def make_backup_delete_sql(self,source_table_name):
        delete_sql=f"""
            delete from {source_table_name}
            where 證券代號 = '{self.code}'
        """.strip()

        return delete_sql
    
    #備份被移除的證券代號
    def back_up(self,code):
        self.code=str(code).strip()
        print(f'[下市備份] 證券代號={self.code} | 將資料移至 delete_* 表並自來源表刪除')

        source_table_name_list=['全證券代碼相關資料表','個股成交資訊','大戶比例','公司資訊','地緣券商資訊',
                                '月營收資訊','融資融券','主力買賣超','三大法人買賣','個股基本指標',
                                '券商進出_十日','券商進出總表','外本比','投本比','當沖資訊',
                                '地緣券商推薦','法人比推薦','觀察清單']

        delete_table_name_list=[f'delete_{source_table_name}' for source_table_name in source_table_name_list]

        select_column_list=[
            [
                '證券代號', '證券名稱', '國際證券辨識號碼(ISIN Code)', '上市日','市場別',
                '產業別','CFICode','備註', 'create_time'
            ],
            ['資料日期', '證券代號','開盤','最高','最低', '收盤', '成交量','create_time'],
            [
                '資料日期', '證券代號', '集保總張數', '總股東人數', '平均張數/人', '>400張大股東持有張數',
                '>400張大股東持有百分比', '>400張大股東人數', '400~600張人數', '600~800張人數',
                '800~1000張人數', '>1000張人數', '>1000張大股東持有百分比', 'create_time'
            ],
            [
                '證券代號', '公司名稱', '成立日期', '董事長', '總經理', '實收資本額', '已發行普通股數', '發言人',
                '代理發言人', '總機電話', '傳真號碼', '統一編號', '公司網站', '公司地址', '電子郵件', '英文簡稱', '英文全名',
                '英文通訊地址', '主要經營業務', 'geometry', 'create_time'
            ],
            ['證券代號', '公司名稱', '券商名稱', '距離', 'create_time'],
            [
                '證券代號', '資料日期', '當月營收', '上月營收', '去年當月營收', '上月營收增減(%)',
                '去年同月增減(%)', '當月累計營收', '去年累計營收', '前期比較增減(%)', 'create_time'
            ],
            [
                '資料日期', '證券代號', '當日融資變化', '當日融券變化', '融資剩餘額', '融券剩餘額', '券資比',
                '融資買進', '融資賣出', '融資使用率', '融券賣出', '融券買進', 'create_time'
            ],
            ['證券代號', '資料日期', '主力買賣超', 'create_time'],
            [
                '資料日期','證券代號','證券名稱','外資買進股數','外資賣出股數',
                '外資買賣超股數','投信買進股數','投信賣出股數','投信買賣超股數',
                '自營商買賣超股數','自營商買進股數','自營商賣出股數','三大法人買賣超股數',
                'create_time'
            ],
            [
                '資料日期', '證券代號', '證券名稱', '殖利率(%)', '股利年度', '本益比', '股價淨值比',
                'create_time'
            ],
            [
                '資料日期', '資料區間', '證券代號', '券商名稱', '買量', '買價', '賣量', '賣價', '買賣超',
                '均價', '資料類型', 'create_time'
            ],
            [
                '資料日期', '證券代號', '券商代號', '券商名稱', '買量', '買價', '賣量', '賣價', '買賣超',
                '均價', '資料類型', 'create_time'
            ],
            ['資料日期', '證券代號', '證券名稱', '外本比分數', 'create_time'],
            ['資料日期', '證券代號', '證券名稱', '投本比分數', 'create_time'],
            [
                '資料日期', '證券代號', '證券名稱', '當日沖銷交易成交股數', '當日沖銷交易買進成交金額',
                '當日沖銷交易賣出成交金額', 'create_time'
            ],
            ['資料日期', '證券代號', '券商名稱', '地址','距離','買量','賣量','推薦方式','買賣','create_time'],
            [
                '資料日期', '證券代號', '證券名稱', '產業別', '市場別',
                '外本比_%', '投本比_%', '三大本比_%', '推薦方式','create_time'
            ],
            ['資料日期', '證券代號', '證券名稱', '推薦方式', 'create_time']
        ]

        method_list=[['insert']]+[['insert','delete'] for i in range(0,len(source_table_name_list)-1)]

        for i in range(len(source_table_name_list)):
            source_table_name=source_table_name_list[i]
            delete_table_name=delete_table_name_list[i]
            select_column=select_column_list[i]
            method=method_list[i]

            if 'insert' in method:
                insert_sql=self.make_backup_insert_sql(source_table_name,delete_table_name,select_column)
                self.cursor.execute(insert_sql)

            if 'delete' in method:
                delete_sql=self.make_backup_delete_sql(source_table_name)
                self.cursor.execute(delete_sql)

            if i % 5 == 0:
                self.conn.commit()
            
            print(f'[下市備份] 證券代號={self.code} | {i+1}/{len(source_table_name_list)} | 表={source_table_name}')

        self.conn.commit()

       
class StockRenwInfoProcess(MySQLConnection):
    """
    更新新上市櫃公司相關的資訊進sql，例如公司資訊、券商距離等，以利後續股票收集
    """
    #google gmap api function
    gmaps = googlemaps.Client(key=api_key)
    
    def __init__(self,sql_configure_path):
        super().__init__(sql_configure_path)
        self.aws_client=boto3.client('lambda',aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_key,region_name='us-east-1')
      
    def simulate_request(self,event):
        """
        被公開觀測站鎖了，放大絕招 aws lambda
        """
        response=self.aws_client.invoke(FunctionName='similar_proxy',
                                        Payload=json.dumps(event))
        
        a=response['Payload'].read()
        a=a.decode('utf8')
        
        return json.loads(a)   
           
    #地址轉換經緯度
    def address_to_geo_info(self,addr):
        try:
            api_result=self.gmaps.geocode(addr)
            api_result=api_result[0]['geometry']['location']
            return (api_result['lng'],api_result['lat'])
        except Exception as e:
            print(f'[新上市-地址轉經緯度] Google Geocode 失敗 | 地址={addr!s} | {type(e).__name__}: {e}')
            return np.nan
        
    #公司地址
    def clean_address(self,addr):
        if bool(re.search(string=addr,pattern=r'至\d+樓$')):
            addr=re.sub(string=addr,pattern=r'至\d+樓',repl='')+'樓'

        addr=re.sub(string=addr,pattern=r'\(.+\)',repl='')
        return addr
    
    def haversine(self,lon1, lat1, lon2, lat2): # 经度1，纬度1，经度2，纬度2 （十进制度数）
        """
        Calculate the great circle distance between two points 
        on the earth (specified in decimal degrees)
        """
        # 将十进制度数转化为弧度
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine公式
        dlon = lon2 - lon1 
        dlat = lat2 - lat1 
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a)) 
        r = 6371 # 地球平均半径，单位为公里
        return c * r * 1000
    
    #更新新增的代號所需的表格
    def renew_info(self,code):
        self.code=str(code).strip()
        print(f'[新上市更新] 證券代號={self.code} | 寫入公司資訊與地緣券商')
        #更新 公司資訊 
        #公開資訊站
        url='https://mopsov.twse.com.tw/mops/web/ajax_t05st03'
        data={
            'encodeURIComponent':'1',
            'step':'1',
            'firstin':'1',
            'off':'1',
            'keyword4':'',
            'code1':'',
            'TYPEK2':'',
            'checkbtn':'',
            'queryName':'co_id',
            'inpuType':'co_id',
            'TYPEK':'all',
            'co_id':f'{code}',
        }
        a=self.simulate_request(event={'url':url,'data':data,
                                  'method':'post'})
        soup=BeautifulSoup(a['text'],'lxml')

        company_info={}
        #預防還沒上市股票
        try:
            if soup.find('h3').text.strip() == '此公司代號不存在或公司未申報基本資料！':
                return
        except Exception:
            pass

        temp=soup.find('table').find_next('table')

        unimportant=['特別股發行','公司債發行','取得係屬科技事業暨其產品或技術開發成功且具有市場性之明確意見書']

        for ii,jj in zip([s for s in temp.find_all('th') if s.text not in unimportant],temp.find_all('td')):
            company_info[ii.text.strip()]=jj.text.strip()

        company_info['公司地址']=company_info['地址']
        company_info['傳真號碼']=company_info['傳真機號碼']
        company_info['已發行普通股數']=company_info['已發行普通股數或TDR原股發行股數'].split('\n')[0].strip('股')
        company_info['電子郵件']=company_info['電子郵件信箱']
        company_info['證券代號']=company_info['股票代號']
        company_info['成立日期']=company_info['公司成立日期']
        company_info['統一編號']=company_info['營利事業統一編號']
        company_info['公司網站']=company_info['公司網址']
        company_info['英文通訊地址']=company_info['英文通訊地址(街巷弄號)']+company_info['英文通訊地址(縣市國別)']
        company_info['總機電話']=company_info['總機']
        company_info=pd.DataFrame(company_info,index=[0])
        company_info=company_info.loc[:,['證券代號','公司名稱','成立日期','董事長','總經理','實收資本額','已發行普通股數',
                                         '發言人','代理發言人','總機電話','傳真號碼','統一編號','公司網站','公司地址',
                                         '電子郵件','英文簡稱','英文全名','英文通訊地址','主要經營業務']]

        #google api 抓取經緯度
        company_info['geometry']=company_info['公司地址'].map(self.clean_address).map(self.address_to_geo_info)
        #記錄此證券代號，到時人為處理地址無法轉經緯度的錯誤
        if company_info.geometry.isnull().iloc[0]:
            pd.DataFrame({'證券代號':code},index=[1]).to_csv(error_path+'company_full_info.csv',
                                                         encoding='utf_8_sig',index=0,mode='a',header=0)
        #增加新資料到公司全資訊
        company_info['geometry']=company_info['geometry'].astype('str')

        company_info.to_sql('公司資訊',self.engine,index=False,if_exists='append')
        print(f'[新上市更新] 證券代號={code} | 已寫入「公司資訊」')
        
        #更新 地緣券商資訊
        B=pd.read_sql_query('select * from 全券商分行地址',self.engine)
        B['geometry']=B.geometry.map(literal_eval)

        company_info['geometry']=company_info['geometry'].map(literal_eval)

        #券商與各公司 距離
        R=[]
        code=company_info.證券代號.iloc[0]
        name=company_info.公司名稱.iloc[0]
        address=company_info.公司地址.iloc[0]
        c_lat=company_info.geometry.iloc[0][1]
        c_lng=company_info.geometry.iloc[0][0]

        for j in range(0,B.shape[0]):
            b_lat=B.geometry.iloc[j][1]
            b_lng=B.geometry.iloc[j][0]       
            d=np.round(self.haversine(c_lng,c_lat,b_lng,b_lat)/1000,decimals=3)

            R.append(pd.DataFrame({
                '證券代號':code,
                '公司名稱':name,
                '券商名稱':B.證券商名稱.iloc[j],
                '距離':d
            },index=[1])) 
        R=pd.concat(R,axis=0)

        R=R.drop_duplicates(['證券代號', '公司名稱', '券商名稱'])

        R.to_sql('地緣券商資訊',self.engine,index=0,if_exists='append')
        print(f'[新上市更新] 證券代號={code} | 已寫入「地緣券商資訊」')

        #主力買賣超、大戶比例、月營收資訊...皆是下次執行個人專屬程式時，會如期更新


class StockAllCodeInfoCollectProcess(StockCollectLogic):
    """
    收集上市上櫃全代碼表格
    """
    
    table_name='全證券代碼相關資料表'
    app_name='stock_all_code_info'
    
    
    def collect(self):
        #全上市證券代碼
        a=requests.get('https://isin.twse.com.tw/isin/C_public.jsp?strMode=2',timeout=self.timeout,
                      headers=self.headers)
        soup=BeautifulSoup(a.text,'lxml')
        tb=soup.find('table',class_='h4')
        temp=tb.find_all('tr')
        for i,s in enumerate(temp):
            try:
                if s.find('b').text == ' 股票  ':
                    st=i
                elif s.find('b').text == ' 上市認購(售)權證  ':
                    ed=i
                else:
                    continue
            except Exception:
                continue
        temp=temp[(st+1):ed]
        temp=[part_row for part_row in temp if len(part_row.find_all('td')) == 7] #排除錯誤的row
        col_name=[s.text for s in tb.find('tr',{'align':'center'}).find_all('td')]
        
        d1=pd.DataFrame([[each_value.text for each_value in part_row.find_all('td')] for part_row in temp],
                        columns=col_name)
        
        temp=[v.split('\u3000') for v in d1['有價證券代號及名稱 ']]
        d1=d1.drop(['有價證券代號及名稱 '],axis=1)
        d1['有價證券代號']=[v[0] for v in temp]
        d1['有價證券名稱']=[v[1] for v in temp]
        d1=d1.loc[:,['有價證券代號','有價證券名稱','國際證券辨識號碼(ISIN Code)','上市日','市場別','產業別','CFICode','備註']]
        
        #全上櫃證券代碼
        b=requests.get('https://isin.twse.com.tw/isin/C_public.jsp?strMode=4',timeout=self.timeout,
                        headers=self.headers)
        soup=BeautifulSoup(b.text,'lxml')
        tb=soup.find('table',class_='h4')
        temp=tb.find_all('tr')
        for i,s in enumerate(temp):
            try:
                if s.find('b').text == ' 股票  ':
                    st=i
                elif s.find('b').text == ' 特別股  ':
                    ed=i
                else:
                    continue
            except Exception:
                continue
        temp=tb.find_all('tr')[(st+1):ed]
        temp=[part_row for part_row in temp if len(part_row.find_all('td')) == 7] #排除錯誤的row
        col_name=[s.text for s in tb.find('tr',{'align':'center'}).find_all('td')]
        
        d2=pd.DataFrame([[each_value.text for each_value in part_row.find_all('td')] for part_row in temp],
                        columns=col_name)
        
        temp=[v.split('\u3000') for v in d2['有價證券代號及名稱 ']]
        d2=d2.drop(['有價證券代號及名稱 '],axis=1)
        d2['有價證券代號']=[v[0] for v in temp]
        d2['有價證券名稱']=[v[1] for v in temp]
        d2=d2.loc[:,['有價證券代號','有價證券名稱','國際證券辨識號碼(ISIN Code)','上市日','市場別','產業別','CFICode','備註']]
        #合併上市上櫃
        D=pd.concat([d1,d2],ignore_index=True).sort_values(['有價證券代號'])
        D=D[~D.duplicated()]
        D.有價證券代號=D.有價證券代號.map(self.clean_comma)
        D=D.rename(columns=self.union_column_name_dict)
        
        old=pd.read_sql_query(f'select * from {self.table_name}',self.engine)
        self.new_code=[s for s in D.證券代號 if s not in old.證券代號.tolist()]
        self.delete_code=[s for s in old.證券代號 if s not in D.證券代號.tolist()]

        print(f'[全證券代碼] 新增 {len(self.new_code)} 支: {self.new_code}')
        print(f'[全證券代碼] 下市/移除 {len(self.delete_code)} 支: {self.delete_code}')
        
        self.result=D
    
    def save(self):
        """
        全覆蓋資料
        """
        if len(self.result) > 0:
            #renew 全證券代碼相關資料表
            #step1: truncate table
            self.cursor.execute(f'TRUNCATE TABLE {self.table_name}')
            self.conn.commit()
            
            self.delete_count = -1
            #step2: insert new data
            self.result.to_sql(f'{self.table_name}',self.engine,index=0,if_exists='append')     
            
            self.insert_count = len(self.result)
            
    def process(self):
        super().process()
        ti=self.ti
        ti.xcom_push(key='new_code',value=self.new_code)
        ti.xcom_push(key='delete_code',value=self.delete_code)


class StockThreeMainInvestorCollectProcess(StockCollectLogic):
    """
    收集三大法人買賣超
    """
    table_name='三大法人買賣'
    app_name='stock_three_main_investor'
    
    #三大法人買賣超
    def convert_three_mainforce_data_into_sql_format(self,df,unique_pair):
        cdf=df.loc[:,['證券代號','證券名稱','資料日期']]
        
        numeric_cols=[col for col in df.columns if col not in ['資料日期','證券代號','證券名稱']]
        for s in numeric_cols:
            df[s]=df[s].apply(self.clean_comma,output_not_string=True)
        
        nan_info=df[numeric_cols].isnull().sum()
        nan_cols=nan_info[nan_info>0]
        if len(nan_cols)>0:
            print(f'[三大法人] 數值轉換含 NaN（來源缺值）| 欄位與筆數: {dict(nan_cols)}')
        
        cdf['外資買進股數']=df.loc[:,[s for s in df.columns if s.find('外') != -1 and s.find('買進') != -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        cdf['外資賣出股數']=df.loc[:,[s for s in df.columns if s.find('外') != -1 and s.find('賣出') != -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        cdf['外資買賣超股數']=df.loc[:,[s for s in df.columns if s.find('外') != -1 and s.find('買賣超') != -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)

        cdf['投信買進股數']=df.loc[:,[s for s in df.columns if s.find('投信') != -1 and s.find('買進') != -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        cdf['投信賣出股數']=df.loc[:,[s for s in df.columns if s.find('投信') != -1 and s.find('賣出') != -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        cdf['投信買賣超股數']=df.loc[:,[s for s in df.columns if s.find('投信') != -1 and s.find('買賣超') != -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        
        cdf['自營商買進股數']=df.loc[:,[s for s in df.columns if s.find('自營商') != -1 and s.find('買進') != -1 and s.find('外') == -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        cdf['自營商賣出股數']=df.loc[:,[s for s in df.columns if s.find('自營商') != -1 and s.find('賣出') != -1 and s.find('外') == -1]].apply(pd.to_numeric,errors='coerce').sum(axis=1)
        
        cdf['自營商買賣超股數']=pd.to_numeric(df['自營商買賣超股數'],errors='coerce')
        cdf['三大法人買賣超股數']=pd.to_numeric(df['三大法人買賣超股數'],errors='coerce')
        
        cdf=cdf[~cdf.duplicated(subset=unique_pair)]
        
        cdf['資料日期']=cdf['資料日期'].map(self.make_regular_date_format)
        
        cdf['證券代號']=cdf['證券代號'].map(str.strip)
        cdf['證券名稱']=cdf['證券名稱'].map(str.strip)
        
        
        return cdf
    
    def collect(self):
        #上市
        time_format=self.now_time.strftime('%Y%m%d')
        twse_url=f'https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={time_format}&selectType=ALL'
        print(f'[三大法人-上市] 請求日={time_format} | {twse_url}')

        a=requests.get(twse_url, timeout=30)
        a=a.json()
        #column name
        d1=pd.DataFrame(columns=a['fields'],data=a['data'])
        d1['資料日期']=self.now_time.strftime('%Y-%m-%d')
        print(f'[三大法人-上市] 回傳筆數={len(d1)} | 欄位={list(d1.columns)}')
        d1=self.convert_three_mainforce_data_into_sql_format(df=d1,unique_pair=['證券代號'])

        #上櫃
        time_format=self.now_time.strftime('%Y/%m/%d')
        n=re.search(string=time_format,pattern=r'^(\d+)/').group(1)
        temp_time_format=re.sub(string=time_format,pattern=n,
                    repl=str(int(n)-1911))
        print(f'[三大法人-上櫃] 請求日={temp_time_format}')
        
        response=requests.post('https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade',
            data={
                'type': 'Daily',
                'sect': 'AL',
                'date': temp_time_format,
                'response': 'json'
            }, timeout=20).json()
        
        #column name
        col_name=[
            '證券代號', '證券名稱', 
            '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)','外陸資買賣超股數(不含外資自營商)', 
            '外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數', 
            '外資及陸資買進股數','外資及陸資賣出股數','外資及陸資買賣超股數',
            '投信買進股數','投信賣出股數', '投信買賣超股數', 
            '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)','自營商買賣超股數(自行買賣)', 
            '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
            '自營商買進股數','自營商賣出股數','自營商買賣超股數', 
            '三大法人買賣超股數'
        ]
        
        d2=pd.DataFrame(response['tables'][0]['data'], columns = col_name)
        d2['資料日期']=time_format
        print(f'[三大法人-上櫃] 回傳筆數={len(d2)} | 欄位={list(d2.columns)}')

        #!!! 雷爆，外資及陸資買進股數=外陸資買進股數(不含外資自營商)+外資自營商買進股數
        if '外資及陸資買進股數' in d2.columns:
            d2.drop(['外資及陸資買進股數','外資及陸資賣出股數','外資及陸資買賣超股數'],axis=1,inplace=True)
            
        d2=self.convert_three_mainforce_data_into_sql_format(df=d2,unique_pair=['證券代號'])

        D=pd.concat([d1,d2],ignore_index=True)
        D=D[~D.duplicated()]
        D=D.sort_values(['證券代號'])
        print(f'[三大法人] 上市+上櫃合併後筆數={len(D)}')
        self.result=D

    def save(self):
        """
        delete insert
        """
        if len(self.result)>0:
            self.delete_count,self.insert_count=self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)


class StockAllCodeDailyDealInfoCollectProcess(StockCollectLogic):
    """
    收集個股成交資訊
    """
    table_name='個股成交資訊'
    numeric_col=['成交量','開盤','最高','最低','收盤']
    app_name='stock_daily_deal'
    
    def __init__(self,_type):
        super().__init__()
        self._type=_type
        self.log_record.app_name=self.log_record.app_name+f'-{_type}'
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]
        self.all_code_info=pd.read_sql_query(
            'select 證券代號, 市場別 from 全證券代碼相關資料表', self.engine
        )
        self.http_session = requests.Session()
        self.http_session.headers.update(self.headers)
        if self._type == 'tpex':
            self._diff_table_name='股票模板_diff2'
    
    def collect(self):
        #truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()
        
        if self._type == 'twse':
            self.collect_twse()
        elif self._type == 'tpex':
            self.collect_tpex()
    
    def twse_crawler(self, time_format, code):
        url=f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={time_format}&stockNo={code}'
        a=self.http_session.get(url,timeout=30)
        a=a.json()

        #column name
        d1=pd.DataFrame(columns=a['fields'],data=a['data'])
        d1['證券代號']=str(code)
        d1=d1.rename(columns=self.union_column_name_dict)
        
        d1=d1.loc[:,['證券代號','資料日期','開盤價','最高價','最低價','收盤價','成交股數']]
        d1.columns=['證券代號','資料日期','開盤','最高','最低','收盤','成交量']
        d1['成交量']=d1['成交量'].apply(self.clean_comma,output_not_string=True)
        d1['成交量']=d1['成交量']/1000
        d1=d1[~d1.duplicated(subset=['證券代號','資料日期'])]
        d1=d1.loc[:,['證券代號','資料日期','開盤','最高','最低','收盤','成交量']]
        d1['資料日期']=d1['資料日期'].map(self.make_regular_date_format)
        for col in self.numeric_col:
            d1[col]=d1[col].apply(self.clean_comma,output_not_string=True) 
        return d1
        
    def collect_twse(self):
        """
        證交所個股成交資訊
        """
        twse_code=self.all_code_info[self.all_code_info.市場別 == '上市'].證券代號.tolist()
        time_format=self.really_date.strftime('%Y%m%d')
        
        ER=[]
        twse=[]

        #為了讓while能運作
        for code in twse_code:
            try:
                d1=self.twse_crawler(time_format=time_format,code=code)
                twse.append(d1)
                time.sleep(random.randint(32,42)/10)
            except Exception as e:
                time.sleep(random.randint(32,42)/10)
                print(f'[個股成交-上市] 證券代號={code} | 首輪失敗 | {type(e).__name__}: {e}')
                ER.append(code)
        
        E=len(ER)
        accurate_code=[]
        N=0
        er={}
        
        for code in ER:
            er[code]=1
            
        while E != 0:
            try:
                for code in ER:
                    d1=self.twse_crawler(time_format=time_format,code=code)
                    twse.append(d1)
                    accurate_code.append(code)
                    time.sleep(random.randint(32,42)/10)
                E=0
            except Exception as e:
                print(f'[個股成交-上市] 重試批次例外 | 目前代號={code} | {type(e).__name__}: {e}')
                time.sleep(random.randint(32,42)/10)
                N+=1
                er[code]=er[code]+1
                print(f'[個股成交-上市] 重試計次 | 輪次={N} | 各代號重試次數={er}')
                if er[code]>2:
                    if code in ER:
                        ER.remove(code)
                    er.pop(code,None)
                if N>20:
                    break
                ER=list(set(ER)-set(accurate_code))
                E=len(ER)
        
        self.result=twse

    def tpex_crawler(self, time_format, code):
        col_name=['資料日期','成交量','成交金額','開盤','最高','最低','收盤','漲跌','成交筆數']
        
        url='https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock'
        response=self.http_session.post(url, timeout=20, data={
                        'code': code,
                        'date': time_format,
                        'response': 'json'
                    }).json()
        
        data=response['tables'][0]['data']
        d2=pd.DataFrame(data, columns=col_name)
        d2['資料日期']=[re.sub(string=s,pattern='＊',repl='') for s in d2['資料日期']]
        
        for col in self.numeric_col:
            d2[col]=d2[col].apply(self.clean_comma, output_not_string=True)

        d2['證券代號']=str(code).strip()
        d2=d2.loc[:,['證券代號', '資料日期', '開盤', '最高', '最低', '收盤','成交量']]
        d2['資料日期']=d2['資料日期'].map(self.make_regular_date_format)
        d2=d2.sort_values(['資料日期'],ascending=False)
        return d2
    
    def collect_tpex(self):
        """
        櫃買中心個股成交資訊
        """
        tpex_code=self.all_code_info[self.all_code_info.市場別 == '上櫃'].證券代號.tolist()
        time_format=self.really_date.strftime('%Y/%m/%d')
        
        #更新櫃買中心
        tpex=[]

        for code in tpex_code:
            try:
                d2=self.tpex_crawler(time_format, code)
                tpex.append(d2) #採取delete insert，先暫存insert資料
            except Exception as e:
                print(f'[個股成交-上櫃] 證券代號={code} | 失敗 | {type(e).__name__}: {e}')
                time.sleep(3)
            time.sleep(1.5)
        
        self.result=tpex
    
    def save(self):
        if len(self.result)>0:
            result=pd.concat(self.result).reset_index(drop=True)
            result=result.replace('--',np.nan)
            self.delete_count,self.insert_count=self.replace_by_keys(result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)


class StockBrokerInOutTPEXTriggerInsertProcess(StockCollectLogic):
    """
    收集櫃買市場 券商分點進出 --- 無法實現
    使用資料庫交流方式，由外面環境執行，此程序是insert trigger進資料庫
    """
    table_name='券商進出_trigger'
    _diff_table_name=f'{table_name}_diff'
    app_name='stock_tpex_broker_inout_trigger'
        
    def collect(self):
        pass
    
    def save(self):
        result=pd.DataFrame({
            '市場別':'tpex',
            'trigger_bool':1
        },index=[0])
        
        self.delete_count,self.insert_count=self.replace_by_keys(result,self.table_name,self._diff_table_name,
            ['市場別','資料日期'],['市場別'])


class StockAllCodeStandardMetricCollectProcess(StockCollectLogic):
    """
    上市上櫃公司當日股價換算本益比、殖利率、淨值比表格(個股基本指標)
    """
    table_name='個股基本指標'
    _diff_table_name='股票模板_diff2'
    app_name='stock_standard_metric'
    
    def __init__(self):
        super().__init__()
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]
            
    def collect(self):
        time_format=self.really_date.strftime('%Y%m%d')
        #上市
        response=requests.get(f'https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=json&date={time_format}&selectType=ALL',
                       timeout=30).json()
        #column name
        d1=pd.DataFrame(columns=response['fields'],data=response['data'])
        
        #上櫃本益比
        time_format=self.really_date.strftime('%Y/%m/%d')

        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/peQryDate'
        response=requests.post(url, data={
            'date': time_format,
            'response': 'json'
        },timeout=30).json()
        data=response['tables'][0]['data']

        #column name
        col_name=['證券代號','證券名稱','本益比','每股股利','股利年度','殖利率(%)','股價淨值比', '財報年/季']
        d2=pd.DataFrame(data, columns=col_name)
        d2=d2.loc[:,['證券代號','證券名稱','殖利率(%)','股利年度','本益比','股價淨值比','財報年/季']]
        d2['本益比']=['-' if s == 'N/A' else s for s in d2.本益比]
        d2['證券代號']=d2.證券代號.astype('str')

        if '股利年度' not in d1.columns:
            d1['股利年度']=d2.股利年度.iloc[0]

        if '收盤價' in d1.columns:
            d1=d1.drop(['收盤價'],axis=1)
        
        assert d1.shape[1] == d2.shape[1]
        D=pd.concat([d2,d1])
        D=D[~D.duplicated()]
        D=D.sort_values(['證券代號'])
        D=D[D['股價淨值比'] != 'N/A']
        D=D[D['本益比'] != 'N/A']
        D['本益比']=D['本益比'].apply(self.clean_comma,output_not_string=True)
        D=D.replace('-', None)
        D['資料日期']=self.really_date.strftime('%Y-%m-%d')
        
        self.result=D
    
    def save(self):
        self.delete_count,self.insert_count=self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
            ['證券代號','資料日期'],self._select_columns)


class StockDayTradeCollectProcess(StockCollectLogic):
    """
    上市上櫃當沖標的(最晚更新，都要晚上8點後才更新)
    """
    table_name='當沖資訊'
    _diff_table_name='股票模板_diff2'
    app_name='stock_day_trade'
    
    def __init__(self):
        super().__init__()
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]
            
    def collect(self):
        time_format=self.really_date.strftime('%Y%m%d')
        #上市
        a=requests.get('https://www.twse.com.tw/exchangeReport/TWTB4U?response=csv&date='+time_format+'&selectType=All',
                       timeout=30)
        s=a.text
        p=s.find('證券代號')
        s=s[p:]
        q=s.find('"備註:"')
        s=s[:q]
        s=re.sub(string=s,pattern=r'=|\r+',repl='')
        
        d1=pd.read_csv(StringIO(s))
        d1['資料日期']=self.really_date.strftime('%Y-%m-%d')
        select_column=['資料日期','證券代號', '證券名稱',
                       '當日沖銷交易成交股數', '當日沖銷交易買進成交金額',
                       '當日沖銷交易賣出成交金額']
        d1.columns=[s.replace("'","").replace('"','').strip() for s in d1.columns]
        d1=d1.loc[:,select_column]
        
        #上櫃
        time_format=self.really_date.strftime('%Y/%m/%d')
        print(f'[當沖資訊-上櫃] 請求日參數={time_format}')
        url = 'https://www.tpex.org.tw/www/zh-tw/intraday/stat'
        col_name = ['證券代號','證券名稱','temp','當日沖銷交易成交股數', '當日沖銷交易買進成交金額','當日沖銷交易賣出成交金額']

        response=requests.post(url, data={'type': 'Daily', 'response': 'json', 'date': time_format}, timeout=30).json()
        data=response['tables'][1]['data']
        d2=pd.DataFrame(data, columns=col_name)
        d2.drop(['temp'], axis=1, inplace=True)
        d2['資料日期']=self.really_date.strftime('%Y-%m-%d')
        D=pd.concat([d1,d2])
        
        if len(D)>0:
            for col in ['當日沖銷交易成交股數', '當日沖銷交易買進成交金額','當日沖銷交易賣出成交金額']:
                D[col]=D[col].apply(self.clean_comma,output_not_string=True)
            D=D[~D.duplicated(subset=['資料日期','證券代號'])]
            D=D.rename(columns=self.union_column_name_dict)
            D['資料日期']=D['資料日期'].map(self.make_regular_date_format)
        
        self.result=D
    
    def save(self):
        if len(self.result)>0:
            self.delete_count,self.insert_count=self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)
            
    def process(self):
        """
        主要的資料流程
        當沖資訊都很晚匯入資料，決定設定retry機制
        """
        retry_time=4*6
        wailt_time=15*60
        count=0
        
        while True:
            try:
                self.collect()
                break
            except Exception as e:
                print(f'[當沖資訊] collect 失敗（將 sleep 重試）| {type(e).__name__}: {e}')
                
                if count >= retry_time:
                    break
                count+=1    
                time.sleep(wailt_time)
                super().__init__()
        
        try:
            if count >= retry_time:
                raise AssertionError('當沖資訊收集失敗')
            self.save()
            self.log_process_record()
            
            self.log_record.success = 1
            print('[當沖資訊] 成功 | 已寫入 DB')
        except Exception as e:
            self.log_record.raise_error(repr(e))
            print(f'[當沖資訊] 失敗 | {type(e).__name__}: {e}')
            raise e
        finally:
            self.log_record.insert_to_log_record()
            self.end()


BSR_CAPTCHA_FAILED='驗證碼錯誤'
BSR_NO_DATA='沒有資料，請檢查'
BSR_PARSE_FAILED='解析失敗'
BSR_BLOCKED='疑似被封鎖'
BSR_BLOCK_MARKERS=('Access is denied','拒絕存取')  # IIS 對被封鎖的來源回的 403 頁面文字
BSR_MIN_VISIBLE_CHARS=200  # the real result page is ~15,000 characters


def order_by_volume(codes,volume_by_code):
    """成交量大的排前面（沒有成交量的排最後，同量維持原順序）。網站只保留最新一天且常收不完，先收重要的股票。"""
    def volume(code):
        value=volume_by_code.get(code)
        try:
            return -float(value) if value == value and value is not None else 0.0
        except (TypeError,ValueError):
            return 0.0
    return sorted(codes,key=volume)


def is_captcha_image(resp):
    """驗證碼圖片請求是否真的拿到圖片（網站常回 404 的 HTML 錯誤頁，過去被當成圖片丟進影像處理而出錯）。"""
    content=getattr(resp,'content',b'') or b''
    ctype=str((getattr(resp,'headers',None) or {}).get('content-type','')).lower()
    is_image=content[:2]==b'\xff\xd8' or content[:4]==b'\x89PNG' or content[:3]==b'GIF'  # JPEG / PNG / GIF
    return getattr(resp,'status_code',200)==200 and 'html' not in ctype and is_image


def is_bsr_menu_rejected(text):
    """
    證交所 bsMenu 送出驗證碼後的回應是否代表「驗證碼沒過」。

    舊版網站答錯會回「驗證碼錯誤!」；現在改成回一個幾乎空白的頁面（約 236 bytes，只有 BOM），
    接著 bsContent 回 403「禁止直接存取此頁面」。只比對舊字串會把答錯的驗證碼
    當成「沒有資料」，既不重試也不記錄，於是約六成的股票每輪都被悄悄丟掉。
    """
    visible=(text or '').replace('﻿','').strip()
    return '驗證碼錯誤' in visible or len(visible) < BSR_MIN_VISIBLE_CHARS


class StockBrokerInOutTWSECollectProcess(StockCollectLogic):
    """
    證交所券商進出收集
    """
    table_name='券商進出總表'
    _diff_table_name='券商進出總表_diff'
    _select_columns=['證券代號','資料日期','券商名稱']
    domain_url='https://bsr.twse.com.tw/'
    allowedChars = '0123456789ABCDEFGHJKLMNPQRSTUVWXYZ'
    # 驗證碼 CNN 單次正確率約 35%，所以每檔要重試；每檔最多 12 次時全部失敗的機率約 0.6%
    MAX_ATTEMPTS_PER_CODE=12
    CAPTCHA_FETCH_TRIES=8  # 驗證碼圖片 404 的機率約 6 成，8 次都失敗的機率不到 2%
    # 連續這麼多次嘗試都失敗（35% 正確率下機率趨近 0）代表網站封鎖或改版：先暫停再試，
    # 暫停幾次仍不行就中止，不要再空轉好幾個小時。
    # 連續 30 次嘗試 0 次成功就判定被封鎖：驗證碼單次正確率約 4 成，全部答錯的機率 0.6^30≈0.00000022。
    BLOCK_STREAK_LIMIT=30
    BLOCK_PAUSE_SECONDS=600  # 第 k 次暫停 = 600 x 2^(k-1) 秒（10、20、40 分鐘）
    BLOCK_MAX_PAUSES=3
    # 暫停次數是「連續」的概念：暫停後連續成功這麼多檔，代表網站已恢復，暫停次數歸零，
    # 否則整天偶發幾次斷線就會累積到上限而中止整個 run。
    PAUSE_RESET_AFTER_OK=10
    CONN_FAIL_WEIGHT=3  # 連線被切斷算幾次失敗（連續 10 次斷線就達 BLOCK_STREAK_LIMIT 暫停）
    # 請求節奏：每次嘗試之間隨機等待，避免被當成機器人而限流。連續被拒絕時（答對 4 成，連續 8 次被拒絕的機率只有 1.7%）
    # 先逐步放慢，比等到整整 30 次才暫停更溫和。
    PACE_SECONDS=(2.5,5.0)
    SOFT_BACKOFF_STREAK=8
    SOFT_BACKOFF_STEP=5   # 超過門檻後每多失敗一次多等 5 秒
    SOFT_BACKOFF_MAX=90
    PROGRESS_EVERY=25  # 每這麼多次嘗試印一行成功率
    # 收集中每累積這麼多檔就先寫入 DB（被中斷最多損失一批；重跑時已寫入的代號會被排除，等於斷點續跑）。
    BATCH_SIZE=40
    FLUSH_BACKOFF_BASE=15  # 寫入失敗後重試前的等待：15、30、60… 秒（指數），上限 FLUSH_BACKOFF_MAX
    FLUSH_BACKOFF_MAX=300
    FLUSH_FAIL_LIMIT=3  # 連續這麼多次分批寫入「一檔都寫不進去」就中止，不要在 DB 掛掉時空轉好幾個小時
    # 最後還要寫入剩餘的一批，所以必須在 Airflow 的 6 小時 execution_timeout 之前自己停下來
    MAX_RUNTIME_SECONDS=5*3600
    
    img_path=img_path
    processed_img_path=processed_img_path
    model_path=model_path
    app_name='stock_twse_broker_inout'
    
    def __init__(self):
        super().__init__()
        self.really_date=pd.read_sql_query(self.really_date_sql,self.engine).iloc[0,0]
        self.today=self.really_date.strftime('%Y-%m-%d')
        self.headers['Referer']=self.domain_url
        
        all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
        twse_code=all_code_info[all_code_info.市場別 == '上市'].證券代號.tolist()
        temp=pd.read_sql_query(f"select 證券代號 from 券商進出總表 where 資料日期 = '{self.today}'",self.engine)
        self.twse_code=[code for code in twse_code if code not in temp.證券代號.unique()]
        self.already_collected=len(twse_code)-len(self.twse_code)  # 本次之前（含第一輪）已經寫入的檔數
        try:
            volume=pd.read_sql_query(f"select 證券代號, 成交量 from 個股成交資訊 where 資料日期 = '{self.today}'",self.engine)
            self.twse_code=order_by_volume(self.twse_code,dict(zip(volume.證券代號,volume.成交量)))
            print(f'[券商進出-證交所] 已依當日成交量排序（有量的 {len(volume)} 檔），成交量大的先收')
        except Exception as e:
            print(f'[券商進出-證交所] 成交量排序失敗，維持原順序 | {type(e).__name__}: {e}')
        print(f'[券商進出-證交所] 待抓上市代號數={len(self.twse_code)}（已排除當日已有資料）')
        
        broker=pd.read_sql_query('select * from 全券商分行地址',self.engine)
        broker_dict={}
        for index,row in broker.iterrows():
            broker_dict[row['證券商代號']]=row['證券商名稱']     
        self.broker_dict=broker_dict
        
        print('[券商進出-證交所] 載入驗證碼 CNN 模型…')
        import tensorflow as tf
        from stock.utilities_tf2 import preprocessing, one_hot_decoding
        self._captcha_preprocessing=preprocessing
        self._captcha_one_hot_decoding=one_hot_decoding
        self.model = tf.keras.models.load_model(self.model_path)
        print('[券商進出-證交所] 驗證碼模型載入完成')

    #整併證交所商買賣的資訊
    def summarize(self,dat):
        for var in ['買進股數','賣出股數','價格']:
            dat[var]=dat[var].astype('float')
        buy=dat.買進股數.sum()/1000
        sell=dat.賣出股數.sum()/1000
        inout=buy-sell
        if buy == 0:
            buy_price=0
        else:
            buy_price=(dat.買進股數*dat.價格).sum()/(1000*buy)
        if sell == 0:
            sell_price=0
        else:
            sell_price=(dat.賣出股數*dat.價格).sum()/(1000*sell)
        mean_price=(buy*buy_price+sell*sell_price).sum()/(buy+sell)
        typ='分點'
        
        return pd.DataFrame({
            '日期':dat.日期.iloc[0],
            '證券代號':dat.證券代號.iloc[0],
            '券商代號':dat.券商代號.iloc[0],
            '券商名稱':dat.券商名稱.iloc[0],
            '買量':np.round(buy,2),
            '買價':np.round(buy_price,2),
            '賣量':np.round(sell,2),
            '賣價':np.round(sell_price,2),
            '買賣超':np.round(inout,2),
            '均價':np.round(mean_price,2),
            '資料類型':typ,
        },index=[0])

    def parse_and_clean_table(self,soup,code):
        date_time=soup.find('td',{'id':'receive_date'}).text.strip().replace('/','-')    
        col_name=[s.text.strip() for s in soup.find(class_='column_title_1').find_all('td')]
        T=[]

        for table in soup.find_all(class_='column_title_1'):
            table=table.find_previous('table')
            temp=pd.DataFrame([[re.sub(string=sub_part.text,pattern=r'^\s+|\s+$|,|　',repl='') for sub_part in s.find_all('td')] for s in table.find_all('tr')[1:]],
                        columns=col_name)
            temp=temp[temp['序'].map(str.isdigit)]
            temp['序']=temp['序'].astype('int')
            T.append(temp)    
        
        T=pd.concat(T).sort_values(['序'])
        T=T.drop(['序'],axis=1)

        c=[]
        name=[]
        for s in T.證券商:
            t=s.split(' ')
            if len(t) == 2:
                c.append(t[0])
                name.append(t[1])
            else:
                c.append(t[0])
                name.append(None)

        T['券商代號']=c
        T['券商名稱']=name
        T.券商名稱=T.券商名稱.fillna(method='ffill')
        T['證券代號']=code
        T['日期']=date_time
        T=T.rename(columns={'成交單價':'價格'})
        T=T.loc[:,['日期','證券代號','券商代號','券商名稱', '價格', '買進股數', '賣出股數']]
        T=T.groupby(['券商代號']).apply(self.summarize).reset_index(drop=True)    
        T=T.sort_values(['買賣超'],ascending=False)
        
        return T
        
    #證交所-券商分點
    def geo_broker_twse(self,code):
        with requests.Session() as session:
            return self._geo_broker_twse_with_session(session, code)

    @staticmethod
    def _min_confidence(prediction):
        """5 個字元中最低的 softmax 機率（一個字不確定整組就不可靠）；算不出來回 None。用來判斷被拒絕是辨識錯還是網站端的問題。"""
        try:
            return float(min(np.max(np.asarray(p),axis=-1).min() for p in prediction))
        except Exception:
            return None

    def _archive_captcha(self,image_bytes,predicted,accepted,confidence=None):
        """
        留存驗證碼圖片，之後可用來重新訓練 / 評估 CNN：
        accepted/ 網站接受了這個答案，所以檔名的預測值＝正確標籤（可直接當訓練資料）；
        rejected/ 答案被拒絕，標籤未知（可能是答錯，也可能是被限流），需人工標註才能用。
        """
        try:
            folder=os.path.join(os.path.dirname(img_path),'captcha_dataset','accepted' if accepted else 'rejected')
            os.makedirs(folder,exist_ok=True)
            tag='' if confidence is None else f'_c{confidence:.2f}'
            with open(os.path.join(folder,f'{predicted}{tag}_{int(time.time()*1000)}.jpg'),'wb') as f:
                f.write(image_bytes)
        except Exception as e:
            print(f'[券商進出-證交所] 驗證碼圖片留存失敗（忽略）| {type(e).__name__}: {e}')

    def _geo_broker_twse_with_session(self, session, code):
        #去證交所下載驗證碼（網站會有約 6 成的機率對驗證碼圖回 404 錯誤頁，所以要換一組新的重抓，沒抓到圖就不送出辨識）
        soup=None
        for _ in range(self.CAPTCHA_FETCH_TRIES):
            response=session.get('https://bsr.twse.com.tw/bshtm/bsMenu.aspx',headers=self.headers,
                        timeout=60,verify=False)
            page=BeautifulSoup(response.text,'lxml')
            srcs=[s.get('src') for s in page.find_all('img') if bool(re.search(string=str(s),pattern=r'CaptchaImage'))]
            if not srcs:
                session.close()
                session.cookies.clear()
                self._sleep(0.5)
                continue
            resp=session.get('https://bsr.twse.com.tw/bshtm/'+srcs[0],timeout=60)
            if is_captcha_image(resp):
                soup=page
                break
            # 圖片 404 是「綁在這個 session 上」的：同一個 session 一旦拿到壞的就會一直是壞的（實測 8/8 都失敗），
            # 換新 session（清 cookie、斷開連線）後才會恢復，所以不能只重新請求選單頁。
            session.close()
            session.cookies.clear()
            self._sleep(0.5)
        if soup is None:
            print(f'[券商進出-證交所] 連續 {self.CAPTCHA_FETCH_TRIES} 次都沒有取得驗證碼圖片 | 將重試')
            return BSR_CAPTCHA_FAILED
        os.makedirs(os.path.dirname(img_path), exist_ok=True)  # temp dir is untracked runtime scratch
        image_bytes=resp.content
        with open(img_path, 'wb') as f:
            f.write(image_bytes)

        #破解驗證碼
        self._captcha_preprocessing(img_path, self.processed_img_path)
        img = cv2.imread(self.processed_img_path)
        if img is None:
            # 下載到的不是圖片（例如網站回錯誤頁）；過去這裡會丟 cvtColor 例外，現在視為可重試的失敗
            print('[券商進出-證交所] 驗證碼圖片無法讀取 | 將重試')
            return BSR_CAPTCHA_FAILED
        img = img / 255.0  # 归一化
        img = np.expand_dims(img, axis=0)  # 添加批次维度
        
        prediction = self.model.predict(img)
        predict_captcha = self._captcha_one_hot_decoding(prediction, self.allowedChars)
        confidence = self._min_confidence(prediction)
        conf_text = '' if confidence is None else f' | 信心={confidence:.2f}'

        payload = {}
        acceptable_input=['__EVENTTARGET','__EVENTARGUMENT','__LASTFOCUS','__VIEWSTATE',
                        '__VIEWSTATEGENERATOR','__EVENTVALIDATION']

        inputs = soup.find_all("input")
        for elem in inputs:
            # 網站在 2026-11 新增了必填的隱藏欄位 __VIEWSTATEENCRYPTED；漏帶時答案一律被拒絕（403），
            # 所以改成帶上表單裡所有隱藏欄位，之後再新增隱藏欄位也不會壞。
            if elem.get("name") in acceptable_input or (elem.get("type") == "hidden" and elem.get("name")):
                if elem.get("value") is not None:
                    payload[elem.get("name")] = elem.get("value")
                else:
                    payload[elem.get("name")] = ""
        payload['TextBox_Stkno']=code
        payload['CaptchaControl1']=predict_captcha
        payload['btnOK']='查詢'
        payload['RadioButton_Normal']='RadioButton_Normal'
        
        #呼叫頁面，讓證交所知道我可以下載這個證券代號的資料
        resp=session.post('https://bsr.twse.com.tw/bshtm/bsMenu.aspx',data=payload,
                    headers=self.headers,timeout=60,verify=False,)
        
        if is_bsr_menu_rejected(resp.text) or 'bsContent.aspx' not in resp.text:
            # 答案被接受時，回應頁會多出「下載 xxxx CSV」連結（含 bsContent.aspx）；答錯時回的是空白頁
            print(f'[券商進出-證交所][答案被拒絕] 代號={code} | 預測={predict_captcha!r}{conf_text} | 回應頁沒有下載連結（驗證碼答錯）')
            self._archive_captcha(image_bytes,predict_captcha,False,confidence)
            return BSR_CAPTCHA_FAILED
        #呼叫 html格式的網頁表（v=Display 才是含交易日期與明細表的 HTML；不帶參數或 v=t 現在回的是 CSV）
        a=session.get('https://bsr.twse.com.tw/bshtm/bsContent.aspx?v=Display',timeout=60,verify=False)
        if a.status_code != 200:
            if any(m in a.text for m in BSR_BLOCK_MARKERS) or a.status_code in (429,503):
                print(f'[券商進出-證交所][疑似被封鎖] 代號={code} | 結果頁 HTTP {a.status_code} 且內容為拒絕存取 | 立即暫停')
                return BSR_BLOCKED
            print(f'[券商進出-證交所] 證券代號={code} | 驗證碼已通過但結果頁 HTTP {a.status_code} | 將重試')
            return BSR_CAPTCHA_FAILED
        soup=BeautifulSoup(a.text,'lxml')

        if soup.find('td',{'id':'receive_date'}) is None:
            # 驗證碼已經通過（回應頁有下載連結），結果頁卻是空白：這檔沒有分點資料（例如當天沒成交），重試也不會有
            print(f'[券商進出-證交所] 證券代號={code} | 驗證碼已通過，但結果頁是空白（沒有分點資料）')
            return BSR_NO_DATA
        if soup.find(class_='column_title_1') is None:
            # 是正常的結果頁但沒有任何成交明細，重試也不會有資料
            print(f'[券商進出-證交所] 證券代號={code} | 結果頁沒有成交明細')
            return BSR_NO_DATA

        # 走到這裡表示網站接受了這個答案，預測值就是正確標籤
        print(f'[券商進出-證交所][答案通過] 代號={code} | 預測={predict_captcha!r}{conf_text}')
        self._archive_captcha(image_bytes,predict_captcha,True,confidence)
        try:
            T=self.parse_and_clean_table(soup,code=code)
        except Exception as e:
            print(f'[券商進出-證交所] 證券代號={code} | 解析頁面失敗 | {type(e).__name__}: {e}')
            return BSR_PARSE_FAILED

        return T

    @staticmethod
    def fill_branch_names(result,broker_dict):
        """分點名稱：對得到就用 全券商分行地址 的名稱，否則用券商代號；回傳 (名稱 Series, 對不到的代號清單)。"""
        mapped=result['券商代號'].map(broker_dict)
        unmapped=sorted(result.loc[mapped.isna(),'券商代號'].unique())
        names=mapped.where(mapped.notna(),result['券商代號'])
        return StockBrokerInOutTWSECollectProcess.disambiguate_names(result,names),unmapped

    @staticmethod
    def disambiguate_names(result,names):
        """
        唯一鍵含券商名稱，而 MySQL 預設定序不分大小寫：同一檔股票裡，不同券商代號（例如 9B2Q 與 9B2q，
        對不到名稱時名稱就是代號）若名稱只差大小寫就會撞鍵，整批寫入失敗且重試也一樣失敗。
        撞名的券商，代號排序在前的保留原名，其餘改成「名稱[代號]」，兩家都保留、名稱穩定。
        """
        names=names.astype(str)
        frame=pd.DataFrame({'stock':result['證券代號'] if '證券代號' in result.columns else '',
                            'code':result['券商代號'],'name':names,'key':names.str.casefold()})
        pairs=frame.drop_duplicates(['stock','code'])
        clash=pairs.groupby(['stock','key'])['code'].nunique()
        clash=clash[clash > 1]
        if clash.empty:
            return names
        rename={}
        for (stock,key),_ in clash.items():
            group=pairs[(pairs['stock'] == stock) & (pairs['key'] == key)].sort_values('code')
            for code,name in list(zip(group['code'],group['name']))[1:]:
                rename[(stock,code)]=f'{name}[{code}]'
        fixed=[rename.get((s,c),n) for s,c,n in zip(frame['stock'],frame['code'],frame['name'])]
        return pd.Series(fixed,index=names.index)

    def _sleep(self,seconds):
        time.sleep(seconds)

    def _pace(self,streak):
        """兩次嘗試之間的等待：基本間隔加上隨機；連續被拒絕越多次就越慢（溫和降速，不等到硬暫停）。"""
        low,high=self.PACE_SECONDS
        delay=random.uniform(low,high)
        if streak > self.SOFT_BACKOFF_STREAK:
            delay+=min(self.SOFT_BACKOFF_MAX,(streak-self.SOFT_BACKOFF_STREAK)*self.SOFT_BACKOFF_STEP)
        self._sleep(delay)
        return delay

    def _attempt(self,code):
        """
        對單一股票做一次完整的查詢流程，回傳 (狀態, 內容)：
        ok（內容是 DataFrame）／no_data（正常結果頁但沒有成交明細，不必重試）／retry（內容是原因）。
        """
        try:
            t=self.geo_broker_twse(code=code)
        except (requests.exceptions.SSLError,requests.exceptions.ConnectionError,requests.exceptions.Timeout) as e:
            # 連線被切斷 / 逾時：可能是限流前兆，也可能只是偶發斷線，單次不能斷定封鎖；
            # 回傳 'conn'，由 collect 累積連續失敗（加權）後才決定要不要暫停。
            print(f'[券商進出-證交所][連線中斷] 代號={code} | {type(e).__name__}')
            self._sleep(random.randint(11,25)/10)
            return 'conn',f'{type(e).__name__}'
        except Exception as e:
            print(f'[券商進出-證交所] 證券代號={code} | 例外 | {type(e).__name__}: {e}')
            self._sleep(random.randint(11,25)/10)
            return 'retry',f'{type(e).__name__}'
        if isinstance(t,pd.DataFrame):
            return 'ok',t
        if t == BSR_NO_DATA:
            return 'no_data',None
        if t == BSR_BLOCKED:
            return 'blocked',str(t)
        return 'retry',str(t)

    def collect(self):
        """
        逐檔查詢；每檔最多重試 MAX_ATTEMPTS_PER_CODE 次。

        舊版的問題：答錯驗證碼被誤判成「沒有資料」，放進從不重試的清單，而且失敗的股票
        完全沒有紀錄，導致每輪只有約三到四成的股票有資料，整天失敗時也毫無警訊。
        現在每一檔的結果都會被分類並統計，最後留下 collect_summary 讓呼叫端通知。
        """
        started=time.monotonic()
        targets=list(self.twse_code)
        result,no_data,given_up=[],[],[]
        self.pending=[]  # 已收到、還沒寫進 DB 的結果
        self.rejected=[]  # 寫入時違反唯一鍵、重試也沒用的結果
        self._saved_delete=self._saved_insert=0
        self._flush_failures=0
        since_flush=0  # 上次嘗試寫入之後新收到的檔數（不論成功或失敗都歸零，避免失敗後每收一檔就重寫一次）
        streak=pauses=attempts=ok_since_pause=0
        aborted=''
        position=0

        while position < len(targets) and not aborted:
            code=targets[position]
            if self._flush_failures >= self.FLUSH_FAIL_LIMIT:
                aborted=f'連續 {self._flush_failures} 次分批寫入 DB 都失敗'
                break
            status='retry'
            for attempt in range(1,self.MAX_ATTEMPTS_PER_CODE+1):
                # 每次嘗試前都檢查時限（單檔最壞可能要 10 幾分鐘 x 12 次，不能只在檔與檔之間檢查）
                if time.monotonic()-started > self.MAX_RUNTIME_SECONDS:
                    aborted=f'超過最長執行時間 {self.MAX_RUNTIME_SECONDS/3600:g} 小時'
                    break
                if attempts > 0:
                    self._pace(streak)  # 連續快速重試時網站會直接斷線，所以每次嘗試之間都要等
                attempts+=1
                kind,payload=self._attempt(code)
                if kind == 'ok':
                    result.append(payload)
                    self.pending.append(payload)
                    status='ok'
                    streak=0
                    ok_since_pause+=1
                    if pauses and ok_since_pause >= self.PAUSE_RESET_AFTER_OK:
                        print(f'[券商進出-證交所] 已連續成功 {ok_since_pause} 檔，暫停次數 {pauses} 歸零')
                        pauses=0
                    print(f'[券商進出-證交所] 證券代號={code} | 完成 | 第 {attempt} 次嘗試')
                    since_flush+=1
                    if since_flush >= self.BATCH_SIZE:
                        since_flush=0
                        self._flush_pending()
                    break
                if kind == 'no_data':
                    no_data.append(code)
                    status='no_data'
                    streak=0
                    break
                if kind == 'blocked':
                    streak=self.BLOCK_STREAK_LIMIT  # 明確的封鎖訊號：立刻暫停，不必累積失敗次數
                elif kind == 'conn':
                    streak+=self.CONN_FAIL_WEIGHT  # 斷線比答錯驗證碼更像限流，加重但不直接暫停
                elif payload != BSR_PARSE_FAILED:  # 解析失敗是改版而不是封鎖，不算進封鎖偵測
                    streak+=1
                if attempts % self.PROGRESS_EVERY == 0:
                    print(f'[券商進出-證交所][進度] 嘗試 {attempts} 次 | 成功 {len(result)} 檔 | '
                          f'放棄 {len(given_up)} 檔 | 目前連續失敗 {streak} 次')
                if streak >= self.BLOCK_STREAK_LIMIT:
                    pauses+=1
                    if pauses > self.BLOCK_MAX_PAUSES:
                        aborted=(f'連續 {self.BLOCK_STREAK_LIMIT} 次嘗試都失敗且已暫停 {self.BLOCK_MAX_PAUSES} 次，'
                                 '網站可能封鎖或改版')
                        break
                    pause=self.BLOCK_PAUSE_SECONDS*2**(pauses-1)
                    why='收到明確的封鎖訊號' if kind == 'blocked' else f'連續 {streak} 次嘗試 0 次成功（單純答錯的機率極低）'
                    print(f'[券商進出-證交所][疑似被封鎖] {why}，暫停 {pause} 秒'
                          f'（第 {pauses}/{self.BLOCK_MAX_PAUSES} 次暫停，逐次加倍）')
                    self._sleep(pause)
                    streak=0
                    ok_since_pause=0
            if aborted:
                break
            if status == 'retry':
                given_up.append(code)
                print(f'[券商進出-證交所] 證券代號={code} | 放棄 | 已嘗試 {self.MAX_ATTEMPTS_PER_CODE} 次')
            position+=1

        unprocessed=targets[position:]
        self.collect_summary={
            'target':len(targets),'ok':len(result),'no_data':len(no_data),
            'given_up':len(given_up),'unprocessed':len(unprocessed),
            'attempts':attempts,'aborted':aborted,'unsaved':len(self.pending)+len(self.rejected),
            'already':getattr(self,'already_collected',0),
            'given_up_codes':given_up[:50],'unprocessed_codes':unprocessed[:50],
        }
        print(f'[券商進出-證交所] 收集摘要 | 目標={len(targets)} 成功={len(result)} 無明細={len(no_data)} '
              f'放棄={len(given_up)} 未處理={len(unprocessed)} 嘗試次數={attempts} 中止原因={aborted or "無"}')
        self.result=result
    
    def _flush_pending(self):
        """
        把還沒寫入的結果寫進 DB；失敗只記錄、保留在 pending（之後會再試），不中斷收集。
        整批失敗時改成一檔一檔寫，這樣單一檔案的資料有問題（例如撞唯一鍵）不會擋住其他檔。
        """
        from sqlalchemy.exc import IntegrityError
        failures=getattr(self,'_flush_failures',0)
        if failures > 0:
            # 上次連一檔都寫不進去（多半是 DB 暫時掛了）：指數退避後再試，不要馬上重打
            wait=min(self.FLUSH_BACKOFF_MAX,self.FLUSH_BACKOFF_BASE*2**(failures-1))
            print(f'[券商進出-證交所] 上次寫入失敗 {failures} 次，等待 {wait} 秒再重試')
            self._sleep(wait)
        try:
            self._write_frames(self.pending)
            self.pending=[]
            self._flush_failures=0
            return True
        except Exception as e:
            print(f'[券商進出-證交所] 分批寫入失敗，改為逐檔寫入 | {type(e).__name__}: {str(e)[:300]}')
        remaining=[]
        rejected=getattr(self,'rejected',None)
        if rejected is None:
            rejected=self.rejected=[]
        for frame in self.pending:
            try:
                self._write_frames([frame])
            except IntegrityError as e:
                # 唯一鍵衝突是資料本身的問題，重試只會一再失敗：隔離起來並回報，不再保留重試
                code=frame['證券代號'].iloc[0] if '證券代號' in frame.columns and len(frame) else '?'
                print(f'[券商進出-證交所] 證券代號={code} 資料違反唯一鍵，不再重試 | {str(e.orig)[:200]}')
                rejected.append(frame)
            except Exception as e:
                code=frame['證券代號'].iloc[0] if '證券代號' in frame.columns and len(frame) else '?'
                print(f'[券商進出-證交所] 證券代號={code} 寫入失敗，保留待下次 | {type(e).__name__}: {str(e)[:300]}')
                remaining.append(frame)
        progressed=len(remaining) < len(self.pending)
        self.pending=remaining
        self._flush_failures=0 if progressed else self._flush_failures+1
        return not remaining

    def _reconnect(self):
        """只重建 DB 連線（舊的先關掉）；不要呼叫 StockCollectLogic.__init__，它會重設執行紀錄並掃全表計數。"""
        try:
            self.conn.close()
            self.engine.dispose()
        except Exception as e:
            print(f'[券商進出-證交所] 關閉舊連線時出錯（忽略）| {type(e).__name__}: {e}')
        MySQLConnection.__init__(self,self.sql_configure_path)

    def _write_frames(self,frames):
        if not frames:
            return
        result=pd.concat(frames)
        # 券商不在 全券商分行地址 時過去會變成 NULL；唯一鍵含券商名稱，NULL 會讓重跑時去不掉舊資料而重複寫入。
        # 對不到就用券商代號當名稱（不用網頁上往前補齊的名稱，那可能掛到別家）。
        result['券商名稱'],unmapped=self.fill_branch_names(result,self.broker_dict)
        if unmapped:
            print(f'[券商進出-證交所] {len(unmapped)} 個券商代號不在 全券商分行地址，改用代號當名稱: {unmapped[:20]}')
        result=result[~result.duplicated(subset=['日期','證券代號','券商代號'])]
        # 最後防線：理論上 fill_branch_names 已處理撞名；若仍有，丟掉重複列並留下紀錄，不要讓整批寫入失敗
        dup=result.assign(_k=result['券商名稱'].astype(str).str.casefold()).duplicated(subset=['證券代號','_k'])
        if dup.any():
            print(f'[券商進出-證交所] 警告：{int(dup.sum())} 列券商名稱（不分大小寫）重複，已略過 | '
                  f'{result.loc[dup,["證券代號","券商代號"]].head(5).values.tolist()}')
            result=result[~dup.values]
        result=result.rename(columns=self.union_column_name_dict)
        result['資料日期']=result['資料日期'].map(self.make_regular_date_format)
        result=result.sort_values(['證券代號','買賣超'])

        self._reconnect() #connection 會timeout，很奇怪，明明時間設定都有超過
        deleted,inserted=self.replace_by_keys(result,self.table_name,self._diff_table_name,
            ['證券代號','資料日期','券商名稱'],self._select_columns)
        self._saved_delete=getattr(self,'_saved_delete',0)+deleted
        self._saved_insert=getattr(self,'_saved_insert',0)+inserted
        self.delete_count,self.insert_count=self._saved_delete,self._saved_insert
        print(f'[券商進出-證交所] 已寫入 {len(frames)} 檔（累計 insert={self._saved_insert}）')

    def save(self):
        """寫入剩餘結果。不丟例外：寫不進去的留在 pending 並記進摘要，由通知回報（丟例外會觸發整段重跑並跳過下游任務）。"""
        if getattr(self,'pending',None) is None:  # 沒走過 collect（理論上不會發生）
            self.pending=list(self.result)
        if self.pending:
            print('[券商進出-證交所] 寫入剩餘結果…')
            self._flush_pending()
        unsaved=len(self.pending)+len(getattr(self,'rejected',[]))
        if hasattr(self,'collect_summary'):
            self.collect_summary['unsaved']=unsaved
        if unsaved:
            print(f'[券商進出-證交所] 仍有 {unsaved} 檔沒寫入 DB')


class StockMajorHolderCollectProcess(StockCollectLogic):
    """
    大戶比收集
    """
    table_name='大戶比例'
    _diff_table_name='大戶比例_diff'
    app_name='stock_major_holder'

    def __init__(self):
        super().__init__()
        all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
        self.all_code=set(
            all_code_info['證券代號'].dropna().astype(str).str.strip()
        )
        self.market_codes_by_market = {
            market: set(
                all_code_info.loc[
                    all_code_info['市場別'].astype(str).str.strip() == market,
                    '證券代號',
                ].dropna().astype(str).str.strip()
            ) - {''}
            for market in ('上市', '上櫃')
        }

    skipped_as_loaded=False

    def _snapshot_loaded_codes(self,effective_date):
        """該週快照已在大戶比例表中的證券數（資料日期在此表為 YYYYMMDD 文字）。"""
        day=effective_date.strftime('%Y%m%d')
        frame=pd.read_sql_query(
            f"select count(distinct 證券代號) as n from {self.table_name} where 資料日期 = '{day}'",
            self.engine,
        )
        return int(frame['n'].iloc[0])

    def rearrange_data(self,df):
        """
        重整從集保中心收集到的資訊，轉成大戶比格式
        '1':'1-999'
        '2':'1,000-5,000'
        '3':'5,001-10,000'
        '4':'10,001-15,000'
        '5':'15,001-20,000'
        '6':'20,001-30,000'
        '7':'30,001-40,000'
        '8':'40,001-50,000'
        '9':'50,001-100,000'
        '10':'100,001-200,000'
        '11':'200,001-400,000'
        '12':'400,001-600,000'
        '13':'600,001-800,000'
        '14':'800,001-1,000,000'
        '15':'1,000,001以上'

        >400:>=12
        400~600:12
        600~800:13
        800~1000:14
        >1000:15
        """
        col_name=['資料日期', '證券代號', '集保總張數','總股東人數','平均張數/人',
                '>400張大股東持有張數', '>400張大股東人數', '>400張大股東持有百分比',
                '400~600張人數', '600~800張人數', '800~1000張人數', '>1000張人數',
                '>1000張大股東持有百分比']
        
        info=df[df.持股分級 == 17].copy()
        
        if len(info) == 0:
            return pd.DataFrame(columns=col_name)
        
        total_people=info.人數.iloc[0]
        if total_people <= 0 or info.股數.iloc[0] <= 0:
            return pd.DataFrame(columns=col_name)
        total_num=np.round(info.股數.iloc[0]/1000)
        mean_shares=np.round(total_num/total_people,3)
        df=df[df['持股分級']<=15]
        
        more_400_num=np.round(df[df['持股分級']>=12]['股數'].sum()/1000)
        more_400_people=df[df['持股分級']>=12]['人數'].sum()
        more_400_percent=np.round(more_400_num/total_num,4)*100

        interval_400_people=df[df['持股分級']==12]['人數'].sum()
        interval_600_people=df[df['持股分級']==13]['人數'].sum()
        interval_800_people=df[df['持股分級']==14]['人數'].sum()
        interval_1000_num=np.round(df[df['持股分級']==15]['股數'].sum()/1000)
        interval_1000_people=df[df['持股分級']==15]['人數'].sum()

        more_1000_percent=np.round(interval_1000_num/total_num,4)*100
        
        d=pd.DataFrame({
            '資料日期':df['資料日期'].iloc[0],
            '證券代號':df['證券代號'].iloc[0],
            '集保總張數':total_num,
            '總股東人數':total_people,
            '平均張數/人':mean_shares,
            '>400張大股東持有張數':more_400_num,
            '>400張大股東人數':more_400_people,
            '>400張大股東持有百分比':more_400_percent,
            '400~600張人數':interval_400_people,
            '600~800張人數':interval_600_people,
            '800~1000張人數':interval_800_people,
            '>1000張人數':interval_1000_people,
            '>1000張大股東持有百分比':more_1000_percent
        },index=[0])
        
        return d

    def collect(self):
        # Fetch the official full-market TDCC OpenAPI batch, not the retired
        # getOD redirect. Reject redirects and empty/changed payloads so an
        # empty transformation cannot be reported as a successful collection.
        response=requests.get(
            'https://openapi.tdcc.com.tw/v1/opendata/1-5',
            headers=self.headers,
            timeout=60,
            allow_redirects=False,
        )
        if 300 <= response.status_code < 400:
            raise ValueError('TDCC shareholding endpoint returned an unexpected redirect')
        response.raise_for_status()
        payload=response.json()
        response.close()
        if not isinstance(payload, list) or not payload:
            raise ValueError('TDCC shareholding endpoint returned no batch rows')
        df=pd.DataFrame(payload)
        df.columns=[str(column).lstrip('\ufeff').strip() for column in df.columns]
        required_columns={
            '資料日期', '證券代號', '持股分級', '人數', '股數', '占集保庫存數比例%'
        }
        missing_columns=required_columns-set(df.columns)
        if missing_columns:
            raise ValueError(
                'TDCC shareholding schema is missing required columns: '
                + ', '.join(sorted(missing_columns))
            )

        data_dates=df['資料日期'].astype(str).str.strip()
        parsed_dates=pd.to_datetime(data_dates, format='%Y%m%d', errors='coerce')
        if data_dates.empty or parsed_dates.isna().any() or data_dates.nunique() != 1:
            raise ValueError('TDCC shareholding batch has invalid or mixed effective dates')
        effective_date=parsed_dates.iloc[0].date()
        if effective_date > self.now_time.date():
            raise ValueError('TDCC shareholding batch is dated after the collection date')
        # 每日輪詢: 這一週的快照若已完整入庫就直接跳過。重寫同一週不會帶來新資料，卻會改掉
        # create_time（讓「這週資料何時第一次出現」的證據消失）。放在新鮮度檢查之前，已入庫的
        # 舊週不會每天都以失敗收場；是否過舊由訊號摘要的過期旗標負責顯示。
        loaded_codes=self._snapshot_loaded_codes(effective_date)
        market_expected=len(set().union(*self.market_codes_by_market.values()))
        if market_expected and loaded_codes >= 0.95*market_expected:
            print(
                '[stock_major_holder] TDCC snapshot already loaded, skipping '
                f'| effective_date={effective_date.isoformat()}'
                f' | loaded_codes={loaded_codes}/{market_expected}'
            )
            self.result=[]
            self.effective_date=effective_date.isoformat()
            self.skipped_as_loaded=True
            return
        print(
            '[stock_major_holder] new TDCC snapshot detected '
            f'| effective_date={effective_date.isoformat()}'
            f' | loaded_codes={loaded_codes}/{market_expected}'
        )
        latest_market_value=pd.read_sql_query(
            'SELECT MAX(資料日期) AS latest_date FROM 個股成交資訊', self.engine
        )['latest_date'].iloc[0]
        latest_market_date=pd.to_datetime(latest_market_value, errors='coerce')
        if pd.isna(latest_market_date):
            raise ValueError('cannot verify TDCC freshness without a latest market date')
        market_date_lag=(latest_market_date.date()-effective_date).days
        if not 0 <= market_date_lag <= 14:
            raise ValueError(
                'TDCC shareholding snapshot is not fresh against the latest market date: '
                f'effective_date={effective_date.isoformat()}, '
                f'market_date={latest_market_date.date().isoformat()}, '
                f'lag_days={market_date_lag}'
            )

        raw_row_count=len(df)
        source_codes=df['證券代號'].fillna('').astype(str).str.strip()
        missing_code_rows=int(source_codes.eq('').sum())
        if missing_code_rows:
            raise ValueError(
                'TDCC shareholding batch quarantined rows with blank security code: '
                f'quarantined_rows={missing_code_rows}'
            )
        df['證券代號']=source_codes
        normalized_tiers=pd.to_numeric(df['持股分級'], errors='coerce')
        valid_tier_mask=normalized_tiers.notna()
        natural_keys=df.loc[valid_tier_mask, ['資料日期', '證券代號']].copy()
        natural_keys['持股分級']=normalized_tiers.loc[valid_tier_mask]
        duplicate_mask=natural_keys.duplicated(
            subset=['資料日期', '證券代號', '持股分級'], keep=False
        )
        duplicate_row_count=int(duplicate_mask.sum())
        duplicate_key_count=int(
            natural_keys.loc[duplicate_mask].drop_duplicates(
                subset=['資料日期', '證券代號', '持股分級']
            ).shape[0]
        )
        if duplicate_key_count:
            raise ValueError(
                'TDCC shareholding batch has duplicate natural keys: '
                f'duplicate_keys={duplicate_key_count}, '
                f'duplicate_rows={duplicate_row_count}'
            )
        #捨棄其他沒使用的代號(只考慮上市、上櫃)
        df=df[df['證券代號'].isin(self.all_code)].reset_index(drop=True)
        outside_stock_master_row_count=raw_row_count-len(df)
        target_row_count=len(df)
        for column in ('持股分級', '人數', '股數', '占集保庫存數比例%'):
            df[column]=pd.to_numeric(df[column], errors='coerce')
        required_values=['持股分級', '人數', '股數', '占集保庫存數比例%']
        invalid_value_mask=df[required_values].isna().any(axis=1)
        invalid_value_mask |= (df[['持股分級', '人數', '股數']] < 0).any(axis=1)
        invalid_value_mask |= (
            (df['持股分級'] < 1)
            | (df['持股分級'] > 17)
            | (df['持股分級'] % 1 != 0)
            | (df['占集保庫存數比例%'] < 0)
            | (df['占集保庫存數比例%'] > 100)
        )
        quarantined_row_count=int(invalid_value_mask.sum())
        if df.empty or quarantined_row_count:
            raise ValueError(
                'TDCC shareholding batch has no target rows or quarantined '
                f'invalid target values: target_rows={target_row_count}, '
                f'quarantined_rows={quarantined_row_count}'
            )
        accepted_row_count=target_row_count-quarantined_row_count

        market_codes=set().union(*self.market_codes_by_market.values())
        observed_codes=set(df['證券代號'])
        coverage_by_market={}
        for market, expected_codes in self.market_codes_by_market.items():
            matched_codes=expected_codes & observed_codes
            coverage=(len(matched_codes) / len(expected_codes)) if expected_codes else 0
            coverage_by_market[market]=(len(matched_codes), len(expected_codes), coverage)
            if expected_codes and coverage < 0.95:
                raise ValueError(
                    f'TDCC shareholding coverage is below 95% for {market}: '
                    f'{len(matched_codes)}/{len(expected_codes)}'
                )

        market_rows=df[df['證券代號'].isin(market_codes)]
        total_tier_codes=set(
            market_rows.loc[market_rows['持股分級'] == 17, '證券代號']
        )
        missing_total_codes=set(market_rows['證券代號'])-total_tier_codes
        if missing_total_codes:
            raise ValueError(
                'TDCC shareholding batch is missing total-holding tier 17 for '
                f'{len(missing_total_codes)} listed/OTC securities'
            )

        final_result=df.groupby(['證券代號']).apply(self.rearrange_data).reset_index(drop=True)
        if final_result.empty:
            raise ValueError('TDCC shareholding transform produced no company summaries')
        transformed_codes=set(final_result['證券代號'].astype(str).str.strip())
        transformed_coverage_by_market={}
        for market, expected_codes in self.market_codes_by_market.items():
            matched_codes=expected_codes & transformed_codes
            coverage=(len(matched_codes) / len(expected_codes)) if expected_codes else 0
            transformed_coverage_by_market[market]=(len(matched_codes), len(expected_codes), coverage)
            if expected_codes and coverage < 0.95:
                raise ValueError(
                    f'TDCC transformed coverage is below 95% for {market}: '
                    f'{len(matched_codes)}/{len(expected_codes)}'
                )

        result=[]
        for code in final_result['證券代號'].unique():
            d=final_result[final_result.證券代號 == code]
            result.append(d)
        
        self.result=result
        self.effective_date=effective_date.isoformat()
        self.raw_row_count=raw_row_count
        self.target_row_count=len(df)
        self.market_coverage={
            'raw': coverage_by_market,
            'transformed': transformed_coverage_by_market,
        }
        print(
            '[stock_major_holder] TDCC batch accepted '
            f'| effective_date={self.effective_date}'
            f' | market_date_lag_days={market_date_lag}'
            f' | raw_rows={raw_row_count}'
            f' | accepted_rows={accepted_row_count}'
            f' | quarantined_rows={quarantined_row_count}'
            f' | duplicate_keys={duplicate_key_count}'
            f' | duplicate_rows={duplicate_row_count}'
            f' | outside_stock_master_rows={outside_stock_master_row_count}'
            f' | target_rows={len(df)}'
            f' | summary_rows={len(final_result)}'
            f' | TWSE raw={coverage_by_market.get("上市", (0, 0, 0))[0]}/'
            f'{coverage_by_market.get("上市", (0, 0, 0))[1]}'
            f' summary={transformed_coverage_by_market.get("上市", (0, 0, 0))[0]}/'
            f'{transformed_coverage_by_market.get("上市", (0, 0, 0))[1]}'
            f' | TPEx raw={coverage_by_market.get("上櫃", (0, 0, 0))[0]}/'
            f'{coverage_by_market.get("上櫃", (0, 0, 0))[1]}'
            f' summary={transformed_coverage_by_market.get("上櫃", (0, 0, 0))[0]}/'
            f'{transformed_coverage_by_market.get("上櫃", (0, 0, 0))[1]}'
        )
    
    def save(self):
        if len(self.result)>0:
            self.result=pd.concat(self.result)
            
            self.delete_count,self.insert_count=self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)
        
            
class StockMarginTradeCollectProcess(StockCollectLogic):
    """
    融資券收集
    """
    table_name='融資融券'
    # 專屬暫存表: 股票模板_diff2 同時被其他收集程式 TRUNCATE 與寫入，每日執行後
    # 與它們共用會互相清掉對方的暫存資料。
    _diff_table_name='融資融券_diff'
    app_name='stock_margin_trade'

    def __init__(self):
        super().__init__()
        self.cursor.execute(
            f'CREATE TABLE IF NOT EXISTS {self._diff_table_name} LIKE 股票模板_diff2'
        )
        self.conn.commit()
        all_code_info=pd.read_sql_query(
            'select 證券代號 from 全證券代碼相關資料表', self.engine
        )
        self.all_code=all_code_info.證券代號.tolist()
        self.http_session = requests.Session()
        self.http_session.headers.update(self.headers)

    def get_margin_data(self,code):
        """
        融資融券收集主程式
        https://concords.moneydj.com/
        """
        url=f'https://concords.moneydj.com/z/zc/zcn/zcn_{code}.djhtm'

        a=self.http_session.get(url,timeout=self.timeout)

        soup=BeautifulSoup(a.text,'lxml')

        col_name=[s.text.strip() for s in soup.find('tr',{'id':'oScrollMenu','align':'center'}).find_all('td')]

        row=[s for s in soup.find('tr',{'id':'oScrollMenu','align':'center'}).find_all_next('tr') if s.get('id') != 'oScrollFoot']
        row=[[g.text.strip() for g in s.find_all('td')] for s in row]

        df=pd.DataFrame(row,columns=col_name)
        df=df[df.日期 != '']
        df['證券代號']=code
        df.columns=[
            '資料日期',
            '融資買進',
            '融資賣出',
            '現償',
            '融資剩餘額',
            '當日融資變化',
            '限額',
            '融資使用率',
            '融券賣出',
            '融券買進',
            '券償',
            '融券剩餘額',
            '當日融券變化',
            '券資比',
            '相抵','證券代號'
        ]
        df=df.loc[:,['資料日期', '證券代號', '當日融資變化', '當日融券變化', '融資剩餘額',
                     '融券剩餘額', '券資比', '融資買進','融資賣出', '融資使用率', '融券賣出', '融券買進']]
        df['資料日期']=df['資料日期'].map(self.make_regular_date_format)

        for ss in ['當日融資變化', '當日融券變化','融資買進','融資剩餘額','融券剩餘額','融資賣出', '融券賣出', '融券買進']:
            df[ss]=[re.sub(string=str(s),pattern=',',repl='') for s in df[ss]]

        df['券資比']=[float(s.strip('%')) for s in df['券資比']]
        df['融資使用率']=[float(s.strip('%')) for s in df['融資使用率']]
        df=df.sort_values(['資料日期'])
        
        return df

    def collect(self):
        error=[]
        result=[]
        for i in range(len(self.all_code)):
            code=self.all_code[i]
            try:
                df=self.get_margin_data(code)
                result.append(df)
                
                print(f'[融資融券] 證券代號={code} | 首輪完成')
                time.sleep(random.randint(1,3))
            except Exception as e:
                error.append(code)
                print(f'[融資融券] 首輪失敗 | 證券代號={code} | {type(e).__name__}: {e}')

        error_length=len(error)
        accurate_code=[]
        N=0

        error_counter_dict={}
        for code in error:
            error_counter_dict[code]=1        

        while error_length != 0:
            try:
                for code in error:
                    df=self.get_margin_data(code)
                    result.append(df)
                    print(f'[融資融券] 證券代號={code} | 重試成功')
                    accurate_code.append(code)
                
                error_length=0
            except Exception as e:
                print(f'[融資融券] 重試失敗 | 證券代號={code} | 第{N}輪 | {type(e).__name__}: {e}')
                N+=1
                error_counter_dict[code]+=+1
                
                if error_counter_dict[code]>3:
                    if code in error:
                        error.remove(code)
                    error_counter_dict.pop(code,None)
                
                if N>150:
                    break
                    
                error=list(set(error)-set(accurate_code))
                error_length=len(error)
                
                time.sleep(0.5)   
        
        self.result=result
    
    def save(self):
        if len(self.result)>0:
            self.result=pd.concat(self.result)
            super().__init__()
            self.delete_count,self.insert_count=self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)


class StockMarginTradeDailyProcess(StockMarginTradeCollectProcess):
    """
    每日融資融券收集（交易日晚間）。

    與週六完整收集共用同一份爬蟲與刪除再寫入邏輯（以 證券代號+資料日期 為鍵，可重複執行），差別在：
    - 只抓最近 45 天內實際有融資融券資料的證券，省去約 1/7 沒有資料的頁面與其重試；
    - 儲存之後驗證「目標交易日」是否真的被收到：目標日的證券數要達到近期每日常態的 90%，
      否則先保存已收到的資料，再讓任務失敗，交給重試補齊。
    """
    app_name='stock_margin_trade_daily'
    # 週六完整收集與每日收集各用自己的暫存表，兩者即使時間重疊也不會互相清掉對方的鍵
    _diff_table_name='融資融券_diff_daily'

    def __init__(self,target_date=None):
        super().__init__()
        self.target_date=pd.Timestamp(target_date or self.now_time.date()).date()
        start=(pd.Timestamp(self.target_date)-pd.Timedelta(days=45)).strftime('%Y-%m-%d')
        recent=pd.read_sql_query(
            f"""
            select 資料日期, count(distinct 證券代號) as n
            from 融資融券
            where 資料日期 >= '{start}'
            group by 資料日期
            """,
            self.engine,
        )
        self.baseline_codes=margin_daily.baseline_code_count(
            pd.Series(recent['n'].values,index=pd.to_datetime(recent['資料日期'])),
            self.target_date,
        )
        eligible=pd.read_sql_query(
            f"select distinct 證券代號 from 融資融券 where 資料日期 >= '{start}'",
            self.engine,
        )['證券代號'].astype(str).tolist()
        if eligible:
            eligible=set(eligible)
            self.all_code=[code for code in self.all_code if str(code) in eligible]

    def source_latest_date(self,code=margin_daily.PROBE_CODE):
        """探測頁面（一次請求）最新一列的日期，用來判斷來源是否已公布目標交易日。"""
        return self.get_margin_data(code)['資料日期'].max()

    def save(self):
        super().save()
        # 以資料庫實際存量判斷（重跑時爬到一部分、但庫裡早已完整，不應誤報）
        stored=pd.read_sql_query(
            f"""
            select count(distinct 證券代號) as n from 融資融券
            where 資料日期 = '{self.target_date.strftime('%Y-%m-%d')}'
            """,
            self.engine,
        )['n'].iloc[0]
        coverage=margin_daily.coverage_from_count(int(stored),self.target_date,self.baseline_codes)
        self.coverage=coverage
        print(f'[{self.app_name}] 目標交易日覆蓋 | {coverage.describe()}')
        if not coverage.accepted:
            raise ValueError(
                '融資融券目標交易日資料不足（已保存已收到的部分，等待重試補齊）: '
                f'{coverage.describe()}'
            )


class StockMonthRevenueCollectProcess(StockCollectLogic):
    """
    月營收資訊收集
    """
    table_name='月營收資訊'
    _diff_table_name='股票模板_diff'
    app_name='stock_month_revenue'

    def __init__(self):
        super().__init__()
        all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
        self.all_code=all_code_info.證券代號.tolist()

    def get_month_revenue(self,code):
        """
        公開資訊站
        """
        url='https://mopsov.twse.com.tw/mops/web/ajax_t146sb05'
        data={
            'encodeURIComponent':'1',
            'step':'2',
            'firstin':'1',
            'off':'1',
            'keyword4':'',
            'code1':'',
            'TYPEK2':'',
            'checkbtn':'',
            'queryName':'co_id',
            'inpuType':'co_id',
            'TYPEK':'all',
            'co_id':f'{code}',
        }
        a=self.simulate_request(event={'url':url,'data':data,
                                  'headers':self.headers,
                                  'method':'post'})
        #pandas 偷懶方式
        df=pd.read_html(StringIO(a['text']),flavor='lxml')[1]
        df.columns=df.iloc[1,:]
        df=df.iloc[2:,:].reset_index(drop=True)
        
        df['年度']=df['年度'].astype('int')+1911
        df['資料日期']=[f'{i}-0{j}-01' if len(j) == 1 else f'{i}-{j}-01' for i,j in zip(df.年度,df.月份)]

        df=df.drop(['年度','月份'],axis=1)
        df['證券代號']=code
        df['上月營收']=df.當月營收.shift(-1)
        df=df[~df.上月營收.isnull()]
        df['上月營收']=df['上月營收'].astype('int')
        df['當月營收']=df['當月營收'].astype('int')
        df['上月營收增減(%)']=((df['當月營收']-df['上月營收'])*100/df['上月營收']).round(2)

        df['去年同月增減(%)']=[float(v.strip('%')) for v in df['去年同月增減(%)']]
        df['前期比較增減(%)']=[float(v.strip('%')) for v in df['前期比較增減(%)']]

        df=df.loc[:,['證券代號','資料日期','當月營收','上月營收','去年當月營收','上月營收增減(%)','去年同月增減(%)',
                     '當月累計營收', '去年累計營收', '前期比較增減(%)']]
        
        return df
    
    def collect(self):
        error=[]
        result=[]
        for i in range(len(self.all_code)):
            code=self.all_code[i]
            try:
                df=self.get_month_revenue(code)
                result.append(df)
                
                print(f'[月營收] 證券代號={code} | 首輪完成')
                time.sleep(random.randint(2,4))
            except Exception as e:
                error.append(code)
                print(f'[月營收] 首輪失敗 | 證券代號={code} | {type(e).__name__}: {e}')

        error_length=len(error)
        accurate_code=[]
        N=0

        error_counter_dict={}
        for code in error:
            error_counter_dict[code]=1        

        while error_length != 0:
            try:
                for code in error:
                    df=self.get_month_revenue(code)
                    result.append(df)
                    print(f'[月營收] 證券代號={code} | 重試成功')
                    accurate_code.append(code)
                    time.sleep(random.randint(2,4))
                
                error_length=0
            except Exception as e:
                print(f'[月營收] 重試失敗 | 證券代號={code} | 第{N}輪 | {type(e).__name__}: {e}')
                N+=1
                error_counter_dict[code]+=+1
                
                if error_counter_dict[code]>3:
                    if code in error:
                        error.remove(code)
                    error_counter_dict.pop(code,None)
                
                if N>500:
                    break
                    
                error=list(set(error)-set(accurate_code))
                error_length=len(error)
                
                time.sleep(1)  
        
        self.result=result 
    
    def save(self):
        if len(self.result)>0:
            self.result=pd.concat(self.result)
            self.result=self.result.replace(np.inf,np.nan)
            self.result=self.result.replace(-np.inf,np.nan)
            
            super().__init__()
            
            self.delete_count,self.insert_count=self.replace_by_keys(self.result,self.table_name,self._diff_table_name,
                ['證券代號','資料日期'],self._select_columns)
