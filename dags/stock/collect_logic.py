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
from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pymysql
from stock.config.config import *
import cv2
import tensorflow as tf
from stock.utilities_tf2 import preprocessing, one_hot_decoding
from lib.log_process_execution import BaseLogRecord
import MySQLdb
import boto3

import os
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"]="python"

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


class StockCollectLogic(MySQLConnection):
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
        before_count=pd.read_sql_query(f'select count(1) as N from {self.table_name}',self.engine)['N'].iloc[0]
        self.log_record.set_before_count(before_count)
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
        
    def clean_comma(self,txt,output_not_string=False):
        """
        清除逗號
        """
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

    def make_regular_date_format(self,dt):
        """
        時間處理函數
        """
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
        after_count=pd.read_sql_query(f'select count(1) as N from {self.table_name}',
                                      self.engine)['N'].iloc[0]  
        self.log_record.set_after_count(after_count)
    
    def process(self):
        """
        主要的資料流程
        """
        try:
            self.collect()
            self.save()
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
        print(f'Delete process for code: {self.code}')

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
            
            print(f'Done {source_table_name_list}')

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
            print('address to geo info error: ',e)
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
        print(f'Add process for new code: {self.code}')
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
        except:
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
        print('Renew 公司資訊 for : '+code)  
        
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
        print('Renew company and broker for : '+code)

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
            except:
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
            except:
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

        print('new_code: ',self.new_code)
        print('delete_code: ',self.delete_code)
        
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
        
        for s in [col for col in df.columns if col not in ['資料日期','證券代號','證券名稱']]:
            df[s]=df[s].apply(self.clean_comma,output_not_string=True)
        
        cdf['外資買進股數']=df.loc[:,[s for s in df.columns if s.find('外') != -1 and s.find('買進') != -1]].apply(sum,axis=1)
        cdf['外資賣出股數']=df.loc[:,[s for s in df.columns if s.find('外') != -1 and s.find('賣出') != -1]].apply(sum,axis=1)
        cdf['外資買賣超股數']=df.loc[:,[s for s in df.columns if s.find('外') != -1 and s.find('買賣超') != -1]].apply(sum,axis=1)

        cdf['投信買進股數']=df.loc[:,[s for s in df.columns if s.find('投信') != -1 and s.find('買進') != -1]].apply(sum,axis=1)
        cdf['投信賣出股數']=df.loc[:,[s for s in df.columns if s.find('投信') != -1 and s.find('賣出') != -1]].apply(sum,axis=1)
        cdf['投信買賣超股數']=df.loc[:,[s for s in df.columns if s.find('投信') != -1 and s.find('買賣超') != -1]].apply(sum,axis=1)
        
        cdf['自營商買進股數']=df.loc[:,[s for s in df.columns if s.find('自營商') != -1 and s.find('買進') != -1 and s.find('外') == -1]].apply(sum,axis=1)
        cdf['自營商賣出股數']=df.loc[:,[s for s in df.columns if s.find('自營商') != -1 and s.find('賣出') != -1 and s.find('外') == -1]].apply(sum,axis=1)
        
        cdf['自營商買賣超股數']=df['自營商買賣超股數']
        cdf['三大法人買賣超股數']=df['三大法人買賣超股數']
        
        cdf=cdf[~cdf.duplicated(subset=unique_pair)]
        
        cdf['資料日期']=cdf['資料日期'].map(self.make_regular_date_format)
        
        cdf['證券代號']=cdf['證券代號'].map(str.strip)
        cdf['證券名稱']=cdf['證券名稱'].map(str.strip)
        
        
        return cdf
    
    def collect(self):
        #上市
        time_format=self.now_time.strftime('%Y%m%d')

        a=requests.get('https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date='+time_format+'&selectType=ALL',
                       timeout=30)
        a=a.json()
        #column name
        d1=pd.DataFrame(columns=a['fields'],data=a['data'])
        d1['資料日期']=self.now_time.strftime('%Y-%m-%d')
        d1=self.convert_three_mainforce_data_into_sql_format(df=d1,unique_pair=['證券代號'])

        #上櫃
        time_format=self.now_time.strftime('%Y/%m/%d')
        n=re.search(string=time_format,pattern=r'^(\d+)/').group(1)
        temp_time_format=re.sub(string=time_format,pattern=n,
                    repl=str(int(n)-1911))
        
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

        #!!! 雷爆，外資及陸資買進股數=外陸資買進股數(不含外資自營商)+外資自營商買進股數
        if '外資及陸資買進股數' in d2.columns:
            d2.drop(['外資及陸資買進股數','外資及陸資賣出股數','外資及陸資買賣超股數'],axis=1,inplace=True)
            
        d2=self.convert_three_mainforce_data_into_sql_format(df=d2,unique_pair=['證券代號'])

        D=pd.concat([d1,d2],ignore_index=True)
        D=D[~D.duplicated()]
        D=D.sort_values(['證券代號'])
        self.result=D

    def save(self):
        """
        delete insert
        """
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
            #step3 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)


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
        self.all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
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
        a=requests.get(url,headers=self.headers,timeout=30)
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
        d1.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,index=0,if_exists='append')
        print(f'twse collect code: {code} is done.')
        
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
                print(e)
                print(code)
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
                print(e)
                time.sleep(random.randint(32,42)/10)
                N+=1
                er[code]=er[code]+1
                print(er)
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
        response=requests.post(url, timeout=20,
                    headers=self.headers, data={
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
        d2.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,index=0,if_exists='append')
        print(f'tpex collect code: {code} is done.')
        
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
                print(f'櫃買市場: {code} is failed!')
                print(e)
                time.sleep(3)
            time.sleep(1.5)
        
        self.result=tpex
    
    def save(self):
        if len(self.result)>0:
            result=pd.concat(self.result).reset_index(drop=True)
            result=result.replace('--',np.nan)
            #delete already exist data in 個股成交資訊
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
            #insert data
            result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            
            self.insert_count=len(result)
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()


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
        
        #truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()
        
        result.loc[:,['市場別']].to_sql(self._diff_table_name,self.engine,index=0,if_exists='append')
        #delete already exist data
        delete_sql=f"""
        delete tb from {self.table_name} as tb
        where exists (
        select *
        from {self._diff_table_name} tb2
        where tb.市場別=tb2.市場別 and tb.資料日期=tb2.資料日期
        )
        """
        self.delete_count=self.cursor.execute(delete_sql)
        self.conn.commit()
        #insert data
        result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
        self.insert_count=len(result)


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
        #step1 truncate delete temp table
        self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
        self.conn.commit()
        self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,
                                                       self.engine,
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
        self.delete_count=self.cursor.execute(delete_sql)
        self.conn.commit()
        #step2 insert data
        self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
        self.insert_count=len(self.result)


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
        
        with open(temp_file_path+'day_trader.csv','w',encoding='utf_8_sig') as h:
            h.write(s)
        d1=pd.read_csv(temp_file_path+'day_trader.csv',encoding='utf_8_sig')
        d1['資料日期']=self.really_date.strftime('%Y-%m-%d')
        select_column=['資料日期','證券代號', '證券名稱',
                       '當日沖銷交易成交股數', '當日沖銷交易買進成交金額',
                       '當日沖銷交易賣出成交金額']
        d1.columns=[s.replace("'","").replace('"','').strip() for s in d1.columns]
        d1=d1.loc[:,select_column]
        
        #上櫃
        time_format=self.really_date.strftime('%Y/%m/%d')
        print('確認 當沖資訊 上櫃 日期參數: ',time_format)
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
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,
                                                        self.engine,
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
            self.delete_count=self.cursor.execute(delete_sql)
            self.conn.commit()
            #step2 insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)
            
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
                print('當沖資訊: ',e)
                
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
            print('收集成功!')
        except Exception as e:
            self.log_record.raise_error(repr(e))
            print('任務失敗')
            raise e
        finally:
            self.log_record.insert_to_log_record()
            self.end()


class StockBrokerInOutTWSECollectProcess(StockCollectLogic):
    """
    證交所券商進出收集
    """
    table_name='券商進出總表'
    _diff_table_name='券商進出總表_diff'
    _select_columns=['證券代號','資料日期','券商名稱']
    domain_url='https://bsr.twse.com.tw/'
    allowedChars = '0123456789ABCDEFGHJKLMNPQRSTUVWXYZ'
    
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
        print(f'Remaining for twse broker inout collect is : {len(self.twse_code)}')
        
        broker=pd.read_sql_query('select * from 全券商分行地址',self.engine)
        broker_dict={}
        for index,row in broker.iterrows():
            broker_dict[row['證券商代號']]=row['證券商名稱']     
        self.broker_dict=broker_dict
        
        print('model loading...')
        self.model = tf.keras.models.load_model(self.model_path)
        print('loading completed')

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
        #去證交所下載驗證碼
        session=requests.session()
        response=session.get('https://bsr.twse.com.tw/bshtm/bsMenu.aspx',headers=self.headers,
                    timeout=60,verify=False)
        soup=BeautifulSoup(response.text,'lxml')

        img_url='https://bsr.twse.com.tw/bshtm/'+[s.get('src') for s in soup.find_all('img') if bool(re.search(string=str(s),pattern=r'CaptchaImage'))][0]
        with open(img_path, 'wb') as f:
            resp = session.get(img_url, stream=True)
            resp.raw.decode_content = True
            shutil.copyfileobj(resp.raw, f)
        
        #破解驗證碼
        preprocessing(img_path, self.processed_img_path)
        img = cv2.imread(self.processed_img_path)
        img = img / 255.0  # 归一化
        img = np.expand_dims(img, axis=0)  # 添加批次维度
        
        prediction = self.model.predict(img)
        predict_captcha = one_hot_decoding(prediction, self.allowedChars)

        payload = {}
        acceptable_input=['__EVENTTARGET','__EVENTARGUMENT','__LASTFOCUS','__VIEWSTATE',
                        '__VIEWSTATEGENERATOR','__EVENTVALIDATION']

        inputs = soup.find_all("input")
        for elem in inputs:
            if elem.get("name") in acceptable_input:
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
        
        if '驗證碼錯誤!' in resp.text:
            print('驗證碼錯誤, predict_captcha: ' + predict_captcha)      
            return '驗證碼錯誤'
        #呼叫 html格式的網頁表
        a=session.get('https://bsr.twse.com.tw/bshtm/bsContent.aspx?v=t',timeout=60,verify=False)
        soup=BeautifulSoup(a.text,'lxml')    
        
        try:
            T=self.parse_and_clean_table(soup,code=code)
        except Exception as e:
            print('code:',code,'\n','Error:',e,'\n\n')
            
            return '沒有資料，請檢查'
        
        session.close()
        
        return T

    def collect(self):
        ER=[]
        ER2=[]
        twse=[]        

        for code in self.twse_code:
            try:
                t=self.geo_broker_twse(code=code)

                if isinstance(t,pd.DataFrame):
                    twse.append(t)
                    print(f'code:{code} is done!')
                else:
                    if t == '沒有資料，請檢查':
                        ER2.append(code)
                    else:
                        ER.append(code)
            except Exception as e:
                print('錯誤訊息:',e,'\n')
                ER.append(code)
                
                time.sleep(random.randint(11,25)/10)
        
        N=0
        er={}
        for code in ER:
            er[code]=1

        while len(ER) != 0:
            accurate_code=[]

            try:
                for code in ER:
                    t=self.geo_broker_twse(code=code)

                    if isinstance(t,pd.DataFrame):
                        twse.append(t)
                        accurate_code.append(code)
                        print(f'code:{code} is done!')
                ER=[s for s in ER if s not in accurate_code]
            except Exception as e:
                try:
                    print('twse broker inout collect : ',e)
                    er[code]=er[code]+1
                except:
                    er[code]=1
                print('N:',N,'\ner:',er)
                time.sleep(random.randint(11,25)/10)
                N=N+1
                er[code]=er[code]+1
                if er[code]>3:
                    if code in ER:
                        ER.remove(code)
                    er.pop(code,None)
                
                if N > 1500:
                    break
                
                ER=[s for s in ER if s not in accurate_code]
        
        self.result=twse
    
    def save(self):
        if len(self.result)>0:
            print('save step start')
            result=pd.concat(self.result)
            result['券商名稱']=result.券商代號.map(self.broker_dict)
            result=result[~result.duplicated(subset=['日期','證券代號','券商代號'])]
            result=result.rename(columns=self.union_column_name_dict)
            result['資料日期']=result['資料日期'].map(self.make_regular_date_format)
            result=result.sort_values(['證券代號','買賣超'])

            super().__init__() #connection 會timeout，很奇怪，明明時間設定都有超過
            #step1 truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            
            result.loc[:,self._select_columns].to_sql(self._diff_table_name,self.engine,
                                                      if_exists='append',index=0)
            #step2 delete already exist data in 券商進出總表
            delete_sql=f"""
            delete tb from {self.table_name} as tb
            where exists (
            select *
            from {self._diff_table_name} tb2
            where tb.證券代號=tb2.證券代號 and tb.資料日期=tb2.資料日期 and tb.券商名稱=tb2.券商名稱
            )
            """
            self.delete_count=self.cursor.execute(delete_sql)
            self.conn.commit()
            #step3 insert data
            result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(result)


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
        self.all_code=all_code_info.證券代號.tolist()

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
        #集保中心一次下載全部在整理成神祕金字塔的樣子
        a=requests.get('https://opendata.tdcc.com.tw/getOD.ashx?id=1-5',headers=self.headers,timeout=30)
        #csv格式轉成dataframe
        data_list=list(csv.reader(a.text.splitlines(), delimiter=','))
        df=pd.DataFrame(data_list[1:],columns=data_list[0])
        #捨棄其他沒使用的代號(只考慮上市、上櫃)
        df=df[df.證券代號.isin(self.all_code)].reset_index(drop=True)
        df['持股分級']=df['持股分級'].astype('int')
        df['股數']=df['股數'].astype('float')
        df['人數']=df['人數'].astype('float')
        #這個column很邪門，"資料日期"這欄位完全看不出來異狀
        df.columns=['資料日期', '證券代號', '持股分級', '人數', '股數', '占集保庫存數比例%']  
        final_result=df.groupby(['證券代號']).apply(self.rearrange_data).reset_index(drop=True)

        result=[]
        for code in final_result['證券代號'].unique():
            d=final_result[final_result.證券代號 == code]
            result.append(d)
        
        self.result=result
    
    def save(self):
        if len(self.result)>0:
            self.result=pd.concat(self.result)
            
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,
                                                        self.engine,
                                                        if_exists='append',index=0)
            
            #delete already exist data
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
            #insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)
        
            
class StockMarginTradeCollectProcess(StockCollectLogic):
    """
    融資券收集
    """
    table_name='融資融券'
    _diff_table_name='股票模板_diff2'
    app_name='stock_margin_trade'

    def __init__(self):
        super().__init__()
        all_code_info=pd.read_sql_query('select * from 全證券代碼相關資料表',self.engine)
        self.all_code=all_code_info.證券代號.tolist()

    def get_margin_data(self,code):
        """
        融資融券收集主程式
        https://concords.moneydj.com/
        """
        url=f'https://concords.moneydj.com/z/zc/zcn/zcn_{code}.djhtm'

        a=requests.get(url,headers=self.headers,timeout=self.timeout)

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
                
                print(f'融資融券收集 - {code} 已完成')
                time.sleep(random.randint(1,3))
            except:
                error.append(code)

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
                    print(f'融資融券收集 - {code} 已完成')
                    accurate_code.append(code)
                
                error_length=0
            except:
                print(f'融資融券收集 - {code} 失敗')
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
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,
                                                        self.engine,
                                                        if_exists='append',index=0)
            
            #delete already exist data
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
            #insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)


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
        df=pd.read_html(a['text'])[1]
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
                
                print(f'月營收資訊收集 - {code} 已完成')
                time.sleep(random.randint(2,4))
            except:
                error.append(code)

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
                    print(f'月營收資訊收集 - {code} 已完成')
                    accurate_code.append(code)
                    time.sleep(random.randint(2,4))
                
                error_length=0
            except:
                print(f'月營收資訊收集 - {code} 失敗')
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
            
            #truncate delete temp table
            self.cursor.execute(f'TRUNCATE TABLE {self._diff_table_name}')
            self.conn.commit()
            self.result.loc[:,self._select_columns].to_sql(self._diff_table_name,
                                                        self.engine,
                                                        if_exists='append',index=0)
            
            #delete already exist data
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
            #insert data
            self.result.to_sql(self.table_name,self.engine,index=0,if_exists='append')
            self.insert_count=len(self.result)
            