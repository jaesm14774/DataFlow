import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
def eng(db='stock', path=r'D:\DataFlow\dags\stock\config\sql_config.txt'):
    c=pd.read_csv(path,encoding='utf_8_sig',index_col='name')['value']
    u=URL.create('mysql+pymysql',username=c['user'],password=c['password'],host=c['host'],port=int(c['port']),database=db,query={'charset':'utf8mb4'})
    return create_engine(u)
