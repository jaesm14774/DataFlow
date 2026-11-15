from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pandas as pd
import pymysql
import MySQLdb

def get_sql(host,port,user,password,db_name):
    """
    host:server ip of sql
    port:port sql
    user:user name of sql
    password:password of sql user
    db_name:name of database
    """
    conn=pymysql.connect(host=host,
                         port=int(port),
                         user=user,
                         password=password,
                         db=db_name)
    cursor=conn.cursor()
    engine = create_engine('mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8mb4' %
                           (user,quote_plus(password),host,port,db_name))

    return (conn,cursor,engine)


def read_sql_config(sql_configure_path=None):
    """讀取 sql_config(name/value) 設定檔，未指定路徑時使用 common.config 的預設值。"""
    if sql_configure_path is None:
        from common.config import sql_configure_path
    return pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')


def get_sql_from_config(db_name,sql_configure_path=None):
    """
    依 sql_config 設定檔連線到 db_name，回傳 (conn,cursor,engine)。

    sql_configure_path:未指定時使用 common.config 的預設值；
    stock 模組使用 stock/config 底下的另一份設定檔，需明確傳入。
    """
    cfg=read_sql_config(sql_configure_path)
    return get_sql(cfg.loc['host','value'],
                   cfg.loc['port','value'],
                   cfg.loc['user','value'],
                   cfg.loc['password','value'],
                   db_name)
