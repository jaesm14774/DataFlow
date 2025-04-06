from urllib.parse import quote_plus
from sqlalchemy import create_engine
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