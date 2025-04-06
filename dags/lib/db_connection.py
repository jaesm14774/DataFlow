from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pymysql
import MySQLdb
from common.config import sql_configure_path
import pandas as pd

class MySQLConnection:
    def __init__(self,db_name):
        self.connection=pd.read_csv(sql_configure_path,index_col='name')
        self.db_name=db_name
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