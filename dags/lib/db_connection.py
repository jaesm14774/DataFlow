from urllib.parse import quote_plus
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from common.config import sql_configure_path as _default_sql_configure_path


class MySQLConnection:
    """統一的 MySQL 連線管理，支援 context manager。

    Parameters
    ----------
    db_name : str
        目標資料庫名稱。
    sql_configure_path : str, optional
        CSV 設定檔路徑，預設使用 common.config 的值。
    """

    def __init__(self, db_name, sql_configure_path=None):
        cfg_path = sql_configure_path or _default_sql_configure_path
        connection = pd.read_csv(cfg_path, index_col='name')

        self.db_name = db_name
        host = connection.loc['host', 'value']
        port = int(connection.loc['port', 'value'])
        user = connection.loc['user', 'value']
        password = connection.loc['password', 'value']

        self.conn = pymysql.connect(
            host=host, port=port, user=user,
            password=password, db=self.db_name,
        )
        self.cursor = self.conn.cursor()
        self.engine = create_engine(
            'mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8mb4'
            % (user, quote_plus(password), host, port, self.db_name)
        )

    def end(self):
        self.conn.close()
        self.engine.dispose()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()
        return False
