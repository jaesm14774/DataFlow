from lib.db_upsert import replace_by_keys
from lib.get_sql import get_sql_from_config


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
        self.db_name = db_name
        self.conn, self.cursor, self.engine = get_sql_from_config(
            db_name, sql_configure_path,
        )

    def replace_by_keys(self, df, table_name, diff_table_name, keys, diff_columns=None):
        """delete-insert：刪掉 table_name 內與 df 同 keys 的舊資料後整批寫入 df。"""
        return replace_by_keys(
            df, table_name, diff_table_name, keys,
            self.conn, self.cursor, self.engine, diff_columns,
        )

    def end(self):
        self.conn.close()
        self.engine.dispose()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()
        return False
