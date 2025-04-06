import datetime
import pandas as pd
from lib.get_sql import *
from common.config import *

#read config
connection=pd.read_csv(sql_configure_path,index_col='name')


class BaseLogRecord(object):
    """
    BaseLogRecord

    Arguments:
        app_name {str} -- [程式名稱]
        success {bool} -- [程式是否成功執行]
        process_date {date} -- [執行日期參數]
        process_start_time {datetime} -- [程序開始時間]
        process_end_time {datetime} -- [程序結束時間]
        error_message {text} -- [程式錯誤訊息]
        before_count {int} -- [處理前該表有多少筆數據]
        after_count {int} -- [處理後該表有多少筆數據]
        insert_count {int} -- [新增幾筆數據]
        delete_count {int} -- [刪除幾筆數據]
        update_count {int} -- [更新幾筆數據]
    """
    PROCESS_LOG_DATABASE = 'logs'
    
    insert_table_name = "process_execution_logs"
    
    column_list=[
        'app_name','success',
        'process_date','process_start_time',
        'process_end_time','error_message',
        'before_count','after_count',
        'insert_count','update_count','delete_count'
    ]

    def __init__(self, app_name, process_date):
        self.app_name = app_name
        self.process_date = process_date
        self.success = 0
        self.process_start_time = (datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        self.error_message = ''
        self.before_count = 0
        self.after_count = 0
        self.insert_count = 0
        self.update_count = 0
        self.delete_count = 0
        
    def set_after_count(self, after_count):
        """
        set_after_count [紀錄最後產生資料筆數]
        Arguments:
            after_count {[int]} -- [最終筆數]
        """
        self.after_count = after_count
    
    def set_before_count(self, before_count):
        """
        set_before_count [設定來源資料筆數]

        Arguments:
            before_count {[int]} -- [來源資料筆數]
        """
        self.before_count = before_count

    def set_insert_count(self, insert_count):
        """
        set_insert_count [設定插入資料筆數]
        """
        
        self.insert_count = insert_count

    def set_update_count(self, update_count):
        """
        set_update_count [設定更新資料筆數]
        """
        
        self.update_count = update_count
    
    def set_delete_count(self, delete_count):
        """
        set_delete_count [設定刪除總rows數量]

        Arguments:
            delete_count {[int]} -- [刪除的rows數]]
        """
        self.delete_count = delete_count

    def raise_error(self, error_message):
        """
        raise_error [當發生Error raise時，透過此程式紀錄error message並且存入資料庫]

        Arguments:
            error_message {[str]} -- [錯誤訊息]
        """
        self.error_message = self.error_message+' '+error_message

    def get_insert_sql(self):
        """
        insert_sql [取得insert sql]

        Returns:
            [string] -- [sql_statement]
        """
        return "INSERT INTO {0} ({1}) " "VALUES ({2})".format(
            self.PROCESS_LOG_DATABASE + "." + self.insert_table_name,
            ",".join(self.column_list),
            ",".join(["%s" for _ in range(len(self.column_list))]),
        )

    def get_insert_data(self):
        """
        get_insert_data [取得insert資料]

        Returns:
            [tuple] -- [最後insert_sql需要接收的tuple]
        """
        return (
            self.app_name,
            self.success,
            self.process_date,
            self.process_start_time,
            (datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S'),
            self.error_message,
            self.before_count,
            self.after_count,
            self.insert_count,
            self.update_count,
            self.delete_count,   
        )   
    
    def insert_to_log_record(self):
        """
        insert_to_log_record [
            將物件資料新增到db table中]
        """
        conn,cursor,engine=get_sql(connection.loc['host','value'],
                                   int(connection.loc['port','value']),
                                   connection.loc['user','value'],
                                   connection.loc['password','value'],
                                   self.PROCESS_LOG_DATABASE)
        
        cursor.execute(self.get_insert_sql(), [str(v) if isinstance(v,str) else int(v) for v in self.get_insert_data()])
        conn.commit()
        
        conn.close()