from stock.collect_logic import *
from lib.common_tool import task_wrapper
from lib.get_sql import get_sql

START=1

#收集全證券代碼
def all_code_info_collect(ti):
    if START:  
        main_etl=StockAllCodeInfoCollectProcess()
        main_etl.ti=ti
        main_etl.process()
    
    print('[stock DAG] 全證券代碼收集任務結束')

#舊的代碼被刪除
def back_up(ti):
    if START:  
        delete_code=ti.xcom_pull(key='delete_code',task_ids=['stock_all_code_info_collect'])[0]
        print(f'[stock DAG] 下市備份 | 待處理代號={delete_code}')
        main_etl=StockBackUpProcess(sql_configure_path)
        for code in delete_code:
            main_etl.back_up(code)
        main_etl.end()
    
    print('[stock DAG] 下市備份任務結束')
        
#有新的代碼
def renew_info(ti):
    if START:  
        new_code=ti.xcom_pull(key='new_code',task_ids=['stock_all_code_info_collect'])[0]
        print(f'[stock DAG] 新上市更新 | 待處理代號={new_code}')
        main_etl=StockRenwInfoProcess(sql_configure_path)
        for code in new_code:
            main_etl.renew_info(code)
        main_etl.end()
    print('[stock DAG] 新上市更新任務結束')

#收集三大法人買賣超
def three_main_inverstor_collect(): 
    if START:     
        main_etl=StockThreeMainInvestorCollectProcess()
        main_etl.process()
        
    print('[stock DAG] 三大法人收集任務結束')

#收集個股成交資訊
@task_wrapper
def all_code_daily_deal_info_collect(_type):
    if START:  
        main_etl=StockAllCodeDailyDealInfoCollectProcess(_type=_type)
        main_etl.process()
        
    print(f'[stock DAG] 個股成交收集結束 | 市場={_type}')

#tpex trigger insert
def broker_inout_tpex_trigger_insert():
    if START:  
        main_etl=StockBrokerInOutTPEXTriggerInsertProcess()
        main_etl.process()
    
    print('[stock DAG] 櫃買券商進出 trigger 寫入結束')

#收集個股基本指標
def all_code_standard_metric_collect():
    if START:  
        main_etl=StockAllCodeStandardMetricCollectProcess()
        main_etl.process()
    
    print('[stock DAG] 個股基本指標收集結束')

#收集當沖資訊
def day_trade_collect():
    if START:  
        main_etl=StockDayTradeCollectProcess()
        main_etl.process()
    
    print('[stock DAG] 當沖資訊收集結束')

#收集證交所券商進出
@task_wrapper
def broker_inout_twse_collect():
    if START:  
        main_etl=StockBrokerInOutTWSECollectProcess()
        main_etl.process()
    
    print('[stock DAG] 證交所券商進出收集結束')

#判斷是否台灣有開盤(所有假日、補班皆不會開盤)
def is_taiwan_stock_close(task_name='stock_all_code_info_collect'):
    connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')
    conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],
                               connection.loc['user','value'],connection.loc['password','value'],'holiday')
    
    today=(BACKFILL_DATE
           if BACKFILL_DATE
           else (datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d'))
    exist_ornot=pd.read_sql_query(f"select * from holiday.taiwan where now_date='{today}'",engine)
    is_holiday = len(exist_ornot) > 0 and not today.endswith('09-03')
    next_task = 'stock_collect_process_is_done' if is_holiday else task_name
    print(f'[開盤判斷] 日期={today} | 假日表命中={len(exist_ornot)} 列 | 視為休市={is_holiday} | 下一節點={next_task}')
    if is_holiday:
        return 'stock_collect_process_is_done'
    return task_name

#判斷今天是否為禮拜六(收集大戶比、融資券資訊、月營收資訊)
def is_weekend():
    if BACKFILL_DATE:
        now_time=datetime.datetime.strptime(BACKFILL_DATE, '%Y-%m-%d')
    else:
        now_time=(datetime.datetime.now()+datetime.timedelta(hours=8))
    if now_time.weekday() in [5]:
        return ['stock_major_holder_collect','stock_margin_trade_collect','stock_month_revenue_collect']
    else:
        return 'stock_collect_process_is_done'
    
#收集大戶比
def major_holder_collect():
    if START:  
        main_etl=StockMajorHolderCollectProcess()
        main_etl.process()
    
    print('[stock DAG] 大戶比收集結束')

#收集融資融券
@task_wrapper
def margin_trade_collect():
    if START:  
        main_etl=StockMarginTradeCollectProcess()
        main_etl.process()
    
    print('[stock DAG] 融資融券收集結束')

#收集月營收資訊
@task_wrapper
def month_revenue_collect():
    if START:  
        main_etl=StockMonthRevenueCollectProcess()
        main_etl.process()
    
    print('[stock DAG] 月營收收集結束')

#暫停函數
def time_sleep():
    if START:
        while datetime.datetime.now().hour+8 <=19:
            time.sleep(600)
    
    print('[stock DAG] 盤後等待結束')