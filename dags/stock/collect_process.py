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
    
    print('Done collection of all code process')

#舊的代碼被刪除
def back_up(ti):
    if START:  
        delete_code=ti.xcom_pull(key='delete_code',task_ids=['stock_all_code_info_collect'])[0]
        print('delete_code',delete_code)    
        main_etl=StockBackUpProcess(sql_configure_path)
        for code in delete_code:
            main_etl.back_up(code)
        main_etl.end()
    
    print('Done back up process')
        
#有新的代碼
def renew_info(ti):
    if START:  
        new_code=ti.xcom_pull(key='new_code',task_ids=['stock_all_code_info_collect'])[0]
        print('new_code',new_code)    
        main_etl=StockRenwInfoProcess(sql_configure_path)
        for code in new_code:
            main_etl.renew_info(code)
        main_etl.end()
    print('Done of renew process')

#收集三大法人買賣超
def three_main_inverstor_collect(): 
    if START:     
        main_etl=StockThreeMainInvestorCollectProcess()
        main_etl.process()
        
    print('Done of collection for three main investor process')

#收集個股成交資訊
@task_wrapper
def all_code_daily_deal_info_collect(_type):
    if START:  
        main_etl=StockAllCodeDailyDealInfoCollectProcess(_type=_type)
        main_etl.process()
        
    print(f'Done of collection for {_type} code daily deal process')

#tpex trigger insert
def broker_inout_tpex_trigger_insert():
    if START:  
        main_etl=StockBrokerInOutTPEXTriggerInsertProcess()
        main_etl.process()
    
    print(f'Done of insert tpex trigger process')

#收集個股基本指標
def all_code_standard_metric_collect():
    if START:  
        main_etl=StockAllCodeStandardMetricCollectProcess()
        main_etl.process()
    
    print(f'Done of collection for all code standard metric process')

#收集當沖資訊
def day_trade_collect():
    if START:  
        main_etl=StockDayTradeCollectProcess()
        main_etl.process()
    
    print(f'Done of collection for day trade collect process')

#收集證交所券商進出
@task_wrapper
def broker_inout_twse_collect():
    if START:  
        main_etl=StockBrokerInOutTWSECollectProcess()
        main_etl.process()
    
    print(f'Done of collection for twse broker inout process')

#判斷是否台灣有開盤(所有假日、補班皆不會開盤)
def is_taiwan_stock_close(task_name='stock_all_code_info_collect'):
    connection=pd.read_csv(sql_configure_path,encoding='utf_8_sig',index_col='name')
    conn,cursor,engine=get_sql(connection.loc['host','value'],connection.loc['port','value'],
                               connection.loc['user','value'],connection.loc['password','value'],'holiday')
    
    today=(datetime.datetime.now()+datetime.timedelta(hours=8)).strftime('%Y-%m-%d')
    exist_ornot=pd.read_sql_query(f"select * from holiday.taiwan where now_date='{today}'",engine)
    print(exist_ornot)
    if len(exist_ornot) > 0 and not today.endswith('09-03'):
        return 'stock_collect_process_is_done'
    else:
        return task_name

#判斷今天是否為禮拜六(收集大戶比、融資券資訊、月營收資訊)
def is_weekend():
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
    
    print(f'Done of collection for major holder process')

#收集融資融券
@task_wrapper
def margin_trade_collect():
    if START:  
        main_etl=StockMarginTradeCollectProcess()
        main_etl.process()
    
    print(f'Done of collection for margin trade process')

#收集月營收資訊
@task_wrapper
def month_revenue_collect():
    if START:  
        main_etl=StockMonthRevenueCollectProcess()
        main_etl.process()
    
    print(f'Done of collection for month revenue process')

#暫停函數
def time_sleep():
    if START:
        while datetime.datetime.now().hour+8 <=19:
            time.sleep(600)
    
    print(f'Done of sleep')