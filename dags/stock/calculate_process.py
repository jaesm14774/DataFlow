from stock.calculate_logic import *
from lib.common_tool import task_wrapper

START=1

#計算當日地緣券商回測
@task_wrapper
def calculate_broker_recommendation():
    if START:
        main_etl=StockCalculateBrokerRecommendationProcess()
        main_etl.process()

    print('[stock 計算] 地緣券商當日回測任務結束')

#計算十日地緣券商回測
@task_wrapper
def calculate_broker_ten_days_recommendation():
    if START:
        main_etl=StockCalculateBrokerTenDaysRecommendationProcess()
        main_etl.process()

    print('[stock 計算] 地緣券商十日回測任務結束')

#地緣券商推薦名單
@task_wrapper
def make_broker_inout_recommendation_list():
    if START:
        main_etl=StockMakeBrokerInOutRecommendationListProcess()
        main_etl.process()

    print('[stock 計算] 地緣券商推薦名單任務結束')

#計算主力買賣超
def calculate_mainforce_buy_sell():
    if START:
        main_etl=StockCalculateMainForceBuySellProcess()
        main_etl.process()

    print('[stock 計算] 主力買賣超計算任務結束')

#主力買賣超推薦名單
def make_mainforce_buy_sell_recommendation_list():
    if START:
        main_etl=StockMakeMainForceBuySellRecommendationListProcess()
        main_etl.process()

    print('[stock 計算] 主力買賣超推薦名單任務結束')

 #大戶比累積推薦名單
def make_major_holder_recommendation_list():
    if START:
        main_etl=StockMajorHolderRecommendationListProcess()
        main_etl.process()

    print('[stock 計算] 大戶比推薦名單任務結束')

 #融資融券推薦名單
def make_margin_trade_recommendation_list():
    if START:
        main_etl=StockMarginTradeRecommendationListProcess()
        main_etl.process()

    print('[stock 計算] 融資融券推薦名單任務結束')

 #月營收資訊推薦名單
def make_month_revenue_recommendation_list():
    if START:
        main_etl=StockMonthRevenueRecommendationListProcess()
        main_etl.process()

    print('[stock 計算] 月營收推薦名單任務結束')
