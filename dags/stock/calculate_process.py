from stock.calculate_logic import *
from lib.common_tool import task_wrapper

START=1

#計算外、投本比
def calculate_outer_and_investment_ratio():
    if START:
        main_etl=StockCalculateOuterAndInvestmentRationProcess()
        main_etl.process()
    
    print('Done calculateion of outer_and_investment_ratio process')

#計算外投本比回測
def calculate_outer_and_investment_recommendation():
    if START:
        main_etl=StockCalculateOuterAndInvestmentRecommendationProcess()
        main_etl.process()
    
    print('Done calculateion of outer_and_investment_recommendation process')

#計算當日地緣券商回測
@task_wrapper
def calculate_broker_recommendation():
    if START:   
        main_etl=StockCalculateBrokerRecommendationProcess()
        main_etl.process()
    
    print('Done calculateion of calculate_broker_recommendation process')

#計算十日地緣券商回測
@task_wrapper
def calculate_broker_ten_days_recommendation():
    if START:
        main_etl=StockCalculateBrokerTenDaysRecommendationProcess()
        main_etl.process()
    
    print('Done calculateion of calculate_broker_ten_days_recommendation process')

#外本比推薦名單
def make_outer_and_investment_recommendation_list():
    if START:
        main_etl=StockMakeOuterAndInvestmentRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_outer_and_investment_recommendation_list process')

#地緣券商推薦名單
@task_wrapper
def make_broker_inout_recommendation_list():
    if START:
        main_etl=StockMakeBrokerInOutRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_broker_inout_recommendation_list process')

#計算主力買賣超
def calculate_mainforce_buy_sell():
    if START:
        main_etl=StockCalculateMainForceBuySellProcess()
        main_etl.process()
    
    print('Done calculateion of calculate_mainforce_buy_sell process')
    
#主力買賣超推薦名單
def make_mainforce_buy_sell_recommendation_list():
    if START:
        main_etl=StockMakeMainForceBuySellRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_mainforce_buy_sell_recommendation_list process')
    
#其他地緣券商推薦名單
@task_wrapper
def make_another_broker_inout_recommendation_list():
    if START:
        main_etl=StockMakeAnotherBrokerInOutRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_another_broker_inout_recommendation_list process')
    
 #外投本比累積推薦名單
def make_outer_and_investment_cumulate_recommendation_list():
    if START:
        main_etl=StockMakeOuterAndInvestmentCumulateRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_outer_and_investment_cumulate_recommendation_list process')   
    
 #大戶比累積推薦名單
def make_major_holder_recommendation_list():
    if START:
        main_etl=StockMajorHolderRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_major_holder_recommendation_list process')   
    
 #融資融券推薦名單
def make_margin_trade_recommendation_list():
    if START:
        main_etl=StockMarginTradeRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_margin_trade_recommendation_list process')

 #月營收資訊推薦名單
def make_month_revenue_recommendation_list():
    if START:
        main_etl=StockMonthRevenueRecommendationListProcess()
        main_etl.process()
        
    print('Done calculateion of make_month_revenue_recommendation_list process')