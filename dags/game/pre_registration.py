import requests
import pandas as pd
import datetime
from bs4 import BeautifulSoup
from common.config import credential_path, sql_configure_path, discord_token_path, chrome_driver_path
from lib.google import google_search
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from lib.log_process_execution import BaseLogRecord
# 新增 Selenium 相關引入
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

app_name = 'game_pre_registration'
database_name = 'game'
table_name = 'pre_registration'

# 初始化 log
log_record = BaseLogRecord(process_date=(datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                           app_name=app_name)

# 連接 MySQL
# 讀取 SQL 配置
connection = pd.read_csv(sql_configure_path, index_col='name')
conn, cursor, engine = get_sql(connection.loc['host', 'value'],
                               int(connection.loc['port', 'value']),
                               connection.loc['user', 'value'],
                               connection.loc['password', 'value'], database_name)

# 設定 Discord token
token = pd.read_csv(discord_token_path, encoding='utf_8_sig', index_col='name')
token = token.loc['遊戲', 'token']
notify = DiscordNotify()
notify.webhook_url = token

# 取得處理前的數量
before_count = pd.read_sql_query(f'SELECT COUNT(id) AS N FROM {table_name}', engine)['N'].iloc[0]
log_record.set_before_count(before_count)
print(f'預約註冊遊戲，處理前總數為 : {before_count}')

def fetch_search_results(**kwargs):
    api_key = pd.read_csv(credential_path, index_col='name').loc['google_search_token', 'value']
    cx = '00e7159cbe5ec4bd2'
    query = "google play pre registration"
    results = google_search(api_key, cx, query)
    kwargs['ti'].xcom_push(key='search_results', value=results)

def parse_search_results(**kwargs):
    results = kwargs['ti'].xcom_pull(key='search_results', task_ids='fetch_search_results')
    url = next((result['link'] for result in results if result['title'] in ['Pre-registration games', 'Google Play']), None)
    kwargs['ti'].xcom_push(key='pre_registration_url', value=url)

# def fetch_game_details(**kwargs):
    # url = kwargs['ti'].xcom_pull(key='pre_registration_url', task_ids='parse_search_results')
    # headers = {
        # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        # 'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        # 'Accept-Encoding': 'gzip, deflate, br',
        # 'Connection': 'keep-alive',
        # 'Upgrade-Insecure-Requests': '1'
    # }
# 
    # domain_url = 'https://play.google.com'
    # try:
        # response = requests.get(url, headers=headers, timeout=20)
        # response.raise_for_status()
        # soup = BeautifulSoup(response.text, 'lxml')
        # games = []
        # for part in soup.find_all('div', {'role': 'listitem'}):
            # title = part.find('img').get('alt', '').replace('Icon image ', '').strip()
            # game_url = domain_url + part.find('a').get('href', '').strip()
            # img = part.find('img').get('srcset', '').strip()
            # game_id = game_url.split('id=')[-1] if 'id=' in game_url else ''
            # if title and game_url and img and game_id:
                # games.append({
                    # 'title': title,
                    # 'game_url': game_url,
                    # 'img': img,
                    # 'game_id': game_id,
                    # 'source': 'google_play'
                # })
    # except Exception as e:
        # log_record.error_message(f'預約註冊遊戲，取得遊戲資料失敗 : {e}')
        # raise e
    # kwargs['ti'].xcom_push(key='game_details', value=games)

def fetch_game_details(**kwargs):
    url = kwargs['ti'].xcom_pull(key='pre_registration_url', task_ids='parse_search_results')
    
    # 設定 Chrome 選項
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 無頭模式
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    try:
        # 初始化 WebDriver
        service = Service(chrome_driver_path)  # 確保 chromedriver 在系統路徑中
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 訪問網頁
        driver.get(url)
        
        # 等待頁面加載完成（等待 listitem 元素出現）
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="listitem"]'))
        )
        
        # 獲取頁面源碼並傳給 BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'lxml')
        
        domain_url = 'https://play.google.com'
        games = []
        for part in soup.find_all('div', {'role': 'listitem'}):
            title = part.find('img').get('alt', '').replace('Icon image ', '').strip()
            game_url = domain_url + part.find('a').get('href', '').strip()
            img = part.find('img').get('srcset', '').strip()
            game_id = game_url.split('id=')[-1] if 'id=' in game_url else ''
            if title and game_url and img and game_id:
                games.append({
                    'title': title,
                    'game_url': game_url,
                    'img': img,
                    'game_id': game_id,
                    'source': 'google_play'
                })
                
    except Exception as e:
        log_record.error_message(f'預約註冊遊戲，取得遊戲資料失敗 : {e}')
        raise e
    finally:
        driver.close()
        driver.quit()  # 確保關閉瀏覽器
        
    kwargs['ti'].xcom_push(key='game_details', value=games)

def write_to_sql(**kwargs):
    games = kwargs['ti'].xcom_pull(key='game_details', task_ids='fetch_game_details')
    df = pd.DataFrame(games)
    
    try:
        # 查詢現有的 game_id
        existing_ids = pd.read_sql('SELECT game_id FROM pre_registration', con=engine)
        new_df = df[~df['game_id'].isin(existing_ids['game_id'])]
        # 寫入新資料
        new_df.to_sql('pre_registration', con=engine, if_exists='append', index=False)
        new_df.to_sql('pre_registration_tmp', con=engine, if_exists='replace', index=False)
        log_record.set_insert_count(len(new_df))
        log_record.set_delete_count = 0
        log_record.set_after_count = before_count + len(new_df)
        log_record.set_update_count = 0
    except Exception as e:
        log_record.error_message(f'預約註冊遊戲，寫入資料庫失敗 : {e}')
        raise e
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()

def notify_new_games(**kwargs):
    # 獲取 new_games_df
    new_games_df =  pd.read_sql('SELECT * FROM pre_registration_tmp', con=engine)
    for _, game in new_games_df.iterrows():
        message = f"標題: {game['title']}\n連結: {game['game_url']}\n"
        notify.notify(message)
        