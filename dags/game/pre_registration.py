import requests
import pandas as pd
import datetime
import re
import time
from bs4 import BeautifulSoup
from common.config import credential_path, sql_configure_path, discord_token_path, chrome_driver_path
from lib.google import google_search
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from lib.log_process_execution import BaseLogRecord
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

app_name = 'game_pre_registration'
database_name = 'game'
table_name = 'pre_registration'

# Google Play 事前登錄頁面固定 URL，避免依賴搜尋結果
GOOGLE_PLAY_PRE_REG_URL = 'https://play.google.com/store/apps/collection/promotion_3000000d51_pre_registration_games?hl=zh_TW'

def _get_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    service = Service(chrome_driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

def fetch_search_results(**kwargs):
    api_key = pd.read_csv(credential_path, index_col='name').loc['google_search_token', 'value']
    cx = '00e7159cbe5ec4bd2'
    query = "google play pre registration"
    results = google_search(api_key, cx, query)
    kwargs['ti'].xcom_push(key='search_results', value=results)

def parse_search_results(**kwargs):
    results = kwargs['ti'].xcom_pull(key='search_results', task_ids='fetch_search_results') or []
    url = next(
        (r['link'] for r in results if 'promotion_3000000d51_pre_registration_games' in r.get('link', '')),
        None
    )
    url = url or GOOGLE_PLAY_PRE_REG_URL
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

def fetch_google_play_pre_registration(**kwargs):
    url = kwargs['ti'].xcom_pull(key='pre_registration_url', task_ids='parse_search_results') or GOOGLE_PLAY_PRE_REG_URL
    games = []

    try:
        driver = _get_chrome_driver()
        driver.get(url)

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="listitem"]'))
        )

        # 捲動載入 lazy loading 內容
        last_height = driver.execute_script('return document.body.scrollHeight')
        for _ in range(5):
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
            new_height = driver.execute_script('return document.body.scrollHeight')
            if new_height == last_height:
                break
            last_height = new_height

        soup = BeautifulSoup(driver.page_source, 'lxml')
        domain_url = 'https://play.google.com'

        for part in soup.find_all('div', {'role': 'listitem'}):
            img_tag = part.find('img')
            if not img_tag:
                continue

            title = img_tag.get('alt', '').replace('Icon image ', '').strip()
            link_tag = part.find('a')
            if not link_tag:
                continue

            href = link_tag.get('href', '').strip()
            game_url = (domain_url + href) if href.startswith('/') else href
            if 'id=' not in game_url:
                continue

            img = img_tag.get('srcset', '').strip() or img_tag.get('src', '').strip()
            game_id = game_url.split('id=')[-1].split('&')[0]

            if title and game_url and game_id:
                games.append({
                    'title': title,
                    'game_url': game_url,
                    'img': img,
                    'game_id': game_id,
                    'source': 'google_play'
                })
    except Exception as e:
        print(f'Google Play 事前登陸收集失敗: {e}')
    finally:
        if 'driver' in locals():
            driver.close()
            driver.quit()
    
    return games

def fetch_qooapp_pre_registration():
    url = 'https://news.qoo-app.com/tag/%E4%BA%8B%E5%89%8D%E7%99%BB%E9%8C%84'
    games = []
    
    try:
        driver = _get_chrome_driver()
        driver.get(url)
        
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        domain_url = 'https://news.qoo-app.com'
        
        article_items = soup.find_all('article')
        print(f'QooApp：找到 {len(article_items)} 個 article 元素')
        
        if not article_items:
            article_items = soup.find_all('div', class_=re.compile(r'article|post|news'))
            print(f'QooApp：找到 {len(article_items)} 個包含 article/post/news 的 div 元素')
        
        for item in article_items[:50]:
            try:
                link_tag = item.find('a')
                if not link_tag:
                    continue
                    
                game_url = link_tag.get('href', '').strip()
                if not game_url:
                    continue
                    
                if not game_url.startswith('http'):
                    game_url = domain_url + game_url if game_url.startswith('/') else domain_url + '/' + game_url
                
                title_tag = item.find('h2') or item.find('h3') or item.find('h1')
                if not title_tag:
                    title_tag = link_tag.find('h2') or link_tag.find('h3') or link_tag.find('h1')
                if not title_tag:
                    title_tag = link_tag
                    
                title = title_tag.get_text(strip=True) if title_tag else ''
                if not title:
                    continue
                
                img_tag = item.find('img')
                img = ''
                if img_tag:
                    img = img_tag.get('src', '') or img_tag.get('data-src', '') or img_tag.get('data-lazy-src', '')
                    if img and not img.startswith('http'):
                        img = domain_url + img if img.startswith('/') else domain_url + '/' + img
                
                game_id = re.search(r'/(\d+)/', game_url)
                game_id = game_id.group(1) if game_id else game_url.split('/')[-1].replace('.html', '').replace('/', '_')
                
                if title and game_url:
                    games.append({
                        'title': title,
                        'game_url': game_url,
                        'img': img,
                        'game_id': f'qooapp_{game_id}',
                        'source': 'qooapp'
                    })
            except Exception as e:
                print(f'QooApp 單筆處理失敗: {e}')
                continue
        
        print(f'QooApp：收集到 {len(games)} 筆遊戲')
    except Exception as e:
        print(f'QooApp 事前登陸收集失敗: {e}')
    finally:
        if 'driver' in locals():
            driver.close()
            driver.quit()
    
    return games

def fetch_game_details(**kwargs):
    log_record = BaseLogRecord(process_date=(datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                               app_name=app_name)
    
    all_games = []
    
    try:
        google_play_games = fetch_google_play_pre_registration(**kwargs)
        all_games.extend(google_play_games)
        print(f'Google Play 收集到 {len(google_play_games)} 筆遊戲')
        
        qooapp_games = fetch_qooapp_pre_registration()
        all_games.extend(qooapp_games)
        print(f'QooApp 收集到 {len(qooapp_games)} 筆遊戲')
        
        if not all_games:
            log_record.raise_error('事前登陸遊戲，所有來源都未收集到資料')
        
        df = pd.DataFrame(all_games)
        if not df.empty:
            df = df.drop_duplicates(subset=['game_id'], keep='first')
            df = df.drop_duplicates(subset=['game_url'], keep='first')
            all_games = df.to_dict('records')
            print(f'去重後總共 {len(all_games)} 筆遊戲')
    except Exception as e:
        log_record.raise_error(f'事前登陸遊戲，取得遊戲資料失敗 : {e}')
        raise e
        
    kwargs['ti'].xcom_push(key='game_details', value=all_games)

def write_to_sql(**kwargs):
    games = kwargs['ti'].xcom_pull(key='game_details', task_ids='fetch_game_details')
    df = pd.DataFrame(games)
    
    connection = pd.read_csv(sql_configure_path, index_col='name')
    conn, cursor, engine = get_sql(connection.loc['host', 'value'],
                                   int(connection.loc['port', 'value']),
                                   connection.loc['user', 'value'],
                                   connection.loc['password', 'value'], database_name)
    
    log_record = BaseLogRecord(process_date=(datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                               app_name=app_name)
    
    new_df = pd.DataFrame()
    before_count = 0
    try:
        try:
            before_count = pd.read_sql_query(f'SELECT COUNT(id) AS N FROM {table_name}', engine)['N'].iloc[0]
        except Exception:
            before_count = 0
        
        log_record.set_before_count(before_count)
        print(f'事前登陸遊戲，處理前總數為 : {before_count}')
        
        if not df.empty:
            df = df.drop_duplicates(subset=['game_id'], keep='first')
            df = df.drop_duplicates(subset=['game_url'], keep='first')
            
            df['title'] = df['title'].astype(str).str[:255]
            
            print(f'寫入前去重後剩餘 {len(df)} 筆遊戲')
            
            try:
                existing_ids = pd.read_sql(f'SELECT game_id FROM {table_name}', con=engine)
                new_df = df[~df['game_id'].isin(existing_ids['game_id'])]
            except Exception:
                new_df = df
            
            if not new_df.empty:
                new_df.to_sql(table_name, con=engine, if_exists='append', index=False)
                new_df.to_sql(f'{table_name}_tmp', con=engine, if_exists='replace', index=False)
        
        log_record.set_insert_count(len(new_df))
        log_record.set_delete_count(0)
        log_record.set_after_count(before_count + len(new_df))
        log_record.set_update_count(0)
        
    except Exception as e:
        log_record.raise_error(f'事前登陸遊戲，寫入資料庫失敗 : {e}')
        raise e
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()
        kwargs['ti'].xcom_push(key='pre_registration_count', value=len(new_df))

def notify_new_games(**kwargs):
    connection = pd.read_csv(sql_configure_path, index_col='name')
    conn, cursor, engine = get_sql(connection.loc['host', 'value'],
                                   int(connection.loc['port', 'value']),
                                   connection.loc['user', 'value'],
                                   connection.loc['password', 'value'], database_name)
    
    token = pd.read_csv(discord_token_path, encoding='utf_8_sig', index_col='name')
    token = token.loc['遊戲', 'token']
    notify = DiscordNotify()
    notify.webhook_url = token
    
    try:
        try:
            new_games_df = pd.read_sql('SELECT * FROM pre_registration_tmp', con=engine)
            for idx, (_, game) in enumerate(new_games_df.iterrows()):
                source_name = 'QooApp' if game.get('source') == 'qooapp' else 'Google Play'
                message = f"【{source_name}】\n標題: {game['title']}\n連結: {game['game_url']}\n"
                notify.notify(message)
        except Exception as e:
            print(f'事前登陸遊戲，通知失敗（可能沒有新遊戲）: {e}')
    finally:
        conn.close()
        engine.dispose()
        