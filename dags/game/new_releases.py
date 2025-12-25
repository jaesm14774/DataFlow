import pandas as pd
import datetime
import requests
import re
from bs4 import BeautifulSoup
from common.config import sql_configure_path, discord_token_path, chrome_driver_path
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from lib.log_process_execution import BaseLogRecord
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

database_name = 'game'

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

def fetch_google_play_new_releases():
    url = 'https://play.google.com/store/apps/collection/promotion_3000791_new_releases_games?hl=zh_TW'
    games = []
    
    try:
        driver = _get_chrome_driver()
        driver.get(url)
        
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[role="listitem"]'))
        )
        
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
                
            game_url = domain_url + link_tag.get('href', '').strip()
            img = img_tag.get('srcset', '').strip()
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
        print(f'Google Play 收集失敗: {e}')
    finally:
        if 'driver' in locals():
            driver.close()
            driver.quit()
    
    return games

def fetch_bahamut_new_releases():
    url = 'https://gnn.gamer.com.tw/'
    games = []
    
    try:
        driver = _get_chrome_driver()
        driver.get(url)
        
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        domain_url = 'https://gnn.gamer.com.tw'
        
        news_items = soup.find_all('div', class_='GN-lbox3B')
        print(f'巴哈姆特：找到 {len(news_items)} 個 GN-lbox3B 元素')
        if not news_items:
            news_items = [item for item in soup.find_all('div') if item.get('class') and any('GN-lbox' in str(c) for c in item.get('class', []))]
            print(f'巴哈姆特：找到 {len(news_items)} 個包含 GN-lbox 的 div 元素')
        if not news_items:
            link_items = soup.find_all('a', href=re.compile(r'/gnn/detail/\d+'))
            print(f'巴哈姆特：找到 {len(link_items)} 個符合 /gnn/detail/ 的連結')
            news_items = [item.parent for item in link_items if item.parent]
        
        for item in news_items[:30]:
            try:
                link_tag = item.find('a')
                if not link_tag:
                    continue
                    
                game_url = link_tag.get('href', '').strip()
                if not game_url:
                    continue
                    
                if not game_url.startswith('http') and not game_url.startswith('//gnn.gamer.com.tw'):
                    game_url = domain_url + game_url if game_url.startswith('/') else domain_url + '/'
                elif game_url.startswith('//gnn.gamer.com.tw'):
                    game_url = 'https:' + game_url
                else:
                    game_url = game_url
                
                title_tag = item.find('p', class_='GN-lbox3B_title')
                if not title_tag:
                    title_tag = item.find('h3') or item.find('h2') or item.find('h1')
                if not title_tag:
                    title_tag = link_tag.find('span')
                    if not title_tag:
                        title_divs = link_tag.find_all('div')
                        for div in title_divs:
                            if div.get('class') and any('title' in str(c).lower() for c in div.get('class', [])):
                                title_tag = div
                                break
                if not title_tag:
                    title_tag = link_tag
                    
                title = title_tag.get_text(strip=True) if title_tag else ''
                if not title:
                    continue
                
                img_tag = item.find('img')
                img = ''
                if img_tag:
                    img = img_tag.get('src', '') or img_tag.get('data-src', '')
                    if img and not img.startswith('http'):
                        img = domain_url + img if img.startswith('/') else domain_url + '/' + img
                
                game_id = re.search(r'/(\d+)\.html', game_url)
                game_id = game_id.group(1) if game_id else game_url.split('/')[-1].replace('.html', '')
                
                if title and game_url:
                    games.append({
                        'title': title,
                        'game_url': game_url,
                        'img': img,
                        'game_id': f'bahamut_{game_id}',
                        'source': 'bahamut'
                    })
            except Exception as e:
                print(f'巴哈姆特單筆處理失敗: {e}')
                continue
    except Exception as e:
        print(f'巴哈姆特收集失敗: {e}')
    finally:
        if 'driver' in locals():
            driver.close()
            driver.quit()
    
    return games

def fetch_4gamers_new_releases():
    url = 'https://www.4gamers.com.tw/news'
    games = []
    
    try:
        driver = _get_chrome_driver()
        driver.get(url)
        
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, 'body'))
        )
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        domain_url = 'https://www.4gamers.com.tw'
        
        news_items = soup.find_all('article', class_='news-item')
        print(f'4Gamers：找到 {len(news_items)} 個 news-item article 元素')
        if not news_items:
            news_items = soup.find_all('article')
            print(f'4Gamers：找到 {len(news_items)} 個 article 元素')
        if not news_items:
            news_items = [item for item in soup.find_all('div') if item.get('class') and any('news' in str(c).lower() or 'article' in str(c).lower() for c in item.get('class', []))]
            print(f'4Gamers：找到 {len(news_items)} 個包含 news/article 的 div 元素')
        if not news_items:
            link_items = soup.find_all('a', href=re.compile(r'/news/detail/\d+'))
            print(f'4Gamers：找到 {len(link_items)} 個符合 /news/detail/ 的連結')
            news_items = [item.parent for item in link_items if item.parent]
        
        for item in news_items[:30]:
            try:
                link_tag = item.find('a')
                if not link_tag:
                    continue
                    
                game_url = link_tag.get('href', '').strip()
                if not game_url:
                    continue
                    
                if not game_url.startswith('http'):
                    game_url = domain_url + game_url if game_url.startswith('/') else domain_url + '/' + game_url
                
                title_tag = item.find('h3') or item.find('h2') or item.find('h1')
                if not title_tag:
                    title_tag = link_tag.find('span')
                    if not title_tag:
                        title_divs = link_tag.find_all('div')
                        for div in title_divs:
                            if div.get('class') and any('title' in str(c).lower() for c in div.get('class', [])):
                                title_tag = div
                                break
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
                game_id = game_id.group(1) if game_id else game_url.split('/')[-1]
                
                if title and game_url:
                    games.append({
                        'title': title,
                        'game_url': game_url,
                        'img': img,
                        'game_id': f'4gamers_{game_id}',
                        'source': '4gamers'
                    })
            except Exception as e:
                print(f'4Gamers 單筆處理失敗: {e}')
                continue
    except Exception as e:
        print(f'4Gamers 收集失敗: {e}')
    finally:
        if 'driver' in locals():
            driver.close()
            driver.quit()
    
    return games

def fetch_new_releases_details(**kwargs):
    app_name = 'game_new_releases'
    log_record = BaseLogRecord(process_date=(datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                               app_name=app_name)
    
    all_games = []
    
    try:
        google_play_games = fetch_google_play_new_releases()
        all_games.extend(google_play_games)
        print(f'Google Play 收集到 {len(google_play_games)} 筆遊戲')
        
        bahamut_games = fetch_bahamut_new_releases()
        all_games.extend(bahamut_games)
        print(f'巴哈姆特收集到 {len(bahamut_games)} 筆遊戲')
        
        gamers_games = fetch_4gamers_new_releases()
        all_games.extend(gamers_games)
        print(f'4Gamers 收集到 {len(gamers_games)} 筆遊戲')
        
        if not all_games:
            log_record.raise_error('新上架遊戲，所有來源都未收集到資料')
        
        df = pd.DataFrame(all_games)
        if not df.empty:
            df = df.drop_duplicates(subset=['game_id'], keep='first')
            df = df.drop_duplicates(subset=['game_url'], keep='first')
            all_games = df.to_dict('records')
            print(f'去重後總共 {len(all_games)} 筆遊戲')
    except Exception as e:
        log_record.raise_error(f'新上架遊戲，取得遊戲資料失敗 : {e}')
        raise e
        
    kwargs['ti'].xcom_push(key='new_releases_details', value=all_games)

def write_new_releases_to_sql(**kwargs):
    games = kwargs['ti'].xcom_pull(key='new_releases_details', task_ids='fetch_new_releases_details')
    df = pd.DataFrame(games)
    
    app_name = 'game_new_releases'
    table_name = 'new_releases'
    
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
        print(f'新上架遊戲，處理前總數為 : {before_count}')
        
        if not df.empty:
            df = df.drop_duplicates(subset=['game_id'], keep='first')
            df = df.drop_duplicates(subset=['game_url'], keep='first')
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
        log_record.raise_error(f'新上架遊戲，寫入資料庫失敗 : {e}')
        raise e
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()
        kwargs['ti'].xcom_push(key='new_releases_count', value=len(new_df))

def notify_new_releases(**kwargs):
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
            new_games_df = pd.read_sql('SELECT * FROM new_releases_tmp', con=engine)
            for idx, (_, game) in enumerate(new_games_df.iterrows()):
                source_name = '巴哈姆特' if game.get('source') == 'bahamut' else '4Gamers' if game.get('source') == '4gamers' else '新上架遊戲'
                message = f"【{source_name}】\n標題: {game['title']}\n連結: {game['game_url']}\n"
                notify.notify(message)
        except Exception as e:
            print(f'新上架遊戲，通知失敗（可能沒有新遊戲）: {e}')
    finally:
        conn.close()
        engine.dispose()

