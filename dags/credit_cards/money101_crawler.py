import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
from common.config import sql_configure_path, discord_token_path, chrome_driver_path
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from lib.log_process_execution import BaseLogRecord
from urllib.parse import quote, unquote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

app_name = 'credit_cards'
database_name = 'crawler'
table_name = 'credit_cards'
notification_table_name = 'credit_cards_notification'
source = 'Money101'
 
# 初始化 log
log_record = BaseLogRecord(process_date=(datetime.datetime.now() + datetime.timedelta(hours=8)).strftime('%Y-%m-%d'),
                           app_name=app_name)
 
connection = pd.read_csv(sql_configure_path, index_col='name')
conn, cursor, engine = get_sql(connection.loc['host', 'value'],
                               int(connection.loc['port', 'value']),
                               connection.loc['user', 'value'],
                               connection.loc['password', 'value'], database_name)
 
# 設定 Discord token
token = pd.read_csv(discord_token_path, encoding='utf_8_sig', index_col='name')
token = token.loc['信用卡', 'token']
notify = DiscordNotify()
notify.webhook_url = token

# 取得處理前的數量
before_count = pd.read_sql_query(f'SELECT COUNT(id) AS N FROM {table_name}', engine)['N'].iloc[0]
log_record.set_before_count(before_count)
print(f'信用卡，處理前總數為 : {before_count}')

def _get_chrome_driver():
    """初始化 Chrome WebDriver"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    service = Service(chrome_driver_path)
    return webdriver.Chrome(service=service, options=chrome_options)

def extract_card_info(part):
    """
    解析單張信用卡的 HTML 區塊
    """
    try:
        # 1. 提取卡片名稱 (使用 h3 標籤通常最穩定)
        card_name_el = part.find('h3')
        if not card_name_el:
            return None
        card_name = card_name_el.get_text(strip=True)
        card_id = quote(card_name)

        # 2. 提取卡面圖片
        img_el = part.find('img')
        img_url = img_el.get('src', '').strip() if img_el else ''

        # 3. 提取卡片關鍵字標籤 (Keywords)
        # Money101 的標籤通常放在 inline-flex 的 p 標籤中
        about_tags = []
        # 選取包含標籤文字的容器
        tag_elements = part.select('div.inline-flex p')
        for tag in tag_elements:
            text = tag.get_text(strip=True)
            if text:
                about_tags.append(text)
        about_tags_str = '; '.join(about_tags)

        # 4. 提取現金回饋/屬性資訊 (Rewards)
        reward_info = {}
        # 利用 data-selector-type 定位回饋區塊
        reward_elements = part.find_all('div', {'data-selector-type': 'productAttribute'})
        for element in reward_elements:
            title_el = element.find('dt')
            # 數值通常在屬性值標籤中
            value_el = element.find('span', {'data-property-name': 'attributeValue'})
            if title_el and value_el:
                title = title_el.get_text(strip=True)
                value = value_el.get_text(strip=True)
                reward_info[title] = value

        # 5. 提取首刷禮優惠活動 (Signup Bonus)
        signup_bonus_data = []
        # 尋找 offer 容器
        offer_elements = part.select('div.offer, div[data-module-id*="offer"]')
        for offer in offer_elements:
            # 禮品名稱
            title_node = offer.find('span', {'data-property-name': 'offer-gift-title'})
            bonus_title = title_node.get_text(strip=True) if title_node else "通路限定禮"
            
            # 活動截止日 (尋找包含"截止日"字樣的標籤)
            bonus_date = "詳見官網"
            date_label = offer.find('p', string=lambda x: x and '截止日' in x)
            if date_label:
                date_val = date_label.find_next('p')
                if date_val:
                    bonus_date = date_val.get_text(strip=True)
            
            # 優惠圖片
            offer_img_el = offer.find('img')
            offer_img = offer_img_el.get('src', '') if offer_img_el else ""
            
            signup_bonus_data.append(f"優惠: {bonus_title}, 截止日期: {bonus_date}, 圖片: {offer_img}")

        # 6. 申請 URL
        # 優先找 data-property-target，這是 Money101 跳轉連結的特徵
        apply_btn = part.find(attrs={"data-property-target": True})
        apply_url = apply_btn.get('data-property-target', '') if apply_btn else ''
        
        # 補底方案：若找不到則找區塊內第一個 a 標籤
        if not apply_url:
            a_tag = part.find('a', href=True)
            if a_tag:
                apply_url = a_tag['href']

        # 7. 詳細描述 (List items)
        detail_text = ""
        detail_el = part.find('ul', class_=lambda x: x and 'list-disc' in x)
        if detail_el:
            detail_text = detail_el.get_text(separator=" | ", strip=True)

        # 回傳結構化資料
        return {
            'card_id': card_id,
            'card_name': card_name,
            'keyword': about_tags_str,
            'rewards': json.dumps(reward_info, ensure_ascii=False),
            'signup_bonus': '\n'.join(signup_bonus_data),
            'detail_info': detail_text,
            'img': img_url,
            'apply_url': apply_url
        }
    except Exception as e:
        print(f"解析卡片時發生錯誤: {e}")
        return None

def fetch_card_data(**kwargs):
    """
    主進入點：爬取網頁並推送到 Airflow XCom
    """
    url = 'https://www.money101.com.tw/%E4%BF%A1%E7%94%A8%E5%8D%A1/%E5%85%A8%E9%83%A8'
    driver = None
    
    try:
        print(f"開始抓取 Money101 信用卡資料: {url}")
        driver = _get_chrome_driver()
        driver.get(url)
        
        # 等待頁面基本載入（等待 body 標籤）
        print("等待頁面基本載入...")
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        
        # 等待 JavaScript 執行完成
        print("等待 JavaScript 執行...")
        time.sleep(5)
        
        # 嘗試多種方式等待內容載入
        try:
            # 方法1: 等待特定元素
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-module-id="results-page-product-card"]')))
            print("找到目標元素: div[data-module-id='results-page-product-card']")
        except Exception:
            print("未找到預期元素，嘗試其他選擇器...")
            # 方法2: 等待任何包含信用卡相關內容的元素
            try:
                wait.until(lambda d: '信用卡' in d.page_source or 'card' in d.page_source.lower())
                print("頁面內容已載入")
            except Exception:
                print("頁面載入超時，繼續嘗試解析...")
        
        # 滾動頁面以觸發懶加載
        print("滾動頁面以載入更多內容...")
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
        
        # 再次滾動到底部
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        # 取得頁面 HTML
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')
        
        # 調試：檢查頁面標題和關鍵內容
        title = driver.title
        print(f"頁面標題: {title}")
        
        # 嘗試多種選擇器策略
        card_containers = []
        
        # 策略1: 原始選擇器
        containers1 = soup.find_all('div', {'data-module-id': 'results-page-product-card'})
        print(f"策略1 - 找到 {len(containers1)} 個容器 (data-module-id='results-page-product-card')")
        card_containers.extend(containers1)
        
        # 策略2: 尋找包含 h3 標籤的卡片容器
        if len(card_containers) == 0:
            containers2 = soup.find_all('div', class_=lambda x: x and ('card' in str(x).lower() or 'product' in str(x).lower()))
            print(f"策略2 - 找到 {len(containers2)} 個可能的卡片容器")
            # 過濾出包含 h3 標籤的容器
            containers2 = [c for c in containers2 if c.find('h3')]
            print(f"策略2 - 過濾後有 {len(containers2)} 個包含 h3 的容器")
            card_containers.extend(containers2)
        
        # 策略3: 直接尋找所有 h3 標籤，然後找其父容器
        if len(card_containers) == 0:
            h3_tags = soup.find_all('h3')
            print(f"策略3 - 找到 {len(h3_tags)} 個 h3 標籤")
            for h3 in h3_tags:
                # 向上尋找包含卡片資訊的父容器
                parent = h3.find_parent('div', class_=lambda x: x and x)
                if parent and parent not in card_containers:
                    card_containers.append(parent)
            print(f"策略3 - 找到 {len(card_containers)} 個可能的卡片容器")
        
        # 去重
        seen = set()
        unique_containers = []
        for container in card_containers:
            container_id = id(container)
            if container_id not in seen:
                seen.add(container_id)
                unique_containers.append(container)
        
        card_containers = unique_containers
        print(f"總共找到 {len(card_containers)} 個唯一的產品卡片容器")
        
        # 如果還是找不到，輸出頁面結構用於調試
        if len(card_containers) == 0:
            print("警告: 未找到任何卡片容器，輸出頁面結構用於調試...")
            # 尋找所有包含 data-module-id 的元素
            all_module_ids = soup.find_all(attrs={'data-module-id': True})
            print(f"頁面中所有包含 data-module-id 的元素: {[el.get('data-module-id') for el in all_module_ids[:10]]}")
        
        data = []
        for idx, container in enumerate(card_containers):
            card_dict = extract_card_info(container)
            if card_dict:
                data.append(card_dict)
                print(f"成功解析第 {idx+1} 張卡片: {card_dict.get('card_name', 'Unknown')}")
            else:
                print(f"警告: 第 {idx+1} 個容器解析失敗")
        
        print(f"成功解析 {len(data)} 張信用卡資訊")

        # 執行 Airflow XCom 推送
        if 'ti' in kwargs:
            kwargs['ti'].xcom_push(key='credit_card_data', value=data)
        
        print(f"成功抓取 {len(data)} 張信用卡資訊")
        return data

    except Exception as e:
        print(f"抓取網頁時發生錯誤: {e}")
        import traceback
        traceback.print_exc()
        
        # 即使出錯也嘗試保存頁面內容用於調試
        if driver:
            try:
                page_source = driver.page_source
                print(f"頁面長度: {len(page_source)} 字元")
                # 檢查頁面是否包含關鍵字
                if '信用卡' in page_source:
                    print("頁面包含'信用卡'關鍵字")
                if 'money101' in page_source.lower():
                    print("頁面包含'money101'關鍵字")
            except Exception:
                pass
        
        return []
    finally:
        if driver:
            driver.quit()

def write_to_sql(**kwargs):
    data = kwargs['ti'].xcom_pull(key='credit_card_data', task_ids='fetch_card_data')
    df = pd.DataFrame(data)
    df = df.drop_duplicates(['card_id'])
    df['source'] = source

    try:
        # 找出新資料，需要不在資料表 或是 距離資料庫建立時間超過6個月以上
        # 查詢現有的 card_id
        existing_ids = pd.read_sql(f'SELECT card_id, create_time FROM {table_name}', con=engine)
        existing_ids['create_time'] = pd.to_datetime(existing_ids['create_time'])
        existing_ids['create_time'] = existing_ids['create_time'].dt.date

        # 計算超過6個月的日期
        six_months_ago = datetime.datetime.now() - datetime.timedelta(days=180)
        six_months_ago = six_months_ago.date()

        # 過濾出不在歷史表裡面或是超過6個月的資料
        old_card_id = existing_ids[existing_ids['create_time'] <= six_months_ago]['card_id']
        old_data = df[df['card_id'].isin(old_card_id)]
        new_df = df[~df['card_id'].isin(existing_ids['card_id'])]
        new_df = pd.concat([new_df, old_data], ignore_index=True)

        new_df = new_df.drop_duplicates(['card_id'])
        new_df.loc[:, ['card_id', 'source']].to_sql(f'{table_name}_diff', engine, index=False, if_exists='replace')

        delete_sql=f"""
        delete target from {table_name} as target
        where exists (
        select *
        from {table_name}_diff source
        where source.card_id=target.card_id and source.source=target.source
        )
        """
        delete_count=cursor.execute(delete_sql)
        conn.commit()

        new_df.to_sql(table_name, engine, index=False, if_exists='append')

        log_record.set_insert_count(len(new_df))
        log_record.set_delete_count = len(old_data)
        log_record.set_after_count = before_count + len(new_df) - len(old_data)
        log_record.set_update_count = 0
    except Exception as e:
        log_record.error_message(f'信用卡，寫入資料庫失敗 : {e}')
        raise e
    finally:
        log_record.insert_to_log_record()
        conn.close()
        engine.dispose()

def notify_new_card(**kwargs):
    # 獲取已通知的資料
    already_notified = pd.read_sql(f'SELECT card_id, source, notification_time FROM {notification_table_name}', con=engine)
    
    # 獲取新資料
    df = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)

    # 計算超過6個月的日期
    six_months_ago = datetime.datetime.now() - datetime.timedelta(days=180)
    six_months_ago = six_months_ago.date()

    # 合併 DataFrame 以找出新資料
    merged_df = df.merge(already_notified, on=['card_id', 'source'], how='left', indicator=True)
    new_cards = merged_df[merged_df['_merge'] == 'left_only']

    # 準備批量插入的資料
    insert_data = []
    for _, row in new_cards.iterrows():
        create_time = row['create_time'].date()
        if create_time <= six_months_ago:
            continue  # 跳過超過六個月的資料

        # 構建通知訊息

        message = f"卡片名稱: {row['card_name']}\t {row['img']}\n"
        message += f"\n關於這張卡片的標籤:\n"
        message += f"\t- {row['keyword']}\n"
        message += f"\n回饋資訊:\n"
        
        for k, v in json.loads(row['rewards']).items():
            message += f"\t{k}: {v}\n"
        
        if row['signup_bonus']:
            message += f"優惠活動詳情:\n{row['signup_bonus']}\n\n"
        
        message += f"\n產品特色:\n\t{row['detail_info']}\n\n"
        
        if row['apply_url']:
            message += f"\n申請網址: {row['apply_url']}\n\n\n\n"
        
        notify.notify(message)
        
        # 收集要插入的資料
        insert_data.append((row['card_id'], row['source'], create_time, datetime.datetime.now()))

    # 批量插入通知記錄
    if insert_data:
        insert_sql = f"INSERT INTO {notification_table_name} (card_id, source, create_time, notification_time) VALUES (%s, %s, %s, %s)"
        cursor.executemany(insert_sql, insert_data)
        conn.commit()