import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
from common.config import sql_configure_path, discord_token_path
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from lib.log_process_execution import BaseLogRecord
from urllib.parse import quote, unquote

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

def extract_card_info(part):
    # 提取卡片名稱(標題)
    card_name = part.find('h3').text.strip()
    if not card_name:
        return 

    card_id = quote(card_name)
    img = part.find('img').get('src').strip()
    # 提取關於這張卡片的標籤
    about_tags = []
    about_elements = part.find_all('div', class_='inline-flex mb-0.5 items-center bg-white shadow-1')
    for element in about_elements:
        tag_text = element.find('p').text.strip()
        about_tags.append(tag_text)
    about_tags_str = '; '.join(about_tags)  # 合併標籤

    # 提取現金回饋資訊、保險回饋、國外回饋、line point回饋...等資訊
    reward_info = {}
    reward_elements = part.find_all('div', {'data-selector-type': 'productAttribute'})
    for element in reward_elements:
        title = element.find('dt').text.strip()
        value = element.find('span', {'data-property-name': 'attributeValue'}).text.strip()
        if not title or not value:
            continue
        reward_info[title] = value

    # 提取首刷禮優惠活動詳情及照片
    signup_bonus_data = []
    signup_bonus_elements = part.find_all('div', class_='offer')
    for signup_bonus in signup_bonus_elements:
        signup_bonus_title = signup_bonus.find('span', {'data-property-name': 'offer-gift-title'}).text.strip()
        signup_bonus_end_date = signup_bonus.find('p', string='活動截止日').find_next('p').text.strip()
        signup_bonus_image = signup_bonus.find('img')['src']  # 獲取優惠活動的圖片 URL
        signup_bonus_data.append(f"優惠: {signup_bonus_title}, 截止日期: {signup_bonus_end_date}, 圖片: {signup_bonus_image}")

    signup_bonus_str = '\\n'.join(signup_bonus_data)  # 合併優惠活動

    # 申請url
    sub_part = part.find(class_='card-detail-button__apply')
    apply_url = sub_part.get('data-property-target') if sub_part else ''

    # 建立 DataFrame
    card_data = {
        'card_id': card_id,
        'card_name': card_name,
        'keyword': about_tags_str,
        'rewards': json.dumps(reward_info),
        'signup_bonus': signup_bonus_str,
        'img': img,
        'apply_url': apply_url
    }

    return card_data  # 返回 DataFrame 格式

def fetch_card_data(**kwargs):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    url = 'https://www.money101.com.tw/%E4%BF%A1%E7%94%A8%E5%8D%A1/%E5%85%A8%E9%83%A8'
    response = requests.get(url, headers=headers, timeout=20)
    soup = BeautifulSoup(response.text, 'lxml')

    card_section_class_html = 'flex flex-col tws-gap-1 tws-p-1'
    data = []
    for card_part, detail_part in zip(soup.find_all(class_=card_section_class_html), soup.find_all('div', {'data-module-id':'results-page-product-card'})):
        card_dict=extract_card_info(card_part)
        if card_dict:
            card_dict['detail_info'] = detail_part.find(class_='list-disc list-inside tws-px-2 hidden md:block').text.strip()
            data.append(card_dict)

    kwargs['ti'].xcom_push(key='credit_card_data', value=data)

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