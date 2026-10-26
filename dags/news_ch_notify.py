import pandas as pd, datetime, re
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from common.config import *
from lib.get_sql import *
from lib.notify import DiscordNotify

default_args = {
    'owner':'jaesm14774',
    'start_date':datetime.datetime(2022, 12, 16),
    'email':[
    'jaesm14774@gmail.com'],
    'email_on_failure':False,
    'execution_timeout':datetime.timedelta(seconds=1000),
    'retries':1,
    'retry_delay':datetime.timedelta(minutes=2),
    'tags':'Chinese news of notification'
}
token = pd.read_csv(discord_token_path, encoding='utf_8_sig', index_col='name')
token = token.loc['程式執行狀態', 'token']
notify = DiscordNotify()
notify.webhook_url = token

token = pd.read_csv(discord_token_path, encoding='utf_8_sig', index_col='name')
token = token.loc['新聞即時通', 'token']
news_ch_notify = DiscordNotify()
news_ch_notify.webhook_url = token

NOTIFICATION_LOOKUP_INDEX = 'ix_news_notification_source_article'

def _ensure_notification_lookup_index(cursor):
    cursor.execute(
        'SHOW INDEX FROM notification_record WHERE Key_name=%s',
        (NOTIFICATION_LOOKUP_INDEX,),
    )
    if cursor.fetchone() is None:
        cursor.execute(
            'ALTER TABLE notification_record '
            f'ADD INDEX {NOTIFICATION_LOOKUP_INDEX} (source, article_id), '
            'ALGORITHM=INPLACE, LOCK=NONE'
        )

def _load_enabled_holding_names(engine):
    all_code = pd.read_sql_query(
        'SELECT 證券代號, 證券名稱 FROM stock.全證券代碼相關資料表', engine
    )
    holdings = pd.read_sql_query(
        'SELECT 持股清單 FROM stock.持股清單 WHERE notify_status=1', engine
    )
    enabled_codes = set(holdings['持股清單'].dropna().astype(str).str.strip())
    all_code['證券代號'] = all_code['證券代號'].fillna('').astype(str).str.strip()
    all_code['證券名稱'] = all_code['證券名稱'].fillna('').astype(str).str.strip()
    return all_code.loc[all_code['證券代號'].isin(enabled_codes), '證券名稱'].tolist()

def split_content_into_chunks(content, chunk_size=500):
    """
    將內容分割成指定大小的塊，以方便發送通知。
    
    :param content: 要分割的內容
    :param chunk_size: 每個塊的字符數
    :return: 分割後的內容塊列表
    """
    return [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]

def notify_about_news(news_df):
    """
    對給定的新聞DataFrame進行discord通知。
    
    :param news_df: 新聞DataFrame，包含source、created_at和content等列
    """
    for _, row in news_df.iterrows():
        content_chunks = split_content_into_chunks(row['content'])
        news_ch_notify.notify(msg=(str(row['source']) + '\n' + str(row['created_at']) + ':' + row['title']))
        for chunk in content_chunks:
            news_ch_notify.notify(msg=f"\n{chunk}")
        
        news_ch_notify.notify(msg=row['article_url'])


def count_keywords_presence(content, keywords):
    """
    計算給定內容中出現的指定關鍵字的數量。
    
    :param content: 要檢查的內容
    :param keywords: 關鍵字列表
    :return: 出現關鍵字的數量
    """
    return sum(keyword in content for keyword in keywords)

def keyword_exist_in_text(content, keywords):
    return all(keyword in content for keyword in keywords)

def _news_notify_with_engine(codes, restricted_conditions, keyword_requirements,
                             required_keyword_count, must_exist_keyword_list, engine):
    """
    根據股票號碼或希望追蹤的關鍵字以及新聞的類別，決定是否使用discord Notify通知。
    只要文章中出現指定的關鍵字中的任意一定數量時即進行通知。
    """
    select_sql_query = """
        SELECT full.*
        FROM (
            SELECT tb.id
            FROM news tb
            LEFT JOIN notification_record notified
                   ON notified.source = tb.source
                  AND notified.article_id = tb.article_id
            WHERE notified.id IS NULL
        ) unseen
        INNER JOIN news full ON full.id = unseen.id
    """
    original_df = pd.read_sql_query(select_sql_query, engine).reset_index(drop=True)
    print(f"這次的中文新聞候選共有 {original_df.shape[0]}篇")

    if len(original_df) == 0:
        return
    
    if codes:
        code_pattern = '|'.join(map(str, codes)) + '|申報轉讓|成分股'
        code_info = original_df[original_df.content.str.contains(code_pattern, regex=True)]
        remain_df = original_df[~original_df.article_id.isin(code_info.article_id)]
        print(f'notify stock code : {code_info.shape[0]}')
    
    if keyword_requirements:
        filter_keyword_frequency = remain_df['content'].apply(lambda x: count_keywords_presence(x, keyword_requirements) >= required_keyword_count)
        keyword_info = remain_df[filter_keyword_frequency]
        
        if len(must_exist_keyword_list) > 0 and len(keyword_info) > 0:
            filter_exist_keyword = keyword_info['content'].apply(lambda x: keyword_exist_in_text(x, must_exist_keyword_list))
            keyword_info=keyword_info[filter_exist_keyword]
        remain_df = remain_df[~remain_df.article_id.isin(keyword_info.article_id)]
        print(f'Required keyword : {keyword_info.shape[0]}')
    
    notify_about_news(code_info)
    notify_about_news(keyword_info)
    
    for i, restrict in enumerate(restricted_conditions):
        restrict_source = restrict.get('source')
        restrict_category = restrict.get('category', [])
        select_df = remain_df.copy()
        if restrict_source:
            select_df = select_df[select_df.source == restrict_source]
            if len(restrict_category) > 0:
                select_df = select_df[select_df.category.isin(restrict_category)]
        print(f'Required restricted condition : {select_df.shape[0]}')
        notify_about_news(select_df)
        print(f"已通知完 {i}th篩選條件")

    original_df.loc[:, ['article_id', 'source']].to_sql('notification_record', engine, index=False, if_exists='append')
    print('通知完成')

def news_notify(codes=None, restricted_conditions=None, keyword_requirements=None,
                required_keyword_count=2, must_exist_keyword_list=None):
    connection = pd.read_csv(sql_configure_path, index_col='name')
    conn, cursor, engine = get_sql(
        connection.loc['host', 'value'], int(connection.loc['port', 'value']),
        connection.loc['user', 'value'], connection.loc['password', 'value'],
        'news_ch',
    )
    try:
        _ensure_notification_lookup_index(cursor)
        if codes is None:
            codes = _load_enabled_holding_names(engine)
        if not codes:
            raise AssertionError('Done')
        return _news_notify_with_engine(
            codes=codes,
            restricted_conditions=restricted_conditions or [],
            keyword_requirements=keyword_requirements,
            required_keyword_count=required_keyword_count,
            must_exist_keyword_list=must_exist_keyword_list or [],
            engine=engine,
        )
    finally:
        conn.close()
        engine.dispose()

with DAG(
    dag_id='news_ch_notify',
    description='篩選尚未處理的新聞通知，依持股公司與關鍵字規則投遞並記錄結果。',
    default_args=default_args,
    schedule='30 1,4,6,10,14,22 * * *',
    catchup=False
) as dag:
    notify_part1 = PythonOperator(
        task_id='news_ch_notify',
        python_callable=news_notify,
        dag=dag,
        op_kwargs={
            'codes':None,
            'restricted_conditions':[
                {'source':'iThome','category':[]},
                {'source':'thenewslens','category':['科技', '財經']}
            ],
            'keyword_requirements': ['機票', '促銷', '購票', '開賣', '航線', '搶票'],
            'must_exist_keyword_list': ['機票']
        },
        on_failure_callback=notify.task_custom_failure_function
    )
    
    finish_task = BashOperator(task_id='run_after_news_ch_notification',
                               bash_command='echo "Notify done!"')
    
    notify_part1 >> finish_task
    
if __name__ == '__main__':
    dag.cli()
