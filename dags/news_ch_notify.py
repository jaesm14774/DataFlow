import pandas as pd, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from common.config import *
from lib.get_sql import *
from lib.notify import discord_notifier
from news_ch.relevance import (
    BUCKET_DEAL, build_holding_matchers, classify, format_alert, select_for_push,
)

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
notify=discord_notifier('程式執行狀態')

news_ch_notify=discord_notifier('新聞即時通')
save_money_ch_notify=discord_notifier('好康報')

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

def _load_enabled_holdings(engine):
    """回傳 [(代號, 名稱或None)]。ETF 在全證券代碼表常查不到名稱，因此代號一律保留供標題比對。"""
    all_code = pd.read_sql_query(
        'SELECT 證券代號, 證券名稱 FROM stock.全證券代碼相關資料表', engine
    )
    holdings = pd.read_sql_query(
        'SELECT 持股清單 FROM stock.持股清單 WHERE notify_status=1', engine
    )
    names = {
        str(c).strip(): (str(n).strip() if pd.notna(n) else '')
        for c, n in zip(all_code['證券代號'], all_code['證券名稱'])
    }
    codes = holdings['持股清單'].dropna().astype(str).str.strip()
    return [(c, names.get(c) or None) for c in codes if c]


def notify_about_news(news_df, classifications, channel):
    """每則新聞只送一則訊息（標籤 + 標題 + 短摘要 + 連結）。回傳送出失敗的列 index，供呼叫端不寫已讀、下次重試。"""
    failed = []
    for (idx, row), cls in zip(news_df.iterrows(), classifications):
        if channel.notify(msg=format_alert(row.to_dict(), cls)) is False:
            failed.append(idx)
    return failed


def _news_notify_with_engine(holdings, engine):
    """只通知四類資訊：我的持股、AI/科技、超級折扣(好康)、股市核心；其餘一律視為雜訊。
    所有候選（含未通知者）都寫入 notification_record，避免下次重複評估。
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

    matchers = build_holding_matchers(holdings)
    candidates = []
    for idx, row in original_df.iterrows():
        cls = classify(row['title'], row['content'], row['source'], row['category'], matchers)
        if cls is not None:
            candidates.append((idx, cls))
    print(f"相關候選 {len(candidates)} 篇（其餘視為雜訊）")

    selected = select_for_push([(original_df.loc[i].to_dict() | {'_idx': i}, c) for i, c in candidates])
    failed_idx = []
    for bucket, items in selected.items():
        df = original_df.loc[[row['_idx'] for row, _ in items]]
        channel = save_money_ch_notify if bucket == BUCKET_DEAL else news_ch_notify
        failed_idx.extend(notify_about_news(df, [c for _, c in items], channel))
        print(f"通知 {bucket}: {len(items)} 篇")

    # 送出失敗（如 Discord 429/5xx）的不寫已讀，下次排程重試；其餘（含雜訊、被上限擠掉者）全數記錄
    original_df.drop(index=failed_idx).loc[:, ['article_id', 'source']].to_sql('notification_record', engine, index=False, if_exists='append')
    print('通知完成')

def news_notify(holdings=None):
    conn, cursor, engine = get_sql_from_config('news_ch')
    try:
        _ensure_notification_lookup_index(cursor)
        if holdings is None:
            holdings = _load_enabled_holdings(engine)
        # 持股清單為空時仍要跑 AI / 好康 / 股市核心三類，不再中止
        return _news_notify_with_engine(holdings=holdings, engine=engine)
    finally:
        conn.close()
        engine.dispose()

with DAG(
    dag_id='news_ch_notify',
    description='篩選尚未處理的新聞，只推持股 / AI科技 / 超級折扣 / 股市核心，並記錄結果。',
    default_args=default_args,
    schedule='30 1,4,6,10,14,22 * * *',
    catchup=False
) as dag:
    notify_part1 = PythonOperator(
        task_id='news_ch_notify',
        python_callable=news_notify,
        dag=dag,
        op_kwargs={'holdings': None},
        on_failure_callback=notify.task_custom_failure_function
    )
    
    finish_task = BashOperator(task_id='run_after_news_ch_notification',
                               bash_command='echo "Notify done!"')
    
    notify_part1 >> finish_task
    
if __name__ == '__main__':
    dag.cli()
