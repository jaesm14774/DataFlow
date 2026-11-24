"""重磅消息輿情快訊：每 5 分鐘掃描官方/通訊社/總統貼文，達門檻立即推播。

與 market_signal_notify（驗證後的選股 digest）完全獨立：這裡推的是「市場風險事件」，
不是選股名單，也不改動 INTRADAY_PUSH_ENABLED 的既有政策。
每一則被評分的消息都會記進 breaking_news_events，供日後用市場反應檢驗門檻。
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable


def _throttled_failure_notice(context):
    """每 5 分鐘一輪的 DAG 若連續失敗，每小時最多通知一次，避免洗版『程式執行狀態』。"""
    interval_start = context.get("data_interval_start")
    if interval_start is not None and interval_start.minute >= 5:
        return
    from lib.notify import discord_notifier

    discord_notifier("程式執行狀態").task_custom_failure_function(context)


def _failure_notice(context):
    from lib.notify import discord_notifier

    discord_notifier("程式執行狀態").task_custom_failure_function(context)


def _scan():
    from breaking_news.pipeline import run_scan

    return run_scan()


def _settle():
    from breaking_news.pipeline import run_settle

    return run_settle()


with DAG(
    dag_id="breaking_news_alert",
    description="每 5 分鐘掃描重磅消息（Fed、川普貼文、戰爭/關稅/黑天鵝、關鍵人物異動），達門檻立即推播。",
    schedule=CronTriggerTimetable("*/5 * * * *", timezone="Asia/Taipei"),
    start_date=pendulum.datetime(2026, 11, 24, tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=1),
        "execution_timeout": pendulum.duration(minutes=5),
        "on_failure_callback": _throttled_failure_notice,
    },
    tags=["breaking-news", "sentiment", "alert"],
) as dag:
    PythonOperator(task_id="scan_and_push_breaking_news", python_callable=_scan)


with DAG(
    dag_id="breaking_news_reaction_settle",
    description="每日回填重磅警示後的 TAIEX 反應（日收盤粗估），供門檻檢驗。",
    schedule=CronTriggerTimetable("30 21 * * *", timezone="Asia/Taipei"),
    start_date=pendulum.datetime(2026, 11, 24, tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
        "execution_timeout": pendulum.duration(minutes=10),
        "on_failure_callback": _failure_notice,
    },
    tags=["breaking-news", "sentiment", "validation"],
) as settle_dag:
    PythonOperator(task_id="settle_breaking_news_reactions", python_callable=_settle)
