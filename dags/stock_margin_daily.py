"""Trading-day evening collection of margin (融資融券) data.

The legacy Saturday run is the only other place margin data is loaded, so the
signal digest could not see it on Tuesday-Friday and reported the margin rules as
"data not ready". The rules lag margin data by one session, so a load finished in
the evening of session T-1 is what the digest of session T needs.

Flow per trading day (Asia/Taipei):
1. skip market holidays;
2. wait (rescheduling, no worker slot held) until the source lists the target session,
   by probing one liquid stock instead of running a whole scrape against a half-updated site;
3. scrape the margin-eligible stocks and verify the target session is present for at least
   90% of a normal session; partial data is saved first and the task then fails so the retry
   fills the rest.
"""

import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sensors.python import PythonSensor
from airflow.timetables.trigger import CronTriggerTimetable

from stock.collect_process import (
    margin_daily_is_trading_day,
    margin_daily_source_ready,
    margin_trade_daily_collect,
)


with DAG(
    dag_id="stock_margin_trade_daily_process",
    description=(
        "交易日晚間收集融資融券，讓隔天的訊號摘要能即時使用融資規則；"
        "來源尚未公布時只探測、不空跑整輪爬蟲。"
    ),
    start_date=pendulum.datetime(2026, 11, 14, 21, 30, tz="Asia/Taipei"),
    schedule=CronTriggerTimetable("30 21 * * 1-5", timezone="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "jaesm14774",
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["stock", "margin", "signal-source"],
) as dag:
    is_trading_day = ShortCircuitOperator(
        task_id="is_trading_day",
        python_callable=margin_daily_is_trading_day,
    )

    wait_margin_published = PythonSensor(
        task_id="wait_margin_published",
        python_callable=margin_daily_source_ready,
        mode="reschedule",
        poke_interval=datetime.timedelta(minutes=20),
        timeout=datetime.timedelta(hours=8),
        retries=0,
    )

    collect_margin_daily = PythonOperator(
        task_id="collect_margin_daily",
        python_callable=margin_trade_daily_collect,
        retries=2,
        retry_delay=datetime.timedelta(minutes=45),
        execution_timeout=datetime.timedelta(hours=3),
    )

    is_trading_day >> wait_margin_published >> collect_margin_daily
