"""Monday refresh for the official full-market TDCC shareholding snapshot."""

import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from stock.collect_process import major_holder_collect


with DAG(
    dag_id="stock_major_holder_monday_refresh_process",
    description=(
        "每週一獨立更新官方 TDCC 全市場持股分布，讓周末新發布的資料"
        "能及時進入點時訊號背景。"
    ),
    start_date=pendulum.datetime(2026, 10, 5, 9, 0, tz="Asia/Taipei"),
    schedule=CronTriggerTimetable("0 9 * * 1", timezone="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=40),
        "execution_timeout": datetime.timedelta(hours=2),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["stock", "TDCC", "signal-source"],
) as dag:
    refresh_tdcc_shareholding = PythonOperator(
        task_id="refresh_tdcc_shareholding",
        python_callable=major_holder_collect,
    )
