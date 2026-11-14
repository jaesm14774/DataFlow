"""Daily poll for the official full-market TDCC shareholding snapshot.

The snapshot itself is weekly (data date = Friday). Polling a few times a day costs one
request per run: a week that is already in the database is skipped (so ``create_time``
keeps recording when it first landed), and a new week is loaded as soon as it appears
instead of waiting for Monday.
"""

import datetime

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from stock.collect_process import failure_callback, major_holder_collect


with DAG(
    dag_id="stock_major_holder_monday_refresh_process",
    description=(
        "每天三次輪詢官方 TDCC 全市場持股分布（來源每週一版）；已入庫的週別直接略過，"
        "新的一週一出現就入庫，讓點時訊號背景不必等到週一。"
    ),
    start_date=pendulum.datetime(2026, 10, 5, 9, 0, tz="Asia/Taipei"),
    schedule=CronTriggerTimetable("0 9,15,21 * * *", timezone="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=20),
        "execution_timeout": datetime.timedelta(hours=1),
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": failure_callback(),
    },
    tags=["stock", "TDCC", "signal-source"],
) as dag:
    refresh_tdcc_shareholding = PythonOperator(
        task_id="refresh_tdcc_shareholding",
        python_callable=major_holder_collect,
    )
