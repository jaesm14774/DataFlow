"""Weekly long-horizon revalidation of every market-signal rule.

Appends the dates whose forward returns matured since the last run to the date-level
evidence table, re-measures all rules on the full out-of-sample window, and moves a rule
between candidate / active / suspended only under the hysteresis policy in
``source_extension.rule_lifecycle`` (short windows never decide anything).
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from source_extension.rule_revalidation import run_weekly_revalidation

with DAG(
    dag_id="market_signal_revalidate_weekly",
    description="每週以全部歷史重新驗證所有市場訊號規則（長期證據，短期僅供參考）。",
    schedule=CronTriggerTimetable("0 9 * * 6", timezone="Asia/Taipei"),
    start_date=pendulum.datetime(2026, 11, 14, tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=10),
        "execution_timeout": pendulum.duration(hours=2),
    },
    tags=["market-signals", "revalidation"],
) as dag:
    PythonOperator(task_id="revalidate_signal_rules", python_callable=run_weekly_revalidation)
