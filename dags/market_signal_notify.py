"""Market-wide source scan, analysis, and notification DAG."""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from source_extension.market_signals import (
    capture_market_signal_snapshot,
    notify_market_signal_snapshot,
)


with DAG(
    dag_id="market_signal_notify",
    description="由來源收集流程觸發市場訊號掃描，整理候選資料並依規則投遞通知。",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 6, tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={
        "owner": "jaesm14774",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
        "execution_timeout": pendulum.duration(minutes=20),
    },
    tags=["market-signals", "collection-analysis-notification"],
) as dag:
    capture = PythonOperator(
        task_id="capture_market_signal_snapshot",
        python_callable=capture_market_signal_snapshot,
    )

    notify = PythonOperator(
        task_id="notify_market_signal_snapshot",
        python_callable=notify_market_signal_snapshot,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    capture >> notify
