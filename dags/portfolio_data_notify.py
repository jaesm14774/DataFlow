"""Airflow DAG for data-level notifications on enabled holdings."""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from portfolio_alerts import ensure_alert_rules, send_portfolio_notifications


@dag(
    dag_id="portfolio_data_notify",
    description="檢查已啟用持股的重大事件、月營收與股利資料，依規則投遞通知。",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 6, tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=10),
        "execution_timeout": pendulum.duration(minutes=20),
    },
    tags=["portfolio", "data-notification", "discord"],
)
def portfolio_data_notify():
    @task(task_id="ensure_portfolio_notification_schema")
    def ensure_schema_task():
        ensure_alert_rules()
        return True

    @task(task_id="notify_enabled_holdings")
    def notify_task():
        return send_portfolio_notifications()

    ensure_schema_task() >> notify_task()


dag = portfolio_data_notify()
