"""Airflow entrypoints for approved, purpose-driven source extensions."""

from __future__ import annotations

import json

import pendulum
from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.trigger_rule import TriggerRule

from source_extension.audit_runs import audit_source_run
from source_extension.equity_order_flow import equity_order_flow_collection_finished
from source_extension.registry import (
    discover_approved_source_keys,
    ensure_schema_and_catalog,
    ingest_source,
)


def _build_collection_dag(
    *,
    dag_id: str,
    description: str,
    schedule: str | CronTriggerTimetable,
    schedule_hint: str,
    cadence_tag: str,
    start_date: pendulum.DateTime | None = None,
    trigger_portfolio_notifications: bool = False,
    trigger_market_scan_mode: str | None = None,
):
    @dag(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=start_date or pendulum.datetime(2026, 8, 6, tz="Asia/Taipei"),
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=1,
        max_active_tasks=4,
        default_args={
            "owner": "jaesm14774",
            "retries": 1,
            # Keep transient DB deadlocks and upstream stream resets from
            # stalling a 15-minute collection cadence for most of an interval.
            "retry_delay": pendulum.duration(minutes=2),
            "execution_timeout": pendulum.duration(minutes=30),
        },
        tags=["source-extension", "approved-sources", cadence_tag],
    )
    def collection_dag():
        @task(task_id="ensure_source_extension_schema")
        def ensure_schema_task():
            ensure_schema_and_catalog()
            return True

        @task(task_id="discover_approved_sources")
        def discover_task():
            return discover_approved_source_keys(schedule_hint=schedule_hint)

        @task(task_id="ingest_approved_source")
        def ingest_task(source_key):
            return ingest_source(source_key)

        @task(task_id="verify_collection_and_triggers", trigger_rule=TriggerRule.ALL_DONE)
        def verify_collection_task(expected_source_keys, trigger_task_ids):
            context = get_current_context()
            dag_run = context["dag_run"]
            try:
                source_audit = audit_source_run(
                    dag_id=dag_id,
                    run_id=dag_run.run_id,
                    schedule_hint=schedule_hint,
                )
                print(
                    "[source-run audit] "
                    + json.dumps(source_audit, ensure_ascii=False, default=str)
                )
                audit_issues = [
                    {
                        "source_key": source["source_key"],
                        "status": source["status"],
                        "http_status": source["http_status"],
                        "row_count": source["row_count"],
                        "accepted_count": source["accepted_count"],
                        "pit_observed_accepted_record_count": source[
                            "pit_observed_accepted_record_count"
                        ],
                        "pit_versioned_accepted_record_count": source[
                            "pit_versioned_accepted_record_count"
                        ],
                        "pit_unversioned_accepted_record_count": source[
                            "pit_unversioned_accepted_record_count"
                        ],
                        "pit_version_coverage_pct": source[
                            "pit_version_coverage_pct"
                        ],
                        "quarantined_count": source["quarantined_count"],
                        "duplicate_count": source["duplicate_count"],
                        "effective_date_min": (
                            source["effective_date_min"].isoformat()
                            if source["effective_date_min"] else None
                        ),
                        "effective_date_max": (
                            source["effective_date_max"].isoformat()
                            if source["effective_date_max"] else None
                        ),
                        "latest_event_time": (
                            source["latest_event_time"].isoformat()
                            if source["latest_event_time"] else None
                        ),
                        "latest_event_age_days": source["latest_event_age_days"],
                        "event_time_max_age_days": source["event_time_max_age_days"],
                        "matched_codes_by_market": source["matched_codes_by_market"],
                        "unmatched_code_count": source["unmatched_code_count"],
                        "market_coverage": source["market_coverage"],
                        "effective_date_alignment": source.get(
                            "effective_date_alignment"
                        ),
                        "contract_drift": source["contract_drift"],
                        "quality_flags": source["quality_flags"],
                    }
                    for source in source_audit.get("sources", [])
                    if (
                        source["status"] != "success"
                        or source["quality_flags"]
                        or not source["runtime_contract_match"]
                    )
                ]
                source_audit_summary = {
                    "status": source_audit.get("status"),
                    "reason": source_audit.get("reason"),
                    "expected_source_count": source_audit.get("expected_source_count"),
                    "reported_source_count": source_audit.get("reported_source_count"),
                    "registered_schedule_source_count": source_audit.get(
                        "registered_schedule_source_count"
                    ),
                    "runtime_expected_source_count": source_audit.get(
                        "runtime_expected_source_count"
                    ),
                    "unattributed_legacy_row_count": source_audit.get(
                        "unattributed_legacy_row_count", 0
                    ),
                    "unattributed_legacy_source_count": source_audit.get(
                        "unattributed_legacy_source_count", 0
                    ),
                    "unattributed_legacy_source_keys": source_audit.get(
                        "unattributed_legacy_source_keys", []
                    ),
                    "expected_source_keys": source_audit.get(
                        "expected_source_keys", []
                    ),
                    "missing_source_keys": source_audit.get("missing_source_keys", []),
                    "missing_runtime_source_keys": source_audit.get(
                        "missing_runtime_source_keys", []
                    ),
                    "contract_drift_source_keys": source_audit.get(
                        "contract_drift_source_keys", []
                    ),
                    "market_wide_coverage": [
                        {
                            "source_key": source["source_key"],
                            "status": source["status"],
                            "effective_date_min": (
                                source["effective_date_min"].isoformat()
                                if source["effective_date_min"] else None
                            ),
                            "effective_date_max": (
                                source["effective_date_max"].isoformat()
                                if source["effective_date_max"] else None
                            ),
                            **source["market_coverage"],
                        }
                        for source in source_audit.get("sources", [])
                        if source.get("market_coverage")
                    ],
                    "source_summaries": [
                        {
                            "source_key": source["source_key"],
                            "status": source["status"],
                            "http_status": source["http_status"],
                            "row_count": int(source["row_count"] or 0),
                            "accepted_count": int(source["accepted_count"] or 0),
                            "pit_observed_accepted_record_count": source[
                                "pit_observed_accepted_record_count"
                            ],
                            "pit_versioned_accepted_record_count": source[
                                "pit_versioned_accepted_record_count"
                            ],
                            "pit_unversioned_accepted_record_count": source[
                                "pit_unversioned_accepted_record_count"
                            ],
                            "pit_version_coverage_pct": source[
                                "pit_version_coverage_pct"
                            ],
                            "quarantined_count": int(
                                source["quarantined_count"] or 0
                            ),
                            "duplicate_count": int(source["duplicate_count"] or 0),
                            "effective_date_min": (
                                source["effective_date_min"].isoformat()
                                if source["effective_date_min"] else None
                            ),
                            "effective_date_max": (
                                source["effective_date_max"].isoformat()
                                if source["effective_date_max"] else None
                            ),
                            "latest_event_time": (
                                source["latest_event_time"].isoformat()
                                if source["latest_event_time"] else None
                            ),
                            "matched_codes_by_market": source[
                                "matched_codes_by_market"
                            ],
                            "unmatched_code_count": int(
                                source["unmatched_code_count"] or 0
                            ),
                            "market_coverage": source.get("market_coverage"),
                            "contract_drift": list(source["contract_drift"] or []),
                            "runtime_contract_match": bool(
                                source["runtime_contract_match"]
                            ),
                            "quality_flags": list(source["quality_flags"] or []),
                        }
                        for source in source_audit.get("sources", [])
                    ],
                    "issues": audit_issues,
                    "data_completeness": "unproven",
                }
            except Exception as exc:
                source_audit_summary = {
                    "status": "unavailable",
                    "error_type": type(exc).__name__,
                    "data_completeness": "unproven",
                }
                print(
                    f"[source-run audit] unavailable; collection verification continues "
                    f"| {type(exc).__name__}"
                )
            task_instances = dag_run.get_task_instances()
            ingestion_instances = [
                task_instance for task_instance in task_instances
                if task_instance.task_id == "ingest_approved_source"
                and task_instance.map_index >= 0
            ]
            expected = [str(source_key) for source_key in (expected_source_keys or [])]
            if not expected or len(ingestion_instances) != len(expected):
                raise RuntimeError(
                    "source ingestion mapping is incomplete: "
                    f"expected {len(expected)} mapped tasks, found {len(ingestion_instances)}"
                )
            failed_ingestions = [
                f"{task_instance.map_index}:{task_instance.state}"
                for task_instance in ingestion_instances
                if task_instance.state != "success"
            ]
            trigger_states = {
                task_instance.task_id: task_instance.state
                for task_instance in task_instances
                if task_instance.task_id in set(trigger_task_ids or [])
            }
            failed_triggers = {
                task_id: state for task_id, state in trigger_states.items()
                # TriggerDagRunOperator skips when its idempotent target already exists.
                if state not in {"success", "skipped"}
            }
            if (
                trigger_market_scan_mode
                and trigger_states.get("trigger_market_signal_notify")
                in {"success", "skipped"}
            ):
                expected_run_id = (
                    f"market_signal__{trigger_market_scan_mode}__{dag_id}__"
                    f"{dag_run.run_id}"
                )
                target_runs = DagRun.find(
                    dag_id="market_signal_notify",
                    run_id=expected_run_id,
                )
                if len(target_runs) != 1:
                    failed_triggers["trigger_market_signal_notify"] = (
                        "expected_target_missing_or_ambiguous"
                    )
                else:
                    target_conf = target_runs[0].conf or {}
                    if (
                        str(target_conf.get("scan_mode") or "").strip().lower()
                        != trigger_market_scan_mode
                        or target_conf.get("source_dag_id") != dag_id
                        or target_conf.get("source_dag_run_id") != dag_run.run_id
                    ):
                        failed_triggers["trigger_market_signal_notify"] = (
                            "expected_target_conf_mismatch"
                        )
            if failed_ingestions or len(trigger_states) != len(trigger_task_ids or []):
                raise RuntimeError(
                    "source collection did not complete: "
                    f"ingestion failures={failed_ingestions}; "
                    f"trigger states={trigger_states}; "
                    f"trigger validation errors={failed_triggers}"
                )
            if failed_triggers:
                raise RuntimeError(f"downstream trigger did not complete: {failed_triggers}")
            source_audit_status = source_audit_summary.get("status")
            successful_audit_statuses = {
                "all_expected_tasks_succeeded_data_review_required",
                "all_expected_tasks_succeeded_data_completeness_unproven",
            }
            if source_audit_status not in successful_audit_statuses:
                raise RuntimeError(
                    "source completeness audit did not pass: "
                    + json.dumps(
                        {
                            key: source_audit_summary.get(key)
                            for key in (
                                "status",
                                "reason",
                                "missing_source_keys",
                                "missing_runtime_source_keys",
                                "contract_drift_source_keys",
                                "unattributed_legacy_source_keys",
                            )
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
            return {
                "sources": len(expected),
                "status": (
                    "data_review_required"
                    if source_audit_status
                    == "all_expected_tasks_succeeded_data_review_required"
                    else "complete_completeness_unproven"
                ),
                "source_audit": source_audit_summary,
            }

        ready = ensure_schema_task()
        sources = discover_task()
        ready >> sources
        ingested = ingest_task.expand(source_key=sources)
        trigger_task_ids = []
        terminal_upstreams = [ingested]
        if trigger_portfolio_notifications:
            notify = TriggerDagRunOperator(
                task_id="trigger_portfolio_data_notify",
                trigger_dag_id="portfolio_data_notify",
                trigger_run_id="portfolio_notify__{{ dag_run.run_id }}",
                logical_date="{{ data_interval_end }}",
                skip_when_already_exists=True,
                wait_for_completion=False,
            )
            ingested >> notify
            terminal_upstreams.append(notify)
        if trigger_market_scan_mode:
            notify_market_scan = TriggerDagRunOperator(
                task_id="trigger_market_signal_notify",
                trigger_dag_id="market_signal_notify",
                trigger_run_id=(
                    f"market_signal__{trigger_market_scan_mode}__{dag_id}__{{{{ dag_run.run_id }}}}"
                ),
                logical_date=(
                    "{{ dag_run.logical_date + macros.timedelta(seconds=1) }}"
                    if trigger_market_scan_mode == "daily"
                    else "{{ ts }}"
                ),
                conf={
                    "scan_mode": trigger_market_scan_mode,
                    "source_dag_id": dag_id,
                    "source_dag_run_id": "{{ dag_run.run_id }}",
                    "source_schedule_hint": schedule_hint,
                },
                trigger_rule=TriggerRule.ALL_DONE,
                skip_when_already_exists=True,
                wait_for_completion=False,
            )
            if trigger_market_scan_mode == "daily":
                # Keep the bounded wait upstream of the notification DAG so
                # its single active-run slot remains available to intraday scans.
                wait_order_flow = PythonSensor(
                    task_id="wait_existing_equity_order_flow_collection",
                    python_callable=equity_order_flow_collection_finished,
                    mode="reschedule", poke_interval=120, timeout=2 * 60 * 60,
                    retries=0, trigger_rule=TriggerRule.ALL_DONE,
                )
                ingested >> wait_order_flow >> notify_market_scan
            else:
                ingested >> notify_market_scan
            terminal_upstreams.append(notify_market_scan)
            trigger_task_ids.append("trigger_market_signal_notify")
        if trigger_portfolio_notifications:
            trigger_task_ids.append("trigger_portfolio_data_notify")
        verified = verify_collection_task(sources, trigger_task_ids)
        for upstream in terminal_upstreams:
            upstream >> verified

    return collection_dag()


source_extension_collect_process = _build_collection_dag(
    dag_id="source_extension_collect_process",
    description="每日收集通過核准的官方來源資料，並觸發持股資料通知。",
    schedule="0 4 * * *",
    schedule_hint="daily",
    cadence_tag="daily",
    trigger_portfolio_notifications=True,
)

source_extension_collect_after_close = _build_collection_dag(
    dag_id="source_extension_collect_after_close",
    description="收盤後收集核准來源資料，完成後觸發每日市場訊號掃描。",
    schedule="0 20 * * 1-5",
    schedule_hint="daily_after_close",
    cadence_tag="daily-after-close",
    trigger_market_scan_mode="daily",
)

source_extension_collect_intraday = _build_collection_dag(
    dag_id="source_extension_collect_intraday",
    description="盤中定時更新核准來源資料，並觸發持股通知與盤中訊號掃描。",
    schedule="*/15 7-22 * * 1-5",
    schedule_hint="intraday",
    cadence_tag="intraday",
    trigger_portfolio_notifications=True,
    trigger_market_scan_mode="intraday",
)

source_extension_collect_weekend = _build_collection_dag(
    dag_id="source_extension_collect_weekend",
    description="週末依核准的盤中來源設定收集資料，完成後觸發訊號掃描。",
    schedule=CronTriggerTimetable("0 8 * * 6,0", timezone="Asia/Taipei"),
    schedule_hint="intraday",
    cadence_tag="weekend",
    start_date=pendulum.datetime(2026, 10, 4, 8, tz="Asia/Taipei"),
    trigger_market_scan_mode="intraday",
)

source_extension_collect_monthly = _build_collection_dag(
    dag_id="source_extension_collect_monthly",
    description="每月收集核准的月頻官方資料並記錄來源執行狀態。",
    schedule="0 4 25 * *",
    schedule_hint="monthly",
    cadence_tag="monthly",
)
