"""Read-only inventory of natural source runs, coverage, outcomes and receipts.

Run in the existing Airflow worker with PYTHONPATH=/opt/airflow/dags.
Use --output /tmp/stock_signal_inventory_snapshot.json, then docker cp the JSON.
Does not trigger DAGs, collect sources, create schema, or send notifications.
"""

import argparse
import datetime as dt
import json
import re
from pathlib import Path
from zoneinfo import ZoneInfo

from source_extension.audit_runs import audit_source_run
from source_extension.registry import _open_connection


SOURCE_DAG_IDS = (
    "source_extension_collect_process", "source_extension_collect_after_close",
    "source_extension_collect_intraday", "source_extension_collect_monthly",
)


def airflow_schedule_state():
    """Keep interval labels separate from actual execution and next due time.

    These metadata rows do not establish source-row DAG attribution or PIT.
    """
    try:
        from airflow.models import DagModel, DagRun
        from airflow.utils.session import create_session

        rows = []
        with create_session() as session:
            for dag_id in SOURCE_DAG_IDS:
                model = session.query(DagModel).filter(DagModel.dag_id == dag_id).one_or_none()
                run = session.query(DagRun).filter(
                    DagRun.dag_id == dag_id, DagRun.run_type == "scheduled",
                ).order_by(DagRun.execution_date.desc()).first()
                rows.append({
                    "dag_id": dag_id,
                    "is_paused": model.is_paused if model else None,
                    "is_active": model.is_active if model else None,
                    "next_logical_date": model.next_dagrun if model else None,
                    "next_run_create_after": model.next_dagrun_create_after if model else None,
                    "latest_scheduled_run": {
                        "run_id": run.run_id, "logical_date": run.execution_date,
                        "state": run.state, "started_at": run.start_date,
                        "finished_at": run.end_date,
                    } if run else None,
                })
        return {"status": "available", "dags": rows}
    except Exception as exc:
        return {"status": "unknown", "error_type": type(exc).__name__, "dags": []}


def summarize_daily_digest_payload(candidate_count, notification_rows):
    """Count candidate cards actually serialized in the latest daily digest.

    ``notification_rows`` are already selected for one run. Never return or
    persist the raw message payload, which can contain security names/details.
    """
    card_count = 0
    receipt_card_count = 0
    summary_count = 0
    omitted_counts = []
    parse_error_count = 0
    receipt_count = 0
    http_2xx_count = 0

    for status, http_status, receipt_id, message_text in notification_rows:
        has_receipt = bool(receipt_id)
        if has_receipt:
            receipt_count += 1
        if http_status is not None and 200 <= int(http_status) < 300:
            http_2xx_count += 1

        try:
            payload = json.loads(message_text or "")
        except (TypeError, ValueError):
            parse_error_count += 1
            continue
        embeds = payload.get("embeds") if isinstance(payload, dict) else None
        if not isinstance(embeds, list):
            parse_error_count += 1
            continue
        row_card_count = 0
        for embed in embeds:
            if not isinstance(embed, dict):
                continue
            title = str(embed.get("title") or "")
            if title.startswith("📡 台股收盤訊號"):
                summary_count += 1
                footer = embed.get("footer") or {}
                footer_text = str(footer.get("text") or "")
                match = re.search(r"另\s*([\d,]+)\s*檔未逐張列出", footer_text)
                if match:
                    omitted_counts.append(int(match.group(1).replace(",", "")))
            elif title.endswith(("偏多觀察", "偏空觀察", "訊號分歧觀察")):
                row_card_count += 1
        card_count += row_card_count
        if status == "sent" and has_receipt and http_status is not None and 200 <= int(http_status) < 300:
            receipt_card_count += row_card_count

    omitted_count = omitted_counts[0] if omitted_counts and len(set(omitted_counts)) == 1 else None
    parsed_candidate_count = int(candidate_count or 0)
    return {
        "candidate_count": parsed_candidate_count,
        "candidate_card_embeds": card_count,
        "candidate_card_embeds_with_http_2xx_receipt": receipt_card_count,
        "summary_embeds": summary_count,
        "summary_declared_omitted_count": omitted_count,
        "candidate_count_reconciles_to_cards_plus_declared_omissions": (
            parsed_candidate_count == card_count + omitted_count
            if omitted_count is not None else None
        ),
        "ledger_rows": len(notification_rows),
        "http_2xx_rows": http_2xx_count,
        "receipt_rows": receipt_count,
        "unparsed_payload_rows": parse_error_count,
    }


def latest_signal_analysis_diagnostics(signal_runs):
    """Read scan counters from the matching capture-task XCom, if present."""
    from airflow.models import DagRun, XCom
    from airflow.utils.session import create_session

    result = {}
    with create_session() as session:
        for signal in signal_runs:
            mode = signal["scan_mode"]
            airflow_run = session.query(DagRun).filter(
                DagRun.dag_id == "market_signal_notify",
                DagRun.run_id.like(f"market_signal__{mode}__%"),
            ).order_by(DagRun.execution_date.desc()).first()
            if airflow_run is None:
                result[mode] = {"status": "xcom_unavailable"}
                continue
            xcom = session.query(XCom).filter_by(
                dag_id=airflow_run.dag_id,
                run_id=airflow_run.run_id,
                task_id="capture_market_signal_snapshot",
                key="return_value",
            ).order_by(XCom.timestamp.desc()).first()
            payload = xcom.value if xcom is not None else None
            if not isinstance(payload, dict) or payload.get("run_key") != signal["run_key"]:
                result[mode] = {
                    "status": "run_key_mismatch_or_xcom_unavailable",
                    "airflow_run_id": airflow_run.run_id,
                    "airflow_run_state": airflow_run.state,
                    "airflow_run_type": str(airflow_run.run_type),
                }
                continue
            result[mode] = {
                "status": payload.get("status"),
                "airflow_run_id": airflow_run.run_id,
                "airflow_run_state": airflow_run.state,
                "airflow_run_type": str(airflow_run.run_type),
                "run_key": payload.get("run_key"),
                "source_dag_id": payload.get("source_dag_id"),
                "source_dag_run_id": payload.get("source_dag_run_id"),
                "as_of_date": payload.get("as_of_date"),
                "source_status": payload.get("source_status"),
                "source_collection_status": payload.get("source_collection_status"),
                "news_context_status": payload.get("news_context_status"),
                "universe_count": payload.get("universe_count"),
                "candidate_count": payload.get("candidate_count"),
                "family_trigger_counts": payload.get("family_trigger_counts"),
                "candidate_filter_counts": payload.get("candidate_filter_counts"),
                "intraday_candidate_diagnostics": payload.get(
                    "intraday_candidate_diagnostics"
                ),
            }
    return result


def latest_news_collection():
    """Summarize the latest scheduled news collection without article data."""
    from airflow.models import DagRun, XCom
    from airflow.utils.session import create_session

    totals_fields = (
        "status", "source_count", "url_count", "parsed_count", "valid_time_count",
        "accepted_count", "fetch_or_parse_rejected", "invalid_time_quarantined",
        "content_or_duplicate_rejected", "article_quality_rejected",
        "title_blank_or_whitespace_rejected", "content_blank_or_whitespace_rejected",
        "duplicate_source_article_id_rejected", "other_post_time_filter_rejected",
        "post_time_filter_rejection_count_delta",
        "post_time_filter_reconciliation_failed_source_count",
        "post_time_filter_rejections_reconciled", "freshness_observed_at_tpe",
        "freshness_cutoff_tpe", "timezone",
    )
    source_fields = (
        "source", "status", "url_count", "parsed_count", "valid_time_count",
        "accepted_count", "fetch_or_parse_rejected", "invalid_time_quarantined",
        "content_or_duplicate_rejected", "title_blank_or_whitespace_rejected",
        "content_blank_or_whitespace_rejected", "duplicate_source_article_id_rejected",
        "other_post_time_filter_rejected", "post_time_filter_rejection_count_delta",
        "post_time_filter_rejections_reconciled", "freshness_status",
        "recorded_datetime_within_48h", "date_only_after_cutoff_date",
        "date_only_cutoff_date_time_unverified", "older_by_recorded_precision",
        "future_recorded_time", "validation_rejections", "article_quality_rejected",
    )
    with create_session() as session:
        run = session.query(DagRun).filter(
            DagRun.dag_id == "news_ch_collect_process",
            DagRun.run_type == "scheduled",
        ).order_by(DagRun.execution_date.desc()).first()
        if run is None:
            return {"status": "scheduled_run_unavailable", "sources": []}
        xcom = session.query(XCom).filter_by(
            dag_id=run.dag_id,
            run_id=run.run_id,
            task_id="collect_news",
            key="return_value",
        ).order_by(XCom.timestamp.desc()).first()
        payload = xcom.value if xcom is not None else None
        if not isinstance(payload, dict):
            return {
                "status": "collection_xcom_unavailable",
                "run_id": run.run_id,
                "run_state": run.state,
                "logical_date": run.execution_date,
                "started_at": run.start_date,
                "finished_at": run.end_date,
                "sources": [],
            }
        return {
            "run_id": run.run_id,
            "run_state": run.state,
            "logical_date": run.execution_date,
            "started_at": run.start_date,
            "finished_at": run.end_date,
            "collection": {key: payload.get(key) for key in totals_fields},
            "sources": [
                {key: source.get(key) for key in source_fields}
                for source in payload.get("sources", [])
                if isinstance(source, dict)
            ],
        }


def snapshot():
    report = {
        "captured_at": dt.datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
        "mode": "read_only_natural_run_inventory",
        "source_audits": [],
    }
    connection, cursor, engine = _open_connection()
    queries = {
        "registry": """SELECT source_key,source_name,schedule_hint,enabled,
            approval_status,entity_type FROM source_registry ORDER BY source_key""",
        "latest_source_attempts": """SELECT sr.id,sr.source_key,sr.dag_id,
            sr.dag_run_id,sr.task_id,sr.map_index,sr.try_number,sr.status,
            sr.http_status,sr.row_count,sr.accepted_count,sr.rejected_count,
            sr.duplicate_count,sr.started_at,sr.finished_at
            FROM source_runs sr JOIN
              (SELECT source_key,MAX(id) id FROM source_runs GROUP BY source_key) x
              ON sr.id=x.id ORDER BY sr.source_key""",
        "latest_signal_runs": """SELECT r.run_key,r.scan_mode,r.as_of_date,
            r.status,r.source_status,r.universe_count,r.price_count,r.flow_count,
            r.revenue_count,r.financial_count,r.borrow_count,r.insider_count,
            r.dividend_count,r.candidate_count,r.notified_count,r.started_at
            FROM market_signal_runs r JOIN
              (SELECT scan_mode,MAX(id) id FROM market_signal_runs GROUP BY scan_mode) x
              ON r.id=x.id ORDER BY r.scan_mode""",
        "latest_signal_delivery_groups": """SELECT r.run_key,r.scan_mode,
            r.as_of_date,r.candidate_count,r.notified_count,
            n.notification_channel,n.status ledger_status,COUNT(n.id) ledger_rows,
            SUM(CASE WHEN n.platform_http_status BETWEEN 200 AND 299
                THEN 1 ELSE 0 END) http_2xx_rows,
            SUM(CASE WHEN n.platform_receipt_id IS NOT NULL
                AND n.platform_receipt_id<>'' THEN 1 ELSE 0 END) receipt_rows,
            SUM(CASE WHEN n.notified_at IS NOT NULL THEN 1 ELSE 0 END)
                notified_at_rows,
            SUM(CASE WHEN n.notification_error_type IS NOT NULL
                THEN 1 ELSE 0 END) error_rows
            FROM market_signal_runs r JOIN
              (SELECT scan_mode,MAX(id) id FROM market_signal_runs GROUP BY scan_mode) x
              ON r.id=x.id LEFT JOIN market_signal_notifications n
              ON n.run_key=r.run_key
            GROUP BY r.run_key,r.scan_mode,r.as_of_date,r.candidate_count,
              r.notified_count,n.notification_channel,n.status
            ORDER BY r.scan_mode""",
        "latest_daily_market_coverage": """SELECT *
            FROM market_signal_run_market_coverage WHERE run_key=
              (SELECT run_key FROM market_signal_runs WHERE scan_mode='daily'
               ORDER BY id DESC LIMIT 1) ORDER BY market""",
        "outcome_groups": """SELECT signal_date,outcome_basis,horizon_sessions,
            outcome_price_source_key,entity_type,evaluation_cohort,status,
            COUNT(*) rows_count,SUM(directional_hit) directional_hits,
            AVG(excess_return_pp) mean_excess_pp
            FROM (
                SELECT o.signal_date,o.outcome_basis,o.horizon_sessions,
                    o.outcome_price_source_key,o.status,o.directional_hit,
                    o.excess_return_pp,c.entity_type,
                    CASE
                        WHEN c.entity_type='stock' THEN 'selected_candidate'
                        WHEN c.entity_type='stock_evidence' AND JSON_VALID(c.details_text)
                            THEN CASE JSON_UNQUOTE(JSON_EXTRACT(
                                c.details_text,'$.candidate_selected'))
                                WHEN 'true' THEN 'selected_evidence'
                                WHEN 'false' THEN 'subthreshold_evidence'
                                ELSE 'unknown_evidence'
                            END
                        ELSE 'other'
                    END evaluation_cohort
                FROM market_signal_outcomes o
                JOIN market_signal_candidates c ON c.id=o.candidate_id
            ) outcome_cohort
            GROUP BY signal_date,outcome_basis,horizon_sessions,
                outcome_price_source_key,entity_type,evaluation_cohort,status
            ORDER BY signal_date DESC,outcome_basis,horizon_sessions,
                entity_type,evaluation_cohort,status""",
        "latest_signal_horizon_reviews": """SELECT as_of_date,horizon_key,
            review_status,reviewed_at,report_json
            FROM market_signal_horizon_reviews ORDER BY reviewed_at DESC,
            horizon_key LIMIT 5""",
        "latest_signal_followup_coverage": """SELECT horizon_key,
            COUNT(*) followup_rows,COUNT(DISTINCT signal_date) signal_dates,
            MIN(signal_date) first_signal_date,MAX(signal_date) last_signal_date,
            SUM(status='settled') settled_rows,
            SUM(status='pending_data') pending_data_rows,
            SUM(execution_status='settled') execution_settled_rows,
            SUM(benchmark_status='settled') benchmark_settled_rows,
            SUM(candidate_selected=1) selected_candidate_rows,
            SUM(candidate_selected=1 AND delivery_status='sent') selected_delivery_sent_rows,
            MAX(updated_at) last_updated_at
            FROM market_signal_followups GROUP BY horizon_key ORDER BY horizon_key""",
        "latest_outcome_notification_receipts": """SELECT n.run_key,
            n.notification_channel,n.status,n.platform_http_status,
            (n.platform_receipt_id IS NOT NULL AND n.platform_receipt_id<>'') has_receipt,
            n.notification_error_type,n.notified_at,n.created_at
            FROM market_signal_notifications n
            WHERE n.notification_channel IN ('market_signal_outcome_review',
                'market_signal_annual_review')
            ORDER BY n.id DESC LIMIT 20""",
        "latest_notification_receipts": """SELECT run_key,status,
            platform_http_status,(platform_receipt_id IS NOT NULL AND
            platform_receipt_id<>'') has_receipt,notification_error_type,notified_at
            FROM market_signal_notifications ORDER BY id DESC LIMIT 10""",
        "tpex_official_flow_dates": """SELECT effective_date,
            COUNT(DISTINCT entity_id) entity_codes,COUNT(*) metric_rows
            FROM source_records WHERE source_key='tpex_institutional_flow_daily'
            AND quality_status='ok' GROUP BY effective_date
            ORDER BY effective_date DESC LIMIT 10""",
    }
    try:
        for key, sql in queries.items():
            cursor.execute(sql)
            columns = [item[0] for item in cursor.description]
            report[key] = [dict(zip(columns, row)) for row in cursor.fetchall()]
        horizon_summary_fields = (
            "expected_due_candidate_count", "tracked_due_candidate_count",
            "untracked_due_candidate_count", "tracking_coverage_pct",
            "due_outcome_row_count", "settled_rows", "pending_rows",
            "directional_group_count", "source_complete_due_rows",
            "source_status_counts", "delivery_status_counts",
            "outcome_status_counts", "execution_status_counts",
            "benchmark_status_counts", "confirmed_delivery_receipt_rows",
            "selected_due_candidate_count", "selected_confirmed_receipt_count",
            "review_status", "policy_decision",
        )
        for row in report["latest_signal_horizon_reviews"]:
            try:
                review = json.loads(row.pop("report_json") or "{}")
            except (TypeError, ValueError):
                review = None
            row["review_summary"] = (
                {field: review.get(field) for field in horizon_summary_fields}
                if isinstance(review, dict) else None
            )
        daily_run = next(
            (row for row in report["latest_signal_runs"] if row["scan_mode"] == "daily"),
            None,
        )
        if daily_run:
            cursor.execute(
                """
                SELECT status,platform_http_status,platform_receipt_id,message_text
                FROM market_signal_notifications WHERE run_key=%s ORDER BY id
                """,
                (daily_run["run_key"],),
            )
            report["latest_daily_digest_content"] = summarize_daily_digest_payload(
                daily_run["candidate_count"], cursor.fetchall(),
            )
        else:
            report["latest_daily_digest_content"] = None
    finally:
        connection.close()
        engine.dispose()
    report["airflow_schedule_state"] = airflow_schedule_state()
    report["latest_signal_analysis_diagnostics"] = latest_signal_analysis_diagnostics(
        report["latest_signal_runs"],
    )
    report["latest_news_collection"] = latest_news_collection()
    for dag_id in SOURCE_DAG_IDS:
        report["source_audits"].append(audit_source_run(dag_id=dag_id))
    report["limits"] = [
        "Connections are read independently; a concurrent natural run can advance between queries.",
        "Enabled/approved is not evidence of completeness or predictive power.",
        "Sparse event lists and unmatched codes are not whole-market coverage or zero activity.",
        "Source-feed version capture does not recreate older PIT vintages.",
        "Receipt metadata is distinct from candidate-card delivery and human reading.",
        "Delivery groups count notification-ledger rows, HTTP 2xx responses, receipt IDs, and timestamps separately; none proves human reading. `notified_count` is a daily-digest flag for daily scans and a notified-candidate count for intraday scans, not a ledger-row count.",
        "The daily digest content summary counts serialized candidate-card embeds and compares them with the summary footer's omitted count; delivery rows are messages/batches, not candidate cards.",
        "Signal analysis diagnostics are read from the matching capture-task XCom by run_key; a missing or mismatched XCom remains unknown rather than inferred from a later run.",
        "A child signal DagRun can be labeled manual by Airflow even when its source DAG parent was scheduled; check source_dag_id/source_dag_run_id against the natural source audit before classifying execution origin.",
        "Same candidate in multiple outcome bases/horizons is not an independent sample.",
        "Horizon review rows summarize their report_json instead of retaining candidate-level detail; review status is descriptive and never authorizes automatic policy changes. Follow-up coverage and notification-channel receipts are reported separately.",
        "Logical dates label data intervals; use actual run timestamps for execution and next_run_create_after only as the scheduler's earliest creation time.",
        "Airflow schedule metadata does not repair missing source_runs DAG attribution; latest_source_attempts may include manual attempts and is separate from natural-run audits.",
        "News freshness summarizes publisher-recorded timestamps from the scheduled collector; it does not verify original-page timestamps or historical revision vintages.",
    ]
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output already exists; choose a new snapshot path.")
    result = snapshot()
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2,
                                      default=str) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "registry_count": len(result["registry"]),
                      "audits": [{"dag_id": a["dag_id"], "status": a["status"]}
                                 for a in result["source_audits"]]}, ensure_ascii=False))
