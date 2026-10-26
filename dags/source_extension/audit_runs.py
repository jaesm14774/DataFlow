"""Read-only completeness audit for one scheduled source-extension DAG run."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from typing import Any

from source_extension.registry import (
    AUTO_ENABLE_THRESHOLD,
    SOURCE_DEFINITIONS,
    SOURCE_DEFINITIONS_BY_KEY,
    SUPPORTED_SCHEDULE_HINTS,
    _load_stock_market_codes,
    _open_connection,
    priority_score,
)


DAG_SCHEDULE_HINTS = {
    "source_extension_collect_process": "daily",
    "source_extension_collect_intraday": "intraday",
    "source_extension_collect_weekend": "intraday",
    "source_extension_collect_after_close": "daily_after_close",
    "source_extension_collect_monthly": "monthly",
}

# Some source files remain downloadable after their underlying event feed stops
# advancing. Keep checks source-semantic and report-only; historical rows remain
# available but are never mistaken for fresh observations.
SOURCE_EVENT_TIME_MAX_AGE_DAYS = {
    "tpex_dividends": 365,
}
SOURCE_EFFECTIVE_DATE_MAX_AGE_DAYS = {
    "twse_trading_status_daily": 7,
    "tpex_trading_status_daily": 7,
    # TDCC's official distribution is weekly; allow holidays and publication lag.
    "tdcc_shareholder_distribution_weekly": 10,
    # These are daily sparse disclosures; allow weekends and short publication lag.
    "twse_major_shareholders_daily": 5,
    "tpex_major_shareholders_daily": 5,
    # Statutory shortfall lists update irregularly; this is a review flag, not a
    # claim that an omitted issuer meets the requirement.
    "twse_director_shareholding_shortfall_daily": 60,
    "tpex_director_shareholding_shortfall_daily": 60,
}
AFTER_CLOSE_MARKET_INDEX_SOURCE_KEYS = (
    "twse_market_index_daily",
    "tpex_market_index_daily",
    "twse_market_total_return_index_daily",
    "tpex_market_total_return_index_daily",
)
AFTER_CLOSE_STOCK_FLOW_SOURCE_KEYS = (
    "tpex_institutional_flow_daily",
)
AFTER_CLOSE_MARKET_QUOTE_SOURCE_KEYS = (
    "twse_equity_quotes_daily",
    "tpex_equity_quotes_daily",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _date_text(value: Any) -> str | None:
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    text = str(value or "").strip()
    return text[:10] or None


def _latest_official_quote_date(on_or_before: str) -> str | None:
    """Read the latest approved market-wide quote date available to the scanner."""

    conn = cursor = engine = None
    try:
        conn, cursor, engine = _open_connection()
        cursor.execute(
            """
            SELECT MAX(effective_date)
            FROM source_records
            WHERE source_key IN (
                'twse_equity_quotes_daily', 'tpex_equity_quotes_daily'
            )
              AND event_type='market_quote_daily'
              AND entity_type='stock'
              AND quality_status='ok'
              AND effective_date<=%s
            """,
            (on_or_before,),
        )
        row = cursor.fetchone()
        return _date_text(row[0] if row else None)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if engine:
            engine.dispose()


def _latest_scheduled_run(cursor, dag_id: str) -> str | None:
    cursor.execute(
        """
        SELECT dag_run_id
        FROM source_runs
        WHERE dag_id=%s AND dag_run_id LIKE 'scheduled__%%'
        GROUP BY dag_run_id
        ORDER BY MAX(started_at) DESC
        LIMIT 1
        """,
        (dag_id,),
    )
    row = cursor.fetchone()
    return str(row[0]) if row and row[0] else None


def _latest_scheduled_airflow_run(
    dag_id: str,
) -> tuple[str | None, str | None, str | None]:
    """Return the latest scheduled Airflow run for default run selection."""

    try:
        from airflow.models import DagRun
        from airflow.utils.session import create_session

        with create_session() as session:
            row = (
                session.query(DagRun.run_id, DagRun.state)
                .filter(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id.like("scheduled__%"),
                )
                .order_by(DagRun.execution_date.desc())
                .first()
            )
        if not row or not row[0]:
            return None, None, None
        return str(row[0]), str(row[1]) if row[1] else None, None
    except Exception as exc:
        # Keep the audit report-only even if Airflow metadata is unavailable.
        return None, None, type(exc).__name__


def _source_key_sets(cursor, schedule_hint: str | None) -> tuple[set[str], set[str], set[str]]:
    """Compare registered and runtime source keys even when a run has no rows."""

    cursor.execute("SELECT source_key FROM source_registry")
    registered_keys = {str(row[0]) for row in cursor.fetchall()}
    expected_keys: set[str] = set()
    runtime_expected_keys: set[str] = set()
    if schedule_hint:
        cursor.execute(
            """
            SELECT source_key FROM source_registry
            WHERE enabled=1 AND approval_status='approved'
              AND priority_score >= 75 AND schedule_hint=%s
            ORDER BY source_key
            """,
            (schedule_hint,),
        )
        expected_keys = {str(row[0]) for row in cursor.fetchall()}
        if schedule_hint in SUPPORTED_SCHEDULE_HINTS:
            runtime_expected_keys = {
                str(source["source_key"])
                for source in SOURCE_DEFINITIONS
                if source.get("enabled")
                and source.get("approval_status") == "approved"
                and source.get("schedule_hint") == schedule_hint
                and priority_score(
                    source["purpose_score"],
                    source["joinability_score"],
                    source["authority_score"],
                    source["freshness_score"],
                    source["cost_score"],
                ) >= AUTO_ENABLE_THRESHOLD
            }
    return registered_keys, expected_keys, runtime_expected_keys


def audit_source_run(
    *, dag_id: str, run_id: str | None = None,
    schedule_hint: str | None = None,
) -> dict[str, Any]:
    """Report attempt counts, accepted-record dates, freshness, and code joins.

    All database operations are SELECTs. A missing record is left missing and
    is never converted into a zero-valued observation.
    """

    inferred_hint = schedule_hint or DAG_SCHEDULE_HINTS.get(dag_id)
    connection, cursor, engine = _open_connection()
    try:
        registered_keys, expected_keys, runtime_expected_keys = _source_key_sets(
            cursor, inferred_hint,
        )
        missing_runtime_keys = sorted(runtime_expected_keys - registered_keys)
        airflow_dag_run_state = None
        airflow_lookup_error = None
        if run_id:
            selected_run_id = run_id
            run_id_source = "explicit"
        else:
            (
                airflow_run_id,
                airflow_dag_run_state,
                airflow_lookup_error,
            ) = _latest_scheduled_airflow_run(dag_id)
            if airflow_run_id:
                selected_run_id = airflow_run_id
                run_id_source = "airflow_metadata"
            else:
                selected_run_id = _latest_scheduled_run(cursor, dag_id)
                run_id_source = "source_runs"
        if not selected_run_id:
            return {
                "dag_id": dag_id,
                "run_id": None,
                "run_id_source": run_id_source,
                "airflow_dag_run_state": airflow_dag_run_state,
                "airflow_lookup_error": airflow_lookup_error,
                "schedule_hint": inferred_hint,
                "status": "unknown",
                "reason": (
                    f"Airflow scheduled-run lookup failed ({airflow_lookup_error})"
                    if airflow_lookup_error
                    else "no scheduled Airflow DagRun or attributed source run found"
                ),
                "expected_source_count": len(expected_keys),
                "registered_schedule_source_count": len(expected_keys),
                "runtime_expected_source_count": len(runtime_expected_keys),
                "missing_runtime_source_keys": missing_runtime_keys,
                "expected_source_keys": sorted(expected_keys),
                "sources": [],
            }

        cursor.execute(
            """
            SELECT id, source_key, task_id, map_index, try_number, status,
                   http_status, row_count, accepted_count, rejected_count,
                   duplicate_count, started_at, finished_at, error_message
            FROM source_runs
            WHERE dag_id=%s AND dag_run_id=%s
            ORDER BY source_key, task_id, map_index,
                     try_number DESC, started_at DESC, id DESC
            """,
            (dag_id, selected_run_id),
        )
        all_attempts = cursor.fetchall()
        unattributed_legacy_row_count = 0
        unattributed_legacy_source_count = 0
        unattributed_legacy_source_keys: list[str] = []
        if not all_attempts:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT source_key)
                FROM source_runs
                WHERE dag_run_id=%s AND dag_id IS NULL
                """,
                (selected_run_id,),
            )
            orphan_counts = cursor.fetchone()
            unattributed_legacy_row_count = int(orphan_counts[0] or 0)
            unattributed_legacy_source_count = int(orphan_counts[1] or 0)
            if unattributed_legacy_row_count:
                cursor.execute(
                    """
                    SELECT DISTINCT source_key
                    FROM source_runs
                    WHERE dag_run_id=%s AND dag_id IS NULL
                    ORDER BY source_key
                    """,
                    (selected_run_id,),
                )
                unattributed_legacy_source_keys = [
                    str(row[0]) for row in cursor.fetchall()
                ]
                return {
                    "dag_id": dag_id,
                    "run_id": selected_run_id,
                    "run_id_source": run_id_source,
                    "airflow_dag_run_state": airflow_dag_run_state,
                    "airflow_lookup_error": airflow_lookup_error,
                    "schedule_hint": inferred_hint,
                    "status": "unknown",
                    "reason": (
                        "source_runs rows for this run ID lack dag_id; refusing to attribute "
                        "them because scheduled run IDs can overlap across DAGs"
                    ),
                    "unattributed_legacy_row_count": unattributed_legacy_row_count,
                    "unattributed_legacy_source_count": unattributed_legacy_source_count,
                    "unattributed_legacy_source_keys": unattributed_legacy_source_keys,
                    "expected_source_count": len(expected_keys),
                    "reported_source_count": 0,
                    "registered_schedule_source_count": len(expected_keys),
                    "runtime_expected_source_count": len(runtime_expected_keys),
                    "missing_runtime_source_keys": missing_runtime_keys,
                    "expected_source_keys": sorted(expected_keys),
                    "sources": [],
                }
            return {
                "dag_id": dag_id,
                "run_id": selected_run_id,
                "run_id_source": run_id_source,
                "airflow_dag_run_state": airflow_dag_run_state,
                "airflow_lookup_error": airflow_lookup_error,
                "schedule_hint": inferred_hint,
                "status": "unknown",
                "reason": (
                    "Airflow scheduled DagRun exists but no source_runs rows were "
                    "attributed to this DAG/run"
                    if run_id_source == "airflow_metadata"
                    else "no source_runs rows found for this DAG/run"
                ),
                "unattributed_legacy_row_count": 0,
                "unattributed_legacy_source_count": 0,
                "unattributed_legacy_source_keys": [],
                "expected_source_count": len(expected_keys),
                "reported_source_count": 0,
                "registered_schedule_source_count": len(expected_keys),
                "runtime_expected_source_count": len(runtime_expected_keys),
                "missing_source_keys": sorted(expected_keys),
                "missing_runtime_source_keys": missing_runtime_keys,
                "expected_source_keys": sorted(expected_keys),
                "sources": [],
            }

        latest_attempts: dict[tuple[str, str, Any], tuple[Any, ...]] = {}
        for row in all_attempts:
            key = (str(row[1]), str(row[2] or ""), row[3])
            latest_attempts.setdefault(key, row)

        source_keys = {str(row[1]) for row in latest_attempts.values()}
        metadata: dict[str, dict[str, Any]] = {}
        if source_keys:
            placeholders = ",".join(["%s"] * len(source_keys))
            cursor.execute(
                "SELECT source_key, source_name, adapter_key, endpoint_url, target_domain, "
                "entity_type, schedule_hint, config_text, enabled, approval_status "
                "FROM source_registry "
                f"WHERE source_key IN ({placeholders})",
                tuple(sorted(source_keys)),
            )
            for row in cursor.fetchall():
                source_key, source_name, adapter_key, endpoint_url = row[:4]
                target_domain, entity_type, schedule, config_text, enabled, approval = row[4:]
                try:
                    config = json.loads(config_text or "{}")
                except (TypeError, ValueError):
                    config = {}
                metadata[str(source_key)] = {
                    "name": str(source_name),
                    "adapter_key": str(adapter_key),
                    "endpoint_url": str(endpoint_url),
                    "target_domain": str(target_domain),
                    "entity_type": str(entity_type),
                    "schedule_hint": str(schedule),
                    "config": config,
                    "enabled": bool(enabled),
                    "approval_status": str(approval),
                }

        enabled_fields: dict[str, set[str]] = {key: set() for key in source_keys}
        if source_keys:
            placeholders = ",".join(["%s"] * len(source_keys))
            cursor.execute(
                "SELECT source_key, source_field FROM source_fields "
                f"WHERE enabled=1 AND source_key IN ({placeholders})",
                tuple(sorted(source_keys)),
            )
            for source_key, source_field in cursor.fetchall():
                enabled_fields.setdefault(str(source_key), set()).add(str(source_field))

        stock_master = {
            str(code).strip(): str(market).strip()
            for code, market in _load_stock_market_codes(common_equity_only=True)
        }
        master_counts = {
            "listed": sum(market == "上市" for market in stock_master.values()),
            "otc": sum(market == "上櫃" for market in stock_master.values()),
        }

        sources: list[dict[str, Any]] = []
        for key in sorted(
            latest_attempts,
            key=lambda item: (item[0], item[1], str(item[2])),
        ):
            row = latest_attempts[key]
            source_key = str(row[1])
            registry_metadata = metadata.get(source_key, {})
            source_name = str(registry_metadata.get("name") or source_key)
            entity_type = str(registry_metadata.get("entity_type") or "")
            runtime_definition = SOURCE_DEFINITIONS_BY_KEY.get(source_key)
            contract_drift: list[str] = []
            if runtime_definition is None:
                contract_drift.append("runtime_definition_missing")
            else:
                comparisons = {
                    "adapter_key": (
                        registry_metadata.get("adapter_key"),
                        runtime_definition.get("adapter_key"),
                    ),
                    "endpoint_url": (
                        registry_metadata.get("endpoint_url"),
                        runtime_definition.get("endpoint_url"),
                    ),
                    "target_domain": (
                        registry_metadata.get("target_domain"),
                        runtime_definition.get("target_domain"),
                    ),
                    "entity_type": (
                        registry_metadata.get("entity_type"),
                        runtime_definition.get("entity_type"),
                    ),
                    "schedule_hint": (
                        registry_metadata.get("schedule_hint"),
                        runtime_definition.get("schedule_hint"),
                    ),
                    "config": (
                        registry_metadata.get("config"),
                        runtime_definition.get("config"),
                    ),
                    "enabled_approval": (
                        (
                            registry_metadata.get("enabled"),
                            registry_metadata.get("approval_status"),
                        ),
                        (
                            bool(runtime_definition.get("enabled")),
                            runtime_definition.get("approval_status"),
                        ),
                    ),
                    "fields": (
                        enabled_fields.get(source_key, set()),
                        {str(field[0]) for field in runtime_definition.get("fields", ())},
                    ),
                }
                contract_drift = [
                    field for field, (stored, current) in comparisons.items()
                    if stored != current
                ]
            item: dict[str, Any] = {
                "source_key": source_key,
                "source_name": source_name,
                "task_id": str(row[2] or ""),
                "map_index": row[3],
                "try_number": row[4],
                "status": str(row[5]),
                "http_status": row[6],
                "runtime_adapter_key": (
                    runtime_definition.get("adapter_key") if runtime_definition else None
                ),
                "registry_adapter_key": registry_metadata.get("adapter_key"),
                "runtime_contract_match": not contract_drift,
                "contract_drift": contract_drift,
                "row_count": int(row[7] or 0),
                "accepted_count": int(row[8] or 0),
                "quarantined_count": int(row[9] or 0),
                "duplicate_count": int(row[10] or 0),
                "started_at": row[11],
                "finished_at": row[12],
                "error_type": str(row[13] or "").partition(":")[0] or None,
                "effective_date_min": None,
                "effective_date_max": None,
                "effective_date_max_age_days": SOURCE_EFFECTIVE_DATE_MAX_AGE_DAYS.get(source_key),
                "effective_date_age_days": None,
                "latest_event_time": None,
                "latest_event_age_days": None,
                "event_time_max_age_days": SOURCE_EVENT_TIME_MAX_AGE_DAYS.get(source_key),
                "accepted_entity_count": 0,
                "matched_codes_by_market": {"listed": 0, "otc": 0},
                "unmatched_code_count": 0,
                "market_coverage": None,
                "pit_observed_accepted_record_count": None,
                "pit_versioned_accepted_record_count": None,
                "pit_unversioned_accepted_record_count": None,
                "pit_version_coverage_pct": None,
                "quality_flags": [],
            }

            if item["quarantined_count"] > 0:
                item["quality_flags"].append("quarantined_records_present")
            if item["duplicate_count"] > 0:
                item["quality_flags"].append("duplicate_records_present")

            accounted_rows = (
                item["accepted_count"] + item["quarantined_count"]
                + item["duplicate_count"]
            )
            if accounted_rows != item["row_count"]:
                item["quality_flags"].append("row_accounting_mismatch")
            if item["status"] == "success" and item["row_count"] == 0:
                item["quality_flags"].append("empty_response_requires_source_semantics")
            if (
                item["status"] == "success"
                and item["accepted_count"] == 0
                and item["row_count"] > 0
            ):
                item["quality_flags"].append("no_accepted_records_do_not_infer_zero_activity")

            if row[5] == "success" and row[11] and row[12]:
                # source_runs timestamps have second precision, while
                # source_record_versions.captured_at keeps microseconds. A
                # version written by this exact run can therefore sort after
                # finished_at even though capture happened before run commit.
                # Accept that version only with exact DAG/run lineage; prior
                # source-feed versions remain eligible when captured by start.
                cursor.execute(
                    """
                    SELECT COUNT(DISTINCT sr.record_hash),
                           COUNT(DISTINCT CASE WHEN EXISTS (
                               SELECT 1
                               FROM source_record_versions v
                               WHERE v.source_key=sr.source_key
                                 AND v.record_hash=sr.record_hash
                                 AND v.capture_basis='source_feed'
                                 AND (
                                     v.captured_at <= %s
                                     OR (v.dag_id=%s AND v.dag_run_id=%s)
                                 )
                           ) THEN sr.record_hash END)
                    FROM source_records sr
                    WHERE sr.source_key=%s AND sr.quality_status='ok'
                      AND sr.observed_at >= %s AND sr.observed_at <= %s
                    """,
                    (
                        row[11], dag_id, selected_run_id,
                        source_key, row[11], row[12],
                    ),
                )
                pit_observed_count, pit_versioned_count = cursor.fetchone()
                pit_observed_count = int(pit_observed_count or 0)
                pit_versioned_count = int(pit_versioned_count or 0)
                item["pit_observed_accepted_record_count"] = pit_observed_count
                item["pit_versioned_accepted_record_count"] = pit_versioned_count
                item["pit_unversioned_accepted_record_count"] = max(
                    0, pit_observed_count - pit_versioned_count
                )
                item["pit_version_coverage_pct"] = (
                    round(pit_versioned_count / pit_observed_count * 100, 2)
                    if pit_observed_count else None
                )
                if pit_observed_count != item["accepted_count"]:
                    item["quality_flags"].append(
                        "accepted_record_observation_count_mismatch"
                    )
                if item["pit_unversioned_accepted_record_count"] > 0:
                    item["quality_flags"].append(
                        "accepted_records_missing_pit_versions"
                    )

                cursor.execute(
                    """
                    SELECT MIN(effective_date), MAX(effective_date),
                           COUNT(DISTINCT entity_id), MAX(event_time)
                    FROM source_records
                    WHERE source_key=%s AND quality_status='ok'
                      AND observed_at >= %s AND observed_at <= %s
                    """,
                    (source_key, row[11], row[12]),
                )
                effective_min, effective_max, entity_count, latest_event_time = cursor.fetchone()
                item["effective_date_min"] = effective_min
                item["effective_date_max"] = effective_max
                item["latest_event_time"] = latest_event_time
                item["accepted_entity_count"] = int(entity_count or 0)
                if item["accepted_count"] > 0 and effective_min is None:
                    item["quality_flags"].append("accepted_records_without_effective_date")

                if (
                    item["effective_date_max_age_days"] is not None
                    and item["accepted_count"] > 0
                ):
                    if effective_max is None:
                        item["quality_flags"].append(
                            "accepted_records_without_effective_date_for_freshness"
                        )
                    else:
                        run_date = row[11].date() if isinstance(row[11], dt.datetime) else row[11]
                        effective_date = (
                            effective_max.date()
                            if isinstance(effective_max, dt.datetime)
                            else effective_max
                        )
                        item["effective_date_age_days"] = (run_date - effective_date).days
                        if item["effective_date_age_days"] < 0:
                            item["quality_flags"].append("latest_effective_date_in_future")
                        elif (
                            item["effective_date_age_days"]
                            > item["effective_date_max_age_days"]
                        ):
                            item["quality_flags"].append(
                                "latest_effective_date_older_than_expected"
                            )

                if item["event_time_max_age_days"] is not None and item["accepted_count"] > 0:
                    if latest_event_time is None:
                        item["quality_flags"].append(
                            "accepted_records_without_freshness_event_time"
                        )
                    else:
                        run_date = row[11].date() if isinstance(row[11], dt.datetime) else row[11]
                        event_date = (
                            latest_event_time.date()
                            if isinstance(latest_event_time, dt.datetime)
                            else latest_event_time
                        )
                        item["latest_event_age_days"] = (run_date - event_date).days
                        if item["latest_event_age_days"] > item["event_time_max_age_days"]:
                            item["quality_flags"].append(
                                "latest_event_time_older_than_expected"
                            )

                if entity_type == "stock":
                    # Some official feeds also persist non-security snapshot
                    # metadata; only stock rows belong in the market join audit.
                    cursor.execute(
                        """
                        SELECT DISTINCT entity_id
                        FROM source_records
                        WHERE source_key=%s AND quality_status='ok'
                          AND entity_type='stock'
                          AND entity_id IS NOT NULL
                          AND observed_at >= %s AND observed_at <= %s
                        """,
                        (source_key, row[11], row[12]),
                    )
                    accepted_codes = {
                        str(value[0]).strip()
                        for value in cursor.fetchall()
                        if str(value[0]).strip()
                    }
                    matched_codes = {
                        code: stock_master[code]
                        for code in accepted_codes
                        if code in stock_master
                    }
                    item["accepted_entity_count"] = len(accepted_codes)
                    item["matched_codes_by_market"] = {
                        "listed": sum(market == "上市" for market in matched_codes.values()),
                        "otc": sum(market == "上櫃" for market in matched_codes.values()),
                    }
                    item["unmatched_code_count"] = len(accepted_codes - matched_codes.keys())
                    if (
                        runtime_definition
                        and runtime_definition.get("config", {}).get("common_equity_only")
                    ):
                        configured_market = str(
                            runtime_definition.get("config", {}).get("market") or ""
                        ).strip().upper()
                        in_scope_markets = {
                            "TWSE": {"listed"},
                            "TPEX": {"otc"},
                            "BOTH": {"listed", "otc"},
                        }.get(configured_market)
                        if in_scope_markets is None:
                            item["quality_flags"].append(
                                "market_coverage_scope_unknown"
                            )
                        else:
                            item["market_coverage"] = {
                                "scope": "current_common_equity_master",
                                "basis": "accepted_codes_matched_to_current_master",
                                "by_market": {
                                    market_key: {
                                        "in_scope": market_key in in_scope_markets,
                                        "matched_code_count": (
                                            item["matched_codes_by_market"][market_key]
                                            if market_key in in_scope_markets else None
                                        ),
                                        "master_code_count": (
                                            master_counts[market_key]
                                            if market_key in in_scope_markets else None
                                        ),
                                        "coverage_pct": (
                                            round(
                                                item["matched_codes_by_market"][market_key]
                                                / master_counts[market_key] * 100,
                                                2,
                                            )
                                            if (
                                                market_key in in_scope_markets
                                                and master_counts[market_key] > 0
                                            ) else None
                                        ),
                                    }
                                    for market_key in ("listed", "otc")
                                },
                            }
                            if runtime_definition.get("config", {}).get(
                                "require_full_common_equity_coverage"
                            ) and any(
                                details["in_scope"]
                                and details["coverage_pct"] != 100.0
                                for details in item["market_coverage"]["by_market"].values()
                            ):
                                item["quality_flags"].append(
                                    "required_common_equity_coverage_incomplete"
                                )
                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM source_records
                        WHERE source_key=%s AND quality_status='ok'
                          AND entity_type='stock'
                          AND observed_at >= %s AND observed_at <= %s
                        """,
                        (source_key, row[11], row[12]),
                    )
                    accepted_stock_row_count = int((cursor.fetchone() or (0,))[0] or 0)
                    if accepted_stock_row_count > 0 and not accepted_codes:
                        item["quality_flags"].append("accepted_stock_records_without_entity_codes")
                    if item["unmatched_code_count"]:
                        item["quality_flags"].append("accepted_codes_outside_common_stock_master")
                    if (
                        source_key == "tpex_quarterly_forecast_variance_events"
                        and accepted_stock_row_count == 0
                        and item["accepted_count"] > 0
                    ):
                        item["quality_flags"].append(
                            "snapshot_metadata_only_no_company_event_rows"
                        )

            sources.append(item)

        if dag_id == "source_extension_collect_after_close":
            source_by_key = {source["source_key"]: source for source in sources}
            index_sources = [
                source_by_key[key]
                for key in AFTER_CLOSE_MARKET_INDEX_SOURCE_KEYS
                if key in source_by_key
            ]
            run_dates = [
                date_text for date_text in (
                    _date_text(source.get("started_at")) for source in sources
                )
                if date_text
            ]
            price_snapshot_error_type = None
            if run_dates:
                run_date = min(run_dates)
                try:
                    price_snapshot_date = _latest_official_quote_date(run_date)
                except Exception as exc:
                    price_snapshot_date = None
                    price_snapshot_error_type = type(exc).__name__
            else:
                run_date = None
                price_snapshot_date = None
            for index_source in index_sources:
                index_date = _date_text(index_source.get("effective_date_max"))
                if index_source["status"] != "success":
                    alignment_status = "index_source_not_successful"
                elif price_snapshot_date is None:
                    alignment_status = "official_quote_snapshot_date_unavailable"
                    index_source["quality_flags"].append(
                        "official_quote_snapshot_date_unavailable_for_alignment"
                    )
                elif not index_date:
                    alignment_status = "index_effective_date_missing"
                elif index_date != price_snapshot_date:
                    alignment_status = "mismatch"
                    index_source["quality_flags"].append(
                        "market_index_not_aligned_to_stock_price_snapshot"
                    )
                else:
                    alignment_status = "aligned"
                index_source["effective_date_alignment"] = {
                    "status": alignment_status,
                    "basis": (
                        "source_records.official_market_quote.MAX(effective_date)"
                        "<=source_run_date"
                    ),
                    "source_run_date": run_date,
                    "index_effective_date": index_date,
                    "official_quote_as_of_date": price_snapshot_date,
                    "snapshot_lookup_error_type": price_snapshot_error_type,
                }
            quote_sources = [
                source_by_key[key]
                for key in AFTER_CLOSE_MARKET_QUOTE_SOURCE_KEYS
                if key in source_by_key
            ]
            for quote_source in quote_sources:
                quote_date = _date_text(quote_source.get("effective_date_max"))
                if quote_source["status"] != "success":
                    alignment_status = "quote_source_not_successful"
                elif price_snapshot_date is None:
                    alignment_status = "official_quote_snapshot_date_unavailable"
                    quote_source["quality_flags"].append(
                        "official_quote_snapshot_date_unavailable_for_quote_alignment"
                    )
                elif not quote_date:
                    alignment_status = "quote_effective_date_missing"
                    quote_source["quality_flags"].append(
                        "market_quote_effective_date_missing_for_alignment"
                    )
                elif quote_date != price_snapshot_date:
                    alignment_status = "mismatch"
                    quote_source["quality_flags"].append(
                        "market_quote_not_aligned_to_stock_price_snapshot"
                    )
                else:
                    alignment_status = "aligned"
                quote_source["effective_date_alignment"] = {
                    "status": alignment_status,
                    "basis": "market_quote.effective_date_max==official_quote_as_of_date",
                    "source_run_date": run_date,
                    "quote_effective_date": quote_date,
                    "official_quote_as_of_date": price_snapshot_date,
                    "snapshot_lookup_error_type": price_snapshot_error_type,
                }
            stock_flow_sources = [
                source_by_key[key]
                for key in AFTER_CLOSE_STOCK_FLOW_SOURCE_KEYS
                if key in source_by_key
            ]
            for flow_source in stock_flow_sources:
                flow_date = _date_text(flow_source.get("effective_date_max"))
                if flow_source["status"] != "success":
                    alignment_status = "flow_source_not_successful"
                elif price_snapshot_date is None:
                    alignment_status = "official_quote_snapshot_date_unavailable"
                    flow_source["quality_flags"].append(
                        "official_quote_snapshot_date_unavailable_for_flow_alignment"
                    )
                elif not flow_date:
                    alignment_status = "flow_effective_date_missing"
                    flow_source["quality_flags"].append(
                        "institutional_flow_effective_date_missing_for_alignment"
                    )
                elif flow_date != price_snapshot_date:
                    alignment_status = "mismatch"
                    flow_source["quality_flags"].append(
                        "institutional_flow_not_aligned_to_stock_price_snapshot"
                    )
                else:
                    alignment_status = "aligned"
                flow_source["effective_date_alignment"] = {
                    "status": alignment_status,
                    "basis": "tpex_institutional_flow.effective_date_max==official_quote_as_of_date",
                    "source_run_date": run_date,
                    "flow_effective_date": flow_date,
                    "official_quote_as_of_date": price_snapshot_date,
                    "snapshot_lookup_error_type": price_snapshot_error_type,
                }

        reported_keys = {source["source_key"] for source in sources}
        missing_keys = sorted(expected_keys - reported_keys)
        missing_runtime_keys = sorted(runtime_expected_keys - registered_keys)
        all_success = bool(sources) and all(source["status"] == "success" for source in sources)
        contract_drift_keys = sorted(
            source["source_key"] for source in sources
            if not source["runtime_contract_match"]
        )
        all_tasks_succeeded = (
            bool(expected_keys) and not missing_keys and not missing_runtime_keys
            and all_success and not contract_drift_keys
        )
        quality_flags_present = any(source["quality_flags"] for source in sources)
        if not all_tasks_succeeded:
            status = "partial"
        elif quality_flags_present:
            status = "all_expected_tasks_succeeded_data_review_required"
        else:
            status = "all_expected_tasks_succeeded_data_completeness_unproven"
        return {
            "dag_id": dag_id,
            "run_id": selected_run_id,
            "run_id_source": run_id_source,
            "airflow_dag_run_state": airflow_dag_run_state,
            "airflow_lookup_error": airflow_lookup_error,
            "schedule_hint": inferred_hint,
            "status": status,
            "reason": None,
            "unattributed_legacy_row_count": 0,
            "unattributed_legacy_source_count": 0,
            "note": (
                "Task success is not proof of effective-date/event-time freshness, full-market coverage, "
                "or feed completeness; interpret counts and joins using each source's semantics."
            ),
            "expected_source_count": len(expected_keys),
            "reported_source_count": len(reported_keys),
            "registered_schedule_source_count": len(expected_keys),
            "runtime_expected_source_count": len(runtime_expected_keys),
            "missing_source_keys": missing_keys,
            "missing_runtime_source_keys": missing_runtime_keys,
            "expected_source_keys": sorted(expected_keys),
            "contract_drift_source_keys": contract_drift_keys,
            "listed_common_stock_master_count": master_counts["listed"],
            "otc_common_stock_master_count": master_counts["otc"],
            "sources": sources,
        }
    finally:
        connection.close()
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument(
        "--run-id",
        help="Defaults to the latest attributed source run, then scheduled Airflow run.",
    )
    parser.add_argument("--schedule-hint", choices=("intraday", "daily", "daily_after_close", "monthly"))
    args = parser.parse_args()
    try:
        report = audit_source_run(
            dag_id=args.dag_id,
            run_id=args.run_id,
            schedule_hint=args.schedule_hint,
        )
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2, default=_json_value)
        sys.stdout.write("\n")
        return 0
    except Exception as exc:
        print(f"source-run audit failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
