"""DDL for the metadata-driven source extension layer.

The existing crawlers write directly to wide, source-specific tables.  This
layer keeps source selection and normalized cross-domain records separate so
new approved sources can be mapped into Airflow without changing those
legacy tables on every iteration.
"""

from __future__ import annotations

from typing import Iterable


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS source_registry (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        source_key VARCHAR(96) NOT NULL,
        source_name VARCHAR(255) NOT NULL,
        adapter_key VARCHAR(64) NOT NULL,
        endpoint_url VARCHAR(1024) NOT NULL,
        target_domain VARCHAR(64) NOT NULL,
        entity_type VARCHAR(64) NOT NULL DEFAULT 'macro',
        purpose VARCHAR(512) NOT NULL,
        join_key VARCHAR(255) NOT NULL,
        purpose_score DECIMAL(5,2) NOT NULL DEFAULT 0,
        joinability_score DECIMAL(5,2) NOT NULL DEFAULT 0,
        authority_score DECIMAL(5,2) NOT NULL DEFAULT 0,
        freshness_score DECIMAL(5,2) NOT NULL DEFAULT 0,
        cost_score DECIMAL(5,2) NOT NULL DEFAULT 0,
        priority_score DECIMAL(5,2) NOT NULL DEFAULT 0,
        approval_status VARCHAR(32) NOT NULL DEFAULT 'review',
        enabled TINYINT(1) NOT NULL DEFAULT 0,
        required_credential VARCHAR(128) NULL,
        schedule_hint VARCHAR(64) NOT NULL DEFAULT 'daily',
        config_text LONGTEXT NOT NULL,
        last_success_at DATETIME NULL,
        last_error TEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_source_registry_key (source_key),
        KEY ix_source_registry_status (approval_status, enabled)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS source_fields (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        source_key VARCHAR(96) NOT NULL,
        source_field VARCHAR(128) NOT NULL,
        target_field VARCHAR(128) NOT NULL,
        data_type VARCHAR(32) NOT NULL,
        purpose VARCHAR(512) NOT NULL,
        required_flag TINYINT(1) NOT NULL DEFAULT 0,
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_source_field (source_key, source_field),
        KEY ix_source_fields_target (target_field)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS source_records (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        source_key VARCHAR(96) NOT NULL,
        source_id BIGINT UNSIGNED NULL,
        record_key VARCHAR(255) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        target_domain VARCHAR(64) NOT NULL,
        entity_type VARCHAR(64) NULL,
        entity_id VARCHAR(128) NULL,
        event_type VARCHAR(96) NOT NULL,
        event_time DATETIME NULL,
        effective_date DATE NULL,
        title VARCHAR(512) NULL,
        metric_name VARCHAR(128) NULL,
        metric_value DECIMAL(30,8) NULL,
        value_text TEXT NULL,
        unit VARCHAR(64) NULL,
        currency VARCHAR(32) NULL,
        source_url VARCHAR(1024) NOT NULL,
        payload LONGTEXT NOT NULL,
        quality_status VARCHAR(32) NOT NULL DEFAULT 'ok',
        observed_at DATETIME NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_source_record_hash (record_hash),
        KEY ix_source_records_event (source_key, event_type, effective_date),
        KEY ix_source_records_entity (entity_type, entity_id, effective_date),
        KEY ix_source_records_quality (quality_status, observed_at),
        KEY ix_source_records_scan_effective (event_type, entity_type, quality_status, effective_date),
        KEY ix_source_records_scan_event_time (event_type, entity_type, quality_status, event_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS source_record_versions (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        source_key VARCHAR(96) NOT NULL,
        source_id BIGINT UNSIGNED NULL,
        record_key VARCHAR(255) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        version_hash CHAR(64) NULL,
        target_domain VARCHAR(64) NOT NULL,
        entity_type VARCHAR(64) NULL,
        entity_id VARCHAR(128) NULL,
        event_type VARCHAR(96) NOT NULL,
        event_time DATETIME NULL,
        effective_date DATE NULL,
        title VARCHAR(512) NULL,
        metric_name VARCHAR(128) NULL,
        metric_value DECIMAL(30,8) NULL,
        value_text TEXT NULL,
        unit VARCHAR(64) NULL,
        currency VARCHAR(32) NULL,
        source_url VARCHAR(1024) NOT NULL,
        payload LONGTEXT NOT NULL,
        quality_status VARCHAR(32) NOT NULL DEFAULT 'ok',
        captured_at DATETIME(6) NOT NULL,
        capture_basis VARCHAR(32) NOT NULL,
        dag_id VARCHAR(250) NULL,
        dag_run_id VARCHAR(250) NULL,
        task_id VARCHAR(250) NULL,
        map_index INT NULL,
        try_number INT NULL,
        PRIMARY KEY (id),
        KEY ix_source_record_versions_asof (record_hash, captured_at),
        KEY ix_source_record_versions_entity_asof
            (entity_type, entity_id, event_type, captured_at),
        KEY ix_source_record_versions_source_asof (source_key, captured_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS source_record_fields (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        field_hash CHAR(64) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        source_key VARCHAR(96) NOT NULL,
        source_field VARCHAR(128) NOT NULL,
        target_field VARCHAR(128) NOT NULL,
        data_type VARCHAR(32) NOT NULL,
        value_text TEXT NULL,
        value_numeric DECIMAL(30,8) NULL,
        value_date DATE NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_source_record_field_hash (field_hash),
        KEY ix_source_record_fields_lookup (source_key, target_field),
        KEY ix_source_record_fields_record (record_hash)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS source_runs (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        source_key VARCHAR(96) NOT NULL,
        started_at DATETIME NOT NULL,
        finished_at DATETIME NULL,
        status VARCHAR(32) NOT NULL,
        http_status INT NULL,
        row_count INT NOT NULL DEFAULT 0,
        accepted_count INT NOT NULL DEFAULT 0,
        rejected_count INT NOT NULL DEFAULT 0,
        duplicate_count INT NOT NULL DEFAULT 0,
        dag_run_id VARCHAR(250) NULL,
        dag_id VARCHAR(250) NULL,
        task_id VARCHAR(250) NULL,
        map_index INT NULL,
        try_number INT NULL,
        error_message TEXT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        KEY ix_source_runs_source (source_key, started_at),
        KEY ix_source_runs_status (status, started_at),
        KEY ix_source_runs_dag_run (dag_id, dag_run_id, started_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_alert_rules (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        rule_key VARCHAR(128) NOT NULL,
        rule_name VARCHAR(255) NOT NULL,
        event_type VARCHAR(96) NOT NULL,
        config_text LONGTEXT NOT NULL,
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_portfolio_alert_rule (rule_key),
        KEY ix_portfolio_alert_rules_status (enabled, event_type)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_notification_record (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        rule_key VARCHAR(128) NOT NULL,
        record_hash CHAR(64) NOT NULL,
        source_key VARCHAR(96) NOT NULL,
        entity_id VARCHAR(128) NOT NULL,
        event_type VARCHAR(96) NOT NULL,
        notification_channel VARCHAR(64) NOT NULL,
        status VARCHAR(32) NOT NULL,
        message_text TEXT NOT NULL,
        notified_at DATETIME NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_portfolio_notification (rule_key, record_hash),
        KEY ix_portfolio_notification_entity (entity_id, notified_at),
        KEY ix_portfolio_notification_status (status, notified_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_notification_state (
        state_key VARCHAR(128) NOT NULL,
        state_text LONGTEXT NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (state_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_runs (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        run_key VARCHAR(128) NOT NULL,
        source_dag_run_id VARCHAR(250) NULL,
        source_dag_id VARCHAR(250) NULL,
        scan_mode VARCHAR(32) NOT NULL,
        as_of_date DATE NULL,
        status VARCHAR(32) NOT NULL,
        source_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
        universe_count INT NOT NULL DEFAULT 0,
        price_count INT NOT NULL DEFAULT 0,
        flow_count INT NOT NULL DEFAULT 0,
        revenue_count INT NOT NULL DEFAULT 0,
        financial_count INT NOT NULL DEFAULT 0,
        borrow_count INT NOT NULL DEFAULT 0,
        insider_count INT NOT NULL DEFAULT 0,
        dividend_count INT NOT NULL DEFAULT 0,
        candidate_count INT NOT NULL DEFAULT 0,
        notified_count INT NOT NULL DEFAULT 0,
        started_at DATETIME NOT NULL,
        finished_at DATETIME NULL,
        error_message TEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_market_signal_run (run_key),
        KEY ix_market_signal_run_date (as_of_date, scan_mode, status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_run_market_coverage (
        run_key VARCHAR(128) NOT NULL,
        market VARCHAR(16) NOT NULL,
        as_of_date DATE NULL,
        universe_count INT NOT NULL DEFAULT 0,
        price_row_code_count INT NULL DEFAULT NULL,
        price_count INT NOT NULL DEFAULT 0,
        price_invalid_close_code_count INT NULL DEFAULT NULL,
        price_no_trade_code_count INT NULL DEFAULT NULL,
        flow_count INT NOT NULL DEFAULT 0,
        traded_without_flow_count INT NULL DEFAULT NULL,
        price_coverage_pct DECIMAL(6,2) NOT NULL DEFAULT 0,
        flow_coverage_pct DECIMAL(6,2) NOT NULL DEFAULT 0,
        flow_readiness_json LONGTEXT NULL,
        observed_at DATETIME NOT NULL,
        PRIMARY KEY (run_key, market),
        KEY ix_market_signal_coverage_date (market, as_of_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_candidates (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        run_key VARCHAR(128) NOT NULL,
        candidate_hash CHAR(64) NOT NULL,
        entity_type VARCHAR(32) NOT NULL,
        entity_id VARCHAR(128) NOT NULL,
        security_name VARCHAR(255) NULL,
        market VARCHAR(32) NULL,
        industry VARCHAR(128) NULL,
        as_of_date DATE NULL,
        observed_at DATETIME NOT NULL,
        signal_count SMALLINT NOT NULL DEFAULT 0,
        signal_keys VARCHAR(512) NOT NULL,
        score DECIMAL(8,4) NOT NULL DEFAULT 0,
        details_text LONGTEXT NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_market_signal_candidate (run_key, candidate_hash),
        KEY ix_market_signal_candidate_date (as_of_date, entity_type, score)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_outcomes (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        candidate_id BIGINT UNSIGNED NOT NULL,
        run_key VARCHAR(128) NOT NULL,
        entity_id VARCHAR(128) NOT NULL,
        market VARCHAR(32) NOT NULL,
        source_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
        outcome_price_source_key VARCHAR(96) NOT NULL DEFAULT 'legacy_stock_table',
        signal_date DATE NOT NULL,
        direction VARCHAR(32) NOT NULL,
        outcome_basis VARCHAR(32) NOT NULL,
        horizon_sessions SMALLINT NOT NULL,
        entry_date DATE NULL,
        exit_date DATE NULL,
        entry_close DECIMAL(20,6) NULL,
        exit_close DECIMAL(20,6) NULL,
        stock_return_pct DECIMAL(12,6) NULL,
        benchmark_code VARCHAR(16) NOT NULL,
        benchmark_entry DECIMAL(20,8) NULL,
        benchmark_exit DECIMAL(20,8) NULL,
        benchmark_return_pct DECIMAL(12,6) NULL,
        excess_return_pp DECIMAL(12,6) NULL,
        directional_hit TINYINT(1) NULL,
        status VARCHAR(32) NOT NULL,
        error_reason VARCHAR(128) NULL,
        settled_at DATETIME NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_market_signal_outcome (candidate_id, outcome_basis, horizon_sessions),
        KEY ix_market_signal_outcome_date (signal_date, horizon_sessions, outcome_basis),
        KEY ix_market_signal_outcome_entity (entity_id, signal_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_followups (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        candidate_id BIGINT UNSIGNED NOT NULL,
        run_key VARCHAR(128) NOT NULL,
        entity_id VARCHAR(128) NOT NULL,
        market VARCHAR(32) NOT NULL,
        source_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
        signal_date DATE NOT NULL,
        direction VARCHAR(32) NOT NULL,
        family_directions_json LONGTEXT NOT NULL,
        family_status_json LONGTEXT NOT NULL,
        policy_version VARCHAR(64) NOT NULL DEFAULT 'unknown',
        evaluation_universe_count INT NULL,
        candidate_selected TINYINT(1) NOT NULL DEFAULT 0,
        delivery_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
        notified_at DATETIME NULL,
        horizon_key VARCHAR(8) NOT NULL,
        target_date DATE NOT NULL,
        event_entry_close DECIMAL(20,6) NULL,
        event_exit_close DECIMAL(20,6) NULL,
        stock_return_pct DECIMAL(12,6) NULL,
        quote_source_key VARCHAR(96) NOT NULL,
        benchmark_code VARCHAR(16) NOT NULL,
        benchmark_entry DECIMAL(20,8) NULL,
        benchmark_exit DECIMAL(20,8) NULL,
        benchmark_return_pct DECIMAL(12,6) NULL,
        excess_return_pp DECIMAL(12,6) NULL,
        benchmark_source_key VARCHAR(96) NOT NULL,
        execution_entry_date DATE NULL,
        execution_entry_open DECIMAL(20,6) NULL,
        execution_exit_close DECIMAL(20,6) NULL,
        execution_return_pct DECIMAL(12,6) NULL,
        max_favorable_excursion_pct DECIMAL(12,6) NULL,
        max_adverse_excursion_pct DECIMAL(12,6) NULL,
        status VARCHAR(32) NOT NULL,
        benchmark_status VARCHAR(32) NOT NULL,
        execution_status VARCHAR(32) NOT NULL,
        error_reason VARCHAR(128) NULL,
        benchmark_error_reason VARCHAR(128) NULL,
        execution_error_reason VARCHAR(128) NULL,
        settled_at DATETIME NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_market_signal_followup (candidate_id, horizon_key),
        KEY ix_market_signal_followup_maturity (target_date, horizon_key, status),
        KEY ix_market_signal_followup_cohort (signal_date, market, policy_version),
        KEY ix_market_signal_followup_delivery (delivery_status, notified_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_outcome_reviews (
        signal_year SMALLINT NOT NULL,
        as_of_date DATE NOT NULL,
        review_due_date DATE NOT NULL,
        review_status VARCHAR(32) NOT NULL,
        report_json LONGTEXT NOT NULL,
        reviewed_at DATETIME NOT NULL,
        PRIMARY KEY (signal_year),
        KEY ix_market_signal_outcome_review_status (review_status, as_of_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_horizon_reviews (
        as_of_date DATE NOT NULL,
        horizon_key VARCHAR(8) NOT NULL,
        review_status VARCHAR(40) NOT NULL,
        report_json LONGTEXT NOT NULL,
        reviewed_at DATETIME NOT NULL,
        PRIMARY KEY (horizon_key),
        KEY ix_market_signal_horizon_review_status (review_status, as_of_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_outcome_notification_candidates (
        candidate_id BIGINT UNSIGNED NOT NULL,
        horizon_key VARCHAR(8) NOT NULL,
        notification_id BIGINT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (candidate_id, horizon_key),
        KEY ix_market_signal_outcome_notification_id (notification_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_notifications (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        dedupe_key CHAR(64) NOT NULL,
        run_key VARCHAR(128) NOT NULL,
        notification_channel VARCHAR(64) NOT NULL,
        status VARCHAR(32) NOT NULL,
        platform_receipt_id VARCHAR(64) NULL,
        platform_http_status SMALLINT NULL,
        notification_error_type VARCHAR(64) NULL,
        message_text TEXT NOT NULL,
        notified_at DATETIME NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uq_market_signal_notification (dedupe_key),
        KEY ix_market_signal_notification_status (status, updated_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_notification_candidates (
        notification_id BIGINT UNSIGNED NOT NULL,
        candidate_id BIGINT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (notification_id, candidate_id),
        KEY ix_market_signal_notification_candidate (candidate_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS market_signal_state (
        state_key VARCHAR(128) NOT NULL,
        state_text LONGTEXT NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (state_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)

PORTFOLIO_SCHEMA_STATEMENTS = tuple(
    statement
    for statement in SCHEMA_STATEMENTS
    if "portfolio_alert_rules" in statement
    or "portfolio_notification_record" in statement
    or "portfolio_notification_state" in statement
)


def create_schema(cursor, statements: Iterable[str] = SCHEMA_STATEMENTS) -> None:
    """Create the extension tables idempotently using an existing cursor."""

    lock_name = "dataflow_source_extension_schema"
    cursor.execute("SELECT GET_LOCK(%s, 60)", (lock_name,))
    if cursor.fetchone()[0] != 1:
        raise RuntimeError("could not acquire source extension schema lock")
    try:
        for statement in statements:
            cursor.execute(statement)
    finally:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))


def create_portfolio_schema(cursor) -> None:
    """Create only portfolio notification tables for an independent DAG."""

    lock_name = "dataflow_portfolio_notification_schema"
    cursor.execute("SELECT GET_LOCK(%s, 60)", (lock_name,))
    if cursor.fetchone()[0] != 1:
        raise RuntimeError("could not acquire portfolio notification schema lock")
    try:
        for statement in PORTFOLIO_SCHEMA_STATEMENTS:
            cursor.execute(statement)
    finally:
        cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
