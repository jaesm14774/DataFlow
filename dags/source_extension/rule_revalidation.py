"""Weekly, incremental, long-horizon revalidation of every signal rule.

The date-level evidence table grows by appending dates as their forward returns mature
(one SQL read of the recent window), so the weekly run never re-reads the full history
once the table is seeded. Statuses come from ``validated_rule_status``; the JSON registry
only supplies definitions-side configuration and seed statuses.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

import pandas as pd

from . import rule_detection, rule_lifecycle, signal_rules, validated_signals
from .schema import SCHEMA_STATEMENTS, create_schema

LOGGER = logging.getLogger(__name__)

BACKFILL_START = dt.date(2015, 6, 1)
BACKFILL_CHUNK_DAYS = 400
INPUT_LOOKBACK_DAYS = 260
FORWARD_PADDING_DAYS = 60
RECOMPUTE_TRAILING_DAYS = 45
RULE_TABLE_STATEMENTS = tuple(s for s in SCHEMA_STATEMENTS if "validated_rule_" in s)


def ensure_tables(cursor) -> None:
    create_schema(cursor, statements=RULE_TABLE_STATEMENTS)


# --- date-level evidence --------------------------------------------------------------

def load_daily(cursor) -> pd.DataFrame:
    cursor.execute(
        "SELECT rule_id, horizon, signal_date, n_names, mean_excess_pct, n_win, "
        "n_win_after_cost, base_n, base_win_after_cost FROM validated_rule_daily"
    )
    frame = pd.DataFrame(cursor.fetchall(), columns=rule_lifecycle.DAILY_COLUMNS)
    if frame.empty:
        return frame
    frame["signal_date"] = pd.to_datetime(frame["signal_date"])
    for column in ("n_names", "n_win", "n_win_after_cost", "base_n", "base_win_after_cost", "horizon"):
        frame[column] = pd.to_numeric(frame[column]).astype(int)
    frame["mean_excess_pct"] = pd.to_numeric(frame["mean_excess_pct"], errors="coerce")
    return frame


def store_daily(cursor, rows: pd.DataFrame, chunk: int = 2000) -> int:
    if rows.empty:
        return 0
    sql = (
        "INSERT INTO validated_rule_daily (rule_id, horizon, signal_date, n_names, mean_excess_pct, "
        "n_win, n_win_after_cost, base_n, base_win_after_cost) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE n_names=VALUES(n_names), mean_excess_pct=VALUES(mean_excess_pct), "
        "n_win=VALUES(n_win), n_win_after_cost=VALUES(n_win_after_cost), base_n=VALUES(base_n), "
        "base_win_after_cost=VALUES(base_win_after_cost)"
    )
    params = []
    for row in rows.itertuples(index=False):
        mean = None if pd.isna(row.mean_excess_pct) else float(row.mean_excess_pct)
        params.append((
            row.rule_id, int(row.horizon), pd.Timestamp(row.signal_date).date(), int(row.n_names), mean,
            int(row.n_win), int(row.n_win_after_cost), int(row.base_n), int(row.base_win_after_cost),
        ))
    for start in range(0, len(params), chunk):
        cursor.executemany(sql, params[start:start + chunk])
    return len(params)


def latest_stored_date(cursor) -> dt.date | None:
    """Newest date stored for the longest horizon: every shorter horizon is at least that fresh."""

    cursor.execute("SELECT MAX(signal_date) FROM validated_rule_daily WHERE horizon=%s", (max(signal_rules.HORIZONS),))
    row = cursor.fetchone()
    value = row[0] if row else None
    if value is None:
        return None
    return value.date() if isinstance(value, dt.datetime) else value


def daily_rows_for_range(stock_cursor, start: dt.date, end: dt.date, today: dt.date) -> pd.DataFrame:
    """Date-level rows for signal dates in [start, end], from a window with enough history and future."""

    data_end = min(end + dt.timedelta(days=FORWARD_PADDING_DAYS), today)
    span = (data_end - (start - dt.timedelta(days=INPUT_LOOKBACK_DAYS))).days
    inputs = rule_detection.fetch_inputs(stock_cursor, data_end, calendar_days=span)
    if inputs["price"].empty:
        return pd.DataFrame(columns=rule_lifecycle.DAILY_COLUMNS)
    panel = signal_rules.compute_panel(inputs["price"], inputs["inst"], inputs["margin"], inputs["tdcc"])
    daily = rule_lifecycle.daily_excess_table(panel)
    mask = (daily["signal_date"] >= pd.Timestamp(start)) & (daily["signal_date"] <= pd.Timestamp(end))
    return daily[mask]


def refresh_daily(ledger_cursor, stock_cursor, today: dt.date) -> dict[str, int]:
    """Append every date whose forward returns matured since the last run (seeds on first use)."""

    last = latest_stored_date(ledger_cursor)
    stored = 0
    if last is None:
        start = BACKFILL_START
        while start <= today:
            end = min(start + dt.timedelta(days=BACKFILL_CHUNK_DAYS - 1), today)
            stored += store_daily(ledger_cursor, daily_rows_for_range(stock_cursor, start, end, today))
            start = end + dt.timedelta(days=1)
        return {"stored": stored, "seeded": 1}
    # recompute a trailing window: rows built while a feed was only partly landed get corrected
    stored = store_daily(
        ledger_cursor,
        daily_rows_for_range(stock_cursor, last - dt.timedelta(days=RECOMPUTE_TRAILING_DAYS), today, today),
    )
    return {"stored": stored, "seeded": 0}


# --- status ----------------------------------------------------------------------------

def _evidence_json(evidence: dict[str, Any]) -> str:
    return json.dumps(evidence, ensure_ascii=False, default=str)


def read_statuses(cursor) -> dict[str, dict[str, Any]]:
    cursor.execute(
        "SELECT rule_id, status, horizon_sessions, status_since, evidence_json, last_evaluated_on "
        "FROM validated_rule_status"
    )
    rows = {}
    for rule_id, status, horizon, since, evidence, evaluated in cursor.fetchall():
        rows[str(rule_id)] = {
            "status": status, "horizon_sessions": int(horizon), "status_since": since,
            "evidence": json.loads(evidence) if evidence else {}, "last_evaluated_on": evaluated,
        }
    return rows


def _write_status(cursor, rule_id: str, version: str, status: str, horizon: int, since: dt.date,
                  evidence: dict[str, Any], today: dt.date, now: dt.datetime) -> None:
    cursor.execute(
        """
        INSERT INTO validated_rule_status
            (rule_id, ruleset_version, status, horizon_sessions, status_since,
             last_evaluated_on, evidence_json, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE ruleset_version=VALUES(ruleset_version), status=VALUES(status),
            horizon_sessions=VALUES(horizon_sessions), status_since=VALUES(status_since),
            last_evaluated_on=VALUES(last_evaluated_on), evidence_json=VALUES(evidence_json),
            updated_at=VALUES(updated_at)
        """,
        (rule_id, version, status, horizon, since, today, _evidence_json(evidence), now),
    )


def _write_history(cursor, rule_id: str, from_status: str | None, to_status: str, reason: str,
                   evidence: dict[str, Any], today: dt.date, now: dt.datetime) -> None:
    cursor.execute(
        "INSERT IGNORE INTO validated_rule_history "
        "(rule_id, from_status, to_status, reason, evidence_json, decided_on, created_at) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (rule_id, from_status, to_status, reason[:255], _evidence_json(evidence), today, now),
    )


def _as_date(value: Any) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value)[:10])


def evaluate_statuses(
    ledger_cursor, daily: pd.DataFrame, registry: dict[str, Any], *, today: dt.date, now: dt.datetime,
) -> list[dict[str, Any]]:
    """Re-measure every rule on the long window and apply the hysteresis policy."""

    summary = rule_lifecycle.summarize(daily) if not daily.empty else pd.DataFrame()
    verdict_table = rule_lifecycle.verdicts(summary) if not summary.empty else pd.DataFrame(
        columns=["signal", "horizon"]
    )
    version = registry.get("ruleset") or signal_rules.RULESET_VERSION
    configured = {item["id"]: item for item in registry["rules"]}
    stored = read_statuses(ledger_cursor)
    changes: list[dict[str, Any]] = []
    for rule in signal_rules.RULES:
        config = configured.get(rule.id, {})
        current = stored.get(rule.id)
        if current is None:
            evidence = rule_lifecycle.long_term_evidence(daily, verdict_table, rule.id, config.get("horizon_sessions"))
            decision = rule_lifecycle.seed_status(evidence)
            seed = config.get("seed_status")
            if seed in rule_lifecycle.STATUSES and not (
                seed == rule_lifecycle.ACTIVE and not evidence.get("gate_pass")
            ):  # a seed can never bypass the gate
                decision = rule_lifecycle.Decision(seed, True, f"初次登錄（登錄檔指定 {seed}）")
            if rule.is_control and decision.status == rule_lifecycle.ACTIVE:
                decision = rule_lifecycle.Decision(rule_lifecycle.CANDIDATE, True, "對照組：不推播，只做比較")
            horizon = int(evidence.get("horizon") or config.get("horizon_sessions") or 10)
            _write_status(ledger_cursor, rule.id, version, decision.status, horizon, today, evidence, today, now)
            _write_history(ledger_cursor, rule.id, None, decision.status, decision.reason, evidence, today, now)
            changes.append({"rule_id": rule.id, "from": None, "to": decision.status, "reason": decision.reason})
            continue
        horizon = int(current["horizon_sessions"])
        pinned = horizon if current["status"] == rule_lifecycle.ACTIVE else None
        evidence = rule_lifecycle.long_term_evidence(daily, verdict_table, rule.id, pinned)
        since = _as_date(current["status_since"]) or today
        if evidence.get("insufficient_data"):
            new_dates = 0
        else:
            h = int(evidence["horizon"])
            group = daily[(daily["rule_id"] == rule.id) & (daily["horizon"] == h)]
            group = group[(group["n_names"] >= rule_lifecycle.MIN_NAMES_PER_DATE) & group["mean_excess_pct"].notna()]
            new_dates = int((group["signal_date"] > pd.Timestamp(since)).sum())
        decision = rule_lifecycle.decide(
            current["status"], evidence,
            days_in_status=rule_lifecycle.days_between(since, today), new_dates_since_change=new_dates,
        )
        if rule.is_control and decision.status == rule_lifecycle.ACTIVE:
            decision = rule_lifecycle.Decision(current["status"], False, "對照組：不推播")
        if decision.changed:
            new_horizon = int(evidence.get("horizon") or horizon)
            _write_status(ledger_cursor, rule.id, version, decision.status, new_horizon, today, evidence, today, now)
            _write_history(ledger_cursor, rule.id, current["status"], decision.status, decision.reason, evidence, today, now)
            changes.append({"rule_id": rule.id, "from": current["status"], "to": decision.status,
                            "reason": decision.reason})
        elif not evidence.get("insufficient_data"):
            if current["status"] != rule_lifecycle.ACTIVE:
                horizon = int(evidence.get("horizon") or horizon)  # a pushed rule keeps its horizon
            _write_status(ledger_cursor, rule.id, version, current["status"], horizon, since, evidence, today, now)
    return changes


def revalidate(ledger_cursor, stock_cursor, registry: dict[str, Any], *, today: dt.date, now: dt.datetime) -> dict[str, Any]:
    ensure_tables(ledger_cursor)
    refreshed = refresh_daily(ledger_cursor, stock_cursor, today)
    daily = load_daily(ledger_cursor)
    changes = evaluate_statuses(ledger_cursor, daily, registry, today=today, now=now)
    return {**refreshed, "dates": int(daily["signal_date"].nunique()) if not daily.empty else 0, "changes": changes}


# --- read side for the digest ---------------------------------------------------------

def load_rule_states(ledger_cursor, registry: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Every rule with its current status: the database wins, the JSON registry is the seed/fallback."""

    registry = registry or validated_signals.load_registry()
    configured = {item["id"]: item for item in registry["rules"]}
    stored: dict[str, dict[str, Any]] = {}
    read_failed = False
    if ledger_cursor is not None:
        try:
            stored = read_statuses(ledger_cursor)
        except Exception:  # table not created yet or ledger unreachable: fall back to the registry
            read_failed = True
            LOGGER.warning("validated_rule_status unavailable; using registry seed statuses", exc_info=True)
    states = []
    for rule in signal_rules.RULES:
        config = configured.get(rule.id, {})
        row = stored.get(rule.id)
        status = (row or {}).get("status") or config.get("seed_status") or rule_lifecycle.CANDIDATE
        if rule.is_control and status == rule_lifecycle.ACTIVE:
            status = rule_lifecycle.CANDIDATE
        horizon = int((row or {}).get("horizon_sessions") or config.get("horizon_sessions") or 10)
        states.append({
            "id": rule.id,
            "label": rule.label,
            "meaning": rule.meaning,
            "metric_label": rule.metric_label,
            "metric_format": rule.metric_format,
            "sources": rule.sources,
            "is_control": rule.is_control,
            "status": status,
            "status_since": (row or {}).get("status_since"),
            "last_evaluated_on": _as_date((row or {}).get("last_evaluated_on")),
            "from_database": row is not None,
            "status_source": "database" if row is not None else ("fallback_error" if read_failed else "seed"),
            "horizon_sessions": horizon,
            "evidence": (row or {}).get("evidence") or config.get("evidence") or {},
            "params": {
                "max_names_shown": int(config.get("max_names_shown", 5)),
                "min_turnover_ntd": signal_rules.MIN_TURNOVER_NTD,
            },
        })
    return states


def recent_transitions(ledger_cursor, as_of_date: dt.date) -> list[dict[str, Any]]:
    """Status changes decided since the previous digest, so each one is announced once.

    The weekly job runs on Saturday: Monday's digest (previous digest = Friday) announces it
    and Tuesday's does not. Initial registrations are not announced.
    """

    today = as_of_date
    previous_digest = as_of_date - dt.timedelta(days=3 if as_of_date.weekday() == 0 else 1)

    if ledger_cursor is None:
        return []
    try:
        ledger_cursor.execute(
            "SELECT rule_id, from_status, to_status, reason, decided_on FROM validated_rule_history "
            "WHERE decided_on > %s AND decided_on <= %s AND from_status IS NOT NULL "
            "ORDER BY decided_on, id",
            (previous_digest, today),
        )
        return [
            {"rule_id": r[0], "from": r[1], "to": r[2], "reason": r[3], "decided_on": _as_date(r[4])}
            for r in ledger_cursor.fetchall()
        ]
    except Exception:
        LOGGER.warning("validated_rule_history unavailable", exc_info=True)
        return []


def run_weekly_revalidation(today: dt.date | None = None) -> dict[str, Any]:
    """Airflow entry point."""

    from . import market_signals
    from .registry import _open_connection

    today = today or dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
    now = dt.datetime.now()
    registry = validated_signals.load_registry()
    ledger_conn = ledger_cursor = ledger_engine = None
    stock_conn = stock_cursor = stock_engine = None
    try:
        ledger_conn, ledger_cursor, ledger_engine = _open_connection()
        stock_conn, stock_cursor, stock_engine = market_signals._open_stock_connection()
        result = revalidate(ledger_cursor, stock_cursor, registry, today=today, now=now)
        ledger_conn.commit()
        LOGGER.info("signal rule revalidation: %s", result)
        return result
    except Exception:
        if ledger_conn:
            ledger_conn.rollback()
        raise
    finally:
        for conn in (stock_conn, ledger_conn):
            if conn:
                conn.close()
        for engine in (stock_engine, ledger_engine):
            if engine:
                engine.dispose()
