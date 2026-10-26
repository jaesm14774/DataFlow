"""Data notifications for the user's enabled holdings.

This module deliberately reuses the existing ``stock.持股清單`` and Discord
configuration used by the news notifier.  It does not send Airflow failure
messages and it keeps its own notification ledger so news notifications are
not affected.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pandas as pd

from common.config import discord_token_path
from lib.notify import DiscordNotify
from source_extension.registry import SOURCE_DEFINITIONS, _open_connection
from source_extension.schema import create_portfolio_schema


NOTIFICATION_CHANNEL = "新聞即時通"
MAX_ALERTS_PER_RUN = 20
APPROVED_SOURCE_KEYS = tuple(item["source_key"] for item in SOURCE_DEFINITIONS)
SOURCE_LABELS = {
    "twse_company_events": "TWSE 上市公司重大訊息",
    "tpex_company_events": "TPEx 上櫃公司重大訊息",
    "twse_monthly_revenue": "TWSE 上市公司月營收公開資料",
    "tpex_monthly_revenue": "TPEx 上櫃公司月營收公開資料",
    "twse_dividends": "TWSE 上市公司股利公開資料",
    "tpex_dividends": "TPEx 上櫃公司股利公開資料",
}

DEFAULT_ALERT_RULES: tuple[dict[str, Any], ...] = (
    {
        "rule_key": "held_company_material_event",
        "rule_name": "持股公司重大訊息",
        "event_type": "company_event",
        "config": {
            "lookback_days": 7,
            "keywords": [
                "暫停交易",
                "停止買賣",
                "終止上市",
                "下市",
                "減資",
                "合併",
                "分割",
                "公開收購",
                "財務困難",
                "跳票",
                "違約",
                "重大訴訟",
                "現金增資",
                "除權",
                "除息",
            ],
        },
    },
    {
        "rule_key": "held_revenue_warning",
        "rule_name": "持股公司月營收異常",
        "event_type": "monthly_revenue",
        "config": {"lookback_days": 90},
        "enabled": False,
    },
    {
        "rule_key": "held_dividend_announcement",
        "rule_name": "持股公司股利公告",
        "event_type": "dividend",
        "config": {"lookback_days": 45, "future_days": 365},
    },
)


def _now() -> dt.datetime:
    return dt.datetime.now().replace(microsecond=0)


def normalize_stock_code(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    if not text:
        return None
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def sanitize_discord_text(value: Any, max_chars: int = 400) -> str:
    """Keep source text from triggering mentions or oversized messages."""

    text = "" if value is None else str(value)
    text = text.replace("@", "@\u200b")
    return text[:max_chars]


def ensure_alert_rules() -> None:
    conn, cursor, engine = _open_connection()
    try:
        create_portfolio_schema(cursor)
        now = _now()
        sql = """
            INSERT INTO portfolio_alert_rules (
                rule_key, rule_name, event_type, config_text, enabled,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rule_name=VALUES(rule_name), event_type=VALUES(event_type),
                enabled=IF(VALUES(rule_key)='held_revenue_warning', VALUES(enabled), enabled),
                updated_at=VALUES(updated_at)
        """
        for rule in DEFAULT_ALERT_RULES:
            cursor.execute(
                sql,
                (
                    rule["rule_key"],
                    rule["rule_name"],
                    rule["event_type"],
                    json.dumps(rule["config"], ensure_ascii=False, sort_keys=True),
                    1 if rule.get("enabled", True) else 0,
                    now,
                    now,
                ),
            )
        conn.commit()
    finally:
        conn.close()
        engine.dispose()


def _load_holdings(cursor) -> dict[str, str]:
    """Return enabled holdings as code -> display name."""

    cursor.execute(
        "SELECT `持股清單` FROM stock.`持股清單` WHERE notify_status=%s",
        (1,),
    )
    codes = {
        code
        for code in (normalize_stock_code(row[0]) for row in cursor.fetchall())
        if code
    }
    if not codes:
        return {}

    cursor.execute("SELECT `證券代號`, `證券名稱` FROM stock.`全證券代碼相關資料表`")
    names: dict[str, str] = {}
    for code, name in cursor.fetchall():
        normalized = normalize_stock_code(code)
        if normalized:
            names[normalized] = str(name).strip()
    return {code: names.get(code, code) for code in sorted(codes)}


def _load_rules(cursor) -> dict[str, dict[str, Any]]:
    cursor.execute(
        """
        SELECT rule_key, rule_name, event_type, config_text
        FROM portfolio_alert_rules
        WHERE enabled=1
        ORDER BY id
        """
    )
    rules = {}
    for rule_key, rule_name, event_type, config_text in cursor.fetchall():
        rules[rule_key] = {
            "rule_key": rule_key,
            "rule_name": rule_name,
            "event_type": event_type,
            "config": json.loads(config_text or "{}"),
        }
    return rules


def _load_recent_records(
    cursor,
    codes: list[str],
    now: dt.datetime,
    rules: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    placeholders = ", ".join(["%s"] * len(codes))
    source_placeholders = ", ".join(["%s"] * len(APPROVED_SOURCE_KEYS))
    config_by_event = {rule["event_type"]: rule["config"] for rule in rules.values()}
    company_days = int(config_by_event.get("company_event", {}).get("lookback_days", 7))
    revenue_days = int(config_by_event.get("monthly_revenue", {}).get("lookback_days", 90))
    dividend_days = int(config_by_event.get("dividend", {}).get("lookback_days", 45))
    dividend_future_days = int(config_by_event.get("dividend", {}).get("future_days", 365))
    cursor.execute(
        f"""
        SELECT source_records.id, source_records.source_key, source_records.record_hash,
               source_records.entity_id, source_records.event_type, source_records.event_time,
               source_records.effective_date, source_records.title, source_records.metric_name,
               source_records.metric_value, source_records.value_text, source_records.source_url
        FROM source_records
        INNER JOIN source_registry registry
                ON registry.source_key=source_records.source_key
               AND registry.enabled=1
               AND registry.approval_status='approved'
               AND registry.priority_score >= 75
        WHERE source_records.source_key IN ({source_placeholders})
          AND source_records.quality_status='ok'
          AND source_records.entity_type='stock'
          AND LPAD(TRIM(source_records.entity_id), 4, '0') IN ({placeholders})
          AND (
              (
                  source_records.event_type='company_event'
                  AND source_records.event_time >= %s
                  AND source_records.event_time <= %s
              )
              OR (
                  source_records.event_type='monthly_revenue'
                  AND source_records.effective_date >= %s
                  AND source_records.effective_date <= %s
              )
              OR (
                  source_records.event_type='dividend'
                  AND (
                      (source_records.event_time >= %s AND source_records.event_time <= %s)
                      OR (
                          source_records.effective_date >= %s
                          AND source_records.effective_date <= %s
                      )
                  )
              )
          )
        ORDER BY COALESCE(source_records.event_time, CAST(source_records.effective_date AS DATETIME)), source_records.id
        """,
        tuple(APPROVED_SOURCE_KEYS)
        + tuple(codes)
        + (
            now - dt.timedelta(days=company_days),
            now + dt.timedelta(days=1),
            now.date() - dt.timedelta(days=revenue_days),
            now.date() + dt.timedelta(days=31),
            now - dt.timedelta(days=dividend_days),
            now + dt.timedelta(days=1),
            now.date() - dt.timedelta(days=dividend_days),
            now.date() + dt.timedelta(days=dividend_future_days),
        ),
    )
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _previous_revenue(cursor, record: dict[str, Any]) -> float | None:
    if record["effective_date"] is None:
        return None
    cursor.execute(
        """
        SELECT metric_value
        FROM source_records
        WHERE source_key=%s AND event_type='monthly_revenue'
          AND quality_status='ok'
          AND LPAD(TRIM(entity_id), 4, '0')=%s AND effective_date < %s
        ORDER BY effective_date DESC, id DESC
        LIMIT 1
        """,
        (
            record["source_key"],
            normalize_stock_code(record["entity_id"]),
            record["effective_date"],
        ),
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else None


def evaluate_record(
    record: dict[str, Any],
    rule: dict[str, Any],
    previous_revenue_yoy: float | None = None,
) -> str | None:
    """Return a human-readable reason when a record should notify."""

    config = rule["config"]
    if rule["event_type"] == "company_event":
        title = record.get("title") or ""
        text = f"{title} {record.get('value_text') or ''}"
        keywords = config.get("keywords", [])
        routine_report_phrases = ("合併財務報告", "合併財報", "合併自結損益")
        if any(phrase in title for phrase in routine_report_phrases):
            keywords = [keyword for keyword in keywords if keyword != "合併"]
        matched = next((keyword for keyword in keywords if keyword in text), None)
        return f"命中重要關鍵字：{matched}" if matched else None

    if rule["event_type"] == "monthly_revenue":
        # Do not turn a reported growth rate into a warning without a
        # point-in-time expectation and a validated surprise definition.
        return None

    if rule["event_type"] == "dividend":
        return "出現新的股利／除權息資料"
    return None


def _format_alert(alert: dict[str, Any], name: str, reason: str) -> str:
    record = alert["record"]
    code = normalize_stock_code(record["entity_id"]) or str(record["entity_id"])
    display_name = sanitize_discord_text(name, 80)
    event_type = alert["rule"]["event_type"]
    source_key = record["source_key"]
    lines = [
        "🚨 **持股資料警示｜自動偵測（非新聞）**",
        f"**{code} {display_name}｜{sanitize_discord_text(alert['rule']['rule_name'], 80)}**",
    ]

    effective_date = record.get("effective_date")
    event_time = record.get("event_time")
    if event_type == "monthly_revenue":
        if isinstance(effective_date, (dt.datetime, dt.date)):
            period = effective_date.strftime("%Y/%m")
        else:
            period = sanitize_discord_text(effective_date or "未提供", 20)
        lines.append(f"資料月份：{period}")
        if record.get("metric_value") is not None:
            lines.append(f"月營收年增率：{float(record['metric_value']):.2f}%")
        if event_time:
            if isinstance(event_time, (dt.datetime, dt.date)):
                release_date = event_time.strftime("%Y/%m/%d")
            else:
                release_date = sanitize_discord_text(event_time, 30)
            lines.append(f"公告日期：{release_date}")
    else:
        event_date = event_time or effective_date
        if isinstance(event_date, (dt.datetime, dt.date)):
            event_date = event_date.strftime("%Y/%m/%d")
        lines.append(f"資料日期：{sanitize_discord_text(event_date or '未提供', 30)}")

    lines.append(f"警示依據：{sanitize_discord_text(reason, 200)}")
    if record.get("title") and event_type != "monthly_revenue":
        lines.append(f"標題：{sanitize_discord_text(record['title'])}")
    lines.append(f"資料來源：{SOURCE_LABELS.get(source_key, source_key)}")
    lines.append(f"原始資料：{sanitize_discord_text(record.get('source_url') or '未提供', 500)}")
    return "\n".join(lines)


def find_alerts(cursor, holdings: dict[str, str], rules: dict[str, dict[str, Any]], now: dt.datetime) -> list[dict[str, Any]]:
    if not holdings or not rules:
        return []
    records = _load_recent_records(cursor, sorted(holdings), now, rules)
    if not records:
        return []

    hashes = [record["record_hash"] for record in records]
    placeholders = ", ".join(["%s"] * len(hashes))
    cursor.execute(
        f"""
        SELECT rule_key, record_hash
        FROM portfolio_notification_record
        WHERE status IN ('sent', 'baseline')
          AND record_hash IN ({placeholders})
        """,
        tuple(hashes),
    )
    notified = {(row[0], row[1]) for row in cursor.fetchall()}

    alerts: list[dict[str, Any]] = []
    for record in records:
        code = normalize_stock_code(record["entity_id"])
        if not code:
            continue
        matching_rules = [
            rule for rule in rules.values() if rule["event_type"] == record["event_type"]
        ]
        if not matching_rules:
            continue
        previous = _previous_revenue(cursor, record) if record["event_type"] == "monthly_revenue" else None
        for rule in matching_rules:
            if (rule["rule_key"], record["record_hash"]) in notified:
                continue
            reason = evaluate_record(record, rule, previous)
            if reason:
                alerts.append({"record": record, "rule": rule, "reason": reason, "code": code})

    priority = {
        "held_company_material_event": 0,
        "held_dividend_announcement": 1,
        "held_revenue_warning": 2,
    }
    alerts.sort(key=lambda item: (priority.get(item["rule"]["rule_key"], 99), item["code"]))
    return alerts[:MAX_ALERTS_PER_RUN]


def _discord_notifier() -> DiscordNotify:
    token = pd.read_csv(discord_token_path, encoding="utf_8_sig", index_col="name")
    notifier = DiscordNotify()
    notifier.webhook_url = token.loc[NOTIFICATION_CHANNEL, "token"]
    return notifier


def _get_state(cursor, state_key: str) -> str | None:
    cursor.execute(
        "SELECT state_text FROM portfolio_notification_state WHERE state_key=%s",
        (state_key,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def _set_state(cursor, state_key: str, state_text: str, now: dt.datetime) -> None:
    cursor.execute(
        """
        INSERT INTO portfolio_notification_state (state_key, state_text, updated_at)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE state_text=VALUES(state_text), updated_at=VALUES(updated_at)
        """,
        (state_key, state_text, now),
    )


def _write_baseline_record(cursor, alert: dict[str, Any], now: dt.datetime) -> None:
    record = alert["record"]
    cursor.execute(
        """
        INSERT IGNORE INTO portfolio_notification_record (
            rule_key, record_hash, source_key, entity_id, event_type,
            notification_channel, status, message_text, notified_at, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, 'baseline', %s, NULL, %s)
        """,
        (
            alert["rule"]["rule_key"],
            record["record_hash"],
            record["source_key"],
            normalize_stock_code(record["entity_id"]) or str(record["entity_id"]),
            record["event_type"],
            NOTIFICATION_CHANNEL,
            "initial baseline; no message sent",
            now,
        ),
    )


def _claim_notification(cursor, alert: dict[str, Any], message: str, now: dt.datetime) -> bool:
    record = alert["record"]
    rule_key = alert["rule"]["rule_key"]
    record_hash = record["record_hash"]
    cursor.execute(
        """
        SELECT status, created_at
        FROM portfolio_notification_record
        WHERE rule_key=%s AND record_hash=%s
        FOR UPDATE
        """,
        (rule_key, record_hash),
    )
    existing = cursor.fetchone()
    if existing:
        status, created_at = existing
        if status == "sent":
            return False
        if status == "pending" and created_at and now - created_at < dt.timedelta(hours=1):
            return False
        cursor.execute(
            """
            UPDATE portfolio_notification_record
            SET status='pending', message_text=%s, notified_at=NULL, created_at=%s
            WHERE rule_key=%s AND record_hash=%s
            """,
            (message, now, rule_key, record_hash),
        )
        return True

    cursor.execute(
        """
        INSERT INTO portfolio_notification_record (
            rule_key, record_hash, source_key, entity_id, event_type,
            notification_channel, status, message_text, notified_at, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, 'pending', %s, NULL, %s)
        """,
        (
            rule_key,
            record_hash,
            record["source_key"],
            normalize_stock_code(record["entity_id"]) or str(record["entity_id"]),
            record["event_type"],
            NOTIFICATION_CHANNEL,
            message,
            now,
        ),
    )
    return True


def _mark_notification(cursor, alert: dict[str, Any], message: str, status: str, now: dt.datetime) -> None:
    cursor.execute(
        """
        UPDATE portfolio_notification_record
        SET status=%s, message_text=%s, notified_at=%s
        WHERE rule_key=%s AND record_hash=%s
        """,
        (
            status,
            message,
            now if status == "sent" else None,
            alert["rule"]["rule_key"],
            alert["record"]["record_hash"],
        ),
    )


def send_portfolio_notifications() -> dict[str, int]:
    ensure_alert_rules()
    conn, cursor, engine = _open_connection()
    sent = 0
    failed = 0
    try:
        holdings = _load_holdings(cursor)
        if not holdings:
            return {"holdings": 0, "candidates": 0, "baseline": 0, "sent": 0, "failed": 0}
        rules = _load_rules(cursor)
        now = _now()
        alerts = find_alerts(cursor, holdings, rules, now)
        if _get_state(cursor, "initial_baseline_initialized") is None:
            for alert in alerts:
                _write_baseline_record(cursor, alert, now)
            _set_state(cursor, "initial_baseline_initialized", "1", now)
            conn.commit()
            return {
                "holdings": len(holdings),
                "candidates": len(alerts),
                "baseline": len(alerts),
                "sent": 0,
                "failed": 0,
            }

        notifier = _discord_notifier() if alerts else None
        for alert in alerts:
            message = _format_alert(alert, holdings[alert["code"]], alert["reason"])
            claimed = _claim_notification(cursor, alert, message, _now())
            conn.commit()
            if not claimed:
                continue
            try:
                if notifier.notify(msg=message) is not True:
                    raise RuntimeError("Discord returned a non-success status")
                _mark_notification(cursor, alert, message, "sent", _now())
                conn.commit()
                sent += 1
            except Exception:
                conn.rollback()
                try:
                    _mark_notification(cursor, alert, message, "failed", _now())
                    conn.commit()
                except Exception:
                    conn.rollback()
                failed += 1
        return {
            "holdings": len(holdings),
            "candidates": len(alerts),
            "baseline": 0,
            "sent": sent,
            "failed": failed,
        }
    finally:
        conn.close()
        engine.dispose()
