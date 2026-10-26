"""Full-stock-universe signal snapshots and Discord delivery."""

from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import math
import re
import statistics
from bisect import bisect_left, bisect_right
from typing import Any
from urllib.parse import urlsplit

import pandas as pd
import pendulum

from airflow.operators.python import get_current_context
from common.config import discord_token_path, sql_configure_path
from lib.get_sql import get_sql
from lib.notify import DiscordNotify
from lib.taiwan_market_calendar import is_taiwan_stock_market_closed
from source_extension.adapters import parse_number
from source_extension.institutional_flow import format_flow_readiness
from source_extension.equity_order_flow import load_equity_order_flow
from source_extension.backtest_diagnostics import summarize_daily_backtest
from source_extension.outcome_lifecycle import (
    FOLLOWUP_HORIZONS,
    direction_sign,
    directional_excursion_pct,
    first_session_on_or_after_sorted,
    followup_target_date,
    percent_return,
    summarize_followup_rows,
)
from source_extension.signal_policy import (
    POLICY_VERSION, evidence_balance, is_observed_market_session, profitability_vote,
    merge_stored_quote_history, quote_window, revenue_period_current,
    strip_negated_terms, trading_risk_action, trading_sessions,
)
from source_extension.registry import _open_connection
from source_extension.schema import create_schema


NOTIFICATION_CHANNEL = "新聞即時通"
MAX_INTRADAY_CANDIDATES_PER_RUN = 60
DAILY_DIGEST_LIMIT = 10
NEWS_CONTEXT_LOOKBACK_HOURS = 48
NEWS_CONTEXT_INGEST_SCAN_LIMIT = 10_000
NEWS_CONTEXT_SAMPLE_LIMIT = 6_000
NEWS_CONTEXT_MATCH_LIMIT = 2
NEWS_CONTEXT_BODY_LEAD_CHARS = 320
NEWS_CONTEXT_MATCH_EXCERPT_CHARS = 50
NEWS_DATE_ONLY_SOURCES = frozenset({"ithome"})
MATERIAL_CB_CASH_TO_PARENT_EQUITY_RATIO = 0.20
TDCC_SHAREHOLDING_SOURCE_URL = "https://openapi.tdcc.com.tw/v1/opendata/1-5"
TDCC_SHAREHOLDING_MAX_SNAPSHOT_AGE_DAYS = 14
TDCC_SHAREHOLDING_MAX_SNAPSHOT_GAP_DAYS = 14
TDCC_SHAREHOLDING_MIN_MARKET_COVERAGE = 0.95
TDCC_SHAREHOLDING_CONTEXT_CHANGE_PP = 2.0
DISCORD_EMBED_CHAR_LIMIT = 5_500
DISCORD_EMBED_COUNT_LIMIT = 10
DISCORD_REQUEST_BYTE_LIMIT = 10_000
MIN_DAILY_MARKET_COVERAGE = 0.70
DAILY_FAMILY_SOURCE_GROUPS = {
    "price": ("market_tape", "行情（價格／量能）"),
    "volume": ("market_tape", "行情（價格／量能）"),
    "institutions": ("equity_order_flow", "交易籌碼（法人／分點）"),
    "broker_branches": ("equity_order_flow", "交易籌碼（法人／分點）"),
    "revenue": ("monthly_revenue", "月營收"),
    "profitability": ("operating_margin", "季營業利益率"),
    "insiders": ("insider_holdings", "內部人持股"),
}
TAIFEX_POSITION_SOURCE_KEY = "taifex_index_futures_positions_daily"
TAIFEX_POSITION_EVENT_TYPE = "market_institutional_positioning_daily"
TAIFEX_OPTION_POSITION_SOURCE_KEY = "taifex_index_options_positions_daily"
TAIFEX_LARGE_TRADER_SOURCE_KEY = "taifex_futures_large_trader_positions_daily"
TAIFEX_LARGE_TRADER_EVENT_TYPE = "market_futures_large_trader_oi_daily"
TAIFEX_POSITION_CONTRACTS = {
    "臺股期貨": "上市指數期貨",
    "櫃買指數期貨": "上櫃指數期貨",
}
TAIFEX_POSITION_INSTITUTIONS = ("外資及陸資", "投信", "自營商")
TAIFEX_OPTION_POSITION_CONTRACT = "臺指選擇權"
TAIFEX_OPTION_POSITION_CALL_PUTS = ("CALL", "PUT")
INTRADAY_CURSOR_KEY = "intraday_source_record_id_cursor"
SOURCE_COLLECTION_ALERT_STATE_KEY = "intraday_source_collection_alert_fingerprint"
INTRADAY_LOOKBACK_DAYS = 7
INTRADAY_CURSOR_OVERLAP_IDS = 10_000
INTRADAY_REVISION_LOOKBACK_DAYS = 1
OFFICIAL_MARKET_QUOTE_SOURCE_BY_MARKET = {
    "上市": "twse_equity_quotes_daily",
    "上櫃": "tpex_equity_quotes_daily",
}


def _now_taipei() -> dt.datetime:
    now = pendulum.now("Asia/Taipei").replace(microsecond=0)
    return dt.datetime(now.year, now.month, now.day, now.hour, now.minute, now.second)


def _source_updated_at_cutoff_utc(started_at: dt.datetime) -> dt.datetime:
    """Convert Taipei-local scan time to the naive UTC clock used by registry timestamps."""
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=pendulum.timezone("Asia/Taipei"))
    return started_at.astimezone(dt.timezone.utc).replace(tzinfo=None)


MONTHLY_REVENUE_ASOF_CTE = """
WITH monthly_revenue_snapshot AS (
    SELECT v.id, v.source_key, v.record_hash, v.entity_id, v.event_type,
           v.entity_type, v.quality_status, v.effective_date, v.event_time,
           v.metric_value, v.payload, v.source_url, v.captured_at AS updated_at
    FROM source_record_versions v
    WHERE v.source_key IN ('twse_monthly_revenue', 'tpex_monthly_revenue')
      AND v.captured_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions newer
          WHERE newer.record_hash=v.record_hash
            AND newer.captured_at<=%s
            AND (newer.captured_at>v.captured_at
                 OR (newer.captured_at=v.captured_at AND newer.id>v.id))
      )
    UNION ALL
    SELECT r.id, r.source_key, r.record_hash, r.entity_id, r.event_type,
           r.entity_type, r.quality_status, r.effective_date, r.event_time,
           r.metric_value, r.payload, r.source_url, r.updated_at
    FROM source_records r
    WHERE r.source_key IN ('twse_monthly_revenue', 'tpex_monthly_revenue')
      AND r.updated_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions v
          WHERE v.record_hash=r.record_hash AND v.captured_at<=%s
      )
)
"""


QUARTERLY_FINANCIAL_ASOF_CTE = """
WITH quarterly_financial_snapshot AS (
    SELECT v.id, v.source_key, v.record_hash, v.entity_id, v.event_type,
           v.entity_type, v.quality_status, v.effective_date, v.event_time,
           v.metric_value, v.payload, v.source_url, v.captured_at AS updated_at
    FROM source_record_versions v
    WHERE v.event_type IN (
        'quarterly_profitability', 'quarterly_income_statement',
        'quarterly_balance_sheet'
    ) AND v.entity_type='stock' AND v.captured_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions newer
          WHERE newer.record_hash=v.record_hash
            AND newer.captured_at<=%s
            AND (newer.captured_at>v.captured_at
                 OR (newer.captured_at=v.captured_at AND newer.id>v.id))
      )
    UNION ALL
    SELECT r.id, r.source_key, r.record_hash, r.entity_id, r.event_type,
           r.entity_type, r.quality_status, r.effective_date, r.event_time,
           r.metric_value, r.payload, r.source_url, r.updated_at
    FROM source_records r
    WHERE r.event_type IN (
        'quarterly_profitability', 'quarterly_income_statement',
        'quarterly_balance_sheet'
    ) AND r.entity_type='stock' AND r.updated_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions v
          WHERE v.record_hash=r.record_hash AND v.captured_at<=%s
      )
)
"""


INSIDER_HOLDINGS_ASOF_CTE = """
WITH insider_holdings_snapshot AS (
    SELECT v.id, v.source_key, v.record_hash, v.entity_id, v.event_type,
           v.entity_type, v.quality_status, v.effective_date, v.event_time,
           v.metric_value, v.payload, v.source_url, v.captured_at AS updated_at
    FROM source_record_versions v
    WHERE v.source_key IN (
        'twse_director_holdings_monthly', 'tpex_director_holdings_monthly'
    ) AND v.event_type='director_holding_snapshot'
      AND v.entity_type='stock' AND v.captured_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions newer
          WHERE newer.record_hash=v.record_hash
            AND newer.captured_at<=%s
            AND (newer.captured_at>v.captured_at
                 OR (newer.captured_at=v.captured_at AND newer.id>v.id))
      )
    UNION ALL
    SELECT r.id, r.source_key, r.record_hash, r.entity_id, r.event_type,
           r.entity_type, r.quality_status, r.effective_date, r.event_time,
           r.metric_value, r.payload, r.source_url, r.updated_at
    FROM source_records r
    WHERE r.source_key IN (
        'twse_director_holdings_monthly', 'tpex_director_holdings_monthly'
    ) AND r.event_type='director_holding_snapshot'
      AND r.entity_type='stock' AND r.updated_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions v
          WHERE v.record_hash=r.record_hash AND v.captured_at<=%s
      )
)
"""


DIVIDEND_ASOF_CTE = """
WITH dividend_snapshot AS (
    SELECT v.id, v.source_key, v.record_hash, v.entity_id, v.event_type,
           v.entity_type, v.quality_status, v.effective_date, v.event_time,
           v.metric_value, v.payload, v.source_url, v.captured_at AS updated_at
    FROM source_record_versions v
    WHERE v.source_key IN ('twse_dividends', 'tpex_dividends')
      AND v.event_type='dividend' AND v.entity_type='stock'
      AND v.captured_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions newer
          WHERE newer.record_hash=v.record_hash
            AND newer.captured_at<=%s
            AND (newer.captured_at>v.captured_at
                 OR (newer.captured_at=v.captured_at AND newer.id>v.id))
      )
    UNION ALL
    SELECT r.id, r.source_key, r.record_hash, r.entity_id, r.event_type,
           r.entity_type, r.quality_status, r.effective_date, r.event_time,
           r.metric_value, r.payload, r.source_url, r.updated_at
    FROM source_records r
    WHERE r.source_key IN ('twse_dividends', 'tpex_dividends')
      AND r.event_type='dividend' AND r.entity_type='stock'
      AND r.updated_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions v
          WHERE v.record_hash=r.record_hash AND v.captured_at<=%s
      )
)
"""


MARKET_CONTEXT_ASOF_CTE = """
WITH market_context_snapshot AS (
    SELECT v.id, v.source_key, v.record_hash, v.entity_id, v.event_type,
           v.entity_type, v.quality_status, v.effective_date, v.event_time,
           v.metric_value, v.value_text, v.title, v.payload, v.source_url,
           v.captured_at AS updated_at
    FROM source_record_versions v
    WHERE v.event_type IN (
        'corporate_action_preannouncement', 'valuation_metrics_daily',
        'market_index_daily', 'margin_balance_snapshot',
        'trading_attention_notice', 'trading_disposition', 'trading_status_snapshot',
        'available_short_shares_snapshot', 'securities_borrowing_activity',
        'market_quote_daily', 'market_daytrade_aggregate_daily',
        'market_total_return_index_daily', 'market_institutional_positioning_daily',
        'market_futures_large_trader_oi_daily', 'foreign_ownership_daily',
        'statutory_director_shareholding_shortfall'
    ) AND v.captured_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions newer
          WHERE newer.record_hash=v.record_hash
            AND newer.captured_at<=%s
            AND (newer.captured_at>v.captured_at
                 OR (newer.captured_at=v.captured_at AND newer.id>v.id))
      )
    UNION ALL
    SELECT r.id, r.source_key, r.record_hash, r.entity_id, r.event_type,
           r.entity_type, r.quality_status, r.effective_date, r.event_time,
           r.metric_value, r.value_text, r.title, r.payload, r.source_url,
           r.updated_at
    FROM source_records r
    WHERE r.event_type IN (
        'corporate_action_preannouncement', 'valuation_metrics_daily',
        'market_index_daily', 'margin_balance_snapshot',
        'trading_attention_notice', 'trading_disposition', 'trading_status_snapshot',
        'available_short_shares_snapshot', 'securities_borrowing_activity',
        'market_quote_daily', 'market_daytrade_aggregate_daily',
        'market_total_return_index_daily', 'market_institutional_positioning_daily',
        'market_futures_large_trader_oi_daily', 'foreign_ownership_daily',
        'statutory_director_shareholding_shortfall'
    ) AND r.updated_at<=%s
      AND NOT EXISTS (
          SELECT 1
          FROM source_record_versions v
          WHERE v.record_hash=r.record_hash AND v.captured_at<=%s
      )
)
"""


def _today_taipei() -> dt.date:
    return pendulum.now("Asia/Taipei").date()


def _run_key(run_id: str) -> str:
    return hashlib.sha256(run_id.encode("utf-8")).hexdigest()


def _normalize_code(value: Any) -> str:
    code = str(value or "").strip()
    if code.endswith(".0") and code[:-2].isdigit():
        code = code[:-2]
    return code.zfill(4) if code.isdigit() and len(code) < 4 else code


def _source_security_name(payload: Any) -> str:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError):
            return ""
    if not isinstance(payload, dict):
        return ""
    for key in ("公司名稱", "證券名稱", "CompanyName", "SecuritiesCompanyName", "Name"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _revision_title_key(value: Any) -> tuple[bool, str]:
    title = " ".join(str(value or "").split())
    match = re.match(r"^更正\s*[-－：:]\s*", title)
    return bool(match), title[match.end():] if match else title


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _quarterly_operating_margin_series(
    rows: list[tuple[dt.date, Any, float | None, Any, int]],
) -> dict[tuple[int, int], float]:
    """Derive standalone-quarter operating margins from cumulative source rows."""

    cumulative_by_quarter: dict[
        tuple[int, int], tuple[Any, float, float, int]
    ] = {}
    for period, published_at, margin, payload, row_id in rows:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError):
                continue
        if not isinstance(payload, dict):
            continue
        try:
            year = int(payload.get("__reporting_year"))
            quarter = int(payload.get("__reporting_quarter"))
        except (TypeError, ValueError):
            continue
        if period.year != year or (period.month - 1) // 3 + 1 != quarter:
            continue
        revenue = _number(payload.get("__revenue_millions"))
        if not (
            1 <= quarter <= 4
            and revenue is not None
            and revenue > 0
            and margin is not None
        ):
            continue
        key = (year, quarter)
        current = (published_at, revenue, margin, row_id)
        previous = cumulative_by_quarter.get(key)
        if previous is None or (published_at, row_id) >= (previous[0], previous[3]):
            cumulative_by_quarter[key] = current

    quarterly: dict[tuple[int, int], float] = {}
    for (year, quarter), (_published_at, revenue, margin, _row_id) in sorted(
        cumulative_by_quarter.items()
    ):
        if quarter == 1:
            quarterly[(year, quarter)] = margin
            continue
        previous = cumulative_by_quarter.get((year, quarter - 1))
        if previous is None:
            continue
        previous_revenue, previous_margin = previous[1], previous[2]
        quarter_revenue = revenue - previous_revenue
        if quarter_revenue <= 0:
            continue
        quarter_operating_profit = (
            revenue * margin - previous_revenue * previous_margin
        ) / 100
        quarterly[(year, quarter)] = quarter_operating_profit / quarter_revenue * 100
    return quarterly


def _date_only(value: Any) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _trailing_three_month_revenue_context(
    rows: list[tuple[Any, ...]],
) -> dict[str, Any]:
    """Summarize three consecutive reported months without changing signal direction."""

    latest_by_period: dict[dt.date, tuple[Any, ...]] = {}
    for row in rows:
        if row[0] and row[1] is not None:
            latest_by_period[row[0]] = row
    if not latest_by_period:
        return {}

    latest_period = max(latest_by_period)
    latest_index = latest_period.year * 12 + latest_period.month - 1
    expected_periods: list[dt.date] = []
    for month_index in range(latest_index - 2, latest_index + 1):
        year, month_zero = divmod(month_index, 12)
        expected_periods.append(dt.date(year, month_zero + 1, 1))
    selected = [latest_by_period.get(period) for period in expected_periods]
    if any(row is None for row in selected):
        return {}

    current_amounts = [_number(row[3]) for row in selected if row is not None]
    prior_year_amounts = [_number(row[4]) for row in selected if row is not None]
    if (
        len(current_amounts) != 3
        or len(prior_year_amounts) != 3
        or any(value is None for value in current_amounts + prior_year_amounts)
    ):
        return {}
    current_total = sum(value for value in current_amounts if value is not None)
    prior_year_total = sum(value for value in prior_year_amounts if value is not None)
    if prior_year_total <= 0:
        return {}

    return {
        "revenue_3m_start_period": expected_periods[0].strftime("%Y/%m"),
        "revenue_3m_end_period": expected_periods[-1].strftime("%Y/%m"),
        "revenue_3m_yoy_pct": round((current_total / prior_year_total - 1) * 100, 2),
    }


def _month_over_month_revenue_context(
    rows: list[tuple[Any, ...]],
) -> dict[str, Any]:
    """Summarize the latest reported month versus the immediately prior month."""

    if not rows or not rows[-1][0] or rows[-1][3] is None:
        return {}
    latest_period = rows[-1][0]
    latest_by_period: dict[dt.date, tuple[Any, ...]] = {}
    for row in rows:
        if row[0] and row[3] is not None:
            latest_by_period[row[0]] = row
    if latest_period not in latest_by_period:
        return {}

    month_index = latest_period.year * 12 + latest_period.month - 2
    prior_year, prior_month_zero = divmod(month_index, 12)
    prior_period = dt.date(prior_year, prior_month_zero + 1, 1)
    if prior_period not in latest_by_period:
        return {}

    latest_amount = _number(latest_by_period[latest_period][3])
    prior_amount = _number(latest_by_period[prior_period][3])
    if latest_amount is None or prior_amount is None or prior_amount <= 0:
        return {}

    return {
        "revenue_mom_pct": round((latest_amount / prior_amount - 1) * 100, 2),
        "revenue_mom_prior_thousand_ntd": round(prior_amount, 2),
    }


def _load_material_convertible_bond_cash_context(
    cursor, *, codes: set[str], as_of_date: dt.date,
    started_at: dt.datetime, source_updated_at_cutoff: dt.datetime,
) -> dict[str, dict[str, Any]]:
    """Add point-in-time financing context without changing directional scores."""
    if not codes:
        return {}
    placeholders = ",".join(["%s"] * len(codes))
    cursor.execute(
        f"""
        SELECT entity_id, effective_date, event_time, title, value_text, source_url
        FROM source_records
        WHERE entity_type='stock' AND event_type='company_event'
          AND source_key IN ('twse_company_events', 'tpex_company_events')
          AND quality_status='ok' AND entity_id IN ({placeholders})
          AND effective_date>=DATE_SUB(%s, INTERVAL 45 DAY)
          AND effective_date<=%s AND event_time<=%s AND updated_at<=%s
          AND title LIKE %s AND value_text LIKE %s
        ORDER BY event_time DESC, id DESC
        """,
        (*sorted(codes), as_of_date, as_of_date, started_at,
         source_updated_at_cutoff, "%轉換公司債%", "%收足所有%"),
    )
    funding_rows = cursor.fetchall()
    result: dict[str, dict[str, Any]] = {}
    seen_codes: set[str] = set()
    for code_value, effective_date, event_time, title, description, source_url in funding_rows:
        code = _normalize_code(code_value)
        if code not in codes or code in seen_codes:
            continue
        seen_codes.add(code)
        compact_description = re.sub(r"\s+", "", str(description or ""))
        if "收足所有應募款項" not in compact_description:
            continue
        amount_match = re.search(
            r"共計\s*新[台臺]幣\s*([\d,]+)\s*元",
            compact_description,
        )
        if not amount_match:
            continue
        cash_received = int(amount_match.group(1).replace(",", ""))
        if cash_received <= 0:
            continue

        cursor.execute(
            """
            SELECT effective_date, event_time, value_text, source_url
            FROM source_records
            WHERE entity_type='stock' AND entity_id=%s
              AND source_key IN ('twse_company_events', 'tpex_company_events')
              AND event_type='company_event' AND quality_status='ok'
              AND event_time IS NOT NULL AND event_time<%s
              AND updated_at<=%s
              AND value_text LIKE %s
            ORDER BY event_time DESC, id DESC
            LIMIT 1
            """,
            (code, event_time, source_updated_at_cutoff,
             "%期末歸屬於母公司業主之權益%"),
        )
        equity_row = cursor.fetchone()
        if not equity_row:
            continue
        equity_amount = None
        equity_period = None
        equity_source_url = str(equity_row[3] or "")
        report_text = str(equity_row[2] or "")
        equity_match = re.search(
            r"期末歸屬於母公司業主之權益\s*[（(]\s*(仟元|千元|元)\s*[）)]\s*[:：]\s*([\d,]+)",
            report_text,
        )
        if not equity_match:
            continue
        report_period_match = re.search(
            r"財務報告或年度自結財務資訊報導期間\s*起訖日期[^:：]{0,60}"
            r"[:：]\s*(\d{2,3}/\d{1,2}/\d{1,2})\s*[~～至-]\s*"
            r"(\d{2,3}/\d{1,2}/\d{1,2})",
            report_text,
        )
        if not report_period_match:
            continue
        roc_end = re.fullmatch(
            r"(\d{2,3})/(\d{1,2})/(\d{1,2})",
            report_period_match.group(2),
        )
        if not roc_end:
            continue
        roc_year = int(roc_end.group(1))
        report_end = dt.date(
            roc_year + 1911 if roc_year < 1911 else roc_year,
            int(roc_end.group(2)), int(roc_end.group(3)),
        )
        unit_multiplier = 1_000 if equity_match.group(1) in {"仟元", "千元"} else 1
        equity_amount = int(equity_match.group(2).replace(",", "")) * unit_multiplier
        equity_period = report_end
        if not equity_row or not equity_amount or not equity_period:
            continue

        ratio_pct = cash_received / equity_amount * 100
        if ratio_pct < MATERIAL_CB_CASH_TO_PARENT_EQUITY_RATIO * 100:
            continue
        result[code] = {
            "event_date": str(_date_only(effective_date) or effective_date),
            "title": str(title or "可轉債應募款項收足")[:160],
            "cash_received_ntd": cash_received,
            "parent_equity_period": equity_period.isoformat(),
            "parent_equity_ntd": equity_amount,
            "cash_to_parent_equity_pct": round(ratio_pct, 1),
            "source_url": str(source_url or ""),
            "parent_equity_source_url": equity_source_url,
            "threshold_pct": round(MATERIAL_CB_CASH_TO_PARENT_EQUITY_RATIO * 100, 1),
        }
    return result


def _open_stock_connection():
    connection = pd.read_csv(sql_configure_path, index_col="name")
    return get_sql(
        connection.loc["host", "value"],
        int(connection.loc["port", "value"]),
        connection.loc["user", "value"],
        connection.loc["password", "value"],
        "stock",
    )


def _open_news_connection():
    connection = pd.read_csv(sql_configure_path, index_col="name")
    return get_sql(
        connection.loc["host", "value"],
        int(connection.loc["port", "value"]),
        connection.loc["user", "value"],
        connection.loc["password", "value"],
        "news_ch",
    )


def _upsert_run(cursor, *, run_key: str, source_dag_run_id: str | None,
                source_dag_id: str | None, scan_mode: str,
                started_at: dt.datetime) -> None:
    cursor.execute(
        f"""
        INSERT INTO market_signal_runs (
            run_key, source_dag_run_id, source_dag_id, scan_mode, as_of_date, status,
            source_status, universe_count, price_count, flow_count,
            revenue_count, financial_count, borrow_count, insider_count,
            dividend_count, candidate_count, notified_count, started_at,
            finished_at, error_message, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, NULL, 'running', 'unknown', 0, 0, 0, 0,
                  0, 0, 0, 0, 0, 0, %s, NULL, NULL, %s, %s)
        ON DUPLICATE KEY UPDATE
            source_dag_run_id=VALUES(source_dag_run_id),
            source_dag_id=VALUES(source_dag_id),
            scan_mode=VALUES(scan_mode), status='running', source_status='unknown',
            universe_count=0, price_count=0, flow_count=0, revenue_count=0,
            financial_count=0, borrow_count=0, insider_count=0, dividend_count=0,
            candidate_count=0, notified_count=0, started_at=VALUES(started_at),
            finished_at=NULL, error_message=NULL, updated_at=VALUES(updated_at)
        """,
        (run_key, source_dag_run_id, source_dag_id, scan_mode,
         started_at, started_at, started_at),
    )
    cursor.execute("DELETE FROM market_signal_outcomes WHERE run_key=%s", (run_key,))
    cursor.execute("DELETE FROM market_signal_candidates WHERE run_key=%s", (run_key,))


def _source_collection_state(
    cursor, source_dag_run_id: str | None, schedule_hint: str | None = None,
    source_dag_id: str | None = None,
) -> str:
    """Classify the run by final task attempts and batch row-accounting quality."""

    if not source_dag_run_id:
        return "unknown"
    if not source_dag_id:
        return "partial"
    cursor.execute(
        """
        SELECT source_key, task_id, map_index, try_number, status, started_at, id,
               row_count, accepted_count, rejected_count, duplicate_count
        FROM source_runs
        WHERE dag_run_id=%s AND dag_id=%s
        ORDER BY source_key, task_id, map_index,
                 try_number DESC, started_at DESC, id DESC
        """,
        (source_dag_run_id, source_dag_id),
    )
    rows = cursor.fetchall()
    latest_by_task: dict[tuple[Any, Any, Any], tuple[Any, ...]] = {}
    for row in rows:
        source_key, task_id, map_index = row[:3]
        task_key = (source_key, task_id, map_index)
        latest_by_task.setdefault(task_key, row)

    if schedule_hint:
        cursor.execute(
            """
            SELECT source_key FROM source_registry
            WHERE enabled=1 AND approval_status='approved'
              AND priority_score >= %s AND schedule_hint=%s
            """,
            (75.0, schedule_hint),
        )
        expected_source_keys = {str(row[0]) for row in cursor.fetchall()}
        reported_source_keys = {str(key[0]) for key in latest_by_task}
        if not expected_source_keys or not expected_source_keys.issubset(reported_source_keys):
            return "partial"
    elif not latest_by_task:
        return "unknown"

    if not latest_by_task or any(str(row[4]) != "success" for row in latest_by_task.values()):
        return "partial"

    # A green task is not a clean batch when it leaves source rows quarantined,
    # reports no parsed rows, or cannot account for every normalized row.
    # Duplicates are accounted separately and are not by themselves an error.
    for row in latest_by_task.values():
        row_count = int(row[7] or 0)
        accepted_count = int(row[8] or 0)
        rejected_count = int(row[9] or 0)
        duplicate_count = int(row[10] or 0)
        if (
            row_count == 0
            or rejected_count > 0
            or row_count != accepted_count + rejected_count + duplicate_count
            or (row_count > 0 and accepted_count == 0 and duplicate_count != row_count)
        ):
            return "data_review_required"
    return "complete"


def _source_collection_issues(
    cursor, source_dag_run_id: str | None, schedule_hint: str | None,
    source_dag_id: str | None,
) -> list[str]:
    """Return failed/missing feeds and batch-quality anomalies for a notice."""

    if not source_dag_run_id or not source_dag_id:
        return ["來源 run 關聯資訊不足"]

    cursor.execute(
        """
        SELECT sr.source_key, sr.task_id, sr.map_index, sr.status,
               sr.http_status, registry.source_name, sr.try_number,
               sr.started_at, sr.id, sr.error_message, sr.row_count,
               sr.accepted_count, sr.rejected_count, sr.duplicate_count,
               sr.finished_at
        FROM source_runs sr
        LEFT JOIN source_registry registry ON registry.source_key=sr.source_key
        WHERE sr.dag_run_id=%s AND sr.dag_id=%s
        ORDER BY sr.source_key, sr.task_id, sr.map_index,
                 sr.try_number DESC, sr.started_at DESC, sr.id DESC
        """,
        (source_dag_run_id, source_dag_id),
    )
    rows = cursor.fetchall()
    if not rows:
        cursor.execute(
            "SELECT COUNT(*) FROM source_runs WHERE dag_run_id=%s",
            (source_dag_run_id,),
        )
        lineage_row = cursor.fetchone()
        if int((lineage_row or (0,))[0] or 0) > 0:
            return ["來源回執缺少 DAG ID 或 DAG 不符，無法逐來源核對"]
        return ["尚無可關聯此來源 run 的逐來源回執"]
    latest_by_task: dict[tuple[str, str, Any], tuple[Any, ...]] = {}
    for row in rows:
        task_key = (str(row[0]), str(row[1] or ""), row[2])
        latest_by_task.setdefault(task_key, row)

    issues: list[str] = []
    reported_source_keys = {key[0] for key in latest_by_task}
    for source_key, _task_id, _map_index in sorted(
        latest_by_task, key=lambda key: (key[0], key[1], str(key[2]))
    ):
        row = latest_by_task[(source_key, _task_id, _map_index)]
        is_success = str(row[3]) == "success"
        row_count = int(row[10] or 0)
        accepted_count = int(row[11] or 0)
        rejected_count = int(row[12] or 0)
        duplicate_count = int(row[13] or 0)
        accounted_count = accepted_count + rejected_count + duplicate_count
        row_accounting_mismatch = row_count != accounted_count
        no_accepted_records = (
            row_count > 0 and accepted_count == 0 and duplicate_count != row_count
        )
        if (
            is_success and rejected_count <= 0 and row_count > 0
            and not row_accounting_mismatch and not no_accepted_records
        ):
            continue
        source_name = str(row[5] or source_key)
        http_status = int(row[4]) if row[4] is not None else None
        error_type = str(row[9] or "").partition(":")[0].strip()
        if is_success:
            if row_count == 0:
                status_label = "任務成功但回傳 0 列，尚未確認來源語意"
            elif rejected_count > 0:
                status_label = "任務成功，但有隔離列"
            elif row_accounting_mismatch:
                status_label = "任務成功，但列數核帳不符"
            elif no_accepted_records:
                status_label = "任務成功，但沒有可接受資料列"
            else:
                status_label = "任務成功，但資料列品質待檢視"
        elif error_type in {
            "IncompleteTPExQuoteResponseError",
            "IncompleteTPExOpenAPIResponseError",
        }:
            status_label = "回應本文不完整"
            if http_status is not None:
                status_label += f"（HTTP {http_status}）"
        elif error_type == "ChunkedEncodingError":
            status_label = "回應本文中斷"
            if http_status is not None:
                status_label += f"（HTTP {http_status}）"
        elif http_status is not None:
            status_label = f"HTTP {http_status}"
            if 200 <= http_status < 300 and error_type:
                status_label += f"｜後處理失敗（{error_type}）"
        else:
            status_label = "HTTP 狀態未保存"
            if error_type:
                status_label += f"（{error_type}）"
        status_label += (
            f"｜批次列 正規化/接受/隔離/重複 "
            f"{row_count}/{accepted_count}/{rejected_count}/{duplicate_count}"
        )
        if is_success and row[7] and row[14]:
            cursor.execute(
                """
                SELECT MIN(effective_date), MAX(effective_date),
                       COUNT(DISTINCT entity_id)
                FROM source_records
                WHERE source_key=%s AND quality_status='ok'
                  AND observed_at >= %s AND observed_at <= %s
                """,
                (source_key, row[7], row[14]),
            )
            effective_min, effective_max, accepted_entities = cursor.fetchone()
            if effective_min is not None:
                date_label = str(effective_min)
                if effective_max is not None and effective_max != effective_min:
                    date_label += f"–{effective_max}"
                status_label += f"｜有效日 {date_label}｜接受實體 {int(accepted_entities or 0)}"
        issues.append(f"{source_name}｜{status_label}")

    if schedule_hint:
        cursor.execute(
            """
            SELECT source_key FROM source_registry
            WHERE enabled=1 AND approval_status='approved'
              AND priority_score >= %s AND schedule_hint=%s
            """,
            (75.0, schedule_hint),
        )
        expected_source_keys = {str(row[0]) for row in cursor.fetchall()}
        issues.extend(
            f"{source_key}｜缺少來源回執"
            for source_key in sorted(expected_source_keys - reported_source_keys)
        )

    return issues or ["未找到單項失敗回執；請檢查來源映射與覆蓋"]


def _source_collection_issue_fingerprint(
    collection_status: str, issues: list[str],
) -> str:
    """Fingerprint issue classes, excluding volatile row counts and run IDs."""

    if collection_status == "complete":
        return "complete"
    issue_classes = sorted({
        f"{issue.partition('｜')[0]}:{issue.partition('｜')[2].partition('｜')[0]}"
        for issue in issues
    })
    payload = json.dumps(
        [collection_status, issue_classes], ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_source_collection_alert_fingerprint(cursor) -> str | None:
    cursor.execute(
        "SELECT state_text FROM market_signal_state WHERE state_key=%s",
        (SOURCE_COLLECTION_ALERT_STATE_KEY,),
    )
    row = cursor.fetchone()
    return str(row[0]) if row and row[0] else None


def _save_source_collection_alert_fingerprint(
    cursor, connection, fingerprint: str, now: dt.datetime,
) -> None:
    cursor.execute(
        """
        INSERT INTO market_signal_state (state_key, state_text, updated_at)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            state_text=VALUES(state_text), updated_at=VALUES(updated_at)
        """,
        (SOURCE_COLLECTION_ALERT_STATE_KEY, fingerprint, now),
    )
    connection.commit()


def _load_intraday_candidates(
    cursor, *, last_cursor: int, upper_cursor: int, today: dt.date,
    started_at: dt.datetime, diagnostics: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    overlap_floor = max(0, last_cursor - INTRADAY_CURSOR_OVERLAP_IDS)
    source_updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    cursor.execute(
        f"""
        SELECT id, source_key, record_hash, entity_id, event_type, event_time,
               effective_date, title, metric_value, metric_name, source_url,
               value_text, payload, created_at AS source_created_at_utc
        FROM source_records
        WHERE (
                (id>%s AND id<=%s)
                OR (id>%s AND id<=%s)
                OR (
                    updated_at>=DATE_SUB(%s, INTERVAL {INTRADAY_REVISION_LOOKBACK_DAYS} DAY)
                )
              )
          AND updated_at<=%s
          AND quality_status='ok'
          AND entity_type='stock'
          AND event_type IN (
                'company_event', 'monthly_revenue', 'company_forecast_variance',
                'company_forecast_variance_threshold',
                'insider_transfer_planned', 'insider_transfer_untransferred'
              )
          AND (
                (
                    event_time IS NOT NULL
                    AND event_time>=DATE_SUB(%s, INTERVAL {INTRADAY_LOOKBACK_DAYS} DAY)
                    AND event_time<=%s
                )
                OR (
                    event_type='company_forecast_variance_threshold'
                    AND created_at>=DATE_SUB(%s, INTERVAL {INTRADAY_LOOKBACK_DAYS} DAY)
                    AND created_at<=%s
                )
              )
        ORDER BY id
        """,
        (last_cursor, upper_cursor, overlap_floor, last_cursor,
         source_updated_at_cutoff, source_updated_at_cutoff,
         started_at, started_at, started_at, started_at),
    )
    rows = cursor.fetchall()
    if diagnostics is not None:
        diagnostics.clear()
        diagnostics.update({
            "selected_source_row_count": len(rows),
            "selected_source_rows_by_event_type": {},
            "company_event_materiality_counts": {},
            "excluded_by_policy_or_quality": {},
            "candidate_fingerprints_checked": 0,
            "already_sent_suppressed": 0,
            "already_sent_with_http_2xx_and_receipt": 0,
            "already_sent_without_confirmed_receipt": 0,
            "already_sent_by_event_type": {},
            "new_candidate_count": 0,
            "cursor_limit_deferred_count": 0,
        })

        def increment(section: str, key: str) -> None:
            counts = diagnostics.setdefault(section, {})
            counts[key] = int(counts.get(key, 0)) + 1
    else:
        increment = None

    if increment:
        for selected_row in rows:
            increment(
                "selected_source_rows_by_event_type",
                f"{selected_row[1]}:{selected_row[4]}",
            )

    latest_forecast_version_ids: dict[tuple[Any, ...], int] = {}
    for source_row in rows:
        if source_row[4] != "company_forecast_variance":
            continue
        try:
            version_details = json.loads(str(source_row[11] or "{}"))
        except (TypeError, ValueError):
            continue
        if not isinstance(version_details, dict):
            continue
        version_key = (
            str(source_row[1]), _normalize_code(source_row[3]),
            _date_only(source_row[6]), version_details.get("reporting_year"),
            version_details.get("reporting_quarter"),
            str(version_details.get("forecast_sequence") or ""),
            str(version_details.get("coverage_period") or ""),
        )
        source_id = int(source_row[0])
        latest_forecast_version_ids[version_key] = max(
            source_id, latest_forecast_version_ids.get(version_key, source_id)
        )
    correction_keys = {
        (_normalize_code(row[3]), _revision_title_key(row[7])[1])
        for row in rows
        if row[4] == "company_event" and _revision_title_key(row[7])[0]
    }
    result: list[dict[str, Any]] = []
    seen_forecast_variances: set[str] = set()
    next_cursor = upper_cursor

    for row in rows:
        (source_id, source_key, record_hash, entity_id, event_type, event_time,
         effective_date, title, metric_value, metric_name, source_url, value_text,
         payload, source_created_at_utc) = row
        code = _normalize_code(entity_id)
        if not code:
            if increment:
                increment("excluded_by_policy_or_quality", "missing_security_code")
            continue
        if event_type == "monthly_revenue":
            # Actual revenue alone gives no market-expectation baseline and is
            # retained as collected context, not an intraday push candidate.
            if increment:
                increment(
                    "excluded_by_policy_or_quality",
                    "monthly_revenue_without_expectation_baseline",
                )
            continue
        security_name = _source_security_name(payload)
        if not security_name and event_type != "company_event":
            security_name = str(title or "").strip()
        is_correction, normalized_title = _revision_title_key(title)
        if (
            event_type == "company_event" and not is_correction
            and (code, normalized_title) in correction_keys
        ):
            # A correction in the same collected batch supersedes the stale
            # disclosure; keep the corrected record as the single candidate.
            if increment:
                increment(
                    "excluded_by_policy_or_quality",
                    "superseded_company_event_revision",
                )
            continue

        event_signal: dict[str, str] | None = None
        if event_type == "company_event":
            event_signal = _classify_company_event(
                str(title or "未提供標題")[:160], str(value_text or "")
            )
            if increment:
                increment(
                    "company_event_materiality_counts",
                    str(event_signal.get("materiality") or "unknown"),
                )
            # Company announcements are retained by the official source feed.
            # Only body-supported, actionable events belong in the radar.
            if event_signal["materiality"] != "key":
                if increment:
                    increment(
                        "excluded_by_policy_or_quality",
                        "company_event_not_key_materiality",
                    )
                continue

        forecast_details: dict[str, Any] | None = None
        if event_type == "company_forecast_variance":
            try:
                parsed = json.loads(str(value_text or "{}"))
            except (TypeError, ValueError):
                if increment:
                    increment("excluded_by_policy_or_quality", "invalid_company_forecast_variance_payload")
                continue
            if not isinstance(parsed, dict):
                if increment:
                    increment("excluded_by_policy_or_quality", "invalid_company_forecast_variance_payload")
                continue
            forecast_details = parsed
            version_key = (
                str(source_key), code, _date_only(effective_date),
                parsed.get("reporting_year"), parsed.get("reporting_quarter"),
                str(parsed.get("forecast_sequence") or ""),
                str(parsed.get("coverage_period") or ""),
            )
            if latest_forecast_version_ids.get(version_key) != int(source_id):
                if increment:
                    increment("excluded_by_policy_or_quality", "superseded_company_forecast_variance_version")
                continue
            # TWSE may emit identical variance rows for several forecast
            # sequence IDs or refresh the publication date without changing
            # values. Keep source versions, but notify once per distinct
            # quarter/coverage/value comparison.
            # This digest is persisted as candidate_hash and becomes part of
            # the notification ledger key. Treat these semantic inputs as a
            # compatibility contract: changing them without reconciling the
            # ledger can replay an already-sent disclosure.
            semantic_payload = [
                source_key, code, effective_date,
                parsed.get("reporting_year"), parsed.get("reporting_quarter"),
                parsed.get("coverage_period"), parsed.get("quarter_actual"),
                parsed.get("cumulative_actual"), parsed.get("quarter_forecast"),
                parsed.get("cumulative_forecast"),
            ]
            revision_fingerprint = hashlib.sha256(
                json.dumps(
                    semantic_payload, ensure_ascii=False, default=str,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if revision_fingerprint in seen_forecast_variances:
                if increment:
                    increment("excluded_by_policy_or_quality", "duplicate_company_forecast_variance_semantics")
                continue
            seen_forecast_variances.add(revision_fingerprint)
        elif event_type == "company_forecast_variance_threshold":
            try:
                parsed = json.loads(str(value_text or "{}"))
            except (TypeError, ValueError):
                if increment:
                    increment("excluded_by_policy_or_quality", "invalid_forecast_variance_threshold_payload")
                continue
            if not isinstance(parsed, dict):
                if increment:
                    increment("excluded_by_policy_or_quality", "invalid_forecast_variance_threshold_payload")
                continue
            forecast_details = parsed
            semantic_payload = [
                source_key, code,
                parsed.get("reporting_year"), parsed.get("reporting_quarter"),
            ]
            revision_fingerprint = hashlib.sha256(
                json.dumps(
                    semantic_payload, ensure_ascii=False, default=str,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if revision_fingerprint in seen_forecast_variances:
                if increment:
                    increment("excluded_by_policy_or_quality", "duplicate_forecast_variance_threshold_semantics")
                continue
            seen_forecast_variances.add(revision_fingerprint)
        else:
            semantic_payload = [
                record_hash, event_type, code, event_time, effective_date,
                title, metric_value, metric_name, value_text,
            ]
            if (
                event_signal is not None
                and event_signal["label"] == "併購交易｜稀釋與效益待驗"
            ):
                # This classifier correction changes the disposition of an
                # already-sent M&A disclosure. Include its stable classification
                # outcome so the corrected neutral card is not suppressed by
                # the legacy bearish notification key. Other event digests stay
                # byte-for-byte compatible with their existing ledger entries.
                semantic_payload.append(
                    ["classification", event_signal["materiality"],
                     event_signal["label"], event_signal["direction"]]
                )
            revision_fingerprint = hashlib.sha256(
                json.dumps(
                    semantic_payload,
                    ensure_ascii=False, default=str, separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        if increment:
            diagnostics["candidate_fingerprints_checked"] += 1
        dedupe_key = hashlib.sha256(
            f"intraday|{revision_fingerprint}".encode("utf-8")
        ).hexdigest()
        cursor.execute(
            """SELECT status, platform_http_status, platform_receipt_id
               FROM market_signal_notifications WHERE dedupe_key=%s""",
            (dedupe_key,),
        )
        sent_row = cursor.fetchone()
        if sent_row and sent_row[0] == "sent":
            if increment:
                diagnostics["already_sent_suppressed"] += 1
                has_confirmed_receipt = (
                    sent_row[1] is not None
                    and 200 <= int(sent_row[1]) < 300
                    and bool(sent_row[2])
                )
                diagnostics[
                    "already_sent_with_http_2xx_and_receipt"
                    if has_confirmed_receipt
                    else "already_sent_without_confirmed_receipt"
                ] += 1
                event_counts = diagnostics.setdefault(
                    "already_sent_by_event_type", {}
                ).setdefault(event_type, {
                    "sent_ledger_count": 0,
                    "http_2xx_and_receipt_count": 0,
                    "without_confirmed_receipt_count": 0,
                })
                event_counts["sent_ledger_count"] += 1
                event_counts[
                    "http_2xx_and_receipt_count"
                    if has_confirmed_receipt
                    else "without_confirmed_receipt_count"
                ] += 1
            continue

        if event_type == "company_event":
            announcement_title = str(title or "未提供標題")[:160]
            description = str(value_text or "")
            assert event_signal is not None
            signal_key = "company_announcement"
            detail = event_signal["label"]
            value = {
                "kind": "company_event",
                "title": announcement_title,
                "description": description,
                "event_time": str(event_time),
                "source_created_at_utc": str(source_created_at_utc or ""),
                "event_signal": event_signal,
                "source_key": source_key,
                "source_url": source_url,
                "source_record_id": int(source_id),
            }
        elif event_type in {
            "insider_transfer_planned", "insider_transfer_untransferred"
        }:
            try:
                transfer_details = json.loads(str(value_text or "{}"))
            except (TypeError, ValueError):
                if increment:
                    increment("excluded_by_policy_or_quality", "invalid_insider_transfer_payload")
                continue
            if not isinstance(transfer_details, dict):
                if increment:
                    increment("excluded_by_policy_or_quality", "invalid_insider_transfer_payload")
                continue
            planned_transfer = event_type == "insider_transfer_planned"
            signal_key = event_type
            event_signal = {
                "label": (
                    "內部人預定轉讓申報｜方向未定"
                    if planned_transfer else "內部人未完成轉讓紀錄｜方向未定"
                ),
                "action": (
                    "官方事前申報，只表示計畫且不等於實際成交；不計入多空分數。"
                    "可與後續未完成轉讓紀錄、公告及價格/成交量反應做點時驗證。"
                    if planned_transfer else
                    "官方列示未完成轉讓的股數與理由；這是後續狀態，不等同實際賣出。"
                    "不計入多空分數，需和原申報及市場成交資料分開核對。"
                ),
            }
            detail = event_signal["label"]
            value = {
                "kind": event_type,
                "title": (
                    "內部人預定轉讓申報"
                    if planned_transfer else "內部人未完成轉讓紀錄"
                ),
                "event_time": str(event_time),
                "published_date": str(event_time.date()) if event_time else None,
                "effective_date": str(effective_date),
                "transfer_details": transfer_details,
                "event_signal": event_signal,
                "source_key": source_key,
                "source_url": source_url,
                "source_record_id": int(source_id),
            }
        elif event_type == "company_forecast_variance_threshold":
            assert forecast_details is not None
            detail = "TPEx 官方簡式名單列示的財測差異門檻事件；方向未定"
            event_signal = {
                "label": "上櫃財測差異達官方門檻",
                "action": (
                    "TPEx 官方清單列示此公司已達財測差異門檻，但簡式資料不含實績/預測金額；"
                    "不能據此判定多空，請核對 MOPS 原始財測與查核數。本事件不納入多空分數。"
                ),
            }
            signal_key = "company_forecast_variance_threshold"
            value = {
                "kind": "company_forecast_variance_threshold",
                "title": "官方簡式財測差異門檻名單列",
                "event_time": None,
                "snapshot_date": forecast_details.get("snapshot_date"),
                "effective_date": str(effective_date),
                "reporting_year": forecast_details.get("reporting_year"),
                "reporting_quarter": forecast_details.get("reporting_quarter"),
                "threshold_basis": forecast_details.get("threshold_basis"),
                "source_observed_at_utc": str(source_created_at_utc),
                "event_signal": event_signal,
                "source_key": source_key,
                "source_url": source_url,
                "source_record_id": int(source_id),
            }
        else:
            assert forecast_details is not None
            detail = "官方差異門檻事件；方向未定"
            event_signal = {
                "label": "公司財測差異",
                "action": (
                    "TWSE 官方差異清單列出此季實績與公司財測差異達門檻的個案。"
                    "請回看原始財測及財報的涵蓋期間、修正版本與差異原因；"
                    "這是公司自己的財測，不是券商預估，暫不計入多空分數。"
                ),
            }
            signal_key = "company_forecast_variance"
            value = {
                "kind": "company_forecast_variance",
                "title": str(title or "公司財測差異")[:100],
                "event_time": str(event_time),
                "published_date": str(event_time.date()) if event_time else None,
                "effective_date": str(effective_date),
                "reporting_year": forecast_details.get("reporting_year"),
                "reporting_quarter": forecast_details.get("reporting_quarter"),
                "forecast_sequence": forecast_details.get("forecast_sequence"),
                "coverage_period": forecast_details.get("coverage_period"),
                "quarter_actual": forecast_details.get("quarter_actual"),
                "cumulative_actual": forecast_details.get("cumulative_actual"),
                "quarter_forecast": forecast_details.get("quarter_forecast"),
                "cumulative_forecast": forecast_details.get("cumulative_forecast"),
                "event_signal": event_signal,
                "source_key": source_key,
                "source_url": source_url,
                "source_record_id": int(source_id),
            }

        value["detail"] = detail
        result.append(
            {
                "candidate_hash": revision_fingerprint,
                "entity_type": "stock",
                "entity_id": code,
                "security_name": security_name[:255],
                "market": None,
                "industry": None,
                "as_of_date": today,
                "observed_at": started_at,
                "signal_count": 1,
                "signal_keys": signal_key,
                "score": 1.0,
                "details": value,
            }
        )
        if increment:
            diagnostics["new_candidate_count"] = len(result)
        if len(result) > MAX_INTRADAY_CANDIDATES_PER_RUN:
            # Keep the next qualifying record for the next poll. Advancing to
            # the preceding id prevents a busy release from being truncated.
            next_cursor = (
                min(upper_cursor, int(source_id) - 1)
                if int(source_id) > last_cursor else last_cursor
            )
            result.pop()
            if increment:
                diagnostics["cursor_limit_deferred_count"] += 1
            break
    if increment:
        diagnostics["new_candidate_count"] = len(result)
    return result, next_cursor


def _load_intraday_revenue_context(
    cursor,
    *,
    source_key: str,
    entity_id: str,
    effective_date: Any,
    started_at: dt.datetime,
    source_updated_at_cutoff: dt.datetime,
) -> dict[str, Any]:
    """Load exact prior-month and three-month revenue context as of this scan."""

    period = _date_only(effective_date)
    if period is None:
        return {
            "previous_revenue_period": None,
            "previous_revenue_yoy_pct": None,
            "revenue_3m_context_status": "unavailable_effective_date",
            "revenue_3m_missing_periods": [],
            "revenue_ytd_yoy_pct": None,
        }
    period = dt.date(period.year, period.month, 1)
    latest_month_index = period.year * 12 + period.month - 1
    expected_periods = []
    for month_index in range(latest_month_index - 2, latest_month_index + 1):
        year, month_zero = divmod(month_index, 12)
        expected_periods.append(dt.date(year, month_zero + 1, 1))

    cursor.execute(
        MONTHLY_REVENUE_ASOF_CTE + """
        SELECT r.effective_date, r.event_time, r.metric_value,
               r.payload,
               r.id
        FROM monthly_revenue_snapshot r
        WHERE r.source_key=%s AND r.entity_type='stock'
          AND r.entity_id=%s AND r.event_type='monthly_revenue'
          AND r.quality_status='ok'
          AND r.effective_date>=%s AND r.effective_date<=%s
          AND r.event_time IS NOT NULL AND r.event_time<=%s
        ORDER BY r.effective_date, r.event_time, r.id
        """,
        (
            source_updated_at_cutoff, source_updated_at_cutoff,
            source_updated_at_cutoff, source_updated_at_cutoff,
            source_key, entity_id, expected_periods[0], period, started_at,
        ),
    )
    history_rows = []
    for row in cursor.fetchall():
        row_period = _date_only(row[0])
        if row_period is None:
            continue
        try:
            raw_payload = json.loads(row[3]) if isinstance(row[3], str) else row[3]
        except (TypeError, ValueError):
            raw_payload = {}
        if not isinstance(raw_payload, dict):
            raw_payload = {}
        history_rows.append(
            (
                dt.date(row_period.year, row_period.month, 1), row[1],
                _number(row[2]),
                parse_number(raw_payload.get("營業收入-當月營收")),
                parse_number(raw_payload.get("營業收入-去年當月營收")),
                parse_number(raw_payload.get("累計營業收入-前期比較增減(%)")),
                "", "", "", int(row[4] or 0),
            )
        )

    latest_by_period: dict[dt.date, tuple[Any, ...]] = {}
    for row in history_rows:
        latest_by_period[row[0]] = row
    previous_period = expected_periods[-2]
    previous_row = latest_by_period.get(previous_period)
    previous_yoy = _number(previous_row[2]) if previous_row else None
    missing_periods = [
        value.strftime("%Y-%m")
        for value in expected_periods
        if value not in latest_by_period
    ]
    trailing_context = _trailing_three_month_revenue_context(history_rows)
    if trailing_context:
        context_status = "complete"
    elif missing_periods:
        context_status = "missing_periods"
    else:
        context_status = "missing_or_invalid_baseline"
    current_row = latest_by_period.get(period)
    return {
        "previous_revenue_period": previous_period.isoformat(),
        "previous_revenue_yoy_pct": previous_yoy,
        "revenue_3m_context_status": context_status,
        "revenue_3m_missing_periods": missing_periods,
        "revenue_ytd_yoy_pct": (
            _number(current_row[5]) if current_row is not None else None
        ),
        **trailing_context,
    }


def _insert_candidates(cursor, run_key: str, candidates: list[dict[str, Any]]) -> None:
    if not candidates:
        return
    cursor.executemany(
        """
        INSERT INTO market_signal_candidates (
            run_key, candidate_hash, entity_type, entity_id, security_name,
            market, industry, as_of_date, observed_at, signal_count,
            signal_keys, score, details_text, created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            (
                run_key,
                candidate["candidate_hash"],
                candidate["entity_type"],
                candidate["entity_id"],
                candidate.get("security_name"),
                candidate.get("market"),
                candidate.get("industry"),
                candidate.get("as_of_date"),
                candidate["observed_at"],
                candidate["signal_count"],
                candidate["signal_keys"],
                candidate["score"],
                json.dumps(candidate["details"], ensure_ascii=False, sort_keys=True),
                candidate["observed_at"],
            )
            for candidate in candidates
        ],
    )


def _update_run(cursor, *, run_key: str, status: str, source_status: str,
                started_at: dt.datetime, as_of_date: dt.date | None = None,
                universe_count: int = 0, price_count: int = 0, flow_count: int = 0,
                revenue_count: int = 0, financial_count: int = 0,
                borrow_count: int = 0, insider_count: int = 0,
                dividend_count: int = 0, candidate_count: int = 0,
                error_message: str | None = None) -> None:
    cursor.execute(
        """
        UPDATE market_signal_runs
        SET as_of_date=%s, status=%s, source_status=%s, universe_count=%s,
        price_count=%s, flow_count=%s, revenue_count=%s,
            financial_count=%s, borrow_count=%s, insider_count=%s,
            dividend_count=%s, candidate_count=%s, finished_at=%s,
            error_message=%s, updated_at=%s
        WHERE run_key=%s
        """,
        (
            as_of_date, status, source_status, universe_count, price_count,
            flow_count, revenue_count, financial_count, borrow_count,
            insider_count, dividend_count, candidate_count, _now_taipei(),
            error_message[:1000] if error_message else None, _now_taipei(), run_key,
        ),
    )


def _persist_market_coverage(
    cursor, *, run_key: str, as_of_date: dt.date | None,
    coverage_by_market: dict[str, dict[str, Any]], observed_at: dt.datetime,
) -> None:
    if not coverage_by_market:
        return
    cursor.executemany(
        """
        INSERT INTO market_signal_run_market_coverage (
            run_key, market, as_of_date, universe_count, price_row_code_count,
            price_count, price_invalid_close_code_count, price_no_trade_code_count, flow_count,
            traded_without_flow_count,
            price_coverage_pct, flow_coverage_pct, flow_readiness_json, observed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            as_of_date=VALUES(as_of_date),
            universe_count=VALUES(universe_count),
            price_row_code_count=VALUES(price_row_code_count),
            price_count=VALUES(price_count),
            price_invalid_close_code_count=VALUES(price_invalid_close_code_count),
            price_no_trade_code_count=VALUES(price_no_trade_code_count),
            flow_count=VALUES(flow_count),
            traded_without_flow_count=VALUES(traded_without_flow_count),
            price_coverage_pct=VALUES(price_coverage_pct),
            flow_coverage_pct=VALUES(flow_coverage_pct),
            flow_readiness_json=VALUES(flow_readiness_json),
            observed_at=VALUES(observed_at)
        """,
        [
            (
                run_key, market, as_of_date,
                int(values.get("universe_count") or 0),
                int(values.get("price_row_code_count") or 0),
                int(values.get("price_count") or 0),
                int(values.get("price_invalid_close_code_count") or 0),
                int(values.get("price_no_trade_code_count") or 0),
                int(values.get("flow_count") or 0),
                (
                    int(values["traded_without_flow_count"])
                    if values.get("traded_without_flow_count") is not None else None
                ),
                float(values.get("price_coverage_pct") or 0),
                float(values.get("flow_coverage_pct") or 0),
                json.dumps(values["flow_readiness"], ensure_ascii=False, default=str)
                if values.get("flow_readiness") else None,
                observed_at,
            )
            for market, values in coverage_by_market.items()
        ],
    )


def _load_market_coverage(cursor, run_key: str) -> dict[str, dict[str, Any]]:
    cursor.execute(
        """
        SELECT market, universe_count, price_row_code_count, price_count,
               price_invalid_close_code_count, price_no_trade_code_count, flow_count,
               traded_without_flow_count, price_coverage_pct, flow_coverage_pct,
               flow_readiness_json
        FROM market_signal_run_market_coverage
        WHERE run_key=%s
        ORDER BY market
        """,
        (run_key,),
    )
    return {
        str(row[0]): {
            "universe_count": int(row[1] or 0),
            "price_row_code_count": (
                int(row[2]) if row[2] is not None else None
            ),
            "price_count": int(row[3] or 0),
            "price_invalid_close_code_count": (
                int(row[4]) if row[4] is not None else None
            ),
            "price_no_trade_code_count": (
                int(row[5]) if row[5] is not None else None
            ),
            "flow_count": int(row[6] or 0),
            "traded_without_flow_count": (
                int(row[7]) if row[7] is not None else None
            ),
            "price_coverage_pct": float(row[8] or 0),
            "flow_coverage_pct": float(row[9] or 0),
            "flow_readiness": json.loads(row[10]) if row[10] else {},
        }
        for row in cursor.fetchall()
    }


def _load_official_market_quote_rows(
    cursor, *, start_date: dt.date, end_date: dt.date,
    observed_at: dt.datetime, source_updated_at_cutoff: dt.datetime,
    entity_ids: set[str] | None = None,
    known_quote_keys: set[tuple[str, str, dt.date]] | None = None,
) -> list[
    tuple[
        str, str, dt.date, float | None, float | None, bool,
        float | None, float | None, float | None,
    ]
]:
    """Read only exchange-approved, point-in-time daily quote snapshots."""

    if start_date > end_date or entity_ids == set():
        return []
    entity_filter = ""
    entity_params: tuple[str, ...] = ()
    if entity_ids is not None:
        entity_values = tuple(sorted({_normalize_code(value) for value in entity_ids}))
        if not entity_values:
            return []
        entity_filter = (
            " AND entity_id IN (" + ",".join(["%s"] * len(entity_values)) + ")"
        )
        entity_params = entity_values
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT source_key, entity_id, effective_date, value_text, quality_status
        FROM market_context_snapshot
        WHERE event_type='market_quote_daily' AND entity_type='stock'
          AND source_key IN ('twse_equity_quotes_daily', 'tpex_equity_quotes_daily')
          AND effective_date>=%s AND effective_date<=%s
          AND (event_time IS NULL OR event_time<=%s)
        """ + (" AND quality_status='ok'" if known_quote_keys is None else "")
        + entity_filter + " ORDER BY effective_date, entity_id, id",
        (
            source_updated_at_cutoff, source_updated_at_cutoff,
            source_updated_at_cutoff, source_updated_at_cutoff,
            start_date, end_date, observed_at, *entity_params,
        ),
    )
    quote_rows: list[
        tuple[
            str, str, dt.date, float | None, float | None, bool,
            float | None, float | None, float | None,
        ]
    ] = []
    for source_key, entity_id, effective_date, value_text, quality_status in cursor.fetchall():
        code = _normalize_code(entity_id)
        period = _date_only(effective_date)
        if not code or not period:
            continue
        if known_quote_keys is not None:
            known_quote_keys.add((str(source_key), code, period))
        if quality_status != "ok":
            continue
        try:
            details = json.loads(value_text) if isinstance(value_text, str) else value_text
        except (TypeError, ValueError):
            details = {}
        if not isinstance(details, dict):
            details = {}
        quote_rows.append((
            str(source_key or ""), code, period,
            _number(details.get("closing_price")),
            _number(details.get("trade_volume")),
            details.get("no_trade") is True,
            _number(details.get("opening_price")),
            _number(details.get("highest_price")),
            _number(details.get("lowest_price")),
        ))
    return quote_rows


def _load_daily_candidates(
    source_cursor, stock_cursor, *, today: dt.date, started_at: dt.datetime,
    source_dag_run_id: str | None, source_dag_id: str | None,
    source_schedule_hint: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source_updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    stock_cursor.execute(
        """
        SELECT TRIM(證券代號), TRIM(證券名稱), TRIM(市場別), TRIM(產業別)
        FROM 全證券代碼相關資料表
        WHERE 證券代號 IS NOT NULL AND 市場別 IN ('上市', '上櫃')
        """
    )
    master_rows = stock_cursor.fetchall()
    master = {
        _normalize_code(row[0]): {
            "name": str(row[1] or "").strip(),
            "market": str(row[2] or "").strip(),
            "industry": str(row[3] or "").strip(),
        }
        for row in master_rows
        if row and row[0]
    }
    known_quote_keys: set[tuple[str, str, dt.date]] = set()
    official_quote_rows = _load_official_market_quote_rows(
        source_cursor,
        start_date=today - dt.timedelta(days=90),
        end_date=today,
        observed_at=started_at,
        source_updated_at_cutoff=source_updated_at_cutoff,
        known_quote_keys=known_quote_keys,
    )
    as_of_date = max((row[2] for row in official_quote_rows), default=None)
    prices_by_code: dict[
        str, list[tuple[dt.date, float | None, float | None, bool]]
    ] = {}
    if as_of_date:
        history_floor = as_of_date - dt.timedelta(days=45)
        for (
            source_key, code, trade_date, close, volume, no_trade,
            _opening, _highest, _lowest,
        ) in official_quote_rows:
            key = _normalize_code(code)
            if key not in master or trade_date < history_floor or trade_date > as_of_date:
                continue
            expected_source = OFFICIAL_MARKET_QUOTE_SOURCE_BY_MARKET.get(
                master[key]["market"]
            )
            if source_key != expected_source:
                continue
            prices_by_code.setdefault(key, []).append(
                (trade_date, _number(close), _number(volume), no_trade)
            )

    stored_history_points_by_code = {}
    if as_of_date:
        stock_cursor.execute("""SELECT TRIM(證券代號),資料日期,收盤,成交量,create_time
            FROM 個股成交資訊 WHERE 資料日期 BETWEEN %s AND %s AND create_time<=%s""",
            (as_of_date - dt.timedelta(days=45), as_of_date, source_updated_at_cutoff))
        stored_history = [dict(zip(("code", "date", "close", "volume_lots", "captured_at"), row))
                          for row in stock_cursor.fetchall()]
        prices_by_code, stored_history_points_by_code = merge_stored_quote_history(
            prices_by_code, stored_history, master=master, cutoff_utc=source_updated_at_cutoff,
            known_quote_keys={(code, day) for key, code, day in known_quote_keys
                              if code in master and key == OFFICIAL_MARKET_QUOTE_SOURCE_BY_MARKET.get(master[code]['market'])})

    # Use the scanner's actual market-wide quote dates to validate multi-session
    # positioning comparisons. A fixed row offset can span a missing session or
    # include an effective date that was not a trading session.
    price_session_dates_by_market: dict[str, set[dt.date]] = {
        "上市": set(),
        "上櫃": set(),
    }
    for code, rows in prices_by_code.items():
        market = str(master.get(code, {}).get("market") or "")
        if market in price_session_dates_by_market:
            price_session_dates_by_market[market].update(
                period for period, _close, _volume, _no_trade in rows
                if as_of_date and period <= as_of_date
            )

    # Use existing observations under the scan cutoff. Missing OTC branch data
    # abstains while available OTC institutional data continues to participate.
    order_flow_by_code, flow_readiness_by_market = load_equity_order_flow(
        source_cursor, stock_cursor, master=master, as_of_date=as_of_date,
        cutoff_utc=source_updated_at_cutoff, price_rows_by_code=prices_by_code,
        session_dates_by_market=price_session_dates_by_market,
    ) if as_of_date else ({}, {})

    revenue_by_code: dict[
        str,
        list[tuple[
            dt.date, dt.datetime | dt.date | None, float | None,
            float | None, float | None, float | None, str, str, str,
        ]],
    ] = {}
    if as_of_date:
        source_cursor.execute(
            MONTHLY_REVENUE_ASOF_CTE + """
            SELECT r.entity_id, r.effective_date, r.event_time, r.metric_value,
                   r.payload,
                   r.source_key, r.source_url
            FROM monthly_revenue_snapshot r
            WHERE r.event_type='monthly_revenue' AND r.entity_type='stock'
              AND r.quality_status='ok' AND r.effective_date IS NOT NULL
              AND r.effective_date<=%s
              AND r.effective_date>=DATE_SUB(%s, INTERVAL 18 MONTH)
              AND (r.event_time IS NULL OR r.event_time<=%s)
            ORDER BY r.entity_id, r.effective_date, r.event_time, r.id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, period, published_at, yoy, payload, source_key, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            if isinstance(period, dt.datetime):
                period = period.date()
            if isinstance(period, str):
                period = dt.date.fromisoformat(period[:10])
            if key in master and period:
                try:
                    raw_payload = json.loads(payload) if isinstance(payload, str) else payload
                except (TypeError, ValueError):
                    raw_payload = {}
                if not isinstance(raw_payload, dict):
                    raw_payload = {}
                revenue_by_code.setdefault(key, []).append(
                    (
                        period, published_at, _number(yoy),
                        parse_number(raw_payload.get("營業收入-當月營收")),
                        parse_number(raw_payload.get("營業收入-去年當月營收")),
                        parse_number(raw_payload.get("累計營業收入-前期比較增減(%)")),
                        str(raw_payload.get("備註") or ""),
                        str(source_key or ""), str(source_url or ""),
                    )
                )

    profitability_by_code: dict[
        str, list[tuple[dt.date, Any, float | None, Any, int, str]]
    ] = {}
    income_statement_by_code: dict[
        str, list[tuple[dt.date, Any, Any, int, str]]
    ] = {}
    balance_sheet_by_code: dict[
        str, list[tuple[dt.date, Any, Any, int, str]]
    ] = {}
    borrowing_by_code: dict[str, dict[str, list[tuple[dt.date, float | None]]]] = {}
    insider_by_code: dict[str, list[tuple[dt.date, Any, float | None]]] = {}
    insider_pledge_by_code: dict[
        str, list[tuple[str, dt.date, Any, float | None, float | None, int, int, str]]
    ] = {}
    shareholding_by_code: dict[str, dict[str, Any]] = {}
    foreign_ownership_by_code: dict[str, dict[str, Any]] = {}
    statutory_shareholding_shortfall_by_code: dict[str, dict[str, Any]] = {}
    dividends_by_code: dict[str, list[tuple[Any, dt.date | None, float | None]]] = {}
    market_index_by_code: dict[str, list[tuple[dt.date, float | None]]] = {}
    valuation_context_by_code: dict[str, dict[str, Any]] = {}
    corporate_action_by_code: dict[str, list[dict[str, Any]]] = {}
    margin_by_code: dict[str, list[tuple[dt.date, str]]] = {}
    regulatory_context_by_code: dict[str, list[dict[str, Any]]] = {}
    if as_of_date:
        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT source_key, entity_id, effective_date, value_text, source_url
            FROM market_context_snapshot
            WHERE event_type='corporate_action_preannouncement'
              AND entity_type='stock' AND quality_status='ok'
              AND effective_date>=DATE_SUB(%s, INTERVAL 45 DAY)
              AND effective_date<=DATE_ADD(%s, INTERVAL 30 DAY)
            ORDER BY entity_id, effective_date, source_key, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date),
        )
        expected_action_sources = {
            "twse_exright_preannouncement_daily": "上市",
            "tpex_exright_preannouncement_daily": "上櫃",
        }
        for source_key, code, period, value_text, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            effective_date = _date_only(period)
            if (
                key not in master or not effective_date
                or master[key]["market"] != expected_action_sources.get(source_key)
                or not value_text
            ):
                continue
            try:
                action_details = json.loads(str(value_text))
            except (TypeError, ValueError):
                continue
            if not isinstance(action_details, dict):
                continue
            corporate_action_by_code.setdefault(key, []).append(
                {
                    "effective_date": effective_date.isoformat(),
                    "action": str(action_details.get("action") or ""),
                    "cash_dividend_per_share": _number(
                        action_details.get("cash_dividend_per_share")
                    ),
                    "source_key": str(source_key),
                    "source_url": str(source_url or ""),
                }
            )

        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT source_key, entity_id, effective_date, value_text, source_url
            FROM market_context_snapshot
            WHERE event_type='valuation_metrics_daily' AND entity_type='stock'
              AND quality_status='ok' AND effective_date=%s
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY entity_id, source_key, effective_date, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, started_at),
        )
        for source_key, code, period, value_text, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key not in master or period != as_of_date or not value_text:
                continue
            expected_source = (
                "twse_valuation_metrics_daily"
                if master[key]["market"] == "上市"
                else "tpex_valuation_metrics_daily"
            )
            if source_key != expected_source:
                continue
            try:
                values = json.loads(str(value_text))
            except (TypeError, ValueError):
                continue
            if not isinstance(values, dict):
                continue
            metrics = {
                "pe_ratio": _number(values.get("pe_ratio")),
                "dividend_yield_pct": _number(values.get("dividend_yield_pct")),
                "price_to_book": _number(values.get("price_to_book")),
            }
            if not any(value is not None for value in metrics.values()):
                continue
            valuation_context_by_code[key] = {
                "effective_date": period.isoformat(),
                **metrics,
                "source_url": str(source_url or ""),
            }

        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT entity_id, effective_date, metric_value
            FROM market_context_snapshot
            WHERE event_type='market_index_daily' AND entity_type='macro'
              AND quality_status='ok' AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 90 DAY)
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY entity_id, effective_date, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for index_code, period, closing_index in source_cursor.fetchall():
            period = _date_only(period)
            if period:
                market_index_by_code.setdefault(str(index_code), []).append(
                    (period, _number(closing_index))
                )

        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT entity_id, effective_date, value_text
            FROM market_context_snapshot
            WHERE event_type='margin_balance_snapshot' AND entity_type='stock'
              AND quality_status='ok' AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 45 DAY)
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY entity_id, effective_date, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, period, value_text in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            market = str((master.get(key) or {}).get("market") or "")
            # Ignore legacy snapshots whose date was synthesized from collection time.
            if (
                key in master and period and value_text
                and is_observed_market_session(
                    period, market, price_session_dates_by_market
                )
            ):
                margin_by_code.setdefault(key, []).append((period, str(value_text)))

        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT entity_id, event_type, effective_date, title, value_text, source_url
            FROM market_context_snapshot
            WHERE event_type IN ('trading_attention_notice', 'trading_disposition')
              AND entity_type='stock' AND quality_status='ok'
              AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 45 DAY)
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY entity_id, effective_date, event_type, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, event_type, period, title, value_text, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key not in master or not period or not value_text:
                continue
            try:
                context_details = json.loads(str(value_text))
            except (TypeError, ValueError):
                continue
            if event_type == "trading_attention_notice":
                if period != as_of_date:
                    continue
            elif event_type == "trading_disposition":
                active_from = _date_only(context_details.get("start_date"))
                active_until = _date_only(context_details.get("end_date"))
                # A published future disposition is actionable risk context too.
                # effective_date is the announcement date; the details carry
                # the separate period when matching and displaying the notice.
                if not active_from or not active_until or as_of_date > active_until:
                    continue
            regulatory_context_by_code.setdefault(key, []).append(
                {
                    "event_type": event_type,
                    "effective_date": period.isoformat(),
                    "title": str(title or ""),
                    "details": context_details,
                    "source_url": str(source_url or ""),
                }
            )

        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT source_key, entity_id, effective_date, value_text, source_url
            FROM market_context_snapshot
            WHERE event_type='trading_status_snapshot' AND entity_type='stock'
              AND quality_status='ok' AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 7 DAY)
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY effective_date, updated_at, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff, today, today, started_at),
        )
        latest_status = {}
        for source_key, code, period, value_text, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            expected = ("twse_trading_status_daily" if master.get(key, {}).get("market") == "上市"
                        else "tpex_trading_status_daily")
            if key not in master or source_key != expected:
                continue
            try:
                status_details = json.loads(value_text or "{}")
            except (ValueError, TypeError):
                continue
            if isinstance(status_details, dict):
                latest_status[key] = {"event_type": "trading_status_snapshot",
                    "effective_date": str(period), "details": status_details,
                    "source_url": str(source_url or "")}
        for code, context in latest_status.items():
            regulatory_context_by_code.setdefault(code, []).insert(0, context)

        source_cursor.execute(
            QUARTERLY_FINANCIAL_ASOF_CTE + """
            SELECT entity_id, effective_date, event_time, metric_value, payload, id, source_url
            FROM quarterly_financial_snapshot
            WHERE event_type='quarterly_profitability' AND entity_type='stock'
              AND quality_status='ok' AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 24 MONTH)
              AND event_time IS NOT NULL AND event_time<=%s
            ORDER BY entity_id, effective_date, event_time, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for (
            code, period, published_at, margin, payload, row_id, source_url
        ) in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key in master and period:
                profitability_by_code.setdefault(key, []).append(
                    (
                        period, published_at, _number(margin), payload,
                        int(row_id), str(source_url or ""),
                    )
                )

        source_cursor.execute(
            QUARTERLY_FINANCIAL_ASOF_CTE + """
            SELECT entity_id, effective_date, event_time, payload, id, source_url
            FROM quarterly_financial_snapshot
            WHERE event_type='quarterly_income_statement' AND entity_type='stock'
              AND quality_status='ok' AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 24 MONTH)
              AND event_time IS NOT NULL AND event_time<=%s
            ORDER BY entity_id, effective_date, event_time, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, period, published_at, payload, row_id, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key in master and period:
                income_statement_by_code.setdefault(key, []).append(
                    (period, published_at, payload, int(row_id), str(source_url or ""))
                )

        source_cursor.execute(
            QUARTERLY_FINANCIAL_ASOF_CTE + """
            SELECT entity_id, effective_date, event_time, payload, id, source_url
            FROM quarterly_financial_snapshot
            WHERE event_type='quarterly_balance_sheet' AND entity_type='stock'
              AND quality_status='ok' AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 24 MONTH)
              AND event_time IS NOT NULL AND event_time<=%s
            ORDER BY entity_id, effective_date, event_time, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, period, published_at, payload, row_id, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key in master and period:
                balance_sheet_by_code.setdefault(key, []).append(
                    (period, published_at, payload, int(row_id), str(source_url or ""))
                )

        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT entity_id, event_type, effective_date, metric_value
            FROM market_context_snapshot
            WHERE event_type IN ('available_short_shares_snapshot', 'securities_borrowing_activity')
              AND entity_type='stock' AND quality_status='ok'
              AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 10 DAY)
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY entity_id, event_type, effective_date, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, event_type, period, amount in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key in master and period:
                borrowing_by_code.setdefault(key, {}).setdefault(event_type, []).append(
                    (period, _number(amount))
                )

        source_cursor.execute(
            INSIDER_HOLDINGS_ASOF_CTE + """
            SELECT entity_id, effective_date, MAX(published_at),
                   SUM(current_shares)
            FROM (
                SELECT entity_id, effective_date, MAX(event_time) AS published_at,
                       JSON_UNQUOTE(JSON_EXTRACT(payload, '$."姓名"')) AS person_name,
                       MAX(metric_value) AS current_shares,
                       COUNT(DISTINCT metric_value) AS share_value_versions
                FROM insider_holdings_snapshot
                WHERE event_type='director_holding_snapshot' AND entity_type='stock'
                  AND quality_status='ok' AND effective_date<=%s
                  AND effective_date>=DATE_SUB(%s, INTERVAL 18 MONTH)
                  AND event_time IS NOT NULL AND event_time<=%s
                  AND JSON_VALID(payload)
                GROUP BY entity_id, effective_date,
                         JSON_UNQUOTE(JSON_EXTRACT(payload, '$."姓名"'))
            ) per_person
            GROUP BY entity_id, effective_date
            HAVING SUM(share_value_versions > 1)=0
            ORDER BY entity_id, effective_date
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for code, period, published_at, current_shares in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key in master and period:
                insider_by_code.setdefault(key, []).append(
                    (period, published_at, _number(current_shares))
                )

        # Keep the official pledged-share aggregate separate from the insider
        # directional family. Deduplicate role rows by person, and retain a
        # company-month conflict count so one ambiguous person suppresses the
        # context instead of silently changing its denominator.
        source_cursor.execute(
            INSIDER_HOLDINGS_ASOF_CTE + """
            SELECT source_key, entity_id, effective_date, MAX(published_at),
                   SUM(current_shares), SUM(pledged_shares),
                   SUM(current_shares > 0),
                   SUM(current_versions <> 1 OR pledged_versions <> 1
                       OR current_shares IS NULL OR pledged_shares IS NULL
                       OR current_shares < 0 OR pledged_shares < 0
                       OR pledged_shares > current_shares),
                   MAX(source_url)
            FROM (
                SELECT source_key, entity_id, effective_date,
                       MAX(event_time) AS published_at,
                       JSON_UNQUOTE(JSON_EXTRACT(payload, '$.\"姓名\"')) AS person_name,
                       MAX(metric_value) AS current_shares,
                       COUNT(DISTINCT metric_value) AS current_versions,
                       MAX(CAST(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(
                           payload, '$.\"設質股數\"')), ',', '') AS DECIMAL(24,4)))
                           AS pledged_shares,
                       COUNT(DISTINCT CAST(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(
                           payload, '$.\"設質股數\"')), ',', '') AS DECIMAL(24,4)))
                           AS pledged_versions,
                       MAX(source_url) AS source_url
                FROM insider_holdings_snapshot
                WHERE source_key IN (
                        'twse_director_holdings_monthly',
                        'tpex_director_holdings_monthly'
                    )
                  AND event_type='director_holding_snapshot'
                  AND entity_type='stock' AND quality_status='ok'
                  AND effective_date<=%s
                  AND effective_date>=DATE_SUB(%s, INTERVAL 18 MONTH)
                  AND event_time IS NOT NULL AND event_time<=%s
                  AND JSON_VALID(payload)
                GROUP BY source_key, entity_id, effective_date,
                         JSON_UNQUOTE(JSON_EXTRACT(payload, '$.\"姓名\"'))
            ) per_person
            GROUP BY source_key, entity_id, effective_date
            ORDER BY source_key, entity_id, effective_date
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        for (
            source_key, code, period, published_at, current_shares,
            pledged_shares, holder_count, invalid_person_count, source_url,
        ) in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period)
            if key not in master or not period:
                continue
            expected_prefix = "twse" if master[key]["market"] == "上市" else "tpex"
            if not str(source_key).startswith(expected_prefix + "_"):
                continue
            insider_pledge_by_code.setdefault(key, []).append(
                (
                    str(source_key), period, published_at,
                    _number(current_shares), _number(pledged_shares),
                    int(holder_count or 0), int(invalid_person_count or 0),
                    str(source_url or ""),
                )
            )

        # TDCC publishes a full-market shareholding distribution snapshot.
        # The legacy stock table keeps its effective date and first/last load
        # time; only compare two recent, point-in-time-available snapshots.
        shareholding_floor = (as_of_date - dt.timedelta(days=45)).strftime("%Y%m%d")
        shareholding_ceiling = as_of_date.strftime("%Y%m%d")
        stock_cursor.execute(
            """
            SELECT TRIM(證券代號), 資料日期,
                   `>400張大股東持有百分比`, `>1000張大股東持有百分比`,
                   create_time
            FROM 大戶比例
            WHERE 資料日期 >= %s AND 資料日期 <= %s
              AND create_time <= %s
            ORDER BY 資料日期, 證券代號, create_time
            """,
            (shareholding_floor, shareholding_ceiling, started_at),
        )
        shareholding_versions: dict[
            tuple[str, dt.date], set[tuple[float | None, float | None]]
        ] = {}
        for code, period_value, over_400_pct, over_1000_pct, _created_at in stock_cursor.fetchall():
            key = _normalize_code(code)
            if key not in master:
                continue
            if isinstance(period_value, str):
                period_text = period_value.strip()
                if len(period_text) == 8 and period_text.isdigit():
                    try:
                        period = dt.datetime.strptime(period_text, "%Y%m%d").date()
                    except ValueError:
                        period = None
                else:
                    period = _date_only(period_text)
            else:
                period = _date_only(period_value)
            if period is None or period > as_of_date:
                continue
            shareholding_versions.setdefault((key, period), set()).add(
                (_number(over_400_pct), _number(over_1000_pct))
            )

        shareholding_periods = sorted(
            {period for _code, period in shareholding_versions}, reverse=True
        )
        if len(shareholding_periods) >= 2:
            current_period, prior_period = shareholding_periods[:2]
            snapshot_age = (as_of_date - current_period).days
            snapshot_gap = (current_period - prior_period).days
            current_values: dict[str, tuple[float, float]] = {}
            prior_values: dict[str, tuple[float, float]] = {}
            for (code, period), versions in shareholding_versions.items():
                if len(versions) != 1:
                    continue
                values = next(iter(versions))
                if (
                    values[0] is None or values[1] is None
                    or not 0 <= values[0] <= 100
                    or not 0 <= values[1] <= 100
                ):
                    continue
                destination = (
                    current_values if period == current_period
                    else prior_values if period == prior_period
                    else None
                )
                if destination is not None:
                    destination[code] = (values[0], values[1])
            master_codes_by_market = {
                market: {
                    code for code, info in master.items()
                    if info.get("market") == market
                }
                for market in ("上市", "上櫃")
            }
            snapshot_coverages = [
                len(expected_codes & set(snapshot_values)) / len(expected_codes)
                if expected_codes else 0
                for snapshot_values in (current_values, prior_values)
                for expected_codes in master_codes_by_market.values()
            ]
            minimum_snapshot_coverage = min(snapshot_coverages, default=0)
            if (
                0 <= snapshot_age <= TDCC_SHAREHOLDING_MAX_SNAPSHOT_AGE_DAYS
                and 0 < snapshot_gap <= TDCC_SHAREHOLDING_MAX_SNAPSHOT_GAP_DAYS
                and snapshot_coverages
                and all(
                    coverage >= TDCC_SHAREHOLDING_MIN_MARKET_COVERAGE
                    for coverage in snapshot_coverages
                )
            ):
                for code in set(current_values) & set(prior_values):
                    current_400, current_1000 = current_values[code]
                    prior_400, prior_1000 = prior_values[code]
                    change_400 = current_400 - prior_400
                    change_1000 = current_1000 - prior_1000
                    if (
                        abs(change_400) > TDCC_SHAREHOLDING_CONTEXT_CHANGE_PP
                        or abs(change_1000) > TDCC_SHAREHOLDING_CONTEXT_CHANGE_PP
                    ):
                        shareholding_by_code[code] = {
                            "effective_date": current_period.isoformat(),
                            "prior_effective_date": prior_period.isoformat(),
                            "minimum_snapshot_market_coverage_pct": round(
                                minimum_snapshot_coverage * 100, 2
                            ),
                            "change_400_lot_share_pp": round(change_400, 2),
                            "change_1000_lot_share_pp": round(change_1000, 2),
                            "source_url": TDCC_SHAREHOLDING_SOURCE_URL,
                        }

        # The licensed TPEx batch contains an effective-date snapshot but does
        # not publish an event timestamp. Use it only when its effective date
        # exactly matches the price scan date, and keep it as non-directional
        # context. Unmatched codes remain unknown and are never filled with 0.
        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT entity_id, effective_date, value_text, source_url
            FROM market_context_snapshot
            WHERE source_key=%s AND event_type='foreign_ownership_daily'
              AND entity_type='stock' AND quality_status='ok'
              AND effective_date=%s
            ORDER BY entity_id, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             "tpex_foreign_investor_holdings_daily", as_of_date),
        )
        for code, effective_date_value, value_text, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            effective_date = _date_only(effective_date_value)
            if (
                not key
                or key not in master
                or master[key].get("market") != "上櫃"
                or effective_date != as_of_date
            ):
                continue
            try:
                details = json.loads(value_text) if isinstance(value_text, str) else value_text
            except (TypeError, ValueError):
                continue
            if not isinstance(details, dict):
                continue
            holding_pct = _number(details.get("foreign_holding_ratio_pct"))
            held_shares = _number(details.get("foreign_held_shares"))
            issued_shares = _number(details.get("issued_shares"))
            if (
                holding_pct is None or not 0 <= holding_pct <= 100
                or held_shares is None or issued_shares is None or issued_shares <= 0
            ):
                continue
            foreign_ownership_by_code[key] = {
                "effective_date": effective_date.isoformat(),
                "rank": details.get("rank"),
                "foreign_held_shares": held_shares,
                "issued_shares": issued_shares,
                "foreign_holding_ratio_pct": holding_pct,
                "foreign_available_ratio_pct": _number(
                    details.get("foreign_available_ratio_pct")
                ),
                "legal_investment_cap_pct": _number(
                    details.get("legal_investment_cap_pct")
                ),
                "source_url": "https://data.gov.tw/dataset/20214",
                "endpoint_url": str(source_url or ""),
                "interpretation": "context_only_not_net_flow_or_directional_vote",
            }

        # The official director/supervisor shortfall endpoints are sparse
        # disclosure lists with irregular publication dates, not a full-market
        # compliance panel. Attach only an explicit, recent positive shortfall
        # to existing candidates; list absence remains unknown.
        source_cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT source_key, entity_id, effective_date, value_text, source_url
            FROM market_context_snapshot
            WHERE event_type='statutory_director_shareholding_shortfall'
              AND entity_type='stock' AND quality_status='ok'
              AND effective_date<=%s
              AND effective_date>=DATE_SUB(%s, INTERVAL 60 DAY)
              AND (event_time IS NULL OR event_time<=%s)
            ORDER BY entity_id, effective_date, source_key, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             as_of_date, as_of_date, started_at),
        )
        shortfall_observations: dict[str, dict[dt.date, list[dict[str, Any]]]] = {}
        shortfall_market_by_source = {
            "twse_director_shareholding_shortfall_daily": "上市",
            "tpex_director_shareholding_shortfall_daily": "上櫃",
        }
        for source_key, code, period_value, value_text, source_url in source_cursor.fetchall():
            key = _normalize_code(code)
            period = _date_only(period_value)
            if (
                not key or key not in master or not period
                or master[key].get("market") != shortfall_market_by_source.get(source_key)
            ):
                continue
            try:
                details = json.loads(value_text) if isinstance(value_text, str) else value_text
            except (TypeError, ValueError):
                continue
            if not isinstance(details, dict):
                continue
            issued_shares = _number(details.get("issued_shares"))
            director_shortfall = _number(details.get("director_shortfall_shares"))
            supervisor_shortfall = _number(details.get("supervisor_shortfall_shares"))
            if (
                issued_shares is None or issued_shares <= 0
                or not any(
                    amount is not None and amount > 0
                    for amount in (director_shortfall, supervisor_shortfall)
                )
            ):
                continue
            context = {
                "effective_date": period.isoformat(),
                "market": shortfall_market_by_source[source_key],
                "issued_shares": issued_shares,
                "director_required_shares": _number(
                    details.get("director_required_shares")
                ),
                "director_actual_shares": _number(
                    details.get("director_actual_shares")
                ),
                "director_shortfall_shares": director_shortfall,
                "supervisor_required_shares": _number(
                    details.get("supervisor_required_shares")
                ),
                "supervisor_actual_shares": _number(
                    details.get("supervisor_actual_shares")
                ),
                "supervisor_shortfall_shares": supervisor_shortfall,
                "notification_status": details.get("notification_status"),
                "source_url": str(source_url or ""),
                "dataset_url": (
                    "https://data.gov.tw/dataset/20767"
                    if source_key.startswith("twse_")
                    else "https://data.gov.tw/dataset/20768"
                ),
                "interpretation": "governance_risk_context_only_not_directional_score",
            }
            shortfall_observations.setdefault(key, {}).setdefault(period, []).append(context)
        for key, observations_by_date in shortfall_observations.items():
            latest_date = max(observations_by_date)
            same_date = observations_by_date[latest_date]
            fingerprints = {
                json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                for item in same_date
            }
            if len(fingerprints) == 1:
                statutory_shareholding_shortfall_by_code[key] = same_date[0]

        source_cursor.execute(
            DIVIDEND_ASOF_CTE + """
            SELECT entity_id, event_time, effective_date, metric_value
            FROM dividend_snapshot
            WHERE event_type='dividend' AND entity_type='stock'
              AND quality_status='ok' AND event_time IS NOT NULL
              AND event_time>=DATE_SUB(%s, INTERVAL 45 DAY)
              AND event_time<=%s
            ORDER BY entity_id, event_time, id
            """,
            (source_updated_at_cutoff, source_updated_at_cutoff,
             source_updated_at_cutoff, source_updated_at_cutoff,
             started_at, started_at),
        )
        for code, published_at, effective_date, cash_dividend in source_cursor.fetchall():
            key = _normalize_code(code)
            effective_date = _date_only(effective_date)
            if key in master:
                dividends_by_code.setdefault(key, []).append(
                    (published_at, effective_date, _number(cash_dividend))
                )

    latest_day_price_codes: set[str] = set()
    latest_price_codes: set[str] = set()
    invalid_close_codes: set[str] = set()
    no_trade_codes: set[str] = set()
    for code, rows in prices_by_code.items():
        latest_day_rows = [row for row in rows if as_of_date and row[0] == as_of_date]
        if not latest_day_rows:
            continue
        latest_day_price_codes.add(code)
        latest_closes = [row[1] for row in latest_day_rows]
        if (
            all(close is not None and close > 0 for close in latest_closes)
            and len(set(latest_closes)) == 1
        ):
            latest_price_codes.add(code)
        elif all(
            no_trade and (volume == 0 or volume is None)
            for _period, _close, volume, no_trade in latest_day_rows
        ):
            no_trade_codes.add(code)
        else:
            invalid_close_codes.add(code)
    price_age_days = (today - as_of_date).days if as_of_date else None
    fresh_price_data = bool(as_of_date and 0 <= price_age_days <= 7)
    fresh_flow_codes = {
        code for code, context in order_flow_by_code.items()
        if context["institutions"].get("same_day_usable")
    }
    fresh_flow_data = bool(fresh_flow_codes)
    fresh_revenue_codes = set()
    for code, rows in revenue_by_code.items():
        publish_dates = [_date_only(row[1]) for row in rows if row[1] is not None]
        publish_dates = [value for value in publish_dates if value is not None]
        latest_published = max(publish_dates, default=None)
        if latest_published and 0 <= (today - latest_published).days <= 45:
            fresh_revenue_codes.add(code)
    fresh_financial_codes = {
        code for code, rows in profitability_by_code.items()
        if rows and (published := _date_only(max(row[1] for row in rows if row[1] is not None)))
        and 0 <= (today - published).days <= 120
    }
    fresh_borrow_codes = {
        code for code, events in borrowing_by_code.items()
        if any(
            rows and 0 <= (as_of_date - max(row[0] for row in rows)).days <= 3
            for rows in events.values()
        )
    } if as_of_date else set()
    fresh_insider_codes = {
        code for code, rows in insider_by_code.items()
        if rows and (published := _date_only(max(row[1] for row in rows if row[1] is not None)))
        and 0 <= (today - published).days <= 90
    }
    insider_pledge_context_by_code: dict[str, dict[str, Any]] = {}
    for code, rows in insider_pledge_by_code.items():
        latest = max(rows, key=lambda row: (row[1], _date_only(row[2]) or dt.date.min))
        (
            _source_key, period, published_at, current_shares,
            pledged_shares, holder_count, invalid_person_count, source_url,
        ) = latest
        published_date = _date_only(published_at)
        if (
            invalid_person_count != 0
            or current_shares is None or current_shares <= 0
            or pledged_shares is None or pledged_shares < 0
            or pledged_shares > current_shares
            or holder_count <= 0
            or not 0 <= (as_of_date - period).days <= 90
            or published_date is None
            or not 0 <= (today - published_date).days <= 90
        ):
            continue
        insider_pledge_context_by_code[code] = {
            "period": period.isoformat(),
            "published_date": published_date.isoformat(),
            "pledged_ratio_pct": round(pledged_shares / current_shares * 100, 2),
            "holder_count": holder_count,
            "source_url": source_url,
            "basis": "direct_filers_only",
        }
    fresh_dividend_codes = {
        code for code, rows in dividends_by_code.items()
        if any(
            (published := _date_only(row[0]))
            and 0 <= (today - published).days <= 45
            for row in rows
        )
    }
    try:
        source_cursor.execute(
            "SELECT now_date FROM holiday.taiwan WHERE now_date BETWEEN %s AND %s",
            (as_of_date - dt.timedelta(days=60), as_of_date),
        )
        expected_sessions = trading_sessions(
            as_of_date, {_date_only(row[0]) for row in source_cursor.fetchall()}
        ) if as_of_date else []
    except Exception:
        expected_sessions = []
    benchmark_windows: dict[str, list[tuple[dt.date, float]]] = {}
    for index_code, index_rows in market_index_by_code.items():
        ordered = sorted(
            ((period, value) for period, value in index_rows if value is not None),
            key=lambda row: row[0],
        )
        index_counts = {period: sum(day == period for day, _ in ordered)
                        for period in expected_sessions[-6:]}
        index_panel = dict(ordered)
        recent_window = [(day, index_panel.get(day)) for day in expected_sessions[-6:]
                         if index_counts[day] == 1]
        if (
            len(recent_window) == 6
            and recent_window[-1][0] == as_of_date
            and all(value is not None and value > 0 for _, value in recent_window)
        ):
            benchmark_windows[index_code] = recent_window
    benchmark_complete = {"TAIEX", "TPEX"}.issubset(benchmark_windows)
    benchmark_alignment_by_market: dict[str, dict[str, Any]] = {}
    for market, index_code in (("上市", "TAIEX"), ("上櫃", "TPEX")):
        ordered = sorted(
            market_index_by_code.get(index_code, []), key=lambda row: row[0]
        )
        recent_window = ordered[-6:]
        latest_effective_date = ordered[-1][0] if ordered else None
        valid_effective_dates = [
            period for period, value in ordered
            if value is not None and value > 0
        ]
        benchmark_alignment_by_market[market] = {
            "index_code": index_code,
            "latest_effective_date": (
                latest_effective_date.isoformat() if latest_effective_date else None
            ),
            "latest_valid_effective_date": (
                max(valid_effective_dates).isoformat() if valid_effective_dates else None
            ),
            "aligned_to_scan_date": (
                latest_effective_date == as_of_date if latest_effective_date else None
            ),
            "window_point_count": len(recent_window) if recent_window else None,
            "window_valid_point_count": (
                sum(value is not None and value > 0 for _, value in recent_window)
                if recent_window else None
            ),
        }

    industry_universe_counts: dict[tuple[str, str], int] = {}
    industry_return_samples: dict[tuple[str, str], list[tuple[str, float]]] = {}
    for info in master.values():
        group = (info["market"], info["industry"])
        if group[0] in {"上市", "上櫃"} and group[1]:
            industry_universe_counts[group] = industry_universe_counts.get(group, 0) + 1
    for code, info in master.items():
        group = (info["market"], info["industry"])
        if group not in industry_universe_counts:
            continue
        index_code = "TAIEX" if info["market"] == "上市" else "TPEX"
        benchmark_window = benchmark_windows.get(index_code)
        rows = prices_by_code.get(code, [])
        if (
            code in invalid_close_codes
            or not benchmark_window or not rows or rows[-1][0] != as_of_date
        ):
            continue
        qualified_rows = quote_window(rows, expected_sessions, 6)
        close_by_date = {row[0]: row[1] for row in qualified_rows}
        aligned_closes = [close_by_date.get(period) for period, _ in benchmark_window]
        if not all(value is not None and value > 0 for value in aligned_closes):
            continue
        return_5d = (aligned_closes[-1] / aligned_closes[0] - 1) * 100
        industry_return_samples.setdefault(group, []).append((code, return_5d))

    industry_peer_context_by_code: dict[str, dict[str, Any]] = {}
    for group, group_returns in industry_return_samples.items():
        group_universe_count = industry_universe_counts[group]
        for code, _return_5d in group_returns:
            peer_returns = [
                value for peer_code, value in group_returns if peer_code != code
            ]
            peer_universe_count = group_universe_count - 1
            covered_count = len(peer_returns)
            coverage_pct = (
                covered_count / peer_universe_count * 100
                if peer_universe_count > 0 else 0
            )
            if (
                peer_universe_count < 10
                or covered_count < 10
                or coverage_pct < 70
            ):
                continue
            industry_peer_context_by_code[code] = {
                "median_return_5d_pct": statistics.median(peer_returns),
                "covered_count": covered_count,
                "universe_count": peer_universe_count,
                "coverage_pct": coverage_pct,
            }

    candidates: list[dict[str, Any]] = []
    evidence_snapshots: list[dict[str, Any]] = []

    family_trigger_counts: dict[str, int] = {}
    candidate_filter_counts = {"evaluated_stocks": 0, "below_three_directional_families": 0}

    if fresh_price_data:
        for code, info in master.items():
            rows = sorted(prices_by_code.get(code, []), key=lambda row: row[0])
            if not rows or rows[-1][0] != as_of_date:
                continue
            if code in invalid_close_codes:
                rows = [
                    (
                        period, None if period == as_of_date else close,
                        volume, no_trade,
                    )
                    for period, close, volume, no_trade in rows
                ]
            signal_values: dict[str, Any] = {}
            family_directions: dict[str, int] = {}
            family_data_status: dict[str, str] = {}
            price_context: dict[str, Any] = {}
            revenue_context: dict[str, Any] = {}
            profitability_context: dict[str, Any] = {}
            earnings_context: dict[str, Any] = {}
            balance_sheet_context: dict[str, Any] = {}
            contradictory_evidence: list[str] = []
            positioning_context: dict[str, Any] = {}
            revenue_direction = 0

            history_21 = quote_window(rows, expected_sessions, 21)
            history_10 = quote_window(rows, expected_sessions, 10)
            closes = [row[1] for row in history_21]
            volumes = [row[2] for row in history_21]
            price_context["calendar_verified"] = bool(expected_sessions)
            price_context["continuous_21_session_history"] = bool(history_21)
            price_context["stored_earlier_history_points"] = stored_history_points_by_code.get(code, 0)
            price_context["stored_history_availability_basis"] = "collection_time_not_immutable_vintage"
            action_dates = {_date_only(item.get("effective_date"))
                            for item in corporate_action_by_code.get(code, [])}
            recent_action = any(day in action_dates for day in expected_sessions[-21:])
            price_context["unadjusted_corporate_action_in_window"] = recent_action
            if recent_action:
                contradictory_evidence.append("近期有除權息事件，原始股價未還原；跨事件價量方向與高低價參考停用。")
                history_21, history_10, closes, volumes = [], [], [], []
            index_code = "TAIEX" if info["market"] == "上市" else "TPEX"
            benchmark_window = benchmark_windows.get(index_code)
            if benchmark_window:
                qualified_rows = quote_window(rows, expected_sessions, 6)
                close_by_date = {row[0]: row[1] for row in qualified_rows}
                aligned_closes = [close_by_date.get(period) for period, _ in benchmark_window]
                price_inputs_ready = (
                    len(aligned_closes) == len(benchmark_window)
                    and bool(aligned_closes)
                    and all(value is not None and value > 0 for value in aligned_closes)
                )
                if recent_action and price_inputs_ready:
                    family_data_status["price"] = "corporate_action_guard"
                elif price_inputs_ready:
                    family_data_status["price"] = "available"
                if not recent_action and price_inputs_ready:
                    return_5d = (aligned_closes[-1] / aligned_closes[0] - 1) * 100
                    benchmark_return = (
                        benchmark_window[-1][1] / benchmark_window[0][1] - 1
                    ) * 100
                    price_context["return_5d_pct"] = round(return_5d, 2)
                    price_context["latest_close"] = round(aligned_closes[-1], 2)
                    price_context["benchmark_code"] = index_code
                    price_context["benchmark_return_5d_pct"] = round(benchmark_return, 2)
                    price_context["relative_5d_return_pp"] = round(
                        return_5d - benchmark_return, 2
                    )
                    industry_peer = industry_peer_context_by_code.get(code)
                    if industry_peer:
                        industry_median = industry_peer["median_return_5d_pct"]
                        price_context.update(
                            {
                                "industry_peer_median_return_5d_pct": round(
                                    industry_median, 2
                                ),
                                "relative_5d_return_to_industry_pp": round(
                                    return_5d - industry_median, 2
                                ),
                                "industry_peer_covered_count": industry_peer[
                                    "covered_count"
                                ],
                                "industry_peer_universe_count": industry_peer[
                                    "universe_count"
                                ],
                                "industry_peer_coverage_pct": round(
                                    industry_peer["coverage_pct"], 1
                                ),
                            }
                        )
                    if (not recent_action and abs(return_5d) >= 8
                            and return_5d * (return_5d - benchmark_return) > 0):
                        signal_values["price_5d_return_pct"] = round(return_5d, 2)
                        family_directions["price"] = 1 if return_5d > 0 else -1
            family_data_status.setdefault("price", "unavailable")

            previous_20_closes = [value for value in closes[-21:-1] if value is not None]
            if len(previous_20_closes) == 20 and closes and closes[-1] is not None:
                previous_20d_close_high = max(previous_20_closes)
                price_context["previous_20d_close_high"] = round(previous_20d_close_high, 2)
                price_context["above_previous_20d_close_high"] = (
                    closes[-1] > previous_20d_close_high
                )
            trailing_10_closes = [row[1] for row in history_10]
            if len(trailing_10_closes) == 10:
                price_context["trailing_10d_close_low"] = round(min(trailing_10_closes), 2)

            volume_inputs_ready = bool(
                history_21 and volumes[-1] is not None and volumes[-1] > 0
                and all(value is not None and value > 0 for value in volumes[-21:-1])
            )
            family_data_status["volume"] = (
                "corporate_action_guard" if recent_action else
                "available" if volume_inputs_ready else "unavailable"
            )
            if volume_inputs_ready:
                prior_volumes = [value for value in volumes[-21:-1] if value is not None]
                average_volume = sum(prior_volumes) / len(prior_volumes) if prior_volumes else 0
                latest_return = (
                    (closes[-1] / closes[-2] - 1) * 100
                    if len(closes) >= 2 and closes[-1] and closes[-2]
                    else 0
                )
                volume_ratio = volumes[-1] / average_volume if average_volume > 0 else 0
                if volume_ratio >= 2 and abs(latest_return) >= 3:
                    signal_values["volume_expansion_ratio"] = round(volume_ratio, 2)
                    family_directions["volume"] = 1 if latest_return > 0 else -1

            order_flow_context = order_flow_by_code.get(code, {})
            for family, metric in (
                ("institutions", "institutional_net_intensity_pct"),
                ("broker_branches", "broker_concentration_pct"),
            ):
                context = order_flow_context.get(family, {})
                if context.get("direction"):
                    family_data_status[family] = "available"
                    family_directions[family] = context["direction"]
                    window = context["selected_window"]
                    signal_values[metric] = window[
                        "net_intensity_pct" if family == "institutions" else "concentration_pct"
                    ]
                elif context.get("status") == "different_windows_disagree":
                    family_data_status[family] = "conflict"
                    label = "法人買賣超" if family == "institutions" else "分點集中度"
                    contradictory_evidence.append(f"{label}短長視窗方向相反，本家族不投方向票。")
                elif context.get("selected_window"):
                    family_data_status[family] = "available"
                elif context.get("status") == "below_threshold" and context.get("windows"):
                    family_data_status[family] = "available"
                else:
                    family_data_status[family] = "unavailable"
            inst_window = (order_flow_context.get("institutions") or {}).get("selected_window")
            if inst_window and inst_window["foreign"] * inst_window["trust"] < 0:
                contradictory_evidence.append("外資與投信買賣方向不同；法人合計方向包含內部分歧。")

            revenue_rows = (
                sorted(revenue_by_code.get(code, []), key=lambda row: row[0])
                if code in fresh_revenue_codes else []
            )
            if revenue_rows:
                if not revenue_period_current(revenue_rows[-1][0], today):
                    revenue_rows = []
            if revenue_rows:
                latest_revenue_published_at = revenue_rows[-1][1]
                if not latest_revenue_published_at:
                    revenue_rows = []
                else:
                    published_date = _date_only(latest_revenue_published_at)
                    if not published_date or not 0 <= (today - published_date).days <= 45:
                        revenue_rows = []
            if revenue_rows:
                (
                    current_period, _published_at, current_yoy, current_amount,
                    _current_prior_year_amount, current_ytd_yoy,
                    current_revenue_note, current_source_key, current_source_url,
                ) = revenue_rows[-1]
                revenue_context = {
                    "period": current_period.strftime("%Y/%m"),
                    "revenue_yoy_pct": current_yoy,
                    "revenue_current_thousand_ntd": current_amount,
                    "revenue_ytd_yoy_pct": current_ytd_yoy,
                    "issuer_note": current_revenue_note,
                    "source_key": current_source_key,
                    "source_url": current_source_url,
                    **_month_over_month_revenue_context(revenue_rows),
                    **_trailing_three_month_revenue_context(revenue_rows),
                }
                # Monthly revenue remains a descriptive context field. It does
                # not add a directional family or help a stock pass the
                # candidate threshold without a point-in-time surprise model.

            if code in fresh_financial_codes:
                profitability_rows = profitability_by_code.get(code, [])
                quarterly_margins = _quarterly_operating_margin_series(
                    [row[:5] for row in profitability_rows]
                )
                latest_profitability = max(
                    (row for row in profitability_rows if row[1] is not None),
                    key=lambda row: (row[1], row[4]),
                    default=None,
                )
                if latest_profitability:
                    (
                        period, _published_at, reported_margin, payload,
                        _row_id, source_url,
                    ) = latest_profitability
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except (TypeError, ValueError):
                            payload = {}
                    try:
                        report_year = int(payload.get("__reporting_year"))
                        report_quarter = int(payload.get("__reporting_quarter"))
                    except (AttributeError, TypeError, ValueError):
                        report_year = report_quarter = 0
                    if (
                        1 <= report_quarter <= 4
                        and period.year == report_year
                        and (period.month - 1) // 3 + 1 == report_quarter
                        and reported_margin is not None
                    ):
                        report_key = (report_year, report_quarter)
                        standalone_margin = quarterly_margins.get(report_key)
                        display_margin = (
                            standalone_margin
                            if standalone_margin is not None
                            else reported_margin
                        )
                        basis = "單季" if standalone_margin is not None else "累計"
                        profitability_context = {
                            "period": f"{report_year}Q{report_quarter}",
                            "operating_margin_pct": round(display_margin, 2),
                            "basis": basis,
                            "source_url": source_url,
                        }
                        prior_year_margin = quarterly_margins.get(
                            (report_year - 1, report_quarter)
                        )
                        if (
                            standalone_margin is not None
                            and prior_year_margin is not None
                        ):
                            margin_yoy_change = (
                                standalone_margin - prior_year_margin
                            )
                            margin_yoy_change_display = round(
                                margin_yoy_change, 1
                            )
                            profitability_context.update(
                                {
                                    "prior_year_period": (
                                        f"{report_year - 1}Q{report_quarter}"
                                    ),
                                    "prior_year_operating_margin_pct": round(
                                        prior_year_margin, 2
                                    ),
                                    "operating_margin_yoy_change_pp": round(
                                        margin_yoy_change, 2
                                    ),
                                }
                            )
                            revenue_family_direction = family_directions.get(
                                "revenue", 0
                            )
                            if (
                                revenue_family_direction > 0
                                and margin_yoy_change_display < 0
                            ):
                                contradictory_evidence.append(
                                    "營收方向偏正，但最新單季營業利益率較去年同季下降 "
                                    f"{abs(margin_yoy_change_display):.1f} 個百分點；此為反向脈絡，"
                                    "不另計訊號家族"
                                )
                            elif (
                                revenue_family_direction < 0
                                and margin_yoy_change_display > 0
                            ):
                                contradictory_evidence.append(
                                    "營收方向偏弱，但最新單季營業利益率較去年同季上升 "
                                    f"{margin_yoy_change_display:.1f} 個百分點；此為反向脈絡，"
                                    "不另計訊號家族"
                                )
                if quarterly_margins:
                    current_key = max(quarterly_margins)
                    current_margin = quarterly_margins[current_key]
                    previous_key = (
                        (current_key[0], current_key[1] - 1)
                        if current_key[1] > 1
                        else (current_key[0] - 1, 4)
                    )
                    previous_margin = quarterly_margins.get(previous_key)
                else:
                    current_key = None
                    current_margin = None
                    previous_margin = None
                margin_vote = profitability_vote(quarterly_margins)
                family_data_status["profitability"] = (
                    "available" if margin_vote else "insufficient_history"
                )
                if margin_vote.get("direction"):
                    signal_values["operating_margin_pct"] = round(margin_vote["margin_pct"], 2)
                    signal_values["operating_margin_change_pp"] = round(margin_vote["change_pp"], 2)
                    signal_values["profitability_period"] = margin_vote["period"]
                    profitability_context["vote_comparison"] = margin_vote["comparison"]
                    family_directions["profitability"] = margin_vote["direction"]

            latest_income_statement = max(
                income_statement_by_code.get(code, []),
                key=lambda row: (row[0], row[1], row[3]),
                default=None,
            )
            if latest_income_statement:
                (
                    income_period, _published_at, income_payload,
                    _income_row_id, income_source_url,
                ) = latest_income_statement
                if isinstance(income_payload, str):
                    try:
                        income_payload = json.loads(income_payload)
                    except (TypeError, ValueError):
                        income_payload = {}
                try:
                    income_year = int(income_payload.get("__reporting_year"))
                    income_quarter = int(income_payload.get("__reporting_quarter"))
                except (AttributeError, TypeError, ValueError):
                    income_year = income_quarter = 0
                basic_eps = _number(income_payload.get("__basic_eps"))
                if (
                    basic_eps is not None
                    and 1 <= income_quarter <= 4
                    and income_period.year == income_year
                    and (income_period.month - 1) // 3 + 1 == income_quarter
                    and 0 <= (as_of_date - income_period).days <= 180
                ):
                    earnings_context = {
                        "period": f"{income_year}Q{income_quarter}",
                        "basic_eps_ntd_per_share": round(basic_eps, 2),
                        "source_url": income_source_url,
                    }
                    if basic_eps < 0 and any(
                        family_direction > 0
                        for family_direction in family_directions.values()
                    ):
                        contradictory_evidence.append(
                            f"最新官方財報 {income_year}Q{income_quarter} 基本EPS為"
                            f"{basic_eps:.2f}元；列作獲利反向脈絡，不另計訊號家族"
                        )

            # Balance-sheet ratios are context only. Financial firms have
            # materially different balance-sheet structures, so do not apply
            # generic liquidity/leverage readings to those industries.
            industry = str(info.get("industry") or "")
            financial_industry = any(
                term in industry
                for term in ("金融", "銀行", "保險", "證券", "期貨")
            )
            latest_balance_sheet = max(
                balance_sheet_by_code.get(code, []),
                key=lambda row: (row[0], row[1], row[3]),
                default=None,
            )
            if (
                latest_balance_sheet
                and not financial_industry
                and 0 <= (as_of_date - latest_balance_sheet[0]).days <= 180
            ):
                (
                    balance_period, _published_at, balance_payload,
                    _balance_row_id, balance_source_url,
                ) = latest_balance_sheet
                if isinstance(balance_payload, str):
                    try:
                        balance_payload = json.loads(balance_payload)
                    except (TypeError, ValueError):
                        balance_payload = {}
                try:
                    balance_year = int(balance_payload.get("__reporting_year"))
                    balance_quarter = int(balance_payload.get("__reporting_quarter"))
                except (AttributeError, TypeError, ValueError):
                    balance_year = balance_quarter = 0
                if (
                    1 <= balance_quarter <= 4
                    and balance_period.year == balance_year
                    and (balance_period.month - 1) // 3 + 1 == balance_quarter
                ):
                    current_assets = _number(
                        balance_payload.get("__current_assets")
                    )
                    current_liabilities = _number(
                        balance_payload.get("__current_liabilities")
                    )
                    total_liabilities = _number(
                        balance_payload.get("__total_liabilities")
                    )
                    total_equity = _number(
                        balance_payload.get("__total_equity")
                    )
                    current_ratio = (
                        current_assets / current_liabilities
                        if current_assets is not None
                        and current_liabilities is not None
                        and current_liabilities > 0
                        else None
                    )
                    liabilities_to_equity = (
                        total_liabilities / total_equity
                        if total_liabilities is not None
                        and total_equity is not None
                        and total_equity > 0
                        else None
                    )
                    if current_ratio is not None or liabilities_to_equity is not None:
                        balance_sheet_context = {
                            "period": f"{balance_year}Q{balance_quarter}",
                            "current_ratio": (
                                round(current_ratio, 2)
                                if current_ratio is not None else None
                            ),
                            "liabilities_to_equity": (
                                round(liabilities_to_equity, 2)
                                if liabilities_to_equity is not None else None
                            ),
                            "source_url": balance_source_url,
                        }

            margin_rows = sorted(margin_by_code.get(code, []), key=lambda row: row[0])
            if margin_rows:
                latest_period, latest_text = margin_rows[-1]
                try:
                    latest_margin = json.loads(latest_text)
                except (TypeError, ValueError):
                    latest_margin = {}
                if 0 <= (as_of_date - latest_period).days <= 3:
                    positioning_context["as_of_date"] = latest_period.isoformat()
                    for key in (
                        "margin_balance", "margin_previous_balance",
                        "margin_utilization_pct", "short_balance",
                        "short_previous_balance", "short_utilization_pct",
                    ):
                        number = _number(latest_margin.get(key))
                        if number is not None:
                            positioning_context[key] = number
                    market_sessions = sorted(
                        period
                        for period in price_session_dates_by_market.get(
                            info["market"], set()
                        )
                        if period <= latest_period
                    )
                    expected_five_session_window = market_sessions[-6:]
                    margin_by_period = {
                        period: value_text
                        for period, value_text in margin_rows
                        if period <= latest_period
                    }
                    if (
                        len(expected_five_session_window) == 6
                        and expected_five_session_window[-1] == latest_period
                        and all(
                            period in margin_by_period
                            for period in expected_five_session_window
                        )
                    ):
                        try:
                            baseline_margin = json.loads(
                                margin_by_period[expected_five_session_window[0]]
                            )
                        except (TypeError, ValueError):
                            baseline_margin = {}
                        current_balance = _number(latest_margin.get("margin_balance"))
                        prior_balance = _number(baseline_margin.get("margin_balance"))
                        if current_balance is not None and prior_balance and prior_balance > 0:
                            positioning_context["margin_balance_change_5d_pct"] = round(
                                (current_balance / prior_balance - 1) * 100, 2
                            )
                        current_short = _number(latest_margin.get("short_balance"))
                        prior_short = _number(baseline_margin.get("short_balance"))
                        if current_short is not None and prior_short and prior_short > 0:
                            positioning_context["short_balance_change_5d_pct"] = round(
                                (current_short / prior_short - 1) * 100, 2
                            )

            if code in fresh_borrow_codes:
                borrow_signals: list[tuple[str, float, int]] = []
                for event_type, rows in borrowing_by_code.get(code, {}).items():
                    ordered = sorted(rows, key=lambda row: row[0])
                    latest_period, current_amount = ordered[-1]
                    if (as_of_date - latest_period).days > 3:
                        continue
                    previous = next(
                        (row for row in reversed(ordered[:-1])
                         if row[0] < latest_period and row[1] is not None),
                        None,
                    )
                    if not previous or current_amount is None or previous[1] is None:
                        continue
                    difference = current_amount - previous[1]
                    if event_type == "available_short_shares_snapshot":
                        baseline = abs(previous[1])
                        change_pct = difference / baseline * 100 if baseline else 0
                        if abs(difference) >= 50_000 and abs(change_pct) >= 25:
                            borrow_signals.append((
                                "available_short_shares_change_pct", change_pct, 0
                            ))
                    else:
                        baseline = abs(previous[1])
                        change_pct = difference / baseline * 100 if baseline else 0
                        if abs(difference) >= 100_000 and abs(change_pct) >= 20:
                            # A rising borrow balance is an increase in short-side
                            # positioning; a falling balance can reflect covering.
                            borrow_signals.append((
                                "securities_borrowing_balance_change_pct",
                                change_pct,
                                -1 if difference > 0 else 1,
                            ))
                if borrow_signals:
                    for key, value, _direction in borrow_signals:
                        signal_values[key] = round(value, 2)

            if code in fresh_insider_codes:
                insider_rows = sorted(insider_by_code.get(code, []), key=lambda row: row[0])
                current_period, _published_at, current_shares = insider_rows[-1]
                previous_shares = next(
                    (row[2] for row in reversed(insider_rows[:-1])
                     if row[0] < current_period
                     and (current_period - row[0]).days <= 100
                     and row[2] is not None),
                    None,
                )
                if current_shares is not None and previous_shares and previous_shares > 0:
                    holdings_change = (current_shares / previous_shares - 1) * 100
                    if abs(holdings_change) >= 5:
                        signal_values["insider_holdings_change_pct"] = round(holdings_change, 2)
                        # Aggregate holdings can change through capital actions
                        # and director turnover. No purchase/sale identity proof.
                        contradictory_evidence.append(
                            "內部人彙總持股變化尚未校正股本與人員異動，僅作背景、不投方向票。"
                        )

            if code in shareholding_by_code:
                # Ownership-distribution movement is a sourced context item,
                # not an independent directional vote until it is calibrated.
                signal_values["tdcc_shareholding_distribution"] = shareholding_by_code[code]

            if code in foreign_ownership_by_code:
                signal_values["foreign_ownership_tpex_daily"] = foreign_ownership_by_code[code]

            if code in statutory_shareholding_shortfall_by_code:
                signal_values["statutory_director_shareholding_shortfall"] = (
                    statutory_shareholding_shortfall_by_code[code]
                )

            if code in fresh_dividend_codes:
                latest_dividend = max(
                    dividends_by_code.get(code, []), key=lambda row: _date_only(row[0]) or dt.date.min
                )
                signal_values["recent_dividend_announcement"] = 1.0
                if latest_dividend[2] is not None:
                    signal_values["cash_dividend_per_share"] = latest_dividend[2]

            family_directions = {
                family: direction for family, direction in family_directions.items()
                if direction != 0
            }
            candidate_filter_counts["evaluated_stocks"] += 1
            for family in family_directions:
                family_trigger_counts[family] = family_trigger_counts.get(family, 0) + 1
            group_balance = evidence_balance(family_directions)
            direction = group_balance["direction"]
            family_status = {}
            for family in (
                "price", "volume", "institutions", "broker_branches", "profitability",
            ):
                if family in family_directions:
                    family_status[family] = (
                        "directional_up" if family_directions[family] > 0
                        else "directional_down"
                    )
                else:
                    status = family_data_status.get(family, "unavailable")
                    family_status[family] = (
                        "neutral" if status == "available" else status
                    )

            candidate_selected = len(family_directions) >= 3
            evidence_direction = direction if family_directions else "訊號不足"
            evidence_hash = hashlib.sha256(
                f"evidence|{POLICY_VERSION}|{as_of_date}|{code}".encode("utf-8")
            ).hexdigest()
            evidence_snapshots.append(
                {
                    "candidate_hash": evidence_hash,
                    "entity_type": "stock_evidence",
                    "entity_id": code,
                    "security_name": info["name"],
                    "market": info["market"],
                    "industry": info["industry"],
                    "as_of_date": as_of_date,
                    "observed_at": started_at,
                    "signal_count": len(family_directions),
                    "signal_keys": ",".join(sorted(family_directions)),
                    "score": float(group_balance["aligned_groups"]),
                    "details": {
                        "direction": evidence_direction,
                        "candidate_selected": candidate_selected,
                        "candidate_rule": "minimum_three_directional_families",
                        "family_directions": family_directions,
                        "family_status": family_status,
                        "data_source_groups": _daily_data_source_groups(family_directions),
                        "evidence_balance": group_balance,
                        "policy_version": POLICY_VERSION,
                    },
                }
            )

            if not candidate_selected:
                candidate_filter_counts["below_three_directional_families"] += 1
                continue
            bullish_families = sum(value > 0 for value in family_directions.values())
            bearish_families = sum(value < 0 for value in family_directions.values())
            aligned_families = (bullish_families if direction == "偏多" else
                                bearish_families if direction == "偏空" else
                                max(bullish_families, bearish_families))
            signal_keys = sorted(signal_values)
            detail = {
                "direction": direction,
                "family_directions": family_directions,
                "family_status": family_status,
                "data_source_groups": _daily_data_source_groups(family_directions),
                "evidence_balance": {
                    **group_balance,
                    "bullish_families": bullish_families,
                    "bearish_families": bearish_families,
                    "aligned_families": aligned_families,
                    "directional_families": len(family_directions),
                },
                "signals": signal_values,
                "price_levels": price_context,
                "revenue_context": revenue_context,
                "profitability_context": profitability_context,
                "earnings_context": earnings_context,
                "balance_sheet_context": balance_sheet_context,
                "valuation_context": valuation_context_by_code.get(code),
                "upcoming_corporate_actions": [item for item in corporate_action_by_code.get(code, [])
                                               if _date_only(item["effective_date"]) > as_of_date][:3],
                "insider_pledge_context": insider_pledge_context_by_code.get(code),
                "positioning_context": positioning_context,
                "equity_order_flow_context": order_flow_context,
                "regulatory_context": regulatory_context_by_code.get(code, []),
                "contradictory_evidence": contradictory_evidence,
                "policy_version": POLICY_VERSION,
                "score_note": "方向與排序依同向資料群數；群內衝突不投票，分數不是報酬或上漲機率",
            }
            digest = hashlib.sha256(
                f"daily|{POLICY_VERSION}|{as_of_date}|{code}|{'|'.join(signal_keys)}".encode("utf-8")
            ).hexdigest()
            candidates.append(
                {
                    "candidate_hash": digest,
                    "entity_type": "stock",
                    "entity_id": code,
                    "security_name": info["name"],
                    "market": info["market"],
                    "industry": info["industry"],
                    "as_of_date": as_of_date,
                    "observed_at": started_at,
                    "signal_count": len(family_directions),
                    "signal_keys": ",".join(signal_keys),
                    "score": float(group_balance["aligned_groups"]),
                    "details": detail,
                }
            )

    evaluation_universe_count = candidate_filter_counts["evaluated_stocks"]
    for evidence in evidence_snapshots:
        evidence["details"]["evaluation_universe_count"] = evaluation_universe_count

    if as_of_date and candidates:
        financing_context = _load_material_convertible_bond_cash_context(
            source_cursor,
            codes={str(candidate["entity_id"]) for candidate in candidates},
            as_of_date=as_of_date,
            started_at=started_at,
            source_updated_at_cutoff=source_updated_at_cutoff,
        )
        for candidate in candidates:
            context = financing_context.get(str(candidate["entity_id"]))
            if context:
                candidate["details"]["official_financing_context"] = [context]

    candidates.sort(
        key=lambda item: (
            0 if item["details"]["direction"] == "偏多" else
            1 if item["details"]["direction"] == "訊號分歧" else 2,
            -item["score"], item["entity_id"],
        )
    )
    source_collection = _source_collection_state(
        source_cursor, source_dag_run_id, source_schedule_hint, source_dag_id
    )
    universe_market_counts = {
        market: sum(1 for info in master.values() if info["market"] == market)
        for market in ("上市", "上櫃")
    }
    market_coverage_by_market: dict[str, dict[str, Any]] = {}
    for market, universe_count in universe_market_counts.items():
        market_codes = {
            code for code, info in master.items() if info["market"] == market
        }
        latest_day_price_count = len(market_codes & latest_day_price_codes)
        price_count = len(market_codes & latest_price_codes)
        invalid_close_count = len(market_codes & invalid_close_codes)
        flow_count = len(market_codes & fresh_flow_codes)
        readiness = flow_readiness_by_market.get(market, {})
        traded_without_flow_count = readiness.get("traded_without_source_count")
        market_coverage_by_market[market] = {
            "universe_count": universe_count,
            "price_row_code_count": latest_day_price_count,
            "price_count": price_count,
            "price_invalid_close_code_count": invalid_close_count,
            "price_no_trade_code_count": len(market_codes & no_trade_codes),
            "flow_count": flow_count,
            "flow_readiness": readiness,
            "traded_without_flow_count": traded_without_flow_count,
            "price_coverage_pct": round(
                price_count / universe_count * 100 if universe_count else 0, 2
            ),
            "flow_coverage_pct": round(
                flow_count / universe_count * 100 if universe_count else 0, 2
            ),
        }
    market_coverage_issues: list[str] = []
    if not fresh_price_data:
        market_coverage_issues.append(
            f"收盤行情有效日 {as_of_date or '缺值'}，相對掃描日超出 0–7 日 freshness 範圍"
        )
    for market, values in market_coverage_by_market.items():
        universe_count = values["universe_count"]
        if universe_count <= 0:
            market_coverage_issues.append(f"{market}證券主檔分母為 0，無法核覆蓋")
            continue
        if values["price_count"] < universe_count * MIN_DAILY_MARKET_COVERAGE:
            market_coverage_issues.append(
                f"{market}收盤行情覆蓋 {values['price_count']}/{universe_count}，"
                f"低於 {MIN_DAILY_MARKET_COVERAGE:.0%}"
            )
        if values["price_invalid_close_code_count"]:
            market_coverage_issues.append(
                f"{market}當日有 {values['price_invalid_close_code_count']} 檔行情列的收盤缺值、"
                "非正或同日互異；未計入有效價格覆蓋，仍視為未知"
            )
    if not fresh_flow_data:
        market_coverage_issues.append(
            "本次沒有能與同日有效收盤及成交量對齊的法人列；"
            "請查各市場已收、可判讀與缺列數。未達買賣超門檻本身不屬資料缺失。"
        )
    else:
        for market, values in market_coverage_by_market.items():
            universe_count = values["universe_count"]
            if values["flow_count"] < universe_count * MIN_DAILY_MARKET_COVERAGE:
                market_coverage_issues.append(
                    f"{market}同日可判讀法人覆蓋 {values['flow_count']}/{universe_count}，"
                    f"低於 {MIN_DAILY_MARKET_COVERAGE:.0%}"
                )
    for market, values in market_coverage_by_market.items():
        missing_for_traded = int(values.get("traded_without_flow_count") or 0)
        if missing_for_traded:
            market_coverage_issues.append(
                f"{market}當日有有效收盤且成交量大於零，但已接入法人來源中未見同日資料 "
                f"{missing_for_traded} 檔；此缺列保持未知，不補成零買賣超"
            )
    price_market_coverage_complete = bool(master) and all(
        values["universe_count"] > 0
        and values["price_count"] >= values["universe_count"] * MIN_DAILY_MARKET_COVERAGE
        for values in market_coverage_by_market.values()
    )
    flow_market_coverage_complete = bool(master) and all(
        values["universe_count"] > 0
        and values["flow_count"] >= values["universe_count"] * MIN_DAILY_MARKET_COVERAGE
        for values in market_coverage_by_market.values()
    )
    market_status = (
        "fresh" if fresh_price_data and price_market_coverage_complete else "partial"
    )
    source_status = (
        "fresh" if market_status == "fresh" and fresh_flow_data
        and flow_market_coverage_complete
        and source_collection == "complete" and benchmark_complete
        else "partial"
    )
    return candidates, {
        "as_of_date": as_of_date,
        "source_status": source_status,
        "universe_count": len(master),
        "universe_market_counts": universe_market_counts,
        "market_coverage_by_market": market_coverage_by_market,
        "market_coverage_issues": market_coverage_issues,
        "price_row_code_count": len(latest_day_price_codes),
        "price_invalid_close_code_count": len(invalid_close_codes),
        "price_no_trade_code_count": len(no_trade_codes),
        "price_count": len(latest_price_codes),
        "flow_count": len(fresh_flow_codes),
        "revenue_count": len(fresh_revenue_codes),
        "financial_count": len(fresh_financial_codes),
        "borrow_count": len(fresh_borrow_codes),
        "insider_count": len(fresh_insider_codes),
        "dividend_count": len(fresh_dividend_codes),
        "benchmark_status": "fresh" if benchmark_complete else "partial",
        "benchmark_coverage": sorted(benchmark_windows),
        "benchmark_alignment_by_market": benchmark_alignment_by_market,
        "policy_version": POLICY_VERSION,
        "family_trigger_counts": family_trigger_counts,
        "candidate_filter_counts": candidate_filter_counts,
        "signal_evidence_snapshot_count": len(evidence_snapshots),
        "signal_evidence_snapshots": evidence_snapshots,
        "history_qualification": {
            "calendar_verified": bool(expected_sessions),
            "expected_21_session_dates": [day.isoformat() for day in expected_sessions[-21:]],
            "complete_21_session_quotes_by_market": {
                market: sum(bool(quote_window(prices_by_code.get(code, []), expected_sessions, 21))
                            for code, info in master.items() if info["market"] == market)
                for market in ("上市", "上櫃")
            },
            "stored_earlier_history_codes_by_market": {
                market: sum(code in stored_history_points_by_code for code, info in master.items()
                            if info["market"] == market) for market in ("上市", "上櫃")
            },
        },
        "source_collection_status": source_collection,
    }


def _attach_recent_news_context(
    candidates: list[dict[str, Any]], *, observed_at: dt.datetime,
) -> str:
    """Attach recent title or opening-text company mentions as non-directional context."""
    if not candidates:
        return "no_candidates"

    news_conn = news_cursor = news_engine = None
    try:
        news_conn, news_cursor, news_engine = _open_news_connection()
        cutoff = observed_at - dt.timedelta(hours=NEWS_CONTEXT_LOOKBACK_HOURS)
        # `created_at` is the source publication field and can be date-only or
        # refreshed when the batch collector rewrites an existing article.
        # Gate on the database ingestion timestamp before limiting the sample
        # so a concurrent news batch cannot add post-scan rows to this card.
        news_cursor.execute(
            f"""
            SELECT source, title, created_at, article_url, content_lead
            FROM (
                SELECT id, source, title, created_at, article_url,
                       LEFT(content, {NEWS_CONTEXT_BODY_LEAD_CHARS}) AS content_lead
                FROM news
                WHERE create_time <= %s
                  AND (
                        (created_at >= %s AND created_at <= %s)
                        OR (
                            TIME(created_at) = '00:00:00'
                            AND created_at >= %s AND created_at <= %s
                        )
                  )
                  AND (
                        (title IS NOT NULL AND TRIM(title) <> '')
                        OR (content IS NOT NULL AND TRIM(content) <> '')
                  )
                ORDER BY id DESC
                LIMIT %s
            ) recent_news
            ORDER BY created_at DESC, id DESC
            LIMIT %s
            """,
            (observed_at, cutoff, observed_at, cutoff.date(), observed_at,
             NEWS_CONTEXT_INGEST_SCAN_LIMIT,
             NEWS_CONTEXT_SAMPLE_LIMIT),
        )
        rows = news_cursor.fetchall()
        news_cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT id
                FROM news
                WHERE create_time <= %s
                  AND (
                        (created_at >= %s AND created_at <= %s)
                        OR (
                            TIME(created_at) = '00:00:00'
                            AND created_at >= %s AND created_at <= %s
                        )
                  )
                  AND (
                        (title IS NOT NULL AND TRIM(title) <> '')
                        OR (content IS NOT NULL AND TRIM(content) <> '')
                  )
                ORDER BY id DESC
                LIMIT %s
            ) recent_news
            """,
            (observed_at, cutoff, observed_at, cutoff.date(), observed_at,
             NEWS_CONTEXT_INGEST_SCAN_LIMIT),
        )
        eligible_count_row = news_cursor.fetchone()
        eligible_count = int(eligible_count_row[0] or 0) if eligible_count_row else 0
        sample_truncated = eligible_count > len(rows)
        indexed_rows = []
        for source, title, published_at, article_url, content_lead in rows:
            title_text = " ".join(str(title or "").split())
            content_text = " ".join(str(content_lead or "").split())
            if not title_text and not content_text:
                continue
            if isinstance(published_at, dt.date) and not isinstance(published_at, dt.datetime):
                published_at = dt.datetime.combine(published_at, dt.time.min)
            indexed_rows.append(
                (str(source or "未知來源"), title_text, published_at,
                 str(article_url or ""), content_text)
            )

        matched_any = False
        for candidate in candidates:
            name = " ".join(str(candidate.get("security_name") or "").split())
            code = _normalize_code(candidate.get("entity_id"))
            matches = []
            seen_urls: set[str] = set()
            seen_titles: set[str] = set()
            seen_title_sources: dict[str, set[str]] = {}

            def company_mention(text: str) -> bool:
                if len(name) >= 3:
                    return bool(name and name in text)
                code_pattern = rf"(?<!\d){re.escape(code)}(?!\d)"
                name_position = text.find(name) if name else -1
                code_match = re.search(code_pattern, text)
                return bool(
                    name_position >= 0 and code_match
                    and abs(code_match.start() - name_position) <= 20
                )

            def company_mention_excerpt(text: str) -> str:
                name_position = text.find(name) if name else -1
                if name_position < 0:
                    return ""
                mention_start = name_position
                mention_end = name_position + len(name)
                if len(name) < 3:
                    code_match = re.search(rf"(?<!\d){re.escape(code)}(?!\d)", text)
                    if not code_match or abs(code_match.start() - name_position) > 20:
                        return ""
                    mention_start = min(mention_start, code_match.start())
                    mention_end = max(mention_end, code_match.end())
                context_start = max(0, mention_start - 24)
                context_end = min(len(text), mention_end + 24)
                excerpt = " ".join(text[context_start:context_end].split())
                if context_start:
                    excerpt = "…" + excerpt
                if context_end < len(text):
                    excerpt += "…"
                return excerpt[:NEWS_CONTEXT_MATCH_EXCERPT_CHARS]

            for source, title, published_at, article_url, content_lead in indexed_rows:
                title_match = company_mention(title)
                body_match = company_mention(content_lead)
                if not title_match and not body_match:
                    continue
                if article_url and article_url in seen_urls:
                    continue
                title_key = title.casefold()
                missing_title = title.strip() in {"", "...", "…"}
                if not missing_title and title_key in seen_titles:
                    existing_match = next(
                        (
                            item for item in matches
                            if not item.get("title_missing")
                            and str(item.get("title") or "").casefold() == title_key
                        ),
                        None,
                    )
                    source_label = str(source or "未知來源").strip() or "未知來源"
                    source_key = source_label.casefold()
                    source_set = seen_title_sources.setdefault(title_key, set())
                    if source_key not in source_set:
                        source_set.add(source_key)
                        if existing_match:
                            existing_match["same_headline_source_count"] = len(source_set)
                            source_labels = existing_match.setdefault(
                                "same_headline_sources", []
                            )
                            if len(source_labels) < 5:
                                source_labels.append(source_label)
                    continue
                if len(matches) >= NEWS_CONTEXT_MATCH_LIMIT:
                    continue
                if article_url:
                    seen_urls.add(article_url)
                if not missing_title:
                    seen_titles.add(title_key)
                    source_label = str(source or "未知來源").strip() or "未知來源"
                    seen_title_sources[title_key] = {source_label.casefold()}
                else:
                    source_label = str(source or "未知來源").strip() or "未知來源"
                date_value = _date_only(published_at)
                source_key = str(source or "未知來源").strip().casefold()
                source_time_field = None
                if isinstance(published_at, dt.datetime):
                    date_only_timestamp = (
                        source_key in NEWS_DATE_ONLY_SOURCES
                        or published_at.time() == dt.time.min
                    )
                    publication_time_precision = (
                        "date_only" if date_only_timestamp else "datetime_field"
                    )
                    source_time_field = (
                        published_at.date().isoformat()
                        if date_only_timestamp
                        else published_at.isoformat(sep=" ", timespec="seconds")
                    )
                elif isinstance(published_at, dt.date):
                    publication_time_precision = "date_only"
                    source_time_field = published_at.isoformat()
                else:
                    publication_time_precision = "unknown"
                display_title = title
                if missing_title:
                    display_title = content_lead[:140].rstrip()
                    if len(content_lead) > 140:
                        display_title += "…"
                matches.append(
                    {
                        "source": source,
                        "published_date": date_value.isoformat() if date_value else "日期未提供",
                        "source_time_field": source_time_field,
                        "source_time_field_basis": "news.created_at",
                        "publication_time_precision": publication_time_precision,
                        "publication_time_verification": (
                            "unverified_against_article_page_and_timezone"
                        ),
                        "title": display_title,
                        "match_basis": "title" if title_match else "content_lead",
                        "match_excerpt": (
                            company_mention_excerpt(content_lead)
                            if not title_match and body_match else ""
                        ),
                        "title_missing": missing_title,
                        "same_headline_source_count": 1 if not missing_title else None,
                        "same_headline_sources": [source_label] if not missing_title else [],
                        "url": article_url,
                    }
                )
            candidate.setdefault("details", {})["news_context"] = {
                "status": "matched" if matches else "no_match_in_sample",
                "window_hours": NEWS_CONTEXT_LOOKBACK_HOURS,
                "ingest_scan_limit": NEWS_CONTEXT_INGEST_SCAN_LIMIT,
                "sample_limit": NEWS_CONTEXT_SAMPLE_LIMIT,
                "eligible_count": eligible_count,
                "sample_count": len(rows),
                "sample_truncated": sample_truncated,
                "matches": matches,
            }
            matched_any = matched_any or bool(matches)
        return "matched" if matched_any else "no_match_in_sample"
    except Exception as exc:
        print(f"[market signal] 新聞背景讀取失敗，保留原掃描 | {type(exc).__name__}")
        for candidate in candidates:
            candidate.setdefault("details", {})["news_context"] = {
                "status": "unavailable",
                "window_hours": NEWS_CONTEXT_LOOKBACK_HOURS,
                "ingest_scan_limit": NEWS_CONTEXT_INGEST_SCAN_LIMIT,
                "sample_limit": NEWS_CONTEXT_SAMPLE_LIMIT,
                "eligible_count": None,
                "sample_count": 0,
                "sample_truncated": False,
                "matches": [],
            }
        return "unavailable"
    finally:
        if news_conn:
            news_conn.close()
        if news_engine:
            news_engine.dispose()


OUTCOME_HORIZONS_SESSIONS = (1, 5, 10)
OUTCOME_LOOKBACK_DAYS = 120


def _settle_daily_candidate_outcomes(
    crawler_cursor, *, as_of_date: dt.date, settled_at: dt.datetime,
) -> dict[str, Any]:
    """Persist matured, market-relative outcomes for recent ready daily signals.

    signal_close is descriptive because daily notifications are sent after the
    close. next_close starts at the first session after the signal date and is
    the conservative entry proxy for an after-close alert.
    """

    lookback_start = as_of_date - dt.timedelta(days=OUTCOME_LOOKBACK_DAYS)
    settled_source_cutoff = _source_updated_at_cutoff_utc(settled_at)
    crawler_cursor.execute(
        """
        SELECT c.id, c.run_key, c.entity_id, c.market, r.source_status,
               c.as_of_date, c.details_text, o.outcome_basis,
               o.horizon_sessions, o.status
        FROM market_signal_candidates c
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        LEFT JOIN market_signal_outcomes o ON o.candidate_id=c.id
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.entity_type IN ('stock','stock_evidence')
          AND (
              c.entity_type='stock_evidence'
              OR NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key AND e.entity_type='stock_evidence'
              )
          )
          AND c.as_of_date>=%s
          AND c.as_of_date<=%s
          AND (
              SELECT COUNT(*) FROM market_signal_outcomes settled
              WHERE settled.candidate_id=c.id AND settled.status='settled'
          ) < 6
        ORDER BY c.as_of_date, c.id
        """,
        (lookback_start, as_of_date),
    )
    rows = crawler_cursor.fetchall()
    candidates: list[dict[str, Any]] = []
    code_set: set[str] = set()
    market_by_code: dict[str, str] = {}
    existing_settled: set[tuple[int, str, int]] = set()
    candidate_ids_seen: set[int] = set()
    for (candidate_id, run_key, entity_id, market, source_status,
         signal_date, details_text, outcome_basis, horizon, outcome_status) in rows:
        candidate_id = int(candidate_id)
        if outcome_status == "settled" and outcome_basis and horizon:
            existing_settled.add((candidate_id, str(outcome_basis), int(horizon)))
        if candidate_id in candidate_ids_seen:
            continue
        candidate_ids_seen.add(candidate_id)
        code = _normalize_code(entity_id)
        signal_date = _date_only(signal_date)
        if not code or not signal_date:
            continue
        try:
            details = json.loads(details_text or "{}")
        except (TypeError, ValueError):
            details = {}
        direction = str(details.get("direction") or "訊號分歧")
        candidates.append({
            "candidate_id": candidate_id,
            "run_key": str(run_key),
            "entity_id": code,
            "market": str(market or ""),
            "source_status": str(source_status or "unknown"),
            "signal_date": signal_date,
            "direction": direction,
        })
        code_set.add(code)
        market_by_code[code] = str(market or "")

    if not candidates:
        return {"reviewed_candidate_count": 0, "settled_count": 0,
                "pending_data_count": 0}

    official_quote_rows = _load_official_market_quote_rows(
        crawler_cursor,
        start_date=lookback_start,
        end_date=as_of_date,
        observed_at=settled_at,
        source_updated_at_cutoff=settled_source_cutoff,
        entity_ids=code_set,
    )
    trade_dates: set[dt.date] = set()
    stock_closes: dict[tuple[str, dt.date], float] = {}
    expected_quote_source = OFFICIAL_MARKET_QUOTE_SOURCE_BY_MARKET
    for (
        source_key, code, period, close, _volume, _no_trade,
        _opening, _highest, _lowest,
    ) in official_quote_rows:
        if source_key != expected_quote_source.get(market_by_code.get(code, "")):
            continue
        trade_dates.add(period)
        if close is not None and close > 0:
            stock_closes[(code, period)] = close

    crawler_cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT entity_id, effective_date, metric_value
        FROM market_context_snapshot
        WHERE event_type='market_index_daily' AND entity_type='macro'
          AND quality_status='ok' AND effective_date>=%s
          AND effective_date<=%s
          AND (event_time IS NULL OR event_time<=%s)
        ORDER BY id
        """,
        (settled_source_cutoff, settled_source_cutoff,
         settled_source_cutoff, settled_source_cutoff,
         lookback_start, as_of_date, settled_at),
    )
    benchmark_closes: dict[tuple[str, dt.date], float] = {}
    benchmark_dates: set[dt.date] = set()
    for code, effective_date, metric_value in crawler_cursor.fetchall():
        period = _date_only(effective_date)
        value = _number(metric_value)
        if period:
            benchmark_dates.add(period)
            if value is not None and value > 0:
                benchmark_closes[(str(code), period)] = value

    trade_dates = sorted(trade_dates | benchmark_dates)
    if not trade_dates:
        return {"reviewed_candidate_count": len(candidates), "settled_count": 0,
                "pending_data_count": 0, "status": "unavailable"}

    trade_date_positions = {period: index for index, period in enumerate(trade_dates)}
    latest_trade_date = trade_dates[-1]
    write_rows: list[tuple[Any, ...]] = []
    for candidate in candidates:
        signal_date = candidate["signal_date"]
        market = candidate["market"]
        benchmark_code = "TAIEX" if market == "上市" else "TPEX" if market == "上櫃" else ""
        direction = candidate["direction"]
        direction_sign = 1 if direction == "偏多" else -1 if direction == "偏空" else 0
        signal_position = trade_date_positions.get(signal_date)

        for basis in ("signal_close", "next_close"):
            if basis == "signal_close":
                entry_position = signal_position
            else:
                entry_position = next(
                    (index for index, period in enumerate(trade_dates)
                     if period > signal_date), None
                )
            if entry_position is None:
                continue

            for horizon in OUTCOME_HORIZONS_SESSIONS:
                if (
                    candidate["candidate_id"], basis, horizon
                ) in existing_settled:
                    continue
                exit_position = entry_position + horizon
                if exit_position >= len(trade_dates):
                    continue  # The holding period has not matured yet.
                entry_date = trade_dates[entry_position]
                exit_date = trade_dates[exit_position]
                if basis == "signal_close" and entry_date != signal_date:
                    stock_entry = None
                else:
                    stock_entry = stock_closes.get((candidate["entity_id"], entry_date))
                stock_exit = stock_closes.get((candidate["entity_id"], exit_date))
                index_entry = benchmark_closes.get((benchmark_code, entry_date)) if benchmark_code else None
                index_exit = benchmark_closes.get((benchmark_code, exit_date)) if benchmark_code else None
                status = "settled"
                reason = None
                stock_return = benchmark_return = excess = None
                hit = None
                if stock_entry is None or stock_exit is None:
                    status, reason = "pending_data", "stock_close_missing"
                elif index_entry is None or index_exit is None:
                    status, reason = "pending_data", "benchmark_close_missing"
                else:
                    stock_return = (stock_exit / stock_entry - 1) * 100
                    benchmark_return = (index_exit / index_entry - 1) * 100
                    excess = stock_return - benchmark_return
                    hit = int(excess > 0) if direction_sign > 0 else (
                        int(excess < 0) if direction_sign < 0 else None
                    )
                write_rows.append((
                    candidate["candidate_id"], candidate["run_key"],
                    candidate["entity_id"], market, candidate["source_status"],
                    signal_date, direction,
                    basis, horizon, entry_date, exit_date, stock_entry,
                    stock_exit, stock_return, benchmark_code, index_entry,
                    index_exit, benchmark_return, excess, hit, status, reason,
                    settled_at if status == "settled" else None,
                    settled_at, settled_at,
                    expected_quote_source.get(market, "unknown"),
                ))

    if write_rows:
        crawler_cursor.executemany(
            """
            INSERT INTO market_signal_outcomes (
                candidate_id, run_key, entity_id, market, source_status,
                signal_date, direction, outcome_basis, horizon_sessions, entry_date,
                exit_date, entry_close, exit_close, stock_return_pct,
                benchmark_code, benchmark_entry, benchmark_exit,
                benchmark_return_pct, excess_return_pp, directional_hit,
                status, error_reason, settled_at, created_at, updated_at,
                outcome_price_source_key
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
            ON DUPLICATE KEY UPDATE
                run_key=IF(status='pending_data', VALUES(run_key), run_key),
                entity_id=IF(status='pending_data', VALUES(entity_id), entity_id),
                market=IF(status='pending_data', VALUES(market), market),
                source_status=IF(status='pending_data', VALUES(source_status), source_status),
                outcome_price_source_key=IF(status='pending_data', VALUES(outcome_price_source_key), outcome_price_source_key),
                signal_date=IF(status='pending_data', VALUES(signal_date), signal_date),
                direction=IF(status='pending_data', VALUES(direction), direction),
                entry_date=IF(status='pending_data', VALUES(entry_date), entry_date),
                exit_date=IF(status='pending_data', VALUES(exit_date), exit_date),
                entry_close=IF(status='pending_data', VALUES(entry_close), entry_close),
                exit_close=IF(status='pending_data', VALUES(exit_close), exit_close),
                stock_return_pct=IF(status='pending_data', VALUES(stock_return_pct), stock_return_pct),
                benchmark_code=IF(status='pending_data', VALUES(benchmark_code), benchmark_code),
                benchmark_entry=IF(status='pending_data', VALUES(benchmark_entry), benchmark_entry),
                benchmark_exit=IF(status='pending_data', VALUES(benchmark_exit), benchmark_exit),
                benchmark_return_pct=IF(status='pending_data', VALUES(benchmark_return_pct), benchmark_return_pct),
                excess_return_pp=IF(status='pending_data', VALUES(excess_return_pp), excess_return_pp),
                directional_hit=IF(status='pending_data', VALUES(directional_hit), directional_hit),
                error_reason=IF(status='pending_data', VALUES(error_reason), error_reason),
                settled_at=IF(status='pending_data', VALUES(settled_at), settled_at),
                updated_at=IF(status='pending_data', VALUES(updated_at), updated_at),
                status=IF(status='pending_data', VALUES(status), status)
            """,
            write_rows,
        )

    settled_count = sum(row[20] == "settled" for row in write_rows)
    pending_count = sum(row[20] == "pending_data" for row in write_rows)
    return {
        "reviewed_candidate_count": len(candidates),
        "settled_count": settled_count,
        "pending_data_count": pending_count,
        "latest_trade_date": latest_trade_date.isoformat(),
    }


FOLLOWUP_LOOKBACK_DAYS = 400
FOLLOWUP_REVIEW_LOOKBACK_DAYS = 2000
PRICE_INDEX_SOURCE_BY_MARKET = {
    "上市": ("twse_market_index_daily", "TAIEX"),
    "上櫃": ("tpex_market_index_daily", "TPEX"),
}


def _settle_calendar_followups(
    crawler_cursor, *, as_of_date: dt.date, settled_at: dt.datetime,
) -> dict[str, Any]:
    """Refresh one-year follow-ups from point-in-time official daily snapshots."""

    lookback_start = as_of_date - dt.timedelta(days=FOLLOWUP_LOOKBACK_DAYS)
    source_cutoff = _source_updated_at_cutoff_utc(settled_at)
    crawler_cursor.execute(
        """
        SELECT c.id, c.run_key, c.entity_id, c.market, r.source_status,
               c.as_of_date, c.details_text, c.entity_type,
               COALESCE(d.confirmed, 0), COALESCE(d.failed, 0),
               COALESCE(d.pending, 0), COALESCE(d.unconfirmed, 0), d.notified_at
        FROM market_signal_candidates c
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        LEFT JOIN (
            SELECT ni.candidate_id,
                   MAX(CASE WHEN n.status='sent'
                                 AND n.platform_http_status BETWEEN 200 AND 299
                                 AND COALESCE(n.platform_receipt_id, '')<>''
                            THEN 1 ELSE 0 END) AS confirmed,
                   MAX(CASE WHEN n.status='failed' THEN 1 ELSE 0 END) AS failed,
                   MAX(CASE WHEN n.status='pending' THEN 1 ELSE 0 END) AS pending,
                   MAX(CASE WHEN n.status='sent'
                                 AND (n.platform_http_status IS NULL
                                      OR n.platform_http_status NOT BETWEEN 200 AND 299
                                      OR COALESCE(n.platform_receipt_id, '')='')
                            THEN 1 ELSE 0 END) AS unconfirmed,
                   MIN(CASE WHEN n.status='sent'
                                 AND n.platform_http_status BETWEEN 200 AND 299
                                 AND COALESCE(n.platform_receipt_id, '')<>''
                            THEN n.notified_at END) AS notified_at
            FROM market_signal_notification_candidates ni
            INNER JOIN market_signal_notifications n ON n.id=ni.notification_id
            GROUP BY ni.candidate_id
        ) d ON d.candidate_id=c.id
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.as_of_date>=%s AND c.as_of_date<=%s
          AND (
              (c.entity_type='stock_evidence'
               AND (CASE WHEN JSON_VALID(c.details_text)
                         THEN COALESCE(JSON_LENGTH(JSON_EXTRACT(
                             c.details_text, '$.family_directions'
                         )), 0) ELSE 0 END)>0)
              OR (c.entity_type='stock' AND NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key
                    AND e.entity_id=c.entity_id
                    AND e.entity_type='stock_evidence'
              ))
          )
          AND (
              NOT EXISTS (
                  SELECT 1 FROM market_signal_followups f
                  WHERE f.candidate_id=c.id
              )
              OR EXISTS (
                  SELECT 1 FROM market_signal_followups f
                  WHERE f.candidate_id=c.id
                    AND (
                        f.status='pending_data'
                        OR f.benchmark_status='pending_data'
                        OR f.execution_status='pending_data'
                        OR (f.status='awaiting_maturity' AND f.target_date<=%s)
                        OR (f.candidate_selected=1 AND f.delivery_status IN (
                            'unknown','pending','unlinked','delivery_unknown'
                        ))
                    )
              )
          )
        ORDER BY c.as_of_date, c.id
        """,
        (lookback_start, as_of_date, as_of_date),
    )
    rows = crawler_cursor.fetchall()
    candidates: list[dict[str, Any]] = []
    code_set: set[str] = set()
    market_by_code: dict[str, str] = {}
    for (
        candidate_id, run_key, entity_id, market, source_status, signal_date,
        details_text, entity_type, confirmed, failed, pending, unconfirmed,
        notified_at,
    ) in rows:
        code = _normalize_code(entity_id)
        signal_date = _date_only(signal_date)
        if not code or not signal_date:
            continue
        try:
            details = json.loads(details_text or "{}")
        except (TypeError, ValueError):
            details = {}
        if not isinstance(details, dict):
            details = {}
        selected = bool(
            details.get("candidate_selected") is True or entity_type == "stock"
        )
        if confirmed:
            delivery_status = "sent"
        elif failed:
            delivery_status = "failed"
            notified_at = None
        elif pending:
            delivery_status = "pending"
            notified_at = None
        elif unconfirmed:
            delivery_status = "delivery_unknown"
            notified_at = None
        elif not selected:
            delivery_status = "not_selected"
            notified_at = None
        else:
            delivery_status = "unlinked"
            notified_at = None
        raw_families = details.get("family_directions")
        family_directions = raw_families if isinstance(raw_families, dict) else {}
        raw_family_status = details.get("family_status")
        family_status = raw_family_status if isinstance(raw_family_status, dict) else {}
        try:
            evaluation_universe_count = int(details.get("evaluation_universe_count"))
        except (TypeError, ValueError):
            evaluation_universe_count = None
        candidates.append({
            "candidate_id": int(candidate_id),
            "run_key": str(run_key),
            "entity_id": code,
            "market": str(market or ""),
            "source_status": str(source_status or "unknown"),
            "signal_date": signal_date,
            "direction": str(details.get("direction") or "訊號不足"),
            "family_directions_json": json.dumps(
                family_directions, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ),
            "family_status_json": json.dumps(
                family_status, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ),
            "policy_version": str(details.get("policy_version") or "legacy_unknown"),
            "evaluation_universe_count": evaluation_universe_count,
            "candidate_selected": selected,
            "delivery_status": delivery_status,
            "notified_at": notified_at,
        })
        code_set.add(code)
        market_by_code[code] = str(market or "")

    if not candidates:
        return {
            "status": "no_due_followup_work",
            "reviewed_candidate_count": 0,
            "written_rows": 0,
            "settled_by_horizon": {},
            "pending_data_by_horizon": {},
        }

    oldest_signal_date = min(candidate["signal_date"] for candidate in candidates)
    quote_rows = _load_official_market_quote_rows(
        crawler_cursor,
        start_date=oldest_signal_date,
        end_date=as_of_date,
        observed_at=settled_at,
        source_updated_at_cutoff=source_cutoff,
        entity_ids=code_set,
    )
    expected_quote_source = OFFICIAL_MARKET_QUOTE_SOURCE_BY_MARKET
    closes: dict[tuple[str, dt.date], float] = {}
    opens: dict[tuple[str, dt.date], float] = {}
    highs: dict[tuple[str, dt.date], float] = {}
    lows: dict[tuple[str, dt.date], float] = {}
    for (
        source_key, code, period, close, _volume, _no_trade,
        opening, highest, lowest,
    ) in quote_rows:
        if source_key != expected_quote_source.get(market_by_code.get(code, "")):
            continue
        for values, value in (
            (closes, close), (opens, opening), (highs, highest), (lows, lowest),
        ):
            if value is not None and value > 0:
                values[(code, period)] = value

    crawler_cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT source_key, entity_id, event_type, effective_date, metric_value
        FROM market_context_snapshot
        WHERE event_type IN ('market_index_daily','market_total_return_index_daily')
          AND entity_type='macro' AND quality_status='ok'
          AND source_key IN (
              'twse_market_index_daily','tpex_market_index_daily',
              'twse_market_total_return_index_daily',
              'tpex_market_total_return_index_daily'
          )
          AND effective_date>=%s AND effective_date<=%s
          AND (event_time IS NULL OR event_time<=%s)
        ORDER BY effective_date, entity_id, id
        """,
        (
            source_cutoff, source_cutoff, source_cutoff, source_cutoff,
            oldest_signal_date, as_of_date, settled_at,
        ),
    )
    market_sessions: dict[str, set[dt.date]] = {"上市": set(), "上櫃": set()}
    price_index_values: dict[tuple[str, dt.date], float] = {}
    price_index_source_by_market = {
        market: values[0]
        for market, values in PRICE_INDEX_SOURCE_BY_MARKET.items()
    }
    index_code_to_market = {
        "TAIEX": "上市", "TPEX": "上櫃",
        "TAIEX_TR": "上市", "TPEX_TR": "上櫃",
    }
    for source_key, code, event_type, effective_date, metric_value in crawler_cursor.fetchall():
        period = _date_only(effective_date)
        market = index_code_to_market.get(str(code or ""))
        if not period or not market:
            continue
        if event_type in {"market_index_daily", "market_total_return_index_daily"}:
            market_sessions[market].add(period)
        if (
            event_type == "market_index_daily"
            and source_key == price_index_source_by_market.get(market)
        ):
            value = _number(metric_value)
            if value is not None and value > 0:
                price_index_values[(market, period)] = value

    latest_session_by_market = {
        market: max(sessions) if sessions else None
        for market, sessions in market_sessions.items()
    }
    sessions_sorted_by_market = {
        market: sorted(sessions) for market, sessions in market_sessions.items()
    }
    quote_ohlc_by_code: dict[str, list[tuple[dt.date, float, float]]] = {}
    for (quote_code, period), high in highs.items():
        low = lows.get((quote_code, period))
        if low is not None:
            quote_ohlc_by_code.setdefault(quote_code, []).append((period, high, low))
    quote_ohlc_dates_by_code: dict[str, list[dt.date]] = {}
    for quote_code, quote_bars in quote_ohlc_by_code.items():
        quote_bars.sort(key=lambda bar: bar[0])
        quote_ohlc_dates_by_code[quote_code] = [bar[0] for bar in quote_bars]
    quote_source_by_market = OFFICIAL_MARKET_QUOTE_SOURCE_BY_MARKET
    write_rows: list[tuple[Any, ...]] = []
    status_counts: dict[str, dict[str, int]] = {
        horizon: {
            "settled": 0,
            "awaiting_maturity": 0,
            "pending_data": 0,
            "benchmark_settled": 0,
            "benchmark_pending": 0,
            "execution_settled": 0,
            "execution_pending": 0,
        }
        for horizon in FOLLOWUP_HORIZONS
    }
    for candidate in candidates:
        code = candidate["entity_id"]
        market = candidate["market"]
        signal_date = candidate["signal_date"]
        sessions = sessions_sorted_by_market.get(market, [])
        latest_session = latest_session_by_market.get(market)
        benchmark = PRICE_INDEX_SOURCE_BY_MARKET.get(market)
        benchmark_code = benchmark[1] if benchmark else "unknown"
        benchmark_source_key = benchmark[0] if benchmark else "unknown"
        event_entry = closes.get((code, signal_date))
        delivery_date = (
            _date_only(candidate["notified_at"])
            if candidate["delivery_status"] == "sent" and candidate["notified_at"]
            else signal_date
        )
        execution_entry_date = first_session_on_or_after_sorted(
            delivery_date + dt.timedelta(days=1), sessions
        )
        execution_entry_open = (
            opens.get((code, execution_entry_date))
            if execution_entry_date else None
        )

        for horizon_key in FOLLOWUP_HORIZONS:
            target_date = followup_target_date(signal_date, horizon_key)
            exit_date = first_session_on_or_after_sorted(target_date, sessions)
            if exit_date is None:
                if latest_session and latest_session < target_date:
                    status, error_reason = "awaiting_maturity", None
                else:
                    status, error_reason = "pending_data", "market_session_after_target_missing"
                exit_close = None
            else:
                exit_close = closes.get((code, exit_date))
                if event_entry is None or exit_close is None:
                    status, error_reason = "pending_data", "stock_close_missing"
                else:
                    status, error_reason = "settled", None
            stock_return = percent_return(event_entry, exit_close)

            benchmark_entry = price_index_values.get((market, signal_date))
            benchmark_exit = (
                price_index_values.get((market, exit_date)) if exit_date else None
            )
            if exit_date is None and status == "awaiting_maturity":
                benchmark_status, benchmark_error = "awaiting_maturity", None
            elif benchmark_entry is None or benchmark_exit is None:
                benchmark_status = "pending_data"
                benchmark_error = "market_price_index_missing"
            else:
                benchmark_status, benchmark_error = "settled", None
            benchmark_return = percent_return(benchmark_entry, benchmark_exit)
            excess_return = (
                stock_return - benchmark_return
                if stock_return is not None and benchmark_return is not None
                else None
            )

            if exit_date is None and status == "awaiting_maturity":
                execution_status, execution_error = "awaiting_maturity", None
            elif execution_entry_date is None:
                if latest_session and latest_session <= delivery_date:
                    execution_status, execution_error = "awaiting_maturity", None
                else:
                    execution_status, execution_error = "pending_data", "entry_session_missing"
            elif execution_entry_open is None:
                execution_status, execution_error = "pending_data", "next_session_open_missing"
            elif exit_close is None:
                execution_status, execution_error = "pending_data", "execution_exit_close_missing"
            else:
                execution_status, execution_error = "settled", None
            execution_exit_close = exit_close if execution_status == "settled" else None
            execution_return = percent_return(
                execution_entry_open, execution_exit_close
            )

            high_excursions: list[float] = []
            low_excursions: list[float] = []
            if (
                execution_entry_date is not None and exit_date is not None
                and execution_entry_open is not None
            ):
                quote_bars = quote_ohlc_by_code.get(code, [])
                quote_dates = quote_ohlc_dates_by_code.get(code, [])
                first_bar = bisect_left(quote_dates, execution_entry_date)
                after_last_bar = bisect_right(quote_dates, exit_date)
                for _, high, low in quote_bars[first_bar:after_last_bar]:
                    high_excursion, low_excursion = directional_excursion_pct(
                        execution_entry_open, high, low, 1
                    )
                    if high_excursion is not None and low_excursion is not None:
                        high_excursions.append(high_excursion)
                        low_excursions.append(low_excursion)
            max_favorable_excursion = max(high_excursions) if high_excursions else None
            max_adverse_excursion = min(low_excursions) if low_excursions else None
            status_counts[horizon_key][status] += 1
            status_counts[horizon_key][
                "benchmark_settled" if benchmark_status == "settled"
                else "benchmark_pending"
            ] += 1
            status_counts[horizon_key][
                "execution_settled" if execution_status == "settled"
                else "execution_pending"
            ] += 1
            write_rows.append((
                candidate["candidate_id"], candidate["run_key"], code, market,
                candidate["source_status"], signal_date, candidate["direction"],
                candidate["family_directions_json"], candidate["family_status_json"],
                candidate["policy_version"], candidate["evaluation_universe_count"],
                int(candidate["candidate_selected"]), candidate["delivery_status"],
                candidate["notified_at"], horizon_key, target_date,
                event_entry, exit_close, stock_return,
                quote_source_by_market.get(market, "unknown"), benchmark_code,
                benchmark_entry, benchmark_exit, benchmark_return, excess_return,
                benchmark_source_key, execution_entry_date, execution_entry_open,
                execution_exit_close, execution_return,
                max_favorable_excursion, max_adverse_excursion,
                status, benchmark_status, execution_status, error_reason,
                benchmark_error, execution_error,
                settled_at if status == "settled" else None, settled_at, settled_at,
            ))

    if write_rows:
        crawler_cursor.executemany(
            """
            INSERT INTO market_signal_followups (
                candidate_id, run_key, entity_id, market, source_status,
                signal_date, direction, family_directions_json, family_status_json,
                policy_version, evaluation_universe_count, candidate_selected,
                delivery_status, notified_at, horizon_key, target_date,
                event_entry_close, event_exit_close, stock_return_pct,
                quote_source_key, benchmark_code, benchmark_entry, benchmark_exit,
                benchmark_return_pct, excess_return_pp, benchmark_source_key,
                execution_entry_date, execution_entry_open, execution_exit_close,
                execution_return_pct, max_favorable_excursion_pct,
                max_adverse_excursion_pct, status, benchmark_status,
                execution_status, error_reason, benchmark_error_reason,
                execution_error_reason, settled_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                delivery_status=VALUES(delivery_status),
                notified_at=VALUES(notified_at),
                event_entry_close=IF(status='settled', event_entry_close, VALUES(event_entry_close)),
                event_exit_close=IF(status='settled', event_exit_close, VALUES(event_exit_close)),
                stock_return_pct=IF(status='settled', stock_return_pct, VALUES(stock_return_pct)),
                quote_source_key=IF(status='settled', quote_source_key, VALUES(quote_source_key)),
                benchmark_entry=IF(benchmark_status='settled', benchmark_entry, VALUES(benchmark_entry)),
                benchmark_exit=IF(benchmark_status='settled', benchmark_exit, VALUES(benchmark_exit)),
                benchmark_return_pct=IF(benchmark_status='settled', benchmark_return_pct, VALUES(benchmark_return_pct)),
                excess_return_pp=IF(benchmark_status='settled', excess_return_pp, VALUES(excess_return_pp)),
                benchmark_source_key=IF(benchmark_status='settled', benchmark_source_key, VALUES(benchmark_source_key)),
                execution_entry_date=IF(execution_status='settled', execution_entry_date, VALUES(execution_entry_date)),
                execution_entry_open=IF(execution_status='settled', execution_entry_open, VALUES(execution_entry_open)),
                execution_exit_close=IF(execution_status='settled', execution_exit_close, VALUES(execution_exit_close)),
                execution_return_pct=IF(execution_status='settled', execution_return_pct, VALUES(execution_return_pct)),
                max_favorable_excursion_pct=IF(execution_status='settled', max_favorable_excursion_pct, VALUES(max_favorable_excursion_pct)),
                max_adverse_excursion_pct=IF(execution_status='settled', max_adverse_excursion_pct, VALUES(max_adverse_excursion_pct)),
                benchmark_error_reason=IF(benchmark_status='settled', benchmark_error_reason, VALUES(benchmark_error_reason)),
                execution_error_reason=IF(execution_status='settled', execution_error_reason, VALUES(execution_error_reason)),
                settled_at=IF(status='settled', settled_at, VALUES(settled_at)),
                error_reason=IF(status='settled', error_reason, VALUES(error_reason)),
                benchmark_status=IF(benchmark_status='settled', benchmark_status, VALUES(benchmark_status)),
                execution_status=IF(execution_status='settled', execution_status, VALUES(execution_status)),
                status=IF(status='settled', status, VALUES(status)),
                updated_at=VALUES(updated_at)
            """,
            write_rows,
        )
    return {
        "status": "ready",
        "reviewed_candidate_count": len(candidates),
        "written_rows": len(write_rows),
        "settled_by_horizon": {
            horizon: values["settled"] for horizon, values in status_counts.items()
        },
        "awaiting_maturity_by_horizon": {
            horizon: values["awaiting_maturity"] for horizon, values in status_counts.items()
        },
        "pending_data_by_horizon": {
            horizon: values["pending_data"] for horizon, values in status_counts.items()
        },
        "benchmark_settled_by_horizon": {
            horizon: values["benchmark_settled"]
            for horizon, values in status_counts.items()
        },
        "benchmark_pending_by_horizon": {
            horizon: values["benchmark_pending"]
            for horizon, values in status_counts.items()
        },
        "execution_settled_by_horizon": {
            horizon: values["execution_settled"]
            for horizon, values in status_counts.items()
        },
        "execution_pending_by_horizon": {
            horizon: values["execution_pending"]
            for horizon, values in status_counts.items()
        },
        "latest_market_session_by_market": {
            market: value.isoformat() if value else None
            for market, value in latest_session_by_market.items()
        },
    }


def _write_horizon_signal_reviews(
    crawler_cursor, *, as_of_date: dt.date, reviewed_at: dt.datetime,
) -> dict[str, Any]:
    """Persist date-bounded horizon summaries and explicit evidence gates."""

    lookback_start = as_of_date - dt.timedelta(
        days=FOLLOWUP_REVIEW_LOOKBACK_DAYS
    )
    crawler_cursor.execute(
        """
        SELECT c.id, c.as_of_date
        FROM market_signal_candidates c
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.as_of_date>=%s AND c.as_of_date<=%s
          AND (
              (c.entity_type='stock_evidence'
               AND (CASE WHEN JSON_VALID(c.details_text)
                         THEN COALESCE(JSON_LENGTH(JSON_EXTRACT(
                             c.details_text, '$.family_directions'
                         )), 0) ELSE 0 END)>0)
              OR (c.entity_type='stock' AND NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key AND e.entity_id=c.entity_id
                    AND e.entity_type='stock_evidence'
              ))
          )
        ORDER BY c.as_of_date, c.id
        """,
        (lookback_start, as_of_date),
    )
    expected_candidates = [
        (_date_only(candidate_id_or_date[1]), int(candidate_id_or_date[0]))
        for candidate_id_or_date in crawler_cursor.fetchall()
        if _date_only(candidate_id_or_date[1]) is not None
    ]
    expected_by_horizon: dict[str, set[int]] = {
        horizon: set() for horizon in FOLLOWUP_HORIZONS
    }
    for signal_date, candidate_id in expected_candidates:
        for horizon in FOLLOWUP_HORIZONS:
            if followup_target_date(signal_date, horizon) <= as_of_date:
                expected_by_horizon[horizon].add(candidate_id)

    crawler_cursor.execute(
        """
        SELECT candidate_id, horizon_key, target_date, signal_date, entity_id,
               market, source_status, direction, family_directions_json,
               family_status_json, policy_version, evaluation_universe_count,
               candidate_selected, delivery_status, status, execution_status,
               execution_error_reason, benchmark_status, benchmark_error_reason,
               error_reason, stock_return_pct, execution_return_pct,
               excess_return_pp, max_favorable_excursion_pct,
               max_adverse_excursion_pct, quote_source_key, benchmark_code,
               benchmark_source_key
        FROM market_signal_followups
        WHERE signal_date>=%s AND signal_date<=%s AND target_date<=%s
        ORDER BY horizon_key, signal_date, entity_id, candidate_id
        """,
        (lookback_start, as_of_date, as_of_date),
    )
    columns = [column[0] for column in crawler_cursor.description]
    due_rows = [dict(zip(columns, row)) for row in crawler_cursor.fetchall()]
    rows_by_horizon: dict[str, list[dict[str, Any]]] = {
        horizon: [] for horizon in FOLLOWUP_HORIZONS
    }
    for row in due_rows:
        horizon = str(row.get("horizon_key") or "")
        if horizon in rows_by_horizon:
            rows_by_horizon[horizon].append(row)

    status_by_horizon: dict[str, str] = {}
    due_counts: dict[str, int] = {}

    def _count_values(rows: list[dict[str, Any]], field: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in rows:
            value = str(row.get(field) or "unknown")
            counts[value] = counts.get(value, 0) + 1
        return dict(sorted(counts.items()))

    for horizon in FOLLOWUP_HORIZONS:
        horizon_rows = rows_by_horizon[horizon]
        expected_ids = expected_by_horizon[horizon]
        tracked_ids = {int(row["candidate_id"]) for row in horizon_rows}
        missing_ids = expected_ids - tracked_ids
        outcomes = summarize_followup_rows(horizon_rows)
        groups = outcomes.get("groups") or []
        all_mature = all(
            row.get("status") == "settled"
            and row.get("execution_status") == "settled"
            and row.get("benchmark_status") == "settled"
            for row in horizon_rows
        )
        all_sources_complete = all(
            row.get("source_status") == "fresh" for row in horizon_rows
        )
        selected_rows = [
            row for row in horizon_rows if bool(row.get("candidate_selected"))
        ]
        selected_receipts_confirmed = bool(selected_rows) and all(
            row.get("delivery_status") == "sent"
            for row in selected_rows
        )
        if not expected_ids:
            review_status = "not_due"
        elif missing_ids:
            review_status = "tracking_gap"
        elif not all_mature:
            review_status = "outcome_data_incomplete"
        elif not groups:
            review_status = "no_directional_observations"
        elif not all_sources_complete or not selected_receipts_confirmed:
            review_status = "input_quality_limited"
        else:
            review_status = "descriptive_only"

        for group in groups:
            ci = group.get("execution_proxy_ci_95") or {}
            if group.get("source_status") != "fresh":
                group["review_gate"] = "source_coverage_incomplete"
            elif (
                group.get("execution_pending_rows", 0)
                or group.get("benchmark_pending_rows", 0)
            ):
                group["review_gate"] = "outcome_data_incomplete"
            elif ci.get("low_pct") is None:
                group["review_gate"] = "insufficient_independent_signal_dates"
            elif group.get("scope") == "family_counterfactual":
                if (
                    ci["low_pct"] > 0
                    and (group.get("mean_directional_execution_proxy_pct") or 0) > 0
                    and (group.get("mean_directional_market_excess_pp") or 0) > 0
                ):
                    group["review_gate"] = (
                        "exploratory_counterfactual_positive_requires_cost_and_holdout"
                    )
                elif ci.get("high_pct") is not None and ci["high_pct"] < 0:
                    group["review_gate"] = "counterfactual_negative_in_current_sample"
                else:
                    group["review_gate"] = "counterfactual_no_clear_positive_edge"
            elif group.get("delivery_status") != "sent":
                group["review_gate"] = "platform_receipt_not_confirmed"
            elif (
                ci.get("low_pct") > 0
                and (group.get("mean_directional_execution_proxy_pct") or 0) > 0
                and (group.get("mean_directional_market_excess_pp") or 0) > 0
            ):
                group["review_gate"] = (
                    "candidate_for_cost_model_and_untouched_holdout_review"
                )
            elif ci.get("high_pct") is not None and ci["high_pct"] < 0:
                group["review_gate"] = "negative_in_current_descriptive_sample"
            else:
                group["review_gate"] = "no_clear_positive_edge_in_current_sample"

        expected_count = len(expected_ids)
        tracked_count = len(tracked_ids & expected_ids)
        report = {
            "report_version": 1,
            "as_of_date": as_of_date.isoformat(),
            "lookback_start": lookback_start.isoformat(),
            "lookback_days": FOLLOWUP_REVIEW_LOOKBACK_DAYS,
            "horizon_key": horizon,
            "target_basis": "calendar target, then first available official market session",
            "expected_due_candidate_count": expected_count,
            "tracked_due_candidate_count": tracked_count,
            "untracked_due_candidate_count": len(missing_ids),
            "tracking_coverage_pct": round(
                tracked_count / expected_count * 100, 4
            ) if expected_count else None,
            "due_outcome_row_count": len(horizon_rows),
            "settled_rows": outcomes.get("settled_row_count", 0),
            "pending_rows": outcomes.get("pending_row_count", 0),
            "directional_group_count": len(groups),
            "source_complete_due_rows": sum(
                row.get("source_status") == "fresh" for row in horizon_rows
            ),
            "source_status_counts": _count_values(horizon_rows, "source_status"),
            "delivery_status_counts": _count_values(horizon_rows, "delivery_status"),
            "outcome_status_counts": _count_values(horizon_rows, "status"),
            "execution_status_counts": _count_values(horizon_rows, "execution_status"),
            "benchmark_status_counts": _count_values(horizon_rows, "benchmark_status"),
            "quote_source_counts": _count_values(horizon_rows, "quote_source_key"),
            "benchmark_source_counts": _count_values(
                horizon_rows, "benchmark_source_key"
            ),
            "confirmed_delivery_receipt_rows": sum(
                row.get("delivery_status") == "sent" for row in horizon_rows
            ),
            "selected_due_candidate_count": len(selected_rows),
            "selected_confirmed_receipt_count": sum(
                row.get("delivery_status") == "sent" for row in selected_rows
            ),
            "review_status": review_status,
            "groups": groups,
            "policy_decision": "no automatic policy changes",
            "promotion_requirements": [
                "complete candidate tracking and point-in-time source coverage",
                "confirmed delivery receipt for the evaluated selected candidates",
                "date-block uncertainty interval that clears zero after a documented cost model",
                "an untouched chronological holdout and multiple-testing correction",
            ],
            "limitations": [
                "Returns use official unadjusted prices and do not include dividends or corporate actions.",
                "Next-session open is an execution proxy, not a guaranteed fill; fees, tax, slippage, borrow and sizing are not modeled.",
                "A platform receipt does not prove the message was read or traded.",
                "Family and threshold groups are exploratory counterfactuals and must not be promoted from the same sample used to discover them.",
            ],
        }
        crawler_cursor.execute(
            """
            INSERT INTO market_signal_horizon_reviews (
                as_of_date, horizon_key, review_status, report_json, reviewed_at
            ) VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                as_of_date=VALUES(as_of_date),
                review_status=VALUES(review_status),
                report_json=VALUES(report_json),
                reviewed_at=VALUES(reviewed_at)
            """,
            (
                as_of_date, horizon, review_status,
                json.dumps(report, ensure_ascii=False, default=str,
                           separators=(",", ":")),
                reviewed_at,
            ),
        )
        status_by_horizon[horizon] = review_status
        due_counts[horizon] = expected_count

    return {
        "status": "ready",
        "as_of_date": as_of_date.isoformat(),
        "review_status_by_horizon": status_by_horizon,
        "expected_due_candidates_by_horizon": due_counts,
    }


def _write_due_annual_signal_reviews(
    crawler_cursor, *, as_of_date: dt.date, reviewed_at: dt.datetime,
) -> dict[str, Any]:
    """Persist a matured calendar-year cohort review without claiming proof."""

    crawler_cursor.execute(
        """
        SELECT YEAR(c.as_of_date), MAX(c.as_of_date)
        FROM market_signal_candidates c
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.entity_type IN ('stock','stock_evidence')
          AND c.as_of_date IS NOT NULL
        GROUP BY YEAR(c.as_of_date)
        ORDER BY YEAR(c.as_of_date)
        """
    )
    cohort_years = crawler_cursor.fetchall()
    written_years: list[int] = []
    for signal_year, last_signal_date in cohort_years:
        signal_year = int(signal_year)
        last_signal_date = _date_only(last_signal_date)
        if not last_signal_date:
            continue
        review_due_date = followup_target_date(last_signal_date, "1y")
        if as_of_date < review_due_date:
            continue
        crawler_cursor.execute(
            """
            SELECT COUNT(DISTINCT c.id)
            FROM market_signal_candidates c
            INNER JOIN market_signal_runs r ON r.run_key=c.run_key
            WHERE r.scan_mode='daily' AND r.status='ready'
              AND YEAR(c.as_of_date)=%s
              AND (
                  (c.entity_type='stock_evidence'
                   AND (CASE WHEN JSON_VALID(c.details_text)
                             THEN COALESCE(JSON_LENGTH(JSON_EXTRACT(
                                 c.details_text, '$.family_directions'
                             )), 0) ELSE 0 END)>0)
                  OR (c.entity_type='stock' AND NOT EXISTS (
                      SELECT 1 FROM market_signal_candidates e
                      WHERE e.run_key=c.run_key
                        AND e.entity_id=c.entity_id
                        AND e.entity_type='stock_evidence'
                  ))
              )
            """,
            (signal_year,),
        )
        expected_candidate_count = int(crawler_cursor.fetchone()[0] or 0)
        crawler_cursor.execute(
            """
            SELECT COUNT(DISTINCT candidate_id)
            FROM market_signal_followups
            WHERE YEAR(signal_date)=%s AND horizon_key='1y'
            """,
            (signal_year,),
        )
        tracked_candidate_count = int(crawler_cursor.fetchone()[0] or 0)
        crawler_cursor.execute(
            """
            SELECT f.horizon_key, f.market, f.policy_version, f.source_status,
                   f.delivery_status, f.direction, f.entity_id, f.signal_date,
                   f.family_directions_json, f.stock_return_pct,
                   f.execution_return_pct, f.excess_return_pp,
                   f.max_favorable_excursion_pct, f.max_adverse_excursion_pct,
                   f.status, f.error_reason, f.candidate_selected,
                   f.execution_status, f.execution_error_reason,
                   f.benchmark_status, f.benchmark_error_reason
            FROM market_signal_followups f
            WHERE YEAR(f.signal_date)=%s
            ORDER BY f.horizon_key, f.market, f.signal_date, f.entity_id
            """,
            (signal_year,),
        )
        columns = [column[0] for column in crawler_cursor.description]
        outcome_rows = [dict(zip(columns, row)) for row in crawler_cursor.fetchall()]
        report = summarize_followup_rows(outcome_rows)
        one_year_rows = [
            row for row in outcome_rows if row.get("horizon_key") == "1y"
        ]
        unsettled_one_year_count = sum(
            row.get("status") != "settled"
            or row.get("execution_status") != "settled"
            or row.get("benchmark_status") != "settled"
            for row in one_year_rows
        )
        selected_one_year_rows = [
            row for row in one_year_rows if bool(row.get("candidate_selected"))
        ]
        delivery_status_counts: dict[str, int] = {}
        for row in one_year_rows:
            delivery_status = str(row.get("delivery_status") or "unknown")
            delivery_status_counts[delivery_status] = (
                delivery_status_counts.get(delivery_status, 0) + 1
            )
        unresolved_selected_delivery_count = sum(
            row.get("delivery_status") in {
                "unknown", "pending", "unlinked", "delivery_unknown",
            }
            for row in selected_one_year_rows
        )
        tracking_coverage = (
            tracked_candidate_count / expected_candidate_count
            if expected_candidate_count else 1.0
        )
        if (
            expected_candidate_count > 0
            and unsettled_one_year_count == 0
            and tracked_candidate_count == expected_candidate_count
        ):
            review_status = "complete_descriptive"
        elif unsettled_one_year_count:
            review_status = "incomplete_data"
        else:
            review_status = "partial_coverage"
        report.update({
            "signal_year": signal_year,
            "as_of_date": as_of_date.isoformat(),
            "review_due_date": review_due_date.isoformat(),
            "last_signal_date": last_signal_date.isoformat(),
            "annual_maturity_status": review_status,
            "candidate_tracking_coverage": {
                "expected_directional_candidate_count": expected_candidate_count,
                "tracked_directional_candidate_count": tracked_candidate_count,
                "coverage_pct": round(tracking_coverage * 100, 4),
                "unsettled_one_year_rows": unsettled_one_year_count,
                "note": "Directional evidence snapshots and selected legacy candidates only; this is not a claim that every security in the market is a tradable signal.",
            },
            "delivery_coverage": {
                "selected_candidate_count": len(selected_one_year_rows),
                "confirmed_platform_receipt_count": sum(
                    row.get("delivery_status") == "sent"
                    for row in selected_one_year_rows
                ),
                "failed_selected_delivery_count": sum(
                    row.get("delivery_status") == "failed"
                    for row in selected_one_year_rows
                ),
                "unresolved_selected_delivery_count": unresolved_selected_delivery_count,
                "all_candidate_delivery_status_counts": delivery_status_counts,
                "receipt_means_platform_acceptance_not_human_reading_or_trade": True,
            },
            "limitations": [
                "Individual stock returns use unadjusted official prices; dividends and corporate actions are not included.",
                "Next-session open is a daily OHLC proxy, not a guaranteed fill; fees, tax, slippage, borrow availability and account sizing are not modeled.",
                "Only confirmed notification receipts count as sent; receipt does not mean read or traded.",
                "Overlapping one-year outcomes are dependent; confidence intervals are withheld unless two horizon-length signal-date blocks are available.",
                "A complete descriptive annual review is not evidence of a persistent investable edge.",
            ],
        })
        crawler_cursor.execute(
            """
            INSERT INTO market_signal_outcome_reviews (
                signal_year, as_of_date, review_due_date, review_status,
                report_json, reviewed_at
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                as_of_date=VALUES(as_of_date),
                review_due_date=VALUES(review_due_date),
                review_status=VALUES(review_status),
                report_json=VALUES(report_json),
                reviewed_at=VALUES(reviewed_at)
            """,
            (
                signal_year, as_of_date, review_due_date, review_status,
                json.dumps(report, ensure_ascii=False, default=str, separators=(",", ":")),
                reviewed_at,
            ),
        )
        written_years.append(signal_year)
    return {
        "status": "ready" if written_years else "no_annual_review_due",
        "reviewed_signal_years": written_years,
    }


def _summarize_family_outcomes(
    crawler_cursor, *, as_of_date: dt.date,
) -> dict[str, Any]:
    """Build a compact descriptive family-quality summary from matured outcomes."""

    lookback_start = as_of_date - dt.timedelta(days=OUTCOME_LOOKBACK_DAYS)
    crawler_cursor.execute(
        """
        SELECT c.id, c.details_text
        FROM market_signal_candidates c
        WHERE c.entity_type IN ('stock','stock_evidence')
          AND (
              c.entity_type='stock_evidence'
              OR NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key AND e.entity_type='stock_evidence'
              )
          )
          AND c.as_of_date>=%s AND c.as_of_date<=%s
          AND EXISTS (
              SELECT 1 FROM market_signal_outcomes o
              WHERE o.candidate_id=c.id AND o.status='settled'
                AND o.excess_return_pp IS NOT NULL
                AND o.signal_date>=%s AND o.signal_date<=%s
          )
        ORDER BY c.id
        """,
        (lookback_start, as_of_date, lookback_start, as_of_date),
    )
    family_by_candidate: dict[int, dict[str, int]] = {}
    candidates_without_families = 0
    for candidate_id, details_text in crawler_cursor.fetchall():
        try:
            details = json.loads(details_text or "{}")
        except (TypeError, ValueError):
            details = {}
        raw_families = details.get("family_directions") if isinstance(details, dict) else None
        if not isinstance(raw_families, dict):
            candidates_without_families += 1
            continue
        families: dict[str, int] = {}
        for family, raw_direction in raw_families.items():
            if isinstance(raw_direction, bool) or raw_direction not in (-1, 1):
                continue
            family_name = str(family).strip()
            if family_name:
                families[family_name] = int(raw_direction)
        if families:
            family_by_candidate[int(candidate_id)] = families
        else:
            candidates_without_families += 1

    crawler_cursor.execute(
        """
        SELECT candidate_id, market, source_status, outcome_price_source_key,
               signal_date, outcome_basis, horizon_sessions, excess_return_pp
        FROM market_signal_outcomes
        WHERE status='settled' AND excess_return_pp IS NOT NULL
          AND signal_date>=%s AND signal_date<=%s
        ORDER BY signal_date, candidate_id, outcome_basis, horizon_sessions
        """,
        (lookback_start, as_of_date),
    )
    outcome_rows = crawler_cursor.fetchall()
    groups: dict[tuple[str, str, str, str, str, int], dict[str, Any]] = {}
    family_observation_count = 0
    for candidate_id, market, source_status, quote_source_key, signal_date, outcome_basis, horizon, excess_value in outcome_rows:
        try:
            excess = float(excess_value)
        except (TypeError, ValueError):
            continue
        for family, direction in family_by_candidate.get(int(candidate_id), {}).items():
            key = (
                family, str(market or ""), str(source_status or "unknown"),
                str(quote_source_key or "unknown"),
                str(outcome_basis or ""), int(horizon),
            )
            group = groups.setdefault(key, {
                "candidate_ids": set(), "signal_dates": set(), "signed_excess": [],
            })
            group["candidate_ids"].add(int(candidate_id))
            group["signal_dates"].add(_date_only(signal_date).isoformat())
            group["signed_excess"].append(direction * excess)
            family_observation_count += 1

    summaries = []
    for key, group in sorted(groups.items()):
        signed_excess = group["signed_excess"]
        family_hit_count = sum(value > 0 for value in signed_excess)
        summaries.append({
            "family": key[0],
            "market": key[1],
            "source_status": key[2],
            "outcome_price_source_key": key[3],
            "outcome_basis": key[4],
            "horizon_sessions": key[5],
            "candidate_count": len(group["candidate_ids"]),
            "signal_date_count": len(group["signal_dates"]),
            "min_signal_date": min(group["signal_dates"]),
            "max_signal_date": max(group["signal_dates"]),
            "directional_outcome_count": len(signed_excess),
            "family_hit_count": family_hit_count,
            "flat_outcome_count": sum(value == 0 for value in signed_excess),
            "family_hit_rate": round(family_hit_count / len(signed_excess), 4),
            "mean_signed_excess_return_pp": round(
                sum(signed_excess) / len(signed_excess), 6
            ),
            "median_signed_excess_return_pp": round(
                statistics.median(signed_excess), 6
            ),
        })

    return {
        "status": "ready" if summaries else "no_mature_family_outcomes",
        "as_of_date": as_of_date.isoformat(),
        "lookback_days": OUTCOME_LOOKBACK_DAYS,
        "candidate_count": len(family_by_candidate) + candidates_without_families,
        "candidate_without_families_count": candidates_without_families,
        "mature_outcome_row_count": len(outcome_rows),
        "family_observation_count": family_observation_count,
        "groups": summaries,
        "interpretation": (
            "descriptive_only; family observations overlap within candidates; "
            "returns are market-relative, unadjusted for corporate actions and costs; "
            "do not tune from a single signal date"
        ),
    }


def capture_market_signal_snapshot() -> dict[str, Any]:
    """Collect one reproducible as-of snapshot for daily or intraday analysis."""

    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = (getattr(dag_run, "conf", None) or {}) if dag_run else {}
    scan_mode = str(conf.get("scan_mode", "daily")).strip().lower()
    if scan_mode not in {"daily", "intraday"}:
        scan_mode = "daily"
    run_id = str(getattr(dag_run, "run_id", "manual__unknown"))
    run_key = _run_key(run_id)
    source_dag_run_id = conf.get("source_dag_run_id")
    source_dag_id = conf.get("source_dag_id")
    source_schedule_hint = conf.get("source_schedule_hint")
    started_at = _now_taipei()
    payload: dict[str, Any] = {
        "run_key": run_key,
        "airflow_run_id": run_id,
        "source_dag_run_id": source_dag_run_id,
        "source_dag_id": source_dag_id,
        "scan_mode": scan_mode,
        "status": "failed",
        "source_status": "unknown",
        "proposed_source_cursor_id": None,
        "error_message": None,
    }
    crawler_conn = crawler_cursor = crawler_engine = None
    stock_conn = stock_cursor = stock_engine = None
    try:
        crawler_conn, crawler_cursor, crawler_engine = _open_connection()
        create_schema(crawler_cursor)
        _upsert_run(
            crawler_cursor,
            run_key=run_key,
            source_dag_run_id=source_dag_run_id,
            source_dag_id=source_dag_id,
            scan_mode=scan_mode,
            started_at=started_at,
        )
        crawler_conn.commit()

        market_date = _today_taipei()
        if scan_mode == "daily" and is_taiwan_stock_market_closed(market_date):
            skip_reason = "taiwan_market_closed"
            print(
                f"[market signal] 略過 daily 趨勢掃描 "
                f"| 日期={market_date.isoformat()} | 原因={skip_reason}"
            )
            _update_run(
                crawler_cursor,
                run_key=run_key,
                status="skipped",
                source_status="market_closed",
                started_at=started_at,
                as_of_date=market_date,
            )
            crawler_conn.commit()
            payload.update(
                status="skipped",
                source_status="market_closed",
                as_of_date=market_date.isoformat(),
                skip_reason=skip_reason,
            )
            return payload

        if scan_mode == "intraday":
            collection_state = _source_collection_state(
                crawler_cursor, source_dag_run_id, source_schedule_hint,
                source_dag_id,
            )
            payload["source_collection_status"] = collection_state
            crawler_cursor.execute("SELECT MAX(id) FROM source_records")
            upper_cursor = int((crawler_cursor.fetchone() or (0,))[0] or 0)
            crawler_cursor.execute(
                "SELECT state_text FROM market_signal_state WHERE state_key=%s",
                (INTRADAY_CURSOR_KEY,),
            )
            state_row = crawler_cursor.fetchone()
            if not state_row:
                crawler_cursor.execute(
                    """
                    INSERT INTO market_signal_state (state_key, state_text, updated_at)
                    VALUES (%s, %s, %s)
                    """,
                    (INTRADAY_CURSOR_KEY, str(upper_cursor), started_at),
                )
                crawler_conn.commit()
                baseline_status = (
                    "baseline_initialized"
                    if collection_state == "complete" else "partial"
                )
                payload.update(
                    status="ready", source_status=baseline_status,
                    proposed_source_cursor_id=upper_cursor,
                )
                _update_run(
                    crawler_cursor, run_key=run_key, status="ready",
                    source_status=baseline_status, started_at=started_at,
                )
                crawler_conn.commit()
                return payload

            last_cursor = int(state_row[0] or 0)
            intraday_candidate_diagnostics: dict[str, Any] = {}
            candidates, proposed_cursor = _load_intraday_candidates(
                crawler_cursor,
                last_cursor=last_cursor,
                upper_cursor=upper_cursor,
                today=_today_taipei(),
                started_at=started_at,
                diagnostics=intraday_candidate_diagnostics,
            )
            payload["intraday_candidate_diagnostics"] = (
                intraday_candidate_diagnostics
            )
            payload["news_context_status"] = _attach_recent_news_context(
                candidates, observed_at=started_at
            )
            _insert_candidates(crawler_cursor, run_key, candidates)
            intraday_status = "fresh" if collection_state == "complete" else "partial"
            _update_run(
                crawler_cursor, run_key=run_key, status="ready",
                source_status=intraday_status, started_at=started_at,
                as_of_date=_today_taipei(), candidate_count=len(candidates),
            )
            crawler_conn.commit()
            payload.update(
                status="ready", source_status=intraday_status,
                source_collection_status=collection_state,
                candidate_count=len(candidates),
                proposed_source_cursor_id=proposed_cursor,
            )
            return payload

        stock_conn, stock_cursor, stock_engine = _open_stock_connection()
        candidates, metrics = _load_daily_candidates(
            crawler_cursor,
            stock_cursor,
            today=market_date,
            started_at=started_at,
            source_dag_run_id=source_dag_run_id,
            source_dag_id=source_dag_id,
            source_schedule_hint=source_schedule_hint,
        )
        payload["news_context_status"] = _attach_recent_news_context(
            candidates, observed_at=started_at
        )
        evidence_snapshots = metrics.pop("signal_evidence_snapshots", [])
        _insert_candidates(
            crawler_cursor, run_key, candidates + evidence_snapshots,
        )
        _update_run(
            crawler_cursor,
            run_key=run_key,
            status="ready",
            source_status=metrics["source_status"],
            started_at=started_at,
            as_of_date=metrics["as_of_date"],
            universe_count=metrics["universe_count"],
            price_count=metrics["price_count"],
            flow_count=metrics["flow_count"],
            revenue_count=metrics["revenue_count"],
            financial_count=metrics["financial_count"],
            borrow_count=metrics["borrow_count"],
            insider_count=metrics["insider_count"],
            dividend_count=metrics["dividend_count"],
            candidate_count=len(candidates),
        )
        _persist_market_coverage(
            crawler_cursor,
            run_key=run_key,
            as_of_date=metrics["as_of_date"],
            coverage_by_market=metrics["market_coverage_by_market"],
            observed_at=started_at,
        )
        crawler_conn.commit()
        payload.update(metrics)
        payload.update(status="ready", candidate_count=len(candidates))
        if metrics.get("as_of_date"):
            try:
                outcome_as_of_date = _date_only(metrics["as_of_date"])
                payload["outcome_ledger"] = _settle_daily_candidate_outcomes(
                    crawler_cursor,
                    as_of_date=outcome_as_of_date,
                    settled_at=_now_taipei(),
                )
                crawler_conn.commit()
            except Exception as exc:
                crawler_conn.rollback()
                payload["outcome_ledger"] = {
                    "status": "unavailable",
                    "error": f"{type(exc).__name__}: {exc}"[:300],
                }
                print(
                    f"[market signal] outcome ledger unavailable; scan remains ready "
                    f"| {type(exc).__name__}: {exc}"
                )
            else:
                try:
                    payload["family_outcome_diagnostics"] = _summarize_family_outcomes(
                        crawler_cursor,
                        as_of_date=outcome_as_of_date,
                    )
                    family_summary = payload["family_outcome_diagnostics"]
                    print(
                        "[market signal] family outcome diagnostics; "
                        + json.dumps(
                            {
                                "status": family_summary.get("status"),
                                "candidate_count": family_summary.get("candidate_count"),
                                "mature_outcome_row_count": family_summary.get(
                                    "mature_outcome_row_count"
                                ),
                                "family_observation_count": family_summary.get(
                                    "family_observation_count"
                                ),
                                "group_count": len(family_summary.get("groups") or []),
                            },
                            ensure_ascii=False,
                            default=str,
                        )
                    )
                except Exception as exc:
                    payload["family_outcome_diagnostics"] = {
                        "status": "unavailable",
                        "error_type": type(exc).__name__,
                    }
                    print(
                        "[market signal] family outcome diagnostics unavailable "
                        f"| {type(exc).__name__}"
                    )
            if scan_mode == "daily":
                try:
                    followup_time = _now_taipei()
                    payload["signal_followup_lifecycle"] = _settle_calendar_followups(
                        crawler_cursor,
                        as_of_date=outcome_as_of_date,
                        settled_at=followup_time,
                    )
                    crawler_conn.commit()
                    payload["signal_horizon_reviews"] = _write_horizon_signal_reviews(
                        crawler_cursor,
                        as_of_date=outcome_as_of_date,
                        reviewed_at=followup_time,
                    )
                    crawler_conn.commit()
                    payload["annual_signal_review"] = _write_due_annual_signal_reviews(
                        crawler_cursor,
                        as_of_date=outcome_as_of_date,
                        reviewed_at=followup_time,
                    )
                    crawler_conn.commit()
                    followup_summary = payload["signal_followup_lifecycle"]
                    annual_summary = payload["annual_signal_review"]
                    print(
                        "[market signal] calendar-horizon follow-ups; "
                        + json.dumps(
                            {
                                "status": followup_summary.get("status"),
                                "reviewed_candidate_count": followup_summary.get(
                                    "reviewed_candidate_count"
                                ),
                                "written_rows": followup_summary.get("written_rows"),
                                "settled_by_horizon": followup_summary.get(
                                    "settled_by_horizon"
                                ),
                                "pending_data_by_horizon": followup_summary.get(
                                    "pending_data_by_horizon"
                                ),
                                "horizon_review_status": payload[
                                    "signal_horizon_reviews"
                                ].get("review_status_by_horizon"),
                                "annual_review_status": annual_summary.get("status"),
                                "reviewed_signal_years": annual_summary.get(
                                    "reviewed_signal_years"
                                ),
                            },
                            ensure_ascii=False,
                            default=str,
                        )
                    )
                except Exception as exc:
                    crawler_conn.rollback()
                    payload["signal_followup_lifecycle"] = {
                        "status": "unavailable",
                        "error_type": type(exc).__name__,
                    }
                    print(
                        "[market signal] calendar-horizon follow-ups unavailable "
                        f"| {type(exc).__name__}: {exc}"
                    )
        if scan_mode == "daily" and metrics.get("as_of_date"):
            try:
                payload["backtest_diagnostics"] = summarize_daily_backtest(
                    crawler_cursor,
                    as_of_date=_date_only(metrics["as_of_date"]),
                )
                backtest_summary = payload["backtest_diagnostics"]
                print(
                    "[market signal] daily backtest diagnostics; "
                    + json.dumps(
                        {
                            "status": backtest_summary.get("status"),
                            "deduplicated_outcome_rows": backtest_summary.get(
                                "deduplicated_outcome_rows"
                            ),
                            "settled_rows": backtest_summary.get("settled_rows"),
                            "pending_data_rows": backtest_summary.get("pending_data_rows"),
                            "complete_evidence_signal_dates": backtest_summary.get(
                                "complete_evidence_signal_dates"
                            ),
                            "notification_delivery_group_count": len(
                                backtest_summary.get("notification_delivery") or []
                            ),
                            "observed_group_count": len(
                                backtest_summary.get("observed_policy") or []
                            ),
                            "threshold_group_count": len(
                                backtest_summary.get("threshold_counterfactuals") or []
                            ),
                            "family_group_count": len(
                                backtest_summary.get("family_counterfactuals") or []
                            ),
                        },
                        ensure_ascii=False,
                        default=str,
                    )
                )
            except Exception as exc:
                payload["backtest_diagnostics"] = {
                    "status": "unavailable",
                    "error_type": type(exc).__name__,
                }
                print(
                    "[market signal] daily backtest diagnostics unavailable "
                    f"| {type(exc).__name__}"
                )
        return payload
    except Exception as exc:
        payload["error_message"] = f"{type(exc).__name__}: {exc}"[:1000]
        if crawler_conn and crawler_cursor:
            try:
                crawler_conn.rollback()
                _update_run(
                    crawler_cursor, run_key=run_key, status="failed",
                    source_status="unknown", started_at=started_at,
                    error_message=payload["error_message"],
                )
                crawler_conn.commit()
            except Exception:
                crawler_conn.rollback()
        return payload
    finally:
        for connection in (stock_conn, crawler_conn):
            if connection:
                connection.close()
        for engine in (stock_engine, crawler_engine):
            if engine:
                engine.dispose()


def _discord_notifier() -> DiscordNotify:
    token = pd.read_csv(discord_token_path, encoding="utf_8_sig", index_col="name")
    notifier = DiscordNotify()
    notifier.webhook_url = token.loc[NOTIFICATION_CHANNEL, "token"]
    return notifier


def _sanitize(value: Any, limit: int = 180) -> str:
    return " ".join(str(value or "").replace("@", "@\u200b").split())[:limit]


def _escape_discord_markdown(value: Any, limit: int = 180) -> str:
    text = _sanitize(value, limit)
    text = re.sub(
        r"(?i)(?:https?://|www\.)\S+|\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s]*)?",
        "[外部連結已省略]",
        text,
    )
    special = "\\`*_~|><#[]()-"
    return "".join(f"\\{char}" if char in special else char for char in text)


def _extract_financial_change_percentages(description: str) -> dict[str, list[float]]:
    compact = " ".join(html.unescape(str(description or "")).split())
    patterns = {
        "營業收入": r"營業收入",
        "稅前淨利": r"稅前淨利",
        "歸屬母公司淨利": r"歸屬(?:母公司|本公司)\s*(?:業主|本期)?\s*淨利",
        "每股盈餘": r"(?:基本)?每股盈餘",
    }
    changes: dict[str, list[float]] = {}
    positions = {
        label: re.search(pattern, compact)
        for label, pattern in patterns.items()
    }
    for label, match in positions.items():
        if match:
            next_positions = [
                other.start()
                for other_label, other in positions.items()
                if other_label != label and other and other.start() > match.start()
            ]
            end = min(next_positions, default=min(len(compact), match.end() + 220))
            percent_text = compact[match.end():end]
            values = re.findall(r"([+-]?\d+(?:\.\d+)?)\s*%", percent_text)
            try:
                changes[label] = [float(value) for value in values[:2]]
            except ValueError:
                continue
    return changes


def _announcement_field_value(text: str, labels: tuple[str, ...]) -> str | None:
    """Read a labeled value from numbered official announcement fields."""

    compact = " ".join(html.unescape(str(text or "")).split())
    segments = re.split(r"(?=\d{1,2}\s*[.、．]\s*)", compact)
    for segment in segments:
        parts = re.split(r"[:：]", segment, maxsplit=1)
        if len(parts) != 2:
            continue
        field = re.sub(r"^\s*\d{1,2}\s*[.、．]\s*", "", parts[0]).strip()
        if any(label in field for label in labels):
            value = parts[1].strip()
            return value or None
    return None


def _announcement_field_number(text: str, labels: tuple[str, ...]) -> float | None:
    value = _announcement_field_value(text, labels)
    if not value:
        return None
    match = re.search(r"[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?", value)
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _buyback_notice_metrics(description: str) -> dict[str, float | str | None]:
    planned_shares = _announcement_field_number(
        description, ("預定買回之數量", "預定買回股數", "擬計畫買回股數")
    )
    actual_shares = _announcement_field_number(
        description, ("本次已買回股份數量", "本次買回股份數量")
    )
    return {
        "planned_shares": planned_shares,
        "actual_shares": actual_shares,
        "execution_pct": (
            round(actual_shares / planned_shares * 100, 2)
            if planned_shares is not None and planned_shares > 0
            and actual_shares is not None and actual_shares >= 0
            else None
        ),
        "actual_amount_ntd": _announcement_field_number(
            description, ("本次已買回股份總金額", "本次買回股份總金額")
        ),
        "holding_ratio_pct": _announcement_field_number(
            description,
            (
                "累積已持有自己公司股份數量占公司已發行股份總數之比率",
                "於買回期間內累積已持有自己公司股份數量占公司已發行股份總數之比率",
                "本次買回股數佔公司已發行股份總數比例",
            ),
        ),
        "planned_ratio_pct": _announcement_field_number(
            description, ("預定買回股份占公司已發行股份總數之比率",)
        ),
        "price_range": _announcement_field_value(
            description,
            ("原預定買回區間價格", "預定買回區間價格", "買回區間價格"),
        ),
    }


def _classify_company_event(title: str, description: str) -> dict[str, str]:
    """Classify announcement content; generic disclosures do not qualify for the radar."""

    body = " ".join(html.unescape(str(description or "")).split())
    headline = " ".join(str(title or "").split())
    text = f"{body} {headline}"

    if (
        "更正" in headline
        and any(term in headline for term in ("董事", "監察人"))
        and "新任者" in headline
        and any(term in headline for term in ("簡歷", "履歷"))
    ):
        return {
            "materiality": "review",
            "label": "董事履歷更正｜既有事件",
            "direction": "中性事件",
            "action": (
                "這是更正既有董事異動公告中的新任者履歷資料，不代表今天新發生董事異動。"
                "先比對前後公告；若身分、任職日期或異動比例也改變，再人工重評治理影響，否則只留官方來源、不推播交易訊號。"
            ),
        }

    if "公開收購" in headline and any(
        term in text for term in ("延長", "收購期間", "收購對價")
    ):
        is_extension = "延長" in text
        return {
            "materiality": "key",
            "label": "公開收購期間延長" if is_extension else "公開收購事件｜條件待確認",
            "direction": "中性事件",
            "action": (
                "核對延長後屆滿日、主管機關審查進度及對價是否變動；條件完成前不推定股價方向"
                if is_extension else "追蹤收購期間、對價與主管機關審查條件；條件完成前不推定股價方向"
            ),
        }

    financial_changes = _extract_financial_change_percentages(body)
    if "財務業務資訊" in body and financial_changes:
        revenue = financial_changes.get("營業收入", [])
        profit = financial_changes.get("稅前淨利") or financial_changes.get("歸屬母公司淨利") or []
        eps = financial_changes.get("每股盈餘", [])
        latest_revenue = revenue[0] if revenue else None
        latest_profit = profit[0] if profit else None
        latest_eps = eps[0] if eps else None
        latest_quarter_revenue = revenue[1] if len(revenue) > 1 else None
        latest_quarter_profit = profit[1] if len(profit) > 1 else None
        latest_quarter_eps = eps[1] if len(eps) > 1 else None
        monthly_weak = (
            latest_revenue is not None and latest_revenue <= -20
            and ((latest_profit is not None and latest_profit <= -30)
                 or (latest_eps is not None and latest_eps <= -30))
        )
        quarter_strong = (
            latest_quarter_revenue is not None and latest_quarter_revenue >= 30
            and ((latest_quarter_profit is not None and latest_quarter_profit >= 100)
                 or (latest_quarter_eps is not None and latest_quarter_eps >= 100))
        )
        if monthly_weak and quarter_strong:
            return {
                "materiality": "key", "label": "單月轉弱、單季仍成長",
                "direction": "訊號分歧",
                "action": (
                    "公告顯示最近一月營收/獲利年減，但最近一季仍年增；"
                    "分開看月與季的期間，追蹤下一期營收和公司說明，不把單月變化外推成整季趨勢。"
                ),
            }
        if latest_revenue is not None and latest_revenue >= 30 and (
            (latest_profit is not None and latest_profit >= 100)
            or (latest_eps is not None and latest_eps >= 100)
        ):
            return {
                "materiality": "key", "label": "財務數據轉強觀察",
                "direction": "偏多候選",
                "action": "公告揭露的同期增幅達觀察門檻；先核對自結/核閱口徑，再看股價與成交量確認",
            }
        if latest_revenue is not None and latest_revenue <= -20 and (
            (latest_profit is not None and latest_profit <= -30)
            or (latest_eps is not None and latest_eps <= -30)
        ):
            return {
                "materiality": "key", "label": "財務數據轉弱風險",
                "direction": "偏空風險",
                "action": "公告揭露營收與獲利同步衰退；先確認是否一次性，再觀察價格是否止跌",
            }

    trading_method_change = (
        any(term in body for term in ("變更交易方法", "變更交易方式"))
        and any(term in body for term in ("先收足款券", "全額交割"))
        and not any(term in body for term in (
            "恢復普通交易", "恢復一般交易", "恢復普通交割", "恢復一般交割",
            "恢復原交易方法", "取消變更交易方法",
        ))
    )
    if trading_method_change:
        return {
            "materiality": "key",
            "label": "交易方式限制風險",
            "direction": "偏空風險（交易限制）",
            "action": (
                "官方公告載明變更交易方法並要求券商先收足款券；核對文內實施日及"
                "TPEx 當前交易狀態。這是交割/流動性限制，不等於已終止上櫃或股價方向預測。"
            ),
        }

    if (
        "可能導致期末淨值為負數" in body
        and "終止櫃檯買賣" in body
        and any(term in body for term in ("認列相關損失", "全額損失"))
    ):
        return {
            "materiality": "key",
            "label": "可能淨值為負與終止櫃買風險",
            "direction": "偏空風險（條件式）",
            "action": (
                "公司公告揭露可能認列損失、期末淨值可能為負及終止櫃檯買賣；"
                "損失金額、認列時點與公司實際承擔比例尚待結算及會計師核閱，"
                "不可視為已確定損失或確定終止上櫃。追蹤季報與後續官方公告。"
            ),
        }

    if "澄清媒體報導" in text or ("澄清" in headline and "報導" in headline):
        return {
            "materiality": "routine", "label": "媒體報導澄清", "direction": "中性",
            "action": "公司表示報導或預測不代表正式財務公告，不列入交易訊號",
        }

    if "募集資金" in text and "變更" in text and re.search(
        r"(?:達|超過|逾).{0,16}(?:募集資金總額|募資總額).{0,8}20\s*%|"
        r"(?:計畫變更|資金運用計畫).{0,30}20\s*%",
        text,
    ):
        return {
            "materiality": "key", "label": "募資用途重大變更",
            "direction": "中性事件",
            "action": "比較變更金額、未支用餘額與新用途；確認董事會/主管機關程序及預計效益",
        }

    if re.search(
        r"三分之一以上董事發生變動"
        r"(?:（[^）]{0,80}）|\([^)]{0,80}\))?\s*[:：]\s*"
        r"(?:是|達成)(?=[。；;，,\s]|$)",
        text,
    ):
        return {
            "materiality": "key", "label": "董事會重大異動",
            "direction": "中性事件",
            "action": "確認改選原因、主要職務與生效日；後續觀察治理和營運決策",
        }

    has_search_event = any(
        term in text for term in ("檢調單位", "搜索調查", "司法機關")
    )
    company_claims_no_material_impact = any(
        term in body for term in (
            "營運正常", "營運未受影響", "對本公司財務業務無重大影響",
            "對公司財務業務無重大影響", "無重大不利影響",
        )
    )
    if has_search_event and company_claims_no_material_impact:
        return {
            "materiality": "key", "label": "檢調搜索｜影響待追蹤",
            "direction": "中性事件",
            "action": (
                "公司公告稱營運正常且無重大財務業務影響；這是公司自述，"
                "後續追蹤司法調查範圍與補充公告，不據此推定多空。"
            ),
        }

    if any(term in text for term in ("資料外洩", "個資外洩", "個資遭竊", "勒索軟體", "資安攻擊", "服務中斷")):
        return {
            "materiality": "key", "label": "資安/營運中斷風險",
            "direction": "偏空風險",
            "action": "核對受影響資料、服務範圍與復原/通報進度；公司說法仍需後續公告確認",
        }

    is_buyback_headline = bool(
        re.search(r"(?:買回.{0,16}(?:股份|庫藏股)|庫藏股.{0,16}買回)", headline)
    )
    if is_buyback_headline and re.search(
        r"(?:代.{0,24}子公司|與公司關係[^:：]{0,30}[:：]\s*子公司)",
        f"{headline} {body}",
    ):
        return {
            "materiality": "routine", "label": "子公司庫藏股｜非本公司買回",
            "direction": "中性事件",
            "action": "公告主體是子公司，不能當成本上市櫃公司買回自家股票或股價支撐訊號。",
        }

    if is_buyback_headline:
        buyback = _buyback_notice_metrics(body)
        ratios = [
            buyback["holding_ratio_pct"], buyback["planned_ratio_pct"],
        ]
        ratio_threshold = any(
            value is not None and value >= 2 for value in ratios
        )
        amount_threshold = (
            buyback["actual_amount_ntd"] is not None
            and buyback["actual_amount_ntd"] >= 300_000_000
        )
        title_threshold = (
            any(term in headline for term in ("三億元以上", "3億元以上", "金額達新台幣三億"))
            or bool(re.search(r"(?:達|累積達)\s*2(?:\.0+)?\s*%", headline))
        )
        if amount_threshold or ratio_threshold or title_threshold:
            if "執行完畢" in headline:
                label = "庫藏股買回執行完畢｜達申報門檻"
            elif any(term in headline for term in ("期間屆滿", "期滿")):
                label = "庫藏股買回期間屆滿｜達申報門檻"
            elif any(term in headline for term in ("達一定標準", "金額達", "股份數量累積達")):
                label = "庫藏股買回進度｜達申報門檻"
            else:
                label = "庫藏股買回計畫｜達申報門檻"
            return {
                "materiality": "key", "label": label,
                "direction": "中性事件",
                "action": (
                    "標題明示門檻，或正文可解析的買回金額達 NT$300M/公告股數比例達 2%；"
                    "依計畫量、已買回量、期間與未執行原因追蹤。庫藏股不保證後續買盤或股價支撐。"
                ),
            }

    turnaround_terms = ("由虧轉盈", "轉虧為盈", "虧損轉盈", "擺脫虧損")
    if any(term in strip_negated_terms(text, turnaround_terms) for term in turnaround_terms):
        return {
            "materiality": "key", "label": "盈虧改善候選", "direction": "偏多候選",
            "action": "核對改善是否一次性；等下一交易日收盤與成交量確認",
        }
    risk_text = text
    for negated_phrase in (
        "未發生重大損失", "未有重大損失", "未發生損失", "未發生虧損",
        "不會造成虧損", "並無虧損", "沒有虧損",
    ):
        risk_text = risk_text.replace(negated_phrase, "")
    risk_terms = (
        "虧損", "減資彌補", "重大損失", "財務困難", "跳票", "退票", "違約",
        "停工", "停產", "停業", "下市", "淨值低於", "遭檢調", "搜索",
        "裁罰", "重大裁罰", "資產減損", "無法如期", "終止契約", "解約",
        "取消訂單", "重整", "接管", "掏空", "舞弊", "違反法令",
    )
    risk_text = strip_negated_terms(risk_text, risk_terms)
    catalyst_terms = (
        "得標", "中標", "取得訂單", "接獲訂單", "在手訂單", "營收創新高",
        "獲利創新高", "新產品量產", "正式量產", "取得藥證", "取得許可",
    )
    routine_terms = (
        "股東常會", "股東臨時會", "股東會", "法人說明會", "股務代理",
        "停止過戶", "除權息", "除息基準日", "除權基準日", "現金股利",
        "股票股利", "股利發放", "可轉換公司債", "轉換公司債轉換",
        "發言人", "公司地址變更", "公司名稱變更", "認股基準日",
        "董事會決議召開", "召開說明會", "公告期間", "股票面額",
        "面額變更", "名稱由", "更名為", "變更營業地址",
    )
    matched_risk_terms = [term for term in risk_terms if term in risk_text]
    if (
        matched_risk_terms == ["下市"]
        and re.search(r"下市\s*[（(]櫃[）)]", body)
        and any(term in body for term in ("併購", "收購", "反三角合併"))
        and any(term in body for term in ("發行新股支付", "以新股支付", "發行普通股"))
        and any(term in body for term in ("並無解散、終止上市", "無解散、終止上市"))
        and "無重大不利影響" in body
    ):
        return {
            "materiality": "key",
            "label": "併購交易｜稀釋與效益待驗",
            "direction": "中性事件",
            "action": (
                "公告載明由子公司發行新股作為併購對價，並提示短期每股淨值/EPS可能稀釋；"
                "長期效益仍待交割及後續營運驗證。公告中的制式下市(櫃)問項有明確否認終止上市，"
                "且公司表示無重大不利影響，因此不把該字樣單獨判成偏空。發股主體為子公司，"
                "現有公告資訊不足以量化母公司 EPS 影響；追蹤最終對價、交割條件及實際營收/獲利貢獻。"
            ),
        }
    if any(term in risk_text for term in risk_terms):
        return {
            "materiality": "key", "label": "偏空風險", "direction": "偏空",
            "action": "先讀公告內容與金額影響；未釐清前暫緩新進場",
        }
    if (
        any(term in text for term in ("授信資產", "金融機構債權"))
        and any(term in text for term in ("轉讓", "移轉"))
    ):
        routine = "例行性業務" in text
        unchanged = bool(re.search(r"是否涉及營運模式變更\s*[:：]\s*否", text))
        qualifiers = []
        if routine:
            qualifiers.append("公告稱屬例行性業務")
        if unchanged:
            qualifiers.append("公告稱未涉及營運模式變更")
        qualifier_text = "；".join(qualifiers) if qualifiers else "公告未提供足夠資料判斷經常性"
        return {
            "materiality": "review", "label": "授信資產移轉｜方向中性",
            "direction": "中性",
            "action": (
                f"{qualifier_text}。公告金額尚未對照公司資產與損益規模，不能視為利多；"
                "觀察下一交易日收盤與成交量。"
            ),
        }
    catalyst_text = strip_negated_terms(text, catalyst_terms)
    if any(term in catalyst_text for term in catalyst_terms):
        return {
            "materiality": "key", "label": "利多題材候選", "direction": "偏多候選",
            "action": "公告不是買點；等下一交易日收盤與成交量確認",
        }
    if any(term in text for term in routine_terms):
        return {
            "materiality": "routine", "label": "例行公告", "direction": "中性",
            "action": "已存檔，不推播為交易訊號",
        }
    if any(term in text for term in ("增資", "私募", "發行公司債", "併購", "收購", "處分")):
        return {
            "materiality": "review", "label": "重大事件待判讀", "direction": "未定",
            "action": "確認金額、資金用途或交易條件，再看股價反應",
        }
    return {
        "materiality": "review", "label": "公告待判讀", "direction": "未定",
        "action": "標題不足以定多空；查看官方說明並等價量確認",
    }


def _summarize_company_event(description: str) -> str:
    compact = " ".join(html.unescape(str(description or "")).split())
    facts: list[str] = []
    segments = re.split(r"(?=\d{1,2}\s*[.、．]\s*)", compact)

    financial_changes = _extract_financial_change_percentages(compact)
    month_quarter_table = "最近一月" in compact and "最近一季" in compact
    for label, percentages in financial_changes.items():
        if month_quarter_table and len(percentages) > 1:
            facts.append(
                f"{label}同期增減：最近一月 {percentages[0]:+g}%；"
                f"最近一季 {percentages[1]:+g}%"
            )
        else:
            rendered = " / ".join(f"{value:+g}%" for value in percentages)
            facts.append(f"{label}同期增減：{rendered}")

    if "公開收購" in compact and "延長" in compact:
        periods = re.findall(
            r"(?:原預定公開收購期間|延長公開收購期間)[^。；;]{0,300}?至\s*"
            r"((?:\d{2,3}\s*年\s*)?\d{1,2}\s*月\s*\d{1,2}\s*日)",
            compact,
        )
        if periods:
            if len(periods) >= 2:
                facts.append(f"原期間屆滿：{periods[0]}；延長後屆滿：{periods[1]}")
            else:
                facts.append(f"延長後期間屆滿：{periods[0]}")
        if re.search(r"公平交易委員會.{0,60}(?:仍在審查中|審查中)", compact):
            facts.append("延長原因：公平交易委員會仍在審查結合案")
        if re.search(r"其他條件(?:均)?維持不變", compact):
            facts.append("收購其他條件與對價維持不變")

    def detail_for(*labels: str) -> str | None:
        for segment in segments:
            split_values = re.split(r"[:：]", segment, maxsplit=1)
            if len(split_values) == 2 and any(label in split_values[0] for label in labels):
                value = re.sub(r"^\s*\d{1,2}\s*[.、．]\s*", "", split_values[1]).strip()
                return _sanitize(value, 180) if value else None
        for label in labels:
            match = re.search(
                rf"{re.escape(label)}\s*[:：]\s*([^;；\n]+)", compact
            )
            if match:
                return _sanitize(match.group(1), 180)
        return None

    buyback = _buyback_notice_metrics(compact)
    if any(value is not None for value in buyback.values()):
        planned_shares = buyback["planned_shares"]
        actual_shares = buyback["actual_shares"]
        actual_amount = buyback["actual_amount_ntd"]
        holding_ratio = buyback["holding_ratio_pct"]
        planned_ratio = buyback["planned_ratio_pct"]
        execution_pct = buyback["execution_pct"]
        price_range = buyback["price_range"]
        if planned_shares is not None:
            facts.append(f"計畫買回 {planned_shares:,.0f} 股")
        if actual_shares is not None:
            facts.append(f"本次實際買回 {actual_shares:,.0f} 股")
        if execution_pct is not None:
            facts.append(
                f"公告股數進度比 {execution_pct:.2f}%（本次已買回／預定股數）"
            )
        if actual_amount is not None:
            facts.append(f"本次實際買回金額 NT${actual_amount:,.0f}")
        if holding_ratio is not None:
            facts.append(f"公告累積庫藏股占已發行股數 {holding_ratio:.2f}%")
        if planned_ratio is not None:
            facts.append(f"計畫買回股數占已發行股數 {planned_ratio:.2f}%")
        if price_range:
            facts.append(f"公告買回價格區間：{_sanitize(price_range, 100)}")

    asset_type = detail_for("標的物之名稱及性質", "交易標的名稱", "標的物")
    if asset_type:
        facts.append(f"交易標的：{asset_type}")

    event_period = detail_for("事實發生日")
    if event_period and event_period not in {"不適用", "無"}:
        facts.append(f"事實/交易期間：{event_period}")
    contract_period = detail_for("契約起迄日期", "契約起迄期間")
    if contract_period and contract_period not in {"不適用", "無"}:
        facts.append(f"契約期間：{contract_period}")

    local_amount = re.search(
        r"(?:交易總金額|交易金額|交易總額|發行總額|發行金額|募集資金總額|募集總額|募資總額)"
        r"[^:：\n]{0,60}[:：]\s*(?:折合\s*)?(?:新台幣|新臺幣)\s*(?:約|約為)?\s*"
        r"([\d,]+(?:\.\d+)?)\s*(億元|億|萬元|萬|元)",
        compact,
    )
    if local_amount:
        amount_value = float(local_amount.group(1).replace(",", ""))
        unit = local_amount.group(2)
        multiplier = {
            "億元": 100_000_000,
            "億": 100_000_000,
            "萬元": 10_000,
            "萬": 10_000,
            "元": 1,
        }[unit]
        facts.append(
            f"公告金額：新台幣 {amount_value * multiplier:,.0f} 元"
            f"（原文 {local_amount.group(1)}{unit}）"
        )
    else:
        local_equivalent = re.search(
            r"(?:折合\s*(?:新台幣|新臺幣)|(?:新台幣|新臺幣)\s*折合)"
            r"\s*(?:約|約為)?\s*([\d,]+(?:\.\d+)?)\s*(億元|億|萬元|萬|元)",
            compact,
        )
        if local_equivalent:
            equivalent_value = float(local_equivalent.group(1).replace(",", ""))
            equivalent_unit = local_equivalent.group(2)
            equivalent_multiplier = {
                "億元": 100_000_000,
                "億": 100_000_000,
                "萬元": 10_000,
                "萬": 10_000,
                "元": 1,
            }[equivalent_unit]
            source_amount = re.search(
                r"(?:泰銖|泰國銖|THB)\s*([\d,]+(?:\.\d+)?)\s*(?:元)?",
                compact,
                re.IGNORECASE,
            )
            source_amount_text = (
                f"原文泰銖 {source_amount.group(1)} 元；"
                if source_amount
                else ""
            )
            qualifier = "約" if re.search(
                r"折合\s*(?:新台幣|新臺幣)\s*(?:約|約為)", compact
            ) else ""
            facts.append(
                f"公告金額：{source_amount_text}原文折合新台幣 {qualifier}"
                f"{equivalent_value * equivalent_multiplier:,.0f} 元"
            )
        else:
            amount = re.search(
                r"\b(JPY|USD|NTD|TWD)\s*([\d,]+(?:\.\d+)?)", compact, re.IGNORECASE
            )
            if amount:
                currency = amount.group(1).upper()
                amount_value = float(amount.group(2).replace(",", ""))
                facts.append(f"公告金額：{currency} {amount.group(2)}")
                rate = re.search(
                    rf"(?:{currency}\s*1|1\s*{currency})\s*(?:元|單位)?\s*(?:折合|約合|兌換|=|約為)\s*"
                    r"(?:NTD|TWD|新臺幣|新台幣)?\s*([\d.]+)",
                    compact,
                    re.IGNORECASE,
                )
                if rate and currency not in {"NTD", "TWD"}:
                    rate_value = float(rate.group(1))
                    converted = amount_value * rate_value
                    facts.append(
                        f"按公告匯率折合約新台幣 {converted:,.0f} 元（匯率 {rate_value:g}）"
                    )
    counterparty = detail_for("交易相對人及其與公司之關係", "交易相對人")
    if counterparty:
        facts.append(f"交易相對人：{counterparty}")
    purpose = detail_for(
        "取得或處分之具體目的或用途", "取得之具體目的或用途",
        "本次交易之具體目的或用途", "本次交易之目的", "交易目的",
    )
    if purpose:
        facts.append(f"交易目的：{purpose}")
    gain_loss = detail_for("處分利益（或損失）", "處分利益", "交易損益")
    if gain_loss:
        facts.append(f"公告填列處分損益：{gain_loss}")
    if re.search(r"超過百分之九十|超過\s*90%", compact):
        facts.append("公司債逾90%已贖回或轉換，觸發提前贖回條款")
    if re.search(r"(?:本次交易|交易相對人)為關係人交易\s*[:：]\s*否", compact):
        facts.append("非關係人交易")
    if "關係人交易" in compact and "誤勾選" in compact and "更正" in compact:
        facts.append("更正事項：先前誤勾選關係人交易；比對前次公告，避免重複計入金額")
    previous_notice_date = detail_for("前已就同一件事件發布重大訊息日期")
    if previous_notice_date and previous_notice_date not in {"不適用", "無"}:
        facts.append(f"同事件前次公告日期：{previous_notice_date}")
    if re.search(r"是否涉及營運模式變更\s*[:：]\s*否", compact):
        facts.append("未涉及營運模式變更")
    if "例行性業務" in compact and not any("例行" in fact for fact in facts):
        facts.append("公告稱屬例行性業務")
    if facts:
        return "\n".join(f"• {_sanitize(fact, 180)}" for fact in facts)[:850]
    return _sanitize(compact, 850)


def _format_daily_message(run: dict[str, Any], candidates: list[dict[str, Any]]) -> str:
    as_of_date = _date_only(run.get("as_of_date"))
    as_of = as_of_date.strftime("%Y/%m/%d") if as_of_date else "無新收盤資料"
    return f"📊 台股收盤訊號｜{as_of}｜{len(candidates)} 檔觀察名單"


def _daily_signal_family(key: str) -> str | None:
    if key == "price_5d_return_pct":
        return "price"
    if key == "volume_expansion_ratio":
        return "volume"
    if key.startswith("institutional_net_"):
        return "institutions"
    if key.startswith("broker_concentration_"):
        return "broker_branches"
    if key.startswith("revenue_"):
        return "revenue"
    if key.startswith("operating_margin_"):
        return "profitability"
    # Unadjusted aggregate holdings are context only.
    return None


def _daily_data_source_groups(family_directions: dict[str, int]) -> dict[str, Any]:
    """Describe broad input groups without implying statistical independence."""
    grouped: dict[str, dict[str, Any]] = {}
    for family, direction in sorted(family_directions.items()):
        mapped = DAILY_FAMILY_SOURCE_GROUPS.get(family)
        group_key, label = mapped or (f"unmapped:{family}", f"未映射家族 {family}")
        group = grouped.setdefault(
            group_key, {"key": group_key, "label": label, "families": [], "directions": []}
        )
        group["families"].append(family)
        group["directions"].append(int(direction))

    groups = []
    mixed_direction_groups = []
    for group_key in sorted(grouped):
        group = grouped[group_key]
        directions = sorted(set(group["directions"]))
        mixed = len(directions) > 1
        if mixed:
            mixed_direction_groups.append(group_key)
        groups.append(
            {
                "key": group_key,
                "label": group["label"],
                "families": group["families"],
                "direction": directions[0] if len(directions) == 1 else 0,
                "mixed_direction": mixed,
            }
        )
    return {
        "group_count": len(groups),
        "groups": groups,
        "mixed_direction_groups": mixed_direction_groups,
        "interpretation": "價格與量能同群，法人與分點同屬交易籌碼；可能描述同一批交易，資料群不代表統計獨立性",
    }


def _daily_signal_explanations(row: dict[str, Any], detail: dict[str, Any]) -> dict[str, str]:
    signals = detail.get("signals") or {}
    priority = {
        "price_5d_return_pct": 0,
        "volume_expansion_ratio": 1,
        "institutional_net_buy_days_5d": 2,
        "institutional_net_sell_days_5d": 2,
        "institutional_net_intensity_pct": 2,
        "broker_concentration_pct": 3,
        "operating_margin_change_pp": 5,
        "operating_margin_pct": 6,
        "insider_holdings_change_pct": 7,
        "tdcc_shareholding_distribution": 8,
        "foreign_ownership_tpex_daily": 9,
    }
    keys = [key for key in str(row.get("signal_keys") or "").split(",") if key in signals]
    keys.sort(key=lambda key: (priority.get(key, 99), key))
    output: dict[str, str] = {}
    for key in keys:
        value = signals[key]
        number = _number(value)
        if key == "price_5d_return_pct" and number is not None:
            output[key] = f"個股近 5 個交易日漲跌 {number:+.1f}%"
        elif key == "volume_expansion_ratio" and number is not None:
            output[key] = f"最新單日成交量為前 20 日均量 {number:.1f} 倍"
        elif key == "institutional_net_intensity_pct" and number is not None:
            window = (detail.get("equity_order_flow_context", {}).get("institutions", {}).get("selected_window") or {})
            days = int(window.get("days") or 0)
            components = "、".join(f"{label} {float(window.get(component) or 0) / 1000:+,.0f} 張"
                                  for component, label in (("foreign", "外資"), ("trust", "投信"), ("dealer", "自營商")))
            output[key] = (f"法人 {days} 日淨買賣超 {float(window.get('total') or 0) / 1000:+,.0f} 張，"
                           f"占同期成交量 {number:+.1f}%；{window.get('same_sign_days', 0)}/{days} 日同向（{components}）")
        elif key == "broker_concentration_pct" and number is not None:
            window = (detail.get("equity_order_flow_context", {}).get("broker_branches", {}).get("selected_window") or {})
            output[key] = (f"分點 {window.get('days', 0)} 日 Top15 買賣超集中度 {number:+.1f}%"
                           "（買方淨買超合計減賣方淨賣超合計／同期成交量；門檻另扣整張精度誤差）")
        elif key in {"institutional_net_buy_days_5d", "institutional_net_sell_days_5d"} and number is not None:
            side = "買超" if key.endswith("buy_days_5d") else "賣超"
            observed_days = int(_number(signals.get("institutional_flow_observation_days_5d")) or 0)
            total_shares = _number(signals.get("institutional_net_total_shares_5d"))
            components = []
            for component_key, label in (
                ("institutional_foreign_net_shares_5d", "外資"),
                ("institutional_trust_net_shares_5d", "投信"),
                ("institutional_dealer_net_shares_5d", "自營商"),
            ):
                component = _number(signals.get(component_key))
                if component is not None:
                    components.append(f"{label} {component / 1000:+,.0f} 張")
            total_text = (
                f"；合計淨{side} {total_shares / 1000:+,.0f} 張"
                if total_shares is not None else ""
            )
            component_text = f"（{'、'.join(components)}）" if components else ""
            output[key] = (
                f"近 5 個交易日中 {int(number)} 日法人{side}"
                f"（有效資料 {observed_days} 日）{total_text}{component_text}"
            )
        elif key == "operating_margin_pct" and number is not None:
            output[key] = f"最新單季營業利益率 {number:+.1f}%"
        elif key == "operating_margin_change_pp" and number is not None:
            comparison = (detail.get("profitability_context") or {}).get("vote_comparison")
            label = "去年同季" if comparison == "same_quarter_yoy" else "前季"
            output[key] = f"單季營業利益率較{label}變化 {number:+.1f} 個百分點"
        elif key == "profitability_period":
            output[key] = f"單季營益率資料期 {value}"
        elif key == "insider_holdings_change_pct" and number is not None:
            output[key] = f"彙總內部人持股較前期變化 {number:+.1f}%（未校正股本與人員異動，背景不計分）"
        elif key == "tdcc_shareholding_distribution" and isinstance(value, dict):
            current_date = _date_only(value.get("effective_date"))
            prior_date = _date_only(value.get("prior_effective_date"))
            current_label = current_date.strftime("%Y/%m/%d") if current_date else "日期未明"
            prior_label = prior_date.strftime("%Y/%m/%d") if prior_date else "日期未明"
            change_400 = _number(value.get("change_400_lot_share_pp"))
            change_1000 = _number(value.get("change_1000_lot_share_pp"))
            if change_400 is not None and change_1000 is not None:
                output[key] = (
                    f"TDCC 集保股權分布（{current_label} 對 {prior_label}）："
                    f"400 張以上 {change_400:+.2f}、1,000 張以上 "
                    f"{change_1000:+.2f} 個百分點（背景，不計方向家族；"
                    f"[TDCC 官方批次]({TDCC_SHAREHOLDING_SOURCE_URL})）"
                )
        elif key == "foreign_ownership_tpex_daily" and isinstance(value, dict):
            effective_date = _date_only(value.get("effective_date"))
            date_label = effective_date.strftime("%Y/%m/%d") if effective_date else "日期未明"
            holding_pct = _number(value.get("foreign_holding_ratio_pct"))
            held_shares = _number(value.get("foreign_held_shares"))
            if holding_pct is not None and held_shares is not None:
                output[key] = (
                    f"TPEx 僑外資及陸資持股快照（{date_label}）：持股比例 "
                    f"{holding_pct:.2f}%、持有 {held_shares / 1000:,.0f} 張"
                    f"（上櫃背景，不代表當日買賣流或方向票；"
                    f"[政府開放資料 20214](https://data.gov.tw/dataset/20214)）"
                )
        elif key == "statutory_director_shareholding_shortfall" and isinstance(value, dict):
            report_date = _date_only(value.get("effective_date"))
            date_label = report_date.strftime("%Y/%m/%d") if report_date else "日期未明"
            shortfall_items = []
            for label, required_key, actual_key, shortfall_key in (
                ("董事", "director_required_shares", "director_actual_shares", "director_shortfall_shares"),
                ("監察人", "supervisor_required_shares", "supervisor_actual_shares", "supervisor_shortfall_shares"),
            ):
                shortfall = _number(value.get(shortfall_key))
                if shortfall is None or shortfall <= 0:
                    continue
                required = _number(value.get(required_key))
                actual = _number(value.get(actual_key))
                numbers = (
                    f"不足 {shortfall:,.0f} 股"
                    + (f"／應持 {required:,.0f} 股" if required is not None else "")
                    + (f"、實持 {actual:,.0f} 股" if actual is not None else "")
                )
                shortfall_items.append(f"{label}{numbers}")
            if shortfall_items:
                source_page = _official_source_url(value.get("dataset_url"))
                source_link = f"｜[官方資料集]({source_page})" if source_page else ""
                output[key] = (
                    f"官方董監持股不足法定成數名單（{date_label}）："
                    f"{'；'.join(shortfall_items)}；治理風險背景、不計多空分數；"
                    f"名單外狀態未知{source_link}"
                )
        elif key == "available_short_shares_change_pct" and number is not None:
            output[key] = f"可借券賣出股數較前一筆資料變化 {number:+.0f}%（背景）"
        elif key == "securities_borrowing_balance_change_pct" and number is not None:
            output[key] = f"借券賣出餘額較前一筆資料變化 {number:+.0f}%（背景）"
        elif key == "recent_dividend_announcement":
            output[key] = "近期有股利公告（背景；不作多空方向）"
        elif key == "cash_dividend_per_share" and number is not None:
            output[key] = f"每股現金股利 {number:.2f} 元（背景；不作多空方向）"
    return output


def _daily_signal_texts(row: dict[str, Any], detail: dict[str, Any]) -> list[str]:
    return list(_daily_signal_explanations(row, detail).values())[:8]


def _daily_candidate_action(detail: dict[str, Any]) -> tuple[str, str]:
    direction = detail.get("direction")
    levels = detail.get("price_levels") or {}
    return_5d = _number(levels.get("return_5d_pct"))
    relative_5d = _number(levels.get("relative_5d_return_pp"))
    if direction == "偏多":
        if return_5d is not None and return_5d >= 15 and relative_5d is not None and relative_5d < 0:
            action = (
                f"近 5 日已漲 {return_5d:.1f}%，但落後所屬市場 {abs(relative_5d):.1f} 個百分點；"
                "不追價，等相對轉強與整理。"
            )
        elif return_5d is not None and return_5d >= 15:
            action = f"近 5 日已漲 {return_5d:.1f}%，短線延伸風險高；不追價，等整理。"
        elif relative_5d is not None and relative_5d <= -3:
            action = (
                f"雖列偏多觀察，近 5 日仍落後所屬市場 {abs(relative_5d):.1f} 個百分點；"
                "先等相對轉強再確認。"
            )
        elif levels.get("above_previous_20d_close_high") is True:
            action = "已站上前 20 個交易日收盤高；等下一個收盤確認，觀察量能能否延續。"
        elif _number(levels.get("previous_20d_close_high")) is not None:
            action = f"尚未站上前 20 日收盤高 {float(levels['previous_20d_close_high']):.2f}；先等收盤突破確認。"
        else:
            action = "偏多觀察，不等於進場訊號；等待下一個收盤與成交量確認。"
    elif direction == "偏空":
        action = "偏空風險觀察；暫緩新增多單，等價格止跌或訊號家族轉向。"
    else:
        action = "支持與反向資料群票數相同，或同群方向衝突；先觀望，等下一次收盤更新。"

    risk_action = trading_risk_action(detail.get("regulatory_context"))
    if risk_action:
        action = risk_action

    low = _number(levels.get("trailing_10d_close_low"))
    reference = (
        f"近 10 個交易日收盤最低 {low:.2f}；跌破可重新檢驗觀察假設，並非自動停損價。"
        if low is not None else ""
    )
    return action, reference


def _safe_news_article_url(value: Any) -> str:
    raw = str(value or "").strip()
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError:
        return ""
    if (
        parsed.scheme != "https" or not parsed.hostname
        or parsed.username or parsed.password or port not in (None, 443)
        or re.search(r"[\s()< >\\]", raw)
    ):
        return ""
    return raw


def _daily_candidate_embed(row: dict[str, Any], detail: dict[str, Any]) -> dict[str, Any]:
    direction = str(detail.get("direction") or "訊號分歧")
    if direction not in {"偏多", "偏空", "訊號分歧"}:
        direction = "訊號分歧"
    code = _escape_discord_markdown(row.get("entity_id"), 16)
    name = _escape_discord_markdown(row.get("security_name"), 32)
    explanations = _daily_signal_explanations(row, detail)
    family_directions = detail.get("family_directions") or {}
    wanted_direction = 1 if direction == "偏多" else -1 if direction == "偏空" else 0
    support_by_family: dict[str, list[str]] = {}
    for key, text in explanations.items():
        family = _daily_signal_family(key)
        if family is not None and (wanted_direction == 0 or family_directions.get(family) == wanted_direction):
            support_by_family.setdefault(family, []).append(text)
    support = ["；".join(items) for items in support_by_family.values()]
    counter = [
        text for key, text in explanations.items()
        if _daily_signal_family(key) is not None
        and wanted_direction != 0
        and family_directions.get(_daily_signal_family(key)) == -wanted_direction
    ]
    counter.extend(detail.get("contradictory_evidence") or [])
    pledge_context = detail.get("insider_pledge_context") or {}
    pledged_ratio = _number(pledge_context.get("pledged_ratio_pct"))
    pledge_period = _date_only(pledge_context.get("period"))
    pledge_holders = int(_number(pledge_context.get("holder_count")) or 0)
    if (
        pledged_ratio is not None and pledge_period is not None
        and pledge_holders > 0 and pledged_ratio > 0
    ):
        pledge_text = (
            f"董監/經理人直接申報持股中設質 {pledged_ratio:.1f}% "
            f"（{pledge_period:%Y/%m} 期、{pledge_holders} 位；背景，不計分）"
        )
        pledge_source_url = _official_source_url(pledge_context.get("source_url"))
        if pledge_source_url:
            pledge_text += f"｜[官方月資料]({pledge_source_url})"
        counter.insert(0, pledge_text)
    counter.extend(
        text for key, text in explanations.items()
        if _daily_signal_family(key) is None
    )
    levels = detail.get("price_levels") or {}
    relative_5d = _number(levels.get("relative_5d_return_pp"))
    benchmark_return = _number(levels.get("benchmark_return_5d_pct"))
    stock_return = _number(levels.get("return_5d_pct"))
    benchmark_code = str(levels.get("benchmark_code") or "")
    market_comparison = (
        f"個股 {stock_return:+.1f}%｜{benchmark_code} {benchmark_return:+.1f}%｜"
        f"相對大盤 {relative_5d:+.1f} 個百分點"
        if stock_return is not None and benchmark_return is not None and relative_5d is not None
        else "官方大盤基準資料暫缺；個股漲幅尚未扣除市場整體走勢。"
    )
    industry_median = _number(
        levels.get("industry_peer_median_return_5d_pct")
    )
    relative_industry = _number(
        levels.get("relative_5d_return_to_industry_pp")
    )
    if industry_median is not None and relative_industry is not None:
        industry = _escape_discord_markdown(row.get("industry"), 40) or "未分類"
        covered_count = int(_number(levels.get("industry_peer_covered_count")) or 0)
        universe_count = int(_number(levels.get("industry_peer_universe_count")) or 0)
        coverage_pct = _number(levels.get("industry_peer_coverage_pct"))
        coverage_label = f"{covered_count}/{universe_count} 檔"
        if coverage_pct is not None:
            coverage_label += f"（{coverage_pct:.0f}%）"
        market_comparison += (
            f"\n同市場{industry}五日等權中位數 {industry_median:+.1f}%｜"
            f"個股相對產業 {relative_industry:+.1f} 個百分點｜"
            f"有效報酬 {coverage_label}"
        )
    else:
        market_comparison += (
            "\n產業相對值暫缺（同市場同產業同日有效樣本未達 10 檔或 70% 覆蓋）；"
            "缺列不填零。"
        )
    positioning = detail.get("positioning_context") or {}
    margin_change = _number(positioning.get("margin_balance_change_5d_pct"))
    short_change = _number(positioning.get("short_balance_change_5d_pct"))
    margin_utilization = _number(positioning.get("margin_utilization_pct"))
    margin_as_of = _escape_discord_markdown(positioning.get("as_of_date"), 16)
    if margin_change is not None or short_change is not None or margin_utilization is not None:
        margin_items = []
        if margin_change is not None:
            margin_items.append(f"近 5 日融資餘額 {margin_change:+.1f}%（槓桿變化，非多空訊號）")
        if short_change is not None:
            margin_items.append(f"近 5 日融券餘額 {short_change:+.1f}%（部位背景，非多空訊號）")
        if margin_utilization is not None:
            margin_items.append(f"融資使用率 {margin_utilization:.1f}%")
        if margin_as_of:
            margin_items.append(f"資料日 {margin_as_of}")
        counter.append("；".join(margin_items))
    if direction == "偏多" and stock_return is not None and stock_return >= 15:
        counter.append(f"短線延伸風險：近 5 日已漲 {stock_return:.1f}%，可能提高追價回撤風險")

    revenue_context = detail.get("revenue_context") or {}
    revenue_period = _escape_discord_markdown(revenue_context.get("period"), 16)
    revenue_amount = _number(revenue_context.get("revenue_current_thousand_ntd"))
    revenue_ytd_yoy = _number(revenue_context.get("revenue_ytd_yoy_pct"))
    revenue_mom = _number(revenue_context.get("revenue_mom_pct"))
    revenue_mom_prior = _number(
        revenue_context.get("revenue_mom_prior_thousand_ntd")
    )
    revenue_3m_yoy = _number(revenue_context.get("revenue_3m_yoy_pct"))
    revenue_3m_start = _escape_discord_markdown(
        revenue_context.get("revenue_3m_start_period"), 16
    )
    revenue_3m_end = _escape_discord_markdown(
        revenue_context.get("revenue_3m_end_period"), 16
    )
    revenue_yoy = _number(revenue_context.get("revenue_yoy_pct"))
    revenue_note = _escape_discord_markdown(revenue_context.get("issuer_note"), 240)
    revenue_context_parts = []
    if revenue_period:
        revenue_context_parts.append(f"最近一期月營收（{revenue_period}）")
    if revenue_amount is not None:
        revenue_context_parts.append(f"{revenue_amount:,.0f} 千元")
    if revenue_yoy is not None:
        revenue_context_parts.append(f"單月年增 {revenue_yoy:+.1f}%")
    if revenue_mom is not None:
        mom_context = "背景，不另計方向票"
        if revenue_mom_prior is not None:
            mom_context = f"前月 {revenue_mom_prior:,.0f} 千元；{mom_context}"
        revenue_context_parts.append(f"較上月 {revenue_mom:+.1f}%（{mom_context}）")
    if revenue_3m_yoy is not None and revenue_3m_start and revenue_3m_end:
        revenue_context_parts.append(
            f"近3月合計（{revenue_3m_start}–{revenue_3m_end}）年增 {revenue_3m_yoy:+.1f}%"
        )
    if revenue_ytd_yoy is not None:
        try:
            revenue_month = int(revenue_period[-2:])
        except (TypeError, ValueError):
            revenue_month = None
        if revenue_month is not None:
            revenue_context_parts.append(f"1–{revenue_month}月累計年增 {revenue_ytd_yoy:+.1f}%")
    if revenue_note:
        revenue_context_parts.append(f"公司申報說明：{revenue_note}")
    revenue_context_text = ""
    if revenue_context_parts:
        revenue_context_text = "；".join(revenue_context_parts)
        conflicts = []
        if revenue_yoy is not None and revenue_ytd_yoy is not None and revenue_yoy * revenue_ytd_yoy < 0:
            conflicts.append("單月與累計方向相反")
        if revenue_yoy is not None and revenue_3m_yoy is not None and revenue_yoy * revenue_3m_yoy < 0:
            conflicts.append("單月與近3月合計方向相反")
        if conflicts:
            revenue_context_text += f"（{'；'.join(conflicts)}）"
        revenue_source_url = _official_source_url(revenue_context.get("source_url"))
        if revenue_source_url:
            revenue_context_text += f"｜[官方原始資料]({revenue_source_url})"

    profitability_context = detail.get("profitability_context") or {}
    profitability_period = _escape_discord_markdown(
        profitability_context.get("period"), 16
    )
    profitability_margin = _number(
        profitability_context.get("operating_margin_pct")
    )
    prior_year_profitability_period = _escape_discord_markdown(
        profitability_context.get("prior_year_period"), 16
    )
    prior_year_profitability_margin = _number(
        profitability_context.get("prior_year_operating_margin_pct")
    )
    profitability_yoy_change = _number(
        profitability_context.get("operating_margin_yoy_change_pp")
    )
    profitability_basis = str(profitability_context.get("basis") or "")
    profitability_context_text = ""
    if profitability_period and profitability_margin is not None:
        if profitability_basis == "單季":
            basis_label = "單季"
        else:
            try:
                quarter = int(profitability_period[-1])
            except (TypeError, ValueError):
                quarter = 0
            basis_label = {
                2: "上半年累計",
                3: "前三季累計",
                4: "全年累計",
            }.get(quarter, "年初至今累計")
        profitability_context_text = (
            f"{profitability_period}｜{basis_label}營業利益率 "
            f"{profitability_margin:+.1f}%"
        )
        if (
            prior_year_profitability_period
            and prior_year_profitability_margin is not None
            and profitability_yoy_change is not None
        ):
            profitability_context_text += (
                f"；同季 {prior_year_profitability_period} "
                f"{prior_year_profitability_margin:+.1f}%"
                f"（年對年 {profitability_yoy_change:+.1f} 個百分點）"
            )
        if profitability_basis != "單季":
            profitability_context_text += (
                "（缺前季累計基準，暫不換算單季趨勢）"
            )
        profitability_source_url = _official_source_url(
            profitability_context.get("source_url")
        )
        if profitability_source_url:
            profitability_context_text += (
                f"｜[交易所官方財務資料]({profitability_source_url})"
            )

    earnings_context = detail.get("earnings_context") or {}
    earnings_period = _escape_discord_markdown(earnings_context.get("period"), 16)
    basic_eps = _number(earnings_context.get("basic_eps_ntd_per_share"))
    earnings_context_text = ""
    if earnings_period and basic_eps is not None:
        earnings_context_text = (
            f"{earnings_period}｜官方列報基本EPS {basic_eps:+.2f}元/股；"
            "未作年對年比較，不計入多空分數"
        )
        if basic_eps < 0:
            earnings_context_text += "（負EPS為反向獲利脈絡）"
        earnings_source_url = _official_source_url(earnings_context.get("source_url"))
        if earnings_source_url:
            earnings_context_text += f"｜[交易所財報資料]({earnings_source_url})"

    valuation_context = detail.get("valuation_context") or {}
    valuation_effective_date = _date_only(valuation_context.get("effective_date"))
    valuation_parts = []
    pe_ratio = _number(valuation_context.get("pe_ratio"))
    dividend_yield = _number(valuation_context.get("dividend_yield_pct"))
    price_to_book = _number(valuation_context.get("price_to_book"))
    if pe_ratio is not None:
        valuation_parts.append(f"本益比 {pe_ratio:.2f}x")
    if dividend_yield is not None:
        valuation_parts.append(f"殖利率 {dividend_yield:.2f}%")
    if price_to_book is not None:
        valuation_parts.append(f"股價淨值比 {price_to_book:.2f}x")
    valuation_context_text = ""
    if valuation_effective_date and valuation_parts:
        valuation_context_text = (
            f"資料日 {valuation_effective_date:%Y/%m/%d}｜"
            + "；".join(valuation_parts)
            + "；只作估值背景，不計多空分數；未列欄位保留為缺值，不當成 0"
        )
        valuation_source_url = _official_source_url(
            valuation_context.get("source_url")
        )
        if valuation_source_url:
            valuation_context_text += f"｜[交易所官方資料]({valuation_source_url})"

    balance_sheet_context = detail.get("balance_sheet_context") or {}
    balance_sheet_period = _escape_discord_markdown(
        balance_sheet_context.get("period"), 16
    )
    current_ratio = _number(balance_sheet_context.get("current_ratio"))
    liabilities_to_equity = _number(
        balance_sheet_context.get("liabilities_to_equity")
    )
    balance_sheet_context_text = ""
    if balance_sheet_period and (
        current_ratio is not None or liabilities_to_equity is not None
    ):
        balance_sheet_parts = [balance_sheet_period]
        if current_ratio is not None:
            balance_sheet_parts.append(f"流動比率 {current_ratio:.2f}x")
        if liabilities_to_equity is not None:
            balance_sheet_parts.append(f"負債/權益 {liabilities_to_equity:.2f}x")
        balance_sheet_context_text = (
            "｜".join(balance_sheet_parts)
            + "；僅列官方期末存量比率，不代表營業現金流或方向訊號"
        )
        balance_sheet_source_url = _official_source_url(
            balance_sheet_context.get("source_url")
        )
        if balance_sheet_source_url:
            balance_sheet_context_text += (
                f"｜[交易所官方資產負債表]({balance_sheet_source_url})"
            )

    balance = detail.get("evidence_balance") or {}
    aligned = int(balance.get("aligned_families") or 0)
    total = int(balance.get("directional_families") or len(family_directions))
    source_groups = detail.get("data_source_groups")
    groups_were_saved = isinstance(source_groups, dict)
    if not groups_were_saved:
        source_groups = _daily_data_source_groups(family_directions)
    group_rows = source_groups.get("groups") or []
    group_labels = [
        str(group.get("label")) for group in group_rows
        if isinstance(group, dict) and group.get("label")
    ]
    group_count = int(source_groups.get("group_count") or len(group_rows))
    support_heading = (f"觀察因素｜{total} 家族、{group_count} 資料群；群票多空相抵"
                       if wanted_direction == 0 else
                       f"支持因素｜同向 {aligned}/{total} 家族")
    if balance.get("policy_version") and wanted_direction:
        support_heading = (f"支持因素｜同向 {int(balance.get('aligned_groups') or 0)}/{group_count} 群"
                           f"（{aligned}/{total} 家族）")
    source_line = (
        f"底層資料群 {group_count} 組（{'、'.join(group_labels)}）；家族數不等於獨立來源數。"
        if group_labels
        else "底層資料群資訊未保存。"
    )
    mixed_groups = source_groups.get("mixed_direction_groups") or []
    if mixed_groups:
        source_line += " 同一資料群內方向有衝突。"
    if not groups_were_saved and group_labels:
        source_line += "（依歷史快照已存家族欄位還原）"
    date_value = _date_only(row.get("as_of_date"))
    date_label = date_value.strftime("%Y/%m/%d") if date_value else "日期未提供"
    action, reference = _daily_candidate_action(detail)
    colors = {"偏多": 0x2E8B57, "偏空": 0xC0392B, "訊號分歧": 0xD68910}
    icons = {"偏多": "🟢", "偏空": "🔴", "訊號分歧": "🟡"}
    embed: dict[str, Any] = {
        "title": f"{icons.get(direction, '⚪')} {code} {name}｜{direction}觀察",
        "description": (
            f"資料日 {date_label}｜至少 3 個有效資料家族有方向才列入觀察；"
            f"多空依資料群方向判定，同群不重複加權，平手列為分歧。\n{source_line}"
        ),
        "color": colors.get(direction, 0x5865F2),
        "fields": [
            {
                "name": support_heading,
                "value": "\n".join(f"• {text}" for text in support[:6])
                or "方向由符合門檻的資料家族決定；本卡未取得可讀細節。",
                "inline": False,
            },
            {
                "name": "相反訊號與風險",
                "value": "\n".join(f"• {text}" for text in counter[:4])
                or "未達系統反向訊號門檻；不代表沒有風險。",
                "inline": False,
            },
            {"name": "相對市場/產業表現｜近 5 個交易日", "value": market_comparison, "inline": False},
            {"name": "接下來怎麼看", "value": action, "inline": False},
        ],
        "footer": {"text": "規則式觀察名單，不是買進建議、上漲機率或自動停損價"
                   + ("｜較早行情取既有股票表，依入庫時間限制，無不可變版本"
                      if levels.get("stored_earlier_history_points") else "")},
    }
    if reference:
        embed["fields"].append({"name": "價位觀察", "value": reference, "inline": False})
    order_flow = detail.get("equity_order_flow_context") or {}
    if order_flow:
        lines = []
        institutional = order_flow.get("institutions") or {}
        latest = institutional.get("latest_observation") or {}
        if latest:
            if institutional.get("vote_enabled") and institutional.get("direction"):
                vote_note = "已達法人多空條件，納入本次判斷"
            elif institutional.get("vote_enabled"):
                vote_note = "已判讀，未達多空條件或短長期方向分歧"
            else:
                vote_note = "本檔缺同日有效行情或可對齊視窗，略過這一項"
            pit_note = (
                "自然批次 PIT 已核對" if latest.get("natural_pit")
                else "依入庫時間限定當時可得；無不可變歷史版本"
            )
            lines.append(
                f"法人資料日 {latest.get('effective_date') or '未知'}，淨額 "
                f"{float(latest.get('total') or 0) / 1000:+,.0f} 張；"
                f"[來源]({latest.get('source_url')})｜{vote_note}（{pit_note}）。"
            )
            lines.append("自營商含避險；規則需併看外資與投信，並核對買賣超占成交量及連續性。")
        elif institutional.get("status") == "source_not_authorized":
            lines.append("此為舊政策快照：當時未讀入既有上市法人表；不代表目前沒有法人資料。")
        else:
            lines.append("本檔缺同日法人列或有效成交資料；只略過本檔這一項，缺列不填零。")
        broker = order_flow.get("broker_branches") or {}
        window = broker.get("selected_window") or (broker.get("windows") or {}).get("1")
        if window:
            lines.append(f"分點 {window['days']} 日集中度 {window['concentration_pct']:+.1f}%；"
                         f"{'有方向票' if broker.get('direction') else '未達門檻／視窗分歧'}。")
            for key, label in (("top_buyers", "買超"), ("top_sellers", "賣超")):
                branches = "、".join(
                    f"{_escape_discord_markdown(item.get('name') or item['branch'], 24)}({item['branch']}) "
                    f"{item['net_shares'] / 1000:+,.0f} 張" for item in window.get(key, [])[:2]
                )
                if branches:
                    lines.append(f"主要{label}分點：{branches}。")
            lines.append("分點含客戶與自營交易，不代表法人身分或持倉成本；與法人證據可能重疊。")
        elif broker.get("status") == "source_unavailable_skipped":
            lines.append("上櫃券商分點買賣資料未收集，只略過分點訊號，法人照常判讀。")
        elif not broker.get("same_day_panel_available"):
            lines.append(f"本檔上市分點最新可用日 {broker.get('latest_effective_date') or '未取得'}；"
                         "缺本次同日面板，不以舊日資料冒充當日。")
        else:
            reasons = broker.get("rejected_date_counts") or {}
            lines.append("上市分點已有同日面板；"
                         + ("成交量未達 100 萬股或同日行情缺值，" if reasons.get("low_or_missing_trade_volume")
                            else "分點買賣量與同日行情成交量口徑不符，")
                         + "本檔不形成分點訊號。")
        embed["fields"].insert(2, {"name": "法人與分點資料", "value": "\n".join(lines)[:1000], "inline": False})
    if revenue_context_text:
        embed["fields"].insert(
            2,
            {
                "name": "營收脈絡（不另計訊號家族）",
                "value": revenue_context_text,
                "inline": False,
            },
        )
    if profitability_context_text:
        embed["fields"].insert(
            3,
            {
                "name": "獲利率脈絡（不另計訊號家族）",
                "value": profitability_context_text,
                "inline": False,
            },
        )
    if earnings_context_text:
        embed["fields"].insert(
            4,
            {
                "name": "財報 EPS 脈絡（不計分）",
                "value": earnings_context_text,
                "inline": False,
            },
        )
    if balance_sheet_context_text:
        embed["fields"].insert(
            5,
            {
                "name": "資產負債脈絡（不計分）",
                "value": balance_sheet_context_text,
                "inline": False,
            },
        )
    if valuation_context_text:
        embed["fields"].insert(
            6,
            {
                "name": "每日估值脈絡（不計分）",
                "value": valuation_context_text,
                "inline": False,
            },
        )
    corporate_action_lines = []
    for item in (detail.get("upcoming_corporate_actions") or [])[:2]:
        action_date = _date_only(item.get("effective_date"))
        action_label = _escape_discord_markdown(item.get("action"), 32)
        if not action_date:
            continue
        line = (
            f"• 預定除權息 {action_date:%Y/%m/%d}｜"
            f"{action_label or '公告類型未提供'}"
        )
        cash_dividend = _number(item.get("cash_dividend_per_share"))
        if cash_dividend is not None:
            line += f"｜每股現金股利 NT${cash_dividend:.4f}"
        source_url = _official_source_url(item.get("source_url"))
        if source_url:
            line += f"｜[交易所公告批次]({source_url})"
        corporate_action_lines.append(line)
    if corporate_action_lines:
        corporate_action_lines.append(
            "來源列於掃描開始前已入庫；公告本身未附發布時刻。只作公司行動脈絡、不列多空分數；未提供的數值保留缺值。"
        )
        embed["fields"].append(
            {
                "name": "交易所除權息預告（事件背景；不計分）",
                "value": "\n".join(corporate_action_lines)[:1_000],
                "inline": False,
            }
        )
    financing_context = detail.get("official_financing_context") or []
    financing_lines = []
    for item in financing_context[:2]:
        event_date = _escape_discord_markdown(item.get("event_date"), 16)
        cash_received = _number(item.get("cash_received_ntd"))
        parent_equity = _number(item.get("parent_equity_ntd"))
        equity_period = _escape_discord_markdown(item.get("parent_equity_period"), 16)
        ratio_pct = _number(item.get("cash_to_parent_equity_pct"))
        line = f"• 公司公告列日期 {event_date or '未提供'}｜可轉債應募款已收足"
        if cash_received is not None:
            line += f" NT${cash_received:,.0f}"
        if parent_equity is not None and ratio_pct is not None:
            line += (
                f"，約為 {equity_period or '最新已披露'}期末歸屬母公司權益 "
                f"NT${parent_equity:,.0f} 的 {ratio_pct:.1f}%"
            )
        financing_source_url = _official_source_url(item.get("source_url"))
        equity_source_url = _official_source_url(item.get("parent_equity_source_url"))
        links = []
        if financing_source_url:
            links.append(f"[收款公告]({financing_source_url})")
        if equity_source_url:
            links.append(f"[權益資料]({equity_source_url})")
        if links:
            line += "｜" + "、".join(links)
        financing_lines.append(line)
    if financing_lines:
        financing_lines.append(
            "融資背景，不列營收、獲利或方向家族；公告未列轉換價/換股股數，"
            "不能把募資與帳面權益比率當作實際稀釋率。僅當規模達母公司權益20%才顯示，"
            "這是脈絡篩選門檻，不是方向或預測門檻。"
        )
        embed["fields"].append(
            {
                "name": "官方融資脈絡（不計入多空訊號）",
                "value": "\n".join(financing_lines)[:1_000],
                "inline": False,
            }
        )
    regulatory_context = detail.get("regulatory_context") or []
    regulatory_lines = []
    for item in regulatory_context[:2]:
        context_details = item.get("details") or {}
        if item.get("event_type") == "trading_status_snapshot":
            labels = {"suspended": "停止交易", "altered": "變更交易",
                      "periodic": "分盤交易", "managed": "管理股票"}
            status = "、".join(label for key, label in labels.items()
                               if context_details.get(key) is True)
            if status:
                source_url = _official_source_url(item.get("source_url"))
                date_basis = ("觀測日，來源未列報表日期" if
                              context_details.get("date_basis") == "observed_date_no_report_date"
                              else "官方資料日")
                regulatory_lines.append(
                    f"• {status}｜{date_basis} {item.get('effective_date')}"
                    + (f"｜[官方限制名單]({source_url})" if source_url else "")
                )
        elif item.get("event_type") == "trading_attention_notice":
            attention_info = _escape_discord_markdown(
                context_details.get("attention_info"), 250
            )
            source_url = _official_source_url(item.get("source_url"))
            notice_link = f"[官方原始資料]({source_url})" if source_url else ""
            regulatory_lines.append(
                f"• 注意交易資訊 {item.get('effective_date')}｜"
                f"{attention_info or '官方公告有注意交易資訊'}"
                + (f"｜{notice_link}" if notice_link else "")
            )
        elif item.get("event_type") == "trading_disposition":
            start_date = _escape_discord_markdown(context_details.get("start_date"), 16)
            end_date = _escape_discord_markdown(context_details.get("end_date"), 16)
            reason = _escape_discord_markdown(context_details.get("reason"), 100)
            measures = _escape_discord_markdown(
                context_details.get("operation_measures") or context_details.get("measures"),
                220,
            )
            period_text = f"{start_date} 至 {end_date}" if start_date and end_date else "有效期間內"
            active_from = _date_only(context_details.get("start_date"))
            disposition_status = "即將處置" if date_value and active_from and date_value < active_from else "處置中"
            reason_text = f"｜原因：{reason}" if reason else ""
            measure_text = f"｜措施：{measures}" if measures else ""
            source_url = _official_source_url(item.get("source_url"))
            disposition_link = f"[官方原始資料]({source_url})" if source_url else ""
            regulatory_lines.append(
                f"• {disposition_status} {period_text}{reason_text}{measure_text}"
                + (f"｜{disposition_link}" if disposition_link else "")
            )
    if regulatory_lines:
        regulatory_lines.append("官方監理風險／交易限制背景；不代表多空方向，也不列入訊號家族。")
        embed["fields"].append(
            {
                "name": "官方交易限制與監理風險",
                "value": "\n".join(regulatory_lines),
                "inline": False,
            }
        )
    news_context = detail.get("news_context") or {}
    if news_context:
        news_matches = news_context.get("matches") or []
        if news_matches:
            news_lines = []
            for item in news_matches[:NEWS_CONTEXT_MATCH_LIMIT]:
                source = _escape_discord_markdown(item.get("source"), 24)
                published_date = _escape_discord_markdown(item.get("published_date"), 16)
                source_time_field = _escape_discord_markdown(
                    item.get("source_time_field"), 24
                )
                time_precision = item.get("publication_time_precision")
                if time_precision == "date_only":
                    published_label = f"{source_time_field or published_date}（僅日期）"
                elif source_time_field:
                    published_label = (
                        f"{source_time_field}（資料庫 created_at；時區/原頁未核）"
                    )
                elif published_date and published_date != "日期未提供":
                    published_label = f"{published_date}（發稿時分未保存）"
                else:
                    published_label = "發稿時間欄缺值"
                headline = _escape_discord_markdown(item.get("title"), 125)
                article_url = _safe_news_article_url(item.get("url"))
                if item.get("title_missing"):
                    excerpt = _escape_discord_markdown(
                        item.get("match_excerpt"), NEWS_CONTEXT_MATCH_EXCERPT_CHARS
                    )
                    evidence_label = (
                        f"正文前 {NEWS_CONTEXT_BODY_LEAD_CHARS} 字命中「{excerpt}」"
                        if excerpt else f"正文前 {NEWS_CONTEXT_BODY_LEAD_CHARS} 字命中：{headline}"
                    )
                elif item.get("match_basis") == "content_lead":
                    excerpt = _escape_discord_markdown(
                        item.get("match_excerpt"), NEWS_CONTEXT_MATCH_EXCERPT_CHARS
                    )
                    evidence_label = (
                        f"正文前 {NEWS_CONTEXT_BODY_LEAD_CHARS} 字命中「{excerpt}」｜標題：{headline}"
                        if excerpt else f"正文前 {NEWS_CONTEXT_BODY_LEAD_CHARS} 字命中｜標題：{headline}"
                    )
                else:
                    evidence_label = f"標題命中：{headline}"
                same_headline_source_count = int(
                    item.get("same_headline_source_count") or 0
                )
                if same_headline_source_count > 1:
                    evidence_label += (
                        f"；相同標題見 {same_headline_source_count} 個來源，"
                        "不視為獨立佐證"
                    )
                label = f"{source}｜{published_label}｜{evidence_label}"
                news_lines.append(
                    f"• [{label}]({article_url})" if article_url else f"• {label}"
                )
            news_value = "\n".join(news_lines)
            news_value += (
                "\n只作媒體背景；標題或內文首段命中均不參與多空判斷。"
                "僅比對訊號掃描開始前已收錄的新聞。"
            )
            if news_context.get("sample_truncated"):
                news_value += (
                    f"\n新聞樣本已截斷：掃描前已收錄、符合時間及內容條件的列，"
                    f"按最新 ID 最多取 "
                    f"{int(news_context.get('ingest_scan_limit') or NEWS_CONTEXT_INGEST_SCAN_LIMIT):,} 篇；"
                    f"本次取到 {int(news_context.get('eligible_count') or 0):,} 篇，"
                    f"僅比對其中 {int(news_context.get('sample_count') or 0):,} 篇。"
                )
            if any(
                item.get("publication_time_precision") == "date_only"
                for item in news_matches
            ):
                news_value += (
                    "\n日期精度列按 cutoff 日期納入；實際發稿時分與時區未核，"
                    "不代表嚴格 48 小時內發布。"
                )
        elif news_context.get("status") == "unavailable":
            news_value = "新聞資料不可讀取；本卡沒有新聞交叉比對。"
        else:
            eligible_count = news_context.get("eligible_count")
            sample_count = int(news_context.get("sample_count") or 0)
            sample_limit = int(news_context.get("sample_limit") or NEWS_CONTEXT_SAMPLE_LIMIT)
            scan_limit = int(news_context.get("ingest_scan_limit") or NEWS_CONTEXT_INGEST_SCAN_LIMIT)
            if eligible_count is None:
                coverage = "符合時間且有標題/內文的筆數無法統計"
            else:
                coverage = (
                    f"掃描前已收錄且符合時間/內容條件的列中，按最新 ID 最多取 "
                    f"{scan_limit:,} 篇；本次取到 {int(eligible_count):,} 篇，"
                    f"實際比對 {sample_count:,} 篇"
                )
                if news_context.get("sample_truncated"):
                    coverage += "（達樣本上限，結果不完整）"
            news_value = (
                f"精確時間欄近 {NEWS_CONTEXT_LOOKBACK_HOURS} 小時；午夜/日期精度列另按 cutoff 日期納入，"
                "可能超出精確窗口，原頁/時區未核；先掃最新 "
                f"{coverage}（上限 {sample_limit:,}）；"
                "標題與內文首段未找到公司全名；"
                "不代表沒有相關報導。新聞僅取訊號掃描開始前已收錄的資料。"
            )
        embed["fields"].append(
            {"name": "新聞背景（不計入多空訊號）", "value": news_value, "inline": False}
        )
    return embed


def _load_daily_market_breadth(
    cursor, as_of_date: dt.date | None, started_at: dt.datetime | None,
    universe_market_counts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize PIT same-date CFI ES* quote changes without affecting scores."""

    if not as_of_date or not started_at:
        return {"markets": {}}
    updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    universe_market_counts = universe_market_counts or {}
    source_markets = {
        "twse_equity_quotes_daily": "上市",
        "tpex_equity_quotes_daily": "上櫃",
    }
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT source_key, COUNT(DISTINCT entity_id) AS quote_count,
               COUNT(DISTINCT CASE WHEN quality_status='ok' THEN entity_id END) AS accepted_count,
               COUNT(DISTINCT CASE WHEN quality_status<>'ok' THEN entity_id END) AS quarantined_count,
               COUNT(DISTINCT CASE WHEN quality_status='ok' AND metric_value>0 THEN entity_id END) AS advancers,
               COUNT(DISTINCT CASE WHEN quality_status='ok' AND metric_value<0 THEN entity_id END) AS decliners,
               COUNT(DISTINCT CASE WHEN quality_status='ok' AND metric_value=0 THEN entity_id END) AS unchanged,
               COUNT(DISTINCT CASE WHEN quality_status='ok' AND metric_value IS NULL THEN entity_id END) AS unclassified
        FROM market_context_snapshot
        WHERE source_key IN (%s, %s) AND event_type='market_quote_daily'
          AND entity_type='stock' AND effective_date=%s
        GROUP BY source_key
        """,
        (updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
         *source_markets.keys(), as_of_date),
    )
    markets: dict[str, dict[str, int]] = {}
    for (source_key, quote_count, accepted_count, quarantined_count,
         advancers, decliners, unchanged, unclassified) in cursor.fetchall():
        market_label = source_markets[str(source_key)]
        markets[source_markets[str(source_key)]] = {
            "quote_count": int(quote_count or 0),
            "accepted_count": int(accepted_count or 0),
            "quarantined_count": int(quarantined_count or 0),
            "advancers": int(advancers or 0),
            "decliners": int(decliners or 0),
            "unchanged": int(unchanged or 0),
            "unclassified": int(unclassified or 0),
            "universe_count": int(universe_market_counts.get(market_label) or 0),
        }
    for market_label, source_key in source_markets.items():
        markets.setdefault(
            market_label,
            {
                "quote_count": 0, "accepted_count": 0,
                "quarantined_count": 0, "advancers": 0,
                "decliners": 0, "unchanged": 0, "unclassified": 0,
                "universe_count": int(universe_market_counts.get(market_label) or 0),
            },
        )
    return {"markets": markets}


def _load_tpex_daytrade_market_context(
    cursor, as_of_date: dt.date | None, started_at: dt.datetime | None,
) -> dict[str, Any]:
    """Load same-date TPEx day-trade market aggregate as non-directional context."""

    empty = {
        "source_date": None,
        "quality_status": None,
        "values": None,
        "five_session_change_pp": None,
        "complete_session_count": 0,
    }
    if not as_of_date or not started_at:
        return empty
    updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    source_key = "tpex_daytrade_market_summary_daily"
    event_type = "market_daytrade_aggregate_daily"
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT effective_date, quality_status, metric_value, value_text
        FROM market_context_snapshot
        WHERE source_key=%s AND event_type=%s AND entity_type='macro'
          AND effective_date<=%s
        ORDER BY effective_date DESC, id DESC
        LIMIT 1
        """,
        (updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
         source_key, event_type, as_of_date),
    )
    latest = cursor.fetchone()
    if not latest:
        return empty
    source_date = _date_only(latest[0])
    context = {**empty, "source_date": source_date, "quality_status": str(latest[1] or "unknown")}
    if context["quality_status"] != "ok":
        return context
    try:
        details = json.loads(latest[3] or "{}")
    except (TypeError, ValueError):
        return {**context, "quality_status": "invalid_payload"}
    required_values = (
        "daytrade_volume_shares", "daytrade_volume_share_pct",
        "daytrade_buy_value_twd", "daytrade_buy_value_share_pct",
        "daytrade_sell_value_twd", "daytrade_sell_value_share_pct",
    )
    values = {key: _number(details.get(key)) for key in required_values}
    if any(value is None for value in values.values()):
        return {**context, "quality_status": "invalid_payload"}
    context["values"] = values

    if source_date != as_of_date:
        return context

    # Use official TPEx index dates, not an assumed sequence of aggregate rows.
    # End the comparison at T-2 because the exchange says T-day values can be
    # revised through T+2; the same-day reading remains preliminary context.
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT DISTINCT effective_date
        FROM market_context_snapshot
        WHERE source_key='tpex_market_index_daily'
          AND event_type='market_index_daily' AND entity_type='macro'
          AND quality_status='ok' AND effective_date<=%s
        ORDER BY effective_date DESC
        LIMIT 8
        """,
        (updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
         as_of_date),
    )
    session_dates = [_date_only(row[0]) for row in cursor.fetchall()]
    session_dates = [value for value in session_dates if value]
    if len(session_dates) != 8 or session_dates[0] != as_of_date:
        return context
    placeholders = ",".join(["%s"] * len(session_dates))
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + f"""
        SELECT effective_date, quality_status, metric_value, value_text
        FROM market_context_snapshot
        WHERE source_key=%s AND event_type=%s AND entity_type='macro'
          AND effective_date IN ({placeholders})
        ORDER BY effective_date DESC
        """,
        (updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
         source_key, event_type, *session_dates),
    )
    history: dict[dt.date, tuple[str, float | None]] = {}
    for effective_date, quality_status, metric_value, value_text in cursor.fetchall():
        period = _date_only(effective_date)
        try:
            detail = json.loads(value_text or "{}")
        except (TypeError, ValueError):
            detail = {}
        value = _number(detail.get("daytrade_volume_share_pct"))
        if value is None:
            value = _number(metric_value)
        if period:
            history[period] = (str(quality_status or "unknown"), value)
    if all(
        period in history and history[period][0] == "ok" and history[period][1] is not None
        for period in session_dates
    ):
        context["five_session_change_pp"] = (
            history[session_dates[2]][1] - history[session_dates[7]][1]
        )
        context["five_session_change_end_date"] = session_dates[2]
        context["five_session_change_start_date"] = session_dates[7]
        context["complete_session_count"] = 8
    return context


def _load_daily_market_total_return_context(
    cursor, as_of_date: dt.date | None, started_at: dt.datetime | None,
) -> dict[str, dict[str, Any]]:
    """Load point-in-time market total-return context, separate from stock scores."""

    source_definitions = {
        "上市": ("twse_market_total_return_index_daily", "TAIEX_TR"),
        "上櫃": ("tpex_market_total_return_index_daily", "TPEX_TR"),
    }
    empty = {
        market: {
            "source_date": None,
            "quality_status": None,
            "index_code": index_code,
            "five_session_return_pct": None,
            "window_start_date": None,
            "window_end_date": None,
            "complete_session_count": 0,
        }
        for market, (_, index_code) in source_definitions.items()
    }
    if not as_of_date or not started_at:
        return empty

    updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    context_by_market: dict[str, dict[str, Any]] = {}
    for market, (source_key, index_code) in source_definitions.items():
        cursor.execute(
            MARKET_CONTEXT_ASOF_CTE + """
            SELECT effective_date, quality_status, metric_value
            FROM market_context_snapshot
            WHERE source_key=%s AND event_type='market_total_return_index_daily'
              AND entity_type='macro' AND effective_date<=%s
            ORDER BY effective_date DESC, id DESC
            LIMIT 6
            """,
            (updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
             source_key, as_of_date),
        )
        rows = cursor.fetchall()
        if not rows:
            context_by_market[market] = dict(empty[market])
            continue

        dates = [_date_only(row[0]) for row in rows]
        latest_date = dates[0]
        context = {
            **empty[market],
            "source_date": latest_date,
            "quality_status": str(rows[0][1] or "unknown"),
        }
        context_by_market[market] = context
        if context["quality_status"] != "ok" or latest_date != as_of_date:
            continue

        dated_values: dict[dt.date, float] = {}
        invalid_dates: set[dt.date] = set()
        for effective_date, quality_status, metric_value in rows:
            period = _date_only(effective_date)
            value = _number(metric_value)
            if not period:
                continue
            if (
                str(quality_status or "unknown") != "ok"
                or value is None
                or not math.isfinite(value)
                or value <= 0
                or period in dated_values
            ):
                invalid_dates.add(period)
            else:
                dated_values[period] = value

        context["complete_session_count"] = sum(
            period in dated_values and period not in invalid_dates
            for period in set(dates)
            if period is not None
        )
        if (
            len(rows) != 6
            or len(set(dates)) != 6
            or any(period is None for period in dates)
            or len(dated_values) != 6
            or invalid_dates
        ):
            continue

        ordered_dates = [period for period in dates if period is not None]
        current_value = dated_values[ordered_dates[0]]
        starting_value = dated_values[ordered_dates[-1]]
        context["five_session_return_pct"] = (
            (current_value / starting_value - 1.0) * 100.0
        )
        context["window_start_date"] = ordered_dates[-1]
        context["window_end_date"] = ordered_dates[0]

    return context_by_market


def _load_taifex_market_positioning(
    cursor, as_of_date: dt.date | None, started_at: dt.datetime | None,
) -> dict[str, Any]:
    """Load point-in-time index-futures positions as non-directional context."""

    expected_keys = {
        (contract_code, institution_type)
        for contract_code in TAIFEX_POSITION_CONTRACTS
        for institution_type in TAIFEX_POSITION_INSTITUTIONS
    }
    if not as_of_date or not started_at:
        return {
            "as_of_date": None, "source_date": None, "expected_count": len(expected_keys),
            "positions": {}, "five_session_changes": {}, "common_date_count": 0,
        }
    updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT entity_id, effective_date, metric_value, value_text
        FROM market_context_snapshot
        WHERE source_key=%s AND event_type=%s AND entity_type='macro'
          AND quality_status='ok' AND effective_date<=%s
        ORDER BY effective_date DESC, entity_id
        LIMIT 42
        """,
        (
            updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
            TAIFEX_POSITION_SOURCE_KEY, TAIFEX_POSITION_EVENT_TYPE,
            as_of_date,
        ),
    )
    histories: dict[tuple[str, str], list[dict[str, Any]]] = {
        key: [] for key in expected_keys
    }
    for entity_id, effective_date, metric_value, value_text in cursor.fetchall():
        try:
            detail = json.loads(value_text or "{}")
        except (TypeError, ValueError):
            continue
        key = (
            str(detail.get("contract_code") or ""),
            str(detail.get("institution_type") or ""),
        )
        if key not in histories:
            continue
        histories[key].append(
            {
                "effective_date": _date_only(effective_date),
                "net_open_interest_lots": _number(metric_value),
            }
        )

    latest_dates = [
        history[0]["effective_date"] for history in histories.values() if history
    ]
    source_date = max(latest_dates) if latest_dates else None
    positions: dict[tuple[str, str], dict[str, Any]] = {}
    for key, history in histories.items():
        if history and history[0]["effective_date"] == source_date:
            positions[key] = history[0]

    common_dates: set[dt.date] | None = None
    for history in histories.values():
        dates = {item["effective_date"] for item in history if item["effective_date"]}
        common_dates = dates if common_dates is None else common_dates & dates
    ordered_common_dates = sorted(common_dates or set(), reverse=True)
    five_session_changes: dict[tuple[str, str], float] = {}
    if as_of_date in ordered_common_dates and len(ordered_common_dates) >= 6:
        baseline_date = ordered_common_dates[5]
        for key, history in histories.items():
            by_date = {item["effective_date"]: item for item in history}
            current = by_date.get(as_of_date)
            baseline = by_date.get(baseline_date)
            if (
                current and baseline
                and current["net_open_interest_lots"] is not None
                and baseline["net_open_interest_lots"] is not None
            ):
                five_session_changes[key] = (
                    current["net_open_interest_lots"]
                    - baseline["net_open_interest_lots"]
                )
    return {
        "as_of_date": as_of_date,
        "source_date": source_date,
        "expected_count": len(expected_keys),
        "positions": positions,
        "five_session_changes": five_session_changes,
        "common_date_count": len(ordered_common_dates),
    }


def _load_taifex_market_option_positioning(
    cursor, as_of_date: dt.date | None, started_at: dt.datetime | None,
) -> dict[str, Any]:
    """Load index-option calls/puts as aggregate, non-directional context."""

    expected_keys = {
        (TAIFEX_OPTION_POSITION_CONTRACT, call_put, institution_type)
        for call_put in TAIFEX_OPTION_POSITION_CALL_PUTS
        for institution_type in TAIFEX_POSITION_INSTITUTIONS
    }
    if not as_of_date or not started_at:
        return {
            "as_of_date": None, "source_date": None, "expected_count": len(expected_keys),
            "positions": {}, "five_session_changes": {}, "common_date_count": 0,
        }
    updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT entity_id, effective_date, metric_value, value_text
        FROM market_context_snapshot
        WHERE source_key=%s AND event_type=%s AND entity_type='macro'
          AND quality_status='ok' AND effective_date<=%s
        ORDER BY effective_date DESC, entity_id
        LIMIT 42
        """,
        (
            updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
            TAIFEX_OPTION_POSITION_SOURCE_KEY, TAIFEX_POSITION_EVENT_TYPE,
            as_of_date,
        ),
    )
    histories: dict[tuple[str, str, str], list[dict[str, Any]]] = {
        key: [] for key in expected_keys
    }
    for entity_id, effective_date, metric_value, value_text in cursor.fetchall():
        try:
            detail = json.loads(value_text or "{}")
        except (TypeError, ValueError):
            continue
        key = (
            str(detail.get("contract_code") or ""),
            str(detail.get("call_put") or "").upper(),
            str(detail.get("institution_type") or ""),
        )
        if key not in histories:
            continue
        histories[key].append(
            {
                "effective_date": _date_only(effective_date),
                "net_open_interest_lots": _number(metric_value),
            }
        )

    latest_dates = [history[0]["effective_date"] for history in histories.values() if history]
    source_date = max(latest_dates) if latest_dates else None
    positions: dict[tuple[str, str, str], dict[str, Any]] = {}
    for key, history in histories.items():
        if history and history[0]["effective_date"] == source_date:
            positions[key] = history[0]

    common_dates: set[dt.date] | None = None
    for history in histories.values():
        dates = {item["effective_date"] for item in history if item["effective_date"]}
        common_dates = dates if common_dates is None else common_dates & dates
    ordered_common_dates = sorted(common_dates or set(), reverse=True)
    five_session_changes: dict[tuple[str, str, str], float] = {}
    if as_of_date in ordered_common_dates and len(ordered_common_dates) >= 6:
        baseline_date = ordered_common_dates[5]
        for key, history in histories.items():
            by_date = {item["effective_date"]: item for item in history}
            current = by_date.get(as_of_date)
            baseline = by_date.get(baseline_date)
            if (
                current and baseline
                and current["net_open_interest_lots"] is not None
                and baseline["net_open_interest_lots"] is not None
            ):
                five_session_changes[key] = (
                    current["net_open_interest_lots"]
                    - baseline["net_open_interest_lots"]
                )
    return {
        "as_of_date": as_of_date,
        "source_date": source_date,
        "expected_count": len(expected_keys),
        "positions": positions,
        "five_session_changes": five_session_changes,
        "common_date_count": len(ordered_common_dates),
    }


def _load_taifex_large_trader_context(
    cursor, as_of_date: dt.date | None, started_at: dt.datetime | None,
) -> dict[str, Any]:
    """Load the all-contract TX row as PIT market concentration context."""

    if not as_of_date or not started_at:
        return {"as_of_date": None, "source_date": None, "quality_status": "unknown"}
    updated_at_cutoff = _source_updated_at_cutoff_utc(started_at)
    cursor.execute(
        MARKET_CONTEXT_ASOF_CTE + """
        SELECT effective_date, quality_status, value_text
        FROM market_context_snapshot
        WHERE source_key=%s AND event_type=%s AND entity_type='macro'
          AND entity_id=%s AND effective_date<=%s
        ORDER BY effective_date DESC
        LIMIT 6
        """,
        (
            updated_at_cutoff, updated_at_cutoff, updated_at_cutoff, updated_at_cutoff,
            TAIFEX_LARGE_TRADER_SOURCE_KEY, TAIFEX_LARGE_TRADER_EVENT_TYPE,
            "TX::999912::0", as_of_date,
        ),
    )
    observations: list[dict[str, Any]] = []
    for effective_date, quality_status, value_text in cursor.fetchall():
        try:
            detail = json.loads(value_text or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(detail, dict):
            continue
        observations.append(
            {
                "effective_date": _date_only(effective_date),
                "quality_status": str(quality_status or "unknown"),
                "top5_buy_lots": _number(detail.get("top5_buy_lots")),
                "top5_sell_lots": _number(detail.get("top5_sell_lots")),
                "top10_buy_lots": _number(detail.get("top10_buy_lots")),
                "top10_sell_lots": _number(detail.get("top10_sell_lots")),
                "market_open_interest_lots": _number(
                    detail.get("market_open_interest_lots")
                ),
            }
        )
    current = next(
        (item for item in observations if item["effective_date"] == as_of_date),
        None,
    )
    latest = observations[0] if observations else None
    return {
        "as_of_date": as_of_date,
        "source_date": latest.get("effective_date") if latest else None,
        "quality_status": current.get("quality_status") if current else (
            latest.get("quality_status") if latest else "missing"
        ),
        "values": current,
        "observed_session_count": len(
            {item["effective_date"] for item in observations if item["effective_date"]}
        ),
    }


def _format_tpex_daytrade_market_context(
    context: dict[str, Any], as_of_date: dt.date | None,
) -> str:
    scan_label = as_of_date.strftime("%Y/%m/%d") if as_of_date else "未提供"
    source_date = _date_only(context.get("source_date"))
    if not source_date:
        return (
            f"上櫃市場當沖彙總截至 {scan_label} 尚無可用列；缺資料不代表零當沖量。"
            "本來源只有 TPEx 市場彙總，不代表上市市場或個股。"
        )
    if context.get("quality_status") != "ok":
        return (
            f"上櫃市場當沖彙總最新列 {source_date:%Y/%m/%d} 未通過品質檢查 "
            f"({context.get('quality_status') or 'unknown'})；不引用數值。"
        )
    if source_date != as_of_date:
        return (
            f"上櫃市場當沖彙總最新有效日 {source_date:%Y/%m/%d}，掃描日 {scan_label}；"
            "日期未對齊，本次不沿用舊值。缺資料不視為零當沖量。"
        )
    values = context.get("values") or {}
    required = (
        "daytrade_volume_shares", "daytrade_volume_share_pct",
        "daytrade_buy_value_twd", "daytrade_buy_value_share_pct",
        "daytrade_sell_value_twd", "daytrade_sell_value_share_pct",
    )
    if any(values.get(key) is None for key in required):
        return "上櫃市場當沖彙總日期已對齊，但欄位不完整；本次不引用數值。"
    lines = [
        f"TPEx {source_date:%Y/%m/%d}｜當沖 {values['daytrade_volume_shares']:,.0f} 股 "
        f"({values['daytrade_volume_share_pct']:.2f}% 市場量)｜"
        f"買 NT${values['daytrade_buy_value_twd']:,.0f} ({values['daytrade_buy_value_share_pct']:.2f}%)｜"
        f"賣 NT${values['daytrade_sell_value_twd']:,.0f} ({values['daytrade_sell_value_share_pct']:.2f}%)"
    ]
    change = context.get("five_session_change_pp")
    end_date = _date_only(context.get("five_session_change_end_date"))
    start_date = _date_only(context.get("five_session_change_start_date"))
    if change is not None and end_date and start_date:
        lines.append(
            f"5 交易日當沖量占比變化 {float(change):+.2f} 個百分點 "
            f"({start_date:%m/%d} 至 {end_date:%m/%d}；區間終點落後掃描日 2 個交易日)"
        )
    else:
        lines.append("5 交易日比較尚未累積 8 個完整上櫃交易日資料。")
    lines.append(
        "上櫃市場彙總，只作交易活躍度背景，不代表上市市場或個股方向，也不計入個股分數；"
        "當日值可能至 T+2 回修，且未保存歷史版本。"
    )
    return "\n".join(lines)


def _format_daily_market_total_return_context(
    context_by_market: dict[str, dict[str, Any]], as_of_date: dt.date | None,
) -> str:
    scan_label = as_of_date.strftime("%Y/%m/%d") if as_of_date else "未提供"
    lines: list[str] = []
    for market in ("上市", "上櫃"):
        context = context_by_market.get(market) or {}
        source_date = _date_only(context.get("source_date"))
        index_code = str(context.get("index_code") or "官方報酬指數")
        if not source_date:
            lines.append(f"{market} {index_code}：尚無可用資料日；不推定市場報酬為零。")
            continue
        if context.get("quality_status") != "ok":
            lines.append(
                f"{market} {index_code}：最新列 {source_date:%Y/%m/%d} 品質狀態 "
                f"{context.get('quality_status') or 'unknown'}；不引用數值。"
            )
            continue
        if source_date != as_of_date:
            lines.append(
                f"{market} {index_code}：最新有效日 {source_date:%Y/%m/%d}，"
                f"掃描日 {scan_label}；未對齊，不沿用舊值。"
            )
            continue

        change = _number(context.get("five_session_return_pct"))
        start_date = _date_only(context.get("window_start_date"))
        end_date = _date_only(context.get("window_end_date"))
        if change is not None and start_date and end_date:
            lines.append(
                f"{market} {index_code} 近 5 交易日總報酬 {change:+.2f}% "
                f"({start_date:%m/%d} 至 {end_date:%m/%d})"
            )
        else:
            count = int(context.get("complete_session_count") or 0)
            lines.append(
                f"{market} {index_code} 資料日已對齊；有效指數點 {count}/6，"
                "尚不計算五日報酬。"
            )

    lines.append(
        "只作市場背景、不加入個股分數；TWSE 資料集說明配股配息再投資，"
        "TPEx 資料依官方『報酬指數』欄位呈現。來源由自然批次累積，缺值不補零。"
    )
    lines.append(
        "來源：[TWSE 11871](https://data.gov.tw/dataset/11871)｜"
        "[TPEx 20219](https://data.gov.tw/dataset/20219)"
    )
    return "\n".join(lines)


def _format_taifex_market_positioning(
    context: dict[str, Any], as_of_date: dt.date | None,
    option_context: dict[str, Any] | None = None,
    large_trader_context: dict[str, Any] | None = None,
) -> str:
    scan_label = as_of_date.strftime("%Y/%m/%d") if as_of_date else "未提供"
    lines: list[str] = []

    futures_date = _date_only(context.get("source_date"))
    if not futures_date:
        lines.append("指數期貨：尚未收集；缺資料不視為零部位。")
    elif futures_date != as_of_date:
        lines.append(
            f"指數期貨：最新資料日 {futures_date:%Y/%m/%d}，掃描日 {scan_label}，"
            "日期未對齊，不沿用舊值。"
        )
    else:
        positions = context.get("positions") or {}
        changes = context.get("five_session_changes") or {}
        lines.append(
            f"指數期貨 {futures_date:%Y/%m/%d}｜{len(positions)}/"
            f"{int(context.get('expected_count') or 0)} 組"
        )
        for contract_code, market_label in TAIFEX_POSITION_CONTRACTS.items():
            components = []
            for institution_type in TAIFEX_POSITION_INSTITUTIONS:
                key = (contract_code, institution_type)
                position = positions.get(key)
                if not position or position.get("net_open_interest_lots") is None:
                    continue
                value = float(position["net_open_interest_lots"])
                text = f"{institution_type} {value:+,.0f}口"
                change = changes.get(key)
                if change is not None:
                    text += f"（5日 {float(change):+,.0f}）"
                components.append(text)
            if components:
                lines.append(f"{market_label}：" + "；".join(components))

    option_context = option_context or {}
    option_date = _date_only(option_context.get("source_date"))
    if not option_date:
        lines.append("臺指選擇權：尚未收集；缺資料不視為零部位。")
    elif option_date != as_of_date:
        lines.append(
            f"臺指選擇權：最新資料日 {option_date:%Y/%m/%d}，掃描日 {scan_label}，"
            "日期未對齊，不沿用舊值。"
        )
    else:
        positions = option_context.get("positions") or {}
        changes = option_context.get("five_session_changes") or {}
        lines.append(
            f"臺指選擇權 {option_date:%Y/%m/%d}｜{len(positions)}/"
            f"{int(option_context.get('expected_count') or 0)} 組"
        )
        for call_put in TAIFEX_OPTION_POSITION_CALL_PUTS:
            components = []
            for institution_type in TAIFEX_POSITION_INSTITUTIONS:
                key = (TAIFEX_OPTION_POSITION_CONTRACT, call_put, institution_type)
                position = positions.get(key)
                if not position or position.get("net_open_interest_lots") is None:
                    continue
                value = float(position["net_open_interest_lots"])
                text = f"{institution_type} {value:+,.0f}口"
                change = changes.get(key)
                if change is not None:
                    text += f"（5日 {float(change):+,.0f}）"
                components.append(text)
            if components:
                lines.append(f"{call_put}：" + "；".join(components))

    large_trader_context = large_trader_context or {}
    large_trader_date = _date_only(large_trader_context.get("source_date"))
    large_values = large_trader_context.get("values") or {}
    if not large_trader_date:
        lines.append("期貨大額交易人：尚未收集；缺資料不視為零部位。")
    elif large_trader_date != as_of_date:
        lines.append(
            f"期貨大額交易人：最新資料日 {large_trader_date:%Y/%m/%d}，掃描日 {scan_label}，"
            "日期未對齊，不沿用舊值。"
        )
    elif large_trader_context.get("quality_status") != "ok":
        lines.append("期貨大額交易人：有效日已對齊但列未通過品質檢查，不引用數值。")
    elif any(
        large_values.get(key) is None
        for key in (
            "top5_buy_lots", "top5_sell_lots", "top10_buy_lots",
            "top10_sell_lots", "market_open_interest_lots",
        )
    ):
        lines.append("期貨大額交易人：有效日已對齊但欄位不完整，不引用數值。")
    else:
        lines.append(
            f"期貨大額交易人 {large_trader_date:%Y/%m/%d}｜TX 所有契約（原始類別碼 0）｜"
            f"前5買 {large_values['top5_buy_lots']:,.0f}、前5賣 {large_values['top5_sell_lots']:,.0f}；"
            f"前10買 {large_values['top10_buy_lots']:,.0f}、前10賣 {large_values['top10_sell_lots']:,.0f}；"
            f"市場總未平倉 {large_values['market_open_interest_lots']:,.0f} 口"
        )

    lines.extend(
        [
            "三類法人是多機構彙總；選擇權多腿策略不等於單純看多/看空。大額交易人買/賣兩側分開排名，不能相減成淨部位。僅作市場背景，不計個股方向分數。",
            "來源：[TAIFEX 官方 OpenAPI](https://openapi.taifex.com.tw/)｜"
            "[期貨法人欄位](https://www.taifex.com.tw/cht/3/futContractsDate)｜"
            "[選擇權買權/賣權統計](https://www.taifex.com.tw/cht/3/callsAndPutsDate)｜"
            "[大額交易人開放資料 11337](https://data.gov.tw/dataset/11337)",
        ]
    )
    return "\n".join(lines)


def _format_daily_embeds(
    run: dict[str, Any], candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    as_of_date = _date_only(run.get("as_of_date"))
    as_of = as_of_date.strftime("%Y/%m/%d") if as_of_date else "無新收盤資料"
    parsed: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for row in candidates:
        try:
            detail = json.loads(row["details_text"])
        except (TypeError, ValueError):
            detail = {}
        parsed.append((row, detail))
    bullish = [(row, detail) for row, detail in parsed if detail.get("direction") == "偏多"]
    bearish = [(row, detail) for row, detail in parsed if detail.get("direction") == "偏空"]
    mixed = [(row, detail) for row, detail in parsed if detail.get("direction") == "訊號分歧"]
    source_status = str(run.get("source_status", "unknown"))
    source_status_label = {
        "fresh": "達最低資料檢查（仍非 100% 覆蓋）",
        "partial": "已依可用資料判讀；部分來源缺列或日期待核",
        "unknown": "資料狀態未知，暫不可判讀",
    }.get(source_status, "狀態未知")
    shown_bullish = bullish[:min(DAILY_DIGEST_LIMIT, 6)]
    cautions = (bearish + mixed)[:2]
    remainder = max(0, len(bullish) - len(shown_bullish)) + max(0, len(bearish) + len(mixed) - len(cautions))
    universe_count = int(run.get("universe_count") or 0)
    price_count = int(run.get("price_count") or 0)
    flow_count = int(run.get("flow_count") or 0)
    price_coverage = price_count / universe_count * 100 if universe_count else 0
    flow_coverage = flow_count / universe_count * 100 if universe_count else 0
    market_coverage = run.get("market_coverage_by_market") or {}
    market_coverage_lines = []
    for market in ("上市", "上櫃"):
        values = market_coverage.get(market)
        if not values:
            continue
        market_universe = int(values.get("universe_count") or 0)
        market_price = int(values.get("price_count") or 0)
        price_row_codes = values.get("price_row_code_count")
        invalid_close_codes = values.get("price_invalid_close_code_count")
        no_trade_codes = values.get("price_no_trade_code_count")
        market_flow = int(values.get("flow_count") or 0)
        traded_without_flow = values.get("traded_without_flow_count")
        market_price_pct = float(values.get("price_coverage_pct") or 0)
        market_flow_pct = float(values.get("flow_coverage_pct") or 0)
        market_coverage_lines.append(
            f"{market}：收盤行情 {market_price:,}/{market_universe:,} 檔 "
            f"({market_price_pct:.1f}%)｜同日可判讀法人 {market_flow:,}/"
            f"{market_universe:,} 檔 ({market_flow_pct:.1f}%)"
            + (
                f"｜該日行情列 {int(price_row_codes):,} 檔、無效收盤 "
                f"{int(invalid_close_codes):,} 檔"
                if price_row_codes is not None and invalid_close_codes is not None
                else (
                    "｜歷史快照未留存日行情列/無效收盤分項"
                    if price_row_codes is None else ""
                )
            )
            + (
                f"、已知零成交 {int(no_trade_codes):,} 檔"
                if no_trade_codes is not None else ""
            )
            + (
                f"｜有成交但法人同日缺列 {int(traded_without_flow):,} 檔（未知）"
                if traded_without_flow is not None and values.get("flow_readiness")
                else (
                    "｜舊快照當時未讀入上市法人表，未保存逐檔缺列數"
                    if (values.get("flow_readiness") or {}).get("status") == "source_not_authorized"
                    else "｜歷史快照未留存同日法人來源缺列口徑"
                )
            )
        )
    coverage_value = (
        "\n".join(market_coverage_lines)
        if market_coverage_lines
        else (
            f"全市場收盤有效資料 {price_count:,}/{universe_count:,} 檔 "
            f"({price_coverage:.1f}%)\n"
            f"全市場同日可判讀法人 {flow_count:,}/{universe_count:,} 檔 "
            f"({flow_coverage:.1f}%)"
        )
    )
    coverage_value += (
        "\n缺少證券列保持未知，不補成零；來源狀態的覆蓋門檻分上市/上櫃檢查。"
    )
    benchmark_alignment = run.get("benchmark_alignment_by_market")
    benchmark_lines = []
    if isinstance(benchmark_alignment, dict):
        for market, index_code in (("上市", "TAIEX"), ("上櫃", "TPEX")):
            values = benchmark_alignment.get(market)
            if not isinstance(values, dict):
                continue
            latest_date = _escape_discord_markdown(
                values.get("latest_effective_date"), 16
            )
            latest_valid_date = _escape_discord_markdown(
                values.get("latest_valid_effective_date"), 16
            )
            aligned = values.get("aligned_to_scan_date")
            window_count = values.get("window_point_count")
            valid_count = values.get("window_valid_point_count")
            if not latest_date:
                detail = "有效資料日未知"
            else:
                detail = f"資料列日 {latest_date}"
                if latest_valid_date and latest_valid_date != latest_date:
                    detail += f"；有效值截至 {latest_valid_date}"
                if aligned is False:
                    detail += f"；未與掃描日 {as_of} 對齊"
                elif window_count is not None and valid_count is not None:
                    detail += f"；近六點有效 {int(valid_count)}/6"
            benchmark_lines.append(f"{market} {index_code}：{detail}")
    if benchmark_lines:
        coverage_value += "\n官方市場基準：" + "；".join(benchmark_lines)
    elif "benchmark_alignment_by_market" in run:
        coverage_value += "\n官方市場基準日期未取得；不能推定與掃描日對齊。"
    else:
        coverage_value += "\n歷史快照未保存官方市場基準有效日。"
    summary = {
        "title": (
            f"📡 台股收盤訊號｜{as_of}｜部分來源待核"
            if source_status != "fresh"
            else f"📡 台股收盤訊號｜{as_of}"
        ),
        "description": (
            f"本輪掃描 {universe_count:,} 檔，{len(parsed)} 檔達到觀察門檻。"
            "個股卡片列出支持證據、反向風險、相對大盤表現與下一步觀察條件。"
        ),
        "color": 0x34495E,
        "fields": [
            {
                "name": "上市/上櫃資料覆蓋",
                "value": (
                    f"{coverage_value}\n來源狀態：{source_status_label}"
                ),
                "inline": False,
            },
            {
                "name": "市場廣度背景（官方個股漲跌欄；不計入分數）",
                "value": "",
                "inline": False,
            },
            {
                "name": "達觀察門檻的名單分布",
                "value": (
                    f"🟢 偏多 **{len(bullish)}**　🔴 偏空 **{len(bearish)}**　"
                    f"🟡 分歧 **{len(mixed)}**"
                ),
                "inline": False,
            },
            {
                "name": "怎麼讀",
                "value": (
                    "至少 3 個有效資料家族有方向才入列；多空依家族票數差判定，平手列為分歧。"
                    "名單分布只計入達門檻候選，不是全市場漲跌家數；未上榜不代表看空。"
                ),
                "inline": False,
            },
        ],
        "footer": {
            "text": (
                f"資料截至 {as_of}｜顯示偏多前 {len(shown_bullish)} 檔與風險前 "
                f"{len(cautions)} 檔；另 {remainder} 檔未逐張列出"
                if remainder else f"資料截至 {as_of}｜名單已完整顯示"
            )
        },
    }
    flow_embed = {
        "title": "法人與券商分點訊號判讀",
        "color": 0x34495E,
        "description": "使用當時已收的資料，完整 1／3／5 交易日視窗各自判斷；缺資料略過，不補成零。",
        "fields": [{
            "name": "法人來源說明",
            "value": "\n".join(format_flow_readiness(
                market, (market_coverage.get(market) or {}).get("flow_readiness") or {},
                int((market_coverage.get(market) or {}).get("universe_count") or 0),
            ) for market in ("上市", "上櫃")),
            "inline": False,
        }],
    }
    family_counts = run.get("family_trigger_counts")
    if isinstance(family_counts, dict):
        labels = {"price": "價格", "volume": "量能", "institutions": "法人",
                  "broker_branches": "分點", "profitability": "季獲利率"}
        summary["fields"].insert(1, {
            "name": "已判讀並達各項多空條件的股票數",
            "value": "；".join(f"{label} {int(family_counts.get(key) or 0):,} 檔"
                               for key, label in labels.items())
                     + "。這是各項訊號數；多項可能屬於同一股票，合併候選另需至少 3 個方向家族。",
            "inline": False,
        })
    source_issues = [
        str(issue).replace("\n", " ").strip()
        for issue in (run.get("source_issues") or [])
        if str(issue).strip()
    ]
    if source_issues:
        displayed_source_issues = [issue[:110] for issue in source_issues[:7]]
        if len(source_issues) > len(displayed_source_issues):
            displayed_source_issues.append(
                f"…另有 {len(source_issues) - len(displayed_source_issues)} 項"
            )
        summary["fields"].insert(
            1,
            {
                "name": "來源批次問題／覆蓋檢查",
                "value": "\n".join(displayed_source_issues)[:1000],
                "inline": False,
            },
        )
    breadth_markets = (run.get("market_breadth_context") or {}).get("markets") or {}
    breadth_lines = []
    for market_label in ("上市", "上櫃"):
        breadth = breadth_markets.get(market_label)
        if not breadth or not breadth.get("quote_count"):
            universe_count = int((breadth or {}).get("universe_count") or 0)
            denominator = (
                f"接受 0/{universe_count:,} 檔目前主檔證券"
                if universe_count else "目前市場主檔分母未提供"
            )
            breadth_lines.append(
                f"{market_label}：沒有與 {as_of} 同日的已接受開放行情（缺資料，不當作零漲跌）；"
                f"{denominator}。"
            )
            continue
        accepted_count = int(breadth.get("accepted_count") or 0)
        universe_count = int(breadth.get("universe_count") or 0)
        accepted_ratio = (
            f"{accepted_count:,}/{universe_count:,} 檔目前主檔證券"
            f"（{accepted_count / universe_count * 100:.1f}%）"
            if universe_count else f"{accepted_count:,} 檔已接受；主檔分母未提供"
        )
        breadth_lines.append(
            f"{market_label} CFI ES* 行情代碼 {breadth['quote_count']:,}｜"
            f"接受/現行主檔 {accepted_ratio}｜隔離 {breadth['quarantined_count']:,}｜"
            f"上漲 {breadth['advancers']:,}／下跌 {breadth['decliners']:,}／"
            f"平盤 {breadth['unchanged']:,}｜漲跌值未分類 {breadth['unclassified']:,}"
        )
    breadth_lines.extend(
        [
            "母體為當日行情與目前證券主檔的 CFI ES* 交集，並非 TWSE『股票』彙總口徑；只作市場背景。",
            "接受行情代碼/目前主檔數只顯示列報範圍；差額可能含停牌、無交易或採集缺列，未逐一辨識，不補成零成交或持平。",
            "來源：[TWSE 開放個股日成交資訊](https://data.gov.tw/dataset/11549)｜"
            "[TPEx 開放上櫃行情](https://data.gov.tw/dataset/11370)",
        ]
    )
    breadth_field = next(
        (
            field for field in summary["fields"]
            if field.get("name") == "市場廣度背景（官方個股漲跌欄；不計入分數）"
        ),
        None,
    )
    if breadth_field is not None:
        breadth_field["value"] = "\n".join(breadth_lines)
    taifex_context = run.get("taifex_market_positioning") or {}
    summary["fields"].insert(
        2,
        {
            "name": "期交所法人期貨／選擇權部位（市場背景；不計分）",
            "value": _format_taifex_market_positioning(
                taifex_context, as_of_date,
                run.get("taifex_market_option_positioning") or {},
                run.get("taifex_large_trader_context") or {},
            ),
            "inline": False,
        },
    )
    summary["fields"].insert(
        3,
        {
            "name": "上櫃市場現股當沖彙總（官方市場背景；不計分）",
            "value": _format_tpex_daytrade_market_context(
                run.get("tpex_daytrade_market_context") or {}, as_of_date
            ),
            "inline": False,
        },
    )
    summary["fields"].insert(
        4,
        {
            "name": "上市/上櫃市場總報酬（官方背景；不計分）",
            "value": _format_daily_market_total_return_context(
                run.get("market_total_return_context") or {}, as_of_date
            ),
            "inline": False,
        },
    )
    if not parsed:
        if source_status == "fresh":
            summary["description"] = (
                "本次沒有股票同時具備至少 3 個有方向的資料家族；暫不列候選。"
                "未達門檻不等於偏空。"
            )
        else:
            summary["description"] = (
                "已依本輪有效資料判讀，沒有股票同時達到至少 3 個有方向的資料家族。"
                "各缺列只略過對應股票／來源，詳見覆蓋與具體缺項；"
                "未達候選門檻不等於沒有法人交易，也不代表偏空。"
            )
    elif not bullish:
        summary["description"] = (
            "本次沒有符合條件的偏多候選；以下列出偏空風險或方向分歧，僅供觀察。"
        )
    if source_status != "fresh" and parsed:
        summary["description"] += "\n部分來源缺列或日期待核，已逐項列示；其餘有效訊號照常判讀。"

    background_field_names = {
        "市場廣度背景（官方個股漲跌欄；不計入分數）",
        "期交所法人期貨／選擇權部位（市場背景；不計分）",
        "上櫃市場現股當沖彙總（官方市場背景；不計分）",
        "上市/上櫃市場總報酬（官方背景；不計分）",
    }
    background_fields = [
        field for field in summary["fields"]
        if field.get("name") in background_field_names
    ]
    summary["fields"] = [
        field for field in summary["fields"]
        if field.get("name") not in background_field_names
    ]
    background_embed = {
        "title": "📈 市場環境背景（不計個股分數）",
        "color": 0x34495E,
        "description": "官方市場廣度、期貨與當沖只作背景；不形成個股多空票。",
        "fields": background_fields,
    }
    embeds = [summary, flow_embed, background_embed]
    displayed_bullish = 0
    displayed_cautions = 0
    for row, detail in [*shown_bullish, *cautions]:
        candidate_embed = _daily_candidate_embed(row, detail)
        embed_characters = _embed_character_count(candidate_embed)
        if embed_characters > DISCORD_EMBED_CHAR_LIMIT:
            continue
        embeds.append(candidate_embed)
        if detail.get("direction") == "偏多":
            displayed_bullish += 1
        else:
            displayed_cautions += 1

    omitted = max(
        0, len(bullish) + len(bearish) + len(mixed)
        - displayed_bullish - displayed_cautions,
    )
    summary["footer"]["text"] = (
        f"資料截至 {as_of}｜本則顯示偏多 {displayed_bullish} 檔、風險/分歧 {displayed_cautions} 檔"
        + (f"｜另 {omitted} 檔未逐張列出" if omitted else "｜符合門檻名單已顯示")
    )
    return embeds


def _daily_candidate_embed_hashes(
    candidates: list[dict[str, Any]],
) -> dict[str, list[str]]:
    """Map rendered stock cards to candidate hashes for delivery evidence."""

    hashes_by_embed: dict[str, list[str]] = {}
    for row in candidates:
        try:
            detail = json.loads(row.get("details_text") or "{}")
        except (TypeError, ValueError):
            detail = {}
        if not isinstance(detail, dict):
            detail = {}
        embed = _daily_candidate_embed(row, detail)
        signature = json.dumps(
            embed, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        hashes_by_embed.setdefault(signature, []).append(
            str(row.get("candidate_hash") or "")
        )
    return hashes_by_embed


def _official_source_url(value: Any) -> str:
    raw = str(value or "").strip()
    try:
        parsed = urlsplit(raw)
        port = parsed.port
    except ValueError:
        return ""
    host = (parsed.hostname or "").lower()
    if parsed.username or parsed.password or port not in (None, 443):
        return ""
    allowed = (
        host == "twse.com.tw" or host.endswith(".twse.com.tw")
        or host == "tpex.org.tw" or host.endswith(".tpex.org.tw")
        or host == "cbc.gov.tw" or host.endswith(".cbc.gov.tw")
        or host == "taifex.com.tw" or host.endswith(".taifex.com.tw")
    )
    return raw if parsed.scheme == "https" and allowed else ""


def _intraday_embed_color(label: str) -> int:
    if "風險" in label or "偏空" in label or "轉弱" in label:
        return 0xC0392B
    if "偏多" in label or "候選" in label:
        return 0x2E8B57
    if "待判讀" in label or "未定" in label or "轉折" in label:
        return 0xD68910
    return 0x5865F2


def _format_forecast_source_value(value: Any) -> str:
    """Add readable digit grouping without inferring the source's missing unit."""
    if value is None:
        return "未提供"
    raw = str(value).strip()
    if not raw:
        return "未提供"

    def format_number(part: str) -> str | None:
        match = re.fullmatch(r"([+-]?)(\d+)(\.\d+)?", part.strip())
        if not match:
            return None
        grouped = f"{int(match.group(2)):,}"
        return f"{match.group(1)}{grouped}{match.group(3) or ''}"

    parts = raw.split("~")
    if len(parts) not in {1, 2}:
        return raw
    formatted = [format_number(part) for part in parts]
    if any(part is None for part in formatted):
        return raw
    return " 至 ".join(str(part) for part in formatted)


def _company_event_ingestion_note(detail: dict[str, Any]) -> str:
    """Show late source-record ingestion without mixing UTC and Taipei timestamps."""
    raw_event_time = detail.get("event_time")
    raw_created_at = detail.get("source_created_at_utc")
    if not raw_event_time or not raw_created_at:
        return ""
    try:
        event_time = (
            raw_event_time if isinstance(raw_event_time, dt.datetime)
            else dt.datetime.fromisoformat(str(raw_event_time))
        )
        created_at = (
            raw_created_at if isinstance(raw_created_at, dt.datetime)
            else dt.datetime.fromisoformat(str(raw_created_at))
        )
    except (TypeError, ValueError):
        return ""

    taipei = dt.timezone(dt.timedelta(hours=8))
    utc = dt.timezone.utc
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=taipei)
    else:
        event_time = event_time.astimezone(taipei)
    if created_at.tzinfo is None:
        # source_records.created_at is written by the UTC Airflow container.
        created_at = created_at.replace(tzinfo=utc)
    else:
        created_at = created_at.astimezone(utc)

    delay = created_at.astimezone(taipei) - event_time
    if delay < dt.timedelta(hours=1):
        return ""
    delay_minutes = int(delay.total_seconds() // 60)
    days, remainder = divmod(delay_minutes, 24 * 60)
    hours, minutes = divmod(remainder, 60)
    if days:
        delay_text = f"{days}天{hours}小時{minutes}分"
    elif hours:
        delay_text = f"{hours}小時{minutes}分"
    else:
        delay_text = f"{minutes}分"
    received_taipei = created_at.astimezone(taipei)
    return (
        f"資料庫首次收錄：{received_taipei:%m/%d %H:%M} 台北時間"
        f"（較公告晚 {delay_text}）"
    )


def _refresh_intraday_company_names(cursor, candidates: list[dict[str, Any]]) -> None:
    source_ids: dict[int, list[dict[str, Any]]] = {}
    for candidate in candidates:
        try:
            detail = json.loads(candidate.get("details_text") or "{}")
            source_id = int(detail.get("source_record_id"))
        except (TypeError, ValueError):
            continue
        if detail.get("kind") != "company_event" or source_id <= 0:
            continue
        candidate["security_name"] = ""
        source_ids.setdefault(source_id, []).append(candidate)
    if not source_ids:
        return

    ids = sorted(source_ids)
    placeholders = ",".join(["%s"] * len(ids))
    cursor.execute(
        f"SELECT id, payload FROM source_records WHERE id IN ({placeholders})",
        tuple(ids),
    )
    for source_id, payload in cursor.fetchall():
        name = _source_security_name(payload)
        for candidate in source_ids.get(int(source_id), []):
            candidate["security_name"] = name[:255]


def _format_intraday_candidate(row: dict[str, Any]) -> dict[str, Any]:
    try:
        detail = json.loads(row["details_text"])
    except (TypeError, ValueError):
        detail = {}
    kind = detail.get("kind")
    title = _escape_discord_markdown(detail.get("title") or "重大訊息", 300)
    source_key = str(detail.get("source_key") or "")
    source_labels = {
        "twse_company_events": "TWSE 上市重大訊息",
        "tpex_company_events": "TPEx 上櫃重大訊息",
        "twse_monthly_revenue": "TWSE 月營收",
        "tpex_monthly_revenue": "TPEx 月營收",
        "twse_quarterly_forecast_variance": "TWSE 公司財測差異",
        "tpex_quarterly_forecast_variance_events": "TPEx 上櫃財測差異門檻",
        "twse_insider_transfer_planned": "TWSE 上市內部人預定轉讓",
        "tpex_insider_transfer_planned": "TPEx 上櫃內部人預定轉讓",
        "twse_insider_transfer_untransferred": "TWSE 上市內部人未完成轉讓",
        "tpex_insider_transfer_untransferred": "TPEx 上櫃內部人未完成轉讓",
    }
    source = source_labels.get(source_key, "官方資料")
    if kind == "company_forecast_variance_threshold":
        when_value = detail.get("snapshot_date")
    elif kind in {
        "company_forecast_variance",
        "insider_transfer_planned",
        "insider_transfer_untransferred",
    }:
        when_value = detail.get("published_date")
    else:
        when_value = detail.get("event_time") or detail.get("effective_date")
    when = _escape_discord_markdown(when_value, 32)
    source_url = _official_source_url(detail.get("source_url"))
    event_signal = detail.get("event_signal") or {}
    label = _escape_discord_markdown(event_signal.get("label") or "方向未定", 32)
    action = _escape_discord_markdown(event_signal.get("action") or "讀取公告後再判斷", 220)
    facts = ""
    if kind == "monthly_revenue":
        yoy = _number(detail.get("revenue_yoy_pct"))
        raw_period = detail.get("effective_date")
        period_date = _date_only(raw_period)
        period = (
            period_date.strftime("%Y/%m")
            if period_date
            else _escape_discord_markdown(raw_period, 24)
        )
        facts_parts = [str(detail.get("detail") or "").strip()]
        if yoy is not None:
            facts_parts.append(f"當月年增率 **{yoy:+.2f}%**")
        previous_yoy = _number(detail.get("previous_revenue_yoy_pct"))
        previous_period = str(detail.get("previous_revenue_period") or "")
        if previous_period and previous_yoy is not None:
            facts_parts.append(
                f"前一曆月（{previous_period[:7].replace('-', '/')}）年增率 "
                f"{previous_yoy:+.2f}%"
            )
        elif previous_period:
            facts_parts.append(
                f"前一曆月（{previous_period[:7].replace('-', '/')}）年增率資料缺漏；"
                "不以較早月份代替月變化"
            )
        three_month_yoy = _number(detail.get("revenue_3m_yoy_pct"))
        three_month_start = str(detail.get("revenue_3m_start_period") or "")
        three_month_end = str(detail.get("revenue_3m_end_period") or "")
        if (
            three_month_yoy is not None
            and three_month_start
            and three_month_end
        ):
            facts_parts.append(
                f"近三月合計年增（{three_month_start}–{three_month_end}）"
                f"**{three_month_yoy:+.2f}%**"
            )
        else:
            status = str(detail.get("revenue_3m_context_status") or "")
            missing_periods = detail.get("revenue_3m_missing_periods") or []
            if status == "missing_periods" and missing_periods:
                missing = "、".join(str(value).replace("-", "/") for value in missing_periods)
                reason = f"缺少月份 {missing}"
            elif status == "missing_or_invalid_baseline":
                reason = "近三月金額或去年同期基期缺漏/無效"
            else:
                reason = "有效月份資料不足"
            facts_parts.append(f"近三月合計年增：無法計算（{reason}）")
        ytd_yoy = _number(detail.get("revenue_ytd_yoy_pct"))
        if ytd_yoy is not None:
            facts_parts.append(f"官方累計年增率 **{ytd_yoy:+.2f}%**")
        facts = _escape_discord_markdown("\n".join(part for part in facts_parts if part), 500)
        if period:
            title = f"{period} 月營收｜{title}"
    elif kind == "company_forecast_variance":
        year = detail.get("reporting_year")
        quarter = detail.get("reporting_quarter")
        period = f"{year} Q{quarter}" if year and quarter else "未提供"
        facts_parts = [f"財報期別 **{period}**"]
        if detail.get("coverage_period"):
            facts_parts.append(
                "財測涵蓋期間：" + str(detail["coverage_period"])
            )
        if detail.get("forecast_sequence"):
            facts_parts.append(
                "財測序號：" + str(detail["forecast_sequence"])
            )
        if detail.get("quarter_actual") is not None or detail.get("quarter_forecast") is not None:
            facts_parts.append(
                "當季查核／核閱實績（TWSE原始數值）："
                + _format_forecast_source_value(detail.get("quarter_actual"))
                + "；當季公司財測："
                + _format_forecast_source_value(detail.get("quarter_forecast"))
            )
        if detail.get("cumulative_actual") is not None or detail.get("cumulative_forecast") is not None:
            facts_parts.append(
                "截至當季累計實績（TWSE原始數值）："
                + _format_forecast_source_value(detail.get("cumulative_actual"))
                + "；累計公司財測："
                + _format_forecast_source_value(detail.get("cumulative_forecast"))
            )
        facts_parts.append("TWSE OpenAPI 未提供金額單位；請勿單憑此卡判定多空方向。")
        facts_parts.append(
            "TWSE 清單門檻：當季差異達 10% 以上，或截至當季累計差異達 20% 以上。"
        )
        facts = _escape_discord_markdown("\n".join(facts_parts), 850)
    elif kind == "company_forecast_variance_threshold":
        year = detail.get("reporting_year")
        quarter = detail.get("reporting_quarter")
        period = f"{year} Q{quarter}" if year and quarter else "未提供"
        facts = _escape_discord_markdown(
            "財報期別 **" + period + "**\n"
            "TPEx 官方簡式資料集列示門檻：當季差異達 10% 以上，或截至當季累計差異達 20% 以上。\n"
            "此清單不提供實績/預測金額；方向未定，請回查官方原始財測與查核數。",
            850,
        )
    elif kind in {
        "insider_transfer_planned", "insider_transfer_untransferred"
    }:
        transfer = detail.get("transfer_details") or {}

        def share_text(value: Any) -> str:
            number = _number(value)
            if number is None:
                return "未提供"
            if float(number).is_integer():
                return f"{int(number):,} 股"
            return f"{number:,.2f} 股"

        if kind == "insider_transfer_planned":
            facts_parts = [
                "申報轉讓方式：" + str(transfer.get("transfer_method") or "未提供"),
                "該方式申報股數：" + share_text(transfer.get("transfer_shares_by_method")),
                "每日申報上限：" + share_text(transfer.get("daily_max_transfer_shares")),
                "申報前自有／信託持股："
                + share_text(transfer.get("current_direct_shares"))
                + " / " + share_text(transfer.get("current_trust_shares")),
                "預定轉讓後自有／信託持股："
                + share_text(transfer.get("post_transfer_direct_shares"))
                + " / " + share_text(transfer.get("post_transfer_trust_shares")),
                "申報有效期間：" + str(transfer.get("transfer_period") or "未提供"),
                "事前申報不是成交回報；無法據此推定實際出售或多空方向。",
            ]
            ratio = _number(transfer.get("planned_total_pct_of_current_holdings"))
            if ratio is not None:
                facts_parts.insert(
                    4, f"申報預定股數／申報前持股：{ratio:.2f}%"
                )
        else:
            facts_parts = [
                "未轉讓自有／信託股數："
                + share_text(transfer.get("untransferred_direct_shares"))
                + " / " + share_text(transfer.get("untransferred_trust_shares")),
                "原申報預定自有／信託股數："
                + share_text(transfer.get("original_planned_direct_shares"))
                + " / " + share_text(transfer.get("original_planned_trust_shares")),
                "官方未轉讓理由：" + str(transfer.get("reason") or "未提供"),
                "未完成轉讓紀錄不等同實際賣出。",
            ]
        facts = _escape_discord_markdown("\n".join(facts_parts), 850)
    else:
        facts = _escape_discord_markdown(_summarize_company_event(detail.get("description")), 850)

    if kind == "company_forecast_variance":
        footer_text = f"{source}｜資料出表日期：{when or '未提供'}｜金額單位未提供"
    elif kind == "company_forecast_variance_threshold":
        first_seen = _escape_discord_markdown(
            detail.get("source_observed_at_utc"), 32
        )
        footer_text = (
            f"{source}｜資料出表日期：{when or '未提供'}｜"
            f"本系統首次收錄 UTC：{first_seen or '未提供'}｜官方事件觀察，不作方向訊號"
        )
    elif kind in {
        "insider_transfer_planned", "insider_transfer_untransferred"
    }:
        footer_text = f"{source}｜資料出表日期：{when or '未提供'}｜官方事實觀察，不作方向訊號"
    else:
        footer_text = f"{source}｜公告時間：{when or '未提供'}"
        if kind == "company_event":
            ingestion_note = _company_event_ingestion_note(detail)
            if ingestion_note:
                footer_text += f"｜{ingestion_note}"

    code = _escape_discord_markdown(row.get("entity_id"), 16)
    security_name = _escape_discord_markdown(row.get("security_name"), 40)
    security_label = f"{code} {security_name}" if security_name else code
    embed: dict[str, Any] = {
        "title": f"{security_label}｜{label}",
        "description": title or "官方公告詳情請查看原文。",
        "color": _intraday_embed_color(label),
        "fields": [
            {"name": "訊號判讀", "value": action or "查看公告後再判斷。", "inline": False},
        ],
        "footer": {"text": footer_text},
    }
    if facts:
        embed["fields"].append({"name": "公告內容重點（依官方說明整理）", "value": facts, "inline": False})
    if source_url:
        embed["fields"].append({"name": "官方來源", "value": f"[開啟 {source}]({source_url})", "inline": False})
    return embed


def _embed_character_count(embed: dict[str, Any]) -> int:
    total = len(str(embed.get("title") or "")) + len(str(embed.get("description") or ""))
    total += len(str((embed.get("footer") or {}).get("text") or ""))
    for field in embed.get("fields") or []:
        total += len(str(field.get("name") or "")) + len(str(field.get("value") or ""))
    return total


def _format_intraday_batch(rows: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    embeds = [_format_intraday_candidate(row) for row in rows]
    message = f"📣 台股公告與營收快訊｜{_today_taipei():%Y/%m/%d}｜本批 {len(rows)} 則"
    return message, embeds


def _serialize_discord_payload(message: str, embeds: list[dict[str, Any]] | None) -> str:
    if not embeds:
        return message
    return json.dumps(
        {
            "content": message,
            "embeds": embeds,
            "allowed_mentions": {"parse": []},
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _discord_request_size(message: str, embeds: list[dict[str, Any]]) -> int:
    # requests.post(json=...) uses ASCII JSON escaping by default; count the
    # encoded request, not just the visible Unicode characters in embeds.
    payload = {
        "content": message,
        "embeds": embeds,
        "allowed_mentions": {"parse": []},
    }
    return len(json.dumps(payload, allow_nan=False).encode("utf-8"))


def _split_discord_embed_batches(
    message: str, embeds: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Keep each Discord request below the same byte budget used intraday."""

    # Reserve room for the user-facing part marker added after the batch count
    # is known. The embed-count limit applies to each request; a digest may span
    # multiple requests when it includes a separate background card.
    budget_message = f"{message}｜第 {DAILY_DIGEST_LIMIT}/{DAILY_DIGEST_LIMIT} 批"
    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    for embed in embeds:
        proposed = [*current, embed]
        if current and (
            len(proposed) > DISCORD_EMBED_COUNT_LIMIT
            or sum(_embed_character_count(embed) for embed in proposed) > DISCORD_EMBED_CHAR_LIMIT
            or _discord_request_size(budget_message, proposed) > DISCORD_REQUEST_BYTE_LIMIT
        ):
            batches.append(current)
            current = []
            proposed = [embed]
        request_size = _discord_request_size(budget_message, proposed)
        if sum(_embed_character_count(embed) for embed in proposed) > DISCORD_EMBED_CHAR_LIMIT:
            raise ValueError("single daily signal embed exceeds the Discord character budget")
        if request_size > DISCORD_REQUEST_BYTE_LIMIT:
            raise ValueError(
                "single daily signal embed exceeds the Discord request budget: "
                f"{request_size}>{DISCORD_REQUEST_BYTE_LIMIT} bytes"
            )
        current = proposed
    if current:
        batches.append(current)
    return batches


def _notification_record(cursor, dedupe_key: str):
    cursor.execute(
        """
        SELECT status, updated_at, run_key FROM market_signal_notifications
        WHERE dedupe_key=%s FOR UPDATE
        """,
        (dedupe_key,),
    )
    return cursor.fetchone()


def _claim_notification(cursor, *, dedupe_key: str, run_key: str,
                        message: str, now: dt.datetime,
                        notification_channel: str = NOTIFICATION_CHANNEL) -> bool:
    existing = _notification_record(cursor, dedupe_key)
    if existing:
        status, updated_at, _existing_run_key = existing
        if status in {"sent", "accepted_unverified", "delivery_unknown"}:
            return False
        if status == "pending" and updated_at and now - updated_at < dt.timedelta(minutes=1):
            return False
        cursor.execute(
            """
            UPDATE market_signal_notifications
            SET run_key=%s, status='pending', message_text=%s,
                platform_receipt_id=NULL, platform_http_status=NULL,
                notification_error_type=NULL, notified_at=NULL, updated_at=%s
            WHERE dedupe_key=%s
            """,
            (run_key, message, now, dedupe_key),
        )
        return True
    cursor.execute(
        """
        INSERT INTO market_signal_notifications (
            dedupe_key, run_key, notification_channel, status, message_text,
            notified_at, created_at, updated_at
        ) VALUES (%s, %s, %s, 'pending', %s, NULL, %s, %s)
        """,
        (dedupe_key, run_key, notification_channel, message, now, now),
    )
    return True


def _mark_notification(cursor, *, dedupe_key: str, status: str,
                       message: str, now: dt.datetime,
                       receipt_id: str | None = None,
                       http_status: int | None = None,
                       notification_error_type: str | None = None) -> None:
    cursor.execute(
        """
        UPDATE market_signal_notifications
        SET status=%s, message_text=%s, platform_receipt_id=%s,
            platform_http_status=%s, notification_error_type=%s,
            notified_at=%s, updated_at=%s
        WHERE dedupe_key=%s
        """,
        (
            status, message, receipt_id, http_status, notification_error_type,
            now if status == "sent" else None, now, dedupe_key,
        ),
    )


def _replace_notification_candidates(
    cursor, *, dedupe_key: str, run_key: str,
    candidate_hashes: list[str], now: dt.datetime,
) -> None:
    """Attach the exact stock cards in a claimed digest batch to its ledger row."""

    cursor.execute(
        """
        SELECT id FROM market_signal_notifications
        WHERE dedupe_key=%s AND run_key=%s
        """,
        (dedupe_key, run_key),
    )
    notification_row = cursor.fetchone()
    if not notification_row:
        return
    notification_id = int(notification_row[0])
    cursor.execute(
        "DELETE FROM market_signal_notification_candidates WHERE notification_id=%s",
        (notification_id,),
    )
    for candidate_hash in candidate_hashes:
        cursor.execute(
            """
            INSERT INTO market_signal_notification_candidates (
                notification_id, candidate_id, created_at
            )
            SELECT %s, target.id, %s
            FROM market_signal_candidates sent
            INNER JOIN market_signal_candidates target
                ON target.run_key=sent.run_key
               AND target.entity_id=sent.entity_id
               AND target.entity_type IN ('stock','stock_evidence')
            WHERE sent.run_key=%s AND sent.candidate_hash=%s
              AND sent.entity_type='stock'
            ON DUPLICATE KEY UPDATE created_at=VALUES(created_at)
            """,
            (notification_id, now, run_key, candidate_hash),
        )


def _replace_notification_outcome_candidates(
    cursor, *, dedupe_key: str, run_key: str,
    outcome_keys: list[tuple[int, str]], now: dt.datetime,
) -> None:
    """Link each reviewed candidate and horizon to its notification receipt."""

    if not outcome_keys:
        return
    cursor.execute(
        """
        SELECT id FROM market_signal_notifications
        WHERE dedupe_key=%s AND run_key=%s
        """,
        (dedupe_key, run_key),
    )
    notification_row = cursor.fetchone()
    if not notification_row:
        return
    notification_id = int(notification_row[0])
    for candidate_id, horizon_key in outcome_keys:
        cursor.execute(
            """
            INSERT INTO market_signal_outcome_notification_candidates (
                candidate_id, horizon_key, notification_id, created_at
            ) VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                notification_id=VALUES(notification_id),
                created_at=VALUES(created_at)
            """,
            (int(candidate_id), str(horizon_key), notification_id, now),
        )


def _notification_delivery_status(result: Any) -> str:
    if not isinstance(result, dict):
        return "failed"
    if result.get("success") is True and result.get("receipt_id"):
        return "sent"
    if result.get("accepted") is True:
        return "accepted_unverified"
    if result.get("outcome_unknown") is True:
        return "delivery_unknown"
    return "failed"


def _send_and_record(cursor, connection, notifier: DiscordNotify, *,
                     dedupe_key: str, run_key: str, message: str,
                     now: dt.datetime, embeds: list[dict[str, Any]] | None = None,
                     candidate_hashes: list[str] | None = None,
                     outcome_keys: list[tuple[int, str]] | None = None,
                     notification_channel: str = NOTIFICATION_CHANNEL) -> bool:
    ledger_message = _serialize_discord_payload(message, embeds)
    claimed = _claim_notification(
        cursor, dedupe_key=dedupe_key, run_key=run_key,
        message=ledger_message, now=now,
        notification_channel=notification_channel,
    )
    if claimed:
        _replace_notification_candidates(
            cursor,
            dedupe_key=dedupe_key,
            run_key=run_key,
            candidate_hashes=list(candidate_hashes or []),
            now=now,
        )
        _replace_notification_outcome_candidates(
            cursor,
            dedupe_key=dedupe_key,
            run_key=run_key,
            outcome_keys=list(outcome_keys or []),
            now=now,
        )
    connection.commit()
    if not claimed:
        existing = _notification_record(cursor, dedupe_key)
        return bool(existing and existing[0] == "sent")
    delivery_result = None
    try:
        delivery_result = notifier.notify_with_receipt(
            msg=message, embeds=embeds,
        )
        status = _notification_delivery_status(delivery_result)
        receipt_id = delivery_result.get("receipt_id") if isinstance(delivery_result, dict) else None
        http_status = (
            delivery_result.get("http_status")
            if isinstance(delivery_result, dict) else None
        )
        _mark_notification(
            cursor, dedupe_key=dedupe_key, status=status,
            message=ledger_message, now=_now_taipei(),
            receipt_id=str(receipt_id) if status == "sent" else None,
            http_status=http_status,
            notification_error_type=(
                delivery_result.get("error_type")
                if isinstance(delivery_result, dict) else None
            ),
        )
        connection.commit()
        return status == "sent"
    except Exception as exc:
        connection.rollback()
        try:
            status = _notification_delivery_status(delivery_result)
            receipt_id = (
                delivery_result.get("receipt_id")
                if isinstance(delivery_result, dict) else None
            )
            _mark_notification(
                cursor, dedupe_key=dedupe_key, status=status,
                message=ledger_message, now=_now_taipei(),
                receipt_id=str(receipt_id) if status == "sent" else None,
                http_status=(
                    delivery_result.get("http_status")
                    if isinstance(delivery_result, dict) else None
                ),
                notification_error_type=(
                    delivery_result.get("error_type")
                    if isinstance(delivery_result, dict)
                    and delivery_result.get("error_type")
                    else type(exc).__name__
                ),
            )
            connection.commit()
            return status == "sent"
        except Exception:
            connection.rollback()
        return False


def _format_review_number(value: Any, *, suffix: str = "%") -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "資料不足"
    if not math.isfinite(number):
        return "資料不足"
    return f"{number:+.2f}{suffix}"


def _format_review_rate(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "資料不足"
    if not math.isfinite(number):
        return "資料不足"
    return f"{number:.1%}"


def _notify_matured_signal_outcomes(
    cursor, connection, notifier: DiscordNotify, *,
    run_key: str, as_of_date: dt.date, now: dt.datetime,
) -> dict[str, Any]:
    """Send one deduplicated review for newly matured, receipt-confirmed signals."""

    cursor.execute(
        """
        SELECT f.candidate_id, f.horizon_key, f.signal_date, f.target_date,
               f.entity_id, f.market, f.source_status, f.direction,
               f.family_directions_json, f.family_status_json, f.policy_version,
               f.candidate_selected, f.delivery_status, f.status AS status,
               f.execution_status, f.execution_error_reason,
               f.benchmark_status, f.benchmark_error_reason, f.error_reason,
               f.stock_return_pct, f.execution_return_pct, f.excess_return_pp,
               f.max_favorable_excursion_pct, f.max_adverse_excursion_pct,
               l.notification_id, n.status AS notification_status,
               n.updated_at AS notification_updated_at,
               n.dedupe_key AS notification_dedupe_key
        FROM market_signal_followups f
        LEFT JOIN market_signal_outcome_notification_candidates l
          ON l.candidate_id=f.candidate_id AND l.horizon_key=f.horizon_key
        LEFT JOIN market_signal_notifications n ON n.id=l.notification_id
        WHERE f.target_date<=%s
          AND f.candidate_selected=1 AND f.delivery_status='sent'
          AND f.status='settled' AND f.execution_status='settled'
          AND f.benchmark_status='settled'
          AND (
              l.notification_id IS NULL
              OR n.status='failed'
              OR (n.status='pending' AND n.updated_at<%s)
          )
        ORDER BY f.horizon_key, f.market, f.signal_date, f.entity_id
        """,
        (as_of_date, now - dt.timedelta(minutes=1)),
    )
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    if not rows:
        return {"status": "no_new_matured_outcomes", "sent": 0, "rows": 0}

    retry_batches: dict[str, list[dict[str, Any]]] = {}
    new_rows: list[dict[str, Any]] = []
    for row in rows:
        prior_dedupe_key = row.get("notification_dedupe_key")
        if prior_dedupe_key and row.get("notification_status") in {"failed", "pending"}:
            retry_batches.setdefault(str(prior_dedupe_key), []).append(row)
        else:
            new_rows.append(row)
    if new_rows:
        retry_batches[""] = new_rows

    sent_batches = 0
    failed_batches = 0
    statuses: list[str] = []
    horizon_order = {key: index for index, key in enumerate(FOLLOWUP_HORIZONS)}
    for prior_key, batch_rows in retry_batches.items():
        report = summarize_followup_rows(batch_rows)
        report_groups = report.get("groups") or []
        selected_groups = [
            group for group in report_groups
            if group.get("scope") == "selected_candidate"
        ]
        family_groups = [
            group for group in report_groups
            if group.get("scope") == "family_counterfactual"
        ]
        selected_groups.sort(key=lambda group: (
            horizon_order.get(group.get("horizon_key"), len(horizon_order)),
            str(group.get("market")), str(group.get("policy_version")),
            str(group.get("source_status")),
        ))
        family_groups.sort(key=lambda group: (
            horizon_order.get(group.get("horizon_key"), len(horizon_order)),
            str(group.get("family")), str(group.get("market")),
            str(group.get("policy_version")), str(group.get("source_status")),
        ))
        fields: list[dict[str, Any]] = []
        displayed_groups = [*selected_groups[:5], *family_groups[:3]]
        for group in displayed_groups:
            ci = group.get("execution_proxy_ci_95") or {}
            ci_text = (
                f"95% 日期區塊區間 {ci['low_pct']:+.2f}% 至 "
                f"{ci['high_pct']:+.2f}%"
                if ci.get("low_pct") is not None and ci.get("high_pct") is not None
                else f"區間尚不可估（訊號日 {group.get('execution_signal_date_count', 0)}）"
            )
            gate = "樣本不足，僅觀察"
            if group.get("source_status") != "fresh":
                gate = "來源不完整；僅作觀察"
            elif group.get("scope") == "family_counterfactual":
                if (
                    ci.get("low_pct") is not None
                    and ci["low_pct"] > 0
                    and (group.get("mean_directional_execution_proxy_pct") or 0) > 0
                    and (group.get("mean_directional_market_excess_pp") or 0) > 0
                ):
                    gate = "已通知樣本中的家族方向正向；不是獨立增益證明"
                elif ci.get("high_pct") is not None and ci["high_pct"] < 0:
                    gate = "已通知樣本中的家族方向目前為負"
                else:
                    gate = "已通知樣本中的家族方向尚無明確優勢"
            elif (
                ci.get("low_pct") is not None
                and ci["low_pct"] > 0
                and (group.get("mean_directional_execution_proxy_pct") or 0) > 0
                and (group.get("mean_directional_market_excess_pp") or 0) > 0
            ):
                gate = "僅可進入成本模型與未觸碰樣本外檢驗"
            elif ci.get("high_pct") is not None and ci["high_pct"] < 0:
                gate = "目前樣本未支持此方向"
            label = (
                f"{group.get('horizon_key')}｜{group.get('market')}｜"
                f"{group.get('policy_version')}｜來源 {group.get('source_status')}"
                + (
                    f"｜家族 {group.get('family')}"
                    if group.get("scope") == "family_counterfactual" else ""
                )
            )[:240]
            value = (
                f"訊號 {group.get('settled_rows', 0)} 筆／"
                f"{group.get('execution_signal_date_count', 0)} 個訊號日；"
                f"個股上漲率 {_format_review_rate(group.get('raw_price_up_rate'))}；"
                f"方向命中率 {_format_review_rate(group.get('directional_price_hit_rate'))}；"
                f"方向執行代理 { _format_review_number(group.get('mean_directional_execution_proxy_pct')) }；"
                f"方向市場超額 { _format_review_number(group.get('mean_directional_market_excess_pp'), suffix='pp') }；"
                f"{ci_text}。{gate}。"
            )[:1000]
            fields.append({"name": label, "value": value, "inline": False})
        omitted_groups = len(report_groups) - len(displayed_groups)
        if omitted_groups > 0:
            fields.append({
                "name": "其餘分組",
                "value": f"另有 {omitted_groups} 組，完整分組已保存於每日回顧表。",
                "inline": False,
            })
        embed = {
            "title": "成熟訊號結果",
            "description": (
                f"本批 {len(batch_rows)} 筆已成熟訊號；只納入已確認平台回執且價格、"
                "市場基準與執行代理都已結算的候選。這是事後觀察，不代表讀取或實際成交。"
            ),
            "color": 0x2874A6,
            "fields": fields or [{
                "name": "結果",
                "value": "已成熟結果已保存；目前沒有足夠的方向性分組可供解讀。",
                "inline": False,
            }],
            "footer": {
                "text": "價格未含股利／公司行動與交易成本；次日開盤是成交代理；家族分組會重疊；不自動調整正式訊號規則。"
            },
        }
        outcome_keys = sorted({
            (int(row["candidate_id"]), str(row["horizon_key"]))
            for row in batch_rows
        })
        dedupe_key = prior_key or hashlib.sha256(
            (
                "matured-signal-outcome-review|"
                + "|".join(f"{candidate_id}:{horizon}" for candidate_id, horizon in outcome_keys)
            ).encode("utf-8")
        ).hexdigest()
        delivered = _send_and_record(
            cursor, connection, notifier,
            dedupe_key=dedupe_key,
            run_key=run_key,
            message="📊 台股訊號雷達｜成熟結果回顧",
            embeds=[embed],
            now=now,
            outcome_keys=outcome_keys,
            notification_channel="market_signal_outcome_review",
        )
        record = _notification_record(cursor, dedupe_key)
        status = str(record[0]) if record else "unknown"
        if delivered:
            sent_batches += 1
            statuses.append("sent")
        elif status == "pending":
            statuses.append("pending")
        else:
            failed_batches += 1
            statuses.append(status)
    return {
        "status": "sent" if not failed_batches else "delivery_not_confirmed",
        "sent": sent_batches,
        "failed": failed_batches,
        "rows": len(rows),
        "notification_statuses": statuses,
    }


def _annual_signal_review_embed(
    signal_year: int, review_status: str, report: dict[str, Any],
) -> dict[str, Any]:
    coverage = report.get("candidate_tracking_coverage") or {}
    delivery = report.get("delivery_coverage") or {}
    expected = int(coverage.get("expected_directional_candidate_count") or 0)
    tracked = int(coverage.get("tracked_directional_candidate_count") or 0)
    groups = [
        group for group in (report.get("groups") or [])
        if group.get("scope") == "selected_candidate"
        and group.get("delivery_status") == "sent"
    ]
    horizon_order = {key: index for index, key in enumerate(FOLLOWUP_HORIZONS)}
    groups.sort(key=lambda group: (
        horizon_order.get(group.get("horizon_key"), len(horizon_order)),
        str(group.get("market")), str(group.get("policy_version")),
    ))
    fields = [
        {
            "name": "追蹤覆蓋",
            "value": (
                f"方向候選 {tracked}/{expected}（"
                f"{coverage.get('coverage_pct', 0)}%）；"
                f"1y 未完整結算 {coverage.get('unsettled_one_year_rows', 0)} 筆。"
            )[:1000],
            "inline": False,
        },
        {
            "name": "通知回執",
            "value": (
                f"已選候選 {delivery.get('selected_candidate_count', 0)} 筆；"
                f"平台回執 {delivery.get('confirmed_platform_receipt_count', 0)}；"
                f"失敗 {delivery.get('failed_selected_delivery_count', 0)}；"
                f"未確認 {delivery.get('unresolved_selected_delivery_count', 0)}。"
            )[:1000],
            "inline": False,
        },
    ]
    for group in groups[:5]:
        ci = group.get("execution_proxy_ci_95") or {}
        ci_text = (
            f"95% 日期區塊區間 {ci['low_pct']:+.2f}% 至 {ci['high_pct']:+.2f}%"
            if ci.get("low_pct") is not None and ci.get("high_pct") is not None
            else "訊號日不足，區間不估"
        )
        fields.append({
            "name": (
                f"{group.get('horizon_key')}｜{group.get('market')}｜"
                f"{group.get('policy_version')}｜來源 {group.get('source_status')}"
            )[:240],
            "value": (
                f"成熟 {group.get('settled_rows', 0)} 筆／"
                f"{group.get('execution_signal_date_count', 0)} 個訊號日；"
                f"個股上漲率 {_format_review_rate(group.get('raw_price_up_rate'))}；"
                f"方向命中率 {_format_review_rate(group.get('directional_price_hit_rate'))}；"
                f"方向執行代理 { _format_review_number(group.get('mean_directional_execution_proxy_pct')) }；"
                f"方向市場超額 { _format_review_number(group.get('mean_directional_market_excess_pp'), suffix='pp') }；"
                f"{ci_text}。"
            )[:1000],
            "inline": False,
        })
    if len(groups) > 5:
        fields.append({
            "name": "其餘分組",
            "value": f"另有 {len(groups) - 5} 組，完整報告留存於資料庫。",
            "inline": False,
        })
    return {
        "title": f"{signal_year} 年度訊號回顧｜{review_status}",
        "description": (
            "年度 cohort 結果是歷史描述；完整追蹤不代表策略有效。"
            "需要另做成本後測試、未觸碰時間留出集與多重測試校正。"
        ),
        "color": 0x7D3C98 if review_status == "complete_descriptive" else 0xD68910,
        "fields": fields,
        "footer": {
            "text": "官方未調整價格；次日開盤為成交代理；回執不代表已讀或成交；不自動改正式訊號政策。"
        },
    }


def _notify_due_annual_signal_reviews(
    cursor, connection, notifier: DiscordNotify, *,
    run_key: str, as_of_date: dt.date, now: dt.datetime,
) -> dict[str, Any]:
    """Deliver due annual cohort snapshots once per substantive report version."""

    cursor.execute(
        """
        SELECT signal_year, review_status, report_json
        FROM market_signal_outcome_reviews
        WHERE review_due_date<=%s
        ORDER BY signal_year
        """,
        (as_of_date,),
    )
    results: list[dict[str, Any]] = []
    failed = 0
    for signal_year, review_status, report_text in cursor.fetchall():
        try:
            report = json.loads(report_text or "{}")
        except (TypeError, ValueError):
            report = {}
        stable_report = dict(report)
        stable_report.pop("as_of_date", None)
        report_hash = hashlib.sha256(
            json.dumps(
                stable_report, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), default=str,
            ).encode("utf-8")
        ).hexdigest()
        dedupe_key = hashlib.sha256(
            f"annual-signal-review|{int(signal_year)}|{report_hash}".encode("utf-8")
        ).hexdigest()
        existing = _notification_record(cursor, dedupe_key)
        if existing and existing[0] in {
            "sent", "accepted_unverified", "delivery_unknown",
        }:
            results.append({"signal_year": int(signal_year), "status": existing[0]})
            continue
        delivered = _send_and_record(
            cursor, connection, notifier,
            dedupe_key=dedupe_key,
            run_key=run_key,
            message="📊 台股訊號雷達｜年度訊號回顧",
            embeds=[_annual_signal_review_embed(
                int(signal_year), str(review_status), report,
            )],
            now=now,
            notification_channel="market_signal_annual_review",
        )
        record = _notification_record(cursor, dedupe_key)
        status = str(record[0]) if record else "unknown"
        results.append({"signal_year": int(signal_year), "status": status})
        if not delivered and status == "failed":
            failed += 1
    return {
        "status": "delivery_failed" if failed else "ready",
        "reviews": results,
        "failed": failed,
    }


def _send_failure_notice(
    payload: dict[str, Any], notifier: DiscordNotify, *, cursor=None, connection=None,
) -> bool:
    run_key = str(payload.get("run_key") or "unknown")
    message = "⚠️ 台股訊號雷達｜流程需要檢查"
    embed = {
        "title": "分析流程異常",
        "description": _escape_discord_markdown(payload.get("error_message") or "掃描階段未回傳狀態", 700),
        "color": 0xC0392B,
        "fields": [
            {"name": "掃描模式", "value": _escape_discord_markdown(payload.get("scan_mode"), 24) or "未知", "inline": True},
            {"name": "Airflow run", "value": _escape_discord_markdown(payload.get("airflow_run_id") or run_key, 120) or "未知", "inline": True},
        ],
    }
    try:
        if cursor is not None and connection is not None and run_key != "unknown":
            dedupe_key = hashlib.sha256(
                f"market-signal-failure|{run_key}".encode("utf-8")
            ).hexdigest()
            return _send_and_record(
                cursor, connection, notifier,
                dedupe_key=dedupe_key, run_key=run_key,
                message=message, embeds=[embed], now=_now_taipei(),
            )
        result = notifier.notify_with_receipt(msg=message, embeds=[embed])
        return bool(
            isinstance(result, dict)
            and result.get("success") is True
            and result.get("receipt_id")
        )
    except Exception:
        return False


def notify_market_signal_snapshot() -> dict[str, Any]:
    """Deliver scan results, or an operational notice when analysis failed."""

    context = get_current_context()
    task_instance = context.get("ti")
    payload = task_instance.xcom_pull(task_ids="capture_market_signal_snapshot") if task_instance else None
    payload = payload if isinstance(payload, dict) else {}
    if payload.get("status") == "skipped":
        print(
            f"[market signal] 掃描正常略過 "
            f"| 日期={payload.get('as_of_date')} "
            f"| 原因={payload.get('skip_reason')}"
        )
        return {
            "status": "skipped",
            "run_key": payload.get("run_key"),
            "skip_reason": payload.get("skip_reason"),
        }
    notifier = _discord_notifier()
    if payload.get("status") != "ready":
        sent = _send_failure_notice(payload, notifier)
        if not sent:
            raise RuntimeError("market scan failed and its failure notice was not delivered")
        raise RuntimeError("market scan failed; failure notice was delivered")

    run_key = str(payload.get("run_key") or "")
    scan_mode = str(payload.get("scan_mode") or "daily")
    connection = cursor = engine = None
    try:
        connection, cursor, engine = _open_connection()
        create_schema(cursor)
        cursor.execute(
            """
            SELECT scan_mode, as_of_date, status, source_status, universe_count,
                   price_count, flow_count, revenue_count, financial_count,
                   borrow_count, insider_count, dividend_count, candidate_count,
                   error_message, started_at
            FROM market_signal_runs WHERE run_key=%s
            """,
            (run_key,),
        )
        run_row = cursor.fetchone()
        if not run_row or run_row[2] != "ready":
            failure_payload = dict(payload)
            failure_payload["status"] = "failed"
            failure_payload["error_message"] = (run_row[13] if run_row else None) or "快照未完成"
            sent = _send_failure_notice(
                failure_payload, notifier, cursor=cursor, connection=connection,
            )
            if not sent:
                raise RuntimeError("market scan did not finish and its failure notice was not delivered")
            raise RuntimeError("market scan did not finish; failure notice was delivered")
        (mode, as_of_date, status, source_status, universe_count, price_count,
         flow_count, revenue_count, financial_count, borrow_count, insider_count,
         dividend_count, candidate_count, error_message, started_at) = run_row
        as_of_date = _date_only(as_of_date)
        cursor.execute(
            """
            SELECT candidate_hash, entity_id, security_name, market, industry,
                   as_of_date, signal_count, signal_keys, score, details_text
            FROM market_signal_candidates
            WHERE run_key=%s AND entity_type='stock'
            ORDER BY score DESC, entity_id
            """,
            (run_key,),
        )
        candidates = [
            {
                "candidate_hash": row[0], "entity_id": row[1], "security_name": row[2],
                "market": row[3], "industry": row[4], "as_of_date": row[5],
                "signal_count": row[6], "signal_keys": row[7], "score": row[8],
                "details_text": row[9],
            }
            for row in cursor.fetchall()
        ]

        if mode == "daily":
            daily_run = {
                "as_of_date": as_of_date,
                "family_trigger_counts": payload.get("family_trigger_counts"),
                "candidate_filter_counts": payload.get("candidate_filter_counts"),
                "source_status": source_status,
                "universe_count": universe_count,
                "market_coverage_by_market": (
                    payload.get("market_coverage_by_market")
                    or _load_market_coverage(cursor, run_key)
                ),
                "benchmark_alignment_by_market": (
                    payload.get("benchmark_alignment_by_market") or {}
                ),
                "price_count": price_count,
                "flow_count": flow_count,
                "revenue_count": revenue_count,
                "financial_count": financial_count,
                "borrow_count": borrow_count,
                "insider_count": insider_count,
                "dividend_count": dividend_count,
                "candidate_count": candidate_count,
                "market_breadth_context": _load_daily_market_breadth(
                    cursor, as_of_date, started_at,
                    payload.get("universe_market_counts"),
                ),
                "taifex_market_positioning": _load_taifex_market_positioning(
                    cursor, as_of_date, started_at
                ),
                "taifex_market_option_positioning": _load_taifex_market_option_positioning(
                    cursor, as_of_date, started_at
                ),
                "taifex_large_trader_context": _load_taifex_large_trader_context(
                    cursor, as_of_date, started_at
                ),
                "tpex_daytrade_market_context": _load_tpex_daytrade_market_context(
                    cursor, as_of_date, started_at
                ),
                "market_total_return_context": _load_daily_market_total_return_context(
                    cursor, as_of_date, started_at
                ),
            }
            if (
                source_status == "partial"
                and payload.get("source_dag_run_id")
                and payload.get("source_dag_id")
            ):
                daily_run["source_issues"] = _source_collection_issues(
                    cursor,
                    payload.get("source_dag_run_id"),
                    payload.get("source_schedule_hint"),
                    payload.get("source_dag_id"),
                )
            if payload.get("market_coverage_issues"):
                daily_run.setdefault("source_issues", []).extend(
                    payload["market_coverage_issues"]
                )
            message = _format_daily_message(daily_run, candidates)
            embeds = _format_daily_embeds(daily_run, candidates)
            candidate_hashes_by_embed = _daily_candidate_embed_hashes(candidates)
            date_key = as_of_date.isoformat() if as_of_date else "no-price-date"
            batches = _split_discord_embed_batches(message, embeds)
            sent_count = 0
            failed_count = 0
            for batch_index, batch in enumerate(batches, start=1):
                batch_message = f"{message}｜第 {batch_index}/{len(batches)} 批"
                batch_candidate_hashes = []
                for embed in batch:
                    embed_signature = json.dumps(
                        embed, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    )
                    matching_hashes = candidate_hashes_by_embed.get(embed_signature) or []
                    if matching_hashes:
                        batch_candidate_hashes.append(matching_hashes.pop(0))
                dedupe_key = hashlib.sha256(
                    f"daily|{date_key}|part|{batch_index}|of|{len(batches)}".encode("utf-8")
                ).hexdigest()
                if _send_and_record(
                    cursor, connection, notifier, dedupe_key=dedupe_key,
                    run_key=run_key, message=batch_message, embeds=batch,
                    now=_now_taipei(), candidate_hashes=batch_candidate_hashes,
                ):
                    sent_count += 1
                else:
                    failed_count += 1
            print(
                f"[market signal] daily digest batches sent={sent_count}/{len(batches)} "
                f"failed={failed_count}"
            )
            if failed_count:
                raise RuntimeError(
                    "daily digest notification was not fully delivered: "
                    f"sent={sent_count}/{len(batches)}, failed={failed_count}"
                )
            outcome_review = _notify_matured_signal_outcomes(
                cursor, connection, notifier,
                run_key=run_key, as_of_date=as_of_date, now=_now_taipei(),
            )
            annual_review_delivery = _notify_due_annual_signal_reviews(
                cursor, connection, notifier,
                run_key=run_key, as_of_date=as_of_date, now=_now_taipei(),
            )
            print(
                "[market signal] matured outcome review; "
                + json.dumps(outcome_review, ensure_ascii=False, default=str)
            )
            print(
                "[market signal] annual review delivery; "
                + json.dumps(annual_review_delivery, ensure_ascii=False, default=str)
            )
            if outcome_review.get("failed") or annual_review_delivery.get("failed"):
                raise RuntimeError(
                    "one or more backtest review notifications were not delivered"
                )
            cursor.execute(
                "UPDATE market_signal_runs SET notified_count=1, updated_at=%s WHERE run_key=%s",
                (_now_taipei(), run_key),
            )
            connection.commit()
            return {
                "sent": sent_count, "failed": failed_count, "mode": "daily",
                "candidates": candidate_count, "batches": len(batches),
                "matured_outcome_review": outcome_review,
                "annual_review_delivery": annual_review_delivery,
            }

        _refresh_intraday_company_names(cursor, candidates)
        batches: list[list[dict[str, Any]]] = []
        current_rows: list[dict[str, Any]] = []
        current_size = 0
        for row in candidates:
            embed = _format_intraday_candidate(row)
            embed_size = _embed_character_count(embed)
            prospective_rows = [*current_rows, row]
            prospective_message, prospective_embeds = _format_intraday_batch(prospective_rows)
            prospective_bytes = _discord_request_size(prospective_message, prospective_embeds)
            if current_rows and (
                len(current_rows) >= DISCORD_EMBED_COUNT_LIMIT
                or current_size + embed_size > DISCORD_EMBED_CHAR_LIMIT
                or prospective_bytes > DISCORD_REQUEST_BYTE_LIMIT
            ):
                batches.append(current_rows)
                current_rows, current_size = [], 0
            current_rows.append(row)
            current_size += embed_size
        if current_rows:
            batches.append(current_rows)

        all_delivered = True
        cursor_stalled = False
        sent_count = 0
        source_warning_status = "not_evaluated"
        source_warning_receipt_confirmed = False
        notified_candidates: set[str] = set()
        for batch_rows in batches:
            unsent_rows: list[dict[str, Any]] = []
            for row in batch_rows:
                dedupe_key = hashlib.sha256(
                    f"intraday|{row['candidate_hash']}".encode("utf-8")
                ).hexdigest()
                existing = _notification_record(cursor, dedupe_key)
                if existing and existing[0] == "sent":
                    notified_candidates.add(row["candidate_hash"])
                    continue
                if existing and existing[0] in {"accepted_unverified", "delivery_unknown"}:
                    # This terminal ambiguous attempt is already recorded. Do not
                    # resend or let it hold the source cursor on later natural runs.
                    if existing[2] == run_key:
                        all_delivered = False
                    continue
                unsent_rows.append(row)
            if not unsent_rows:
                continue
            claim_ok = True
            dedupe_keys: list[str] = []
            claimed_rows: list[dict[str, Any]] = []
            now = _now_taipei()
            for row in unsent_rows:
                dedupe_key = hashlib.sha256(
                    f"intraday|{row['candidate_hash']}".encode("utf-8")
                ).hexdigest()
                existing = _notification_record(cursor, dedupe_key)
                if existing and existing[0] == "pending" and existing[1] \
                        and now - existing[1] < dt.timedelta(minutes=1):
                    claim_ok = False
                    cursor_stalled = True
                    continue
                claimed_rows.append(row)
            if not claimed_rows:
                all_delivered = all_delivered and claim_ok
                continue
            message, embeds = _format_intraday_batch(claimed_rows)
            ledger_message = _serialize_discord_payload(message, embeds)
            rows_to_send = claimed_rows
            claimed_rows = []
            for row in rows_to_send:
                dedupe_key = hashlib.sha256(
                    f"intraday|{row['candidate_hash']}".encode("utf-8")
                ).hexdigest()
                if _claim_notification(
                    cursor, dedupe_key=dedupe_key, run_key=run_key,
                    message=ledger_message, now=now,
                ):
                    dedupe_keys.append(dedupe_key)
                    claimed_rows.append(row)
                else:
                    existing = _notification_record(cursor, dedupe_key)
                    if existing and existing[0] == "sent":
                        notified_candidates.add(row["candidate_hash"])
                    elif existing and existing[0] in {"accepted_unverified", "delivery_unknown"}:
                        if existing[2] == run_key:
                            all_delivered = False
                    else:
                        claim_ok = False
                        cursor_stalled = True
            connection.commit()
            if not claimed_rows:
                all_delivered = all_delivered and claim_ok
                continue
            message, embeds = _format_intraday_batch(claimed_rows)
            ledger_message = _serialize_discord_payload(message, embeds)
            if not claim_ok:
                all_delivered = False
            delivery_result = None
            try:
                delivery_result = notifier.notify_with_receipt(
                    msg=message, embeds=embeds,
                )
                notification_status = _notification_delivery_status(delivery_result)
                receipt_id = (
                    delivery_result.get("receipt_id")
                    if isinstance(delivery_result, dict) else None
                )
                http_status = (
                    delivery_result.get("http_status")
                    if isinstance(delivery_result, dict) else None
                )
                notification_error_type = (
                    delivery_result.get("error_type")
                    if isinstance(delivery_result, dict) else None
                )
                for dedupe_key in dedupe_keys:
                    _mark_notification(
                        cursor, dedupe_key=dedupe_key,
                        status=notification_status,
                        message=ledger_message, now=_now_taipei(),
                        receipt_id=str(receipt_id) if notification_status == "sent" else None,
                        http_status=http_status,
                        notification_error_type=notification_error_type,
                    )
                connection.commit()
                if notification_status != "sent":
                    # Do not retry an accepted or ambiguous request: Discord may already
                    # have created the message, and the webhook has no idempotency key.
                    all_delivered = False
                    if notification_status == "failed":
                        cursor_stalled = True
                    continue
                sent_count += 1
                notified_candidates.update(row["candidate_hash"] for row in claimed_rows)
            except Exception as exc:
                connection.rollback()
                notification_status = _notification_delivery_status(delivery_result)
                receipt_id = (
                    delivery_result.get("receipt_id")
                    if isinstance(delivery_result, dict) else None
                )
                for dedupe_key in dedupe_keys:
                    try:
                        _mark_notification(
                            cursor, dedupe_key=dedupe_key, status=notification_status,
                            message=ledger_message, now=_now_taipei(),
                            receipt_id=(
                                str(receipt_id)
                                if notification_status == "sent" else None
                            ),
                            http_status=(
                                delivery_result.get("http_status")
                                if isinstance(delivery_result, dict) else None
                            ),
                            notification_error_type=(
                                delivery_result.get("error_type")
                                if isinstance(delivery_result, dict)
                                and delivery_result.get("error_type")
                                else type(exc).__name__
                            ),
                        )
                    except Exception:
                        pass
                connection.commit()
                if notification_status == "sent":
                    sent_count += 1
                    notified_candidates.update(
                        row["candidate_hash"] for row in claimed_rows
                    )
                else:
                    all_delivered = False
                    if notification_status == "failed":
                        cursor_stalled = True

        if not candidates:
            all_delivered = True
        collection_status = payload.get("source_collection_status", "unknown")
        if mode == "intraday":
            source_issues = (
                _source_collection_issues(
                    cursor,
                    payload.get("source_dag_run_id"),
                    payload.get("source_schedule_hint"),
                    payload.get("source_dag_id"),
                )
                if collection_status != "complete" else []
            )
            current_fingerprint = _source_collection_issue_fingerprint(
                collection_status, source_issues
            )
            previous_fingerprint = _load_source_collection_alert_fingerprint(cursor)
            if current_fingerprint == "complete":
                source_warning_status = (
                    "cleared" if previous_fingerprint != "complete" else "not_needed"
                )
                if previous_fingerprint != "complete":
                    _save_source_collection_alert_fingerprint(
                        cursor, connection, "complete", _now_taipei()
                    )
            elif current_fingerprint != previous_fingerprint:
                source_warning_status = "unconfirmed"
                source_run_id = str(payload.get("source_dag_run_id") or run_key)
                data_review_required = collection_status == "data_review_required"
                warning = (
                    "⚠️ 台股訊號雷達｜來源批次品質待確認"
                    if data_review_required
                    else "⚠️ 台股訊號雷達｜本輪來源收集不完整"
                )
                displayed_issues = source_issues[:7]
                if len(source_issues) > len(displayed_issues):
                    displayed_issues.append(f"…另有 {len(source_issues) - len(displayed_issues)} 項")
                warning_embed = {
                    "title": (
                        "來源批次品質待確認"
                        if data_review_required else "部分來源未完成"
                    ),
                    "description": (
                        "來源任務已成功，但至少一批有隔離列、空回應或列數核帳問題；"
                        "未確認來源語意前，訊號名單請視為暫定。"
                        if data_review_required
                        else "本輪仍已分析可用資料；訊號名單請視為暫定。"
                    ),
                    "color": 0xD68910,
                    "fields": [
                        {"name": "來源待確認項目", "value": "\n".join(displayed_issues)[:1000], "inline": False},
                        {"name": "來源 DAG run", "value": _escape_discord_markdown(source_run_id, 140), "inline": False},
                    ],
                }
                warning_key = hashlib.sha256(
                    f"collection-warning|{source_run_id}".encode("utf-8")
                ).hexdigest()
                existing_warning = _notification_record(cursor, warning_key)
                warning_receipt_confirmed = bool(
                    existing_warning and existing_warning[0] == "sent"
                )
                if warning_receipt_confirmed:
                    source_warning_status = "already_sent"
                    source_warning_receipt_confirmed = True
                    _save_source_collection_alert_fingerprint(
                        cursor, connection, current_fingerprint, _now_taipei()
                    )
                elif _send_and_record(
                    cursor, connection, notifier, dedupe_key=warning_key,
                    run_key=run_key, message=warning, embeds=[warning_embed],
                    now=_now_taipei(),
                ):
                    source_warning_status = "sent"
                    source_warning_receipt_confirmed = True
                    _save_source_collection_alert_fingerprint(
                        cursor, connection, current_fingerprint, _now_taipei()
                    )
                else:
                    warning_record = _notification_record(cursor, warning_key)
                    if warning_record and warning_record[0] in {
                        "accepted_unverified", "delivery_unknown",
                    }:
                        # Preserve at-most-once behavior after an ambiguous receipt.
                        _save_source_collection_alert_fingerprint(
                            cursor, connection, current_fingerprint, _now_taipei()
                        )
                    else:
                        cursor_stalled = True
                    all_delivered = False
            else:
                source_warning_status = "unchanged"
        if not cursor_stalled:
            proposed_cursor = payload.get("proposed_source_cursor_id")
            if proposed_cursor is not None:
                cursor.execute(
                    """
                    INSERT INTO market_signal_state (state_key, state_text, updated_at)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE state_text=VALUES(state_text), updated_at=VALUES(updated_at)
                    """,
                    (INTRADAY_CURSOR_KEY, str(int(proposed_cursor)), _now_taipei()),
                )
        cursor.execute(
            "UPDATE market_signal_runs SET notified_count=%s, updated_at=%s WHERE run_key=%s",
            (len(notified_candidates), _now_taipei(), run_key),
        )
        connection.commit()
        if not all_delivered:
            raise RuntimeError("one or more intraday signal messages were not delivered")
        return {
            "sent": sent_count,
            "candidate_batches_sent": sent_count,
            "failed": int(not all_delivered),
            "mode": "intraday",
            "candidates": len(candidates),
            "source_warning_status": source_warning_status,
            "source_warning_receipt_confirmed": source_warning_receipt_confirmed,
        }
    except Exception as exc:
        if str(exc) in {
            "market scan failed; failure notice was delivered",
            "market scan did not finish; failure notice was delivered",
        }:
            raise
        if connection:
            connection.rollback()
        failure_payload = dict(payload)
        failure_payload["error_message"] = f"通知階段：{type(exc).__name__}: {exc}"[:500]
        sent = _send_failure_notice(
            failure_payload, notifier, cursor=cursor, connection=connection,
        )
        if not sent:
            raise RuntimeError("market notification failed and its failure notice was not delivered") from exc
        raise RuntimeError("market notification failed; failure notice was delivered") from exc
    finally:
        if connection:
            connection.close()
        if engine:
            engine.dispose()
