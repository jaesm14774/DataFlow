"""Read-only SQL query runner used by stock_signal_family_history_quality.ipynb.

Run this through the project's airflow-scheduler container so connection details
stay in the existing local configuration and are never copied into the notebook.
"""

import json
import sys
from collections import defaultdict
from statistics import median

sys.path.insert(0, "/opt/airflow/dags")

from lib.db_connection import MySQLConnection
from source_extension.registry import _open_connection


result = {}


def query(cursor, key, sql):
    cursor.execute(sql)
    columns = [column[0] for column in cursor.description]
    result[key] = [dict(zip(columns, row)) for row in cursor.fetchall()]


source_connection, source_cursor, source_engine = _open_connection()
try:
    query(
        source_cursor,
        "source_history",
        """
        SELECT source_key, event_type, quality_status, COUNT(*) AS rows_n,
               COUNT(DISTINCT entity_id) AS entities_n,
               COUNT(DISTINCT effective_date) AS effective_dates_n,
               MIN(effective_date) AS min_effective, MAX(effective_date) AS max_effective,
               MIN(event_time) AS min_event_time, MAX(event_time) AS max_event_time
        FROM source_records
        WHERE source_key IN ('twse_monthly_revenue','tpex_monthly_revenue',
            'twse_quarterly_profitability','tpex_quarterly_profitability',
            'twse_director_holdings_monthly','tpex_director_holdings_monthly',
            'twse_market_index_daily','tpex_market_index_daily',
            'twse_quarterly_forecast_variance','twse_quarterly_forecast_attainment')
        GROUP BY source_key, event_type, quality_status
        ORDER BY source_key, quality_status
        """,
    )
    query(
        source_cursor,
        "source_runs_latest",
        """
        SELECT r.source_key, r.started_at, r.finished_at, r.status, r.row_count,
               r.accepted_count, r.rejected_count, r.duplicate_count, r.dag_id, r.task_id
        FROM source_runs r JOIN (
            SELECT source_key, MAX(id) AS max_id FROM source_runs
            WHERE source_key IN ('twse_monthly_revenue','tpex_monthly_revenue',
                'twse_market_index_daily','tpex_market_index_daily',
                'twse_quarterly_forecast_variance','twse_quarterly_forecast_attainment')
            GROUP BY source_key
        ) latest ON latest.max_id=r.id ORDER BY r.source_key
        """,
    )
    query(
        source_cursor,
        "daily_signal_runs",
        """
        SELECT r.as_of_date, r.status, r.source_status, r.universe_count,
               r.price_count, r.flow_count, r.revenue_count, r.financial_count,
               r.candidate_count, r.started_at, r.finished_at
        FROM market_signal_runs r WHERE r.scan_mode='daily'
        ORDER BY r.started_at DESC LIMIT 12
        """,
    )
    query(
        source_cursor,
        "outcomes",
        """
        SELECT outcome_price_source_key, outcome_basis, horizon_sessions,
               status, source_status,
               COUNT(*) AS rows_n, COUNT(DISTINCT candidate_id) AS candidates_n,
               MIN(signal_date) AS min_signal_date, MAX(signal_date) AS max_signal_date,
               SUM(directional_hit IS NOT NULL) AS hit_scored_n,
               AVG(CASE WHEN directional_hit IS NOT NULL THEN directional_hit END) AS directional_hit_rate,
               AVG(excess_return_pp) AS mean_excess_return_pp
        FROM market_signal_outcomes
        GROUP BY outcome_price_source_key, outcome_basis, horizon_sessions,
                 status, source_status
        ORDER BY outcome_price_source_key, outcome_basis, horizon_sessions, status
        """,
    )
    source_cursor.execute(
        """
        SELECT o.candidate_id, o.market, o.source_status,
               o.outcome_price_source_key, o.signal_date,
               o.outcome_basis, o.horizon_sessions, o.excess_return_pp,
               c.details_text
        FROM market_signal_outcomes o
        INNER JOIN market_signal_candidates c ON c.id=o.candidate_id
        WHERE o.status='settled' AND o.excess_return_pp IS NOT NULL
        ORDER BY o.signal_date, o.candidate_id, o.outcome_basis, o.horizon_sessions
        """
    )
    family_columns = [column[0] for column in source_cursor.description]
    family_rows = [dict(zip(family_columns, row)) for row in source_cursor.fetchall()]
    family_groups = defaultdict(lambda: {
        "candidate_ids": set(), "signal_dates": set(), "signed_excess": [],
    })
    for row in family_rows:
        try:
            details = json.loads(row["details_text"] or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(details, dict):
            continue
        directions = details.get("family_directions") or {}
        if not isinstance(directions, dict):
            continue
        try:
            excess = float(row["excess_return_pp"])
        except (TypeError, ValueError):
            continue
        for family, raw_direction in directions.items():
            try:
                direction = int(raw_direction)
            except (TypeError, ValueError, OverflowError):
                continue
            if direction not in {-1, 1}:
                continue
            key = (
                str(family), str(row["outcome_basis"]),
                int(row["horizon_sessions"]), str(row["source_status"]),
                str(row["market"]), str(row["outcome_price_source_key"]),
            )
            group = family_groups[key]
            group["candidate_ids"].add(int(row["candidate_id"]))
            group["signal_dates"].add(str(row["signal_date"]))
            group["signed_excess"].append(direction * excess)

    result["family_attribution"] = []
    for key, group in sorted(family_groups.items()):
        signed_excess = group["signed_excess"]
        family_hit_n = sum(value > 0 for value in signed_excess)
        result["family_attribution"].append({
            "family": key[0],
            "outcome_basis": key[1],
            "horizon_sessions": key[2],
            "source_status": key[3],
            "market": key[4],
            "outcome_price_source_key": key[5],
            "candidate_n": len(group["candidate_ids"]),
            "signal_dates_n": len(group["signal_dates"]),
            "directional_outcome_n": len(signed_excess),
            "family_hit_n": family_hit_n,
            "family_hit_rate": round(family_hit_n / len(signed_excess), 4),
            "mean_signed_excess_return_pp": round(
                sum(signed_excess) / len(signed_excess), 6
            ),
            "median_signed_excess_return_pp": round(median(signed_excess), 6),
        })
    result["family_attribution_note"] = (
        "Descriptive only: observations overlap when one candidate has multiple "
        "families and multiple outcomes. Family hit means family direction times "
        "market-relative excess return is positive. Returns inherit the outcome "
        "ledger's unadjusted-close and pre-cost limitations; do not tune from a "
        "single signal date."
    )
finally:
    source_connection.close()
    source_engine.dispose()

stock = MySQLConnection(db_name="stock")
try:
    cursor = stock.conn.cursor()
    query(
        cursor,
        "stock_history",
        """
        SELECT 'daily_price' AS dataset, MIN(資料日期) AS min_date,
               MAX(資料日期) AS max_date FROM 個股成交資訊
        UNION ALL
        SELECT 'institutional_flow', MIN(資料日期), MAX(資料日期)
        FROM 三大法人買賣
        """,
    )
    query(
        cursor,
        "pre_2014_date_buckets",
        """
        SELECT dataset, stored_year, rows_n, min_date, max_date
        FROM (
            SELECT 'daily_price' AS dataset, YEAR(資料日期) AS stored_year,
                   COUNT(*) AS rows_n, MIN(資料日期) AS min_date, MAX(資料日期) AS max_date
            FROM 個股成交資訊 WHERE 資料日期 < '2014-01-01' GROUP BY YEAR(資料日期)
            UNION ALL
            SELECT 'institutional_flow', YEAR(資料日期), COUNT(*), MIN(資料日期), MAX(資料日期)
            FROM 三大法人買賣 WHERE 資料日期 < '2014-01-01' GROUP BY YEAR(資料日期)
        ) old_dates ORDER BY dataset, stored_year
        """,
    )
    query(
        cursor,
        "pre_1000_shifted_key_check",
        """
        SELECT dataset, YEAR(stored_date) AS stored_year, COUNT(*) AS rows_n,
               COUNT(DISTINCT security_code) AS codes_n,
               SUM(shifted_key_exists) AS keys_also_present_at_roc_plus_1911_n
        FROM (
            SELECT 'daily_price' AS dataset, p.資料日期 AS stored_date,
                   p.證券代號 AS security_code,
                   EXISTS (
                       SELECT 1 FROM 個股成交資訊 q
                       WHERE q.證券代號=p.證券代號
                         AND q.資料日期=DATE_ADD(p.資料日期, INTERVAL 1911 YEAR)
                   ) AS shifted_key_exists
            FROM 個股成交資訊 p WHERE p.資料日期 < '1000-01-01'
        ) suspicious_dates GROUP BY dataset, YEAR(stored_date)
        ORDER BY dataset, stored_year
        """,
    )
    query(
        cursor,
        "market_universe",
        """
        SELECT 市場別 AS market, COUNT(*) AS master_rows_n,
               COUNT(DISTINCT TRIM(證券代號)) AS master_codes_n
        FROM 全證券代碼相關資料表 WHERE 市場別 IN ('上市','上櫃')
        GROUP BY 市場別 ORDER BY 市場別
        """,
    )
    query(
        cursor,
        "latest_price_coverage",
        """
        SELECT p.資料日期 AS trade_date, COALESCE(m.市場別,'unmatched') AS market,
               COUNT(*) AS rows_n, COUNT(DISTINCT TRIM(p.證券代號)) AS codes_n,
               SUM(p.收盤 IS NULL) AS null_close_n, SUM(p.成交量 IS NULL) AS null_volume_n
        FROM 個股成交資訊 p
        LEFT JOIN 全證券代碼相關資料表 m ON TRIM(p.證券代號)=TRIM(m.證券代號)
        WHERE p.資料日期=(SELECT MAX(資料日期) FROM 個股成交資訊)
        GROUP BY p.資料日期, m.市場別 ORDER BY market
        """,
    )
    query(
        cursor,
        "latest_flow_coverage",
        """
        SELECT f.資料日期 AS trade_date, COALESCE(m.市場別,'unmatched') AS market,
               COUNT(*) AS rows_n, COUNT(DISTINCT TRIM(f.證券代號)) AS codes_n,
               SUM(f.三大法人買賣超股數 IS NULL) AS null_total_n,
               SUM(f.外資買賣超股數 IS NULL) AS null_foreign_n
        FROM 三大法人買賣 f
        LEFT JOIN 全證券代碼相關資料表 m ON TRIM(f.證券代號)=TRIM(m.證券代號)
        WHERE f.資料日期=(SELECT MAX(資料日期) FROM 三大法人買賣)
        GROUP BY f.資料日期, m.市場別 ORDER BY market
        """,
    )
    query(
        cursor,
        "recent_duplicate_grain",
        """
        SELECT dataset, COUNT(*) AS duplicate_keys_n, SUM(excess_rows) AS excess_rows_n
        FROM (
            SELECT 'daily_price' AS dataset, 資料日期, 證券代號, COUNT(*)-1 AS excess_rows
            FROM 個股成交資訊
            WHERE 資料日期 >= DATE_SUB((SELECT MAX(資料日期) FROM 個股成交資訊), INTERVAL 18 DAY)
            GROUP BY 資料日期, 證券代號 HAVING COUNT(*)>1
            UNION ALL
            SELECT 'institutional_flow', 資料日期, 證券代號, COUNT(*)-1
            FROM 三大法人買賣
            WHERE 資料日期 >= DATE_SUB((SELECT MAX(資料日期) FROM 三大法人買賣), INTERVAL 18 DAY)
            GROUP BY 資料日期, 證券代號 HAVING COUNT(*)>1
        ) duplicates GROUP BY dataset
        """,
    )
    cursor.close()
finally:
    stock.end()

print(json.dumps(result, ensure_ascii=False, default=str))
