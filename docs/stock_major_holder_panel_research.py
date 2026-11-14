"""Read-only exploratory panel analysis for historical TDCC holder-share changes.

Run inside the Airflow container so the existing local stock-database
configuration remains in place. The repository ``docs`` folder is not mounted
there, so first copy the script into the worker::

    docker cp docs/stock_major_holder_panel_research.py dataflow-airflow-worker-1:/tmp/stock_major_holder_panel_research.py
    docker exec dataflow-airflow-worker-1 bash -lc 'PYTHONPATH=/opt/airflow/dags python /tmp/stock_major_holder_panel_research.py'

This is research output, not a production signal or a substitute for the
candidate/outcome ledger. It deliberately requires stored rows to have been
loaded soon after their effective date and enters at a later close.
"""

from __future__ import annotations

import json
import statistics
from collections import defaultdict

from lib.db_connection import MySQLConnection


PANEL_SQL = """
WITH holder_base AS (
    SELECT TRIM(證券代號) AS code,
           STR_TO_DATE(資料日期, %s) AS effective_date,
           MAX(`>400張大股東持有百分比`) AS pct400,
           MAX(`>1000張大股東持有百分比`) AS pct1000,
           MIN(create_time) AS created_at
    FROM 大戶比例
    WHERE 資料日期 REGEXP %s
      AND 資料日期 BETWEEN %s AND %s
    GROUP BY TRIM(證券代號), STR_TO_DATE(資料日期, %s)
    HAVING COUNT(DISTINCT `>400張大股東持有百分比`) = 1
       AND COUNT(DISTINCT `>1000張大股東持有百分比`) = 1
       AND COUNT(DISTINCT create_time) = 1
),
snapshot_stats AS (
    SELECT effective_date,
           COUNT(*) AS raw_codes,
           SUM(DATEDIFF(created_at, effective_date) BETWEEN 0 AND 7) AS timely_codes
    FROM holder_base
    GROUP BY effective_date
),
eligible_dates AS (
    SELECT effective_date, raw_codes, timely_codes
    FROM snapshot_stats
    WHERE raw_codes > 0 AND timely_codes / raw_codes >= 0.95
),
valid_holders AS (
    SELECT h.*
    FROM holder_base h
    INNER JOIN eligible_dates e ON e.effective_date = h.effective_date
    WHERE DATEDIFF(h.created_at, h.effective_date) BETWEEN 0 AND 7
),
lagged AS (
    SELECT code, effective_date, pct400, pct1000, created_at,
           LAG(effective_date) OVER (
               PARTITION BY code ORDER BY effective_date
           ) AS prior_date,
           LAG(pct400) OVER (
               PARTITION BY code ORDER BY effective_date
           ) AS prior_pct400,
           LAG(pct1000) OVER (
               PARTITION BY code ORDER BY effective_date
           ) AS prior_pct1000,
           LAG(created_at) OVER (
               PARTITION BY code ORDER BY effective_date
           ) AS prior_created_at
    FROM valid_holders
),
signals AS (
    SELECT code, effective_date, created_at,
           pct400 - prior_pct400 AS change400,
           pct1000 - prior_pct1000 AS change1000
    FROM lagged
    WHERE prior_date IS NOT NULL
      AND DATEDIFF(effective_date, prior_date) BETWEEN 5 AND 14
      AND prior_created_at < created_at
),
price_base AS (
    SELECT TRIM(證券代號) AS code,
           STR_TO_DATE(資料日期, %s) AS trade_date,
           MAX(收盤) AS close_price
    FROM 個股成交資訊
    WHERE 資料日期 REGEXP %s
      AND 資料日期 BETWEEN %s AND %s
    GROUP BY TRIM(證券代號), STR_TO_DATE(資料日期, %s)
    HAVING COUNT(DISTINCT 收盤) = 1 AND MAX(收盤) > 0
),
price_forward AS (
    SELECT code, trade_date, close_price,
           LEAD(close_price, 1) OVER (
               PARTITION BY code ORDER BY trade_date
           ) AS close_h1,
           LEAD(close_price, 5) OVER (
               PARTITION BY code ORDER BY trade_date
           ) AS close_h5,
           LEAD(close_price, 10) OVER (
               PARTITION BY code ORDER BY trade_date
           ) AS close_h10
    FROM price_base
),
entry_candidates AS (
    SELECT s.code, s.effective_date, s.created_at,
           s.change400, s.change1000,
           p.trade_date AS signal_trade_date,
           p.close_price AS entry_close,
           p.close_h1, p.close_h5, p.close_h10,
           ROW_NUMBER() OVER (
               PARTITION BY s.code, s.effective_date ORDER BY p.trade_date
           ) AS entry_rank
    FROM signals s
    INNER JOIN price_forward p
        ON p.code = s.code
       AND p.trade_date > DATE(s.created_at)
       AND p.trade_date <= DATE_ADD(DATE(s.created_at), INTERVAL 7 DAY)
),
entries AS (
    SELECT * FROM entry_candidates WHERE entry_rank = 1
),
return_rows AS (
    SELECT effective_date, 1 AS factor, change400,
           CASE WHEN change400 > 2 THEN 1
                WHEN change400 < -2 THEN -1 ELSE 0 END AS bucket,
           1 AS horizon,
           (close_h1 / entry_close - 1) * 100 AS return_pp
    FROM entries WHERE close_h1 > 0
    UNION ALL
    SELECT effective_date, 1, change400,
           CASE WHEN change400 > 2 THEN 1
                WHEN change400 < -2 THEN -1 ELSE 0 END,
           5, (close_h5 / entry_close - 1) * 100
    FROM entries WHERE close_h5 > 0
    UNION ALL
    SELECT effective_date, 1, change400,
           CASE WHEN change400 > 2 THEN 1
                WHEN change400 < -2 THEN -1 ELSE 0 END,
           10, (close_h10 / entry_close - 1) * 100
    FROM entries WHERE close_h10 > 0
    UNION ALL
    SELECT effective_date, 2, change1000,
           CASE WHEN change1000 > 2 THEN 1
                WHEN change1000 < -2 THEN -1 ELSE 0 END,
           1, (close_h1 / entry_close - 1) * 100
    FROM entries WHERE close_h1 > 0
    UNION ALL
    SELECT effective_date, 2, change1000,
           CASE WHEN change1000 > 2 THEN 1
                WHEN change1000 < -2 THEN -1 ELSE 0 END,
           5, (close_h5 / entry_close - 1) * 100
    FROM entries WHERE close_h5 > 0
    UNION ALL
    SELECT effective_date, 2, change1000,
           CASE WHEN change1000 > 2 THEN 1
                WHEN change1000 < -2 THEN -1 ELSE 0 END,
           10, (close_h10 / entry_close - 1) * 100
    FROM entries WHERE close_h10 > 0
),
daily_baseline AS (
    SELECT effective_date, factor, horizon, AVG(return_pp) AS baseline_pp
    FROM return_rows
    GROUP BY effective_date, factor, horizon
),
daily_excess AS (
    SELECT r.effective_date, r.factor, r.bucket, r.horizon,
           COUNT(*) AS observations,
           AVG(r.return_pp - b.baseline_pp) AS excess_pp,
           AVG(CASE WHEN r.bucket = 1 AND r.return_pp > b.baseline_pp THEN 1
                    WHEN r.bucket = -1 AND r.return_pp < b.baseline_pp THEN 1
                    WHEN r.bucket <> 0 THEN 0 ELSE NULL END) AS directional_hit
    FROM return_rows r
    INNER JOIN daily_baseline b
        ON b.effective_date = r.effective_date
       AND b.factor = r.factor
       AND b.horizon = r.horizon
    GROUP BY r.effective_date, r.factor, r.bucket, r.horizon
)
SELECT YEAR(effective_date) AS effective_year,
       effective_date, factor, bucket, horizon, observations,
       excess_pp, directional_hit,
       (SELECT COUNT(*) FROM eligible_dates) AS eligible_snapshot_dates,
       (SELECT ROUND(AVG(timely_codes / raw_codes), 4)
        FROM eligible_dates) AS mean_stored_row_timeliness
FROM daily_excess
ORDER BY effective_date, factor, horizon, bucket
"""

PARAMS = (
    "%Y%m%d", "^[0-9]{8}$", "20230101", "20250425", "%Y%m%d",
    "%Y-%m-%d", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$",
    "2023-01-01", "2025-12-31", "%Y-%m-%d",
)


def main() -> None:
    stock = MySQLConnection(db_name="stock")
    try:
        cursor = stock.conn.cursor()
        cursor.execute(PANEL_SQL, PARAMS)
        rows = cursor.fetchall()
    finally:
        stock.conn.close()

    grouped: dict[tuple[str, int, int, int], list[tuple[str, float, float | None]]] = defaultdict(list)
    eligible_snapshot_dates = 0
    mean_stored_row_timeliness = None
    for year, signal_date, factor, bucket, horizon, _n, excess, hit, snapshot_dates, timely in rows:
        eligible_snapshot_dates = int(snapshot_dates or 0)
        mean_stored_row_timeliness = float(timely) if timely is not None else None
        key = (str(year), int(factor), int(bucket), int(horizon))
        grouped[key].append(
            (
                str(signal_date),
                float(excess),
                float(hit) if hit is not None else None,
            )
        )

    summary = []
    for (year, factor, bucket, horizon), observations in sorted(grouped.items()):
        daily_excess = [row[1] for row in observations]
        daily_hits = [row[2] for row in observations if row[2] is not None]
        summary.append({
            "effective_year": year,
            "holder_metric": ">400_lot" if factor == 1 else ">1000_lot",
            "change_bucket": "increase_gt_2pp" if bucket == 1 else (
                "decrease_lt_minus_2pp" if bucket == -1 else "within_2pp"
            ),
            "horizon_sessions": horizon,
            "signal_dates": len({row[0] for row in observations}),
            "equal_date_mean_excess_pp": round(statistics.mean(daily_excess), 4),
            "median_date_excess_pp": round(statistics.median(daily_excess), 4),
            "equal_date_directional_hit_rate": (
                round(statistics.mean(daily_hits), 4) if daily_hits else None
            ),
        })

    output = {
        "eligible_snapshot_dates": eligible_snapshot_dates,
        "mean_stored_row_timeliness": mean_stored_row_timeliness,
        "method": {
            "availability_gate": "stored create_time is 0-7 days after effective date; snapshot requires >=95% of stored rows to pass",
            "entry": "first valid security close strictly after DATE(create_time), within 7 calendar days",
            "outcomes": "unadjusted close-to-close returns after 1, 5, and 10 further sessions",
            "excess_baseline": "same effective-date and horizon mean return across the eligible TDCC panel",
            "threshold": "strict +/-2 percentage-point change, matching existing context display threshold",
            "limits": [
                "coverage gate is relative to rows in the legacy table, not a historical listed/OTC master",
                "current-code survivorship and historical market membership are unresolved",
                "weekly observations and forward horizons overlap",
                "returns exclude corporate actions and transaction costs",
                "descriptive research only; do not add a directional vote or tune thresholds from it",
            ],
        },
        "results": summary,
    }
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
