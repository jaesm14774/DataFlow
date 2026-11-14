"""Read-only cross-sectional study of the existing institutional-flow signal.

Run inside the Airflow worker, where the existing local stock DB configuration
is available::

    docker cp docs/institutional_flow_intensity_research.py \
        dataflow-airflow-worker-1:/tmp/institutional_flow_intensity_research.py
    docker exec dataflow-airflow-worker-1 bash -lc \
        'PYTHONPATH=/opt/airflow/dags python /tmp/institutional_flow_intensity_research.py'

The script reads ``stock.三大法人買賣`` and ``stock.個股成交資訊`` only. It
compares the production rule's fixed 100,000-share threshold with a
within-market/day top/bottom-decile ranking of five-session net shares divided
by the preceding 20-session average reported volume. It also splits the
existing flow signals by whether five- or twenty-session market-relative price
momentum agrees with the flow direction. The percentile ranking is within each
market, so it does not compare legacy TWSE lots with TPEx volume units. Results
are exploratory, not a production scoring change.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from lib.db_connection import MySQLConnection


START_DATE = dt.date(2023, 1, 1)
READ_START_DATE = dt.date(2022, 11, 1)  # warm-up for 20-session volume
MIN_MARKET_COVERAGE = 0.70  # existing scanner completeness floor
OUT_PATH = Path("/tmp/institutional_flow_intensity_research.json")
HORIZONS = (1, 5, 10)


def _date(value: Any) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if value:
        return dt.date.fromisoformat(str(value)[:10])
    return None


def _listing_date(value: Any) -> dt.date | None:
    """Parse the security-master listing date, including Taiwan ROC years."""
    if isinstance(value, (dt.date, dt.datetime)):
        return _date(value)
    if value is None:
        return None
    parts = str(value).strip().replace("-", "/").split("/")
    if len(parts) != 3:
        return None
    try:
        year = int(parts[0])
        if len(parts[0]) <= 3:
            year += 1911
        return dt.date(year, int(parts[1]), int(parts[2]))
    except (TypeError, ValueError, OverflowError):
        return None


def _eligible_codes(
    codes: list[str], listing_dates: dict[str, dt.date], as_of_date: dt.date
) -> list[str]:
    return [
        code for code in codes
        if listing_dates.get(code) is not None and listing_dates[code] <= as_of_date
    ]


def _code(value: Any) -> str:
    result = str(value or "").strip()
    return result.zfill(4) if result.isdigit() and len(result) < 4 else result


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _entry_open_utc_naive(trade_date: dt.date) -> dt.datetime:
    taipei_open = dt.datetime.combine(trade_date, dt.time(9, 0)).replace(
        tzinfo=ZoneInfo("Asia/Taipei")
    )
    return taipei_open.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _query_history() -> tuple[
    dict[str, str], dict[str, dt.date],
    dict[str, dict[dt.date, tuple[float | None, float | None, float | None, dt.datetime]]],
    dict[str, dict[dt.date, tuple[float, dt.datetime | None]]], dict[str, Any],
]:
    db = MySQLConnection(db_name="stock")
    cursor = db.conn.cursor()
    try:
        cursor.execute(
            """SELECT TRIM(證券代號), TRIM(市場別), TRIM(上市日)
               FROM 全證券代碼相關資料表
               WHERE 市場別 IN ('上市','上櫃') AND 證券代號 IS NOT NULL
                 AND CFICode LIKE 'ES%'"""
        )
        master_rows = cursor.fetchall()
        market_by_code: dict[str, str] = {}
        listing_date_by_code: dict[str, dt.date] = {}
        invalid_listing_dates = 0
        for raw_code, raw_market, raw_listing_date in master_rows:
            code = _code(raw_code)
            market_by_code[code] = str(raw_market)
            listing_date = _listing_date(raw_listing_date)
            if listing_date is None:
                invalid_listing_dates += 1
            else:
                listing_date_by_code[code] = listing_date

        cursor.execute(
            """SELECT TRIM(p.證券代號), m.市場別, p.資料日期,
                      MAX(p.開盤), MAX(p.收盤), MAX(p.成交量), MIN(p.create_time),
                      COUNT(*), COUNT(p.create_time),
                      COUNT(DISTINCT p.開盤), COUNT(DISTINCT p.收盤),
                      COUNT(DISTINCT p.成交量)
               FROM 個股成交資訊 p
               JOIN 全證券代碼相關資料表 m
                 ON TRIM(p.證券代號)=TRIM(m.證券代號)
               WHERE p.資料日期 >= %s AND m.市場別 IN ('上市','上櫃')
               GROUP BY TRIM(p.證券代號), m.市場別, p.資料日期""",
            (READ_START_DATE,),
        )
        prices: dict[str, dict[dt.date, tuple[float | None, float | None, float | None, dt.datetime]]] = defaultdict(dict)
        duplicate_groups = 0
        conflicting_price_groups = 0
        missing_price_create_time_groups = 0
        price_dates: dict[str, set[dt.date]] = {"上市": set(), "上櫃": set()}
        price_codes_by_date: dict[tuple[str, dt.date], set[str]] = defaultdict(set)
        for row in cursor.fetchall():
            (
                code, market, raw_date, opening, closing, volume, created_at,
                row_count, created_count, open_versions, close_versions, volume_versions,
            ) = row
            trade_date = _date(raw_date)
            code = _code(code)
            if not trade_date or code not in market_by_code:
                continue
            if int(row_count) > 1:
                duplicate_groups += 1
            if max(int(open_versions), int(close_versions), int(volume_versions)) > 1:
                conflicting_price_groups += 1
                continue
            if created_at is None or int(created_count) != int(row_count):
                missing_price_create_time_groups += 1
                continue
            prices[code][trade_date] = (
                _number(opening), _number(closing), _number(volume), created_at
            )
            price_dates[str(market)].add(trade_date)
            price_codes_by_date[(str(market), trade_date)].add(code)

        cursor.execute(
            """SELECT TRIM(f.證券代號), m.市場別, f.資料日期,
                      MAX(f.三大法人買賣超股數), MAX(f.create_time), COUNT(*),
                      COUNT(DISTINCT f.三大法人買賣超股數),
                      COUNT(DISTINCT f.create_time)
               FROM 三大法人買賣 f
               JOIN 全證券代碼相關資料表 m
                 ON TRIM(f.證券代號)=TRIM(m.證券代號)
               WHERE f.資料日期 >= %s AND m.市場別 IN ('上市','上櫃')
               GROUP BY TRIM(f.證券代號), m.市場別, f.資料日期""",
            (READ_START_DATE,),
        )
        flows: dict[str, dict[dt.date, tuple[float, dt.datetime | None]]] = defaultdict(dict)
        duplicate_flow_groups = 0
        conflicting_flow_groups = 0
        conflicting_flow_create_times = 0
        missing_flow_create_times = 0
        flow_codes_by_date: dict[tuple[str, dt.date], set[str]] = defaultdict(set)
        for code, market, raw_date, net, created_at, row_count, net_versions, create_time_versions in cursor.fetchall():
            trade_date = _date(raw_date)
            code = _code(code)
            if not trade_date or code not in market_by_code:
                continue
            if int(row_count) > 1:
                duplicate_flow_groups += 1
            if int(net_versions) > 1:
                conflicting_flow_groups += 1
                continue
            if int(create_time_versions) > 1:
                conflicting_flow_create_times += 1
                continue
            if created_at is None:
                missing_flow_create_times += 1
                continue
            net_value = _number(net)
            if net_value is None:
                continue
            flows[code][trade_date] = (net_value, created_at)
            flow_codes_by_date[(str(market), trade_date)].add(code)

        quality = {
            "current_master_code_count": len(market_by_code),
            "current_master_missing_or_invalid_listing_date_count": invalid_listing_dates,
            "point_in_time_listing_date_applied": True,
            "price_duplicate_code_date_groups": duplicate_groups,
            "price_conflicting_code_date_groups_excluded": conflicting_price_groups,
            "price_missing_create_time_groups_excluded": missing_price_create_time_groups,
            "flow_duplicate_code_date_groups": duplicate_flow_groups,
            "flow_conflicting_code_date_groups_excluded": conflicting_flow_groups,
            "flow_conflicting_create_time_groups_excluded": conflicting_flow_create_times,
            "flow_missing_create_time_groups_excluded": missing_flow_create_times,
            "market_coverage_by_year": {},
        }
        for market in ("上市", "上櫃"):
            denominator = sum(value == market for value in market_by_code.values())
            for year in range(2023, 2027):
                day_values = []
                for period in sorted(d for d in price_dates[market] if d.year == year):
                    price_n = len(price_codes_by_date[(market, period)])
                    flow_n = len(flow_codes_by_date[(market, period)])
                    day_values.append((price_n / denominator if denominator else 0.0,
                                       flow_n / denominator if denominator else 0.0))
                if day_values:
                    quality["market_coverage_by_year"][f"{market}_{year}"] = {
                        "session_dates": len(day_values),
                        "price_coverage_median_pct": round(statistics.median(x[0] for x in day_values) * 100, 2),
                        "flow_coverage_median_pct": round(statistics.median(x[1] for x in day_values) * 100, 2),
                    }
        return market_by_code, listing_date_by_code, prices, flows, quality
    finally:
        db.conn.close()


def _summarize(
    daily_rows: dict[tuple[str, str, int, dt.date], list[tuple[float, int]]]
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, int, int], list[dict[str, float]]] = defaultdict(list)
    for (signal, market, horizon, signal_date), observations in daily_rows.items():
        excess = [item[0] for item in observations]
        signed = [item[0] * item[1] for item in observations]
        groups[(signal, market, signal_date.year, horizon)].append(
            {
                "mean_excess": statistics.mean(excess),
                "median_excess": statistics.median(excess),
                "mean_signed_excess": statistics.mean(signed),
                "hit_rate": sum(value > 0 for value in signed) / len(signed),
                "n": float(len(observations)),
            }
        )
    output = []
    for (signal, market, year, horizon), dates in sorted(groups.items()):
        output.append(
            {
                "signal": signal,
                "market": market,
                "year": year,
                "horizon_sessions_after_next_open": horizon,
                "signal_dates": len(dates),
                "mean_equal_date_excess_pp": round(statistics.mean(x["mean_excess"] for x in dates), 4),
                "median_equal_date_excess_pp": round(statistics.median(x["median_excess"] for x in dates), 4),
                "mean_equal_date_signed_excess_pp": round(statistics.mean(x["mean_signed_excess"] for x in dates), 4),
                "median_equal_date_signed_mean_excess_pp": round(statistics.median(x["mean_signed_excess"] for x in dates), 4),
                "positive_signal_date_rate": round(
                    sum(x["mean_signed_excess"] > 0 for x in dates) / len(dates), 4
                ),
                "pooled_signed_hit_rate": round(sum(x["hit_rate"] * x["n"] for x in dates) / sum(x["n"] for x in dates), 4),
                "mean_stocks_per_signal_date": round(statistics.mean(x["n"] for x in dates), 1),
            }
        )
    return output


def main() -> None:
    master, listing_dates, prices, flows, quality = _query_history()
    sessions = {
        market: sorted({period for code, market_name in master.items() if market_name == market
                        for period in prices.get(code, {})})
        for market in ("上市", "上櫃")
    }
    codes_by_market = {
        market: [code for code, market_name in master.items() if market_name == market]
        for market in ("上市", "上櫃")
    }
    momentum_excess: dict[tuple[str, dt.date, int, str], float] = {}
    for market, market_sessions in sessions.items():
        for window in (5, 20):
            for index in range(window, len(market_sessions)):
                signal_date = market_sessions[index]
                if index + 1 >= len(market_sessions):
                    continue
                prior_date = market_sessions[index - window]
                next_date = market_sessions[index + 1]
                cutoff = _entry_open_utc_naive(next_date)
                universe = _eligible_codes(
                    codes_by_market[market], listing_dates, signal_date
                )
                returns: dict[str, float] = {}
                for code in universe:
                    current = prices.get(code, {}).get(signal_date)
                    prior = prices.get(code, {}).get(prior_date)
                    current_close = current[1] if current else None
                    prior_close = prior[1] if prior else None
                    if (
                        current_close is not None and current_close > 0
                        and prior_close is not None and prior_close > 0
                        and current[3] < cutoff and prior[3] < cutoff
                    ):
                        returns[code] = current_close / prior_close - 1
                if not universe or len(returns) / len(universe) < MIN_MARKET_COVERAGE:
                    continue
                benchmark_return = statistics.mean(returns.values())
                for code, value in returns.items():
                    momentum_excess[(market, signal_date, window, code)] = (
                        value - benchmark_return
                    ) * 100
    daily_results: dict[tuple[str, str, int, dt.date], list[tuple[float, int]]] = defaultdict(list)
    eligible_market_dates: dict[tuple[str, dt.date], bool] = {}
    flow_availability_by_year: dict[tuple[str, int], list[int]] = defaultdict(lambda: [0, 0])
    flow_market_coverage_by_year: dict[tuple[str, int], list[float]] = defaultdict(list)
    price_market_coverage_by_year: dict[tuple[str, int], list[float]] = defaultdict(list)

    for market, market_sessions in sessions.items():
        for index, signal_date in enumerate(market_sessions):
            if signal_date < START_DATE or index + 1 >= len(market_sessions):
                continue
            cutoff = _entry_open_utc_naive(market_sessions[index + 1])
            universe = _eligible_codes(
                codes_by_market[market], listing_dates, signal_date
            )
            denominator = len(universe)
            if not denominator:
                continue
            available_count = 0
            price_count = 0
            for code in universe:
                price = prices.get(code, {}).get(signal_date)
                if price is not None and price[3] < cutoff:
                    price_count += 1
                row = flows.get(code, {}).get(signal_date)
                if row is None:
                    continue
                flow_availability_by_year[(market, signal_date.year)][1] += 1
                if row[1] < cutoff:
                    available_count += 1
                    flow_availability_by_year[(market, signal_date.year)][0] += 1
            flow_market_coverage_by_year[(market, signal_date.year)].append(
                available_count / denominator
            )
            price_market_coverage_by_year[(market, signal_date.year)].append(
                price_count / denominator
            )
    quality["flow_availability_before_next_open_by_market_year"] = {
        f"{market}_{year}": {
            "flow_rows": total,
            "available_before_next_open": available,
            "available_before_next_open_pct": round(available / total * 100, 2) if total else None,
        }
        for (market, year), (available, total) in sorted(flow_availability_by_year.items())
    }
    quality["market_coverage_by_year"] = {
        f"{market}_{year}": {
            "session_dates": len(flow_market_coverage_by_year[(market, year)]),
            "price_coverage_median_pct": round(
                statistics.median(price_market_coverage_by_year[(market, year)]) * 100, 2
            ),
            "flow_coverage_median_pct": round(
                statistics.median(flow_market_coverage_by_year[(market, year)]) * 100, 2
            ),
        }
        for market, year in sorted(flow_market_coverage_by_year)
    }

    for market, market_sessions in sessions.items():
        if not market_sessions or not codes_by_market[market]:
            continue
        session_index = {period: index for index, period in enumerate(market_sessions)}
        for signal_date in market_sessions:
            index = session_index[signal_date]
            if signal_date < START_DATE or index < 20 or index + 10 >= len(market_sessions):
                continue
            universe = _eligible_codes(
                codes_by_market[market], listing_dates, signal_date
            )
            denominator = len(universe)
            if not denominator:
                eligible_market_dates[(market, signal_date)] = False
                continue
            entry_date = market_sessions[index + 1]
            flow_cutoff = _entry_open_utc_naive(entry_date)
            price_coverage = sum(
                (row := prices.get(code, {}).get(signal_date)) is not None
                and row[3] < flow_cutoff
                for code in universe
            ) / denominator
            flow_coverage = sum(
                (row := flows.get(code, {}).get(signal_date)) is not None
                and row[1] < flow_cutoff
                for code in universe
            ) / denominator
            if price_coverage < MIN_MARKET_COVERAGE or flow_coverage < MIN_MARKET_COVERAGE:
                eligible_market_dates[(market, signal_date)] = False
                continue
            eligible_market_dates[(market, signal_date)] = True

            all_returns: dict[int, dict[str, float]] = {horizon: {} for horizon in HORIZONS}
            for code in universe:
                code_prices = prices.get(code, {})
                entry_date = market_sessions[index + 1]
                entry = code_prices.get(entry_date)
                entry_open = entry[0] if entry else None
                if entry_open is None or entry_open <= 0:
                    continue
                for horizon in HORIZONS:
                    exit_date = market_sessions[index + horizon]
                    exit_row = code_prices.get(exit_date)
                    exit_close = exit_row[1] if exit_row else None
                    if exit_close is not None and exit_close > 0:
                        all_returns[horizon][code] = (exit_close / entry_open - 1) * 100

            market_baseline = {
                horizon: statistics.mean(values.values()) if values else None
                for horizon, values in all_returns.items()
            }
            features: list[dict[str, Any]] = []
            prior_sessions = market_sessions[index - 20:index]
            flow_sessions = market_sessions[max(0, index - 4):index + 1]
            if len(prior_sessions) != 20 or len(flow_sessions) != 5:
                continue
            for code in universe:
                code_prices = prices.get(code, {})
                current = code_prices.get(signal_date)
                if (
                    not current or current[1] is None or current[1] <= 0
                    or current[3] >= flow_cutoff
                ):
                    continue
                raw_flows = [flows.get(code, {}).get(period) for period in flow_sessions]
                valid_flows = [
                    row[0] for row in raw_flows
                    if row is not None and row[1] < flow_cutoff
                ]
                if len(valid_flows) < 4:
                    continue
                net = sum(valid_flows)
                positive_days = sum(value > 0 for value in valid_flows)
                negative_days = sum(value < 0 for value in valid_flows)
                volumes = [code_prices.get(period, (None, None, None))[2] for period in prior_sessions]
                if any(value is None or value < 0 for value in volumes):
                    intensity = None
                else:
                    average_volume = statistics.mean(volumes)
                    intensity = net / average_volume if average_volume > 0 else None
                raw_direction = 1 if positive_days >= 4 and net >= 100_000 else (
                    -1 if negative_days >= 4 and net <= -100_000 else 0
                )
                features.append(
                    {
                        "code": code,
                        "net": net,
                        "positive_days": positive_days,
                        "negative_days": negative_days,
                        "intensity": intensity,
                        "raw_direction": raw_direction,
                    }
                )

            positive = sorted(
                (row for row in features if row["positive_days"] >= 4 and row["intensity"] is not None and row["intensity"] > 0),
                key=lambda row: row["intensity"],
            )
            negative = sorted(
                (row for row in features if row["negative_days"] >= 4 and row["intensity"] is not None and row["intensity"] < 0),
                key=lambda row: row["intensity"],
            )
            positive_cut = (
                positive[max(0, len(positive) - math.ceil(len(positive) * 0.10))]["intensity"]
                if positive else None
            )
            negative_cut = (
                negative[min(len(negative) - 1, math.ceil(len(negative) * 0.10) - 1)]["intensity"]
                if negative else None
            )
            selections: list[tuple[str, int, list[str]]] = [
                ("existing_fixed_100k_buy", 1, [row["code"] for row in features if row["raw_direction"] == 1]),
                ("existing_fixed_100k_sell", -1, [row["code"] for row in features if row["raw_direction"] == -1]),
                ("flow_intensity_buy_top_decile", 1, [row["code"] for row in positive if row["intensity"] >= positive_cut]),
                ("flow_intensity_sell_bottom_decile", -1, [row["code"] for row in negative if row["intensity"] <= negative_cut]),
            ]
            for signal_name, direction, selected_codes in selections:
                for horizon in HORIZONS:
                    baseline = market_baseline[horizon]
                    if baseline is None:
                        continue
                    for code in selected_codes:
                        stock_return = all_returns[horizon].get(code)
                        if stock_return is not None:
                            excess_return = stock_return - baseline
                            daily_results[(signal_name, market, horizon, signal_date)].append(
                                (excess_return, direction)
                            )
                            for window in (5, 20):
                                momentum = momentum_excess.get(
                                    (market, signal_date, window, code)
                                )
                                if momentum is None:
                                    continue
                                alignment = direction * momentum
                                category = (
                                    "aligned" if alignment > 0 else
                                    "countertrend" if alignment < 0 else
                                    "neutral"
                                )
                                daily_results[(
                                    f"{signal_name}_momentum{window}_{category}",
                                    market, horizon, signal_date,
                                )].append((excess_return, direction))

    summary = _summarize(daily_results)
    output = {
        "title": "Institutional flow intensity cross-sectional research",
        "data_start_date": min((date for days in sessions.values() for date in days), default=None),
        "data_end_date": max((date for days in sessions.values() for date in days), default=None),
        "signal_start_date": START_DATE,
        "current_master_only": True,
        "point_in_time_listing_dates_applied": True,
        "method": {
            "existing_rule": "4 of the latest 5 market sessions have same-sign net institutional flow and 5-session net is at least +/-100,000 shares",
            "normalized_candidate": "rank 5-session net shares / prior 20-session mean reported volume separately within market and signal date; top/bottom decile among same-sign 4-of-5 persistent flows",
            "momentum_confirmation_candidate": "split existing fixed-threshold flow signals by whether the stock's 5/20-session close-to-close return relative to its market's equal-weight return agrees with flow direction; both features are known by the next-session-open entry cutoff",
            "entry": "next market session open",
            "exit": "close of entry session for h1, and close of the 5th/10th session after signal for h5/h10",
            "excess": "stock return minus same-date equal-weight return of current-master stocks with valid open/close prices",
            "market_coverage_floor": MIN_MARKET_COVERAGE,
            "flow_availability_gate": "each flow record's UTC-naive DB create_time must be strictly before 09:00 Asia/Taipei on the next market session",
            "price_availability_gate": "signal-date and lookback prices must have UTC-naive DB create_time strictly before 09:00 Asia/Taipei on the next market session; entry/exit prices are realized outcome data",
            "universe": "current ES* common-equity master filtered by security listing date; delisted membership is not represented by this master",
            "unit_handling": "No absolute intensity cutoff is compared across markets; within-market/date ranks are invariant to a market-wide lots-vs-shares scale factor.",
        },
        "quality": quality,
        "eligible_market_date_count": sum(eligible_market_dates.values()),
        "excluded_low_coverage_market_date_count": sum(not value for value in eligible_market_dates.values()),
        "results": summary,
        "limitations": [
            "Uses current security master for historical dates; delisted securities and historical market membership are not reconstructed.",
            "Legacy price returns are unadjusted for dividends, splits, and other corporate actions; no transaction costs or slippage are included.",
            "The current-master denominator is a coverage diagnostic, not a point-in-time historical universe.",
            "Listing dates exclude securities before listing, but this current master has no delisting dates; delisted securities and historical market transfers are not reconstructed.",
            "create_time is DB ingestion time, not the official source publication time; historical source revisions are not versioned in this legacy table.",
            "A within-market percentile ranking is exploratory and does not establish a tradable threshold or causal signal.",
            "Momentum confirmation groups are descriptive subgroups with overlapping dates; they are exploratory and were not selected on an independent holdout period.",
        ],
    }
    OUT_PATH.write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=False, separators=(",", ":"), default=str))


if __name__ == "__main__":
    main()
