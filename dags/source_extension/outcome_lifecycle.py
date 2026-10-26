"""Pure calendar-horizon and cohort-report helpers for stock signal outcomes."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import random
import statistics
from bisect import bisect_left
from collections import defaultdict
from typing import Any, Iterable


FOLLOWUP_HORIZONS = ("1w", "1m", "3m", "6m", "1y")
HORIZON_BLOCK_LENGTHS = {"1w": 5, "1m": 21, "3m": 63, "6m": 126, "1y": 252}


def add_calendar_months(value: dt.date, months: int) -> dt.date:
    """Add calendar months, clamping month-end dates (Jan 31 -> Feb 28/29)."""

    month_index = value.year * 12 + (value.month - 1) + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    if month == 12:
        next_month = dt.date(year + 1, 1, 1)
    else:
        next_month = dt.date(year, month + 1, 1)
    last_day = (next_month - dt.timedelta(days=1)).day
    return dt.date(year, month, min(value.day, last_day))


def followup_target_date(signal_date: dt.date, horizon_key: str) -> dt.date:
    if horizon_key == "1w":
        return signal_date + dt.timedelta(days=7)
    months_by_horizon = {"1m": 1, "3m": 3, "6m": 6, "1y": 12}
    try:
        return add_calendar_months(signal_date, months_by_horizon[horizon_key])
    except KeyError as exc:
        raise ValueError(f"Unsupported follow-up horizon: {horizon_key}") from exc


def first_session_on_or_after(
    target_date: dt.date, sessions: Iterable[dt.date],
) -> dt.date | None:
    return next((session for session in sorted(set(sessions)) if session >= target_date), None)


def first_session_on_or_after_sorted(
    target_date: dt.date, sessions: list[dt.date] | tuple[dt.date, ...],
) -> dt.date | None:
    """Resolve a target in a pre-sorted session list without sorting per signal."""

    position = bisect_left(sessions, target_date)
    return sessions[position] if position < len(sessions) else None


def direction_sign(direction: Any) -> int:
    if not isinstance(direction, bool):
        try:
            numeric = int(direction)
            if numeric in {-1, 1} and float(direction) == numeric:
                return numeric
        except (TypeError, ValueError, OverflowError):
            pass
    value = str(direction or "")
    if value in {"偏多", "directional_up", "up", "long"}:
        return 1
    if value in {"偏空", "directional_down", "down", "short"}:
        return -1
    return 0


def percent_return(entry: Any, exit_price: Any) -> float | None:
    try:
        start, end = float(entry), float(exit_price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(start) or not math.isfinite(end) or start <= 0 or end <= 0:
        return None
    return (end / start - 1) * 100


def directional_excursion_pct(
    entry: Any, high: Any, low: Any, direction: int,
) -> tuple[float | None, float | None]:
    """Return best/worst daily-bar price excursion from the entry, direction signed."""

    try:
        start, high_price, low_price = float(entry), float(high), float(low)
    except (TypeError, ValueError):
        return None, None
    if (
        not all(math.isfinite(value) for value in (start, high_price, low_price))
        or start <= 0 or high_price <= 0 or low_price <= 0 or direction == 0
    ):
        return None, None
    signed_prices = (
        direction * (high_price / start - 1) * 100,
        direction * (low_price / start - 1) * 100,
    )
    return max(signed_prices), min(signed_prices)


def _percentile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    left = math.floor(position)
    right = math.ceil(position)
    if left == right:
        return ordered[left]
    weight = position - left
    return ordered[left] * (1 - weight) + ordered[right] * weight


def _date_block_ci(
    values_by_date: dict[str, list[float]], *, horizon_key: str, seed_text: str,
    bootstrap_samples: int = 1_000,
) -> dict[str, Any]:
    date_values = [
        sum(values) / len(values)
        for _, values in sorted(values_by_date.items()) if values
    ]
    block_length = HORIZON_BLOCK_LENGTHS[horizon_key]
    minimum_dates = max(30, 2 * block_length)
    if len(date_values) < minimum_dates:
        return {
            "method": "moving_block_bootstrap_on_signal_date_means",
            "low_pct": None,
            "high_pct": None,
            "signal_date_count": len(date_values),
            "block_length_signal_dates": block_length,
            "reason": "too_few_signal_dates_for_two_horizon_blocks",
        }

    seed = int.from_bytes(hashlib.sha256(seed_text.encode("utf-8")).digest()[:8], "big")
    rng = random.Random(seed)
    sample_size = len(date_values)
    start_count = sample_size - block_length + 1
    means: list[float] = []
    for _ in range(bootstrap_samples):
        draw: list[float] = []
        while len(draw) < sample_size:
            start = rng.randrange(start_count)
            draw.extend(date_values[start:start + block_length])
        means.append(sum(draw[:sample_size]) / sample_size)
    return {
        "method": "moving_block_bootstrap_on_signal_date_means",
        "low_pct": round(float(_percentile(means, 0.025)), 6),
        "high_pct": round(float(_percentile(means, 0.975)), 6),
        "signal_date_count": len(date_values),
        "block_length_signal_dates": block_length,
        "reason": None,
    }


def _new_bucket() -> dict[str, Any]:
    return {
        "rows": 0,
        "settled_rows": 0,
        "pending_rows": 0,
        "pending_reasons": defaultdict(int),
        "execution_settled_rows": 0,
        "execution_pending_rows": 0,
        "execution_pending_reasons": defaultdict(int),
        "benchmark_settled_rows": 0,
        "benchmark_pending_rows": 0,
        "benchmark_pending_reasons": defaultdict(int),
        "entities": set(),
        "signal_dates": set(),
        "raw_price_returns": [],
        "directional_price_returns": [],
        "directional_execution_returns": [],
        "directional_excess_returns": [],
        "raw_up_count": 0,
        "directional_price_hits": 0,
        "directional_excess_hits": 0,
        "by_date_execution": defaultdict(list),
        "favorable_excursions": [],
        "adverse_excursions": [],
    }


def _consume(bucket: dict[str, Any], row: dict[str, Any], direction: int) -> None:
    bucket["rows"] += 1
    bucket["signal_dates"].add(str(row.get("signal_date") or "unknown"))
    bucket["entities"].add(str(row.get("entity_id") or "unknown"))
    status = str(row.get("status") or "unknown")
    execution_status = str(row.get("execution_status") or "unknown")
    if execution_status == "settled":
        bucket["execution_settled_rows"] += 1
    else:
        bucket["execution_pending_rows"] += 1
        bucket["execution_pending_reasons"][
            str(row.get("execution_error_reason") or execution_status)
        ] += 1
    benchmark_status = str(row.get("benchmark_status") or "unknown")
    if benchmark_status == "settled":
        bucket["benchmark_settled_rows"] += 1
    else:
        bucket["benchmark_pending_rows"] += 1
        bucket["benchmark_pending_reasons"][
            str(row.get("benchmark_error_reason") or benchmark_status)
        ] += 1
    if status != "settled":
        bucket["pending_rows"] += 1
        bucket["pending_reasons"][str(row.get("error_reason") or status)] += 1
        return

    bucket["settled_rows"] += 1
    close_return = _to_float(row.get("stock_return_pct"))
    execution_return = _to_float(row.get("execution_return_pct"))
    excess_return = _to_float(row.get("excess_return_pp"))
    if close_return is not None:
        bucket["raw_price_returns"].append(close_return)
        bucket["raw_up_count"] += int(close_return > 0)
        if direction:
            signed = direction * close_return
            bucket["directional_price_returns"].append(signed)
            bucket["directional_price_hits"] += int(signed > 0)
    signal_date = str(row.get("signal_date") or "unknown")
    if direction and execution_return is not None:
        signed = direction * execution_return
        bucket["directional_execution_returns"].append(signed)
        bucket["by_date_execution"][signal_date].append(signed)
    if direction and excess_return is not None:
        signed_excess = direction * excess_return
        bucket["directional_excess_returns"].append(signed_excess)
        bucket["directional_excess_hits"] += int(signed_excess > 0)
    high_excursion = _to_float(row.get("max_favorable_excursion_pct"))
    low_excursion = _to_float(row.get("max_adverse_excursion_pct"))
    if high_excursion is not None and low_excursion is not None and direction:
        favorable = max(direction * high_excursion, direction * low_excursion)
        adverse = min(direction * high_excursion, direction * low_excursion)
        bucket["favorable_excursions"].append(favorable)
        bucket["adverse_excursions"].append(adverse)


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mean(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 6) if values else None


def _median(values: list[float]) -> float | None:
    return round(float(statistics.median(values)), 6) if values else None


def summarize_followup_rows(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate full-year outcomes by market, policy, delivery, and family."""

    buckets: dict[tuple[Any, ...], dict[str, Any]] = {}
    row_count = 0
    settled_count = 0
    pending_count = 0
    for row in rows:
        row_count += 1
        status = str(row.get("status") or "unknown")
        settled_count += int(status == "settled")
        pending_count += int(status != "settled")
        try:
            family_directions = row.get("family_directions_json") or "{}"
            if isinstance(family_directions, str):
                family_directions = json.loads(family_directions)
        except (TypeError, ValueError):
            family_directions = {}
        if not isinstance(family_directions, dict):
            family_directions = {}

        base = (
            str(row.get("horizon_key") or "unknown"),
            str(row.get("market") or "unknown"),
            str(row.get("policy_version") or "unknown"),
            str(row.get("source_status") or "unknown"),
            str(row.get("delivery_status") or "unknown"),
        )
        selected = bool(row.get("candidate_selected"))
        direction = direction_sign(row.get("direction"))
        scopes: list[tuple[str, str | None, int]] = []
        if selected and direction:
            scopes.append(("selected_candidate", None, direction))
        for family, raw_direction in family_directions.items():
            family_direction = direction_sign(raw_direction)
            if family_direction:
                scopes.append(("family_counterfactual", str(family), family_direction))
        for scope, family, score_direction in scopes:
            key = base + (scope, family, score_direction)
            bucket = buckets.setdefault(key, _new_bucket())
            _consume(bucket, row, score_direction)

    groups: list[dict[str, Any]] = []
    for key, bucket in sorted(buckets.items(), key=lambda item: tuple(map(str, item[0]))):
        horizon_key, market, policy, source_status, delivery, scope, family, direction = key
        close_n = len(bucket["raw_price_returns"])
        directional_n = len(bucket["directional_execution_returns"])
        date_means = [
            sum(values) / len(values)
            for _, values in sorted(bucket["by_date_execution"].items()) if values
        ]
        execution_date_count = len(bucket["by_date_execution"])
        ci = _date_block_ci(
            bucket["by_date_execution"], horizon_key=horizon_key,
            seed_text="|".join(map(str, key)),
        ) if horizon_key in HORIZON_BLOCK_LENGTHS else None
        date_count = len(bucket["signal_dates"])
        if directional_n == 0:
            evidence_grade = "no_settled_directional_execution_outcomes"
            signal_evidence_status = "no_mature_directional_execution_outcomes"
        elif ci and ci["low_pct"] is not None:
            evidence_grade = "date_block_interval_available"
            if ci["low_pct"] > 0:
                signal_evidence_status = "positive_unadjusted_date_block_interval"
            elif ci["high_pct"] < 0:
                signal_evidence_status = "negative_unadjusted_date_block_interval"
            else:
                signal_evidence_status = "interval_crosses_zero"
        elif execution_date_count < 30:
            evidence_grade = "insufficient_signal_dates"
            signal_evidence_status = "insufficient_signal_dates"
        else:
            evidence_grade = "descriptive_only_overlapping_horizons"
            signal_evidence_status = "descriptive_interval_unavailable"
        groups.append({
            "horizon_key": horizon_key,
            "market": market,
            "policy_version": policy,
            "source_status": source_status,
            "delivery_status": delivery,
            "scope": scope,
            "family": family,
            "direction": "偏多" if direction > 0 else "偏空",
            "observation_rows": bucket["rows"],
            "settled_rows": bucket["settled_rows"],
            "pending_rows": bucket["pending_rows"],
            "pending_reasons": dict(sorted(bucket["pending_reasons"].items())),
            "execution_settled_rows": bucket["execution_settled_rows"],
            "execution_pending_rows": bucket["execution_pending_rows"],
            "execution_pending_reasons": dict(sorted(
                bucket["execution_pending_reasons"].items()
            )),
            "benchmark_settled_rows": bucket["benchmark_settled_rows"],
            "benchmark_pending_rows": bucket["benchmark_pending_rows"],
            "benchmark_pending_reasons": dict(sorted(
                bucket["benchmark_pending_reasons"].items()
            )),
            "signal_date_count": date_count,
            "execution_signal_date_count": execution_date_count,
            "security_count": len(bucket["entities"]),
            "raw_price_up_rate": (
                round(bucket["raw_up_count"] / close_n, 6) if close_n else None
            ),
            "directional_price_hit_rate": (
                round(bucket["directional_price_hits"] / len(bucket["directional_price_returns"]), 6)
                if bucket["directional_price_returns"] else None
            ),
            "mean_directional_event_price_pct": _mean(bucket["directional_price_returns"]),
            "median_directional_event_price_pct": _median(bucket["directional_price_returns"]),
            "mean_directional_execution_proxy_pct": _mean(bucket["directional_execution_returns"]),
            "date_equal_mean_directional_execution_proxy_pct": _mean(date_means),
            "execution_proxy_ci_95": ci,
            "mean_directional_market_excess_pp": _mean(bucket["directional_excess_returns"]),
            "directional_market_relative_hit_rate": (
                round(bucket["directional_excess_hits"] / len(bucket["directional_excess_returns"]), 6)
                if bucket["directional_excess_returns"] else None
            ),
            "mean_max_favorable_excursion_pct": _mean(bucket["favorable_excursions"]),
            "mean_max_adverse_excursion_pct": _mean(bucket["adverse_excursions"]),
            "break_even_round_trip_friction_bps": (
                round(_mean(bucket["directional_execution_returns"]) * 100, 2)
                if bucket["directional_execution_returns"] else None
            ),
            "evidence_grade": evidence_grade,
            "signal_evidence_status": signal_evidence_status,
        })
    return {
        "status": "ready" if row_count else "no_followup_rows",
        "outcome_row_count": row_count,
        "settled_row_count": settled_count,
        "pending_row_count": pending_count,
        "groups": groups,
        "interpretation": {
            "raw_price_return": "signal-close to maturity-close price change, not a tradable result",
            "execution_proxy": "next-session open to maturity-close price change, before fees, tax, slippage, borrow and corporate actions",
            "benchmark": "official market price index, close-to-close; compared with unadjusted stock price return on the same price-only basis",
            "confidence_interval": "moving-block bootstrap on signal-date means; omitted if fewer than two horizon-length blocks",
            "annual_review": "descriptive until independent signal-date blocks and complete source coverage are available",
        },
    }
