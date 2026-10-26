"""Read-only, date-clustered diagnostics for daily market-signal outcomes."""

from __future__ import annotations

import datetime as dt
import json
import math
import statistics
from collections import defaultdict
from typing import Any

from pymysql.cursors import SSCursor


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _direction(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    return int(value) if value in (-1, 1) else 0


def _mean(values: list[float]) -> float | None:
    return round(statistics.mean(values), 6) if values else None


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def _new_group() -> dict[str, Any]:
    return {
        "rows": 0,
        "selected_candidates": 0,
        "settled": 0,
        "pending_data": 0,
        "signal_dates": set(),
        "raw_count": 0,
        "raw_sum": 0.0,
        "raw_positive": 0,
        "directional_stock_count": 0,
        "directional_stock_sum": 0.0,
        "directional_stock_positive": 0,
        "directional_excess_count": 0,
        "directional_excess_sum": 0.0,
        "neutral_excess_count": 0,
        "neutral_excess_sum": 0.0,
        "relative_hits": 0,
        "relative_scored": 0,
        "by_date_stock": defaultdict(lambda: [0.0, 0]),
        "by_date_excess": defaultdict(lambda: [0.0, 0]),
        "pending_reasons": defaultdict(int),
    }


def _add_row(
    group: dict[str, Any], row: dict[str, Any], *, direction: int,
) -> None:
    group["rows"] += 1
    group["signal_dates"].add(row["signal_date_key"])
    if row.get("candidate_selected") is True:
        group["selected_candidates"] += 1

    status = str(row.get("status") or "unknown")
    if status == "pending_data":
        group["pending_data"] += 1
        group["pending_reasons"][str(row.get("error_reason") or "unspecified")] += 1
        return
    if status != "settled":
        return

    group["settled"] += 1
    stock_return = _float(row.get("stock_return_pct"))
    excess_return = _float(row.get("excess_return_pp"))
    if stock_return is not None:
        group["raw_count"] += 1
        group["raw_sum"] += stock_return
        group["raw_positive"] += int(stock_return > 0)
        if direction:
            signed_stock_return = direction * stock_return
            group["directional_stock_count"] += 1
            group["directional_stock_sum"] += signed_stock_return
            group["directional_stock_positive"] += int(signed_stock_return > 0)
            date_values = group["by_date_stock"][row["signal_date"]]
            date_values[0] += signed_stock_return
            date_values[1] += 1
    if excess_return is not None and direction:
        signed_excess_return = direction * excess_return
        group["directional_excess_count"] += 1
        group["directional_excess_sum"] += signed_excess_return
        date_values = group["by_date_excess"][row["signal_date"]]
        date_values[0] += signed_excess_return
        date_values[1] += 1

    if direction and excess_return is not None:
        group["relative_scored"] += 1
        group["relative_hits"] += int(direction * excess_return > 0)


def _finish_group(key: dict[str, Any], group: dict[str, Any]) -> dict[str, Any]:
    stock_count = group["directional_stock_count"]
    excess_count = group["directional_excess_count"]
    date_stock = [total / count for total, count in group["by_date_stock"].values()]
    date_excess = [total / count for total, count in group["by_date_excess"].values()]
    result = {
        **key,
        "outcome_rows": group["rows"],
        "selected_candidate_rows": group["selected_candidates"],
        "settled_rows": group["settled"],
        "pending_data_rows": group["pending_data"],
        "pending_reasons": dict(sorted(group["pending_reasons"].items())),
        "signal_date_count": len(group["signal_dates"]),
        "raw_price_up_rate": _rate(group["raw_positive"], group["raw_count"]),
        "directional_price_hit_rate": _rate(
            group["directional_stock_positive"], stock_count,
        ),
        "mean_directional_stock_return_pct": (
            round(group["directional_stock_sum"] / stock_count, 6)
            if stock_count else None
        ),
        "date_equal_mean_directional_stock_return_pct": _mean(date_stock),
        "positive_signal_date_rate": _rate(
            sum(value > 0 for value in date_stock), len(date_stock),
        ),
        "mean_directional_excess_pp": (
            round(group["directional_excess_sum"] / excess_count, 6)
            if excess_count else None
        ),
        "date_equal_mean_directional_excess_pp": _mean(date_excess),
        "market_relative_hit_rate": _rate(
            group["relative_hits"], group["relative_scored"],
        ),
        "market_relative_scored_rows": group["relative_scored"],
        "break_even_round_trip_friction_bps": (
            round(group["directional_stock_sum"] / stock_count * 100, 2)
            if stock_count else None
        ),
    }
    if str(key.get("family") or "").endswith(":neutral"):
        date_neutral_excess = date_excess
        result.update(
            family_state="neutral",
            mean_raw_stock_return_pct=(
                round(group["raw_sum"] / group["raw_count"], 6)
                if group["raw_count"] else None
            ),
            mean_neutral_excess_pp=(
                round(group["neutral_excess_sum"] / group["neutral_excess_count"], 6)
                if group["neutral_excess_count"] else None
            ),
            date_equal_mean_neutral_excess_pp=_mean(date_neutral_excess),
            directional_price_hit_rate=None,
            mean_directional_stock_return_pct=None,
            date_equal_mean_directional_stock_return_pct=None,
            positive_signal_date_rate=None,
            mean_directional_excess_pp=None,
            date_equal_mean_directional_excess_pp=None,
            market_relative_hit_rate=None,
            break_even_round_trip_friction_bps=None,
        )
        result["family"] = str(key["family"]).removesuffix(":neutral")
    elif key.get("family"):
        result["family_state"] = "directional"
    return result


def _iter_dict_rows(cursor, query: str, params: tuple[Any, ...], *, batch_size: int = 2_000):
    """Stream query rows so the 120-day evidence ledger stays memory-bounded."""

    connection = getattr(cursor, "connection", None)
    query_cursor = connection.cursor(SSCursor) if connection is not None else cursor
    owned_cursor = query_cursor is not cursor
    try:
        query_cursor.execute(query, params)
        columns = [item[0] for item in query_cursor.description]
        while True:
            batch = query_cursor.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                yield dict(zip(columns, row))
    finally:
        if owned_cursor:
            query_cursor.close()


def summarize_daily_backtest(
    cursor, *, as_of_date: dt.date, lookback_days: int = 120,
) -> dict[str, Any]:
    """Summarize mature daily outcomes and threshold/family counterfactuals.

    Counterfactual threshold and family metrics use only runs that contain a
    complete evidence snapshot for every stock evaluated by the signal scan.
    Historical selected candidates remain available as a separate observed-
    policy series, but cannot stand in for the missing non-candidate universe.
    """

    start_date = as_of_date - dt.timedelta(days=lookback_days)
    coverage_query = """
        SELECT c.run_key,
               MAX(CAST(JSON_UNQUOTE(JSON_EXTRACT(
                   c.details_text, '$.evaluation_universe_count'
               )) AS UNSIGNED)) AS expected_count,
               COUNT(DISTINCT c.entity_id) AS actual_count
        FROM market_signal_candidates c
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.entity_type='stock_evidence'
          AND c.as_of_date>=%s AND c.as_of_date<=%s
        GROUP BY c.run_key
        """
    complete_run_keys: set[str] = set()
    for coverage in _iter_dict_rows(
        cursor,
        coverage_query,
        (start_date, as_of_date),
    ):
        expected = int(coverage.get("expected_count") or 0)
        if expected > 0 and int(coverage.get("actual_count") or 0) == expected:
            complete_run_keys.add(str(coverage.get("run_key") or ""))

    signal_dates_query = """
        SELECT DISTINCT o.signal_date
        FROM market_signal_outcomes o
        INNER JOIN market_signal_candidates c ON c.id=o.candidate_id
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.entity_type IN ('stock','stock_evidence')
          AND (
              c.entity_type='stock_evidence'
              OR NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key AND e.entity_type='stock_evidence'
              )
          )
          AND o.signal_date>=%s AND o.signal_date<=%s
        ORDER BY o.signal_date
        """
    signal_dates = [
        row["signal_date"] for row in _iter_dict_rows(
            cursor, signal_dates_query, (start_date, as_of_date),
        )
    ]
    candidates_query = """
        SELECT c.id AS candidate_id, c.entity_type, c.details_text,
               COALESCE((
                   SELECT n.status
                   FROM market_signal_notification_candidates ni
                   INNER JOIN market_signal_notifications n
                       ON n.id=ni.notification_id
                   WHERE ni.candidate_id=c.id AND n.run_key=c.run_key
                   ORDER BY n.updated_at DESC, n.id DESC
                   LIMIT 1
               ), 'unlinked_or_not_listed') AS notification_status
        FROM market_signal_candidates c
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.entity_type IN ('stock','stock_evidence')
          AND c.as_of_date=%s
          AND (
              c.entity_type='stock_evidence'
              OR NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key AND e.entity_type='stock_evidence'
              )
          )
        ORDER BY c.id
        """
    outcomes_query = """
        SELECT o.id, o.candidate_id, c.run_key, o.entity_id, o.market, o.source_status,
               o.outcome_price_source_key, o.signal_date, o.direction,
               o.outcome_basis, o.horizon_sessions, o.stock_return_pct,
               o.excess_return_pp, o.directional_hit, o.status, o.error_reason,
               c.entity_type, r.started_at, r.source_dag_id, r.source_dag_run_id
        FROM market_signal_outcomes o
        INNER JOIN market_signal_candidates c ON c.id=o.candidate_id
        INNER JOIN market_signal_runs r ON r.run_key=c.run_key
        WHERE r.scan_mode='daily' AND r.status='ready'
          AND c.entity_type IN ('stock','stock_evidence')
          AND (
              c.entity_type='stock_evidence'
              OR NOT EXISTS (
                  SELECT 1 FROM market_signal_candidates e
                  WHERE e.run_key=c.run_key AND e.entity_type='stock_evidence'
              )
          )
          AND o.signal_date=%s
        ORDER BY r.started_at DESC, o.id DESC
        """

    threshold_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    observed_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    notification_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    family_groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    family_readiness: dict[tuple[Any, ...], dict[str, Any]] = {}
    pending_total = 0
    settled_total = 0
    deduplicated_rows = 0
    complete_dates: set[str] = set()
    seen_identities: set[tuple[Any, ...]] = set()
    # Fetch compact feature metadata once per signal date; then stream that
    # date's outcome rows, newest run first, and discard both maps before the
    # next date. This avoids transferring the same JSON feature snapshot for
    # each of its six entry-basis/horizon outcome rows.
    for signal_date in signal_dates:
        if isinstance(signal_date, dt.datetime):
            signal_date = signal_date.date()
        signal_date_key = (
            signal_date.isoformat() if isinstance(signal_date, dt.date)
            else str(signal_date)
        )
        metadata_by_candidate: dict[int, dict[str, Any]] = {}
        for candidate_row in _iter_dict_rows(
            cursor, candidates_query, (signal_date,),
        ):
            candidate_id = int(candidate_row.get("candidate_id") or 0)
            try:
                details = json.loads(candidate_row.get("details_text") or "{}")
            except (TypeError, ValueError):
                details = {}
            if not isinstance(details, dict):
                details = {}
            raw_directions = details.get("family_directions")
            if not isinstance(raw_directions, dict):
                raw_directions = {}
            family_directions = {
                str(name): direction
                for name, value in raw_directions.items()
                if (direction := _direction(value))
            }
            family_status = details.get("family_status")
            if not isinstance(family_status, dict):
                family_status = {}
            metadata = {
                "policy_version": str(details.get("policy_version") or "legacy_unknown"),
                "family_directions": family_directions,
                "family_status": family_status,
                "notification_status": str(
                    candidate_row.get("notification_status")
                    or "unlinked_or_not_listed"
                ),
                "candidate_selected": (
                    candidate_row.get("entity_type") == "stock"
                    or details.get("candidate_selected") is True
                ),
            }
            metadata_by_candidate[candidate_id] = metadata

        seen_identities.clear()
        for row in _iter_dict_rows(cursor, outcomes_query, (signal_date,)):
            candidate_id = int(row.get("candidate_id") or 0)
            metadata = metadata_by_candidate.get(candidate_id)
            if metadata is None:
                continue
            execution_origin = (
                "natural_parent_run"
                if row.get("source_dag_id") and row.get("source_dag_run_id")
                else "manual_or_unattributed"
            )
            identity = (
                signal_date_key, str(row.get("entity_id") or ""),
                metadata["policy_version"], execution_origin,
                str(row.get("market") or ""),
                str(row.get("outcome_price_source_key") or ""),
                str(row.get("outcome_basis") or ""),
                int(row.get("horizon_sessions") or 0),
            )
            if identity in seen_identities:
                continue
            seen_identities.add(identity)
            deduplicated_rows += 1

            run_key = str(row.get("run_key") or "")
            complete_evidence = run_key in complete_run_keys
            if complete_evidence:
                complete_dates.add(signal_date_key)
            row.update(
                family_directions=metadata["family_directions"],
                family_status=metadata["family_status"],
                policy_version=metadata["policy_version"],
                execution_origin=execution_origin,
                signal_date_key=signal_date_key,
                direction_sign=(1 if row.get("direction") == "偏多" else
                                -1 if row.get("direction") == "偏空" else 0),
                candidate_selected=metadata["candidate_selected"],
            )
            if row.get("status") == "settled":
                settled_total += 1
            elif row.get("status") == "pending_data":
                pending_total += 1

            common = (
                row["policy_version"], row["execution_origin"],
                str(row.get("market") or "unknown"),
                str(row.get("source_status") or "unknown"),
                str(row.get("outcome_price_source_key") or "unknown"),
                str(row.get("outcome_basis") or "unknown"),
                int(row.get("horizon_sessions") or 0),
                signal_date_key,
            )
            direction_sign = row["direction_sign"]
            family_count = len(row["family_directions"])

            if row["candidate_selected"]:
                if direction_sign:
                    key = common[:7]
                    group = observed_groups.setdefault(key, _new_group())
                    _add_row(group, row, direction=direction_sign)
                delivery_key = common[:7] + (metadata["notification_status"],)
                delivery_group = notification_groups.setdefault(
                    delivery_key, _new_group(),
                )
                _add_row(delivery_group, row, direction=direction_sign)

            if complete_evidence:
                if row.get("outcome_basis") == "next_close" and int(row.get("horizon_sessions") or 0) == 1:
                    for family, state in row["family_status"].items():
                        key = common[:4] + (str(family), str(state))
                        group = family_readiness.setdefault(key, {
                            "stock_observations": 0, "signal_dates": set(),
                        })
                        group["stock_observations"] += 1
                        group["signal_dates"].add(signal_date_key)

                for threshold in (1, 2, 3):
                    if family_count < threshold or not direction_sign:
                        continue
                    key = common[:7] + (threshold,)
                    group = threshold_groups.setdefault(key, _new_group())
                    _add_row(group, row, direction=direction_sign)

                for family, family_direction in row["family_directions"].items():
                    key = common[:7] + (family,)
                    group = family_groups.setdefault(key, _new_group())
                    _add_row(group, row, direction=family_direction)

                for family, state in row["family_status"].items():
                    if state != "neutral":
                        continue
                    stock_return = _float(row.get("stock_return_pct"))
                    excess_return = _float(row.get("excess_return_pp"))
                    # Neutral observations are retained as an unscored control;
                    # their returns are not direction-signed or mixed with triggers.
                    neutral_key = common[:7] + (f"{family}:neutral",)
                    neutral = family_groups.setdefault(neutral_key, _new_group())
                    neutral["rows"] += 1
                    neutral["signal_dates"].add(row["signal_date_key"])
                    neutral["settled"] += int(row.get("status") == "settled")
                    neutral["pending_data"] += int(row.get("status") == "pending_data")
                    if row.get("candidate_selected") is True:
                        neutral["selected_candidates"] += 1
                    if stock_return is not None:
                        neutral["raw_count"] += 1
                        neutral["raw_sum"] += stock_return
                        neutral["raw_positive"] += int(stock_return > 0)
                        if excess_return is not None:
                            neutral["neutral_excess_count"] += 1
                            neutral["neutral_excess_sum"] += excess_return
                            date_values = neutral["by_date_excess"][
                                row["signal_date_key"]
                            ]
                            date_values[0] += excess_return
                            date_values[1] += 1
                    if row.get("status") == "pending_data":
                        neutral["pending_reasons"][str(row.get("error_reason") or "unspecified")] += 1

    def export(groups: dict[tuple[Any, ...], dict[str, Any]], labels: tuple[str, ...]):
        results = []
        for key, values in sorted(groups.items(), key=lambda item: tuple(str(v) for v in item[0])):
            results.append(_finish_group(dict(zip(labels, key)), values))
        return results

    common_labels = (
        "policy_version", "execution_origin", "market", "source_status",
        "price_source", "outcome_basis", "horizon_sessions",
    )
    readiness_rows = []
    for key, values in sorted(
        family_readiness.items(), key=lambda item: tuple(str(value) for value in item[0]),
    ):
        readiness_rows.append({
            "policy_version": key[0],
            "execution_origin": key[1],
            "market": key[2],
            "source_status": key[3],
            "family": key[4],
            "family_status": key[5],
            "stock_observations": values["stock_observations"],
            "signal_date_count": len(values["signal_dates"]),
        })
    return {
        "status": "ready" if deduplicated_rows else "no_outcome_rows",
        "as_of_date": as_of_date.isoformat(),
        "lookback_days": lookback_days,
        "deduplicated_outcome_rows": deduplicated_rows,
        "settled_rows": settled_total,
        "pending_data_rows": pending_total,
        "complete_evidence_signal_dates": len(complete_dates),
        "family_readiness": readiness_rows,
        "observed_policy": export(observed_groups, common_labels),
        "notification_delivery": export(
            notification_groups, common_labels + ("notification_status",),
        ),
        "threshold_counterfactuals": export(
            threshold_groups, common_labels + ("minimum_directional_families",),
        ),
        "family_counterfactuals": export(
            family_groups, common_labels + ("family",),
        ),
        "interpretation": {
            "primary_basis": "next_close; after-close signal entry proxy at the next session close",
            "signal_close": "descriptive only; it assumes entry at the signal-day close",
            "directional_price_hit_rate": "share where signal direction times raw stock return is positive",
            "market_relative_hit_rate": "existing directional_hit against the market benchmark; reported separately",
            "date_weighting": "date_equal metrics average each independent signal date equally",
            "break_even_round_trip_friction_bps": "gross directional stock return in basis points; not a net return or a personal fee estimate",
            "costs_and_corporate_actions": "not adjusted; short borrow cost and executable intraday fills are unavailable",
            "event_alerts": "intraday event and source-quality alerts are excluded from price-prediction metrics",
            "thresholds": "counterfactual thresholds use only dates with a complete persisted evaluation-universe snapshot",
            "promotion_gate": "do not change policy from descriptive rows; require an untouched date holdout, clustered confidence interval, complete source coverage and positive after-cost returns",
        },
    }
