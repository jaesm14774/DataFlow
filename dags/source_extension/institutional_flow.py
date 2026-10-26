"""PIT diagnostics of official institutional data and readiness rendering.

This module reads stored observations but does not assign institutional votes.
A complete five-session panel is a data qualification, not evidence of predictive value.
"""

from __future__ import annotations

import datetime as dt
import json
from collections import Counter
from typing import Any

from source_extension.adapters import parse_number


TPEX_FLOW_SOURCE = "tpex_institutional_flow_daily"
FLOW_SOURCE_BY_MARKET = {"上市": None, "上櫃": TPEX_FLOW_SOURCE}

# Rank before filtering quality so a later rejected revision cannot revive an
# older accepted value. Current-row fallback can establish availability only.
FLOW_ASOF_SQL = """
WITH flow_snapshot AS (
    SELECT v.record_hash, v.entity_id, v.effective_date, v.quality_status,
           v.unit, v.value_text, v.captured_at, v.capture_basis,
           v.dag_id, v.dag_run_id,
           EXISTS (
               SELECT 1 FROM source_runs sr
               WHERE sr.source_key=v.source_key AND sr.dag_id=v.dag_id
                 AND sr.dag_run_id=v.dag_run_id AND sr.status='success'
                 AND sr.finished_at<=%s
           ) AS successful_source_run
    FROM source_record_versions v
    WHERE v.source_key=%s AND v.captured_at<=%s
      AND NOT EXISTS (
          SELECT 1 FROM source_record_versions newer
          WHERE newer.record_hash=v.record_hash AND newer.captured_at<=%s
            AND (newer.captured_at>v.captured_at
                 OR (newer.captured_at=v.captured_at AND newer.id>v.id))
      )
    UNION ALL
    SELECT r.record_hash, r.entity_id, r.effective_date, r.quality_status,
           r.unit, r.value_text, r.updated_at, 'current_row_only',
           NULL, NULL, 0
    FROM source_records r
    WHERE r.source_key=%s AND r.updated_at<=%s
      AND NOT EXISTS (
          SELECT 1 FROM source_record_versions v
          WHERE v.record_hash=r.record_hash AND v.captured_at<=%s
      )
)
SELECT record_hash, entity_id, effective_date, quality_status, unit, value_text,
       captured_at, capture_basis, dag_id, dag_run_id, successful_source_run
FROM flow_snapshot
WHERE effective_date BETWEEN %s AND %s
ORDER BY effective_date, entity_id, record_hash
"""


def _date(value: Any) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _flow_values(row: dict[str, Any]) -> tuple[float, ...] | None:
    if row.get("quality_status") != "ok" or row.get("unit") != "shares":
        return None
    try:
        detail = row["value_text"]
        detail = json.loads(detail) if isinstance(detail, str) else detail
    except (KeyError, TypeError, ValueError):
        return None
    if not isinstance(detail, dict) or detail.get("market") != "上櫃":
        return None
    values = tuple(parse_number(detail.get(key)) for key in (
        "three_institution_total_net_shares",
        "foreign_net_shares_excluding_foreign_dealers",
        "investment_trust_net_shares", "dealers_net_shares",
    ))
    if not all(v is not None and v.is_integer() for v in values):
        return None
    # Foreign dealers are already included in dealers; adding them again is
    # double counting. Signed shares and explicit zeroes are valid facts.
    if values[0] != sum(values[1:]):
        return None
    return values


def assess_flow_readiness(
    rows: list[dict[str, Any]], *, master: dict[str, dict[str, Any]],
    as_of_date: dt.date, cutoff_utc: dt.datetime,
    expected_session_dates: list[dt.date],
    price_rows_by_code: dict[str, list[tuple]],
    calendar_verified: bool,
) -> dict[str, dict[str, Any]]:
    """Assess security/date panels; never impute missing rows or assign votes."""
    periods = sorted(set(expected_session_dates))[-5:]
    calendar_complete = (
        calendar_verified and len(periods) == 5 and periods[-1] == as_of_date
    )
    grouped: dict[tuple[str, dt.date], list[dict[str, Any]]] = {}
    for row in rows:
        period = _date(row.get("effective_date"))
        captured = row.get("captured_at")
        # SQL applies this boundary as well; keep the pure assessor safe when
        # called on exported snapshots or controlled fixtures.
        if (
            period is None or period > as_of_date
            or not isinstance(captured, dt.datetime) or captured > cutoff_utc
        ):
            continue
        code = str(row.get("entity_id") or "").strip()
        if code:
            grouped.setdefault((code, period), []).append(row)

    valid: dict[tuple[str, dt.date], tuple[tuple[float, ...], bool]] = {}
    rejected = Counter()
    for key, observations in grouped.items():
        if len(observations) != 1:
            rejected["duplicate_security_date"] += 1
            continue
        row = observations[0]
        values = _flow_values(row)
        if values is None:
            rejected["invalid_values_quality_or_unit"] += 1
            continue
        natural_pit = bool(
            row.get("capture_basis") == "source_feed"
            and row.get("dag_id") == "source_extension_collect_after_close"
            and str(row.get("dag_run_id") or "").startswith("scheduled__")
            and row.get("successful_source_run")
        )
        valid[key] = (values, natural_pit)

    latest_date = max((period for _, period in grouped), default=None)
    latest_codes = {code for code, period in valid if period == latest_date}
    result = {}
    for market, source_key in FLOW_SOURCE_BY_MARKET.items():
        universe = {code for code, info in master.items() if info["market"] == market}
        if source_key is None:
            result[market] = {
                "source_key": None, "status": "stored_source_not_assessed",
                "source_available_count": None, "same_day_count": None,
                "natural_pit_same_day_count": None, "five_session_ready_count": None,
                "traded_without_source_count": None, "vote_enabled": None,
                "blocking_reasons": ["this_audit_only_reads_official_tpex_feed"],
            }
            continue
        same_day = {code for code, period in valid if period == as_of_date} & universe
        pit_same_day = {
            code for code in same_day if valid[(code, as_of_date)][1]
        }
        traded = {
            code for code in universe
            if any(period == as_of_date and close is not None and close > 0
                   and volume is not None and volume > 0
                   for period, close, volume, *_ in price_rows_by_code.get(code, []))
        }
        eligible = {}
        for code in pit_same_day:
            if not calendar_complete or not all(
                (code, period) in valid and valid[(code, period)][1] for period in periods
            ):
                continue
            price_by_period = {}
            for period, close, volume, *_ in price_rows_by_code.get(code, []):
                price_by_period.setdefault(period, []).append((close, volume))
            if not all(
                len(price_by_period.get(period, [])) == 1
                and price_by_period[period][0][0] is not None
                and price_by_period[period][0][0] > 0
                and price_by_period[period][0][1] is not None
                and price_by_period[period][0][1] > 0
                for period in periods
            ):
                continue
            panel = [valid[(code, period)][0] for period in periods]
            eligible[code] = {
                "total_net_shares_5d": sum(values[0] for values in panel),
                "positive_days": sum(values[0] > 0 for values in panel),
                "negative_days": sum(values[0] < 0 for values in panel),
                "foreign_net_shares_5d": sum(values[1] for values in panel),
                "trust_net_shares_5d": sum(values[2] for values in panel),
                "dealer_net_shares_5d": sum(values[3] for values in panel),
            }
        reasons = []
        if not calendar_complete:
            reasons.append("five_market_session_calendar_unavailable")
        if not same_day:
            reasons.append("same_day_flow_unavailable")
        if len(pit_same_day) < len(same_day):
            reasons.append("natural_pit_lineage_incomplete")
        if len(eligible) < len(universe):
            reasons.append("five_session_flow_price_panel_incomplete")
        result[market] = {
            "source_key": source_key,
            "status": "official_feed_observed" if eligible else "insufficient_history",
            "latest_effective_date": latest_date.isoformat() if latest_date else None,
            "source_available_count": len(latest_codes & universe),
            "source_latest_row_count": sum(period == latest_date for _, period in grouped),
            "source_unmatched_count": len(latest_codes - universe),
            "same_day_count": len(same_day),
            "natural_pit_same_day_count": len(pit_same_day),
            "traded_without_source_count": len(traded - same_day),
            "observed_effective_dates": sorted({period.isoformat() for _, period in valid}),
            "expected_session_dates": [period.isoformat() for period in periods],
            "calendar_verified": calendar_complete,
            "five_session_ready_count": len(eligible),
            "shadow_observations_5d": eligible,
            "vote_enabled": None, "blocking_reasons": reasons,
            "direction_evaluation": "handled_by_equity_order_flow",
            "rejected_key_counts": dict(rejected),
            "as_of_date": as_of_date.isoformat(),
            "cutoff_utc": cutoff_utc.isoformat(),
        }
    return result


def load_flow_readiness(
    cursor, *, master: dict[str, dict[str, Any]], as_of_date: dt.date,
    cutoff_utc: dt.datetime, price_rows_by_code: dict[str, list[tuple]],
) -> dict[str, dict[str, Any]]:
    """Use stored market calendar and successful natural PIT versions only."""
    floor = as_of_date - dt.timedelta(days=45)
    cursor.execute(FLOW_ASOF_SQL, (
        cutoff_utc, TPEX_FLOW_SOURCE, cutoff_utc, cutoff_utc,
        TPEX_FLOW_SOURCE, cutoff_utc, cutoff_utc, floor, as_of_date,
    ))
    names = ("record_hash", "entity_id", "effective_date", "quality_status", "unit",
             "value_text", "captured_at", "capture_basis", "dag_id", "dag_run_id",
             "successful_source_run")
    rows = [dict(zip(names, row)) for row in cursor.fetchall()]
    # Calendar access failure must not silently shorten the five-session window.
    calendar_verified = True
    try:
        cursor.execute("SELECT now_date FROM holiday.taiwan WHERE now_date BETWEEN %s AND %s",
                       (floor, as_of_date))
        closed = {_date(row[0]) for row in cursor.fetchall()}
        dates = [as_of_date - dt.timedelta(days=i) for i in range(45)]
        # The shared holiday table includes weekends. A missing or truncated
        # calendar must not certify five weekdays as actual market sessions.
        calendar_verified = all(day in closed for day in dates if day.weekday() >= 5)
        sessions = (
            sorted(day for day in dates if day.weekday() < 5 and day not in closed)[-5:]
            if calendar_verified else []
        )
    except Exception:
        sessions = []
        calendar_verified = False
    return assess_flow_readiness(
        rows, master=master, as_of_date=as_of_date, cutoff_utc=cutoff_utc,
        expected_session_dates=sessions, price_rows_by_code=price_rows_by_code,
        calendar_verified=calendar_verified,
    )


def format_flow_readiness(market: str, readiness: dict[str, Any], universe_count: int) -> str:
    if readiness.get("rule_version") == "equity_order_flow_v2":
        available = int(readiness.get("same_day_count") or 0)
        usable = int(readiness.get("direction_usable_count") or 0)
        triggered = int(readiness.get("qualified_vote_count") or 0)
        broker = readiness.get("broker") or {}
        broker_text = (
            "上櫃券商分點買賣資料未收集，只略過上櫃分點訊號；法人照常判讀。"
            if market == "上櫃" else
            f"上市券商分點：最新資料日 {broker.get('latest_effective_date') or '未取得'}；"
            f"同日已收 {int(broker.get('same_day_count') or 0):,} 檔、"
            f"可判讀 {int(broker.get('direction_usable_count') or 0):,} 檔、"
            f"達集中度條件 {int(broker.get('qualified_vote_count') or 0):,} 檔。"
        )
        return (
            f"{market}法人{'已納入判讀' if available else '本次缺同日列'}："
            f"資料日 {readiness.get('latest_effective_date') or '未取得'}；"
            f"同日已收 {available:,}/{universe_count:,} 檔、可判讀 {usable:,} 檔、"
            f"達多空條件 {triggered:,} 檔。完整三／五日 "
            f"{int(readiness.get('three_session_ready_count') or 0):,}/"
            f"{int(readiness.get('five_session_ready_count') or 0):,} 檔。"
            f"有成交但缺同日法人列 {int(readiness.get('traded_without_source_count') or 0):,} 檔。\n"
            f"版本留存 {int(readiness.get('natural_pit_same_day_count') or 0):,} 檔；"
            f"僅有入庫時間 {int(readiness.get('stored_collection_time_count') or 0):,} 檔。"
            "規則已執行，預測成效仍待跨日期驗證。\n" + broker_text
        )
    if readiness.get("rule_version") == "equity_order_flow_v1":
        broker = readiness.get("broker") or {}
        broker_text = (
            "上櫃分點：無可用來源，略過。" if market == "上櫃" else
            f"上市分點：已收 {broker.get('same_day_count', 0):,} 檔、面板核對 "
            f"{broker.get('accepted_panel_count', 0):,} 檔；量能／精度核對後可判讀 "
            f"{broker.get('direction_usable_count', 0):,} 檔、達方向門檻 {broker.get('qualified_vote_count', 0):,} 檔。"
        )
        if readiness.get("status") == "source_not_authorized":
            return (
                f"{market}法人：這份歷史 v1 快照當時未讀入既有法人表；請勿作為目前來源現況。\n"
                f"{broker_text}"
            )
        return (
            f"{market}法人：官方批次同日列 {readiness.get('same_day_count', 0):,}/{universe_count:,} 檔，"
            f"自然 PIT {readiness.get('natural_pit_same_day_count', 0):,} 檔；完整三／五日面板 "
            f"{readiness.get('three_session_ready_count', 0):,}/{readiness.get('five_session_ready_count', 0):,} 檔。"
            f"影子方向條件符合 {readiness.get('shadow_direction_candidate_count', 0):,} 檔；"
            "此為歷史 v1 停用政策的紀錄；目前政策請看新快照。\n"
            f"\n{broker_text}"
        )
    if readiness.get("status") in {"source_not_authorized", "stored_source_not_assessed"}:
        return f"{market}法人：此來源版本稽核未讀取既有股票表；已收與判讀數請看整合法人／分點快照。"
    if not readiness:
        return f"{market}法人：歷史快照未保存來源資格分項；0 票不能當成零買賣超。"
    dates = readiness.get("observed_effective_dates") or []
    return (
        f"{market}法人：資料日 {readiness.get('latest_effective_date') or '未知'}；"
        f"當日已收 {readiness.get('same_day_count', 0):,}/{universe_count:,} 檔，"
        f"自然 PIT {readiness.get('natural_pit_same_day_count', 0):,} 檔；"
        f"已存 {len(dates)} 個有效日；五交易日與成交對齊 "
        f"{readiness.get('five_session_ready_count', 0):,} 檔。"
        "此欄只稽核來源版本；多空條件由整合法人／分點規則判讀。"
    )
