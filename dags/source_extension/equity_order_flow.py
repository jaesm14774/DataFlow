"""Evaluate stored institutional flow and available branch data at scan cutoff.

Source availability, rule triggers and historical performance are separate facts.
"""
from __future__ import annotations

import datetime as dt
import math
from collections import Counter, defaultdict
from typing import Any

from source_extension.adapters import parse_number
from source_extension.institutional_flow import FLOW_ASOF_SQL, TPEX_FLOW_SOURCE, _date, _flow_values

RULE_VERSION = "equity_order_flow_v2"
INSTITUTION_MIN_NET_SHARES = 100_000
INSTITUTION_INTENSITY_PCT = {1: 10.0, 3: 5.0, 5: 5.0}
BROKER_CONCENTRATION_PCT = {1: 20.0, 3: 15.0, 5: 15.0}
BROKER_MIN_DAILY_VOLUME_SHARES = 1_000_000
TPEX_FLOW_URL = "https://data.gov.tw/dataset/11856"
BROKER_URL = "https://bsr.twse.com.tw/bshtm/bsWelcome.aspx"
COLLECTION_TASKS = ("stock_three_main_investor_collect", "stock_broker_inout_twse_collect_second_time")


def should_wait_for_collection(*, scan_mode, now_taipei, run_state=None, task_states=None):
    if scan_mode != "daily" or (now_taipei.hour, now_taipei.minute) >= (21, 30):
        return False
    if run_state in {"success", "failed"}:
        return False
    terminal = {"success", "failed", "skipped", "upstream_failed"}
    return not all((task_states or {}).get(task) in terminal for task in COLLECTION_TASKS)


def equity_order_flow_collection_finished(**context):
    """Called only by the after-close source DAG, before triggering the scan."""
    import pendulum
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    now = pendulum.now("Asia/Taipei")
    source_run = context.get("dag_run")
    if source_run and source_run.start_date and source_run.start_date.astimezone(now.tzinfo).date() < now.date():
        return True
    if not should_wait_for_collection(scan_mode="daily", now_taipei=now):
        return True
    floor = now.start_of("day").in_timezone("UTC")
    with create_session() as session:
        run = (session.query(DagRun)
               .filter(DagRun.dag_id == "stock_collect_everyday_process", DagRun.start_date >= floor)
               .order_by(DagRun.start_date.desc()).first())
        if not run:
            return False
        states = dict(session.query(TaskInstance.task_id, TaskInstance.state).filter(
            TaskInstance.dag_id == run.dag_id, TaskInstance.run_id == run.run_id,
            TaskInstance.task_id.in_(COLLECTION_TASKS),
        ).all())
        return not should_wait_for_collection(scan_mode="daily", now_taipei=now,
                                             run_state=run.state, task_states=states)


def _finite(value):
    value = parse_number(value)
    return value if value is not None and math.isfinite(value) else None


def select_institutional_observations(official_rows, legacy_rows=(), *, master,
                                      as_of_date, cutoff_utc):
    """Prefer versioned OTC data; also read the existing two-market stock table.

    Stored rows certify collection-time availability, not immutable PIT vintages.
    An explicit bad official revision blocks fallback for that security/date.
    """
    official = defaultdict(list)
    rejected = Counter()
    for row in official_rows:
        code = str(row.get("entity_id") or "").strip()
        day = _date(row.get("effective_date"))
        capture = row.get("captured_at")
        if code not in master or not day or day > as_of_date:
            continue
        if not isinstance(capture, dt.datetime) or capture > cutoff_utc:
            continue
        if master[code]["market"] != "上櫃":
            continue
        official[(code, day)].append(row)
    selected = defaultdict(dict)
    for (code, day), versions in sorted(official.items()):
        values = _flow_values(versions[0]) if len(versions) == 1 else None
        if values is None:
            rejected["official_quality_or_duplicate"] += 1
            continue
        row = versions[0]
        source = TPEX_FLOW_SOURCE
        basis = row.get("capture_basis") or "current_row_only"
        natural = bool(basis == "source_feed" and row.get("successful_source_run")
                       and row.get("dag_id") == "source_extension_collect_after_close"
                       and str(row.get("dag_run_id") or "").startswith("scheduled__"))
        foreign_basis = "source_excluding_foreign_dealers"
        selected[code][day] = {
            "total": values[0], "foreign": values[1], "trust": values[2], "dealer": values[3],
            "source_key": source, "source_url": TPEX_FLOW_URL,
            "effective_date": day.isoformat(),
            "captured_at": row["captured_at"].isoformat(), "capture_basis": basis,
            "natural_pit": natural, "foreign_component_basis": foreign_basis,
        }
    stored = defaultdict(list)
    for row in legacy_rows:
        code, day = str(row.get("code") or "").strip(), _date(row.get("date"))
        capture = row.get("captured_at")
        if (code not in master or master[code]["market"] not in ("上市", "上櫃")
                or not day or day > as_of_date or not isinstance(capture, dt.datetime)
                or capture > cutoff_utc):
            continue
        if (code, day) not in official:
            stored[(code, day)].append(row)
    for (code, day), rows in stored.items():
        values = tuple(_finite(rows[0].get(k)) for k in ("total", "foreign", "trust", "dealer"))
        if (len(rows) != 1 or any(v is None or not v.is_integer() for v in values)
                or values[0] != sum(values[1:])):
            rejected["stored_quality_or_duplicate"] += 1
            continue
        listed = master[code]["market"] == "上市"
        selected[code][day] = {
            "total": values[0], "foreign": values[1], "trust": values[2], "dealer": values[3],
            "source_key": "twse_t86_stored" if listed else "tpex_daily_trade_stored",
            "source_url": ("https://www.twse.com.tw/zh/trading/foreign/t86.html"
                           if listed else TPEX_FLOW_URL),
            "effective_date": day.isoformat(), "captured_at": rows[0]["captured_at"].isoformat(),
            "capture_basis": "stored_collection_time", "natural_pit": False,
            "foreign_component_basis": "stored_components_reconciled_to_total",
        }
    return dict(selected), dict(rejected)


def _price_panel(rows):
    grouped = defaultdict(list)
    for day, close, volume, *rest in rows:
        grouped[day].append((close, volume, bool(rest and rest[0])))
    return {
        day: float(items[0][1]) for day, items in grouped.items()
        if len(items) == 1 and _finite(items[0][0]) is not None and items[0][0] > 0
        and _finite(items[0][1]) is not None and items[0][1] > 0 and not items[0][2]
    }


def _choose_window(windows):
    qualified = [w for w in windows.values() if w.get("direction")]
    if len({w["direction"] for w in qualified}) > 1:
        return 0, None, "different_windows_disagree"
    if qualified:
        chosen = max(qualified, key=lambda w: w["days"])
        return chosen["direction"], chosen, "qualified"
    return 0, None, "below_threshold" if windows else "insufficient_same_day_data"


def institutional_signal(observations, price_rows, sessions, as_of_date):
    prices = _price_panel(price_rows)
    sessions = sorted(set(sessions))
    windows = {}
    if not sessions or sessions[-1] != as_of_date:
        return {
            "direction": 0, "vote_enabled": False,
            "status": "session_calendar_unavailable", "same_day_usable": False,
            "selected_window": None, "windows": {},
            "rule_version": RULE_VERSION,
            "latest_observation": observations.get(as_of_date),
        }
    for size, threshold in INSTITUTION_INTENSITY_PCT.items():
        dates = sessions[-size:]
        if len(dates) != size or any(day not in observations or day not in prices for day in dates):
            continue
        panel = [observations[day] for day in dates]
        totals = {key: sum(r[key] for r in panel) for key in ("total", "foreign", "trust", "dealer")}
        volume = sum(prices[day] for day in dates)
        intensity = totals["total"] / volume * 100
        sign = 1 if totals["total"] > 0 else -1 if totals["total"] < 0 else 0
        same_sign = sum(r["total"] * sign > 0 for r in panel)
        persistence = same_sign >= (4 if size == 5 else size)
        # Dealer totals include hedging. Dealer-only imbalance cannot activate
        # the institutional family when foreign + trust trades oppose it.
        professional_alignment = (totals["foreign"] + totals["trust"]) * sign > 0
        natural_pit_complete = all(bool(row.get("natural_pit")) for row in panel)
        direction = sign if (
            persistence and professional_alignment
            and abs(totals["total"]) >= INSTITUTION_MIN_NET_SHARES
            and abs(intensity) >= threshold
        ) else 0
        windows[str(size)] = {
            "days": size, "dates": [d.isoformat() for d in dates], **totals,
            "net_intensity_pct": round(intensity, 4), "same_sign_days": same_sign,
            "direction": direction,
            "natural_pit_complete": natural_pit_complete, "volume_shares": volume,
            "sources": sorted({r["source_key"] for r in panel}),
            "capture_bases": sorted({r["capture_basis"] for r in panel}),
            "foreign_trust_aligned": professional_alignment,
        }
    direction, chosen, status = _choose_window(windows)
    return {"direction": direction, "vote_enabled": bool(windows),
            "status": status, "same_day_usable": "1" in windows,
            "selected_window": chosen,
            "windows": windows, "rule_version": RULE_VERSION,
            "latest_observation": observations.get(as_of_date)}


def select_broker_panels(rows, *, master, as_of_date, cutoff_utc):
    grouped = defaultdict(list)
    for row in rows:
        code, day = str(row.get("code") or "").strip(), _date(row.get("date"))
        captured = row.get("captured_at")
        if (code in master and master[code]["market"] == "上市" and day and day <= as_of_date
                and isinstance(captured, dt.datetime) and captured <= cutoff_utc
                and row.get("type") == "分點"):
            grouped[(code, day)].append(row)
    panels, rejected = defaultdict(dict), Counter()
    for (code, day), items in grouped.items():
        branches, captures = {}, set()
        invalid = False
        for row in items:
            # Case matters: 989e (中和) and 989E (草屯) are different branches.
            branch = str(row.get("branch") or "").strip()
            buy, sell, net = (_finite(row.get(k)) for k in ("buy_lots", "sell_lots", "net_lots"))
            if (not branch or branch in branches or any(v is None for v in (buy, sell, net))
                    or buy < 0 or sell < 0 or abs(buy - sell - net) > 1.01):
                invalid = True
                break
            captures.add(row["captured_at"])
            branches[branch] = {"name": str(row.get("name") or ""),
                                "buy": buy * 1000, "sell": sell * 1000, "net": net * 1000}
        # Existing collector writes a whole security panel in one transaction.
        # Mixed cohorts or true duplicate IDs cannot certify its completeness.
        if invalid or len(captures) != 1:
            rejected["duplicate_invalid_or_mixed_capture_panel"] += 1
            continue
        buy, sell = sum(b["buy"] for b in branches.values()), sum(b["sell"] for b in branches.values())
        if buy <= 0 or sell <= 0 or abs(buy - sell) > max(2000, len(branches) * 1000):
            rejected["unbalanced_panel"] += 1
            continue
        panels[code][day] = {"branches": branches, "buy": buy, "sell": sell,
                             "captured_at": next(iter(captures)).isoformat()}
    return dict(panels), dict(rejected)


def broker_signal(panels, price_rows, sessions, as_of_date):
    prices, valid_panels = _price_panel(price_rows), {}
    rejected = Counter()
    for day, panel in panels.items():
        volume = prices.get(day)
        if not volume or volume < BROKER_MIN_DAILY_VOLUME_SHARES:
            rejected["low_or_missing_trade_volume"] += 1
        elif not (0.90 <= panel["buy"] / volume <= 1.10 and 0.90 <= panel["sell"] / volume <= 1.10):
            rejected["broker_quote_scope_or_coverage_mismatch"] += 1
        else:
            valid_panels[day] = panel
    sessions, windows = sorted(set(sessions)), {}
    if sessions and sessions[-1] == as_of_date:
        for size, threshold in BROKER_CONCENTRATION_PCT.items():
            dates = sessions[-size:]
            if len(dates) != size or any(day not in valid_panels for day in dates):
                continue
            by_branch, names = defaultdict(float), {}
            for day in dates:
                for branch, values in valid_panels[day]["branches"].items():
                    by_branch[branch] += values["net"]
                    names[branch] = values["name"]
            buyers = sorted(((b, v) for b, v in by_branch.items() if v > 0), key=lambda r: (-r[1], r[0]))[:15]
            sellers = sorted(((b, v) for b, v in by_branch.items() if v < 0), key=lambda r: (r[1], r[0]))[:15]
            buy_top, sell_top = sum(v for _, v in buyers), -sum(v for _, v in sellers)
            difference, volume = buy_top - sell_top, sum(prices[day] for day in dates)
            concentration = difference / volume * 100
            # Collector rounds to 0.01 lot before MySQL rounds to INT lots:
            # reserve 0.505 lot per top branch, side and session (15.15 lots/day).
            uncertainty = 15_150 * size
            conservative_pct = max(0, abs(difference) - uncertainty) / volume * 100
            direction = (1 if difference > 0 else -1) if conservative_pct >= threshold else 0
            windows[str(size)] = {
                "days": size, "dates": [d.isoformat() for d in dates], "direction": direction,
                "concentration_pct": round(concentration, 4), "conservative_abs_pct": round(conservative_pct, 4),
                "top15_buy_net_shares": buy_top, "top15_sell_net_shares": sell_top,
                "concentration_net_shares": difference, "volume_shares": volume,
                "rounding_bound_shares": uncertainty,
                "top_buyers": [{"branch": b, "name": names[b], "net_shares": v} for b, v in buyers[:3]],
                "top_sellers": [{"branch": b, "name": names[b], "net_shares": v} for b, v in sellers[:3]],
                "captures": [valid_panels[d]["captured_at"] for d in dates],
            }
    direction, chosen, status = _choose_window(windows)
    return {"direction": direction, "status": status, "selected_window": chosen,
            "windows": windows, "rule_version": RULE_VERSION, "source_url": BROKER_URL,
            "latest_effective_date": max(panels).isoformat() if panels else None,
            "same_day_panel_available": as_of_date in panels,
            "same_day_usable": as_of_date in valid_panels, "rejected_date_counts": dict(rejected)}


def load_equity_order_flow(source_cursor, stock_cursor, *, master, as_of_date, cutoff_utc,
                           price_rows_by_code, session_dates_by_market):
    """Read only existing tables; database creation timestamps are UTC."""
    # Quote availability is not a trading calendar: a market-wide missing quote
    # day must not turn an older date into the third/fifth previous session.
    calendar_verified = False
    try:
        calendar_floor = as_of_date - dt.timedelta(days=45)
        source_cursor.execute("SELECT now_date FROM holiday.taiwan WHERE now_date BETWEEN %s AND %s",
                              (calendar_floor, as_of_date))
        closed = {_date(row[0]) for row in source_cursor.fetchall()}
        dates = [as_of_date - dt.timedelta(days=i) for i in range(45)]
        calendar_verified = all(day in closed for day in dates if day.weekday() >= 5)
        sessions = sorted(day for day in dates if day.weekday() < 5 and day not in closed)[-5:] if calendar_verified else []
    except Exception:
        sessions = []
    session_dates_by_market = {
        market: sessions if calendar_verified else ([as_of_date] if as_of_date in observed else [])
        for market, observed in session_dates_by_market.items()
    }
    required_dates = {day for dates in session_dates_by_market.values() for day in sorted(dates)[-5:]}
    floor = min(required_dates, default=as_of_date)
    source_cursor.execute(FLOW_ASOF_SQL, (cutoff_utc, TPEX_FLOW_SOURCE, cutoff_utc, cutoff_utc,
                                         TPEX_FLOW_SOURCE, cutoff_utc, cutoff_utc, floor, as_of_date))
    names = ("record_hash", "entity_id", "effective_date", "quality_status", "unit", "value_text",
             "captured_at", "capture_basis", "dag_id", "dag_run_id", "successful_source_run")
    official = [dict(zip(names, row)) for row in source_cursor.fetchall()]
    stock_cursor.execute("""SELECT TRIM(證券代號),資料日期,三大法人買賣超股數,
        外資買賣超股數,投信買賣超股數,自營商買賣超股數,create_time
        FROM 三大法人買賣 WHERE 資料日期 BETWEEN %s AND %s AND create_time<=%s""",
                         (floor, as_of_date, cutoff_utc))
    names = ("code", "date", "total", "foreign", "trust", "dealer", "captured_at")
    stored_rows = [dict(zip(names, row)) for row in stock_cursor.fetchall()]
    observations, inst_rejected = select_institutional_observations(
        official, stored_rows, master=master, as_of_date=as_of_date, cutoff_utc=cutoff_utc)
    stock_cursor.execute("""SELECT TRIM(證券代號),資料日期,券商代號,券商名稱,
        買量,賣量,買賣超,資料類型,create_time FROM 券商進出總表
        WHERE 資料日期 BETWEEN %s AND %s AND create_time<=%s AND 資料類型='分點'""",
                         (floor, as_of_date, cutoff_utc))
    names = ("code", "date", "branch", "name", "buy_lots", "sell_lots", "net_lots", "type", "captured_at")
    broker_rows = [dict(zip(names, row)) for row in stock_cursor.fetchall()]
    panels, broker_rejected = select_broker_panels(
        broker_rows, master=master, as_of_date=as_of_date, cutoff_utc=cutoff_utc)
    contexts, readiness = {}, {}
    for code, info in master.items():
        sessions = session_dates_by_market.get(info["market"], set())
        inst = institutional_signal(observations.get(code, {}), price_rows_by_code.get(code, []), sessions, as_of_date)
        if as_of_date not in observations.get(code, {}):
            inst["status"] = "same_day_data_unavailable"
        broker = (broker_signal(panels.get(code, {}), price_rows_by_code.get(code, []), sessions, as_of_date)
                  if info["market"] == "上市" else {"direction": 0, "status": "source_unavailable_skipped", "windows": {}})
        contexts[code] = {"institutions": inst, "broker_branches": broker}
    for market in ("上市", "上櫃"):
        codes = {code for code, info in master.items() if info["market"] == market}
        available = {code for code in codes if as_of_date in observations.get(code, {})}
        traded = {code for code in codes if as_of_date in _price_panel(price_rows_by_code.get(code, []))}
        qualified = {
            code for code in codes
            if contexts[code]["institutions"].get("direction")
        }
        usable = {
            code for code in codes
            if "1" in contexts[code]["institutions"].get("windows", {})
        }
        broker = {
            "status": "available_subset" if market == "上市" else "source_unavailable_skipped",
            "same_day_count": len({str(row["code"]).strip() for row in broker_rows
                if str(row["code"]).strip() in codes and _date(row["date"]) == as_of_date
            }) if market == "上市" else None,
            "accepted_panel_count": sum(
                bool(contexts[code]["broker_branches"].get("same_day_panel_available"))
                for code in codes
            ) if market == "上市" else None,
            "direction_usable_count": sum(
                bool(contexts[code]["broker_branches"].get("same_day_usable"))
                for code in codes
            ) if market == "上市" else None,
            "qualified_vote_count": sum(
                bool(contexts[code]["broker_branches"]["direction"])
                for code in codes
            ) if market == "上市" else None,
            "rejected_panel_counts": broker_rejected if market == "上市" else {},
            "latest_effective_date": max(
                (_date(row["date"]) for row in broker_rows if str(row["code"]).strip() in codes),
                default=None,
            ).isoformat() if any(str(row["code"]).strip() in codes for row in broker_rows) else None,
        }
        expected_sessions = [
            day.isoformat() for day in sorted(session_dates_by_market.get(market, set()))[-5:]
        ]
        readiness[market] = {
            "rule_version": RULE_VERSION,
            "status": "evaluated" if available else "same_day_data_unavailable",
            "source_key": "twse_t86_stored" if market == "上市" else TPEX_FLOW_SOURCE,
            "vote_enabled": bool(usable),
            "same_day_count": len(available), "source_available_count": len(available),
            "direction_usable_count": len(usable), "qualified_vote_count": len(qualified),
            "five_session_ready_count": sum("5" in contexts[c]["institutions"]["windows"] for c in codes),
            "three_session_ready_count": sum("3" in contexts[c]["institutions"]["windows"] for c in codes),
            "natural_pit_same_day_count": sum(observations[c][as_of_date]["natural_pit"] for c in available),
            "traded_without_source_count": len(traded - available),
            "latest_effective_date": max(
                (day for c in codes for day in observations.get(c, {})), default=None
            ).isoformat() if any(observations.get(c) for c in codes) else None,
            "source_counts": dict(Counter(observations[c][as_of_date]["source_key"] for c in available)),
            "expected_session_dates": expected_sessions,
            "calendar_verified": calendar_verified,
            "broker": broker,
            "blocking_reasons": [] if available else ["same_day_records_missing"],
            "rejected_institution_counts": inst_rejected, "cutoff_utc": cutoff_utc.isoformat(),
            "performance_status": "rules_active_predictive_performance_unvalidated",
            "stored_collection_time_count": sum(
                observations[c][as_of_date]["capture_basis"] == "stored_collection_time" for c in available),
        }
    return contexts, readiness
