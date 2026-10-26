from datetime import date

import pytest

from source_extension.outcome_lifecycle import (
    add_calendar_months,
    directional_excursion_pct,
    first_session_on_or_after,
    first_session_on_or_after_sorted,
    followup_target_date,
    percent_return,
    summarize_followup_rows,
)


def test_followup_targets_use_calendar_periods_and_month_end_clamping():
    assert followup_target_date(date(2026, 10, 22), "1w") == date(2026, 10, 29)
    assert followup_target_date(date(2026, 1, 31), "1m") == date(2026, 2, 28)
    assert add_calendar_months(date(2024, 1, 31), 1) == date(2024, 2, 29)
    assert followup_target_date(date(2026, 2, 28), "1y") == date(2027, 2, 28)


def test_followup_exit_uses_first_official_session_on_or_after_target():
    sessions = [date(2026, 10, 29), date(2026, 10, 30), date(2026, 11, 2)]
    assert first_session_on_or_after(date(2026, 10, 31), sessions) == date(2026, 11, 2)
    assert first_session_on_or_after(date(2026, 11, 3), sessions) is None
    sorted_sessions = tuple(sorted(sessions))
    assert first_session_on_or_after_sorted(
        date(2026, 10, 31), sorted_sessions
    ) == date(2026, 11, 2)
    assert first_session_on_or_after_sorted(
        date(2026, 11, 3), sorted_sessions
    ) is None


def test_price_returns_and_directional_excursions_validate_prices():
    assert percent_return(100, 105) == pytest.approx(5)
    assert percent_return(0, 105) is None
    assert directional_excursion_pct(100, 110, 95, 1) == pytest.approx((10, -5))
    assert directional_excursion_pct(100, 110, 95, -1) == pytest.approx((5, -10))


def test_summary_separates_price_direction_market_and_execution_proxy():
    """User can inspect outcome quality without conflating return measures.

    Given settled follow-ups use the deployed favorable/adverse excursion fields
    When a date-bounded outcome summary is generated
    Then price direction, execution proxy, market excess, and excursions stay separate.
    """
    row = {
        "horizon_key": "1w",
        "market": "上市",
        "policy_version": "policy_test",
        "source_status": "complete",
        "delivery_status": "sent",
        "scope": "signal",
        "candidate_selected": True,
        "direction": "偏空",
        "family_directions_json": '{"price":-1}',
        "entity_id": "2330",
        "signal_date": "2026-10-01",
        "status": "settled",
        "execution_status": "settled",
        "execution_error_reason": None,
        "benchmark_status": "settled",
        "benchmark_error_reason": None,
        "stock_return_pct": 4.0,
        "execution_return_pct": 3.0,
        "excess_return_pp": 2.0,
        "max_favorable_excursion_pct": 6.0,
        "max_adverse_excursion_pct": -2.0,
    }
    summary = summarize_followup_rows([row])
    selected = next(
        group for group in summary["groups"]
        if group["scope"] == "selected_candidate"
    )
    family = next(
        group for group in summary["groups"]
        if group["scope"] == "family_counterfactual"
    )
    assert summary["status"] == "ready"
    assert selected["raw_price_up_rate"] == 1
    assert selected["directional_price_hit_rate"] == 0
    assert selected["mean_directional_event_price_pct"] == -4
    assert selected["mean_directional_execution_proxy_pct"] == -3
    assert selected["mean_directional_market_excess_pp"] == -2
    assert selected["mean_max_favorable_excursion_pct"] == 2
    assert selected["mean_max_adverse_excursion_pct"] == -6
    assert selected["execution_proxy_ci_95"]["low_pct"] is None
    assert selected["signal_evidence_status"] == "insufficient_signal_dates"
    assert family["family"] == "price"
