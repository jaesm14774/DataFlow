import datetime as dt

import pytest

pytest.importorskip("pendulum")  # market_signals needs the Airflow runtime; runs in the worker image
from source_extension import market_signals as ms  # noqa: E402


def _row(period, yoy):
    # (period, published_at, yoy, amount, prior_year_amount, ytd_yoy, note, source_key, source_url)
    return (period, dt.datetime(2026, 11, 10), yoy, 100.0, 90.0, 5.0, None, "twse_monthly_revenue", "u")


def test_prior_month_yoy_uses_only_the_immediately_preceding_month():
    rows = [_row(dt.date(2026, 9, 1), 30.0), _row(dt.date(2026, 10, 1), 12.0)]
    assert ms._prior_month_yoy_context(rows) == {"revenue_yoy_prior_month_pct": 30.0}


def test_gap_or_missing_prior_yoy_is_unknown_not_zero():
    assert ms._prior_month_yoy_context([_row(dt.date(2026, 8, 1), 30.0), _row(dt.date(2026, 10, 1), 12.0)]) == {}
    assert ms._prior_month_yoy_context([_row(dt.date(2026, 9, 1), None), _row(dt.date(2026, 10, 1), 12.0)]) == {}
    assert ms._prior_month_yoy_context([]) == {}


def test_year_boundary_prior_month_is_december():
    rows = [_row(dt.date(2025, 12, 1), 8.0), _row(dt.date(2026, 1, 1), 20.0)]
    assert ms._prior_month_yoy_context(rows) == {"revenue_yoy_prior_month_pct": 8.0}
