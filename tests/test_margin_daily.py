"""Acceptance rules of the daily margin collection (pure helpers, no database or network)."""

import datetime as dt

import pandas as pd

from stock import margin_daily

TARGET = dt.date(2026, 11, 16)


def _scrape(codes: int, day: str = "2026-11-16", extra_days: int = 3) -> pd.DataFrame:
    """What the scraper returns: each code carries its recent window of dates."""

    dates = [day] + [f"2026-11-{13 - i:02d}" for i in range(extra_days)]
    rows = [{"證券代號": f"{1000 + code}", "資料日期": date} for code in range(codes) for date in dates]
    return pd.DataFrame(rows)


def _per_date(counts):
    index = pd.to_datetime([f"2026-11-{day:02d}" for day in (4, 5, 6, 9, 10, 11, 12, 13)][-len(counts):])
    return pd.Series(counts, index=index)


def test_baseline_is_the_normal_session_size_despite_a_few_partial_days():
    series = _per_date([1000, 1690, 1692, 1688, 1691, 1689, 1690, 1693])
    assert margin_daily.baseline_code_count(series, TARGET) >= 1690


def test_partial_loads_do_not_ratchet_the_baseline_down():
    full = [1690] * 14
    partial = [1000, 1000, 1000, 1000, 1000, 1000]
    index = pd.bdate_range(end="2026-11-13", periods=20)
    series = pd.Series(full + partial, index=index)
    baseline = margin_daily.baseline_code_count(series, TARGET)
    assert baseline == 1690
    assert not margin_daily.coverage_from_count(1000, TARGET, baseline).accepted


def test_baseline_ignores_the_target_session_itself():
    series = pd.Series([1690, 1690, 1690, 1690, 1690, 12], index=pd.to_datetime(
        ["2026-11-09", "2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13", "2026-11-16"]))
    assert margin_daily.baseline_code_count(series, TARGET) == 1690


def test_baseline_without_history_is_zero_and_never_accepts():
    assert margin_daily.baseline_code_count(pd.Series(dtype=float), TARGET) == 0
    coverage = margin_daily.target_coverage(_scrape(50), TARGET, 0)
    assert not coverage.accepted


def test_complete_target_session_is_accepted():
    coverage = margin_daily.target_coverage(_scrape(1650), TARGET, 1690)
    assert coverage.codes_on_target == 1650
    assert coverage.accepted


def test_short_coverage_is_rejected():
    coverage = margin_daily.target_coverage(_scrape(1000), TARGET, 1690)
    assert not coverage.accepted
    assert "ratio=0.59" in coverage.describe()


def test_target_session_missing_is_rejected_even_when_older_days_are_complete():
    result = _scrape(1690, day="2026-11-13")
    coverage = margin_daily.target_coverage(result, TARGET, 1690)
    assert coverage.codes_on_target == 0
    assert not coverage.accepted


def test_empty_or_non_frame_result_is_rejected():
    assert not margin_daily.target_coverage([], TARGET, 1690).accepted
    assert not margin_daily.target_coverage(pd.DataFrame(columns=["證券代號", "資料日期"]), TARGET, 1690).accepted


def test_a_code_repeated_on_the_target_counts_once():
    result = pd.concat([_scrape(1000), _scrape(1000)])
    assert margin_daily.target_coverage(result, TARGET, 1690).codes_on_target == 1000


def test_coverage_from_stored_count_uses_the_database_not_the_scrape():
    assert margin_daily.coverage_from_count(1690, TARGET, 1690).accepted
    assert not margin_daily.coverage_from_count(1500, TARGET, 1690).accepted
    assert not margin_daily.coverage_from_count(10, TARGET, 0).accepted


def test_boundary_is_ninety_percent():
    baseline = 1000
    assert margin_daily.target_coverage(_scrape(900), TARGET, baseline).accepted
    assert not margin_daily.target_coverage(_scrape(899), TARGET, baseline).accepted


def test_source_probe_waits_until_target_is_listed():
    assert not margin_daily.source_has_target("2026-11-13", TARGET)
    assert margin_daily.source_has_target("2026-11-16", TARGET)
    assert margin_daily.source_has_target("2026-11-17", TARGET)
    assert not margin_daily.source_has_target(None, TARGET)
    assert not margin_daily.source_has_target("not a date", TARGET)
