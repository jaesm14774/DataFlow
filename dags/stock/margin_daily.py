"""Pure helpers for the daily margin (融資融券) collection.

Kept free of database and network imports so the acceptance rules can be unit tested.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd

# A daily load counts as complete when the target session carries at least this share of the
# codes a normal session carries (upper quartile of the recent sessions already stored).
MIN_TARGET_COVERAGE = 0.90
BASELINE_SESSIONS = 20
# Partial daily loads must not drag the norm down (a ratchet that would eventually accept them):
# the baseline is an upper-quartile count, so only a majority-degraded window can lower it.
BASELINE_QUANTILE = 0.75
# The page of a very liquid, always margin-eligible stock; its newest row tells whether the
# source has published the target session yet (one cheap request instead of ~1,700).
PROBE_CODE = "2330"


@dataclass(frozen=True)
class TargetCoverage:
    target_date: str
    codes_on_target: int
    baseline_codes: int
    ratio: float

    @property
    def accepted(self) -> bool:
        return self.baseline_codes > 0 and self.ratio >= MIN_TARGET_COVERAGE

    def describe(self) -> str:
        return (
            f"target={self.target_date} codes_on_target={self.codes_on_target} "
            f"baseline={self.baseline_codes} ratio={self.ratio:.3f} "
            f"min_ratio={MIN_TARGET_COVERAGE:.2f}"
        )


def baseline_code_count(per_date_codes: pd.Series, target_date: dt.date) -> int:
    """Upper-quartile distinct-code count of the latest sessions strictly before the target."""

    if per_date_codes is None or len(per_date_codes) == 0:
        return 0
    counts = per_date_codes.copy()
    counts.index = pd.to_datetime(counts.index)
    earlier = counts[counts.index < pd.Timestamp(target_date)].sort_index().tail(BASELINE_SESSIONS)
    if earlier.empty:
        return 0
    return int(earlier.quantile(BASELINE_QUANTILE))


def coverage_from_count(codes_on_target: int, target_date: dt.date, baseline_codes: int) -> TargetCoverage:
    """Coverage from a stored count (the database is the truth, not the last scrape)."""

    target = pd.Timestamp(target_date).strftime("%Y-%m-%d")
    ratio = (codes_on_target / baseline_codes) if baseline_codes > 0 else 0.0
    return TargetCoverage(target, int(codes_on_target), int(baseline_codes), float(ratio))


def target_coverage(result, target_date: dt.date, baseline_codes: int) -> TargetCoverage:
    """How many codes the scrape returned for the target session, against the stored norm."""

    target = pd.Timestamp(target_date).strftime("%Y-%m-%d")
    if not isinstance(result, pd.DataFrame) or result.empty:
        return TargetCoverage(target, 0, baseline_codes, 0.0)
    dates = pd.to_datetime(result["資料日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    on_target = result.loc[dates == target, "證券代號"].astype(str).nunique()
    ratio = (on_target / baseline_codes) if baseline_codes > 0 else 0.0
    return TargetCoverage(target, int(on_target), int(baseline_codes), float(ratio))


def source_has_target(latest_source_date, target_date: dt.date) -> bool:
    """True once the probe page lists the target session (or a later one)."""

    latest = pd.to_datetime(latest_source_date, errors="coerce")
    return bool(pd.notna(latest) and latest.date() >= pd.Timestamp(target_date).date())
