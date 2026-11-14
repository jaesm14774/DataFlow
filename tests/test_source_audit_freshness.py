from datetime import date

from source_extension.audit_runs import classify_effective_date_freshness


def test_daily_source_audit_flags_stale_effective_date_without_rejecting_rows():
    """User Given a daily source succeeds with an effective date six days old
    and a five-day freshness budget, When the source audit classifies it,
    Then the audit reports its age and stale flag.
    """
    age_days, quality_flag = classify_effective_date_freshness(
        effective_date=date(2026, 10, 22),
        run_date=date(2026, 10, 28),
        max_age_days=5,
    )

    assert age_days == 6
    assert quality_flag == "latest_effective_date_older_than_expected"


def test_effective_date_at_freshness_boundary_remains_current():
    """User Given a daily source is exactly at its freshness budget, When the
    source audit classifies it, Then it does not report the date as stale.
    """
    age_days, quality_flag = classify_effective_date_freshness(
        effective_date=date(2026, 10, 22),
        run_date=date(2026, 10, 27),
        max_age_days=5,
    )

    assert age_days == 5
    assert quality_flag is None


def test_effective_date_after_run_date_is_flagged_as_future():
    """User Given a source effective date is later than its collection run,
    When the audit classifies it, Then the audit reports a future-date flag.
    """
    age_days, quality_flag = classify_effective_date_freshness(
        effective_date=date(2026, 10, 28),
        run_date=date(2026, 10, 27),
        max_age_days=3,
    )

    assert age_days == -1
    assert quality_flag == "latest_effective_date_in_future"


def test_effective_date_without_configured_age_limit_remains_unclassified():
    """User Given a source has no configured age limit, When its effective
    date freshness is classified, Then age and flag remain unknown.
    """
    age_days, quality_flag = classify_effective_date_freshness(
        effective_date=date(2026, 10, 22),
        run_date=date(2026, 10, 27),
        max_age_days=None,
    )

    assert age_days is None
    assert quality_flag is None
