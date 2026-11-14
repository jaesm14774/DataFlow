from source_extension.adapters import _normalize_market_record
from source_extension.registry import (
    SOURCE_DEFINITIONS_BY_KEY,
    source_allows_empty_result,
    source_batch_requires_review,
)


def test_user_treats_dated_blank_untransferred_feeds_as_no_event():
    """User
    Given a dated untransferred report row with no company or share values
    When the source row is normalized
    Then it is omitted instead of quarantined as a stock event
    """
    for source_key, code_key, name_key, date_key, role_key, share_keys in (
        (
            "twse_insider_transfer_untransferred",
            "公司代號", "公司名稱", "出表日期", "申報人身分",
            ("未轉讓股數-自有持股", "未轉讓股數-保留運用決定權信託股數"),
        ),
        (
            "tpex_insider_transfer_untransferred",
            "SecuritiesCompanyCode", "CompanyName", "Date", "申請人身分",
            ("未轉讓股數自有持股", "未轉讓股數保留運用決定權信託股數"),
        ),
    ):
        source = SOURCE_DEFINITIONS_BY_KEY[source_key]
        raw = {
            code_key: None,
            name_key: None,
            date_key: "1151030",
            "市場別": "上市" if source_key.startswith("twse_") else "上櫃",
            role_key: None,
            **{key: "" for key in share_keys},
        }

        assert _normalize_market_record(source, raw) is None


def test_user_keeps_nonempty_untransferred_rows_in_quality_review():
    """User
    Given an untransferred row has a report date and a role but no company code
    When the source row is normalized
    Then the incomplete row remains quarantined for review
    """
    source = SOURCE_DEFINITIONS_BY_KEY[
        "twse_insider_transfer_untransferred"
    ]
    record = _normalize_market_record(
        source,
        {
            "公司代號": None,
            "公司名稱": None,
            "出表日期": "1151030",
            "申報人身分": "董事",
            "未轉讓股數-自有持股": None,
            "未轉讓股數-保留運用決定權信託股數": None,
            "未轉讓理由": None,
        },
    )

    assert record["quality_status"] == "quarantine"


def test_user_accepts_a_successful_empty_threshold_event_batch():
    """User
    Given an official threshold-event source returns no qualifying companies
    When a successful batch is checked for quality
    Then an empty result is accepted as a valid no-event observation
    """
    for source_key in (
        "twse_quarterly_forecast_variance",
        "tpex_quarterly_forecast_variance_events",
    ):
        assert source_allows_empty_result(source_key)
        assert not source_batch_requires_review(
            source_key,
            row_count=0,
            accepted_count=0,
            rejected_count=0,
            duplicate_count=0,
        )


def test_user_still_sees_unexpected_empty_snapshot_as_needing_review():
    """User
    Given a stock snapshot source that requires current market rows returns none
    When its successful batch is checked for quality
    Then the empty response remains a review issue
    """
    assert not source_allows_empty_result("twse_monthly_revenue")
    assert source_batch_requires_review(
        "twse_monthly_revenue",
        row_count=0,
        accepted_count=0,
        rejected_count=0,
        duplicate_count=0,
    )


def test_user_still_sees_quarantine_and_count_mismatches_for_sparse_feeds():
    """User
    Given a sparse event source returns rows with rejected data or bad accounting
    When its successful batch is checked for quality
    Then the batch still requires review
    """
    source_key = "twse_quarterly_forecast_variance"
    assert source_batch_requires_review(
        source_key,
        row_count=2,
        accepted_count=1,
        rejected_count=1,
        duplicate_count=0,
    )
    assert source_batch_requires_review(
        source_key,
        row_count=2,
        accepted_count=1,
        rejected_count=0,
        duplicate_count=0,
    )
    assert source_batch_requires_review(
        source_key,
        row_count=0,
        accepted_count=1,
        rejected_count=0,
        duplicate_count=0,
    )
