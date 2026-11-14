"""Announcement cards must not turn accounting wording into trading risk."""

from source_extension import market_signals

TITLE = "本公司115年第三季合併財報拆分停業部門， 更正本公司115年7月至9月營收公告數"
BODY = (
    "1.事實發生日:115/11/06 2.公司名稱:閎康 3.與公司關係:本公司 "
    "4.相互持股比例:不適用 5.發生緣由:115年第三季合併財報拆分停業部門，"
    "更正本公司115年7月至9月營收公告數。營業收入 6.因應措施:無"
)


def test_discontinued_operations_correction_is_not_a_bearish_risk():
    result = market_signals._classify_company_event(TITLE, BODY)

    assert result["label"] != "偏空風險"
    assert result["direction"] != "偏空"
    assert "比對更正前後" in result["action"]


def test_real_stoppage_is_still_bearish_risk():
    result = market_signals._classify_company_event(
        "公告本公司工廠停業並進入重整程序", "本公司因財務困難宣布停業。"
    )
    assert result["label"] == "偏空風險"


def test_summary_skips_financial_labels_without_percentages():
    summary = market_signals._summarize_company_event(BODY)

    assert "同期增減：" not in summary or "%" in summary
    assert summary.strip() != ""
    assert "更正" in summary


def test_intraday_card_has_no_ingestion_lag_footer():
    row = {
        "entity_id": "3587", "security_name": "閎康",
        "details_text": (
            '{"kind":"company_event","title":"%s","description":"%s",'
            '"event_time":"2026-11-06T16:23:58","source_created_at_utc":"2026-11-08T00:00:00",'
            '"source_key":"tpex_company_events",'
            '"event_signal":{"label":"x","action":"y"}}' % (TITLE, BODY)
        ),
    }
    card = market_signals._format_intraday_candidate(row)

    assert "資料庫首次收錄" not in card["footer"]["text"]
