import datetime as dt
import json

import pytest

from source_extension.signal_policy import (
    evidence_balance, is_observed_market_session, profitability_vote, quote_window,
    revenue_period_current,
    strip_negated_terms, trading_risk_action, trading_sessions,
)
from source_extension.adapters import _normalize_trading_status_snapshot
from source_extension.registry import SOURCE_DEFINITIONS_BY_KEY


def test_correlated_signals_cannot_outvote_opposing_fundamental_group():
    result = evidence_balance({"institutions": 1, "broker_branches": 1, "revenue": -1})
    assert result["direction"] == "訊號分歧"
    assert result["bullish_groups"] == result["bearish_groups"] == 1
    result = evidence_balance({"price": 1, "volume": 1, "institutions": -1,
                               "broker_branches": 1, "revenue": -1})
    assert result["group_directions"]["equity_order_flow"] == 0
    assert result["direction"] == "訊號分歧"
    assert evidence_balance({"institutions": 1, "broker_branches": 1,
                             "revenue": 1})["aligned_groups"] == 2


def test_calendar_gap_duplicate_and_no_trade_do_not_form_price_window():
    end = dt.date(2026, 10, 7)
    dates = [end - dt.timedelta(days=i) for i in range(60)]
    closed = {day for day in dates if day.weekday() >= 5} | {dt.date(2026, 10, 5)}
    sessions = trading_sessions(end, closed)
    assert dt.date(2026, 10, 5) not in sessions
    rows = [(day, 100., 1000., False) for day in sessions]
    assert len(quote_window(rows, sessions, 21)) == 21
    assert not quote_window(rows[:-2] + rows[-1:], sessions, 21)
    assert not quote_window(rows + [rows[-2]], sessions, 21)
    assert not quote_window(rows[:-1] + [(end, 100., 0., True)], sessions, 21)
    assert not trading_sessions(end, set())


def test_market_context_date_must_match_an_observed_quote_session():
    sessions = {"上市": {dt.date(2026, 10, 8)}, "上櫃": {dt.date(2026, 10, 8)}}
    assert is_observed_market_session(dt.date(2026, 10, 8), "上市", sessions)
    assert not is_observed_market_session(dt.date(2026, 10, 9), "上市", sessions)
    assert not is_observed_market_session(dt.date(2026, 10, 8), "未定", sessions)


def test_profitability_requires_same_season_not_positive_qoq_only():
    margins = {(2026, 1): -5., (2026, 2): 10.}
    assert not profitability_vote(margins)
    margins[(2025, 2)] = 15.
    vote = profitability_vote(margins)
    assert vote["direction"] == -1
    assert vote["change_pp"] == -5.
    assert profitability_vote({(2025, 2): -0.1, (2026, 2): 0.1})["direction"] == 1


def test_refreshed_old_revenue_export_does_not_revive_old_or_future_period():
    today = dt.date(2026, 10, 8)
    assert revenue_period_current(dt.date(2026, 8, 1), today)
    assert revenue_period_current(dt.date(2026, 9, 1), today)
    assert not revenue_period_current(dt.date(2026, 7, 1), today)
    assert not revenue_period_current(dt.date(2026, 10, 1), today)


def test_denied_events_do_not_match_positive_or_risk_substrings():
    text = "本公司尚未接獲訂單，尚未轉虧為盈；並無虧損。"
    terms = ("接獲訂單", "轉虧為盈", "虧損")
    assert not any(term in strip_negated_terms(text, terms) for term in terms)
    assert "接獲訂單" in strip_negated_terms("本公司接獲訂單", terms)


def test_trading_restrictions_override_direction_and_no_flag_is_not_a_vote():
    def context(**flags):
        return [{"event_type": "trading_status_snapshot", "details": flags}]
    assert "停止交易" in trading_risk_action(context(suspended=True, altered=True))
    assert "暫緩" in trading_risk_action(context(periodic=True))
    assert trading_risk_action(context(suspended=None, altered=False)) is None


def test_tpex_complete_risk_snapshot_clears_removed_name_and_keeps_dates():
    source = SOURCE_DEFINITIONS_BY_KEY["tpex_trading_status_daily"]
    codes = {("1234", "上櫃"), ("5678", "上櫃"), ("2330", "上市")}
    raw = {"Date": "1151007", "SecuritiesCompanyCode": "1234", "CompanyName": "測試",
           "AlteredTrading": "Ｙ", "PeriodicTrading": "", "ManagedStock": "",
           "SuspensionOfTrading": "Y"}
    records = _normalize_trading_status_snapshot(source, [raw], codes, dt.date(2026, 10, 8))
    assert len(records) == 2
    first, cleared = [json.loads(row["value_text"]) for row in records]
    assert first["suspended"] and first["altered"]
    assert not cleared["suspended"] and not cleared["listed_in_restricted_snapshot"]
    assert records[0]["effective_date"] == dt.date(2026, 10, 7)
    with pytest.raises(ValueError):
        _normalize_trading_status_snapshot(source, [], codes, dt.date(2026, 10, 8))
    with pytest.raises(ValueError):
        _normalize_trading_status_snapshot(source, [raw, raw], codes, dt.date(2026, 10, 8))
    with pytest.raises(ValueError):
        _normalize_trading_status_snapshot(source, [{**raw, "AlteredTrading": "?"}], codes, dt.date(2026, 10, 8))


def test_twse_undated_snapshot_is_never_backdated_or_treated_as_suspension_list():
    source = SOURCE_DEFINITIONS_BY_KEY["twse_trading_status_daily"]
    codes = {("1234", "上市"), ("5678", "上市")}
    records = _normalize_trading_status_snapshot(source,
        [{"Code": "1234", "Name": "測試", "PeriodicCallAuctionTrading": "**"}],
        codes, dt.date(2026, 10, 8))
    first, absent = [json.loads(row["value_text"]) for row in records]
    assert first["altered"] and first["periodic"]
    assert first["suspended"] is None and absent["suspended"] is None
    assert records[0]["effective_date"] == dt.date(2026, 10, 8)
    assert first["date_basis"] == "observed_date_no_report_date"
