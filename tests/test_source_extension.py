from datetime import date
import json

import pytest
import requests

from http_fixture import make_response
from source_extension.adapters import (
    _normalize_market_record,
    _resolve_record_key_collisions,
    parse_number,
    parse_roc_date,
    parse_roc_month,
    validate_source_contract,
)
from source_extension import adapters
from source_extension.registry import (
    SOURCE_DEFINITIONS,
    _required_fields_missing,
    _source_field_value,
    priority_score,
)


def test_parse_roc_date_and_month():
    assert parse_roc_date("1150805") == date(2026, 8, 5)
    assert parse_roc_date("20260805") == date(2026, 8, 5)
    assert parse_roc_month("11506") == date(2026, 6, 1)


def test_priority_score_is_bounded_and_matches_gate():
    assert priority_score(1, 1, 1, 1, 1) == 100.0
    assert priority_score(0, 0, 0, 0, 0) == 0.0
    assert priority_score(0.75, 0.75, 0.75, 0.75, 0.75) == 75.0


def test_market_event_normalizes_to_joinable_stock_record():
    definition = next(item for item in SOURCE_DEFINITIONS if item["source_key"] == "twse_company_events")
    record = _normalize_market_record(
        {**definition, "config": definition["config"]},
        {
            "發言日期": "1150805",
            "發言時間": "70004",
            "公司代號": "2330",
            "公司名稱": "台積電",
            "主旨": "測試重大訊息",
            "符合條款": "第1款",
            "說明": "測試內容",
        },
    )
    assert record["entity_type"] == "stock"
    assert record["entity_id"] == "2330"
    assert record["effective_date"] == date(2026, 8, 5)
    assert record["event_type"] == "company_event"
    assert record["quality_status"] == "ok"


def test_twse_daily_quote_preserves_ohlc_for_future_followups():
    definition = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "twse_equity_quotes_daily"
    )
    record = _normalize_market_record(
        {**definition, "config": definition["config"]},
        {
            "Date": "20261021",
            "Code": "2330",
            "Name": "台積電",
            "TradeVolume": "1000000",
            "OpeningPrice": "1000",
            "HighestPrice": "1020",
            "LowestPrice": "995",
            "ClosingPrice": "1015",
            "Change": "15",
        },
    )
    values = json.loads(record["value_text"])
    assert record["quality_status"] == "ok"
    assert values["opening_price"] == 1000
    assert values["highest_price"] == 1020
    assert values["lowest_price"] == 995
    assert values["closing_price"] == 1015


def test_twse_margin_missing_source_date_stays_unknown_and_quarantined(monkeypatch):
    definition = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "twse_margin_balance_daily"
    )
    payload = [{
        "股票代號": "2330",
        "股票名稱": "測試公司",
        "融資今日餘額": "87",
        "融資前日餘額": "100",
        "融資限額": "500",
        "融券今日餘額": "28",
        "融券前日餘額": "30",
        "融券限額": "200",
    }]
    captured = {}

    def fake_fetch(source_key, endpoint_url, *, request_params, request_headers):
        captured.update(
            source_key=source_key,
            endpoint_url=endpoint_url,
            request_params=request_params,
        )
        return payload, 200

    monkeypatch.setattr(adapters, "_fetch_source_json_payload", fake_fetch)
    # No verifiable date (e.g. publication lag): rows must stay quarantined.
    monkeypatch.setattr(adapters, "_twse_margin_balance_data_date", lambda *a, **k: None)
    records, status = adapters.fetch_and_normalize(
        definition,
        allowed_stock_market_codes={("2330", "上市")},
    )

    assert status == 200
    assert captured["endpoint_url"] == (
        "https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN"
    )
    assert captured["request_params"] is None
    assert len(records) == 1
    assert records[0]["effective_date"] is None
    assert records[0]["quality_status"] == "quarantine"
    assert records[0]["_raw_record"]["Date"] is None


def test_network_contract_rejects_tampered_registry_endpoint():
    definition = next(item for item in SOURCE_DEFINITIONS if item["source_key"] == "cbc_fx_daily")
    assert validate_source_contract(definition)
    tampered = {**definition, "endpoint_url": "http://127.0.0.1/internal"}
    assert not validate_source_contract(tampered)


def test_taifex_large_trader_official_csv_variant_is_normalized(monkeypatch):
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_futures_large_trader_positions_daily"
    )
    body = (
        "\ufeff日期,契約,商品名稱(契約名稱),到期月份(週別),交易人類別,"
        "前五大交易人買方數量,前五大交易人賣方數量,前十大交易人買方數量,"
        "前十大交易人賣方數量,全市場未沖銷部位數\n"
        "20261012,TX,臺股期貨,202610,自營商,10,11,20,21,100\n"
    ).encode("utf-8")
    # The natural failed run returned the approved TAIFEX endpoint as
    # application/octet-stream without a usable filename header.
    response = make_response(body, content_type="application/octet-stream")
    request = {}

    def fake_get(url, **kwargs):
        request.update(url=url, **kwargs)
        return response

    monkeypatch.setattr(adapters.requests, "get", fake_get)
    records, status = adapters.fetch_and_normalize(source)

    assert status == 200
    assert request["headers"]["Accept"] == "application/json, text/csv"
    assert len(records) == 1
    assert records[0]["effective_date"] == date(2026, 10, 12)
    assert records[0]["entity_id"] == "TX::202610::自營商"
    assert records[0]["metric_value"] == 100
    assert records[0]["quality_status"] == "ok"

    runtime_source = {
        **source,
        "fields": [
            {
                "source_field": field[0],
                "target_field": field[1],
                "data_type": field[2],
                "required_flag": bool(field[4]),
            }
            for field in source["fields"]
        ],
    }
    assert _required_fields_missing(runtime_source, records[0]) == []
    assert _source_field_value(
        runtime_source, records[0]["_raw_record"], "Top5Buy"
    ) == "10"


@pytest.mark.parametrize("content_type", ["application/json", "application/octet-stream"])
def test_taifex_large_trader_official_json_variant_is_normalized(
    monkeypatch, content_type,
):
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_futures_large_trader_positions_daily"
    )
    body = json.dumps([{
        "Date": "20261012",
        "Contract": "TX",
        "ContractName": "臺股期貨",
        "SettlementMonth": "202610",
        "TypeOfTraders": "自營商",
        "Top5Buy": "10",
        "Top5Sell": "11",
        "Top10Buy": "20",
        "Top10Sell": "21",
        "OIOfMarket": "100",
    }]).encode("utf-8")
    response = make_response(body, content_type=content_type)
    request = {}

    def fake_get(url, **kwargs):
        request.update(url=url, **kwargs)
        return response

    monkeypatch.setattr(adapters.requests, "get", fake_get)
    records, status = adapters.fetch_and_normalize(source)

    assert status == 200
    assert request["headers"]["Accept"] == "application/json, text/csv"
    assert len(records) == 1
    assert records[0]["effective_date"] == date(2026, 10, 12)
    assert records[0]["entity_id"] == "TX::202610::自營商"
    assert records[0]["metric_value"] == 100
    assert records[0]["quality_status"] == "ok"


def test_taifex_large_trader_json_requires_official_fields(monkeypatch):
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_futures_large_trader_positions_daily"
    )
    body = b'[{"Date":"20261012","Contract":"TX"}]'
    response = make_response(body, content_type="application/json")
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(ValueError, match="fields do not match") as caught:
        adapters.fetch_and_normalize(source)

    assert getattr(caught.value, "http_status", None) == 200
    assert getattr(caught.value, "response_bytes_received", None) == len(body)


@pytest.mark.parametrize(
    ("filename", "body"),
    [
        (
            "unexpected.csv",
            b"Date,Contract\n20261012,TX\n",
        ),
        (
            "OpenInterestOfLargeTradersFutures.csv",
            b"wrong,header\n20261012,TX\n",
        ),
    ],
)
def test_taifex_octet_stream_requires_expected_csv_contract(monkeypatch, filename, body):
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_futures_large_trader_positions_daily"
    )
    response = make_response(body, content_type="application/octet-stream")
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(ValueError) as caught:
        adapters.fetch_and_normalize(source)

    assert getattr(caught.value, "http_status", None) == 200
    assert parse_number("NaN") is None
    assert parse_number("Infinity") is None


def test_taifex_index_options_official_csv_is_normalized(monkeypatch):
    """User. Given TAIFEX serves its options batch as CSV, When the approved source ingests it, Then date, side, institution, and net positions are retained."""
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_index_options_positions_daily"
    )
    body = (
        "日期,商品名稱,買賣權別,身份別,買方交易口數,買方交易契約金額(千元),"
        "賣方交易口數,賣方交易契約金額(千元),交易口數買賣淨額,"
        "交易契約金額買賣淨額(千元),買方未平倉口數,買方未平倉契約金額(千元),"
        "賣方未平倉口數,賣方未平倉契約金額(千元),未平倉口數買賣淨額,"
        "未平倉契約金額買賣淨額(千元)\n"
        "20261030,臺指選擇權,CALL,自營商,10,100,20,200,-10,-100,"
        "30,300,20,200,10,100\n"
    ).encode("utf-8")
    response = make_response(body, content_type="application/octet-stream")
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: response)

    records, status = adapters.fetch_and_normalize(source)

    assert source["adapter_key"] == "taifex_csv"
    assert status == 200
    assert len(records) == 1
    assert records[0]["effective_date"] == date(2026, 10, 30)
    assert records[0]["entity_id"] == "臺指選擇權::CALL::自營商"
    assert records[0]["metric_value"] == 10
    required_source_fields = [field[0] for field in source["fields"] if field[4]]
    assert all(
        _source_field_value(source, records[0]["_raw_record"], field) is not None
        for field in required_source_fields
    )
    assert json.loads(records[0]["value_text"]) == {
        "contract_code": "臺指選擇權",
        "call_put": "CALL",
        "institution_type": "自營商",
        "net_trading_lots": -10,
        "net_trading_value_thousands": -100,
        "net_open_interest_lots": 10,
        "net_open_interest_value_thousands": 100,
    }


def test_taifex_index_options_rejects_csv_header_drift(monkeypatch):
    """User. Given the official options feed changes its columns, When an octet-stream batch is parsed, Then the source is rejected instead of silently misread."""
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_index_options_positions_daily"
    )
    body = b"wrong,header\nvalue,value\n"
    response = make_response(body, content_type="application/octet-stream")
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(ValueError, match="header does not match") as caught:
        adapters.fetch_and_normalize(source)

    assert getattr(caught.value, "http_status", None) == 200


def test_other_csv_sources_still_reject_octet_stream(monkeypatch):
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/octet-stream"
    body = b"date,value\n20261016,1\n"
    response._content = body
    response._content_consumed = True
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(
        ValueError, match="unexpected content type: application/octet-stream",
    ):
        adapters._fetch_source_csv_rows(
            "tpex_dividends", "https://example.invalid/source", request_headers={},
        )


def test_incomplete_natural_key_does_not_drop_distinct_payloads():
    definition = next(item for item in SOURCE_DEFINITIONS if item["source_key"] == "twse_company_events")
    raw = {
        "發言日期": "1150805",
        "發言時間": "70004",
        "公司代號": "2330",
        "公司名稱": "台積電",
        "主旨": "重大訊息",
        "符合條款": "條款",
        "說明": "第一筆",
    }
    first = _normalize_market_record(definition, raw)
    second = _normalize_market_record(definition, {**raw, "說明": "第二筆"})
    resolved = _resolve_record_key_collisions([first, second])
    assert len({record["record_key"] for record in resolved}) == 2
    assert len({record["record_hash"] for record in resolved}) == 2


def _json_response(body: bytes, content_type: str):
    return make_response(body, content_type=content_type)


def test_taifex_index_options_json_variant_is_normalized(monkeypatch):
    """TAIFEX now serves the options operation as a JSON array."""
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "taifex_index_options_positions_daily"
    )
    body = json.dumps([{
        "Date": "20261106",
        "ContractCode": "臺指選擇權",
        "CallPut": "CALL",
        "Item": "外資及陸資",
        "TradingVolume(Long)": "1",
        "TradingVolume(Short)": "2",
        "TradingVolume(Net)": "-1",
        "TradingValue(Net)(Thousands)": "10",
        "OpenInterest(Net)": "-21734",
        "ContractValueofOpenInterest(Net)(Thousands)": "912268",
    }]).encode("utf-8")
    monkeypatch.setattr(
        adapters.requests, "get",
        lambda *a, **k: _json_response(body, "application/octet-stream"),
    )
    records, status = adapters.fetch_and_normalize(source)

    assert status == 200
    assert len(records) == 1
    assert records[0]["effective_date"] == date(2026, 11, 6)
    assert records[0]["metric_value"] is not None
    assert records[0]["quality_status"] == "ok"


def test_html_body_from_json_endpoint_backs_off_before_retrying(monkeypatch):
    """A 200 HTML block page needs a real pause, not a 1-2 second retry."""
    sleeps = []
    monkeypatch.setattr(adapters.time, "sleep", sleeps.append)
    responses = [
        _json_response(b"<html>busy</html>", "text/html; charset=UTF-8"),
        _json_response(b'[{"Date":"1151106"}]', "application/json"),
    ]
    monkeypatch.setattr(adapters.requests, "get", lambda *a, **k: responses.pop(0))

    payload, status = adapters._fetch_source_json_payload(
        "twse_company_events", "https://example.invalid/x",
        request_params=None, request_headers={},
    )

    assert payload == [{"Date": "1151106"}]
    assert sleeps == [adapters.HTML_BODY_RETRY_BACKOFF_SECONDS]


def test_margin_rows_get_a_date_only_when_it_matches_official_quotes(monkeypatch):
    """MI_MARGN carries no date: infer it, but never guess."""
    source = next(
        item for item in SOURCE_DEFINITIONS
        if item["source_key"] == "twse_margin_balance_daily"
    )
    margin_rows = [{
        "股票代號": "2330", "股票名稱": "台積電", "融資前日餘額": "10",
        "融資今日餘額": "12", "融資限額": "1000", "融券前日餘額": "1",
        "融券今日餘額": "2",
    }]
    monkeypatch.setattr(
        adapters, "_fetch_source_json_payload",
        lambda *a, **k: (list(margin_rows), 200),
    )

    monkeypatch.setattr(
        adapters, "_twse_margin_balance_data_date", lambda *a, **k: date(2026, 11, 6)
    )
    records, _ = adapters.fetch_and_normalize(
        source, allowed_stock_market_codes={("2330", "上市")}
    )
    assert records[0]["effective_date"] == date(2026, 11, 6)
    assert records[0]["quality_status"] == "ok"

    monkeypatch.setattr(
        adapters, "_twse_margin_balance_data_date", lambda *a, **k: None
    )
    records, _ = adapters.fetch_and_normalize(
        source, allowed_stock_market_codes={("2330", "上市")}
    )
    assert records[0]["effective_date"] is None
    assert records[0]["quality_status"] == "quarantine"


def _margin_open_api_rows(pairs):
    return [
        {"股票代號": f"{1000 + index}", "融資前日餘額": str(previous), "融資今日餘額": str(today)}
        for index, (previous, today) in enumerate(pairs)
    ]


def test_margin_date_follows_the_session_whose_balances_match(monkeypatch):
    """The OpenAPI file lags one session in the evening; match content, not timestamps."""
    day_a = {f"{1000 + i}": (100.0 + i, 110.0 + i) for i in range(400)}   # 11-05
    day_b = {f"{1000 + i}": (110.0 + i, 90.0 + i) for i in range(400)}    # 11-06
    dated = {date(2026, 11, 5): day_a, date(2026, 11, 6): day_b}
    monkeypatch.setattr(
        adapters, "_fetch_source_json_payload", lambda *a, **k: ([{"Date": "1151106"}], 200)
    )
    monkeypatch.setattr(adapters, "_latest_twse_market_data_date", lambda payload: date(2026, 11, 6))
    monkeypatch.setattr(
        adapters, "_twse_margin_dated_balances", lambda day, headers: dated.get(day, {})
    )

    evening_rows = _margin_open_api_rows(day_a.values())  # still the previous session
    assert adapters._twse_margin_balance_data_date("u", {}, evening_rows) == date(2026, 11, 5)
    morning_rows = _margin_open_api_rows(day_b.values())
    assert adapters._twse_margin_balance_data_date("u", {}, morning_rows) == date(2026, 11, 6)


def test_margin_date_stays_unknown_without_a_matching_report(monkeypatch):
    day = {f"{1000 + i}": (100.0 + i, 110.0 + i) for i in range(400)}
    monkeypatch.setattr(
        adapters, "_fetch_source_json_payload", lambda *a, **k: ([{"Date": "1151106"}], 200)
    )
    monkeypatch.setattr(adapters, "_latest_twse_market_data_date", lambda payload: date(2026, 11, 6))
    monkeypatch.setattr(adapters, "_twse_margin_dated_balances", lambda d, h: {})
    assert adapters._twse_margin_balance_data_date("u", {}, _margin_open_api_rows(day.values())) is None

    monkeypatch.setattr(adapters, "_twse_margin_dated_balances", lambda d, h: day)
    drifted = {code: (previous, today + 5) for code, (previous, today) in day.items()}
    assert adapters._twse_margin_balance_data_date("u", {}, _margin_open_api_rows(drifted.values())) is None
    assert adapters._twse_margin_balance_data_date("u", {}, []) is None


def _dated_margin_report(day, entries):
    fields = ["代號", "名稱", "買進", "賣出", "現金償還", "前日餘額", "今日餘額", "次一營業日限額",
              "買進", "賣出", "現券償還", "前日餘額", "今日餘額", "次一營業日限額", "資券互抵", "註記"]

    def row(code, name, margin_prev, margin_today, short_prev, short_today):
        return [code, name, "1", "1", "0", margin_prev, margin_today, "9", "0", "0", "0",
                short_prev, short_today, "9", "0", " "]

    data = [row("　", "合計", "9,999", "8,888", "1", "2")]
    data += [row(code, f"n{code}", f"{prev:,.0f}", f"{today:,.0f}", "5", "6") for code, (prev, today) in entries.items()]
    return {"stat": "OK", "date": day, "tables": [{}, {"title": "融資融券彙總 (股票)", "fields": fields, "data": data}]}


class _FakeReport:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status

    def json(self):
        return self._body


def test_dated_margin_report_parser_reads_margin_columns_only(monkeypatch):
    entries = {"2330": (34596.0, 35072.0), "1101": (36385.0, 33741.0)}
    monkeypatch.setattr(
        adapters.requests, "get",
        lambda url, **k: _FakeReport(_dated_margin_report("20261106", entries)),
    )
    parsed = adapters._twse_margin_dated_balances(date(2026, 11, 6), {})
    assert parsed == entries  # margin pair, not the short pair; total row skipped

    # a report whose own date differs from the requested one is rejected
    assert adapters._twse_margin_dated_balances(date(2026, 11, 5), {}) == {}
    monkeypatch.setattr(adapters.requests, "get", lambda url, **k: _FakeReport({}, status=503))
    assert adapters._twse_margin_dated_balances(date(2026, 11, 6), {}) == {}


def test_margin_date_survives_one_failed_candidate_and_is_stamped(monkeypatch):
    day = {f"{1000 + i}": (100.0 + i, 110.0 + i) for i in range(400)}
    monkeypatch.setattr(
        adapters, "_fetch_source_json_payload", lambda *a, **k: ([{"Date": "1151106"}], 200)
    )
    monkeypatch.setattr(adapters, "_latest_twse_market_data_date", lambda payload: date(2026, 11, 6))

    def dated(day_, headers):
        if day_ == date(2026, 11, 6):
            raise ValueError("html throttle page")
        return day if day_ == date(2026, 11, 5) else {}

    monkeypatch.setattr(adapters, "_twse_margin_dated_balances", dated)
    rows = _margin_open_api_rows(day.values())
    assert adapters._twse_margin_balance_data_date("u", {}, rows) == date(2026, 11, 5)

    # holiday weekday in the lookback (no report) does not stop the search
    monkeypatch.setattr(adapters, "_latest_twse_market_data_date", lambda payload: date(2026, 11, 9))
    monkeypatch.setattr(adapters, "_twse_margin_dated_balances", lambda d_, h: day if d_ == date(2026, 11, 6) else {})
    assert adapters._twse_margin_balance_data_date("u", {}, rows) == date(2026, 11, 6)
