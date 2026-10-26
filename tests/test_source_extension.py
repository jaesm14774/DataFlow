from datetime import date
import json

import pytest
import requests

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
    AUTO_ENABLE_THRESHOLD,
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
    assert AUTO_ENABLE_THRESHOLD == 75.0


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
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/octet-stream"
    response.headers["Content-Length"] = str(len(body))
    # The natural failed run returned the approved TAIFEX endpoint as
    # application/octet-stream without a usable filename header.
    response._content = body
    response._content_consumed = True
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
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = content_type
    response.headers["Content-Length"] = str(len(body))
    response._content = body
    response._content_consumed = True
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
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/json"
    response.headers["Content-Length"] = str(len(body))
    response._content = body
    response._content_consumed = True
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
    response = requests.Response()
    response.status_code = 200
    response.headers["Content-Type"] = "application/octet-stream"
    response.headers["Content-Length"] = str(len(body))
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response._content = body
    response._content_consumed = True
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(ValueError) as caught:
        adapters.fetch_and_normalize(source)

    assert getattr(caught.value, "http_status", None) == 200
    assert parse_number("NaN") is None
    assert parse_number("Infinity") is None


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
