"""Failed source bodies retain transport evidence without accepting bad data."""

import json

import pytest
import requests

from source_extension import adapters


def response(body, status=200, encoding="utf-8", content_type="application/json"):
    item = requests.Response()
    item.status_code = status
    item.encoding = encoding
    item.headers["Content-Length"] = str(len(body))
    item.headers["Content-Type"] = content_type
    item._content = body
    item._content_consumed = True
    return item


def fetch(monkeypatch, body, status=200, encoding="utf-8"):
    item = response(body, status, encoding)
    monkeypatch.setattr(adapters.requests, "get", lambda *args, **kwargs: item)
    return adapters._fetch_source_json_payload(
        "taifex_futures_large_trader_positions_daily",
        "https://example.invalid/source",
        request_params=None,
        request_headers={},
    )


@pytest.mark.parametrize("body", [b"", b"<html>temporarily unavailable</html>", b'{"data":'])
def test_invalid_json_retains_actual_http_status(monkeypatch, body):
    with pytest.raises(json.JSONDecodeError) as caught:
        fetch(monkeypatch, body)
    assert caught.value.http_status == 200
    assert caught.value.source_key == "taifex_futures_large_trader_positions_daily"
    assert caught.value.response_content_type == "application/json"
    assert caught.value.response_content_length == str(len(body))
    assert caught.value.response_bytes_received == len(body)


def test_invalid_encoding_retains_http_status(monkeypatch):
    with pytest.raises(UnicodeDecodeError) as caught:
        fetch(monkeypatch, b"\xff")
    assert caught.value.http_status == 200


def test_valid_source_keeps_payload_and_status(monkeypatch):
    payload, status = fetch(monkeypatch, b'[{"Date":"20261007"}]')
    assert payload == [{"Date": "20261007"}]
    assert status == 200


def test_http_failure_is_not_reclassified_as_json_failure(monkeypatch):
    with pytest.raises(requests.HTTPError) as caught:
        fetch(monkeypatch, b"unavailable", status=404)
    assert caught.value.http_status == 404
