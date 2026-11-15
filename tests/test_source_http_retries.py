"""Acceptance checks for transient failures from official source endpoints."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from source_extension import adapters

ResponseSpec = tuple[int, str, bytes, int | None]


class _ResponseSequenceHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        status, content_type, body, content_length = self.server.responses.pop(0)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header(
            "Content-Length", str(content_length if content_length is not None else len(body))
        )
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True

    def log_message(self, format: str, *args: object) -> None:
        return


@contextmanager
def _serve_responses(responses: list[ResponseSpec]) -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ResponseSequenceHandler)
    server.responses = list(responses)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/source"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _fetch(endpoint_url: str) -> tuple[object, int]:
    return adapters._fetch_source_json_payload(
        "twse_company_events",
        endpoint_url,
        request_params=None,
        request_headers={"Accept": "application/json"},
    )


def _fetch_csv(endpoint_url: str) -> tuple[list[dict[str, str]], int]:
    return adapters._fetch_source_csv_rows(
        "taifex_futures_large_trader_positions_daily",
        endpoint_url,
        request_headers={"Accept": "text/csv"},
    )


def test_json_fetch_recovers_after_truncated_http_response():
    """User can collect a source after an interrupted response stream.

    Given the endpoint closes a short HTTP 200 body before its declared length
    When the source adapter fetches the endpoint
    Then it retries and returns the next complete JSON payload.
    """
    with _serve_responses(
        [
            (200, "application/json", b"[]", 100),
            (200, "application/json", b'[{"company":"2317"}]', None),
        ]
    ) as endpoint_url:
        payload, status = _fetch(endpoint_url)

    assert payload == [{"company": "2317"}]
    assert status == 200


def test_csv_fetch_recovers_after_truncated_http_response():
    """User can collect a CSV source after an interrupted response stream.

    Given the endpoint closes a short CSV body before its declared length
    When the source adapter fetches the endpoint
    Then it retries and returns the next complete source row.
    """
    columns = [
        "日期",
        "契約",
        "商品名稱(契約名稱)",
        "到期月份(週別)",
        "交易人類別",
        "前五大交易人買方數量",
        "前五大交易人賣方數量",
        "前十大交易人買方數量",
        "前十大交易人賣方數量",
        "全市場未沖銷部位數",
    ]
    row = ["20261030", "TXF", "台指期", "202611", "外資", "1", "2", "3", "4", "5"]
    complete_csv = (",".join(columns) + "\n" + ",".join(row) + "\n").encode("utf-8")
    with _serve_responses(
        [
            (200, "text/csv; charset=utf-8", b"date,contract\n", 100),
            (200, "text/csv; charset=utf-8", complete_csv, None),
        ]
    ) as endpoint_url:
        rows, status = _fetch_csv(endpoint_url)

    assert len(rows) == 1
    assert rows[0]["日期"] == "20261030"
    assert status == 200
