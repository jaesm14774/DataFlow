"""Allowlisted source adapters and normalization rules.

Adapters intentionally return a small common record shape.  The complete
source payload is retained for traceability, while the common columns make
cross-domain joins possible without repeatedly altering legacy tables.
"""

from __future__ import annotations

import datetime as dt
import calendar
import csv
import hashlib
import io
import json
import math
import re
import time
from typing import Any, Iterable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests


DEFAULT_HEADERS = {"User-Agent": "DataFlow-source-extension/1.0"}
MAX_RESPONSE_BYTES = 10 * 1024 * 1024
MAX_RESPONSE_BYTES_BY_SOURCE = {
    # TDCC returns one unpaginated full-market snapshot; the current weekly
    # response is close to the generic limit and grows with listed securities.
    "tdcc_shareholder_distribution_weekly": 20 * 1024 * 1024,
}
MAX_SOURCE_ROWS = 100_000
MAX_RECORD_PAYLOAD_BYTES = 1 * 1024 * 1024
MAX_TEXT_FIELD_BYTES = 60 * 1024
MAX_MAPPED_SOURCES = 32
TWSE_DAILY_QUOTES_FALLBACK_URL = (
    "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"
)
TWSE_DAILY_INDEX_FALLBACK_URL = (
    "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=open_data"
)
TWSE_TOTAL_RETURN_FALLBACK_URL = (
    "https://www.twse.com.tw/indicesReport/MFI94U?response=open_data"
)
TPEX_OPENAPI_RANGE_SOURCE_KEYS = frozenset(
    {
        "tpex_company_events",
        "tpex_monthly_revenue",
        "tpex_equity_quotes_daily",
    }
)
TPEX_OPENAPI_RANGE_CHUNK_BYTES = 64 * 1024
TPEX_OPENAPI_RANGE_SEGMENT_ATTEMPTS = 3
SOURCE_HTTP_SERVER_ERROR_ATTEMPTS = 3
CSV_REQUIRED_COLUMNS_BY_SOURCE = {
    "twse_equity_quotes_daily": {
        "日期", "證券代號", "證券名稱", "成交股數", "開盤價", "最高價",
        "最低價", "收盤價", "漲跌價差",
    },
    "twse_market_index_daily": {
        "日期", "開盤指數", "最高指數", "最低指數", "收盤指數",
    },
    "twse_market_total_return_index_daily": {
        "日　期", "發行量加權股價報酬指數",
    },
    "taifex_futures_large_trader_positions_daily": {
        "日期", "契約", "商品名稱(契約名稱)", "到期月份(週別)", "交易人類別",
        "前五大交易人買方數量", "前五大交易人賣方數量",
        "前十大交易人買方數量", "前十大交易人賣方數量", "全市場未沖銷部位數",
    },
    "tpex_company_events": {
        "出表日期", "發言日期", "發言時間", "公司代號", "公司名稱", "主旨", "說明",
    },
    "tpex_dividends": {
        "出表日期", "公司代號", "股利年度", "期別", "決議（擬議）進度",
    },
    "tpex_monthly_revenue": {
        "出表日期", "資料年月", "公司代號", "公司名稱", "營業收入-當月營收",
        "營業收入-去年同月增減(%)",
    },
    "tpex_daytrade_eligibility_daily": {
        "資料日期", "證券代號", "證券名稱",
        "暫停現股賣出後現款買進當沖註記",
    },
    "tpex_quarterly_forecast_variance_events": {
        "出表日期", "年度", "季別", "公司代號", "公司名稱",
    },
    "tpex_foreign_investor_holdings_daily": {
        "資料日期", "排行", "代號", "名稱", "發行股數",
        "僑外資及陸資尚可投資股數", "僑外資及陸資持有股數",
        "僑外資及陸資尚可投資比率", "僑外資及陸資持股比率",
        "法令投資上限比率",
    },
}
TAIFEX_LARGE_TRADER_JSON_REQUIRED_COLUMNS = {
    "Date", "Contract", "SettlementMonth", "TypeOfTraders",
    "Top5Buy", "Top5Sell", "Top10Buy", "Top10Sell", "OIOfMarket",
}


class IncompleteTPExOpenAPIResponseError(ValueError):
    """Incomplete official TPEx batch response after bounded byte ranges."""

    def __init__(self, message: str, *, http_status: int | None) -> None:
        super().__init__(message)
        self.http_status = http_status


def _fetch_tpex_openapi_json_payload(
    source_key: str,
    endpoint_url: str,
    *,
    request_params: dict[str, Any] | None,
    request_headers: dict[str, str],
) -> tuple[Any, int]:
    """Fetch the approved TPEx batch JSON feeds in validated byte ranges."""

    if source_key not in TPEX_OPENAPI_RANGE_SOURCE_KEYS:
        raise ValueError("TPEx range source key is not allowlisted")

    body = bytearray()
    expected_total: int | None = None
    entity_tag: str | None = None
    last_modified: str | None = None
    last_status: int | None = None
    request_count = 0
    identity_headers = {**request_headers, "Accept-Encoding": "identity"}

    while expected_total is None or len(body) < expected_total:
        byte_start = len(body)
        requested_end = byte_start + TPEX_OPENAPI_RANGE_CHUNK_BYTES - 1
        segment: bytes | None = None
        last_error: Exception | None = None

        for attempt in range(1, TPEX_OPENAPI_RANGE_SEGMENT_ATTEMPTS + 1):
            headers = {
                **identity_headers,
                "Range": f"bytes={byte_start}-{requested_end}",
            }
            validator = entity_tag or last_modified
            if validator:
                headers["If-Range"] = validator

            response = None
            attempt_status: int | None = None
            request_count += 1
            try:
                response = requests.get(
                    endpoint_url,
                    params=request_params,
                    headers=headers,
                    timeout=60,
                    allow_redirects=False,
                    stream=True,
                )
                last_status = int(response.status_code)
                attempt_status = last_status
                if 500 <= last_status < 600 and attempt < TPEX_OPENAPI_RANGE_SEGMENT_ATTEMPTS:
                    last_error = requests.HTTPError(
                        f"TPEx batch endpoint returned HTTP {last_status}"
                    )
                    time.sleep(min(attempt, 2))
                    continue
                response.raise_for_status()
                if 300 <= last_status < 400:
                    raise ValueError("source endpoint returned an unexpected redirect")
                if last_status != 206:
                    raise ValueError("TPEx batch endpoint did not honor byte range")

                content_encoding = str(
                    response.headers.get("Content-Encoding") or "identity"
                )
                if content_encoding.casefold() not in {"", "identity"}:
                    raise ValueError("TPEx batch endpoint ignored identity encoding")

                content_range = str(response.headers.get("Content-Range") or "")
                unit, separator, range_spec = content_range.partition(" ")
                byte_spec, slash, total_text = range_spec.partition("/")
                start_text, dash, end_text = byte_spec.partition("-")
                if (
                    unit.casefold() != "bytes" or not separator or not slash or not dash
                    or not start_text.isdigit() or not end_text.isdigit()
                    or not total_text.isdigit()
                ):
                    raise ValueError("TPEx batch endpoint returned invalid Content-Range")

                range_start = int(start_text)
                range_end = int(end_text)
                range_total = int(total_text)
                if (
                    range_start != byte_start
                    or range_total <= 0
                    or range_end != min(requested_end, range_total - 1)
                ):
                    raise ValueError("TPEx batch endpoint returned a mismatched range")
                if expected_total is not None and range_total != expected_total:
                    raise ValueError("TPEx batch representation size changed during retrieval")
                if range_total > MAX_RESPONSE_BYTES:
                    raise ValueError("source response exceeds size limit")
                expected_total = range_total

                response_etag = response.headers.get("ETag")
                response_modified = response.headers.get("Last-Modified")
                if entity_tag and response_etag != entity_tag:
                    raise ValueError("TPEx batch ETag changed during retrieval")
                if last_modified and response_modified != last_modified:
                    raise ValueError("TPEx batch Last-Modified changed during retrieval")
                if entity_tag is None and response_etag:
                    entity_tag = response_etag
                if last_modified is None and response_modified:
                    last_modified = response_modified
                if not entity_tag and not last_modified:
                    raise ValueError("TPEx batch response has no representation validator")

                expected_segment_bytes = range_end - range_start + 1
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) != expected_segment_bytes:
                    raise ValueError("TPEx range length conflicts with Content-Length")

                segment_body = bytearray()
                try:
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if chunk:
                            segment_body.extend(chunk)
                except requests.RequestException as exc:
                    last_error = exc
                else:
                    if len(segment_body) == expected_segment_bytes:
                        segment = bytes(segment_body)
                    else:
                        last_error = ValueError("TPEx byte-range response was incomplete")
            except requests.RequestException as exc:
                last_error = exc
                if attempt_status is not None and attempt_status < 500:
                    raise
            finally:
                if response is not None:
                    response.close()

            if segment is not None:
                break
            if attempt < TPEX_OPENAPI_RANGE_SEGMENT_ATTEMPTS:
                time.sleep(min(attempt, 2))

        if segment is None:
            message = (
                "TPEx batch response remained incomplete after bounded byte-range retrieval; "
                f"source_key={source_key} requests={request_count} "
                f"bytes_received={len(body)} "
                f"expected_bytes={expected_total if expected_total is not None else 'unknown'} "
                f"last_http_status={last_status} error={type(last_error).__name__ if last_error else 'unknown'}"
            )
            raise IncompleteTPExOpenAPIResponseError(message, http_status=last_status)

        body.extend(segment)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("source response exceeds size limit")

    payload = json.loads(bytes(body).decode("utf-8-sig"))
    if not isinstance(payload, list):
        raise ValueError("TPEx batch source response is not a JSON array")
    if len(payload) > MAX_SOURCE_ROWS:
        raise ValueError("source row count exceeds limit")
    print(
        "[tpex_batch_transfer] state=complete "
        f"source_key={source_key} requests={request_count} "
        f"bytes_received={len(body)} rows={len(payload)} "
        f"last_http_status={last_status} last_modified={last_modified or 'unknown'}"
    )
    return payload, int(last_status or 206)


def _fetch_source_json_payload(
    source_key: str,
    endpoint_url: str,
    *,
    request_params: dict[str, Any] | None,
    request_headers: dict[str, str],
) -> tuple[Any, int]:
    """Fetch one bounded source JSON response, retrying transient HTTP 5xx."""

    if source_key in TPEX_OPENAPI_RANGE_SOURCE_KEYS:
        return _fetch_tpex_openapi_json_payload(
            source_key,
            endpoint_url,
            request_params=request_params,
            request_headers=request_headers,
        )

    for attempt in range(1, SOURCE_HTTP_SERVER_ERROR_ATTEMPTS + 1):
        response = None
        total_bytes = 0
        try:
            response = requests.get(
                endpoint_url,
                params=request_params,
                headers=request_headers,
                timeout=60,
                allow_redirects=False,
                stream=True,
            )
            status = int(response.status_code)
            if 500 <= status < 600 and attempt < SOURCE_HTTP_SERVER_ERROR_ATTEMPTS:
                response.close()
                response = None
                print(
                    "[source_http_retry] "
                    f"source_key={source_key} attempt={attempt}/"
                    f"{SOURCE_HTTP_SERVER_ERROR_ATTEMPTS} status={status}"
                )
                time.sleep(min(attempt, 2))
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                exc.http_status = status
                raise
            if 300 <= status < 400:
                raise ValueError("source endpoint returned an unexpected redirect")
            response_limit = MAX_RESPONSE_BYTES_BY_SOURCE.get(
                source_key, MAX_RESPONSE_BYTES
            )
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > response_limit:
                raise ValueError("source response exceeds size limit")
            chunks: list[bytes] = []
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total_bytes += len(chunk)
                if total_bytes > response_limit:
                    raise ValueError("source response exceeds size limit")
                chunks.append(chunk)
            try:
                payload = json.loads(
                    b"".join(chunks).decode(response.encoding or "utf-8-sig")
                )
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                # collect_source already persists exc.http_status on failure.
                # A successful HTTP response can still contain an invalid body;
                # retain safe transport metadata without logging the body.
                exc.http_status = status
                exc.source_key = source_key
                exc.response_content_type = str(
                    response.headers.get("Content-Type") or "unknown"
                )[:128]
                exc.response_content_length = str(content_length or "unknown")[:64]
                exc.response_bytes_received = total_bytes
                raise
            return payload, status
        finally:
            if response is not None:
                response.close()

    raise RuntimeError("source JSON retry loop exited without a response")


def _fetch_source_csv_rows(
    source_key: str,
    endpoint_url: str,
    *,
    request_headers: dict[str, str],
) -> tuple[list[dict[str, Any]], int]:
    """Fetch one bounded official CSV batch, retrying transient HTTP 5xx."""

    for attempt in range(1, SOURCE_HTTP_SERVER_ERROR_ATTEMPTS + 1):
        response = None
        status: int | None = None
        content_length: str | None = None
        total_bytes = 0
        try:
            response = requests.get(
                endpoint_url,
                headers=request_headers,
                timeout=60,
                allow_redirects=False,
                stream=True,
            )
            status = int(response.status_code)
            if 500 <= status < 600 and attempt < SOURCE_HTTP_SERVER_ERROR_ATTEMPTS:
                response.close()
                response = None
                print(
                    "[source_http_retry] "
                    f"source_key={source_key} attempt={attempt}/"
                    f"{SOURCE_HTTP_SERVER_ERROR_ATTEMPTS} status={status}"
                )
                time.sleep(min(attempt, 2))
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                exc.http_status = status
                raise
            if 300 <= status < 400:
                raise ValueError("source endpoint returned an unexpected redirect")
            content_type = (
                response.headers.get("Content-Type", "")
                .split(";", 1)[0]
                .strip()
                .lower()
            )
            allowed_content_types = {"text/csv"}
            if source_key == "taifex_futures_large_trader_positions_daily":
                # TAIFEX has served this OpenAPI operation as either JSON or
                # CSV, even when Accept requests one representation. Some
                # responses use application/octet-stream without a filename;
                # permit that media type only for this allowlisted feed, then
                # require the strict JSON/CSV schema below.
                allowed_content_types.add("application/json")
                allowed_content_types.add("application/octet-stream")
            if source_key in {
                "tpex_daytrade_eligibility_daily",
                "tpex_quarterly_forecast_variance_events",
                "tpex_foreign_investor_holdings_daily",
            }:
                allowed_content_types.add("application/csv")
            if content_type not in allowed_content_types:
                raise ValueError(
                    f"source CSV endpoint returned unexpected content type: {content_type}"
                )
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > MAX_RESPONSE_BYTES:
                raise ValueError("source response exceeds size limit")
            chunks: list[bytes] = []
            total_bytes = 0
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total_bytes += len(chunk)
                if total_bytes > MAX_RESPONSE_BYTES:
                    raise ValueError("source response exceeds size limit")
                chunks.append(chunk)

            body = b"".join(chunks)
            decoded_body = body.decode("utf-8-sig")
            taifex_payload_is_json = (
                source_key == "taifex_futures_large_trader_positions_daily"
                and decoded_body.lstrip().startswith(("[", "{"))
            )
            if (
                taifex_payload_is_json
            ):
                payload = json.loads(decoded_body)
                if not isinstance(payload, list) or any(
                    not isinstance(row, dict) for row in payload
                ):
                    raise ValueError("TAIFEX JSON payload is not an array of objects")
                if any(
                    not TAIFEX_LARGE_TRADER_JSON_REQUIRED_COLUMNS.issubset(row)
                    for row in payload
                ):
                    raise ValueError("TAIFEX JSON fields do not match the approved contract")
                rows = payload
            else:
                # This official CSV carries a UTF-8 BOM, while requests guesses
                # ISO-8859-1 for its text/csv response without a charset.
                reader = csv.DictReader(io.StringIO(decoded_body, newline=""))
                required_columns = CSV_REQUIRED_COLUMNS_BY_SOURCE.get(source_key)
                if required_columns is None:
                    raise ValueError("source CSV key is not allowlisted")
                if not reader.fieldnames or not required_columns.issubset(reader.fieldnames):
                    raise ValueError("source CSV header does not match the approved contract")
                rows = list(reader)
            if len(rows) > MAX_SOURCE_ROWS:
                raise ValueError("source row count exceeds limit")
            return rows, status
        except Exception as exc:
            # Keep bounded transport diagnostics for CSV contract/decoding
            # failures, just as the JSON path does, without logging the body.
            if response is not None and status is not None:
                exc.http_status = status
                exc.source_key = source_key
                exc.response_content_type = str(
                    response.headers.get("Content-Type") or "unknown"
                )[:128]
                exc.response_content_length = str(content_length or "unknown")[:64]
                exc.response_bytes_received = total_bytes
            raise
        finally:
            if response is not None:
                response.close()

    raise RuntimeError("source CSV retry loop exited without a response")


# The registry is metadata-driven, but the network boundary must remain
# code-controlled.  A DB row may select one of these contracts only.
SOURCE_CONTRACTS = {
    "twse_trading_status_daily": (
        "twse_json", "https://openapi.twse.com.tw/v1/exchangeReport/TWT85U"
    ),
    "tpex_trading_status_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_cmode"
    ),
    "twse_company_events": ("twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap04_L"),
    "tpex_company_events": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O"
    ),
    "twse_insider_transfer_planned": (
        "twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap12_L"
    ),
    "twse_insider_transfer_untransferred": (
        "twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap13_L"
    ),
    "tpex_insider_transfer_planned": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap12_O"
    ),
    "tpex_insider_transfer_untransferred": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap13_O"
    ),
    "twse_monthly_revenue": ("twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"),
    "tpex_monthly_revenue": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
    ),
    "twse_dividends": ("twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap45_L"),
    "tpex_dividends": (
        "tpex_csv", "https://mopsfin.twse.com.tw/opendata/t187ap45_O.csv"
    ),
    "twse_exright_preannouncement_daily": (
        "twse_json", "https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL"
    ),
    "tpex_exright_preannouncement_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_exright_prepost"
    ),
    "twse_valuation_metrics_daily": (
        "twse_json", "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
    ),
    "tpex_valuation_metrics_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_peratio_analysis"
    ),
    "tpex_institutional_flow_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading"
    ),
    "twse_major_shareholders_daily": (
        "twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap02_L"
    ),
    "tpex_major_shareholders_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap02_O"
    ),
    "twse_director_shareholding_shortfall_daily": (
        "twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap08_L"
    ),
    "tpex_director_shareholding_shortfall_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap08_O"
    ),
    "tdcc_shareholder_distribution_weekly": (
        "twse_json", "https://openapi.tdcc.com.tw/v1/opendata/1-5"
    ),
    "twse_tpex_sbl_available_daily": ("twse_json", "https://openapi.twse.com.tw/v1/SBL/TWT96U"),
    "tpex_securities_borrowing_daily": ("tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_margin_sbl"),
    "twse_market_index_daily": ("twse_json", "https://openapi.twse.com.tw/v1/indicesReport/MI_5MINS_HIST"),
    "tpex_market_index_daily": ("tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_index"),
    "twse_market_total_return_index_daily": (
        "twse_json", "https://openapi.twse.com.tw/v1/indicesReport/MFI94U"
    ),
    "tpex_market_total_return_index_daily": (
        "tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_reward_index"
    ),
    "tpex_daytrade_market_summary_daily": (
        "tpex_json",
        "https://www.tpex.org.tw/openapi/v1/tpex_intraday_trading_statistics",
    ),
    "tpex_daytrade_eligibility_daily": (
        "tpex_csv",
        "https://www.tpex.org.tw/web/stock/trading/intraday_trading/intraday_trading_list_result.php?l=zh-tw&o=data",
    ),
    "tpex_foreign_investor_holdings_daily": (
        "tpex_csv",
        "https://www.tpex.org.tw/web/stock/3insti/qfii/qfii_result.php?l=zh-tw&o=data",
    ),
    "twse_daytrade_eligibility_daily": (
        "twse_json",
        "https://openapi.twse.com.tw/v1/exchangeReport/TWTB4U",
    ),
    "taifex_index_futures_positions_daily": (
        "taifex_json",
        "https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate",
    ),
    "taifex_index_options_positions_daily": (
        "taifex_json",
        "https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate",
    ),
    "taifex_futures_large_trader_positions_daily": (
        "taifex_csv",
        "https://openapi.taifex.com.tw/v1/OpenInterestOfLargeTradersFutures",
    ),
    "twse_equity_quotes_daily": (
        "twse_json",
        "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
    ),
    "tpex_equity_quotes_daily": (
        "tpex_json",
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
    ),
    "twse_margin_balance_daily": ("twse_json", "https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN"),
    "tpex_margin_balance_daily": ("tpex_json", "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"),
    "twse_trading_attention_daily": ("twse_json", "https://openapi.twse.com.tw/v1/announcement/notice"),
    "twse_trading_disposition_daily": ("twse_json", "https://openapi.twse.com.tw/v1/announcement/punish"),
    "tpex_trading_attention_daily": (
        "tpex_json",
        "https://www.tpex.org.tw/openapi/v1/tpex_trading_warning_information",
    ),
    "tpex_trading_disposition_daily": (
        "tpex_json",
        "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information",
    ),
    "twse_quarterly_forecast_attainment": (
        "twse_json",
        "https://openapi.twse.com.tw/v1/opendata/t187ap15_L",
    ),
    "twse_quarterly_forecast_variance": (
        "twse_json",
        "https://openapi.twse.com.tw/v1/opendata/t187ap16_L",
    ),
    "tpex_quarterly_forecast_variance_events": (
        "tpex_csv",
        "https://www.tpex.org.tw/www/zh-tw/api/mopsfin?response=csv&type=t187ap16_O",
    ),
    "twse_director_holdings_monthly": ("twse_json", "https://openapi.twse.com.tw/v1/opendata/t187ap11_L"),
    "tpex_director_holdings_monthly": ("tpex_json", "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap11_O"),
    "cbc_fx_daily": ("cbc_api_json", "https://cpx.cbc.gov.tw/API/DataAPI/Get?FileName=BP01D01"),
}
SOURCE_CONTRACTS.update(
    {
        "twse_quarterly_profitability": (
            "twse_json",
            "https://openapi.twse.com.tw/v1/opendata/t187ap17_L",
        ),
        "tpex_quarterly_profitability": (
            "tpex_json",
            "https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O",
        ),
    }
)
for _sector in ("basi", "bd", "ci", "fh", "ins", "mim"):
    SOURCE_CONTRACTS[f"twse_balance_sheet_{_sector}"] = (
        "twse_json",
        f"https://openapi.twse.com.tw/v1/opendata/t187ap07_L_{_sector}",
    )
    SOURCE_CONTRACTS[f"twse_income_statement_{_sector}"] = (
        "twse_json",
        f"https://openapi.twse.com.tw/v1/opendata/t187ap06_L_{_sector}",
    )
for _sector in ("bd", "ci"):
    SOURCE_CONTRACTS[f"tpex_balance_sheet_{_sector}"] = (
        "tpex_json",
        f"https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_{_sector}",
    )
    SOURCE_CONTRACTS[f"tpex_income_statement_{_sector}"] = (
        "tpex_json",
        f"https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_{_sector}",
    )
ALLOWED_SOURCE_HOSTS = frozenset(
    {
        "openapi.twse.com.tw", "www.tpex.org.tw", "mopsfin.twse.com.tw",
        "www.twse.com.tw", "cpx.cbc.gov.tw", "openapi.taifex.com.tw",
        "openapi.tdcc.com.tw",
    }
)


def _first_value(record: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = record.get(name)
        if value is not None and str(value).strip() != "":
            return value
    return None


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "--", "N/A", "NA", "null", "None"}:
        return None
    text = text.replace("%", "")
    try:
        parsed = float(text)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _invalid_numeric_value(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if text in {"", "-", "--", "N/A", "NA", "null", "None"}:
        return False
    return parse_number(value) is None


def parse_roc_date(value: Any) -> dt.date | None:
    """Parse Taiwan ROC dates such as 1150805, 1150629 or 20260805."""

    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    try:
        if len(digits) == 8 and int(digits[:4]) >= 1900:
            year, month, day = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
        elif len(digits) == 7:
            year, month, day = int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7])
        elif len(digits) == 6:
            year, month, day = int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:6])
        else:
            return None
        return dt.date(year, month, day)
    except (TypeError, ValueError):
        return None


def parse_roc_datetime(date_value: Any, time_value: Any) -> dt.datetime | None:
    date_value = parse_roc_date(date_value)
    if date_value is None:
        return None
    digits = re.sub(r"\D", "", str(time_value or "")).zfill(6)[-6:]
    try:
        return dt.datetime.combine(
            date_value,
            dt.time(int(digits[:2]), int(digits[2:4]), int(digits[4:6])),
        )
    except (TypeError, ValueError):
        return dt.datetime.combine(date_value, dt.time.min)


def parse_roc_month(value: Any) -> dt.date | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    try:
        if len(digits) == 6 and int(digits[:4]) >= 1900:
            year, month = int(digits[:4]), int(digits[4:6])
        elif len(digits) >= 5:
            year, month = int(digits[:-2]) + 1911, int(digits[-2:])
        else:
            return None
        return dt.date(year, month, 1)
    except (TypeError, ValueError):
        return None


def parse_roc_quarter(year_value: Any, quarter_value: Any) -> tuple[int, int, dt.date] | None:
    """Return Gregorian year, quarter, and quarter-end for ROC report periods."""

    try:
        year_digits = re.sub(r"\D", "", str(year_value or ""))
        quarter_digits = re.sub(r"\D", "", str(quarter_value or ""))
        year = int(year_digits)
        quarter = int(quarter_digits)
        if year < 1911:
            year += 1911
        if quarter not in {1, 2, 3, 4}:
            return None
        month = quarter * 3
        period_end = dt.date(year, month, calendar.monthrange(year, month)[1])
        return year, quarter, period_end
    except (TypeError, ValueError):
        return None


def _record_hash(source_key: str, record_key: str) -> str:
    return hashlib.sha256(f"{source_key}|{record_key}".encode("utf-8")).hexdigest()


def _payload_text(record: Any) -> str:
    payload = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
    if len(payload.encode("utf-8")) > MAX_RECORD_PAYLOAD_BYTES:
        raise ValueError("normalized record payload exceeds size limit")
    return payload


def _bounded_text(value: Any, limit: int = MAX_TEXT_FIELD_BYTES) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text.encode("utf-8")) > limit:
        raise ValueError("normalized text field exceeds size limit")
    return text


def validate_source_contract(source: dict[str, Any]) -> bool:
    expected = SOURCE_CONTRACTS.get(source.get("source_key"))
    endpoint = source.get("endpoint_url")
    if not expected or source.get("adapter_key") != expected[0] or endpoint != expected[1]:
        return False
    parsed = urlparse(str(endpoint))
    try:
        has_nonstandard_port = parsed.port is not None
    except ValueError:
        return False
    expected_fallbacks = {
        "twse_equity_quotes_daily": (
            TWSE_DAILY_QUOTES_FALLBACK_URL,
            "/exchangeReport/STOCK_DAY_ALL",
            "11549",
        ),
        "twse_market_index_daily": (
            TWSE_DAILY_INDEX_FALLBACK_URL,
            "/indicesReport/MI_5MINS_HIST",
            "11755",
        ),
        "twse_market_total_return_index_daily": (
            TWSE_TOTAL_RETURN_FALLBACK_URL,
            "/indicesReport/MFI94U",
            "11871",
        ),
    }
    source_key = source.get("source_key")
    if source_key in expected_fallbacks:
        fallback_url, expected_path, expected_dataset_id = expected_fallbacks[source_key]
        fallback = (source.get("config") or {}).get("same_day_fallback") or {}
        if (
            fallback.get("endpoint_url") != fallback_url
            or str(fallback.get("dataset_id") or "") != expected_dataset_id
        ):
            return False
        fallback_parts = urlparse(fallback_url)
        try:
            fallback_has_nonstandard_port = fallback_parts.port is not None
        except ValueError:
            return False
        if (
            fallback_parts.scheme != "https"
            or fallback_parts.hostname not in ALLOWED_SOURCE_HOSTS
            or fallback_parts.path != expected_path
            or fallback_parts.query != "response=open_data"
            or fallback_parts.username is not None
            or fallback_parts.password is not None
            or fallback_has_nonstandard_port
        ):
            return False
    return (
        parsed.scheme == "https"
        and parsed.hostname in ALLOWED_SOURCE_HOSTS
        and parsed.username is None
        and parsed.password is None
        and not has_nonstandard_port
    )


def _common_record(
    source: dict[str, Any],
    *,
    record_key: str,
    entity_id: str | None,
    event_type: str,
    event_time: dt.datetime | None,
    effective_date: dt.date | None,
    title: str | None,
    metric_name: str | None,
    metric_value: float | None,
    value_text: str | None,
    unit: str | None,
    currency: str | None,
    raw_record: Any,
    quality_status: str = "ok",
) -> dict[str, Any]:
    return {
        "source_key": source["source_key"],
        "record_key": record_key,
        "record_hash": _record_hash(source["source_key"], record_key),
        "target_domain": source["target_domain"],
        "entity_type": source.get("entity_type", source["target_domain"]),
        "entity_id": entity_id,
        "event_type": event_type,
        "event_time": event_time,
        "effective_date": effective_date,
        "title": _bounded_text(str(title).strip(), 512) if title is not None else None,
        "metric_name": metric_name,
        "metric_value": metric_value,
        "value_text": _bounded_text(value_text),
        "unit": unit,
        "currency": currency,
        "source_url": source["endpoint_url"],
        "payload": _payload_text(raw_record),
        "_raw_record": raw_record,
        "quality_status": quality_status,
    }


def _normalize_quarterly_financial_record(
    source: dict[str, Any], raw: dict[str, Any]
) -> dict[str, Any] | None:
    config = source["config"]
    market = config["market"]
    code = _first_value(raw, "公司代號", "SecuritiesCompanyCode", "證券代號")
    if code is None or not str(code).strip():
        # Some TPEx industry endpoints return one empty sentinel object.
        return None
    name = _first_value(raw, "公司名稱", "CompanyName", "證券名稱")
    period = parse_roc_quarter(
        _first_value(raw, "年度", "Year"),
        _first_value(raw, "季別", "Season", "季度"),
    )
    if period is None:
        return None
    year, quarter, period_end = period
    published_date = parse_roc_date(_first_value(raw, "出表日期", "Date"))
    safe_record = dict(raw)
    safe_record.update(
        {
            "__company_code": str(code).strip(),
            "__company_name": str(name).strip() if name is not None else None,
            "__reporting_year": year,
            "__reporting_quarter": quarter,
            "__report_period": period_end,
            "__published_date": published_date,
        }
    )

    if config["kind"] == "quarterly_profitability":
        revenue = parse_number(
            _first_value(raw, "營業收入(百萬元)", "營業收入百萬元")
        )
        gross_margin = parse_number(
            _first_value(raw, "毛利率(%)(營業毛利)/(營業收入)", "毛利率")
        )
        operating_margin = parse_number(
            _first_value(raw, "營業利益率(%)(營業利益)/(營業收入)", "營業利益率")
        )
        pretax_margin = parse_number(
            _first_value(raw, "稅前純益率(%)(稅前純益)/(營業收入)", "稅前純益率")
        )
        net_margin = parse_number(
            _first_value(raw, "稅後純益率(%)(稅後純益)/(營業收入)", "稅後純益率")
        )
        safe_record.update(
            {
                "__revenue_millions": revenue,
                "__gross_margin_pct": gross_margin,
                "__operating_margin_pct": operating_margin,
                "__pretax_margin_pct": pretax_margin,
                "__net_margin_pct": net_margin,
            }
        )
        return _common_record(
            source,
            record_key=f"{market}:profitability:{code}:{year}Q{quarter}",
            entity_id=str(code).strip(),
            event_type="quarterly_profitability",
            event_time=(
                dt.datetime.combine(published_date, dt.time.min)
                if published_date
                else None
            ),
            effective_date=period_end,
            title=str(name or ""),
            metric_name="operating_profit_margin_pct",
            metric_value=operating_margin,
            value_text=None,
            unit="percent",
            currency="TWD",
            raw_record=safe_record,
            quality_status=(
                "ok" if operating_margin is not None else "quarantine"
            ),
        )

    if config["kind"] == "quarterly_income_statement":
        basic_eps = parse_number(_first_value(raw, "基本每股盈餘（元）", "基本每股盈餘"))
        parent_net_income = parse_number(
            _first_value(
                raw,
                "淨利（淨損）歸屬於母公司業主",
                "本期淨利（淨損）歸屬於母公司業主",
            )
        )
        reported_revenue = parse_number(_first_value(raw, "營業收入"))
        safe_record.update(
            {
                "__basic_eps": basic_eps,
                "__parent_net_income_reported": parent_net_income,
                "__reported_revenue": reported_revenue,
                "__industry_type": config["industry_type"],
            }
        )
        industry = config["industry_type"]
        return _common_record(
            source,
            record_key=(
                f"{market}:{industry}:income_statement:{code}:{year}Q{quarter}"
            ),
            entity_id=str(code).strip(),
            event_type="quarterly_income_statement",
            event_time=(
                dt.datetime.combine(published_date, dt.time.min)
                if published_date
                else None
            ),
            effective_date=period_end,
            title=str(name or ""),
            metric_name="basic_eps_ntd_per_share",
            metric_value=basic_eps,
            value_text=None,
            unit="TWD_per_share",
            currency="TWD",
            raw_record=safe_record,
            quality_status="ok" if basic_eps is not None else "quarantine",
        )

    if config["kind"] == "quarterly_balance_sheet":
        assets = parse_number(_first_value(raw, "資產總計", "資產總額"))
        liabilities = parse_number(_first_value(raw, "負債總計", "負債總額"))
        equity = parse_number(_first_value(raw, "權益總計", "權益總額"))
        current_assets = parse_number(_first_value(raw, "流動資產"))
        current_liabilities = parse_number(_first_value(raw, "流動負債"))
        safe_record.update(
            {
                "__total_assets": assets,
                "__total_liabilities": liabilities,
                "__total_equity": equity,
                "__current_assets": current_assets,
                "__current_liabilities": current_liabilities,
            }
        )
        industry = config["industry_type"]
        return _common_record(
            source,
            record_key=f"{market}:{industry}:balance_sheet:{code}:{year}Q{quarter}",
            entity_id=str(code).strip(),
            event_type="quarterly_balance_sheet",
            event_time=(
                dt.datetime.combine(published_date, dt.time.min)
                if published_date
                else None
            ),
            effective_date=period_end,
            title=str(name or ""),
            metric_name="total_assets",
            metric_value=assets,
            value_text=None,
            unit="thousand_TWD",
            currency="TWD",
            raw_record=safe_record,
            quality_status="ok" if assets is not None else "quarantine",
        )

    raise ValueError(f"Unsupported quarterly financial kind: {config['kind']}")


def _normalize_quarterly_forecast_record(
    source: dict[str, Any], raw: dict[str, Any]
) -> dict[str, Any] | None:
    """Retain official issuer forecast-versus-actual disclosures verbatim."""

    config = source["config"]
    kind = config["kind"]
    code = _first_value(raw, "公司代號")
    period = parse_roc_quarter(
        _first_value(raw, "年度"), _first_value(raw, "季別")
    )
    published_date = parse_roc_date(_first_value(raw, "出表日期"))
    if not code or not period:
        return None

    year, quarter, period_end = period
    company_name = _first_value(raw, "公司名稱")
    forecast_sequence = str(_first_value(raw, "財測序號") or "")
    coverage_period = _first_value(raw, "涵蓋期間")
    if kind == "quarterly_forecast_attainment":
        fields = {
            "published_date": published_date,
            "reporting_year": year,
            "reporting_quarter": quarter,
            "report_period_end": period_end,
            "company_code": str(code).strip(),
            "company_name": str(company_name).strip() if company_name else None,
            "forecast_sequence": forecast_sequence or None,
            "coverage_period": coverage_period,
            "reported_actual": _first_value(
                raw, "截至該季經會計師查核或核閱數"
            ),
            "reported_forecast": _first_value(
                raw, "截至該季綜合損益預測數"
            ),
        }
        fields["reported_actual_value"] = parse_number(fields["reported_actual"])
        event_type = "company_forecast_attainment_observation"
        metric_name = "reported_actual_comprehensive_income"
        metric_value = fields["reported_actual_value"]
        comparison_pairs = [(fields["reported_actual"], fields["reported_forecast"])]
    elif kind == "quarterly_forecast_variance":
        fields = {
            "published_date": published_date,
            "reporting_year": year,
            "reporting_quarter": quarter,
            "report_period_end": period_end,
            "company_code": str(code).strip(),
            "company_name": str(company_name).strip() if company_name else None,
            "forecast_sequence": forecast_sequence or None,
            "coverage_period": coverage_period,
            "quarter_actual": _first_value(raw, "當季經會計師查核或核閱數"),
            "cumulative_actual": _first_value(raw, "截至當季經會計師查核或核閱數"),
            "quarter_forecast": _first_value(raw, "當季稅前(綜合)損益預測數"),
            "cumulative_forecast": _first_value(raw, "截至當季稅前(綜合)損益預測數"),
        }
        fields["quarter_actual_value"] = parse_number(fields["quarter_actual"])
        fields["cumulative_actual_value"] = parse_number(fields["cumulative_actual"])
        event_type = "company_forecast_variance"
        metric_name = "quarter_actual_pretax_or_comprehensive_income"
        metric_value = parse_number(fields["quarter_actual"])
        comparison_pairs = [
            (fields["quarter_actual"], fields["quarter_forecast"]),
            (fields["cumulative_actual"], fields["cumulative_forecast"]),
        ]
    else:
        raise ValueError(f"Unsupported quarterly forecast kind: {kind}")

    def has_reported_value(value: Any) -> bool:
        text = "" if value is None else str(value).strip()
        return text not in {"", "-", "--", "---", "N/A", "NA", "null", "None"}

    safe_record = dict(raw)
    safe_record.update({f"__{key}": value for key, value in fields.items()})
    safe_record["市場別"] = "上市"
    version_fingerprint = hashlib.sha256(
        json.dumps(
            fields, ensure_ascii=False, sort_keys=True, default=str,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return _common_record(
        source,
        record_key=(
            f"TWSE:{kind}:{str(code).strip()}:{year}Q{quarter}:"
            f"{forecast_sequence or 'unknown'}:{version_fingerprint}"
        ),
        entity_id=str(code).strip(),
        event_type=event_type,
        event_time=(
            dt.datetime.combine(published_date, dt.time.min)
            if published_date
            else None
        ),
        effective_date=period_end,
        title=str(company_name or code).strip(),
        metric_name=metric_name,
        metric_value=metric_value,
        value_text=json.dumps(fields, ensure_ascii=False, separators=(",", ":"), default=str),
        unit=None,
        currency=None,
        raw_record=safe_record,
        quality_status=(
            "ok"
            if published_date and any(
                has_reported_value(actual) and has_reported_value(forecast)
                for actual, forecast in comparison_pairs
            )
            else "quarantine"
        ),
    )


def _normalize_tpex_forecast_variance_event(
    source: dict[str, Any], raw: dict[str, Any]
) -> dict[str, Any] | None:
    """Normalize TPEx's official threshold-event roster without inventing amounts."""

    snapshot_date_raw = _first_value(raw, "出表日期")
    year_raw = _first_value(raw, "年度")
    quarter_raw = _first_value(raw, "季別")
    code_raw = _first_value(raw, "公司代號")
    name_raw = _first_value(raw, "公司名稱")
    # Keep the dated blank sentinel as snapshot metadata so natural-run audits
    # can prove the feed was refreshed without creating a security/zero row.
    if not any((year_raw, quarter_raw, code_raw, name_raw)):
        snapshot_date = parse_roc_date(snapshot_date_raw)
        safe_record = {
            **raw,
            "市場別": "上櫃",
            "__snapshot_date": snapshot_date,
        }
        fingerprint = hashlib.sha256(
            _payload_text(safe_record).encode("utf-8")
        ).hexdigest()[:16]
        snapshot = _common_record(
            source,
            record_key=(
                f"TPEx:forecast_variance_threshold:snapshot:"
                f"{snapshot_date.isoformat() if snapshot_date else fingerprint}"
            ),
            entity_id=None,
            event_type="dataset_snapshot_observation",
            event_time=None,
            effective_date=snapshot_date,
            title="TPEx 財測差異清單批次快照",
            metric_name="official_dataset_snapshot_observation",
            metric_value=None,
            value_text=json.dumps(
                {
                    "snapshot_date": snapshot_date,
                    "stock_event_rows": None,
                    "interpretation": "blank_sentinel_no_security_count_inferred",
                },
                ensure_ascii=False, separators=(",", ":"), default=str,
            ),
            unit=None,
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if snapshot_date else "quarantine",
        )
        snapshot["entity_type"] = "source_snapshot"
        return snapshot

    snapshot_date = parse_roc_date(snapshot_date_raw)
    period = parse_roc_quarter(year_raw, quarter_raw)
    code = str(code_raw).strip() if code_raw is not None else ""
    company_name = str(name_raw).strip() if name_raw is not None else ""
    year, quarter, period_end = period if period else (None, None, None)
    fields = {
        "snapshot_date": snapshot_date,
        "reporting_year": year,
        "reporting_quarter": quarter,
        "report_period_end": period_end,
        "company_code": code or None,
        "company_name": company_name or None,
        "threshold_basis": (
            "當季差異達10%以上，或截至當季累計差異達20%以上"
        ),
    }
    safe_record = {**raw, "市場別": "上櫃"}
    safe_record.update({f"__{key}": value for key, value in fields.items()})
    if code and period and snapshot_date:
        record_key = (
            f"TPEx:forecast_variance_threshold:{code}:"
            f"{year}Q{quarter}:{snapshot_date.isoformat()}"
        )
    else:
        fingerprint = hashlib.sha256(
            _payload_text(safe_record).encode("utf-8")
        ).hexdigest()[:16]
        record_key = f"TPEx:forecast_variance_threshold:invalid:{fingerprint}"

    return _common_record(
        source,
        record_key=record_key,
        entity_id=code or None,
        event_type="company_forecast_variance_threshold",
        # The CSV's 出表日期 is a snapshot date, not the issuer's event time.
        event_time=None,
        effective_date=period_end,
        title="TPEx 上櫃公司財測差異門檻名單列",
        metric_name="official_forecast_variance_threshold_event",
        metric_value=None,
        value_text=json.dumps(
            fields, ensure_ascii=False, separators=(",", ":"), default=str
        ),
        unit=None,
        currency=None,
        raw_record=safe_record,
        quality_status=(
            "ok" if code and period and snapshot_date else "quarantine"
        ),
    )


def _resolve_record_key_collisions(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep distinct upstream rows when a source's natural key is incomplete."""

    groups: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for record in records:
        fingerprint = hashlib.sha256(
            _payload_text(record.get("_raw_record") or {}).encode("utf-8")
        ).hexdigest()
        groups.setdefault(record["record_key"], {}).setdefault(fingerprint, []).append(record)

    for record_key, variants in groups.items():
        if len(variants) <= 1:
            continue
        for fingerprint, variant_records in variants.items():
            for record in variant_records:
                record["record_key"] = f"{record_key}:payload:{fingerprint[:16]}"
                record["record_hash"] = _record_hash(
                    record["source_key"], record["record_key"]
                )
    return records


def _normalize_taifex_position_record(
    source: dict[str, Any], raw: dict[str, Any],
) -> dict[str, Any] | None:
    """Normalize official index-futures positioning without assigning direction."""

    config = source["config"]
    if config.get("kind") == "taifex_futures_large_trader_oi":
        return _normalize_taifex_large_trader_record(source, raw)

    effective_date = parse_roc_date(_first_value(raw, "Date", "日期"))
    contract_code = _first_value(raw, "ContractCode")
    institution_type = _first_value(raw, "Item")
    if not contract_code or str(contract_code).strip() not in set(
        config.get("contract_codes") or ()
    ):
        return None

    contract_code = str(contract_code).strip()
    institution_type = str(institution_type or "").strip()
    call_put = None
    if config.get("kind") == "taifex_index_option_position":
        call_put = str(_first_value(raw, "CallPut") or "").strip().upper()
        if call_put not in set(config.get("call_put_types") or ()):
            return None
    net_trading_lots = parse_number(_first_value(raw, "TradingVolume(Net)"))
    net_trading_value = parse_number(_first_value(raw, "TradingValue(Net)(Thousands)"))
    net_open_interest_lots = parse_number(_first_value(raw, "OpenInterest(Net)"))
    net_open_interest_value = parse_number(
        _first_value(raw, "ContractValueofOpenInterest(Net)(Thousands)")
    )
    details = {
        "contract_code": contract_code,
        "call_put": call_put,
        "institution_type": institution_type or None,
        "net_trading_lots": net_trading_lots,
        "net_trading_value_thousands": net_trading_value,
        "net_open_interest_lots": net_open_interest_lots,
        "net_open_interest_value_thousands": net_open_interest_value,
    }
    safe_record = {
        **raw,
        "__effective_date": effective_date,
        "__contract_code": contract_code,
        "__call_put": call_put,
        "__institution_type": institution_type or None,
        "__net_open_interest_lots": net_open_interest_lots,
    }
    return _common_record(
        source,
        record_key=(
            f"TAIFEX:{effective_date}:{contract_code}:"
            f"{call_put + ':' if call_put else ''}{institution_type or 'unknown'}"
        ),
        entity_id=(
            f"{contract_code}::{call_put}::{institution_type}"
            if call_put and institution_type
            else f"{contract_code}::{institution_type}" if institution_type else None
        ),
        event_type="market_institutional_positioning_daily",
        event_time=(
            dt.datetime.combine(effective_date, dt.time.min)
            if effective_date
            else None
        ),
        effective_date=effective_date,
        title=(
            f"{contract_code}｜{call_put + '｜' if call_put else ''}"
            f"{institution_type or '法人類別未提供'}"
        ),
        metric_name="net_open_interest_lots",
        metric_value=net_open_interest_lots,
        value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
        unit="contracts",
        currency=None,
        raw_record=safe_record,
        quality_status=(
            "ok"
            if effective_date and contract_code and institution_type
            and net_open_interest_lots is not None
            else "quarantine"
        ),
    )


def _normalize_taifex_large_trader_record(
    source: dict[str, Any], raw: dict[str, Any],
) -> dict[str, Any]:
    """Preserve TAIFEX large-trader buy/sell ranks as separate market aggregates."""

    effective_date = parse_roc_date(_first_value(raw, "Date", "日期"))
    contract = str(_first_value(raw, "Contract", "契約") or "").strip()
    contract_name = str(
        _first_value(raw, "ContractName", "商品名稱(契約名稱)") or ""
    ).strip()
    settlement_month = str(
        _first_value(raw, "SettlementMonth", "到期月份(週別)") or ""
    ).strip()
    raw_trader_type = _first_value(raw, "TypeOfTraders", "交易人類別")
    trader_type = str(raw_trader_type if raw_trader_type is not None else "").strip()
    top5_buy = parse_number(_first_value(raw, "Top5Buy", "前五大交易人買方數量"))
    top5_sell = parse_number(_first_value(raw, "Top5Sell", "前五大交易人賣方數量"))
    top10_buy = parse_number(_first_value(raw, "Top10Buy", "前十大交易人買方數量"))
    top10_sell = parse_number(_first_value(raw, "Top10Sell", "前十大交易人賣方數量"))
    market_open_interest = parse_number(
        _first_value(raw, "OIOfMarket", "全市場未沖銷部位數")
    )
    complete_key = bool(effective_date and contract and settlement_month and trader_type)
    details = {
        "contract": contract or None,
        "contract_name": contract_name or None,
        "settlement_month": settlement_month or None,
        "trader_type": trader_type or None,
        "top5_buy_lots": top5_buy,
        "top5_sell_lots": top5_sell,
        "top10_buy_lots": top10_buy,
        "top10_sell_lots": top10_sell,
        "market_open_interest_lots": market_open_interest,
    }
    safe_record = {
        **raw,
        "__effective_date": effective_date,
        "__contract": contract or None,
        "__settlement_month": settlement_month or None,
        "__trader_type": trader_type or None,
    }
    stable_key = (
        f"TAIFEX-LARGE:{effective_date}:{contract}:{settlement_month}:{trader_type}"
        if complete_key
        else "TAIFEX-LARGE:incomplete:" + hashlib.sha256(
            _payload_text(raw).encode("utf-8")
        ).hexdigest()
    )
    return _common_record(
        source,
        record_key=stable_key,
        entity_id=(
            f"{contract}::{settlement_month}::{trader_type}" if complete_key else None
        ),
        event_type="market_futures_large_trader_oi_daily",
        event_time=(
            dt.datetime.combine(effective_date, dt.time.min)
            if effective_date else None
        ),
        effective_date=effective_date,
        title=(
            f"{contract or '商品未提供'}｜到期別 {settlement_month or '未提供'}｜"
            f"交易人類別 {trader_type or '未提供'}"
        ),
        metric_name="market_open_interest_lots",
        metric_value=market_open_interest,
        value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
        unit="contracts",
        currency=None,
        raw_record=safe_record,
        quality_status=(
            "ok"
            if complete_key and all(
                value is not None and float(value).is_integer()
                for value in (
                    top5_buy, top5_sell, top10_buy, top10_sell,
                    market_open_interest,
                )
            )
            else "quarantine"
        ),
    )


def _normalize_market_record(
    source: dict[str, Any], raw: dict[str, Any]
) -> dict[str, Any] | None:
    config = source["config"]
    kind = config["kind"]
    market = config["market"]
    code = _first_value(raw, "公司代號", "SecuritiesCompanyCode", "證券代號")
    name = _first_value(raw, "公司名稱", "CompanyName", "證券名稱")

    if kind == "major_shareholders":
        effective_date = parse_roc_date(
            _first_value(raw, "出表日期", "Date", "資料日期")
        )
        code_text = str(
            _first_value(raw, "公司代號", "SecuritiesCompanyCode") or ""
        ).strip()
        company_name = str(
            _first_value(raw, "公司名稱", "CompanyName") or ""
        ).strip()
        holder_name = str(_first_value(raw, "大股東名稱") or "").strip()
        market_label = "上市" if market == "TWSE" else "上櫃"
        safe_record = {
            **raw,
            "市場別": market_label,
            "__entity_id": code_text or None,
            "__effective_date": effective_date,
            "__company_name": company_name or None,
            "__holder_name": holder_name or None,
        }
        valid = bool(effective_date and code_text and company_name and holder_name)
        record_key = (
            f"{market}:major_shareholder_over_10pct:"
            f"{effective_date}:{code_text}:{holder_name}"
            if effective_date and code_text and holder_name
            else "major_shareholder_over_10pct:incomplete:"
            + hashlib.sha256(_payload_text(raw).encode("utf-8")).hexdigest()
        )
        details = {
            "market": market_label,
            "holder_name": holder_name or None,
            "disclosure_scope": "reported_shareholders_over_10pct_only",
            "ownership_quantity_or_exact_ratio": "not_provided_by_source",
            "interpretation": "ownership_identity_context_only_until_snapshot_change_validation",
        }
        return _common_record(
            source,
            record_key=record_key,
            entity_id=code_text or None,
            event_type="major_shareholder_over_10pct",
            event_time=None,
            effective_date=effective_date,
            title=f"{market_label}大股東名單 {company_name} {holder_name}".strip(),
            metric_name=None,
            metric_value=None,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit=None,
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if valid else "quarantine",
        )

    if kind == "director_shareholding_shortfall":
        effective_date = parse_roc_date(_first_value(raw, "出表日期", "Date"))
        code_text = str(
            _first_value(raw, "公司代號", "SecuritiesCompanyCode") or ""
        ).strip()
        company_name = str(
            _first_value(raw, "公司名稱", "CompanyName") or ""
        ).strip()
        director_prefix = (
            "全體董事" if market == "TWSE" else "全體董事不包含獨立董事"
        )
        numeric_fields = {
            "issued_shares": _first_value(raw, "已發行股份總額"),
            "director_required_shares": _first_value(
                raw, f"{director_prefix}應持有股數"
            ),
            "director_actual_shares": _first_value(
                raw, f"{director_prefix}本人實際持有股數"
            ),
            "director_legal_representative_shares": _first_value(
                raw, f"{director_prefix}法人代表人分戶集保股數"
            ),
            "director_trust_shares": _first_value(
                raw, f"{director_prefix}保留運用決定權信託股數"
            ),
            "director_shortfall_shares": _first_value(
                raw, f"{director_prefix}不足股數"
            ),
            "supervisor_required_shares": _first_value(
                raw, "全體監察人應持有股數"
            ),
            "supervisor_actual_shares": _first_value(
                raw, "全體監察人本人實際持有股數"
            ),
            "supervisor_legal_representative_shares": _first_value(
                raw, "全體監察人法人代表人分戶集保股數"
            ),
            "supervisor_trust_shares": _first_value(
                raw, "全體監察人保留運用決定權信託股數"
            ),
            "supervisor_shortfall_shares": _first_value(
                raw, "全體監察人不足股數"
            ),
        }
        parsed_fields = {
            name: parse_number(value) for name, value in numeric_fields.items()
        }
        core_values = (
            parsed_fields["issued_shares"],
            parsed_fields["director_required_shares"],
            parsed_fields["director_actual_shares"],
            parsed_fields["director_shortfall_shares"],
        )
        all_numeric_values = tuple(numeric_fields.values())
        integer_values_valid = all(
            value is None or float(value).is_integer()
            for value in parsed_fields.values()
        )
        shortfall_confirmed = any(
            parsed_fields[key] is not None and parsed_fields[key] > 0
            for key in ("director_shortfall_shares", "supervisor_shortfall_shares")
        )
        valid = bool(
            effective_date
            and code_text.isdigit()
            and 4 <= len(code_text) <= 6
            and company_name
            and all(value is not None for value in core_values)
            and parsed_fields["issued_shares"] > 0
            and all(not _invalid_numeric_value(value) for value in all_numeric_values)
            and integer_values_valid
            and shortfall_confirmed
        )
        details = {
            "market": "上市" if market == "TWSE" else "上櫃",
            "company_code": code_text or None,
            "company_name": company_name or None,
            "effective_date": effective_date.isoformat() if effective_date else None,
            **parsed_fields,
            "notification_status": _first_value(raw, "持股不足已通知其董監"),
            "disclosure_scope": "currently_reported_statutory_shareholding_shortfalls_only",
            "absence_semantics": "unknown_not_zero_or_compliance",
            "interpretation": "governance_risk_context_only_not_directional_score",
        }
        safe_record = {
            **raw,
            "市場別": details["market"],
            "__entity_id": code_text or None,
            "__effective_date": effective_date,
            "__company_name": company_name or None,
        }
        return _common_record(
            source,
            record_key=(
                f"{market}:director_shareholding_shortfall:{effective_date}:{code_text}"
                if effective_date and code_text
                else "director_shareholding_shortfall:incomplete:"
                + hashlib.sha256(_payload_text(raw).encode("utf-8")).hexdigest()
            ),
            entity_id=code_text or None,
            event_type="statutory_director_shareholding_shortfall",
            event_time=None,
            effective_date=effective_date,
            title=f"{company_name or code_text} 董監持股不足法定成數",
            metric_name=None,
            metric_value=None,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit=None,
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if valid else "quarantine",
        )

    if kind == "shareholder_distribution":
        effective_date = parse_roc_date(_first_value(raw, "資料日期"))
        code_text = str(_first_value(raw, "證券代號") or "").strip()
        tier_value = parse_number(_first_value(raw, "持股分級"))
        holder_count = parse_number(_first_value(raw, "人數"))
        shares = parse_number(_first_value(raw, "股數"))
        custody_pct = parse_number(_first_value(raw, "占集保庫存數比例%"))
        integer_values = (tier_value, holder_count, shares)
        valid = bool(
            effective_date
            and code_text
            and all(value is not None and float(value).is_integer() for value in integer_values)
            and tier_value is not None
            and 1 <= tier_value <= 17
            and holder_count is not None
            and holder_count >= 0
            and shares is not None
            and shares >= 0
            and custody_pct is not None
            and 0 <= custody_pct <= 100
        )
        tier = (
            int(tier_value)
            if tier_value is not None and float(tier_value).is_integer()
            else None
        )
        safe_record = {
            **raw,
            "__entity_id": code_text or None,
            "__effective_date": effective_date,
            "__holding_tier": tier,
            "__holder_count": (
                int(holder_count)
                if holder_count is not None and float(holder_count).is_integer()
                else None
            ),
            "__shares": (
                int(shares)
                if shares is not None and float(shares).is_integer()
                else None
            ),
            "__custody_ratio_pct": custody_pct,
        }
        details = {
            "holding_tier": tier,
            "holder_count": safe_record["__holder_count"],
            "shares": safe_record["__shares"],
            "share_of_tdcc_custody_pct": custody_pct,
            "interpretation": "weekly_shareholder_distribution_context_only",
        }
        record_key = (
            f"TDCC:shareholder_distribution:{effective_date}:{code_text}:{tier}"
            if effective_date and code_text and tier is not None
            else "TDCC:shareholder_distribution:incomplete:"
            + hashlib.sha256(_payload_text(raw).encode("utf-8")).hexdigest()
        )
        return _common_record(
            source,
            record_key=record_key,
            entity_id=code_text or None,
            event_type="shareholder_distribution_weekly",
            event_time=None,
            effective_date=effective_date,
            title=f"TDCC 持股分級 {code_text} {tier or ''}".strip(),
            metric_name="share_of_tdcc_custody_pct_by_tier",
            metric_value=custody_pct,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="percent_of_tdcc_custody_balance",
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if valid else "quarantine",
        )

    if kind == "foreign_investor_holding":
        effective_date = parse_roc_date(_first_value(raw, "資料日期"))
        code_text = str(_first_value(raw, "代號") or "").strip()
        name_text = str(_first_value(raw, "名稱") or "").strip()
        rank = parse_number(_first_value(raw, "排行"))
        issued_shares = parse_number(_first_value(raw, "發行股數"))
        available_shares = parse_number(
            _first_value(raw, "僑外資及陸資尚可投資股數")
        )
        held_shares = parse_number(_first_value(raw, "僑外資及陸資持有股數"))
        available_pct = parse_number(
            _first_value(raw, "僑外資及陸資尚可投資比率")
        )
        holding_pct = parse_number(_first_value(raw, "僑外資及陸資持股比率"))
        legal_cap_pct = parse_number(_first_value(raw, "法令投資上限比率"))
        integer_values = (rank, issued_shares, available_shares, held_shares)
        valid = (
            effective_date is not None
            and bool(code_text)
            and bool(name_text)
            and rank is not None
            and rank >= 1
            and float(rank).is_integer()
            and issued_shares is not None
            and issued_shares > 0
            and available_shares is not None
            and available_shares >= 0
            and held_shares is not None
            and 0 <= held_shares <= issued_shares
            and all(value is not None and float(value).is_integer() for value in integer_values)
            and all(
                value is not None and 0 <= value <= 100
                for value in (available_pct, holding_pct, legal_cap_pct)
            )
            and abs((held_shares / issued_shares * 100) - holding_pct) <= 0.02
        )
        safe_record = {
            **raw,
            "__effective_date": effective_date,
            "__entity_id": code_text or None,
        }
        details = {
            "effective_date": effective_date.isoformat() if effective_date else None,
            "rank": int(rank) if rank is not None and float(rank).is_integer() else rank,
            "security_name": name_text or None,
            "issued_shares": int(issued_shares) if issued_shares is not None else None,
            "foreign_available_shares": (
                int(available_shares) if available_shares is not None else None
            ),
            "foreign_held_shares": int(held_shares) if held_shares is not None else None,
            "foreign_available_ratio_pct": available_pct,
            "foreign_holding_ratio_pct": holding_pct,
            "legal_investment_cap_pct": legal_cap_pct,
            "note": _first_value(raw, "備註"),
            "interpretation": "otc_foreign_ownership_snapshot_context_only",
        }
        stable_key = (
            f"TPEX-FOREIGN-HOLDING:{effective_date}:{code_text}"
            if effective_date and code_text
            else "TPEX-FOREIGN-HOLDING:incomplete:" + hashlib.sha256(
                _payload_text(raw).encode("utf-8")
            ).hexdigest()
        )
        return _common_record(
            source,
            record_key=stable_key,
            entity_id=code_text or None,
            event_type="foreign_ownership_daily",
            event_time=None,
            effective_date=effective_date,
            title=f"TPEx 外資及陸資持股比例排行列 {code_text} {name_text}".strip(),
            metric_name="foreign_holding_ratio_pct",
            metric_value=holding_pct,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="percent_of_issued_shares",
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if valid else "quarantine",
        )

    if kind in {"market_index", "market_total_return_index"}:
        date_raw = _first_value(raw, "Date", "日期", "資料日期", "日　期")
        effective_date = parse_roc_date(date_raw)
        if kind == "market_total_return_index":
            closing_value = _first_value(
                raw,
                str(config["value_field"]),
                "發行量加權股價報酬指數",
            )
        else:
            closing_value = _first_value(raw, "ClosingIndex", "Close", "收盤指數", "收市")
        closing_index = parse_number(closing_value)
        index_code = str(config["index_code"])
        if (
            not effective_date or closing_index is None
            or (kind == "market_total_return_index" and closing_index <= 0)
        ):
            return None
        total_return = kind == "market_total_return_index"
        return _common_record(
            source,
            record_key=f"{index_code}:{effective_date}",
            entity_id=index_code,
            event_type=(
                "market_total_return_index_daily" if total_return else "market_index_daily"
            ),
            event_time=dt.datetime.combine(effective_date, dt.time.min),
            effective_date=effective_date,
            title=str(config.get("index_name") or index_code),
            metric_name="total_return_index" if total_return else "closing_index",
            metric_value=closing_index,
            value_text=None,
            unit="index_points",
            currency=None,
            raw_record=raw,
        )

    if kind == "corporate_action_preannouncement":
        market_code = str(market or "").strip().upper()
        market_label = "上市" if market_code == "TWSE" else "上櫃"
        effective_date = parse_roc_date(
            _first_value(
                raw,
                "Date",
                "ExRrightsExDividendDate",
                "除權息日期",
            )
        )
        code = _first_value(
            raw,
            "Code",
            "SecuritiesCompanyCode",
            "股票代號",
            "代號",
        )
        name = _first_value(raw, "Name", "CompanyName", "名稱")
        action = _first_value(
            raw,
            "Exdividend",
            "ExRrightsExDividend",
            "除權息",
        )
        cash_dividend = parse_number(
            _first_value(raw, "CashDividend", "CashDivdend", "現金股利")
        )
        stock_dividend_ratio = parse_number(
            _first_value(raw, "StockDividendRatio", "StockDividend", "無償配股率")
        )
        subscription_ratio = parse_number(
            _first_value(
                raw,
                "SubscriptionRatio",
                "SubscriptionRatioToNewSharesIssued",
                "現金增資配股率",
            )
        )
        subscription_price = parse_number(
            _first_value(
                raw,
                "SubscriptionPricePerShare",
                "現金增資認購價",
            )
        )
        code_text = str(code).strip() if code is not None else ""
        action_text = str(action).strip() if action is not None else ""
        name_text = str(name).strip() if name is not None else ""
        quality_status = (
            "ok"
            if market_code in {"TWSE", "TPEX"}
            and effective_date
            and code_text
            and action_text
            else "quarantine"
        )
        details = {
            "market": market_label,
            "action": action_text or None,
            "cash_dividend_per_share": cash_dividend,
            "stock_dividend_ratio": stock_dividend_ratio,
            "subscription_ratio": subscription_ratio,
            "subscription_price_per_share": subscription_price,
            "interpretation": "official_preannouncement_context_only_not_directional",
        }
        invalid_key = hashlib.sha256(
            _payload_text(raw).encode("utf-8")
        ).hexdigest()
        record_key = (
            f"{market_code}:exright_preannouncement:{code_text}:{effective_date}"
            if effective_date and code_text
            else f"{market_code}:exright_preannouncement:invalid:{invalid_key}"
        )
        return _common_record(
            source,
            record_key=record_key,
            entity_id=code_text or None,
            event_type="corporate_action_preannouncement",
            # These public tables do not expose a trustworthy publish timestamp.
            # Point-in-time use is bounded by source_records.updated_at instead.
            event_time=None,
            effective_date=effective_date,
            title=(f"{name_text or code_text} 除權息預告" if code_text else "除權息預告"),
            metric_name="cash_dividend_per_share",
            metric_value=cash_dividend,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="TWD_per_share",
            currency="TWD",
            raw_record=raw,
            quality_status=quality_status,
        )

    if kind == "daytrade_eligibility":
        market_code = str(config.get("market") or "TWSE")
        market_label = str(
            config.get("market_label")
            or ("上市" if market_code == "TWSE" else "上櫃")
        )
        effective_date = parse_roc_date(
            _first_value(raw, "Date", "日期", "資料日期")
        )
        code = _first_value(raw, "Code", "證券代號", "SecuritiesCompanyCode")
        name = _first_value(raw, "Name", "證券名稱", "CompanyName")
        suspension_field = "Suspension" in raw or "暫停現股賣出後現款買進當沖註記" in raw
        suspension_raw = raw.get(
            "Suspension", raw.get("暫停現股賣出後現款買進當沖註記")
        )
        suspension = str(suspension_raw or "").strip()
        valid_suspension_markers = {
            str(value).strip().upper()
            for value in config.get("valid_suspension_markers", ("", "Y"))
        }
        quality_status = (
            "ok"
            if effective_date
            and code
            and str(code).strip()
            and name
            and str(name).strip()
            and suspension_field
            and suspension.upper() in valid_suspension_markers
            else "quarantine"
        )
        code_text = str(code).strip() if code is not None else None
        name_text = str(name).strip() if name is not None else None
        if effective_date and code_text:
            record_key = (
                f"{market_code}:{code_text}:daytrade_eligibility:{effective_date}"
            )
        else:
            invalid_key = hashlib.sha256(
                _payload_text(raw).encode("utf-8")
            ).hexdigest()
            record_key = f"{market_code}:daytrade_eligibility:invalid:{invalid_key}"
        suspension_text = json.dumps(
            {
                "market": market_label,
                "security_name": name_text,
                "suspension_after_sell_cash_buy": suspension,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return _common_record(
            source,
            record_key=record_key,
            entity_id=code_text,
            event_type="daytrade_eligibility_daily",
            event_time=(
                dt.datetime.combine(effective_date, dt.time.min)
                if effective_date else None
            ),
            effective_date=effective_date,
            title=(
                f"{market_code} {market_label}現股當沖標的：{name_text}"
                if name_text else f"{market_code} {market_label}現股當沖標的"
            ),
            metric_name="daytrade_target_list_membership",
            metric_value=1.0 if quality_status == "ok" else None,
            value_text=suspension_text,
            unit="official_target_list_member",
            currency=None,
            raw_record=raw,
            quality_status=quality_status,
        )

    if kind == "market_daytrade_summary":
        effective_date = parse_roc_date(_first_value(raw, "Date", "資料日期"))
        daytrade_volume = parse_number(_first_value(raw, "DayTradingVolume"))
        daytrade_volume_share = parse_number(
            _first_value(raw, "DayTradingVolumeOfTheMarket")
        )
        buy_value = parse_number(_first_value(raw, "DayTradingValueOfBuys"))
        buy_value_share = parse_number(
            _first_value(raw, "DayTradingValueOfBuyOfTheMarket")
        )
        sell_value = parse_number(_first_value(raw, "DayTradingValueOfSells"))
        sell_value_share = parse_number(
            _first_value(raw, "DayTradingValueOfSellsOfTheMarket")
        )
        metric_values = (
            daytrade_volume, daytrade_volume_share, buy_value, buy_value_share,
            sell_value, sell_value_share,
        )
        valid_values = (
            all(value is not None and value >= 0 for value in metric_values)
            and all(0 <= value <= 100 for value in (
                daytrade_volume_share, buy_value_share, sell_value_share,
            ) if value is not None)
        )
        today = dt.datetime.now(ZoneInfo("Asia/Taipei")).date()
        quality_status = (
            "ok"
            if effective_date and effective_date <= today and valid_values
            else "quarantine"
        )
        details = {
            "market": "上櫃",
            "daytrade_volume_shares": daytrade_volume,
            "daytrade_volume_share_pct": daytrade_volume_share,
            "daytrade_buy_value_twd": buy_value,
            "daytrade_buy_value_share_pct": buy_value_share,
            "daytrade_sell_value_twd": sell_value,
            "daytrade_sell_value_share_pct": sell_value_share,
        }
        if not effective_date:
            invalid_key = hashlib.sha256(_payload_text(raw).encode("utf-8")).hexdigest()
            return _common_record(
                source,
                record_key=f"TPEx:daytrade_market:invalid_date:{invalid_key}",
                entity_id="TPEX",
                event_type="market_daytrade_aggregate_daily",
                event_time=None,
                effective_date=None,
                title="TPEx 上櫃市場現股當沖交易統計（日期無效）",
                metric_name="daytrade_volume_share_pct",
                metric_value=None,
                value_text=None,
                unit="percent_of_otc_market",
                currency=None,
                raw_record=raw,
                quality_status="quarantine",
            )
        return _common_record(
            source,
            record_key=f"TPEx:daytrade_market:{effective_date}",
            entity_id="TPEX",
            event_type="market_daytrade_aggregate_daily",
            event_time=dt.datetime.combine(effective_date, dt.time.min),
            effective_date=effective_date,
            title="TPEx 上櫃市場現股當沖交易統計",
            metric_name="daytrade_volume_share_pct",
            metric_value=daytrade_volume_share,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="percent_of_otc_market",
            currency=None,
            raw_record=raw,
            quality_status=quality_status,
        )

    if kind == "market_quote":
        date_raw = _first_value(raw, "Date", "資料日期", "日期")
        effective_date = parse_roc_date(date_raw)
        code = _first_value(raw, "Code", "SecuritiesCompanyCode", "證券代號")
        name = _first_value(raw, "Name", "CompanyName", "證券名稱")
        change_raw = _first_value(raw, "Change", "漲跌價差", "漲跌")
        change = parse_number(change_raw)
        closing_price = _first_value(raw, "ClosingPrice", "Close", "收盤價", "收盤")
        closing_price_value = parse_number(closing_price)
        opening_price_value = parse_number(
            _first_value(raw, "OpeningPrice", "OpenPrice", "Open", "開盤價", "開盤")
        )
        highest_price_value = parse_number(
            _first_value(raw, "HighestPrice", "HighPrice", "High", "最高價", "最高")
        )
        lowest_price_value = parse_number(
            _first_value(raw, "LowestPrice", "LowPrice", "Low", "最低價", "最低")
        )
        trade_volume = _first_value(raw, "TradeVolume", "TradingShares", "成交股數")
        trade_volume_value = parse_number(trade_volume)
        transaction_amount = parse_number(
            _first_value(raw, "TransactionAmount", "成交金額")
        )
        transaction_count = parse_number(
            _first_value(raw, "TransactionNumber", "成交筆數")
        )
        no_trade = bool(
            market == "TPEx"
            and trade_volume_value == 0
            and transaction_amount == 0
            and transaction_count == 0
            and str(change_raw or "").strip() == "---"
            and str(closing_price or "").strip() == "---"
        )
        market_label = "上市" if market == "TWSE" else "上櫃"
        safe_record = {**raw, "市場別": market_label}
        quality_status = (
            "ok"
            if code and effective_date and (
                change is not None
                or (closing_price_value is not None and closing_price_value > 0)
                or no_trade
            )
            else "quarantine"
        )
        value_text = json.dumps(
            {
                "market": market_label,
                "change": change,
                "opening_price": opening_price_value,
                "highest_price": highest_price_value,
                "lowest_price": lowest_price_value,
                "closing_price": closing_price_value,
                "trade_volume": trade_volume_value,
                "no_trade": no_trade,
                "price_change_unavailable": (
                    "no_trade" if no_trade
                    else "exchange_marker_or_missing" if change is None
                    else None
                ),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return _common_record(
            source,
            record_key=f"{market}:quote:{str(code or '').strip()}:{effective_date}",
            entity_id=str(code).strip() if code is not None else None,
            event_type="market_quote_daily",
            event_time=(
                dt.datetime.combine(effective_date, dt.time.min)
                if effective_date
                else None
            ),
            effective_date=effective_date,
            title=str(name or code or ""),
            metric_name="daily_price_change",
            metric_value=change,
            value_text=value_text,
            unit="TWD_per_share",
            currency="TWD",
            raw_record=safe_record,
            quality_status=quality_status,
        )

    if kind == "institutional_flow":
        effective_date = parse_roc_date(_first_value(raw, "Date", "資料日期"))
        code_raw = _first_value(raw, "SecuritiesCompanyCode", "代號", "公司代號")
        code_text = str(code_raw or "").strip()
        name = _first_value(raw, "CompanyName", "名稱", "公司名稱")
        foreign_net = parse_number(
            _first_value(
                raw,
                "Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference",
            )
        )
        foreign_dealer_net = parse_number(_first_value(raw, "ForeignDealers-Difference"))
        trust_net = parse_number(
            _first_value(raw, "SecuritiesInvestmentTrustCompanies-Difference")
        )
        dealers_net = parse_number(_first_value(raw, "Dealers-Difference"))
        total_net = parse_number(_first_value(raw, "TotalDifference"))
        required_values = (foreign_net, trust_net, dealers_net, total_net)
        integer_values_valid = all(
            value is not None and float(value).is_integer() for value in required_values
        )
        totals_reconcile = (
            integer_values_valid
            and foreign_net + trust_net + dealers_net == total_net
        )
        # Preserve official non-standard/unmatched symbols for the coverage
        # audit; only blank codes are invalid here, not securities absent from
        # the current common-equity master.
        code_valid = bool(code_text)
        market_label = "上市" if market == "TWSE" else "上櫃"
        details = {
            "market": market_label,
            "foreign_net_shares_excluding_foreign_dealers": foreign_net,
            "foreign_dealers_net_shares": foreign_dealer_net,
            "investment_trust_net_shares": trust_net,
            "dealers_net_shares": dealers_net,
            "three_institution_total_net_shares": total_net,
            "interpretation": "official_daily_flow_context_only_until_pit_and_outcome_validation",
            "publication_time_semantics": "not_provided_by_source",
        }
        safe_record = {**raw, "市場別": market_label}
        record_key = (
            f"TPEx:institutional_flow:{effective_date}:{code_text}"
            if effective_date and code_text
            else "TPEx:institutional_flow:incomplete:"
            + hashlib.sha256(_payload_text(raw).encode("utf-8")).hexdigest()
        )
        quality_ok = bool(
            effective_date
            and code_valid
            and name
            and integer_values_valid
            and totals_reconcile
        )
        return _common_record(
            source,
            record_key=record_key,
            entity_id=code_text or None,
            event_type="institutional_flow_daily",
            event_time=None,
            effective_date=effective_date,
            title=f"{name or ''} {code_text}".strip(),
            metric_name="three_institution_net_shares",
            metric_value=total_net,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="shares",
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if quality_ok else "quarantine",
        )

    if kind == "valuation_metrics":
        effective_date = parse_roc_date(_first_value(raw, "Date", "資料日期", "日期"))
        code = _first_value(
            raw, "Code", "SecuritiesCompanyCode", "證券代號", "股票代號", "公司代號"
        )
        name = _first_value(raw, "Name", "CompanyName", "證券名稱", "股票名稱", "公司名稱")
        if market == "TWSE":
            pe_raw = _first_value(raw, "PEratio", "本益比")
            yield_raw = _first_value(raw, "DividendYield", "殖利率(%)", "殖利率")
            pb_raw = _first_value(raw, "PBratio", "股價淨值比")
            dividend_raw = None
        else:
            pe_raw = _first_value(raw, "PriceEarningRatio", "本益比")
            yield_raw = _first_value(raw, "YieldRatio", "殖利率(%)", "殖利率")
            pb_raw = _first_value(raw, "PriceBookRatio", "股價淨值比")
            dividend_raw = _first_value(raw, "DividendPerShare", "每股股利")
        pe_ratio = parse_number(pe_raw)
        dividend_yield = parse_number(yield_raw)
        price_to_book = parse_number(pb_raw)
        dividend_per_share = parse_number(dividend_raw)
        market_label = "上市" if market == "TWSE" else "上櫃"
        details = {
            "market": market_label,
            "pe_ratio": pe_ratio,
            "dividend_yield_pct": dividend_yield,
            "price_to_book": price_to_book,
        }
        if market == "TPEx":
            details["dividend_per_share"] = dividend_per_share
        has_metric = any(
            value is not None
            for value in (pe_ratio, dividend_yield, price_to_book)
        )
        safe_record = {**raw, "市場別": market_label}
        return _common_record(
            source,
            record_key=f"{market}:valuation:{str(code or '').strip()}:{effective_date}",
            entity_id=str(code).strip() if code is not None else None,
            event_type="valuation_metrics_daily",
            event_time=(
                dt.datetime.combine(effective_date, dt.time.min)
                if effective_date
                else None
            ),
            effective_date=effective_date,
            title=str(name or code or ""),
            metric_name="pe_ratio",
            metric_value=pe_ratio,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="exchange_reported_ratio",
            currency=None,
            raw_record=safe_record,
            quality_status="ok" if code and effective_date and has_metric else "quarantine",
        )

    if kind == "margin_balance":
        date_raw = _first_value(raw, "Date", "資料日期", "SnapshotDate")
        effective_date = parse_roc_date(date_raw)
        code = _first_value(raw, "公司代號", "SecuritiesCompanyCode", "股票代號", "代號")
        market_label = "上市" if market == "TWSE" else "上櫃"
        margin_current_raw = _first_value(raw, "融資今日餘額", "MarginPurchaseBalance")
        margin_previous_raw = _first_value(raw, "融資前日餘額", "MarginPurchaseBalancePreviousDay")
        margin_limit_raw = _first_value(raw, "融資限額", "MarginPurchaseQuota")
        margin_utilization_raw = _first_value(raw, "MarginPurchaseUtilizationRate", "融資使用率")
        short_current_raw = _first_value(raw, "融券今日餘額", "ShortSaleBalance")
        short_previous_raw = _first_value(raw, "融券前日餘額", "ShortSaleBalancePreviousDay")
        short_utilization_raw = _first_value(raw, "ShortSaleUtilizationRate", "融券使用率")
        margin_current = parse_number(margin_current_raw)
        margin_previous = parse_number(margin_previous_raw)
        margin_limit = parse_number(margin_limit_raw)
        margin_utilization = parse_number(margin_utilization_raw)
        if margin_utilization is None and margin_current is not None and margin_limit:
            margin_utilization = margin_current / margin_limit * 100
        short_current = parse_number(short_current_raw)
        short_previous = parse_number(short_previous_raw)
        short_utilization = parse_number(short_utilization_raw)
        safe_record = {
            "市場別": market_label,
            "Date": date_raw,
            "SecuritiesCompanyCode": code,
            "CompanyName": name,
            "margin_balance": margin_current,
            "margin_previous_balance": margin_previous,
            "margin_utilization_pct": margin_utilization,
            "short_balance": short_current,
            "short_previous_balance": short_previous,
            "short_utilization_pct": short_utilization,
        }
        details = {
            "margin_balance": margin_current,
            "margin_previous_balance": margin_previous,
            "margin_utilization_pct": margin_utilization,
            "short_balance": short_current,
            "short_previous_balance": short_previous,
            "short_utilization_pct": short_utilization,
        }
        return _common_record(
            source,
            record_key=f"{market}:margin_balance:{code}:{effective_date}",
            entity_id=str(code) if code is not None else None,
            event_type="margin_balance_snapshot",
            event_time=(
                dt.datetime.combine(effective_date, dt.time.min)
                if effective_date
                else None
            ),
            effective_date=effective_date,
            title=str(name or code or ""),
            metric_name="margin_purchase_balance",
            metric_value=margin_current,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="reported_units",
            currency=None,
            raw_record=safe_record,
            quality_status=(
                "ok"
                if code and effective_date
                and (margin_current is not None or short_current is not None)
                else "quarantine"
            ),
        )

    if kind in {
        "quarterly_profitability",
        "quarterly_balance_sheet",
        "quarterly_income_statement",
    }:
        return _normalize_quarterly_financial_record(source, raw)

    if kind == "quarterly_forecast_variance_event":
        return _normalize_tpex_forecast_variance_event(source, raw)

    if kind in {"quarterly_forecast_attainment", "quarterly_forecast_variance"}:
        return _normalize_quarterly_forecast_record(source, raw)

    if kind == "trading_attention":
        code = _first_value(raw, "Code", "SecuritiesCompanyCode")
        announcement_date_raw = _first_value(raw, "Date")
        announcement_date = parse_roc_date(announcement_date_raw)
        # The live endpoint can return an all-empty sentinel before its
        # post-close publication. It is not a zero-count market observation.
        if not code or not announcement_date:
            return None
        name = _first_value(raw, "Name", "CompanyName")
        market_label = "上市" if market == "TWSE" else "上櫃"
        safe_record = {**raw, "市場別": market_label}
        api_number = _first_value(raw, "NumberOfAnnouncement")
        attention = _first_value(raw, "TradingInfoForAttention", "TradingInformation")
        value = {
            "source_number": api_number,
            "attention_info": attention,
            "closing_price": parse_number(_first_value(raw, "ClosingPrice", "ClosePrice")),
            "pe_ratio": parse_number(_first_value(raw, "PE", "PriceEarningRatio")),
        }
        return _common_record(
            source,
            record_key=f"{market}:trading_attention:{code}:{announcement_date}",
            entity_id=str(code).strip(),
            event_type="trading_attention_notice",
            event_time=dt.datetime.combine(announcement_date, dt.time.min),
            effective_date=announcement_date,
            title=str(name or code),
            metric_name=None,
            metric_value=None,
            value_text=json.dumps(value, ensure_ascii=False, separators=(",", ":")),
            unit=None,
            currency=None,
            raw_record=safe_record,
            quality_status="ok",
        )

    if kind == "trading_disposition":
        code = _first_value(raw, "Code", "SecuritiesCompanyCode")
        if not code:
            return None
        market_label = "上市" if market == "TWSE" else "上櫃"
        safe_record = {**raw, "市場別": market_label}
        announcement_date = parse_roc_date(_first_value(raw, "Date"))
        period_text = str(_first_value(raw, "DispositionPeriod") or "")
        period_parts = re.split(r"\s*(?:~|～|至|到)\s*", period_text.strip())
        period_start = parse_roc_date(period_parts[0]) if len(period_parts) >= 2 else None
        period_end = parse_roc_date(period_parts[1]) if len(period_parts) >= 2 else None
        reason = _first_value(raw, "ReasonsOfDisposition", "DispositionReasons")
        measures = _first_value(raw, "DispositionMeasures", "DisposalCondition")
        detail = _first_value(raw, "Detail")
        measure_match = re.search(r"[３3]處置措施[:：]([\s\S]*)$", str(detail or ""))
        operation_measures = (
            " ".join(measure_match.group(1).split()) if measure_match else None
        )
        value = {
            "source_number": _first_value(raw, "NumberOfAnnouncement"),
            "start_date": period_start.isoformat() if period_start else None,
            "end_date": period_end.isoformat() if period_end else None,
            "reason": reason,
            "measures": measures,
            "operation_measures": operation_measures,
            "detail": detail,
        }
        quality_status = (
            "ok"
            if announcement_date and period_start and period_end and period_start <= period_end
            else "quarantine"
        )
        return _common_record(
            source,
            record_key=(
                f"{market}:trading_disposition:{code}:{period_start}:{period_end}"
            ),
            entity_id=str(code).strip(),
            event_type="trading_disposition",
            event_time=(
                dt.datetime.combine(announcement_date, dt.time.min)
                if announcement_date else None
            ),
            effective_date=announcement_date,
            title=str(_first_value(raw, "Name", "CompanyName") or code),
            metric_name=None,
            metric_value=None,
            value_text=json.dumps(value, ensure_ascii=False, separators=(",", ":")),
            unit=None,
            currency=None,
            raw_record=safe_record,
            quality_status=quality_status,
        )

    if kind == "securities_borrowing":
        date_raw = _first_value(raw, "Date")
        effective_date = parse_roc_date(date_raw)
        code = _first_value(raw, "SecuritiesCompanyCode")
        unique_fields = (
            "SecuritiesBorrowingBalancePreviousDay",
            "SecuritiesBorrowingSale",
            "SecuritiesBorrowingReturn",
            "SecuritiesBorrowingAdjustment",
            "SecuritiesBorrowingBalanceOfTheMarketDay",
        )
        values = {field: raw.get(field) for field in unique_fields}
        quality_status = (
            "ok"
            if code
            and effective_date
            and all(not _invalid_numeric_value(value) for value in values.values())
            else "quarantine"
        )
        safe_record = {
            "市場別": "上櫃",
            "Date": date_raw,
            "SecuritiesCompanyCode": code,
            **values,
        }
        return _common_record(
            source,
            record_key=f"TPEx:securities_borrowing:{code}:{effective_date}",
            entity_id=str(code) if code is not None else None,
            event_type="securities_borrowing_activity",
            event_time=(
                dt.datetime.combine(effective_date, dt.time.min)
                if effective_date
                else None
            ),
            effective_date=effective_date,
            title=str(code or ""),
            metric_name="borrowed_short_sale_balance",
            metric_value=parse_number(values["SecuritiesBorrowingBalanceOfTheMarketDay"]),
            value_text=None,
            unit="shares",
            currency=None,
            raw_record=safe_record,
            quality_status=quality_status,
        )

    if kind == "director_holdings":
        date_raw = _first_value(raw, "出表日期")
        month_raw = _first_value(raw, "資料年月")
        effective_date = parse_roc_month(month_raw)
        code = _first_value(raw, "公司代號")
        role = _first_value(raw, "職稱")
        person = _first_value(raw, "姓名")
        numeric_fields = (
            ("選任時持股", _first_value(raw, "選任時持股", "選任時持股 ")),
            ("目前持股", _first_value(raw, "目前持股")),
            ("設質股數", _first_value(raw, "設質股數")),
            ("設質股數佔持股比例", _first_value(raw, "設質股數佔持股比例")),
            (
                "內部人關係人目前持股合計",
                _first_value(raw, "內部人關係人目前持股合計"),
            ),
            ("內部人關係人設質股數", _first_value(raw, "內部人關係人設質股數")),
            ("內部人關係人設質比例", _first_value(raw, "內部人關係人設質比例")),
        )
        quality_status = (
            "ok"
            if code
            and effective_date
            and role
            and person
            and all(not _invalid_numeric_value(value) for _, value in numeric_fields)
            else "quarantine"
        )
        safe_record = {
            "市場別": "上市" if market == "TWSE" else "上櫃",
            "公司代號": code,
            "出表日期": date_raw,
            "資料年月": month_raw,
            "職稱": role,
            "姓名": person,
            **{field: value for field, value in numeric_fields},
        }
        published_date = parse_roc_date(date_raw)
        return _common_record(
            source,
            record_key=(
                f"{market}:director_holding:{code}:{effective_date}:{role}:{person}"
            ),
            entity_id=str(code) if code is not None else None,
            event_type="director_holding_snapshot",
            event_time=(
                dt.datetime.combine(published_date, dt.time.min)
                if published_date
                else None
            ),
            effective_date=effective_date,
            title=f"{role} / {person}" if role or person else str(code or ""),
            metric_name="current_insider_shares",
            metric_value=parse_number(_first_value(raw, "目前持股")),
            value_text=str(role or ""),
            unit="shares",
            currency=None,
            raw_record=safe_record,
            quality_status=quality_status,
        )

    if kind == "insider_transfer":
        report_type = config.get("report_type")
        twse = market == "TWSE"
        market_label = "上市" if twse else "上櫃"
        code_raw = _first_value(
            raw, "公司代號", "SecuritiesCompanyCode"
        )
        code_text = str(code_raw or "").strip()
        code = code_text if re.fullmatch(r"\d{4,6}", code_text) else None
        company_name = _first_value(raw, "公司名稱", "CompanyName")
        role_key = "申報人身分" if twse else "申請人身分"
        role = _first_value(raw, role_key)
        date_key = "出表日期" if twse else "Date"
        date_raw = _first_value(raw, date_key)
        published_date = parse_roc_date(date_raw)
        code_key = "公司代號" if twse else "SecuritiesCompanyCode"
        name_key = "公司名稱" if twse else "CompanyName"
        safe_record: dict[str, Any] = {
            "市場別": market_label,
            code_key: code_text or None,
            name_key: company_name,
            date_key: date_raw,
            role_key: role,
        }

        if report_type == "planned":
            method = _first_value(raw, "預定轉讓方式及股數-轉讓方式")
            shares_raw = _first_value(raw, "預定轉讓方式及股數-轉讓股數")
            shares = parse_number(shares_raw)
            details = {
                "report_type": "planned",
                "transfer_method": method,
                "transfer_shares_by_method": shares_raw,
                "daily_max_transfer_shares": _first_value(
                    raw, "每日於盤中交易最大得轉讓股數"
                ),
                "current_direct_shares": _first_value(
                    raw, "目前持有股數-自有持股", "目前持有自有持股"
                ),
                "current_trust_shares": _first_value(
                    raw,
                    "目前持有股數-保留運用決定權信託股數",
                    "目前持有保留運用決定權信託股數",
                ),
                "planned_direct_shares": _first_value(
                    raw,
                    "預定轉讓總股數-自有持股",
                    "預定轉讓總股數自有持股",
                ),
                "planned_trust_shares": _first_value(
                    raw,
                    "預定轉讓總股數-保留運用決定權信託股數",
                    "預定轉讓總股數保留運用決定權信託股數",
                ),
                "post_transfer_direct_shares": _first_value(
                    raw,
                    "預定轉讓後持股-自有持股",
                    "預定轉讓後持股自有持股",
                ),
                "post_transfer_trust_shares": _first_value(
                    raw,
                    "預定轉讓後持股-保留運用決定權信託股數",
                    "預定轉讓後持股保留運用決定權信託股數",
                ),
                "transfer_period": _first_value(raw, "有效轉讓期間"),
            }
            copied_fields = (
                "預定轉讓方式及股數-轉讓方式",
                "預定轉讓方式及股數-轉讓股數",
                "每日於盤中交易最大得轉讓股數",
                "目前持有股數-自有持股",
                "目前持有股數-保留運用決定權信託股數",
                "目前持有自有持股",
                "目前持有保留運用決定權信託股數",
                "預定轉讓總股數-自有持股",
                "預定轉讓總股數-保留運用決定權信託股數",
                "預定轉讓總股數自有持股",
                "預定轉讓總股數保留運用決定權信託股數",
                "預定轉讓後持股-自有持股",
                "預定轉讓後持股-保留運用決定權信託股數",
                "預定轉讓後持股自有持股",
                "預定轉讓後持股保留運用決定權信託股數",
                "有效轉讓期間",
            )
            safe_record.update(
                {key: raw[key] for key in copied_fields if key in raw}
            )
            if shares is not None and shares > 0:
                current_values = (
                    parse_number(details["current_direct_shares"]),
                    parse_number(details["current_trust_shares"]),
                )
                planned_values = (
                    parse_number(details["planned_direct_shares"]),
                    parse_number(details["planned_trust_shares"]),
                )
                if all(value is not None for value in (*current_values, *planned_values)):
                    current_total = sum(value for value in current_values if value is not None)
                    planned_total = sum(value for value in planned_values if value is not None)
                    if current_total > 0:
                        details["planned_total_pct_of_current_holdings"] = round(
                            planned_total / current_total * 100, 4
                        )
            quality_status = (
                "ok"
                if (
                    code and published_date and method and shares is not None
                    and shares > 0 and details["transfer_period"]
                )
                else "quarantine"
            )
            record_key = (
                f"{market}:insider_transfer_planned:{code or 'unmatched'}:"
                f"{published_date}:{role or ''}:{method or ''}:{shares_raw or ''}:"
                f"{details['transfer_period'] or ''}"
            )
            event_type = "insider_transfer_planned"
            metric_name = "planned_transfer_shares_by_method"
            metric_value = shares
            title = str(company_name or "內部人預定轉讓申報")
        elif report_type == "untransferred":
            direct_key = "未轉讓股數-自有持股" if twse else "未轉讓股數自有持股"
            trust_key = (
                "未轉讓股數-保留運用決定權信託股數"
                if twse else "未轉讓股數保留運用決定權信託股數"
            )
            direct_raw = _first_value(raw, direct_key)
            trust_raw = _first_value(raw, trust_key)
            direct_shares = parse_number(direct_raw)
            trust_shares = parse_number(trust_raw)
            details = {
                "report_type": "untransferred",
                "untransferred_direct_shares": direct_raw,
                "untransferred_trust_shares": trust_raw,
                "current_direct_shares": _first_value(
                    raw, "目前持股-自有持股", "目前持有自有持股"
                ),
                "current_trust_shares": _first_value(
                    raw,
                    "目前持股-保留運用決定權信託股數",
                    "目前持有保留運用決定權信託股數",
                ),
                "original_planned_direct_shares": _first_value(
                    raw,
                    "原申報預定轉讓股數-自有持股",
                    "原申報預定轉讓股數自有持股",
                ),
                "original_planned_trust_shares": _first_value(
                    raw,
                    "原申報預定轉讓股數-保留運用決定權信託股數",
                    "原申報預定轉讓股數保留運用決定權信託股數",
                ),
                "reason": _first_value(raw, "未轉讓理由"),
            }
            safe_record.update(
                {
                    direct_key: direct_raw,
                    trust_key: trust_raw,
                    "未轉讓理由": details["reason"],
                }
            )
            holding_fields = (
                (
                    "目前持股-自有持股",
                    "目前持股-保留運用決定權信託股數",
                    "原申報預定轉讓股數-自有持股",
                    "原申報預定轉讓股數-保留運用決定權信託股數",
                )
                if twse else
                (
                    "目前持有自有持股",
                    "目前持有保留運用決定權信託股數",
                    "原申報預定轉讓股數自有持股",
                    "原申報預定轉讓股數保留運用決定權信託股數",
                )
            )
            safe_record.update(
                {key: raw[key] for key in holding_fields if key in raw}
            )
            has_untransferred_shares = any(
                value is not None and value > 0
                for value in (direct_shares, trust_shares)
            )
            quality_status = (
                "ok"
                if code and published_date and has_untransferred_shares
                else "quarantine"
            )
            record_key = (
                f"{market}:insider_transfer_untransferred:{code or 'unmatched'}:"
                f"{published_date}:{role or ''}:{direct_raw or ''}:{trust_raw or ''}"
            )
            event_type = "insider_transfer_untransferred"
            metric_name = None
            metric_value = None
            title = str(company_name or "內部人未完成轉讓申報")
        else:
            raise ValueError("Unsupported insider transfer report type")

        return _common_record(
            source,
            record_key=record_key,
            entity_id=code,
            event_type=event_type,
            event_time=(
                dt.datetime.combine(published_date, dt.time.min)
                if published_date else None
            ),
            effective_date=published_date,
            title=title,
            metric_name=metric_name,
            metric_value=metric_value,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit="shares" if metric_value is not None else None,
            currency=None,
            raw_record=safe_record,
            quality_status=quality_status,
        )

    if kind == "events":
        event_date = parse_roc_date(_first_value(raw, "發言日期", "出表日期", "事實發生日"))
        event_time = parse_roc_datetime(
            _first_value(raw, "發言日期", "出表日期"),
            _first_value(raw, "發言時間"),
        )
        subject = _first_value(raw, "主旨 ", "主旨")
        record_key = f"{market}:event:{code}:{event_date}:{_first_value(raw, '發言時間')}:{subject}"
        return _common_record(
            source,
            record_key=record_key,
            entity_id=str(code) if code is not None else None,
            event_type="company_event",
            event_time=event_time,
            effective_date=event_date,
            title=str(subject) if subject is not None else str(name or ""),
            metric_name=None,
            metric_value=None,
            value_text=str(_first_value(raw, "說明") or ""),
            unit=None,
            currency="TWD",
            raw_record=raw,
            quality_status="ok" if code and event_date and subject else "quarantine",
        )

    if kind == "revenue":
        period = _first_value(raw, "資料年月")
        effective_date = parse_roc_month(period)
        yoy_raw = _first_value(raw, "營業收入-去年同月增減(%)")
        yoy = parse_number(yoy_raw)
        record_key = f"{market}:revenue:{code}:{period}"
        return _common_record(
            source,
            record_key=record_key,
            entity_id=str(code) if code is not None else None,
            event_type="monthly_revenue",
            event_time=parse_roc_date(_first_value(raw, "出表日期")),
            effective_date=effective_date,
            title=str(name or ""),
            metric_name="revenue_yoy_pct",
            metric_value=yoy,
            value_text=str(_first_value(raw, "備註") or ""),
            unit="percent",
            currency="TWD",
            raw_record=raw,
            quality_status=(
                "ok"
                if code and effective_date and not _invalid_numeric_value(yoy_raw)
                else "quarantine"
            ),
        )

    if kind == "dividend":
        year = _first_value(raw, "股利年度")
        period = _first_value(raw, "期別")
        effective_date = parse_roc_date(
            _first_value(raw, "股東會日期", "董事會（擬議）股利分派日", "董事會決議通過股利分派日")
        )
        cash = _first_value(
            raw,
            "股東配發-盈餘分配之現金股利(元/股)",
            "股東配發內容-盈餘分配之現金股利(元/股)",
        )
        cash_value = parse_number(cash)
        record_key = f"{market}:dividend:{code}:{year}:{period}"
        return _common_record(
            source,
            record_key=record_key,
            entity_id=str(code) if code is not None else None,
            event_type="dividend",
            event_time=parse_roc_date(_first_value(raw, "出表日期")),
            effective_date=effective_date,
            title=str(name or ""),
            metric_name="cash_dividend_per_share",
            metric_value=cash_value,
            value_text=str(_first_value(raw, "決議（擬議）進度") or ""),
            unit="TWD_per_share",
            currency="TWD",
            raw_record=raw,
            quality_status=(
                "ok" if code and year and not _invalid_numeric_value(cash) else "quarantine"
            ),
        )

    raise ValueError(f"Unsupported market record kind: {kind}")


def _normalize_sbl_available_row(
    source: dict[str, Any], raw: dict[str, Any], snapshot_date: dt.date
) -> list[dict[str, Any]]:
    result = []
    pairs = (
        ("TWSE", "上市", "TWSECode", "TWSEAvailableVolume"),
        ("TPEx", "上櫃", "GRETAICode", "GRETAIAvailableVolume"),
    )
    for market, market_label, code_field, volume_field in pairs:
        code = _first_value(raw, code_field)
        volume = _first_value(raw, volume_field)
        if code is None and volume is None:
            continue
        normalized = {
            "市場別": market_label,
            "SecuritiesCompanyCode": str(code).strip() if code is not None else None,
            "AvailableVolume": volume,
            "SnapshotDate": snapshot_date.isoformat(),
        }
        numeric_volume = parse_number(volume)
        result.append(
            _common_record(
                source,
                record_key=(
                    f"{market}:available_short_shares:"
                    f"{normalized['SecuritiesCompanyCode']}:{snapshot_date}"
                ),
                entity_id=normalized["SecuritiesCompanyCode"],
                event_type="available_short_shares_snapshot",
                event_time=dt.datetime.combine(snapshot_date, dt.time.min),
                effective_date=snapshot_date,
                title=f"{market_label} {normalized['SecuritiesCompanyCode']}",
                metric_name="available_short_shares",
                metric_value=numeric_volume,
                value_text=None,
                unit="shares",
                currency=None,
                raw_record=normalized,
                quality_status=(
                    "ok"
                    if normalized["SecuritiesCompanyCode"] and numeric_volume is not None
                    else "quarantine"
                ),
            )
        )
    return result


def _normalize_cbc(source: dict[str, Any], payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") or {}
    rows = data.get("dataSets") or []
    pairs = [item.get("data") for item in (data.get("structure") or {}).get("Table1", [])]
    parsed_dates = [parse_roc_date(row[0]) for row in rows if row]
    valid_dates = [value for value in parsed_dates if value is not None]
    if not valid_dates:
        return []
    latest_date = max(valid_dates)
    lookback_days = int(source["config"].get("lookback_days", 365))
    cutoff = latest_date - dt.timedelta(days=lookback_days)

    result: list[dict[str, Any]] = []
    for row in rows:
        if not row:
            continue
        effective_date = parse_roc_date(row[0])
        if effective_date is None or effective_date < cutoff:
            continue
        for index, raw_value in enumerate(row[1:]):
            pair = pairs[index] if index < len(pairs) else None
            metric_value = parse_number(raw_value)
            if not pair or metric_value is None:
                continue
            record_key = f"{effective_date.isoformat()}:{pair}"
            result.append(
                _common_record(
                    source,
                    record_key=record_key,
                    entity_id=str(pair),
                    event_type="fx_daily",
                    event_time=dt.datetime.combine(effective_date, dt.time.min),
                    effective_date=effective_date,
                    title=str(pair),
                    metric_name="rate_to_usd",
                    metric_value=metric_value,
                    value_text=None,
                    unit="rate",
                    currency=str(pair),
                    raw_record={"date": row[0], "pair": pair, "value": raw_value},
                )
            )
            if len(result) > MAX_SOURCE_ROWS:
                raise ValueError("normalized source row count exceeds limit")
    return result


def _fetch_tpex_equity_quotes_payload(
    endpoint_url: str,
    *,
    request_params: dict[str, Any] | None,
    request_headers: dict[str, str],
) -> tuple[Any, int]:
    """Fetch TPEx daily quotes as validated, retryable fixed byte ranges."""

    return _fetch_tpex_openapi_json_payload(
        "tpex_equity_quotes_daily",
        endpoint_url,
        request_params=request_params,
        request_headers=request_headers,
    )


def _latest_twse_market_data_date(rows: Any) -> dt.date | None:
    if not isinstance(rows, list):
        return None
    return max(
        (
            date for row in rows if isinstance(row, dict)
            if (date := parse_roc_date(
                _first_value(row, "Date", "資料日期", "日期", "日　期")
            ))
        ),
        default=None,
    )


def _twse_csv_rows_with_api_fields(
    source_key: str, rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Add the source contract's canonical fields while retaining CSV columns."""

    aliases = {
        "twse_equity_quotes_daily": {
            "Date": "日期",
            "Code": "證券代號",
            "Name": "證券名稱",
            "TradeVolume": "成交股數",
            "OpeningPrice": "開盤價",
            "OpenPrice": "開盤價",
            "HighestPrice": "最高價",
            "HighPrice": "最高價",
            "LowestPrice": "最低價",
            "LowPrice": "最低價",
            "ClosingPrice": "收盤價",
            "Change": "漲跌價差",
        },
        "twse_market_index_daily": {
            "Date": "日期",
            "ClosingIndex": "收盤指數",
        },
        "twse_market_total_return_index_daily": {
            "Date": "日　期",
            "TAIEXTotalReturnIndex": "發行量加權股價報酬指數",
        },
    }
    fields = aliases.get(source_key)
    if not fields:
        raise ValueError("TWSE CSV fallback source is not allowlisted")
    return [
        {**row, **{target: row.get(source_field) for target, source_field in fields.items()}}
        for row in rows
    ]


def _normalize_trading_status_snapshot(source, rows, allowed_codes, observed_date):
    """Complete risk-list snapshot, including explicit absences for common stocks.

    TWSE has no report date; its observations must never be backdated.
    An absent TWSE name clears altered/periodic only, not suspension risk.
    """
    market = "上市" if source["config"]["market"] == "TWSE" else "上櫃"
    codes = {code for code, stock_market in (allowed_codes or set()) if stock_market == market}
    if not codes or not isinstance(rows, list) or any(not isinstance(r, dict) for r in rows):
        raise ValueError("trading status snapshot requires stock master and JSON object array")
    dated = market == "上櫃"
    if dated:
        dates = {parse_roc_date(row.get("Date")) for row in rows}
        if len(dates) != 1 or None in dates:
            raise ValueError("TPEx trading status has missing/mixed report dates")
        effective_date = dates.pop()
        if effective_date > observed_date:
            raise ValueError("trading status has future report date")
    else:
        effective_date = observed_date
    panel = {}
    for row in rows:
        code = str(row.get("SecuritiesCompanyCode" if dated else "Code") or "").strip()
        if not code or code in panel:
            raise ValueError("trading status has missing/duplicate security code")
        if dated:
            flags = {}
            for source_field, target_field in (("AlteredTrading", "altered"),
                    ("PeriodicTrading", "periodic"), ("ManagedStock", "managed"),
                    ("SuspensionOfTrading", "suspended")):
                value = str(row.get(source_field) or "").strip().upper()
                if value not in ("", "Y", "Ｙ"):
                    raise ValueError("trading status contains unknown flag")
                flags[target_field] = value in ("Y", "Ｙ")
        else:
            marker = str(row.get("PeriodicCallAuctionTrading") or "").strip()
            if marker not in ("", "**"):
                raise ValueError("TWSE periodic auction marker is unknown")
            flags = {"altered": True, "periodic": marker == "**", "managed": None, "suspended": None}
        panel[code] = (row, flags)
    result = []
    for code in sorted(codes):
        raw, flags = panel.get(code, ({}, {"altered": False, "periodic": False,
                                       "managed": False if dated else None,
                                       "suspended": False if dated else None}))
        details = {**flags, "listed_in_restricted_snapshot": code in panel,
                   "snapshot_scope": "altered_periodic_managed_suspended" if dated else "altered_periodic_only",
                   "date_basis": "official_report_date" if dated else "observed_date_no_report_date",
                   "matching_frequency": str(raw.get("MatchingFrequency") or "").strip()}
        safe = {**raw, "市場別": market, "公司代號": code,
                "__entity_id": code, "__snapshot_details": details}
        result.append(_common_record(
            source, record_key=f"{market}:trading_status:{code}:{effective_date}",
            entity_id=code, event_type="trading_status_snapshot",
            event_time=dt.datetime.combine(effective_date, dt.time.min), effective_date=effective_date,
            title=str(raw.get("CompanyName", raw.get("Name", code))),
            metric_name=None, metric_value=None,
            value_text=json.dumps(details, ensure_ascii=False, separators=(",", ":")),
            unit=None, currency=None, raw_record=safe, quality_status="ok",
        ))
    return result


def fetch_and_normalize(
    source: dict[str, Any],
    allowed_stock_market_codes: set[tuple[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch one allowlisted source and return normalized records plus HTTP status."""

    if not validate_source_contract(source):
        raise ValueError("source contract is not allowlisted")
    request_params = None
    request_headers = DEFAULT_HEADERS
    if source["adapter_key"] == "taifex_json":
        request_headers = {**DEFAULT_HEADERS, "Accept": "application/json"}
    elif source["adapter_key"] == "taifex_csv":
        request_headers = {
            **DEFAULT_HEADERS,
            "Accept": "application/json, text/csv",
        }
    elif source["adapter_key"] == "tpex_csv":
        request_headers = {**DEFAULT_HEADERS, "Accept": "text/csv"}
    snapshot_date = dt.datetime.now(ZoneInfo("Asia/Taipei")).date()
    record_source = source
    if source["source_key"] == "tpex_equity_quotes_daily":
        payload, response_status = _fetch_tpex_equity_quotes_payload(
            source["endpoint_url"],
            request_params=request_params,
            request_headers=request_headers,
        )
    elif source["source_key"] == "twse_equity_quotes_daily":
        primary_error = None
        try:
            payload, response_status = _fetch_source_json_payload(
                source["source_key"],
                source["endpoint_url"],
                request_params=request_params,
                request_headers=request_headers,
            )
        except Exception as exc:
            primary_error = exc
            payload, response_status = None, 0

        latest_primary_date = _latest_twse_market_data_date(payload)
        if (
            primary_error
            or latest_primary_date is None
            or latest_primary_date < snapshot_date
        ):
            fallback = (source["config"].get("same_day_fallback") or {})
            fallback_url = str(fallback.get("endpoint_url") or "")
            try:
                fallback_payload, fallback_status = _fetch_source_csv_rows(
                    source["source_key"],
                    fallback_url,
                    request_headers={**request_headers, "Accept": "text/csv"},
                )
                fallback_date = _latest_twse_market_data_date(fallback_payload)
                if fallback_date != snapshot_date:
                    raise ValueError(
                        "TWSE licensed daily quote resource is not current for the scan date"
                    )
                fallback_payload = _twse_csv_rows_with_api_fields(
                    source["source_key"], fallback_payload
                )
                payload = fallback_payload
                response_status = fallback_status
                record_source = {
                    **source,
                    "endpoint_url": fallback_url,
                }
                print(
                    "[twse_quote_fallback] state=used "
                    f"requested_date={snapshot_date.isoformat()} "
                    f"rows={len(payload)} effective_date={fallback_date} "
                    f"http_status={fallback_status}"
                )
            except Exception as fallback_error:
                print(
                    "[twse_quote_fallback] state=unavailable "
                    f"requested_date={snapshot_date.isoformat()} "
                    f"fallback_error={type(fallback_error).__name__} "
                    f"primary_error={type(primary_error).__name__ if primary_error else 'none'} "
                    f"primary_effective_date={latest_primary_date or 'unknown'}"
                )
                if primary_error:
                    raise primary_error
    elif source["source_key"] in {
        "twse_market_index_daily",
        "twse_market_total_return_index_daily",
    }:
        primary_error = None
        try:
            payload, response_status = _fetch_source_json_payload(
                source["source_key"],
                source["endpoint_url"],
                request_params=request_params,
                request_headers=request_headers,
            )
        except Exception as exc:
            primary_error = exc
            payload, response_status = None, 0

        latest_primary_date = _latest_twse_market_data_date(payload)
        if (
            primary_error
            or latest_primary_date is None
            or latest_primary_date < snapshot_date
        ):
            fallback = (source["config"].get("same_day_fallback") or {})
            fallback_url = str(fallback.get("endpoint_url") or "")
            try:
                fallback_payload, fallback_status = _fetch_source_csv_rows(
                    source["source_key"],
                    fallback_url,
                    request_headers={**request_headers, "Accept": "text/csv"},
                )
                fallback_date = _latest_twse_market_data_date(fallback_payload)
                if fallback_date != snapshot_date:
                    raise ValueError(
                        "TWSE licensed daily index resource is not current for the scan date"
                    )
                fallback_payload = _twse_csv_rows_with_api_fields(
                    source["source_key"], fallback_payload
                )
                payload = fallback_payload
                response_status = fallback_status
                record_source = {
                    **source,
                    "endpoint_url": fallback_url,
                }
                print(
                    "[twse_index_fallback] state=used "
                    f"requested_date={snapshot_date.isoformat()} "
                    f"rows={len(payload)} effective_date={fallback_date} "
                    f"http_status={fallback_status}"
                )
            except Exception as fallback_error:
                print(
                    "[twse_index_fallback] state=unavailable "
                    f"requested_date={snapshot_date.isoformat()} "
                    f"fallback_error={type(fallback_error).__name__} "
                    f"primary_error={type(primary_error).__name__ if primary_error else 'none'} "
                    f"primary_effective_date={latest_primary_date or 'unknown'}"
                )
                if primary_error:
                    raise primary_error
    elif source["adapter_key"] in {"tpex_csv", "taifex_csv"}:
        payload, response_status = _fetch_source_csv_rows(
            source["source_key"],
            source["endpoint_url"],
            request_headers=request_headers,
        )
    else:
        payload, response_status = _fetch_source_json_payload(
            source["source_key"],
            source["endpoint_url"],
            request_params=request_params,
            request_headers=request_headers,
        )
    if source["adapter_key"] == "cbc_api_json":
        return _normalize_cbc(source, payload), response_status
    if source["config"].get("kind") == "trading_status":
        return _normalize_trading_status_snapshot(
            source, payload, allowed_stock_market_codes, snapshot_date
        ), response_status
    if source["adapter_key"] in {"taifex_json", "taifex_csv"}:
        if not isinstance(payload, list):
            raise ValueError("TAIFEX source response is not a JSON array")
        if len(payload) > MAX_SOURCE_ROWS:
            raise ValueError("source row count exceeds limit")
        if any(not isinstance(row, dict) for row in payload):
            raise ValueError("TAIFEX source contains a non-object row")
        records = [
            record
            for row in payload
            if (record := _normalize_taifex_position_record(source, row)) is not None
        ]
        return _resolve_record_key_collisions(records), response_status
    if source["adapter_key"] in {"twse_json", "tpex_json", "tpex_csv"}:
        if not isinstance(payload, list):
            raise ValueError("market source response is not a JSON array")
        rows: Iterable[dict[str, Any]] = payload
        rows = list(rows)
        if len(rows) > MAX_SOURCE_ROWS:
            raise ValueError("source row count exceeds limit")
        tdcc_incomplete_common_codes: set[str] = set()
        stock_market_by_code: dict[str, str] = {}
        if source["source_key"] == "tdcc_shareholder_distribution_weekly":
            if not rows or any(not isinstance(row, dict) for row in rows):
                raise ValueError("TDCC shareholder distribution is empty or malformed")
            if not allowed_stock_market_codes:
                raise ValueError("TDCC coverage classification requires a stock master")
            stock_market_by_code = {
                code: market for code, market in allowed_stock_market_codes
            }
            normalized_rows = [
                {str(key).lstrip("\ufeff"): value for key, value in row.items()}
                for row in rows
            ]
            source_dates = {
                parse_roc_date(_first_value(row, "資料日期"))
                for row in normalized_rows
            }
            if len(source_dates) != 1 or None in source_dates:
                raise ValueError(
                    "TDCC shareholder distribution must contain one valid effective date"
                )
            tier_counts_by_code: dict[str, dict[int, int]] = {}
            for row in normalized_rows:
                code = str(_first_value(row, "證券代號") or "").strip()
                tier_value = parse_number(_first_value(row, "持股分級"))
                if code in stock_market_by_code:
                    tier_counts = tier_counts_by_code.setdefault(code, {})
                    if tier_value is not None and float(tier_value).is_integer():
                        tier = int(tier_value)
                        tier_counts[tier] = tier_counts.get(tier, 0) + 1
                    else:
                        tier_counts[-1] = tier_counts.get(-1, 0) + 1
            expected_tiers = set(range(1, 18))
            tdcc_incomplete_common_codes = {
                code
                for code, tier_counts in tier_counts_by_code.items()
                if set(tier_counts) != expected_tiers
                or any(count != 1 for count in tier_counts.values())
            }
            rows = normalized_rows
        records = []
        if source["config"].get("filter_to_stock_master") and not allowed_stock_market_codes:
            raise ValueError("stock source filter requires a non-empty stock master")
        for row in rows:
            if source["config"].get("kind") == "sbl_available":
                records.extend(_normalize_sbl_available_row(source, row, snapshot_date))
            else:
                record = _normalize_market_record(record_source, row)
                if record is not None:
                    if source["source_key"] == "tdcc_shareholder_distribution_weekly":
                        code_text = str(record.get("entity_id") or "").strip()
                        stock_market = stock_market_by_code.get(code_text)
                        if stock_market:
                            safe_record = {
                                **(record.get("_raw_record") or {}),
                                "市場別": stock_market,
                            }
                            record["entity_type"] = "stock"
                            record["_raw_record"] = safe_record
                            record["payload"] = _payload_text(safe_record)
                        else:
                            # Keep other securities in the source record history,
                            # but do not let them masquerade as common-stock rows.
                            record["entity_type"] = "security"
                        if code_text in tdcc_incomplete_common_codes:
                            record["quality_status"] = "quarantine"
                    records.append(record)
        if source["config"].get("filter_to_stock_master"):
            records = [
                record
                for record in records
                if (
                    record.get("entity_id"),
                    (record.get("_raw_record") or {}).get("市場別"),
                )
                in allowed_stock_market_codes
            ]
        if source["source_key"] == "tpex_daytrade_market_summary_daily":
            date_counts: dict[dt.date, int] = {}
            for record in records:
                effective_date = record.get("effective_date")
                if effective_date:
                    date_counts[effective_date] = date_counts.get(effective_date, 0) + 1
            duplicate_dates = {
                effective_date for effective_date, count in date_counts.items()
                if count > 1
            }
            for record in records:
                if record.get("effective_date") in duplicate_dates:
                    record["quality_status"] = "quarantine"
        if source["source_key"] in {
            "twse_daytrade_eligibility_daily",
            "tpex_daytrade_eligibility_daily",
        }:
            if not records:
                raise ValueError("day-trade eligibility source returned no rows")
            identity_payloads: dict[tuple[dt.date | None, str | None], set[str]] = {}
            for record in records:
                if record.get("effective_date") and record.get("entity_id"):
                    identity = (record["effective_date"], record["entity_id"])
                    payload_fingerprint = hashlib.sha256(
                        _payload_text(record.get("_raw_record") or {}).encode("utf-8")
                    ).hexdigest()
                    identity_payloads.setdefault(identity, set()).add(payload_fingerprint)
            conflicting_identities = {
                identity for identity, fingerprints in identity_payloads.items()
                if len(fingerprints) > 1
            }
            for record in records:
                identity = (record.get("effective_date"), record.get("entity_id"))
                if identity in conflicting_identities:
                    record["quality_status"] = "quarantine"
        if (
            source["source_key"] == "tpex_daytrade_market_summary_daily"
            and not records
        ):
            raise ValueError("TPEx day-trade market aggregate returned no dated rows")
        if source["source_key"] in {
            "twse_market_total_return_index_daily",
            "tpex_market_total_return_index_daily",
        }:
            if not records or len(records) != len(rows):
                raise ValueError(
                    "market total-return index source contains no data or invalid rows"
                )
            effective_dates = [record.get("effective_date") for record in records]
            if len(effective_dates) != len(set(effective_dates)):
                raise ValueError("market total-return index source returned duplicate dates")
        if len(records) > MAX_SOURCE_ROWS:
            raise ValueError("normalized source row count exceeds limit")
        return _resolve_record_key_collisions(records), response_status
    raise ValueError(f"Adapter is not allowlisted: {source['adapter_key']}")
