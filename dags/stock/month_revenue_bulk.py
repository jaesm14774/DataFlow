"""Bulk monthly revenue (official OpenAPI) -> legacy `月營收資訊` columns.

The per-code MOPS scrape (ajax_t146sb05) broke in 2025-03 (page layout lost the
`年度` column) and kept "succeeding" with insert=0 for months. The listed
(t187ap05_L) and OTC (mopsfin_t187ap05_O) open-data feeds carry the same
fields for every company's latest month, so the table is kept current from
them. Missing/invalid numbers stay NaN, never 0.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Iterable, Mapping

import pandas as pd

TWSE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TPEX_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"

COLUMNS = [
    "證券代號", "資料日期", "當月營收", "上月營收", "去年當月營收", "上月營收增減(%)",
    "去年同月增減(%)", "當月累計營收", "去年累計營收", "前期比較增減(%)",
]
_SOURCE_FIELDS = {
    "當月營收": "營業收入-當月營收",
    "上月營收": "營業收入-上月營收",
    "去年當月營收": "營業收入-去年當月營收",
    "上月營收增減(%)": "營業收入-上月比較增減(%)",
    "去年同月增減(%)": "營業收入-去年同月增減(%)",
    "當月累計營收": "累計營業收入-當月累計營收",
    "去年累計營收": "累計營業收入-去年累計營收",
    "前期比較增減(%)": "累計營業收入-前期比較增減(%)",
}


def _number(value: Any) -> float | None:
    text = str(value if value is not None else "").strip().replace(",", "")
    if text in {"", "-", "--", "N/A", "NA", "null", "None"}:
        return None
    try:
        number = float(text)
    except ValueError:
        return None
    return number if number == number and abs(number) != float("inf") else None


def _revenue_month(value: Any) -> dt.date | None:
    """ROC `11510` -> 2026-10-01 (the legacy table's month-start convention)."""
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) not in (5, 6):
        return None
    try:
        return dt.date(int(digits[:-2]) + 1911, int(digits[-2:]), 1)
    except ValueError:
        return None


def bulk_frame(
    rows_by_market: Mapping[str, Iterable[Mapping[str, Any]]],
    valid_codes: Iterable[str] | None = None,
    today: dt.date | None = None,
) -> pd.DataFrame:
    """Normalise the two feeds; rows with no code/month/current revenue are dropped."""
    allowed = set(valid_codes) if valid_codes is not None else None
    today = today or dt.date.today()
    records: list[dict[str, Any]] = []
    for rows in rows_by_market.values():
        for row in rows:
            code = str(row.get("公司代號") or "").strip()
            month = _revenue_month(row.get("資料年月"))
            if not code or month is None or month > today:
                continue
            if allowed is not None and code not in allowed:
                continue
            record = {"證券代號": code, "資料日期": month.isoformat()}
            for target, source in _SOURCE_FIELDS.items():
                record[target] = _number(row.get(source))
            if record["當月營收"] is None:
                continue
            records.append(record)
    frame = pd.DataFrame(records, columns=COLUMNS)
    return frame.drop_duplicates(["證券代號", "資料日期"]).reset_index(drop=True)
