import datetime as dt

import pytest

from source_extension import adapters, registry

FIELDS = [
    "證券代號", "證券名稱", "國際證券編碼", "發行股數", "外資及陸資尚可投資股數",
    "全體外資及陸資持有股數", "外資及陸資尚可投資比率", "全體外資及陸資持股比率",
    "外資及陸資共用法令投資上限比率", "陸資法令投資上限比率", "與前日異動原因(註)",
    "最近一次上市公司申報外資及陸資持股異動日期",
]


def _source():
    return next(s for s in registry.SOURCE_DEFINITIONS if s["source_key"] == "twse_foreign_holdings_daily")


def _row(code="2330", issued="1,000,000", available="400,000", held="600,000", avail_pct=40.0, hold_pct=60.0):
    return [code, "x", "TW000", issued, available, held, avail_pct, hold_pct, "100.00", "100.00", "", "115/07/07"]


def _payload(rows, date="20261127"):
    return {"stat": "OK", "date": date, "fields": FIELDS, "data": rows}


CODES = {("2330", "上市"), ("6488", "上櫃")}
TODAY = dt.date(2026, 11, 28)


def test_contract_allowlisted_and_shares_event_with_tpex_feed():
    assert adapters.validate_source_contract(_source())
    records = adapters._normalize_twse_foreign_holdings_table(_source(), _payload([_row()]), CODES, TODAY)
    assert records[0]["event_type"] == "foreign_ownership_daily"
    assert records[0]["metric_name"] == "foreign_holding_ratio_pct"


def test_listed_rows_extracted_and_non_master_codes_dropped():
    records = adapters._normalize_twse_foreign_holdings_table(
        _source(), _payload([_row("2330"), _row("00400A"), _row("6488")]), CODES, TODAY,
    )
    assert [r["entity_id"] for r in records] == ["2330"]
    assert records[0]["metric_value"] == 60.0
    assert records[0]["effective_date"] == dt.date(2026, 11, 27)
    assert records[0]["quality_status"] == "ok"


def test_inconsistent_ratio_or_overheld_is_quarantined_not_stored_as_ok():
    mismatch = adapters._normalize_twse_foreign_holdings_table(
        _source(), _payload([_row(hold_pct=75.0)]), CODES, TODAY,
    )
    over = adapters._normalize_twse_foreign_holdings_table(
        _source(), _payload([_row(held="2,000,000")]), CODES, TODAY,
    )
    assert mismatch[0]["quality_status"] == "quarantine"
    assert over[0]["quality_status"] == "quarantine"


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(stat="很抱歉，沒有符合條件的資料!"),
    lambda p: p.update(fields=FIELDS[1:]),
    lambda p: p.update(date="20261201"),
    lambda p: p.update(date=""),
    lambda p: p.update(data=[_row("2330"), _row("2330")]),
])
def test_layout_drift_future_date_and_duplicates_rejected(mutate):
    payload = _payload([_row()])
    mutate(payload)
    with pytest.raises(ValueError):
        adapters._normalize_twse_foreign_holdings_table(_source(), payload, CODES, TODAY)
