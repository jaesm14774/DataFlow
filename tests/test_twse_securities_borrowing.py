import datetime as dt

import pytest

from source_extension import adapters, registry

FIELDS = ["代號", "名稱", "前日餘額", "賣出", "買進", "現券", "今日餘額", "次一營業日限額",
          "前日餘額", "當日賣出", "當日還券", "當日調整", "當日餘額", "次一營業日可限額", "備註"]


def _source():
    return next(s for s in registry.SOURCE_DEFINITIONS if s["source_key"] == "twse_securities_borrowing_daily")


def _payload(rows, date="20261127"):
    return {"stat": "OK", "date": date, "fields": FIELDS, "data": rows}


def _row(code, bal="1,000", sale="10", ret="5", adj="0", prev="995"):
    return [code, "x", "0", "0", "0", "0", "0", "1", prev, sale, ret, adj, bal, "9", " "]


CODES = {("2330", "上市"), ("6488", "上櫃")}
TODAY = dt.date(2026, 11, 28)


def test_contract_is_allowlisted_and_registered():
    assert adapters.validate_source_contract(_source())


def test_listed_borrowing_balance_is_extracted_and_filters_to_stock_master():
    records = adapters._normalize_twse_securities_borrowing_table(
        _source(), _payload([_row("2330"), _row("00400A"), _row("6488")]), CODES, TODAY,
    )
    assert [r["entity_id"] for r in records] == ["2330"]
    record = records[0]
    assert record["metric_value"] == 1000
    assert record["event_type"] == "securities_borrowing_activity"
    assert record["effective_date"] == dt.date(2026, 11, 27)
    assert record["quality_status"] == "ok"


def test_bad_number_quarantines_instead_of_becoming_zero():
    records = adapters._normalize_twse_securities_borrowing_table(
        _source(), _payload([_row("2330", bal="abc")]), CODES, TODAY,
    )
    assert records[0]["quality_status"] == "quarantine"


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(stat="很抱歉，沒有符合條件的資料!"),
    lambda p: p.update(fields=FIELDS[:12]),
    lambda p: p.update(fields=FIELDS[:8] + ["a", "b", "c", "d", "e", "f", "g"]),
    lambda p: p.update(date="20261201"),
    lambda p: p.update(date=""),
    lambda p: p.update(data=[_row("2330"), _row("2330")]),
])
def test_layout_drift_future_date_and_duplicates_are_rejected(mutate):
    payload = _payload([_row("2330")])
    mutate(payload)
    with pytest.raises(ValueError):
        adapters._normalize_twse_securities_borrowing_table(_source(), payload, CODES, TODAY)


def test_every_schedule_stays_under_mapped_source_cap_with_headroom():
    from collections import Counter

    counts = Counter(
        d["schedule_hint"] for d in registry.SOURCE_DEFINITIONS
        if d.get("enabled") and d.get("approval_status") == "approved"
    )
    # discover_approved_sources raises above the cap, which would fail the whole DAG run
    assert max(counts.values()) <= adapters.MAX_MAPPED_SOURCES - 4
