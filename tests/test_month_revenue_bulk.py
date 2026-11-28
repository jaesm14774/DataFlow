import datetime as dt

import pandas as pd

from stock import month_revenue_bulk as mr


def _row(code="1101", ym="11510", cur="16,357,176", **extra):
    row = {
        "公司代號": code, "資料年月": ym, "營業收入-當月營收": cur, "營業收入-上月營收": "14321556",
        "營業收入-去年當月營收": "13882248", "營業收入-上月比較增減(%)": "14.21",
        "營業收入-去年同月增減(%)": "17.83", "累計營業收入-當月累計營收": "129405701",
        "累計營業收入-去年累計營收": "123320545", "累計營業收入-前期比較增減(%)": "4.93",
    }
    row.update(extra)
    return row


TODAY = dt.date(2026, 11, 28)


def test_maps_both_markets_to_legacy_columns_with_month_start_dates():
    df = mr.bulk_frame({"twse": [_row()], "tpex": [_row("1240", cur="227988")]}, today=TODAY)
    assert list(df.columns) == mr.COLUMNS
    assert sorted(df["證券代號"]) == ["1101", "1240"]
    first = df[df["證券代號"] == "1101"].iloc[0]
    assert first["資料日期"] == "2026-10-01" and first["當月營收"] == 16357176
    assert first["去年同月增減(%)"] == 17.83


def test_missing_or_invalid_revenue_is_dropped_never_zero_and_blank_optionals_stay_nan():
    df = mr.bulk_frame({"twse": [_row("1101", cur="-"), _row("1102", cur="abc"),
                                 _row("1103", **{"營業收入-去年同月增減(%)": ""})]}, today=TODAY)
    assert list(df["證券代號"]) == ["1103"]
    assert pd.isna(df.iloc[0]["去年同月增減(%)"])


def test_future_month_bad_month_unknown_code_and_duplicates_are_skipped():
    rows = [_row("1101", ym="11601"), _row("1102", ym="abc"), _row("9999"), _row("1101"), _row("1101")]
    df = mr.bulk_frame({"twse": rows}, valid_codes={"1101", "1102"}, today=TODAY)
    assert list(df["證券代號"]) == ["1101"] and len(df) == 1
