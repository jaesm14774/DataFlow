"""Taiwan stock-market open-day checks shared by collection and signal DAGs."""

from __future__ import annotations

import datetime as dt

import pandas as pd

from common.config import sql_configure_path
from lib.get_sql import get_sql


def is_taiwan_stock_market_closed(target_date: dt.date | dt.datetime | str) -> bool:
    """Return whether the Taiwan holiday table contains the requested date."""

    if isinstance(target_date, dt.datetime):
        market_date = target_date.date()
    elif isinstance(target_date, dt.date):
        market_date = target_date
    else:
        market_date = dt.date.fromisoformat(str(target_date)[:10])

    connection = pd.read_csv(
        sql_configure_path, encoding="utf_8_sig", index_col="name"
    )
    conn = cursor = engine = None
    try:
        conn, cursor, engine = get_sql(
            connection.loc["host", "value"],
            connection.loc["port", "value"],
            connection.loc["user", "value"],
            connection.loc["password", "value"],
            "holiday",
        )
        cursor.execute(
            "SELECT 1 FROM taiwan WHERE now_date=%s LIMIT 1",
            (market_date.isoformat(),),
        )
        holiday_record_found = cursor.fetchone() is not None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if engine:
            engine.dispose()

    is_closed = holiday_record_found
    print(
        f"[開盤判斷] 日期={market_date.isoformat()} "
        f"| 判斷來源=holiday.taiwan "
        f"| 假日表命中={int(holiday_record_found)} 列 "
        f"| 視為休市={is_closed}"
    )
    return is_closed
