"""股票資料共用的純函數（不依賴 config / DB），collect 與 calculate 兩邊共用。"""
import datetime
import re

import numpy as np

KEEP = object()  # clean_comma(unparsable=KEEP)：無法轉成數字時保留原字串


def clean_comma(txt, output_not_string=False, unparsable=np.nan):
    """清除千分位逗號。

    output_not_string=True 時強制轉數字(先 int 再 float)；
    無法轉換時回傳 unparsable（預設 np.nan，傳 KEEP 則回傳清除逗號後的原字串）。
    """
    try:
        txt = str(txt).strip()
    except Exception:
        pass

    if not isinstance(txt, str):
        return txt

    txt = txt.replace(',', '')
    if not output_not_string:
        return txt

    try:
        return int(txt)
    except Exception:
        try:
            return float(txt)
        except Exception:
            return txt if unparsable is KEEP else unparsable


def make_regular_date_format(dt):
    """民國 9 碼('112/01/02')或西元 10 碼日期 -> 'YYYY-MM-DD'，其餘格式 raise ValueError。"""
    if isinstance(dt, datetime.datetime):
        dt = str(dt)

    if len(dt) == 9:  # 民國轉西元
        dt = f'{int(dt[:3]) + 1911}{dt[3:]}'
    elif len(dt) == 10:  # 西元年
        pass
    else:
        raise ValueError(f'沒看過的日期格式: {dt}')

    return dt.replace('/', '-')


def roc_to_gregorian_text(t):
    """'112/01/02' -> '2023/01/02'（只轉換開頭的年份）。"""
    year = re.search(string=t, pattern=r'^(\d+)/').group(1)
    return re.sub(string=t, pattern=year, repl=str(int(year) + 1911))


def gregorian_to_roc_text(t):
    """'2023/01/02' -> '112/01/02'（只轉換開頭的年份）。"""
    year = re.search(string=t, pattern=r'^(\d+)/').group(1)
    return re.sub(string=t, pattern=year, repl=str(int(year) - 1911))
