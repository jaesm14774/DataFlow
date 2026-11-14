"""Historical, point-in-time study of candidate Taiwan signal families.

Read-only. Run inside the Airflow worker container:

    docker exec dataflow-airflow-worker-1 python /opt/airflow/docs/research_signal_family_backtest.py \
        --out /tmp/research_signal_family_backtest.json

Design (see docs/research_signal_family_backtest_20261108.md):

* Every family comes from the pre-registered table in
  ``source_extension.research_signals.FAMILIES``; signs and rank-decile cut
  points are fixed in code, not tuned.
* A signal dated T uses only rows dated <= T (weekly TDCC and monthly revenue
  are delayed by their publication rule). Entry is the close of the next
  session, exit H sessions later, which matches the production ``next_close``
  basis.
* Excess return is measured against the same-date mean of the eligible
  universe (TAIEX is dominated by one stock), and raw returns are kept.
* Forward windows containing a daily move above 10.5% are dropped because the
  stored prices are not adjusted for splits/capital reductions.
* Inference uses non-overlapping dates, a within-date shuffle control,
  Benjamini-Hochberg across every family x horizon, a discovery period
  (<= 2022-12-31) and an untouched holdout (>= 2023-01-01).
* A freqtrade-style truncation test recomputes each score from data cut at a
  cutoff date and diffs it against the full-history value.

Stored tables record collection time, not immutable vintages; restatements,
delistings and survivorship in ``全證券代碼相關資料表`` are not corrected here.
"""

from __future__ import annotations

import argparse
import hashlib
import datetime as dt
import json
import math
import os
import pickle
import re
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
for candidate in (HERE.parent / "dags", Path("/opt/airflow/dags")):
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        break

from source_extension import research_signals as rs  # noqa: E402

DISCOVERY_END = pd.Timestamp("2022-12-31")
STUDY_START = pd.Timestamp("2014-01-01")
FIRST_SIGNAL_DATE = pd.Timestamp("2015-07-01")
MIN_VALUE_NTD = 10_000_000  # 20-session median traded value
SPLIT_MOVE = 0.105
MIN_CROSS_SECTION = 150
SHUFFLE_T_LIMIT = 3.0
SEED = 20261108


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _frame(cursor, sql: str, params: tuple, columns: list[str]) -> pd.DataFrame:
    # an empty tuple would still %-format the SQL, which breaks "(%)" column names
    cursor.execute(sql, params or None)
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _year_ranges(start_year: int, end_year: int):
    for year in range(start_year, end_year + 1):
        yield dt.date(year, 1, 1), dt.date(year, 12, 31)


def load_raw(cache_dir: Path | None) -> dict[str, pd.DataFrame]:
    if cache_dir:
        cache_file = cache_dir / "raw_v2.pkl"
        if cache_file.exists():
            with cache_file.open("rb") as handle:
                cached = pickle.load(handle)
            # text counts are cached under a key derived from the lexicon and matching
            # rules, never inside raw_v2.pkl, so rule edits cannot reuse stale counts
            cached.pop("text", None)
            text_cache = cache_dir / f"text_{_TEXT_VERSION}.pkl"
            if text_cache.exists():
                with text_cache.open("rb") as handle:
                    cached["text"] = pickle.load(handle)
            else:
                cached["text"] = load_text_counts(cached["master"])
                with text_cache.open("wb") as handle:
                    pickle.dump(cached["text"], handle)
            return cached
    from lib.db_connection import MySQLConnection

    today = dt.date.today()
    code_filter = "CHAR_LENGTH(`證券代號`)=4 AND `證券代號` BETWEEN '1000' AND '9999'"
    raw: dict[str, pd.DataFrame] = {}
    with MySQLConnection("stock") as connection:
        cursor = connection.cursor
        specs = {
            "price": ("個股成交資訊", ["date", "code", "close", "volume"], "`收盤`,`成交量`", 2014),
            "inst": (
                "三大法人買賣", ["date", "code", "foreign", "trust", "dealer"],
                "`外資買賣超股數`,`投信買賣超股數`,`自營商買賣超股數`", 2014,
            ),
            "margin": (
                "融資融券", ["date", "code", "margin_bal", "short_bal", "short_to_margin", "margin_util"],
                "`融資剩餘額`,`融券剩餘額`,`券資比`,`融資使用率`", 2016,
            ),
            "daytrade": ("當沖資訊", ["date", "code", "dt_shares"], "`當日沖銷交易成交股數`", 2014),
        }
        for key, (table, columns, select, first_year) in specs.items():
            parts = []
            for start, end in _year_ranges(first_year, today.year):
                started = time.time()
                part = _frame(
                    cursor,
                    f"SELECT `資料日期`,`證券代號`,{select} FROM `{table}` "
                    f"WHERE `資料日期` BETWEEN %s AND %s AND {code_filter}",
                    (start, end), columns,
                )
                parts.append(part)
                print(f"loaded {table} {start.year}: {len(part)} rows in {time.time() - started:.0f}s", flush=True)
            try:
                # delisted securities live in delete_<table>; without them every
                # window ends in a survivor and high-risk means are biased up
                deleted = _frame(
                    cursor,
                    f"SELECT `資料日期`,`證券代號`,{select} FROM `delete_{table}` "
                    f"WHERE `資料日期` >= %s AND {code_filter}",
                    (dt.date(2014, 1, 1),), columns,
                )
                parts.append(deleted)
                print(f"loaded delete_{table}: {len(deleted)} rows", flush=True)
            except Exception as error:  # pragma: no cover - table may not exist
                print(f"delete_{table} unavailable: {error}", flush=True)
            raw[key] = pd.concat(parts, ignore_index=True)
        raw["tdcc"] = _frame(
            cursor,
            "SELECT `資料日期`,`證券代號`,`>400張大股東持有百分比`,`總股東人數` FROM `大戶比例` "
            f"WHERE CHAR_LENGTH(`資料日期`)=8 AND {code_filter}",
            (), ["date", "code", "large_pct", "holders"],
        )
        try:
            raw["tdcc"] = pd.concat([raw["tdcc"], _frame(
                cursor,
                "SELECT `資料日期`,`證券代號`,`>400張大股東持有百分比`,`總股東人數` FROM `delete_大戶比例` "
                f"WHERE CHAR_LENGTH(`資料日期`)=8 AND {code_filter}",
                (), ["date", "code", "large_pct", "holders"],
            )], ignore_index=True)
        except Exception as error:  # pragma: no cover
            print(f"delete_大戶比例 unavailable: {error}", flush=True)
        raw["revenue"] = _frame(
            cursor,
            "SELECT `資料日期`,`證券代號`,`去年同月增減(%)` FROM `月營收資訊` "
            f"WHERE {code_filter}",
            (), ["date", "code", "yoy"],
        )
        raw["master"] = _frame(
            cursor, "SELECT `證券代號`,`證券名稱` FROM `全證券代碼相關資料表`", (), ["code", "name"],
        )
    with MySQLConnection("crawler") as connection:
        raw["ptt"] = _frame(
            connection.cursor,
            "SELECT created_at,title,LEFT(content,500) FROM ptt_article "
            "WHERE board='Stock' AND title LIKE %s AND created_at>=%s",
            ("[標的]%", "2018-01-01"), ["created_at", "title", "content"],
        )
    print(f"loaded ptt [標的] articles: {len(raw['ptt'])}", flush=True)
    text_cache = (cache_dir / f"text_{_TEXT_VERSION}.pkl") if cache_dir else None
    if text_cache and text_cache.exists():
        with text_cache.open("rb") as handle:
            raw["text"] = pickle.load(handle)
    else:
        raw["text"] = load_text_counts(raw["master"])
        if text_cache:
            cache_dir.mkdir(parents=True, exist_ok=True)
            with text_cache.open("wb") as handle:
                pickle.dump(raw["text"], handle)
    print(f"text mention keys: {len(raw['text']):,}", flush=True)
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        with (cache_dir / "raw_v2.pkl").open("wb") as handle:
            pickle.dump({key: value for key, value in raw.items() if key != "text"}, handle)
    return raw


# ---------------------------------------------------------------------------
# News / PTT text signals (stock mention counts and title polarity)
# ---------------------------------------------------------------------------

# The lexicon and the matching rules live in research_signals.py so the daily scan
# and this study use exactly the same code.
BULL_WORDS = rs.BULL_WORDS
BEAR_WORDS = rs.BEAR_WORDS
_TEXT_VERSION = "v" + hashlib.sha1(
    ("|".join(BULL_WORDS) + "#" + "|".join(BEAR_WORDS) + "#min3-unique-negation-midnight-dedupe-newlow-v3").encode()
).hexdigest()[:8]
_match_codes = rs.match_codes
_after_cutoff = rs.after_cutoff


def _build_name_trie(master: pd.DataFrame) -> tuple[dict, set[str]]:
    return rs.build_name_trie(zip(master["code"], master["name"]))


def load_text_counts(master: pd.DataFrame) -> pd.DataFrame:
    """Daily per-stock mention counts: news (with polarity) and PTT Stock titles."""

    from lib.db_connection import MySQLConnection
    from pymysql.cursors import SSCursor

    trie, codes = _build_name_trie(master)
    totals: dict[tuple, list[int]] = {}
    seen_titles: set[tuple] = set()

    def scan(connection_name: str, sql: str, params: tuple, source: str, bare: bool) -> int:
        seen = 0
        with MySQLConnection(connection_name) as connection:
            cursor = connection.conn.cursor(SSCursor)
            cursor.execute(sql, params)
            while True:
                batch = cursor.fetchmany(100_000)
                if not batch:
                    break
                for title, created_at in batch:
                    seen += 1
                    if not title or created_at is None or created_at.year < 2018:
                        continue
                    matched = _match_codes(str(title), trie, codes, bare_codes=bare)
                    if not matched:
                        continue
                    title_key = "".join(str(title).split())
                    bull, bear = rs.title_polarity_flags(str(title))
                    day = created_at.date()
                    late = _after_cutoff(created_at)
                    for code in matched:
                        if (day, code, source, title_key) in seen_titles:
                            continue  # syndicated copies of one headline count once
                        seen_titles.add((day, code, source, title_key))
                        row = totals.setdefault((day, late, code, source), [0, 0, 0])
                        row[0] += 1
                        row[1] += int(bull)
                        row[2] += int(bear)
                if seen % 1_000_000 < 100_000:
                    print(f"  scanned {source}: {seen:,} titles, {len(totals):,} keys", flush=True)
            cursor.close()
        return seen

    started = time.time()
    scan("news_ch", "SELECT title, created_at FROM news WHERE created_at >= %s",
         ("2018-01-01",), "news", False)
    print(f"news scan done in {time.time() - started:.0f}s", flush=True)
    scan("crawler", "SELECT title, created_at FROM ptt_article WHERE board='Stock' AND created_at >= %s",
         ("2018-01-01",), "ptt", True)
    rows = [(day, late, code, source, *counts) for (day, late, code, source), counts in totals.items()]
    return pd.DataFrame(rows, columns=["day", "late", "code", "source", "n", "bull", "bear"])


# ---------------------------------------------------------------------------
# Panels and scores
# ---------------------------------------------------------------------------

def _pivot(frame: pd.DataFrame, value: str, dates: pd.DatetimeIndex, codes: pd.Index) -> pd.DataFrame:
    work = frame[["date", "code", value]].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).drop_duplicates(["date", "code"], keep="last")
    matrix = work.pivot(index="date", columns="code", values=value)
    return matrix.reindex(index=dates, columns=codes).astype("float64")


def build_panels(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    price = raw["price"].copy()
    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price = price[(price["date"] >= STUDY_START) & price["date"].notna()]
    counts = price.groupby("date")["code"].count()
    dates = pd.DatetimeIndex(sorted(counts[counts >= 800].index))
    codes = pd.Index(sorted(price["code"].unique()))
    close = _pivot(price, "close", dates, codes)
    close = close.where(close > 0)
    volume = _pivot(price, "volume", dates, codes).where(lambda frame: frame >= 0)
    panels = {"close": close, "volume": volume}
    for key, columns in (
        ("inst", ["foreign", "trust", "dealer"]),
        ("margin", ["margin_bal", "short_bal", "short_to_margin", "margin_util"]),
        ("daytrade", ["dt_shares"]),
    ):
        for column in columns:
            panels[column] = _pivot(raw[key], column, dates, codes)
    text = raw["text"]
    if len(text):
        assigned = pd.to_datetime(text["day"]) + pd.to_timedelta(text["late"].astype(int), unit="D")
        position = dates.searchsorted(assigned.to_numpy(), side="left")
        frame = text.assign(pos=position)
        frame = frame[(frame["pos"] < len(dates)) & frame["code"].isin(codes)]
        for source, label in (("news", "news"), ("ptt", "ptt")):
            part = frame[frame["source"] == source]
            for value, suffix in (("n", "n"), ("bull", "bull"), ("bear", "bear")):
                grouped = part.groupby(["pos", "code"])[value].sum()
                matrix = np.zeros((len(dates), len(codes)), dtype="float64")
                if len(grouped):
                    rows = grouped.index.get_level_values(0).to_numpy()
                    cols = codes.get_indexer(grouped.index.get_level_values(1))
                    matrix[rows, cols] = grouped.to_numpy()
                panels[f"{label}_{suffix}"] = pd.DataFrame(matrix, index=dates, columns=codes)
    panels["_tdcc"] = raw["tdcc"]
    panels["_revenue"] = raw["revenue"]
    return panels


def _availability_matrix(
    weekly: pd.DataFrame, dates: pd.DatetimeIndex, codes: pd.Index,
    *, delay: pd.Timedelta, fill_limit: int,
) -> pd.DataFrame:
    """Place values at the first session on/after ``index + delay``; ffill."""

    positions = dates.searchsorted(weekly.index + delay, side="left")
    matrix = pd.DataFrame(np.nan, index=dates, columns=codes)
    for position, (_, row) in zip(positions, weekly.reindex(columns=codes).iterrows()):
        if position >= len(dates):
            continue
        current = matrix.iloc[position]
        matrix.iloc[position] = row.where(row.notna(), current)
    return matrix.ffill(limit=fill_limit)


def build_scores(panels: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    close, volume = panels["close"], panels["volume"]
    dates, codes = close.index, close.columns
    shares = volume * 1000.0
    avg_shares_20 = shares.rolling(20, min_periods=15).mean()
    returns = close.pct_change(fill_method=None)
    split_flag = (returns.abs() > SPLIT_MOVE).astype(float).fillna(0.0)

    def clean_window(window: int) -> pd.DataFrame:
        return split_flag.rolling(window, min_periods=1).sum() == 0

    scores: dict[str, pd.DataFrame] = {}

    def flow(name: str, series: pd.DataFrame, window: int) -> None:
        scores[name] = series.rolling(window, min_periods=max(3, window - 2)).sum() / avg_shares_20

    flow("foreign_flow_5d", panels["foreign"], 5)
    flow("foreign_flow_20d", panels["foreign"], 20)
    flow("trust_flow_5d", panels["trust"], 5)
    flow("dealer_flow_5d", panels["dealer"], 5)

    margin_bal = panels["margin_bal"]
    prior_margin = margin_bal.shift(5)
    scores["margin_balance_change_5d"] = (margin_bal / prior_margin - 1).where(prior_margin >= 200)
    scores["margin_utilization"] = panels["margin_util"]
    short_bal = panels["short_bal"]
    prior_short = short_bal.shift(5)
    scores["short_balance_change_5d"] = (short_bal / prior_short - 1).where(prior_short >= 100)
    scores["short_to_margin_ratio"] = panels["short_to_margin"]

    day_ratio = panels["dt_shares"] / shares.where(shares > 0)
    scores["daytrade_heat_5d"] = day_ratio.rolling(5, min_periods=3).mean().clip(upper=1.5)

    tdcc = panels["_tdcc"].copy()
    tdcc["date"] = pd.to_datetime(tdcc["date"], format="%Y%m%d", errors="coerce")
    tdcc = tdcc.dropna(subset=["date"]).drop_duplicates(["date", "code"], keep="last")
    def calendar_change(weekly: pd.DataFrame, relative: bool) -> pd.DataFrame:
        """Change versus the observation >=28 days earlier (gap <=42 days).

        The stored table has only ~15 weeks in 2025 and 5 in 2026, so a row
        shift of 4 would silently span months there.
        """

        index = weekly.index
        previous = index.searchsorted(index - pd.Timedelta(days=28), side="right") - 1
        clipped = np.clip(previous, 0, len(index) - 1)
        valid = (previous >= 0) & (index - index[clipped] <= pd.Timedelta(days=42))
        base = np.where(valid[:, None], weekly.iloc[clipped].to_numpy(), np.nan)
        base_frame = pd.DataFrame(base, index=index, columns=weekly.columns)
        change = (weekly / base_frame - 1) if relative else (weekly - base_frame)
        return change.replace([np.inf, -np.inf], np.nan)

    for name, column, relative in (
        ("tdcc_large_holder_change_4w", "large_pct", False),
        ("tdcc_holder_count_change_4w", "holders", True),
    ):
        weekly = tdcc.pivot(index="date", columns="code", values=column).sort_index()
        scores[name] = _availability_matrix(
            calendar_change(weekly, relative), dates, codes,
            delay=pd.Timedelta(days=4), fill_limit=8,
        )

    # Holder counts tend to rise after price drops: residualise the rank of the
    # holder-count change against the rank of the trailing 20-session return.
    holder = scores["tdcc_holder_count_change_4w"]
    trailing = (close / close.shift(20) - 1).where(clean_window(21))
    both = holder.notna() & trailing.notna()
    rank_holder = holder.where(both).rank(axis=1, pct=True)
    rank_trailing = trailing.where(both).rank(axis=1, pct=True)
    centred_h = rank_holder.sub(rank_holder.mean(axis=1), axis=0)
    centred_t = rank_trailing.sub(rank_trailing.mean(axis=1), axis=0)
    beta = (centred_h * centred_t).sum(axis=1) / (centred_t ** 2).sum(axis=1)
    scores["tdcc_holder_count_change_4w_resid20"] = centred_h - centred_t.mul(beta, axis=0)

    revenue = panels["_revenue"].copy()
    revenue["date"] = pd.to_datetime(revenue["date"], errors="coerce")
    revenue = revenue.dropna(subset=["date"]).drop_duplicates(["date", "code"], keep="last")
    monthly = revenue.pivot(index="date", columns="code", values="yoy").sort_index()
    # revenue for month M is due by the 10th of M+1; use the 11th
    monthly.index = [
        (month + pd.offsets.MonthBegin(1)) + pd.Timedelta(days=10) for month in monthly.index
    ]
    scores["revenue_yoy"] = _availability_matrix(
        monthly, dates, codes, delay=pd.Timedelta(days=0), fill_limit=25,
    )
    scores["revenue_yoy_acceleration"] = _availability_matrix(
        monthly - monthly.shift(1), dates, codes, delay=pd.Timedelta(days=0), fill_limit=25,
    )

    def attention_shock(count: pd.DataFrame) -> pd.DataFrame:
        five = count.rolling(5, min_periods=5).sum()
        baseline = np.log1p(five).shift(5).rolling(60, min_periods=40).mean()
        return (np.log1p(five) - baseline).where(five >= 2)

    if "news_n" in panels:
        scores["news_attention_shock_5d"] = attention_shock(panels["news_n"])
        bull5 = panels["news_bull"].rolling(5, min_periods=5).sum()
        bear5 = panels["news_bear"].rolling(5, min_periods=5).sum()
        news5 = panels["news_n"].rolling(5, min_periods=5).sum()
        scores["news_polarity_5d"] = ((bull5 - bear5) / (bull5 + bear5 + 1)).where(news5 >= 2)
        scores["ptt_attention_shock_5d"] = attention_shock(panels["ptt_n"])
    scores["short_term_reversal_5d"] = (close / close.shift(5) - 1).where(clean_window(6))
    scores["high_52w_proximity"] = (
        close / close.rolling(252, min_periods=200).max()
    ).where(clean_window(252))
    value_traded = (close * shares).where(shares > 0)
    amihud = (returns.abs() / value_traded * 1e6).rolling(20, min_periods=15).mean()
    scores["amihud_illiquidity_20d"] = amihud.where(clean_window(21))
    scores["volatility_20d"] = returns.rolling(20, min_periods=15).std().where(clean_window(21))
    return scores


def production_rule_directions(
    panels: dict[str, pd.DataFrame], eligible: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Replicate the live voting rules with their production constants."""

    close, volume = panels["close"], panels["volume"]
    shares = volume * 1000.0
    returns = close.pct_change(fill_method=None)
    split_flag = (returns.abs() > SPLIT_MOVE).astype(float).fillna(0.0)
    clean6 = split_flag.rolling(6, min_periods=1).sum() == 0
    clean21 = split_flag.rolling(21, min_periods=1).sum() == 0

    total = panels["foreign"] + panels["trust"] + panels["dealer"]
    thresholds = {1: 10.0, 3: 5.0, 5: 5.0}  # equity_order_flow.INSTITUTION_INTENSITY_PCT
    window_dirs = []
    for size, threshold in thresholds.items():
        tot = total.rolling(size, min_periods=size).sum()
        vol = shares.rolling(size, min_periods=size).sum()
        intensity = tot / vol.where(vol > 0) * 100
        sign = np.sign(tot)
        positive_days = (total > 0).astype(float).rolling(size, min_periods=size).sum()
        negative_days = (total < 0).astype(float).rolling(size, min_periods=size).sum()
        same_days = positive_days.where(sign > 0, negative_days)
        persistence = same_days >= (4 if size == 5 else size)
        aligned = (panels["foreign"] + panels["trust"]).rolling(size, min_periods=size).sum() * sign > 0
        active = persistence & aligned & (tot.abs() >= 100_000) & (intensity.abs() >= threshold)
        window_dirs.append(sign.where(active, 0.0).where(tot.notna()))
    stacked = np.stack([frame.to_numpy() for frame in window_dirs])
    has_up = np.nan_to_num(np.nanmax(np.where(np.isnan(stacked), -2, stacked), axis=0), nan=0) > 0
    has_down = np.nan_to_num(np.nanmin(np.where(np.isnan(stacked), 2, stacked), axis=0), nan=0) < 0
    institutions = pd.DataFrame(
        np.where(has_up & has_down, 0.0, np.where(has_up, 1.0, np.where(has_down, -1.0, 0.0))),
        index=close.index, columns=close.columns,
    )

    five_day = (close / close.shift(5) - 1).where(clean6)
    benchmark = five_day.where(eligible).mean(axis=1)
    price_rule = np.sign(five_day).where(
        (five_day.abs() >= 0.08) & (np.sign(five_day) * five_day.sub(benchmark, axis=0) > 0), 0.0,
    )

    prior_avg = volume.shift(1).rolling(20, min_periods=20).mean()
    one_day = returns.where(clean21)
    volume_rule = np.sign(one_day).where((volume >= 2 * prior_avg) & (one_day.abs() >= 0.03), 0.0)

    stack = institutions.fillna(0) + price_rule.fillna(0) + volume_rule.fillna(0)
    agree = np.sign(stack).where(stack.abs() >= 2, 0.0)
    return {
        "prod_institutions_rule": institutions,
        "prod_price_rule": price_rule,
        "prod_volume_rule": volume_rule,
        "prod_two_of_three_agree": agree,
    }


def eligibility(panels: dict[str, pd.DataFrame]) -> pd.DataFrame:
    close, volume = panels["close"], panels["volume"]
    value = (close * volume * 1000.0).where(volume > 0)
    median_value = value.rolling(20, min_periods=15).median()
    eligible = close.notna() & (median_value >= MIN_VALUE_NTD)
    eligible.loc[eligible.index < FIRST_SIGNAL_DATE] = False
    return eligible


def forward_excess(
    panels: dict[str, pd.DataFrame], eligible: pd.DataFrame, horizon: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Entry next close, exit ``horizon`` sessions later; (raw, excess)."""

    close = panels["close"]
    returns = close.pct_change(fill_method=None)
    flag = (returns.abs() > SPLIT_MOVE).astype(float).fillna(0.0).cumsum()
    entry = close.shift(-1)
    # a security that stopped trading for good (delisted) is settled at its last
    # traded close instead of being dropped, so delisting losses stay in the mean
    last_valid = close.apply(pd.Series.last_valid_index)
    cutoff = close.index[-1] - pd.Timedelta(days=60)
    gone = [code for code, day in last_valid.items() if day is not None and day < cutoff]
    close_exit = close.copy()
    if gone:
        close_exit[gone] = close[gone].ffill()
    exit_ = close_exit.shift(-(1 + horizon))
    window_flags = flag.shift(-(1 + horizon)) - flag.shift(-1)
    raw = (exit_ / entry - 1).where(window_flags == 0)
    raw = raw.where(eligible)
    excess = raw.sub(raw.mean(axis=1), axis=0)
    return raw * 100, excess * 100


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def _row_ic(score_rank: pd.DataFrame, fwd_rank: pd.DataFrame, count: pd.Series) -> pd.Series:
    sc = score_rank.sub(score_rank.mean(axis=1), axis=0)
    fc = fwd_rank.sub(fwd_rank.mean(axis=1), axis=0)
    numerator = (sc * fc).sum(axis=1)
    denominator = np.sqrt((sc ** 2).sum(axis=1) * (fc ** 2).sum(axis=1))
    ic = numerator / denominator
    return ic.where(count >= MIN_CROSS_SECTION)


def evaluate_cross_section(
    score: pd.DataFrame, sign: int, raw: pd.DataFrame, excess: pd.DataFrame,
    eligible: pd.DataFrame, horizon: int, rng: np.random.Generator,
) -> dict | None:
    signed = score * sign
    mask = signed.notna() & excess.notna() & eligible
    count = mask.sum(axis=1)
    if int((count >= MIN_CROSS_SECTION).sum()) < 30:
        return None
    s_masked = signed.where(mask)
    f_masked = excess.where(mask)
    score_rank = s_masked.rank(axis=1)
    fwd_rank = f_masked.rank(axis=1)
    ic = _row_ic(score_rank, fwd_rank, count)
    shuffled = pd.DataFrame(rng.random(mask.shape), index=mask.index, columns=mask.columns).where(mask).rank(axis=1)
    shuffle_ic = _row_ic(shuffled, fwd_rank, count)

    pct = score_rank.div(count, axis=0)
    top = pct > (1 - rs.DEFAULT_QUANTILE)
    bottom = pct <= rs.DEFAULT_QUANTILE
    top_excess = f_masked.where(top).mean(axis=1)
    bottom_excess = f_masked.where(bottom).mean(axis=1)
    top_raw = raw.where(mask).where(top).mean(axis=1)
    # Robustness to right-skew: winsorise each date's cross-section at 1%/99%
    # and also compare medians, because a few lottery-like winners (plus the
    # missing delisted losers) can flip a mean spread that the rank IC denies.
    low_cut = f_masked.quantile(0.01, axis=1)
    high_cut = f_masked.quantile(0.99, axis=1)
    f_win = f_masked.clip(lower=low_cut, upper=high_cut, axis=0)
    spread_win = f_win.where(top).mean(axis=1) - f_win.where(bottom).mean(axis=1)
    spread_median = f_masked.where(top).median(axis=1) - f_masked.where(bottom).median(axis=1)

    index = np.arange(len(mask.index))
    sample = ic.index[index % horizon == 0]
    out: dict = {"horizon": horizon, "universe_median": int(count[count > 0].median())}

    def block(series: pd.Series, name: str) -> None:
        values = series.loc[sample].dropna()
        out[name] = rs.mean_t_stat(values.tolist())
        d = values[values.index <= DISCOVERY_END]
        h = values[values.index > DISCOVERY_END]
        out[name + "_discovery"] = rs.mean_t_stat(d.tolist())
        out[name + "_holdout"] = rs.mean_t_stat(h.tolist())

    block(ic, "ic")
    block(shuffle_ic, "shuffle_ic")
    block(top_excess, "long_top_decile_excess_pct")
    block(bottom_excess, "bottom_decile_excess_pct")
    block(top_excess - bottom_excess, "long_short_excess_pct")
    block(top_raw, "long_top_decile_raw_pct")
    block(spread_win, "long_short_winsor_excess_pct")
    block(spread_median, "long_short_median_excess_pct")
    top_values = top_excess.loc[sample].dropna()
    out["long_top_decile_net_excess_pct"] = (
        None if top_values.empty else float(top_values.mean() - rs.ROUND_TRIP_COST_PCT)
    )
    ci = rs.bootstrap_mean_ci(ic.loc[sample].dropna().tolist(), seed=SEED)
    out["ic_bootstrap95"] = None if ci is None else [round(ci[0], 5), round(ci[1], 5)]
    by_year = ic.loc[sample].dropna().groupby(ic.loc[sample].dropna().index.year).mean()
    out["ic_by_year"] = {int(year): round(float(value), 4) for year, value in by_year.items()}
    return out


def evaluate_events(
    direction: pd.DataFrame, excess: pd.DataFrame, eligible: pd.DataFrame, horizon: int,
    rng: np.random.Generator | None = None,
) -> dict | None:
    """Event study: per-date mean of direction * excess return over events."""

    valid = direction.notna() & (direction != 0) & excess.notna() & eligible
    events_per_date = valid.sum(axis=1)
    usable = events_per_date > 0
    signed = direction.where(valid) * excess.where(valid)
    per_date = signed.sum(axis=1) / events_per_date.where(usable)
    index = np.arange(len(per_date.index))
    sample = per_date.index[index % horizon == 0]
    values = per_date.loc[sample].dropna()
    if len(values) < 20:
        return None
    out = {"horizon": horizon, "events": int(valid.sum().sum())}
    out["ic"] = rs.mean_t_stat(values.tolist())
    out["ic_discovery"] = rs.mean_t_stat(values[values.index <= DISCOVERY_END].tolist())
    out["ic_holdout"] = rs.mean_t_stat(values[values.index > DISCOVERY_END].tolist())
    if rng is not None:  # random-sign control: same events, coin-flip direction
        coin = pd.DataFrame(
            np.where(rng.random(direction.shape) < 0.5, 1.0, -1.0),
            index=direction.index, columns=direction.columns,
        )
        shuffled = (
            (coin.where(valid) * excess.where(valid)).sum(axis=1) / events_per_date.where(usable)
        ).loc[sample].dropna()
        out["shuffle_ic"] = rs.mean_t_stat(shuffled.tolist())
    else:
        out["shuffle_ic"] = {"n": len(values), "mean": 0.0, "t": 0.0, "p": 1.0}
    for label, side in (("bullish", 1.0), ("bearish", -1.0)):
        chosen = valid & (direction == side)
        count = chosen.sum(axis=1)
        mean_excess = (excess.where(chosen).sum(axis=1) / count.where(count > 0)).loc[sample].dropna()
        out[f"{label}_events_mean_excess_pct"] = rs.mean_t_stat(mean_excess.tolist())
    out["mean_directional_excess_pct"] = out["ic"]["mean"]
    out["net_directional_excess_pct"] = (
        None if out["ic"]["mean"] is None else out["ic"]["mean"] - rs.ROUND_TRIP_COST_PCT
    )
    out["ic_by_year"] = {
        int(year): round(float(value), 4)
        for year, value in values.groupby(values.index.year).mean().items()
    }
    return out


# ---------------------------------------------------------------------------
# PTT labelled-article events
# ---------------------------------------------------------------------------

_TEMPLATE_NOISE = re.compile(r"\(.*?請.*?\)|（.*?請.*?）|多/空/討論/心得|多空/討論/心得")


def parse_ptt_label(content: str) -> int:
    match = re.search(r"分類[：:](.*?)分析", content or "", flags=re.S)
    if not match:
        return 0
    text = _TEMPLATE_NOISE.sub("", match.group(1))
    bullish, bearish = "多" in text, "空" in text
    return 1 if bullish and not bearish else -1 if bearish and not bullish else 0


def parse_ptt_code(
    title: str, content: str, names: dict[str, str], valid_codes: set[str] | None = None,
) -> str | None:
    """First listed code in the 標的 field, else its unique full name; then the title.

    Four-digit numbers that are not listed codes (years such as 2026) are skipped
    instead of ending the search.
    """

    target = re.search(r"標的[：:]([^\n]{0,40})", content or "")
    for text in ((target.group(1) if target else ""), title or ""):
        for code in re.findall(r"(?<!\d)([1-9]\d{3})(?!\d)", text):
            if valid_codes is None or code in valid_codes:
                return code
        matches = {code for name, code in names.items() if name in text}
        if len(matches) == 1:
            return next(iter(matches))
    return None


def build_ptt_direction(raw: dict[str, pd.DataFrame], dates: pd.DatetimeIndex, codes: pd.Index) -> pd.DataFrame:
    master = raw["master"]
    owners: dict[str, set[str]] = {}
    for code, name in zip(master["code"], master["name"]):
        if isinstance(name, str) and len(name.strip()) >= 3 and re.fullmatch(r"[1-9]\d{3}", str(code)):
            owners.setdefault(name.strip(), set()).add(str(code))
    names = {name: next(iter(owned)) for name, owned in owners.items() if len(owned) == 1}
    valid_codes = {str(code) for code in codes}
    articles = raw["ptt"].copy()
    articles["created_at"] = pd.to_datetime(articles["created_at"])
    events = []
    for created_at, title, content in zip(articles["created_at"], articles["title"], articles["content"]):
        label = parse_ptt_label(content)
        if not label:
            continue
        code = parse_ptt_code(title, content, names, valid_codes)
        if not code:
            continue
        # after the 13:30 close, or on a non-session day, the post belongs to the next session
        day = created_at.normalize() + pd.Timedelta(days=int(_after_cutoff(created_at)))
        events.append((day, code, label))
    frame = pd.DataFrame(events, columns=["day", "code", "label"])
    direction = pd.DataFrame(0.0, index=dates, columns=codes)
    if frame.empty:
        return direction
    frame["pos"] = dates.searchsorted(frame["day"], side="left")
    frame = frame[(frame["pos"] < len(dates)) & frame["code"].isin(codes)]
    grouped = frame.groupby(["pos", "code"])["label"].sum()
    for (position, code), value in grouped.items():
        direction.iat[position, codes.get_loc(code)] = np.sign(value)
    return direction


# ---------------------------------------------------------------------------
# Market-level overlay (non-voting)
# ---------------------------------------------------------------------------

def market_overlay(panels: dict[str, pd.DataFrame], eligible: pd.DataFrame, scores: dict[str, pd.DataFrame]) -> list[dict]:
    close, volume = panels["close"], panels["volume"]
    shares = volume * 1000.0
    series: dict[str, pd.Series] = {}
    elig = eligible.astype(bool)
    series["margin_balance_change_5d_mean"] = scores["margin_balance_change_5d"].where(elig).mean(axis=1)
    series["foreign_net_to_volume_5d"] = (
        panels["foreign"].where(elig).rolling(5, min_periods=3).sum().sum(axis=1)
        / shares.where(elig).rolling(5, min_periods=3).sum().sum(axis=1)
    )
    series["daytrade_ratio_5d"] = (
        panels["dt_shares"].where(elig).rolling(5, min_periods=3).sum().sum(axis=1)
        / shares.where(elig).rolling(5, min_periods=3).sum().sum(axis=1)
    )
    series["breadth_above_ma20"] = ((close > close.rolling(20, min_periods=15).mean()).where(elig)).mean(axis=1)
    series["short_to_margin_ratio_mean"] = scores["short_to_margin_ratio"].where(elig).mean(axis=1)
    out = []
    for horizon in (5, 20):
        entry = close.shift(-1)
        exit_ = close.shift(-(1 + horizon))
        universe_forward = ((exit_ / entry - 1).where(elig)).mean(axis=1) * 100
        for name, value in series.items():
            z = (value - value.rolling(250, min_periods=120).mean()) / value.rolling(250, min_periods=120).std()
            frame = pd.DataFrame({"z": z, "fwd": universe_forward}).dropna()
            frame = frame[frame.index >= FIRST_SIGNAL_DATE]
            frame = frame.iloc[::horizon]
            if len(frame) < 30:
                continue
            ic = frame["z"].rank().corr(frame["fwd"].rank())
            n = len(frame)
            t_stat = ic * math.sqrt((n - 2) / max(1e-9, 1 - ic * ic))
            high = frame["fwd"][frame["z"] >= frame["z"].quantile(2 / 3)].mean()
            low = frame["fwd"][frame["z"] <= frame["z"].quantile(1 / 3)].mean()
            out.append({
                "series": name, "horizon": horizon, "n": n, "rank_corr": round(float(ic), 4),
                "t": round(float(t_stat), 2), "p": rs.normal_two_sided_p(t_stat),
                "mean_fwd_when_high_pct": round(float(high), 3),
                "mean_fwd_when_low_pct": round(float(low), 3),
            })
    qs = rs.benjamini_hochberg([row["p"] for row in out])
    for row, q in zip(out, qs):
        row["q_bh"] = None if q is None else round(q, 4)
    return out


# ---------------------------------------------------------------------------
# Lookahead / truncation test
# ---------------------------------------------------------------------------

def truncation_test(panels: dict[str, pd.DataFrame], full_scores: dict[str, pd.DataFrame], cutoffs: list[str]) -> dict:
    results: dict[str, dict] = {name: {"max_abs_diff": 0.0, "mismatch_cells": 0} for name in full_scores}
    dates = panels["close"].index
    for cutoff in cutoffs:
        cut = pd.Timestamp(cutoff)
        sliced = {
            key: (value.loc[value.index <= cut] if isinstance(value, pd.DataFrame) and key[0] != "_" else value)
            for key, value in panels.items()
        }
        for key in ("_tdcc", "_revenue"):
            frame = panels[key].copy()
            frame["date"] = pd.to_datetime(frame["date"], format="%Y%m%d" if key == "_tdcc" else None, errors="coerce")
            # data published after the cutoff cannot exist on the cutoff date
            if key == "_tdcc":
                available = frame["date"] + pd.Timedelta(days=4)
            else:
                available = frame["date"] + pd.offsets.MonthBegin(1) + pd.Timedelta(days=10)
            frame = frame[available <= cut]
            sliced[key] = frame.assign(date=frame["date"].dt.strftime("%Y%m%d" if key == "_tdcc" else "%Y-%m-%d"))
        part = build_scores(sliced)
        for name, matrix in part.items():
            full = full_scores[name].loc[full_scores[name].index <= cut]
            tail = full.index >= cut - pd.Timedelta(days=45)
            a, b = matrix.loc[tail].to_numpy(), full.loc[tail].to_numpy()
            both = np.isfinite(a) & np.isfinite(b)
            mismatch = int((np.isfinite(a) != np.isfinite(b)).sum())
            diff = float(np.abs(a[both] - b[both]).max()) if both.any() else 0.0
            results[name]["max_abs_diff"] = max(results[name]["max_abs_diff"], diff)
            results[name]["mismatch_cells"] += mismatch
    for name, value in results.items():
        value["clean"] = value["max_abs_diff"] < 1e-8 and value["mismatch_cells"] == 0
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(args: argparse.Namespace) -> dict:
    started = time.time()
    cache_dir = Path(args.cache) if args.cache else None
    raw = load_raw(cache_dir)
    panels = build_panels(raw)
    scores = build_scores(panels)
    eligible = eligibility(panels)
    dates, codes = panels["close"].index, panels["close"].columns
    print(f"panels ready: {len(dates)} sessions x {len(codes)} codes ({time.time() - started:.0f}s)", flush=True)
    rng = np.random.default_rng(SEED)

    forward = {h: forward_excess(panels, eligible, h) for h in (1, 5, 10, 20, 60)}
    results: list[dict] = []
    for spec in rs.FAMILIES:
        for horizon in spec.horizons:
            raw_ret, excess = forward[horizon]
            if spec.name in rs.EVENT_FAMILIES:
                continue
            evaluated = evaluate_cross_section(
                scores[spec.name], spec.sign, raw_ret, excess, eligible, horizon, rng,
            )
            if evaluated:
                evaluated.update(family=spec.name, source=spec.source, sign=spec.sign, kind="cross_section")
                results.append(evaluated)
        print(f"evaluated {spec.name} ({time.time() - started:.0f}s)", flush=True)

    event_directions = production_rule_directions(panels, eligible)
    event_directions["ptt_label_event"] = build_ptt_direction(raw, dates, codes)
    # Text coverage is sparse (a few hundred names per day), so these are event
    # studies rather than decile ranks. Thresholds are fixed before looking.
    five_news = panels["news_n"].rolling(5, min_periods=5).sum()
    five_ptt = panels["ptt_n"].rolling(5, min_periods=5).sum()
    shock_news = scores["news_attention_shock_5d"]
    shock_ptt = scores["ptt_attention_shock_5d"]
    polarity = scores["news_polarity_5d"]
    labelled = (panels["news_bull"] + panels["news_bear"]).rolling(5, min_periods=5).sum()
    event_directions["news_attention_shock_5d"] = pd.DataFrame(
        np.where((five_news >= 3) & (shock_news >= 1.0), 1.0, 0.0), index=dates, columns=codes)
    event_directions["ptt_attention_shock_5d"] = pd.DataFrame(
        np.where((five_ptt >= 3) & (shock_ptt >= 1.0), 1.0, 0.0), index=dates, columns=codes)
    event_directions["news_polarity_5d"] = pd.DataFrame(
        np.where((labelled >= 2) & (polarity.abs() >= 0.5), np.sign(polarity.fillna(0.0)), 0.0),
        index=dates, columns=codes)
    for name, direction in event_directions.items():
        spec = rs.FAMILY_BY_NAME[name]
        for horizon in spec.horizons:
            evaluated = evaluate_events(
                direction * spec.sign, forward[horizon][1], eligible, horizon, rng,
            )
            if evaluated:
                evaluated.update(family=name, source=spec.source, sign=spec.sign, kind="event")
                results.append(evaluated)
        print(f"evaluated event family {name} ({time.time() - started:.0f}s)", flush=True)

    # Supplementary control (not part of the BH set): title polarity words such as
    # 大漲/跌停 restate the price move that already happened, so split the polarity
    # events by whether the trailing 5-session return agrees with the title.
    close_clean = panels["close"]
    trailing_5 = close_clean / close_clean.shift(5) - 1
    polarity_events = event_directions["news_polarity_5d"]
    price_control = {}
    for label, mask in (
        ("title_agrees_with_trailing_return", np.sign(trailing_5) == polarity_events),
        ("title_disagrees_or_flat", trailing_5.notna() & (np.sign(trailing_5) != polarity_events)),
    ):
        for horizon in (5, 10, 20):
            part = evaluate_events(
                polarity_events.where(mask, 0.0), forward[horizon][1], eligible, horizon, None,
            )
            if part:
                price_control[f"{label}_h{horizon}"] = {
                    "events": part["events"], "mean_excess_pct": part["ic"]["mean"],
                    "t": part["ic"]["t"], "discovery": part["ic_discovery"]["mean"],
                    "holdout": part["ic_holdout"]["mean"],
                }
    qs = rs.benjamini_hochberg([row["ic"]["p"] for row in results])
    for row, q in zip(results, qs):
        row["q_bh"] = q
        control = row["shuffle_ic"]["t"]
        shuffle_ok = control is None or abs(control) < SHUFFLE_T_LIMIT
        row["verdict"] = rs.family_verdict(
            discovery=row["ic_discovery"], holdout=row["ic_holdout"], q_value=q, shuffle_ok=shuffle_ok,
            spread_discovery=row.get("long_short_winsor_excess_pct_discovery"),
            spread_holdout=row.get("long_short_winsor_excess_pct_holdout"),
        )
    truncation = truncation_test(panels, scores, args.cutoffs)
    overlay = market_overlay(panels, eligible, scores)
    summary = {
        "policy_version": rs.RESEARCH_POLICY_VERSION,
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "sessions": len(dates),
        "first_signal_date": str(FIRST_SIGNAL_DATE.date()),
        "last_session": str(dates[-1].date()),
        "discovery_end": str(DISCOVERY_END.date()),
        "tests_counted": len(results),
        "round_trip_cost_pct": rs.ROUND_TRIP_COST_PCT,
        "truncation_test": truncation,
        "market_overlay": overlay,
        "news_polarity_price_control": price_control,
        "results": results,
        "limitations": [
            "stored prices are unadjusted; windows with a >10.5% daily move are excluded",
            "tables record collection time, not immutable vintages",
            "no delisting history: survivorship favours long signals on small caps",
            "no borrow cost or lending availability: short legs are research only",
            "commission discounts, slippage and capacity are not modelled beyond the round-trip cost",
        ],
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="/tmp/research_signal_family_backtest.json")
    parser.add_argument("--cache", default="/tmp/rsfb_cache")
    parser.add_argument("--cutoffs", nargs="*", default=["2019-06-28", "2022-12-30", "2025-06-30"])
    args = parser.parse_args()
    summary = run(args)
    Path(args.out).write_text(json.dumps(summary, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
