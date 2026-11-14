"""Point-in-time validation of the pre-registered Taiwan equity signal rules (research CLI).

Rule definitions, publication lags, the date-level evidence table, the statistics and the
verdict gate all live in ``dags/source_extension`` (``signal_rules`` and ``rule_lifecycle``)
and are shared with the weekly revalidation job, so this script only loads data and
prints/saves the snapshot. Reads parquet extracts of the ``stock`` database
(``--data-dir``) and scores each rule with the trading *date* as the unit of inference:

* inputs are attributed to the session on which they were really published (margin data
  one session later, TDCC three sessions later); entry is the next session's close
* forward return over H sessions minus the same-day liquid-universe MEAN
* per-date mean excess -> time-series t-stat (Newey-West, lag = horizon)
* in-sample (< 2023-01-01) vs untouched out-of-sample (>= 2023-01-01)
* round-trip cost deducted, Benjamini-Hochberg across the fixed pre-registered family
* ``passes`` is a snapshot of the full gate, NOT a rule's status: status is decided by the
  weekly long-horizon revalidation with hysteresis (``rule_lifecycle.decide``)

Prices are unadjusted for dividends, so ex-dividend gaps add noise that is common to
all names; the same-day benchmark absorbs most of it. ``--margin-lag 0`` reproduces the
first (leaky) version of the report.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from source_extension.signal_rules import (  # noqa: E402
    HORIZONS, MIN_TURNOVER_NTD, RET_CLIP, ROUND_TRIP_COST_PCT, compute_panel,
)

from source_extension.rule_lifecycle import (  # noqa: E402
    SPLIT_DATE, daily_excess_table, summarize, verdicts,
)

def load(data_dir: Path):
    price = pd.read_parquet(data_dir / "price.parquet")
    price["date"] = pd.to_datetime(price["資料日期"])
    price = price[price["date"] >= "2015-06-01"]
    price = price.rename(columns={"證券代號": "code", "收盤": "close", "成交量": "vol"})
    price = price[price["code"].str.fullmatch(r"[1-9]\d{3}")]
    price = price.drop_duplicates(["date", "code"]).sort_values(["code", "date"])
    inst = pd.read_parquet(data_dir / "inst.parquet")
    inst["date"] = pd.to_datetime(inst["資料日期"])
    inst = inst.rename(columns={"證券代號": "code", "外資買賣超股數": "foreign", "投信買賣超股數": "trust",
                                 "三大法人買賣超股數": "total"})
    inst = inst.drop_duplicates(["date", "code"])
    margin = pd.read_parquet(data_dir / "margin.parquet")
    margin["date"] = pd.to_datetime(margin["資料日期"])
    margin = margin.rename(columns={"證券代號": "code", "融資剩餘額": "margin_bal", "券資比": "short_ratio",
                                     "融資使用率": "margin_use"})
    margin = margin.drop_duplicates(["date", "code"])
    tdcc = pd.read_parquet(data_dir / "tdcc.parquet")
    tdcc = tdcc[tdcc["資料日期"].astype(str).str.fullmatch(r"\d{8}")].copy()
    tdcc["date"] = pd.to_datetime(tdcc["資料日期"].astype(str), format="%Y%m%d")
    tdcc = tdcc.rename(columns={"證券代號": "code", ">400張大股東持有百分比": "big_pct"})
    tdcc["big_pct"] = pd.to_numeric(tdcc["big_pct"], errors="coerce")
    tdcc = tdcc.drop_duplicates(["date", "code"])
    return price, inst, margin, tdcc


def evaluate(panel):
    """Date-level evidence table and its window summary (shared with the weekly revalidation)."""
    daily = daily_excess_table(panel)
    return daily, summarize(daily, SPLIT_DATE)


def buyback_event_study(data_dir: Path, close, liquid, sessions):
    bb = pd.read_parquet(data_dir / "buyback.parquet")
    out = []
    for _, r in bb.iterrows():
        try:
            y, m, d = str(r["董事會決議日期"]).split("/")
            dt = pd.Timestamp(int(y) + 1911, int(m), int(d))
        except Exception:
            continue
        code = str(r["證券代號"])
        if code not in close.columns or dt < sessions[0] or dt > sessions[-1]:
            continue
        pos = sessions.searchsorted(dt)
        if pos + 21 >= len(sessions):
            continue
        entry = close[code].iloc[pos + 1]
        row = {"code": code, "date": sessions[pos]}
        for h in HORIZONS:
            ex = close[code].iloc[pos + 1 + h] / entry - 1
            day = (close.iloc[pos + 1 + h] / close.iloc[pos + 1] - 1).where(liquid.iloc[pos])
            row[f"ex{h}"] = (np.clip(ex, -RET_CLIP, RET_CLIP) - np.clip(day, -RET_CLIP, RET_CLIP).mean()) * 100
        out.append(row)
    ev = pd.DataFrame(out)
    summary = {}
    for h in HORIZONS:
        s = ev[f"ex{h}"].dropna()
        summary[f"h{h}"] = dict(events=int(len(s)), mean_excess_pct=float(s.mean()), median=float(s.median()),
                                win=float((s > 0).mean()), t=float(s.mean() / (s.std() / math.sqrt(len(s)))))
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--daily-out", help="write the date-level evidence table (parquet) used to seed the database")
    ap.add_argument("--margin-lag", type=int, default=None,
                    help="sessions before margin data is usable (default: the rule definition's lag; 0 reproduces the first report)")
    a = ap.parse_args()
    data_dir = Path(a.data_dir)
    price, inst, margin, tdcc = load(data_dir)
    panel = compute_panel(price, inst, margin, tdcc, margin_lag=a.margin_lag)
    sessions, close, liquid = panel.sessions, panel.close, panel.liquid
    daily, res = evaluate(panel)
    ver = verdicts(res)
    if a.daily_out:
        daily.to_parquet(a.daily_out, index=False)
    bb = buyback_event_study(data_dir, close, liquid, sessions)
    payload = {
        "split_date": str(SPLIT_DATE.date()), "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
        "universe": f"4-digit stocks, 20d median turnover >= {MIN_TURNOVER_NTD}",
        "sessions": [str(sessions[0].date()), str(sessions[-1].date())],
        "results": json.loads(res.to_json(orient="records")),
        "verdicts": json.loads(ver.to_json(orient="records")),
        "buyback_event_study": bb,
    }
    Path(a.out).write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    pd.set_option("display.width", 220)
    cols = ["signal", "horizon", "dates", "mean_excess_pct", "t_nw", "stock_win_rate_after_cost", "base_win_rate_after_cost", "is_mean", "bh_pass", "passes"]
    print(ver[cols].sort_values(["passes", "t_nw"], ascending=False).to_string())
    print(json.dumps(bb, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
