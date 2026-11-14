"""Point-in-time validation of candidate Taiwan equity signals (research only).

Reads parquet extracts of the ``stock`` database (see ``--data-dir``), builds a
pre-registered set of signals, and scores each one with the trading *date* as the
unit of inference:

* feature known after the close of day D, entry at the close of D+1 (no look-ahead)
* forward return over H sessions minus the same-day universe median
* per-date mean excess -> time-series t-stat (Newey-West, lag = horizon)
* in-sample (< SPLIT_DATE) vs untouched out-of-sample (>= SPLIT_DATE)
* round-trip cost deducted, Benjamini-Hochberg across the pre-registered family
* a signal passes only if the OOS edge is positive after cost, significant after BH,
  and has the same sign in-sample

Prices are unadjusted for dividends, so ex-dividend gaps add noise that is common to
all names; the same-day median benchmark absorbs most of it.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

SPLIT_DATE = pd.Timestamp("2023-01-01")
HORIZONS = (5, 10, 20)
ROUND_TRIP_COST_PCT = 0.6
MIN_TURNOVER_NTD = 50_000_000
MIN_NAMES_PER_DATE = 3
RET_CLIP = 0.5
FAMILY_ALPHA = 0.10

SIGNALS = [
    "foreign_buy_5d_top",
    "trust_buy_5d_top",
    "inst_buy_streak_5d",
    "foreign_trust_both_buy_3d",
    "breakout_60d_volume",
    "volume_surge_3x_up",
    "margin_cleanup_up",
    "short_squeeze_setup",
    "tdcc_big_holder_up",
    "oversold_foreign_buy",
    "foreign_sell_5d_bottom",
    "tdcc_big_holder_down",
]


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


def build_panel(price, inst, margin, tdcc):
    sessions = pd.Index(sorted(price["date"].unique()))
    p = price.set_index(["date", "code"])
    close = p["close"].unstack("code").reindex(sessions)
    vol = p["vol"].unstack("code").reindex(sessions)
    turnover = close * vol * 1000.0
    liquid = turnover.rolling(20, min_periods=15).median() >= MIN_TURNOVER_NTD
    shares_vol = (vol * 1000.0)

    def wide(df, col):
        return df.set_index(["date", "code"])[col].unstack("code").reindex(sessions)

    foreign = wide(inst, "foreign").reindex(columns=close.columns)
    trust = wide(inst, "trust").reindex(columns=close.columns)
    total = wide(inst, "total").reindex(columns=close.columns)
    avg_vol20 = shares_vol.rolling(20, min_periods=15).mean()
    f5 = foreign.rolling(5, min_periods=4).sum() / avg_vol20
    t5 = trust.rolling(5, min_periods=4).sum() / avg_vol20
    ret20 = close / close.shift(20) - 1
    ret5 = close / close.shift(5) - 1
    vr = shares_vol / avg_vol20.shift(1)
    hi60 = close.rolling(60, min_periods=50).max().shift(1)
    ms = wide(margin, "margin_bal").reindex(columns=close.columns)
    sr = wide(margin, "short_ratio").reindex(columns=close.columns)
    margin_chg5 = ms / ms.shift(5) - 1

    sig = {}
    sig["foreign_buy_5d_top"] = (f5 >= 0.05) & (f5.rank(axis=1, pct=True) >= 0.95)
    sig["trust_buy_5d_top"] = (t5 >= 0.02) & (t5.rank(axis=1, pct=True) >= 0.95)
    sig["inst_buy_streak_5d"] = (total.gt(0).rolling(5).sum() == 5) & (f5 > 0.02)
    sig["foreign_trust_both_buy_3d"] = (
        (foreign.gt(0).rolling(3).sum() == 3) & (trust.gt(0).rolling(3).sum() == 3) & (f5 > 0.02))
    sig["breakout_60d_volume"] = (close > hi60) & (vr >= 2.0)
    sig["volume_surge_3x_up"] = (vr >= 3.0) & (ret5 > 0.03)
    sig["margin_cleanup_up"] = (margin_chg5 <= -0.05) & (ret5 > 0.02)
    sig["short_squeeze_setup"] = (sr >= 30) & (ret5 > 0.03)
    sig["oversold_foreign_buy"] = (ret20 <= -0.20) & (f5 > 0.03)
    sig["foreign_sell_5d_bottom"] = (f5 <= -0.05) & (f5.rank(axis=1, pct=True) <= 0.05)

    # TDCC weekly holder share: usable after a 3-session publication lag
    big = tdcc.set_index(["date", "code"])["big_pct"].unstack("code").sort_index()
    d_big = big - big.shift(1)
    d_big_lagged = pd.DataFrame(np.nan, index=sessions, columns=close.columns)
    for dt, row in d_big.iterrows():
        pos = sessions.searchsorted(dt)
        avail = pos + 3
        if avail < len(sessions):
            d_big_lagged.iloc[avail] = row.reindex(close.columns).values
    sig["tdcc_big_holder_up"] = d_big_lagged >= 1.0
    sig["tdcc_big_holder_down"] = d_big_lagged <= -1.0
    for k in sig:
        sig[k] = (sig[k].fillna(False)) & liquid

    fwd = {}
    for h in HORIZONS:
        entry = close.shift(-1)
        fwd[h] = ((close.shift(-1 - h) / entry - 1).clip(-RET_CLIP, RET_CLIP)) * 100.0
    return sessions, close, liquid, sig, fwd


def newey_west_t(x: np.ndarray, lag: int) -> float:
    n = len(x)
    if n < 10:
        return float("nan")
    mu = x.mean()
    e = x - mu
    var = (e @ e) / n
    for k in range(1, min(lag, n - 1) + 1):
        w = 1 - k / (lag + 1)
        var += 2 * w * (e[k:] @ e[:-k]) / n
    if var <= 0:
        return float("nan")
    return mu / math.sqrt(var / n)


def evaluate(sig, fwd, liquid, sessions):
    rows = []
    for name in SIGNALS:
        for h in HORIZONS:
            f = fwd[h]
            base = f.where(liquid).mean(axis=1)  # mean, not median: skewed returns bias a mean-vs-median test
            excess = f.sub(base, axis=0)
            s = sig[name] & f.notna()
            cnt = s.sum(axis=1)
            day_mean = excess.where(s).mean(axis=1)
            day_mean = day_mean[cnt >= MIN_NAMES_PER_DATE].dropna()
            # step across non-overlapping-ish dates by NW; also stock-level win rate
            stock_ex = excess.where(s).stack()
            univ_ex = excess.where(liquid & f.notna()).stack()
            for label, mask in (("IS", day_mean.index < SPLIT_DATE), ("OOS", day_mean.index >= SPLIT_DATE)):
                x = day_mean[mask].values
                if len(x) < 10:
                    rows.append(dict(signal=name, horizon=h, window=label, dates=int(len(x))))
                    continue
                idx = day_mean.index[mask]
                se = stock_ex[(stock_ex.index.get_level_values(0) >= idx.min()) &
                              (stock_ex.index.get_level_values(0) <= idx.max())]
                se = se[se.index.get_level_values(0).isin(idx)]
                rows.append(dict(
                    signal=name, horizon=h, window=label, dates=int(len(x)),
                    names=int(len(se)), mean_excess_pct=float(x.mean()),
                    mean_excess_after_cost_pct=float(x.mean() - ROUND_TRIP_COST_PCT),
                    t_nw=float(newey_west_t(x, h)),
                    stock_win_rate=float((se > 0).mean()),
                    date_win_rate=float((x > 0).mean()),
                    stock_win_rate_after_cost=float((se > ROUND_TRIP_COST_PCT).mean()),
                    base_win_rate_after_cost=float((univ_ex[(univ_ex.index.get_level_values(0) >= idx.min()) & (univ_ex.index.get_level_values(0) <= idx.max())] > ROUND_TRIP_COST_PCT).mean()),
                    avg_names_per_date=float(cnt[day_mean.index[mask]].mean()),
                ))
    return pd.DataFrame(rows)


def bh_flags(pvals: pd.Series, alpha: float) -> pd.Series:
    order = pvals.sort_values().index
    m = len(pvals)
    thresh = pd.Series({idx: alpha * (i + 1) / m for i, idx in enumerate(order)})
    passed = pvals.loc[order] <= thresh
    if passed.any():
        last = passed[passed].index[-1]
        cutoff = pvals.loc[last]
        return pvals <= cutoff
    return pd.Series(False, index=pvals.index)


def p_from_t(t):
    if t is None or (isinstance(t, float) and math.isnan(t)):
        return 1.0
    # two-sided normal approximation
    return math.erfc(abs(t) / math.sqrt(2))


def verdicts(res: pd.DataFrame) -> pd.DataFrame:
    oos = res[res["window"] == "OOS"].dropna(subset=["t_nw"]).copy()
    ins = res[res["window"] == "IS"].set_index(["signal", "horizon"])
    oos["p"] = oos["t_nw"].map(p_from_t)
    oos = oos.set_index(["signal", "horizon"])
    # one-sided family: only count positive OOS edges as discoveries
    oos["p_one"] = np.where(oos["t_nw"] > 0, oos["p"] / 2, 1.0)
    oos["bh_pass"] = bh_flags(oos["p_one"], FAMILY_ALPHA)
    oos["is_mean"] = ins["mean_excess_pct"].reindex(oos.index)
    oos["is_t"] = ins["t_nw"].reindex(oos.index)
    oos["passes"] = (
        oos["bh_pass"] & (oos["mean_excess_after_cost_pct"] > 0)
        & (oos["is_mean"] > 0)
        & (oos["stock_win_rate_after_cost"] >= oos["base_win_rate_after_cost"] + 0.02)
    )
    return oos.reset_index()


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
    a = ap.parse_args()
    data_dir = Path(a.data_dir)
    price, inst, margin, tdcc = load(data_dir)
    sessions, close, liquid, sig, fwd = build_panel(price, inst, margin, tdcc)
    res = evaluate(sig, fwd, liquid, sessions)
    ver = verdicts(res)
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
