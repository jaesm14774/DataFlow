"""Chip-signal backtest core.
Entry: next trading day OPEN after signal date t. Exit horizons H (trading days). Touch: close >= entry*1.07 any day within 240 td.
Baseline: same-date mean over the eligible universe (removes bull-market drift). Significance: month-level clustering.
"""
import numpy as np, pandas as pd
from numpy.lib.stride_tricks import sliding_window_view as swv
import os
S = os.environ.get('CHIP_DATA', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
os.makedirs(S, exist_ok=True)
HS = (20, 60, 120, 240)
TOUCH_W, TARGET = 240, 0.07


def load_px():
    px = pd.read_parquet(f'{S}/px.parquet')
    px = px[px.code.str.match(r'^[1-9]\d{3}$')].copy()
    px['d'] = pd.to_datetime(px.d, errors='coerce')
    px = px[(px.d >= '2010-01-01') & (px.d <= '2026-12-31')]
    for c in 'ohlcv':
        px[c] = pd.to_numeric(px[c], errors='coerce')
    px = px[(px.c > 0)].drop_duplicates(['code', 'd']).sort_values(['code', 'd']).reset_index(drop=True)
    px.loc[px.o <= 0, 'o'] = np.nan
    return px


def build_outcomes(px):
    """Per (code,d): forward outcomes of entering at next-day open."""
    out = []
    for code, g in px.groupby('code', sort=False):
        c = g.c.values; o = g.o.values; n = len(g)
        r = np.full((n, len(HS) + 3), np.nan)  # ret_H..., touch_ret(max), first_hit_day, min_close_60
        entry = np.full(n, np.nan); entry[:-1] = o[1:]
        ratio = np.r_[1, c[1:] / c[:-1]]
        jump = (ratio < 0.75) | (ratio > 1.3)  # split / capital reduction contamination
        cj = np.cumsum(jump)
        for k, H in enumerate(HS):
            if n > H:
                r[:n - H, k] = c[H:] / entry[:n - H] - 1
                bad = (cj[H:] - cj[1:n - H + 1]) > 0  # window t+1..t+H contains a jump -> invalid
                r[:n - H, k][bad] = np.nan
        W = TOUCH_W
        if n > W + 1:
            win = swv(c[1:], W)  # win[i] = c[i+1 .. i+W]
            mx = win.max(axis=1)
            r[:len(mx), len(HS)] = mx / entry[:len(mx)] - 1
            hit = win >= (entry[:len(mx), None] * (1 + TARGET))
            fh = np.where(hit.any(axis=1), hit.argmax(axis=1) + 1, np.nan)
            r[:len(mx), len(HS) + 1] = fh
            badw = (cj[W:] - cj[1:len(mx) + 1]) > 0
            r[:len(mx)][badw[:len(mx)], len(HS):len(HS) + 2] = np.nan
        w60 = 60
        if n > w60 + 1:
            mn = swv(c[1:], w60).min(axis=1)
            r[:len(mn), len(HS) + 2] = mn / entry[:len(mn)] - 1
        df = pd.DataFrame(r, columns=[f'r{H}' for H in HS] + ['maxret', 'fhit', 'min60'])
        df['code'] = code; df['d'] = g.d.values; df['entry'] = entry
        out.append(df)
    res = pd.concat(out, ignore_index=True)
    res['hit7'] = (res.maxret >= TARGET).astype(float).where(res.maxret.notna())
    res['win7_240'] = (res.r240 >= TARGET).astype(float).where(res.r240.notna())
    for H in HS[:3]:
        res[f'w7_{H}'] = (res[f'r{H}'] >= TARGET).astype(float).where(res[f'r{H}'].notna())
    return res


def add_liquidity(px, res):
    px = px.copy()
    px['val'] = px.c * px.v * 1000
    px['adv20'] = px.groupby('code').val.transform(lambda s: s.rolling(20, min_periods=15).mean())
    px['mom20'] = px.groupby('code').c.pct_change(20)
    px['mom60'] = px.groupby('code').c.pct_change(60)
    px['mom120'] = px.groupby('code').c.pct_change(120)
    px['hi250'] = px.c / px.groupby('code').c.transform(lambda s: s.rolling(250, min_periods=120).max())
    return res.merge(px[['code', 'd', 'c', 'v', 'val', 'adv20', 'mom20', 'mom60', 'mom120', 'hi250']], on=['code', 'd'], how='left')


OUTCOMES = ['r20', 'r60', 'r120', 'r240', 'hit7', 'w7_60', 'w7_120', 'win7_240']


def add_excess(df, min_adv=1e7):
    """df: outcome panel with eligibility; adds ex_* = outcome - same-date eligible mean."""
    df = df.copy()
    df['gap'] = df.entry / df.c - 1
    df['elig'] = (df.adv20 >= min_adv) & (df.gap < 0.085) & df.entry.notna()
    base = df[df.elig].groupby('d')[OUTCOMES].mean()
    for c in OUTCOMES:
        df['ex_' + c] = df[c] - df.d.map(base[c])
    df['bench_n'] = df.d.map(df[df.elig].groupby('d').size())
    return df


def evaluate(ev, label='', min_n=30):
    """ev: events subset (with ex_* cols). Month-clustered stats."""
    if len(ev) == 0 or 'elig' not in ev: return {'signal': label, 'n': 0}
    ev = ev[ev.elig]
    row = {'signal': label, 'n': len(ev), 'months': ev.d.dt.to_period('M').nunique()}
    if len(ev) < min_n:
        return row
    ev = ev.assign(m=ev.d.dt.to_period('M'))
    for c in ['r60', 'r240', 'hit7', 'win7_240']:
        e = ev[['m', c, 'ex_' + c]].dropna()
        if len(e) < min_n: continue
        mo = e.groupby('m')['ex_' + c].mean()
        t = mo.mean() / (mo.std(ddof=1) / np.sqrt(len(mo))) if len(mo) > 3 and mo.std() > 0 else np.nan
        row[c + '_mean'] = e[c].mean(); row[c + '_ex'] = e['ex_' + c].mean(); row[c + '_t'] = t
        row[c + '_pos_months'] = (mo > 0).mean()
    return row


def cooldown(ev, days=20):
    """Keep at most one event per stock per `days` trading-ish days (calendar approx 1.45x)."""
    ev = ev.sort_values(['code', 'd'])
    keep = []; last = {}
    for code, d in zip(ev.code.values, ev.d.values):
        l = last.get(code)
        if l is None or (d - l).astype('timedelta64[D]').astype(int) >= days * 1.45:
            keep.append(True); last[code] = d
        else:
            keep.append(False)
    return ev[np.array(keep)]
