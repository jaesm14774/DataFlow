"""Value families (earnings_yield, book_to_market, dividend_yield) on the legacy
個股基本指標 table, 2011-2026. Signs fixed beforehand (+1: cheaper -> stronger), same
method as run6 (next-open entry, momentum x liquidity matched baseline, NW t on monthly
excess). Prices are NOT dividend-adjusted, so dividend_yield is biased against itself:
reported for completeness, never a verdict. Read-only SELECTs; nothing is promoted."""
import os, numpy as np, pandas as pd, bt, db
from sqlalchemy import text

def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan

e = db.eng()
if not os.path.exists(f'{bt.S}/px.parquet'):
    pd.read_sql(text("select 資料日期 d,證券代號 code,開盤 o,最高 h,最低 l,收盤 c,成交量 v from 個股成交資訊 where 資料日期>='2010-01-01' and 資料日期<'2027-01-01'"), e).to_parquet(f'{bt.S}/px.parquet')
if not os.path.exists(f'{bt.S}/val.parquet'):
    pd.read_sql(text("select 資料日期 d,證券代號 code,本益比 pe,股價淨值比 pb,`殖利率(%)` dy from 個股基本指標 where 證券代號 regexp '^[1-9][0-9]{3}$'"), e).to_parquet(f'{bt.S}/val.parquet')
px = bt.load_px()
res = bt.add_excess(bt.add_liquidity(px, bt.build_outcomes(px)))
V = pd.read_parquet(f'{bt.S}/val.parquet'); V['d'] = pd.to_datetime(V.d); V['code'] = V.code.astype(str)
for c in ('pe', 'pb', 'dy'): V[c] = pd.to_numeric(V[c], errors='coerce')
V = V.drop_duplicates(['d', 'code'])
E = res[res.elig].merge(V, on=['d', 'code'], how='inner')
E['earnings_yield'] = np.where(E.pe > 0, 100 / E.pe, np.nan)
E['book_to_market'] = np.where(E.pb > 0, 1 / E.pb, np.nan)
E['dividend_yield'] = np.where(E.dy > 0, E.dy, np.nan)
E['mq'] = E.groupby('d').mom60.transform(lambda s: pd.qcut(s.rank(method='first'), 5, labels=False, duplicates='drop'))
E['lq'] = E.groupby('d').adv20.transform(lambda s: pd.qcut(s.rank(method='first'), 3, labels=False, duplicates='drop'))
E['tp'] = np.where(E.hit7 == 1, 0.07, E.r240); E.loc[E.hit7.isna() | E.r240.isna(), 'tp'] = np.nan
for c in bt.OUTCOMES + ['tp']:
    E['ex_' + c] = E[c] - E.groupby(['d', 'mq', 'lq'])[c].transform('mean')
rows = []
for f in ('earnings_yield', 'book_to_market', 'dividend_yield'):
    rk = E[E[f].notna()].groupby('d')[f].rank(pct=True)
    n = E[E[f].notna()].groupby('d')[f].transform('size')
    for side, mask in (('TOP20 (hypothesis: bullish)', rk > 0.8), ('BOTTOM20 (control)', rk <= 0.2)):
        ev = E[E[f].notna()][(mask) & (n >= 200)]
        ev = bt.cooldown(ev); ev = ev.assign(m=ev.d.dt.to_period('M'))
        r = {'factor': f, 'side': side, 'n': len(ev)}
        for c, L in (('r60', 3), ('r240', 11)):
            mo = ev.groupby('m')['ex_' + c].mean()
            r[c + '_ex'] = ev['ex_' + c].mean(); r[c + '_t'] = nw_t(mo.values, L)
        yr = ev.groupby(ev.d.dt.year).ex_r240.mean(); r['yrs_pos'] = f"{(yr > 0).sum()}/{len(yr)}"
        h1 = ev[ev.d < '2019-01-01']; h2 = ev[ev.d >= '2019-01-01']
        r['r240_ex_pre2019'] = h1.ex_r240.mean(); r['r240_ex_post2019'] = h2.ex_r240.mean()
        rows.append(r)
R = pd.DataFrame(rows); R.to_csv(f'{bt.S}/run9_value.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
print(R.round(4).to_string(index=False))
