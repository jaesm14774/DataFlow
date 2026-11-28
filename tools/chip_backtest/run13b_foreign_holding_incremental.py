"""Incremental check for the foreign holding-ratio change (run13): is it just the 20-day
foreign net-buy flow in another form? Compares top-20% 13-week holding increase
(a) with, and (b) without being in the top-20% foreign 20-day net buy / 20-day volume,
plus (c) the flow signal without the holding increase. Same matching/NW method as run13.
Read-only SELECT; nothing promoted."""
import numpy as np, pandas as pd, bt, db
from sqlalchemy import text


def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan


Q = pd.read_parquet(f'{bt.S}/qfiis.parquet')
Q['d'] = pd.to_datetime(Q.d); Q['code'] = Q.code.astype(str).str.strip()
Q = Q[Q.code.str.match(r'^[1-9]\d{3}$')].drop_duplicates(['d', 'code']).sort_values(['code', 'd'])
num = lambda s: pd.to_numeric(s.astype(str).str.replace(',', ''), errors='coerce')
Q['issued'] = num(Q.issued); Q['pct'] = num(Q.held_pct)
g = Q.groupby('code')
Q['d13'] = Q.pct - g.pct.shift(13)
Q.loc[(Q.issued / g.issued.shift(13) - 1).abs() > 0.02, 'd13'] = np.nan

e = db.eng()
inst = pd.read_sql(text("select 資料日期 d,證券代號 code,外資買賣超股數 f from 三大法人買賣 where 資料日期>='2017-09-01' and 證券代號 regexp '^[1-9][0-9]{3}$'"), e)
inst['d'] = pd.to_datetime(inst.d); inst['f'] = pd.to_numeric(inst.f, errors='coerce')
inst = inst.drop_duplicates(['d', 'code'])

px = bt.load_px()
px['vol_sh'] = px.v * 1000
F = px[['d', 'code', 'vol_sh']].merge(inst, on=['d', 'code'], how='left').sort_values(['code', 'd'])
F['f20'] = F.groupby('code').f.transform(lambda s: s.rolling(20, min_periods=15).sum())
F['v20'] = F.groupby('code').vol_sh.transform(lambda s: s.rolling(20, min_periods=15).mean())
F['flow20'] = F.f20 / F.v20 / 20
F['f60'] = F.groupby('code').f.transform(lambda s: s.rolling(60, min_periods=45).sum())
F['v60'] = F.groupby('code').vol_sh.transform(lambda s: s.rolling(60, min_periods=45).mean())
F['flow60'] = F.f60 / F.v60 / 60

res = bt.add_excess(bt.add_liquidity(px, bt.build_outcomes(px)))
E = res[res.elig].copy()
E['mq'] = E.groupby('d').mom60.transform(lambda s: pd.qcut(s.rank(method='first'), 5, labels=False, duplicates='drop'))
E['lq'] = E.groupby('d').adv20.transform(lambda s: pd.qcut(s.rank(method='first'), 3, labels=False, duplicates='drop'))
for c in bt.OUTCOMES:
    E['ex_' + c] = E[c] - E.groupby(['d', 'mq', 'lq'])[c].transform('mean')
E = E.merge(F[['d', 'code', 'flow20', 'flow60']], on=['d', 'code'], how='left')
days = np.sort(px.d.unique())
idx = np.searchsorted(days, Q.d.values, side='right')  # snapshot day +1 session publication lag
ok = idx < len(days); Q = Q[ok].copy(); Q['sd'] = days[idx[ok]]
Z = Q[Q.d13.notna()].rename(columns={'sd': 'd_sig'})
n = Z.groupby('d_sig').d13.transform('size'); Z['rk'] = Z.groupby('d_sig').d13.rank(pct=True)
Z = Z[n >= 200]
M = E.merge(Z[['code', 'd_sig', 'rk', 'd13']].rename(columns={'d_sig': 'd'}), on=['code', 'd'], how='inner')
M['frk'] = M.groupby('d').flow20.rank(pct=True)
M['frk60'] = M.groupby('d').flow60.rank(pct=True)
M = M[M.frk.notna() & M.frk60.notna()]


def stat(sub, label):
    s = bt.cooldown(sub); s = s.assign(m=s.d.dt.to_period('M'))
    r = {'group': label, 'n': len(s)}
    for c, L in (('r20', 3), ('r60', 3)):
        mo = s.groupby('m')['ex_' + c].mean(); r[c + '_ex'] = s['ex_' + c].mean(); r[c + '_t'] = nw_t(mo.values, L)
    yr = s.groupby(s.d.dt.year).ex_r60.mean(); r['yrs_pos_r60'] = f"{(yr > 0).sum()}/{len(yr)}"
    return r


top = M.rk > 0.8; flow_top = M.frk > 0.8
rows = [stat(M[top], 'holding +13w top20 (all)'),
        stat(M[top & flow_top], 'holding top20 AND foreign 20d flow top20'),
        stat(M[top & ~flow_top], 'holding top20 but NOT flow top20  <- incremental'),
        stat(M[~top & flow_top], 'flow top20 but NOT holding top20'),
        stat(M[top & (M.frk <= 0.5)], 'holding top20 with below-median flow'),
        stat(M[top & ~(M.frk60 > 0.8)], 'holding top20 but NOT flow60 top20  <- fair incremental'),
        stat(M[~top & (M.frk60 > 0.8)], 'flow60 top20 but NOT holding top20'),
        stat(M[top & (M.frk60 <= 0.5)], 'holding top20 with below-median flow60')]
corr = M[['d13', 'flow20']].corr(method='spearman').iloc[0, 1]
R = pd.DataFrame(rows); R.to_csv(f'{bt.S}/run13b.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 20)
print('spearman(d13, flow20) =', round(corr, 3)); print(R.round(4).to_string(index=False))
