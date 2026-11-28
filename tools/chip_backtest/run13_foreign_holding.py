"""外資及陸資持股比例變化（TWSE MI_QFIIS 週快照 2018-2026，fetch_qfiis.py）。
預先登記（看結果前固定）：持股比 4 週增加（Δ4w，百分點）偏多 +1；13 週變化（Δ13w）同向。
可用時點：快照日 T 盤後公布，保守延後 1 個交易日才算可用，再隔日開盤進場。
排除發行股數 4 週內變動 >2%（增資/減資使比率機械性變動）。同動能×流動性配對基準、
20 日冷卻、月序列 Newey-West t。價格未還原、存活者偏差。唯讀；不升級規則。"""
import numpy as np, pandas as pd, bt


def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan


Q = pd.read_parquet(f'{bt.S}/qfiis.parquet')
Q['d'] = pd.to_datetime(Q.d); Q['code'] = Q.code.astype(str).str.strip()
Q = Q[Q.code.str.match(r'^[1-9]\d{3}$')].drop_duplicates(['d', 'code'])
num = lambda s: pd.to_numeric(s.astype(str).str.replace(',', ''), errors='coerce')
Q['issued'] = num(Q.issued); Q['pct'] = num(Q.held_pct)
Q = Q.sort_values(['code', 'd'])
g = Q.groupby('code')
Q['d4'] = Q.pct - g.pct.shift(4); Q['d13'] = Q.pct - g.pct.shift(13)
Q['iss4'] = (Q.issued / g.issued.shift(4) - 1).abs(); Q['iss13'] = (Q.issued / g.issued.shift(13) - 1).abs()
Q.loc[Q.iss4 > 0.02, 'd4'] = np.nan; Q.loc[Q.iss13 > 0.02, 'd13'] = np.nan
print('snapshots', Q.d.nunique(), Q.d.min().date(), Q.d.max().date(), 'rows', len(Q))
px = bt.load_px()
res = bt.add_excess(bt.add_liquidity(px, bt.build_outcomes(px)))
E = res[res.elig].copy()
E['mq'] = E.groupby('d').mom60.transform(lambda s: pd.qcut(s.rank(method='first'), 5, labels=False, duplicates='drop'))
E['lq'] = E.groupby('d').adv20.transform(lambda s: pd.qcut(s.rank(method='first'), 3, labels=False, duplicates='drop'))
for c in bt.OUTCOMES:
    E['ex_' + c] = E[c] - E.groupby(['d', 'mq', 'lq'])[c].transform('mean')
days = np.sort(px.d.unique())
idx = np.searchsorted(days, Q.d.values, side='right') - 1 + 1  # one-session publication lag
ok = idx < len(days); Q = Q[ok].copy(); Q['sd'] = days[idx[ok]]
rows = []
for f in ('d4', 'd13'):
    Z = Q[Q[f].notna()]
    n = Z.groupby('sd')[f].transform('size'); rk = Z.groupby('sd')[f].rank(pct=True)
    for side, mask in (('TOP20 增持（假設偏多）', (rk > 0.8)), ('BOTTOM20 減持（對照）', (rk <= 0.2))):
        M = E.merge(Z[mask & (n >= 200)][['code', 'sd', f]].rename(columns={'sd': 'd'}), on=['code', 'd'], how='inner')
        M = bt.cooldown(M); M = M.assign(m=M.d.dt.to_period('M'))
        r = {'feature': f, 'side': side, 'n': len(M)}
        for c, L in (('r20', 3), ('r60', 3), ('r240', 11)):
            if M['ex_' + c].notna().sum() < 30: continue
            mo = M.groupby('m')['ex_' + c].mean(); r[c + '_ex'] = M['ex_' + c].mean(); r[c + '_t'] = nw_t(mo.values, L)
        yr = M.groupby(M.d.dt.year).ex_r60.mean(); r['yrs_pos_r60'] = f"{(yr > 0).sum()}/{len(yr)}"
        rows.append(r)
R = pd.DataFrame(rows); R.to_csv(f'{bt.S}/run13_foreign_holding.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 20)
print(R.round(4).to_string(index=False))
