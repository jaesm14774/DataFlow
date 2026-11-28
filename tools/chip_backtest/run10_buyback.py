"""Treasury-stock (庫藏股) announcement event study on 庫藏股_歷史 (board resolution date).
Signal date = last trading day <= resolution date (so an after-hours or weekend filing
enters at the next open). Same matching/NW method as run6/run9. Hypothesis fixed before
looking: announcement is bullish (+1), strongest for purpose 3 (維護公司信用及股東權益).
Prices not dividend-adjusted; survivorship bias applies. Read-only SELECT; nothing promoted."""
import numpy as np, pandas as pd, bt, db
from sqlalchemy import text

def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan

e = db.eng()
raw = pd.read_sql(text("select * from 庫藏股_歷史"), e)
ev = raw.iloc[:, [1, 3, 4]].copy(); ev.columns = ['code', 'roc', 'purpose']
ev['code'] = ev.code.astype(str).str.strip()
def roc(x):
    try:
        y, m, d = str(x).strip().split('/'); return pd.Timestamp(int(y) + 1911, int(m), int(d))
    except Exception: return pd.NaT
ev['bd'] = ev.roc.map(roc); ev = ev.dropna(subset=['bd'])
ev = ev[ev.code.str.match(r'^[1-9]\d{3}$')].drop_duplicates(['code', 'bd'])
print('events', len(ev), ev.bd.min().date(), ev.bd.max().date(), ev.purpose.value_counts().to_dict())

px = bt.load_px()
res = bt.add_excess(bt.add_liquidity(px, bt.build_outcomes(px)))
E = res[res.elig].copy()
E['mq'] = E.groupby('d').mom60.transform(lambda s: pd.qcut(s.rank(method='first'), 5, labels=False, duplicates='drop'))
E['lq'] = E.groupby('d').adv20.transform(lambda s: pd.qcut(s.rank(method='first'), 3, labels=False, duplicates='drop'))
E['tp'] = np.where(E.hit7 == 1, 0.07, E.r240); E.loc[E.hit7.isna() | E.r240.isna(), 'tp'] = np.nan
for c in bt.OUTCOMES + ['tp']:
    E['ex_' + c] = E[c] - E.groupby(['d', 'mq', 'lq'])[c].transform('mean')
days = np.sort(px.d.unique())
idx = np.searchsorted(days, ev.bd.values, side='right') - 1
ev['d'] = np.where(idx >= 0, days[np.clip(idx, 0, None)], np.datetime64('NaT'))
ev = ev.dropna(subset=['d']); ev['d'] = pd.to_datetime(ev.d)
M = E.merge(ev[['code', 'd', 'purpose']], on=['code', 'd'], how='inner')
rows = []
for label, sub in (('all announcements', M), ('purpose 3 (維護公司信用及股東權益)', M[M.purpose.astype(str).str.strip() == '3']),
                   ('purpose 1 (轉讓員工)', M[M.purpose.astype(str).str.strip() == '1']),
                   ('control: purpose 2 (認股權)', M[M.purpose.astype(str).str.strip() == '2'])):
    s2 = bt.cooldown(sub); s2 = s2.assign(m=s2.d.dt.to_period('M'))
    r = {'group': label, 'n': len(s2)}
    for c, L in (('r20', 3), ('r60', 3), ('r240', 11)):
        if c not in s2 or 'ex_' + c not in s2: continue
        mo = s2.groupby('m')['ex_' + c].mean()
        r[c + '_ex'] = s2['ex_' + c].mean(); r[c + '_t'] = nw_t(mo.values, L)
    yr = s2.groupby(s2.d.dt.year).ex_r60.mean(); r['yrs_pos_r60'] = f"{(yr > 0).sum()}/{len(yr)}"
    r['r60_pre2023'] = s2[s2.d < '2023-01-01'].ex_r60.mean(); r['r60_post2023'] = s2[s2.d >= '2023-01-01'].ex_r60.mean()
    rows.append(r)
R = pd.DataFrame(rows); R.to_csv(f'{bt.S}/run10_buyback.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
print(R.round(4).to_string(index=False))
