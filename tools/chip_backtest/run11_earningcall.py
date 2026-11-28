"""法說會 event study (法說會_歷史: 資料日期=會議日, 2023-2026, 18.7k events).
Pre-registered, sign fixed before looking: (a) POST: enter next open after the meeting day,
hypothesis +1 (information release / attention); (b) PRE: enter 5 trading days before the
meeting day, hypothesis +1 (run-up). PRE assumes the schedule was public >=5 sessions ahead;
the first-landing time of schedules is not stored, so PRE is an assumption-labelled lead only.
Same matched baseline (momentum x liquidity), 20-day cooldown, NW t as run6/run9/run10.
Prices not dividend-adjusted; survivorship bias applies. Read-only; nothing promoted."""
import numpy as np, pandas as pd, bt, db
from sqlalchemy import text


def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan


e = db.eng()
raw = pd.read_sql(text("select 資料日期 d, 證券代號 code from 法說會_歷史"), e)
raw['code'] = raw.code.astype(str).str.strip(); raw['d'] = pd.to_datetime(raw.d, errors='coerce')
raw = raw.dropna().drop_duplicates(['code', 'd'])
raw = raw[raw.code.str.match(r'^[1-9]\d{3}$')]
print('events', len(raw), raw.d.min().date(), raw.d.max().date())
px = bt.load_px()
last = px.d.max()
raw = raw[raw.d <= last - pd.Timedelta(days=100)]  # need forward windows
res = bt.add_excess(bt.add_liquidity(px, bt.build_outcomes(px)))
E = res[res.elig].copy()
E['mq'] = E.groupby('d').mom60.transform(lambda s: pd.qcut(s.rank(method='first'), 5, labels=False, duplicates='drop'))
E['lq'] = E.groupby('d').adv20.transform(lambda s: pd.qcut(s.rank(method='first'), 3, labels=False, duplicates='drop'))
for c in bt.OUTCOMES:
    E['ex_' + c] = E[c] - E.groupby(['d', 'mq', 'lq'])[c].transform('mean')
days = np.sort(px.d.unique())
rows = []
for label, shift in (('POST: enter next open after meeting day', 0), ('PRE: enter 5 sessions before meeting day (assumes schedule known)', -5)):
    ev = raw.copy()
    idx = np.searchsorted(days, ev.d.values, side='right') - 1 + shift
    ok = (idx >= 0) & (idx < len(days)); ev = ev[ok]; ev['sd'] = days[idx[ok]]
    M = E.merge(ev[['code', 'sd']].rename(columns={'sd': 'd'}), on=['code', 'd'], how='inner')
    M = bt.cooldown(M); M = M.assign(m=M.d.dt.to_period('M'))
    r = {'group': label, 'n': len(M)}
    for c, L in (('r20', 3), ('r60', 3)):
        mo = M.groupby('m')['ex_' + c].mean(); r[c + '_ex'] = M['ex_' + c].mean(); r[c + '_t'] = nw_t(mo.values, L)
    yr = M.groupby(M.d.dt.year).ex_r20.mean(); r['yrs_pos_r20'] = f"{(yr > 0).sum()}/{len(yr)}"
    rows.append(r)
R = pd.DataFrame(rows); R.to_csv(f'{bt.S}/run11_earningcall.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 20)
print(R.round(4).to_string(index=False))
