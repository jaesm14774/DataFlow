"""Historical evidence for two pre-registered research families that had none:
  daytrade_heat_5d   (當沖資訊, sign -1): 5-day mean of day-trade shares / total shares
  revenue_yoy, revenue_yoy_acceleration (月營收資訊, sign +1; table ends 2025-02)
Same method as run6/run9: next-open entry, momentum x liquidity matched baseline, 20-day
cooldown, NW t on monthly excess. Day-trade data usable at T close (lag 0 -> next open).
Revenue month M is usable from the first session on/after the 11th of M+1 (legal deadline is
the 10th). Signs fixed in research_signals.FAMILIES before looking. Prices not adjusted,
survivorship bias applies. Read-only; nothing promoted."""
import numpy as np, pandas as pd, bt, db
from sqlalchemy import text


def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan


e = db.eng()
px = bt.load_px()
res = bt.add_excess(bt.add_liquidity(px, bt.build_outcomes(px)))
E = res[res.elig].copy()
E['mq'] = E.groupby('d').mom60.transform(lambda s: pd.qcut(s.rank(method='first'), 5, labels=False, duplicates='drop'))
E['lq'] = E.groupby('d').adv20.transform(lambda s: pd.qcut(s.rank(method='first'), 3, labels=False, duplicates='drop'))
for c in bt.OUTCOMES:
    E['ex_' + c] = E[c] - E.groupby(['d', 'mq', 'lq'])[c].transform('mean')
days = np.sort(px.d.unique())


def evaluate(feature_frame, name, sign_label):
    """feature_frame: columns code, d (signal session, already lagged), x (score)."""
    Z = feature_frame.dropna(subset=['x']).copy()
    n = Z.groupby('d').x.transform('size'); Z['rk'] = Z.groupby('d').x.rank(pct=True)
    Z = Z[n >= 200]
    rows = []
    for side, mask in (('TOP20', Z.rk > 0.8), ('BOTTOM20', Z.rk <= 0.2)):
        M = E.merge(Z[mask][['code', 'd']], on=['code', 'd'], how='inner')
        M = bt.cooldown(M); M = M.assign(m=M.d.dt.to_period('M'))
        r = {'factor': name, 'side': side, 'hypothesis': sign_label, 'n': len(M)}
        for c, L in (('r20', 3), ('r60', 3), ('r240', 11)):
            if M['ex_' + c].notna().sum() < 30: continue
            mo = M.groupby('m')['ex_' + c].mean(); r[c + '_ex'] = M['ex_' + c].mean(); r[c + '_t'] = nw_t(mo.values, L)
        yr = M.groupby(M.d.dt.year).ex_r60.mean(); r['yrs_pos_r60'] = f"{(yr > 0).sum()}/{len(yr)}"
        rows.append(r)
    return rows


out = []
# ---- day-trade heat (5-day mean ratio of day-trade shares to total shares)
dt_ = pd.read_sql(text("select 資料日期 d, 證券代號 code, 當日沖銷交易成交股數 s from 當沖資訊 where 證券代號 regexp '^[1-9][0-9]{3}$'"), e)
dt_['d'] = pd.to_datetime(dt_.d); dt_['s'] = pd.to_numeric(dt_.s, errors='coerce')
dt_ = dt_.drop_duplicates(['d', 'code'])
P = px[['d', 'code', 'v']].copy(); P['tot'] = P.v * 1000
H = P.merge(dt_, on=['d', 'code'], how='left')
H['ratio'] = (H.s / H.tot).where(H.tot > 0)
H = H.sort_values(['code', 'd'])
H['x'] = H.groupby('code').ratio.transform(lambda s: s.rolling(5, min_periods=4).mean())
out += evaluate(H[['code', 'd', 'x']], 'daytrade_heat_5d', 'sign -1 (high heat bearish)')

# ---- monthly revenue (legacy table, 2016-01 .. 2025-02)
rv = pd.read_sql(text("select 證券代號 code, 資料日期 m, `去年同月增減(%)` yoy from 月營收資訊 where 證券代號 regexp '^[1-9][0-9]{3}$'"), e)
rv['m'] = pd.to_datetime(rv.m); rv['yoy'] = pd.to_numeric(rv.yoy, errors='coerce')
rv = rv.drop_duplicates(['code', 'm']).sort_values(['code', 'm'])
prev_ok = rv.groupby('code').m.shift(1) == (rv.m - pd.offsets.MonthBegin(1))
rv['yoy_prev'] = rv.groupby('code').yoy.shift(1).where(prev_ok)
rv['acc'] = rv.yoy - rv.yoy_prev
avail = rv.m + pd.offsets.MonthBegin(1) + pd.Timedelta(days=10)  # the 11th of month M+1
idx = np.searchsorted(days, avail.values, side='left')
ok = idx < len(days)
rv = rv[ok].copy(); rv['d'] = days[idx[ok]]
out += evaluate(rv.rename(columns={'yoy': 'x'})[['code', 'd', 'x']], 'revenue_yoy', 'sign +1')
out += evaluate(rv.rename(columns={'acc': 'x'})[['code', 'd', 'x']], 'revenue_yoy_acceleration', 'sign +1')

R = pd.DataFrame(out); R.to_csv(f'{bt.S}/run14.csv', index=False)
pd.set_option('display.width', 260); pd.set_option('display.max_columns', 20)
print(R.round(4).to_string(index=False))
