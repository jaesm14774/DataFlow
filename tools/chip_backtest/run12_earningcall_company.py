"""法說會: 整體 + 逐家「實誠度」。資料庫只有會議日，沒有會議內容（財測/展望），所以「看好/看壞」
以市場的讀法代理：反應 R = 會議日 t 與 t+1 的超額報酬（對全市場日均報酬）。
後續漂移 D = t+3..t+22（20 日，延遲一日進場）的超額報酬。
連續性分數 s = sign(R)*D：s>0 代表市場反應之後「續行」（反應可信），s<0 代表「反轉」（過度反應）。
任一方向若穩定且可信都算有資訊；若都沒有就是雜訊。
逐家：事件>=10 的公司，sign-flip 置換檢定 + BH；並做時間切分的樣本外檢驗（前 60% 事件挑選、後 40% 驗證）。
價格未還原、存活者偏差；成交值門檻 20 日均 >= 1,000 萬。唯讀；不升級任何規則。"""
import numpy as np, pandas as pd, bt, db
from sqlalchemy import text
rng = np.random.default_rng(7)


def nw_t(x, L):
    x = np.asarray(pd.Series(x).dropna(), float); n = len(x)
    if n < 8: return np.nan
    u = x - x.mean(); v = (u * u).sum() / n
    for l in range(1, min(L, n - 1) + 1): v += 2 * (1 - l / (L + 1)) * (u[l:] * u[:-l]).sum() / n
    return x.mean() / np.sqrt(v / n) if v > 0 else np.nan


def bh(p):
    p = np.asarray(p, float); n = len(p); o = np.argsort(p); q = np.empty(n)
    run = 1.0
    for rank, i in zip(range(n, 0, -1), o[::-1]):
        run = min(run, p[i] * n / rank); q[i] = run
    return q


e = db.eng()
ev = pd.read_sql(text("select 資料日期 d, 證券代號 code from 法說會_歷史"), e)
ev['code'] = ev.code.astype(str).str.strip(); ev['d'] = pd.to_datetime(ev.d, errors='coerce')
ev = ev.dropna().drop_duplicates(['code', 'd']); ev = ev[ev.code.str.match(r'^[1-9]\d{3}$')]
px = bt.load_px()
C = px.pivot(index='d', columns='code', values='c'); V = px.pivot(index='d', columns='code', values='v')
ret = C.pct_change(fill_method=None)
ret = ret.where((ret > -0.25) & (ret < 0.3))  # split / reduction jumps -> unknown
mkt = ret.mean(axis=1)
exr = ret.sub(mkt, axis=0)
adv = (C * V * 1000).rolling(20, min_periods=15).mean()
days = C.index.values; col = {c: i for i, c in enumerate(C.columns)}
rows = []
X = exr.values; A = adv.values
for code, d in zip(ev.code.values, ev.d.values):
    if code not in col: continue
    t = np.searchsorted(days, d, side='right') - 1
    if t < 25 or t + 22 >= len(days): continue
    j = col[code]
    if not (A[t, j] >= 1e7): continue
    r = X[t:t + 2, j]; dr = X[t + 3:t + 23, j]
    if np.isnan(r).any() or np.isnan(dr).sum() > 2: continue
    R = r.sum(); D = np.nansum(dr)
    rows.append((code, pd.Timestamp(days[t]), R, D))
S = pd.DataFrame(rows, columns=['code', 'd', 'R', 'D']); S['s'] = np.sign(S.R) * S.D
print('eligible events', len(S), 'companies', S.code.nunique(), S.d.min().date(), S.d.max().date())
out = []
# ---- aggregate
S['m'] = S.d.dt.to_period('M')
qs = S.R.quantile([.2, .8]).values
for label, sub in (('看好反應 R 前20%', S[S.R >= qs[1]]), ('中性反應', S[(S.R > qs[0]) & (S.R < qs[1])]), ('看壞反應 R 後20%', S[S.R <= qs[0]])):
    mo = sub.groupby('m').D.mean()
    yr = sub.groupby(sub.d.dt.year).D.mean()
    out.append({'group': label, 'n': len(sub), 'meanR%': round(sub.R.mean() * 100, 2), 'drift20%': round(sub.D.mean() * 100, 2),
                't(NW)': round(nw_t(mo.values, 3), 2), 'yrs_pos': f"{(yr > 0).sum()}/{len(yr)}"})
sc = S.groupby('m').s.mean()
out.append({'group': '全體連續性 s=sign(R)*D', 'n': len(S), 'meanR%': np.nan, 'drift20%': round(S.s.mean() * 100, 2), 't(NW)': round(nw_t(sc.values, 3), 2),
            'yrs_pos': f"{(S.groupby(S.d.dt.year).s.mean() > 0).sum()}/{S.d.dt.year.nunique()}"})
A1 = pd.DataFrame(out); print(A1.to_string(index=False)); A1.to_csv(f'{bt.S}/run12_aggregate.csv', index=False)
# ---- per company
cnt = S.groupby('code').size(); S2 = S[S.code.isin(cnt[cnt >= 10].index)]
res = []
for code, g in S2.groupby('code'):
    s = g.s.values; obs = s.mean()
    signs = rng.choice([-1, 1], size=(5000, len(s)))
    null = (signs * np.abs(s)).mean(axis=1)
    p = (np.abs(null) >= abs(obs)).mean(); p = max(p, 1 / 5001)
    res.append({'code': code, 'n': len(s), 'mean_s%': obs * 100, 'p': p})
P = pd.DataFrame(res); P['q'] = bh(P.p.values)
print('companies tested', len(P), '| q<0.05:', int((P.q < 0.05).sum()), '| q<0.10:', int((P.q < 0.10).sum()), '| expected by chance at p<0.05:', round(len(P) * 0.05, 1), 'raw p<0.05:', int((P.p < 0.05).sum()))
# ---- OOS persistence
tr, te = [], []
for code, g in S2.groupby('code'):
    g = g.sort_values('d'); k = int(len(g) * 0.6)
    if k >= 6 and len(g) - k >= 4: tr.append((code, g.s.iloc[:k].mean(), g.s.iloc[k:].mean(), g.s.iloc[:k].values, g.s.iloc[k:].mean()))
T = pd.DataFrame([(c, a, b) for c, a, b, _, _ in tr], columns=['code', 'train', 'test'])
rho = T[['train', 'test']].corr(method='spearman').iloc[0, 1]
perm = [T.train.sample(frac=1, random_state=i).reset_index(drop=True).corr(T.test, method='spearman') for i in range(2000)]
p_rho = float((np.abs(perm) >= abs(rho)).mean())
top = T.nlargest(max(10, len(T) // 10), 'train'); bot = T.nsmallest(max(10, len(T) // 10), 'train')
print(f'OOS companies {len(T)} | rank corr(train s, test s)={rho:.3f} perm p={p_rho:.3f}')
print(f'  train top10% -> test mean s {top.test.mean()*100:.2f}% (train {top.train.mean()*100:.2f}%) | bottom10% -> test {bot.test.mean()*100:.2f}% (train {bot.train.mean()*100:.2f}%) | all test {T.test.mean()*100:.2f}%')
P.sort_values('q').head(15).to_csv(f'{bt.S}/run12_top_companies.csv', index=False)
print(P.sort_values('q').head(8).round(4).to_string(index=False))
