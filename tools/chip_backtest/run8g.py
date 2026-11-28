import numpy as np, pandas as pd, bt
pd.set_option('display.width',250)
exec(open('run6.py',encoding='utf-8').read().split('rows=[]')[0])
def cl_var(e,g):
    n=len(e); m=e.mean(); s=(e-m).groupby(g).sum(); return (s**2).sum()/n**2
def two_way_t(ev,col,tcl):
    e=ev[col].dropna(); ev=ev.loc[e.index]; n=len(e); m=e.mean()
    gs=ev.code; gt=ev[tcl]; gst=ev.code.astype(str)+'|'+ev[tcl].astype(str)
    v=cl_var(e,gs)+cl_var(e,gt)-cl_var(e,gst); return m, m/np.sqrt(max(v,1e-18)), m/np.sqrt(cl_var(e,gs)), m/np.sqrt(cl_var(e,gt))
rows=[]
# ---- full sample (2015-2025) institutional signals: time cluster = entry year
P=pd.read_parquet(f'{bt.S}/feat.parquet').replace([np.inf,-np.inf],np.nan); F=prep(P[P.elig])
rk=lambda D,f:D.groupby('d')[f].rank(pct=True)
ok=F.tot_ch4.abs()<0.02
full={'投信5日 top20% [全樣本]':rk(F,'t_r5')>0.8,'投信&外資20日 [全樣本]':(rk(F,'t_r5')>0.8)&(rk(F,'f_r20')>0.8),'外資20日 top20% [全樣本]':rk(F,'f_r20')>0.8,
      '大戶13週 top20% [全樣本]':(rk(F,'big400_d13')>0.8)&ok,'近250日新高>0.95 對照 [全樣本]':F.hi250>0.95}
for k,m in full.items():
    ev=bt.cooldown(F[m]); ev=ev.assign(yr=ev.d.dt.year)
    for c in ['ex_r240','ex_win7_240','ex_hit7']:
        mean,t2,ts,tt=two_way_t(ev,c,'yr'); rows.append({'signal':k,'metric':c,'n':len(ev),'mean':mean,'t_2way':t2,'t_stock':ts,'t_year':tt})
# ---- broker window 2021-08..2024-12: time cluster = quarter
E=pd.read_parquet(f'{bt.S}/E_brk.parquet'); E=E.drop(columns=[c for c in E.columns if c.startswith('ex_') or c=='tp']); E=prep(E)
W=pd.read_parquet('wfeat.parquet'); E=E.merge(W,on=['code','d'],how='left')
for c in [c for c in E.columns if c.startswith(('dbuy','dsell'))]: E[c]=E[c].fillna(0)
rb=lambda f:E.groupby('d')[f].rank(pct=True)
bs={'投信5日 top20% [分點期]':rb('t_r5')>0.8,'投信&外資 [分點期]':(rb('t_r5')>0.8)&(rb('f_r20')>0.8),'dbuy1_20>=5 多分點大買':E.dbuy1_20>=5,'dsell1_20>=5 多分點大賣':E.dsell1_20>=5,
    '買盤總流量 top10% (10d)':rb('buyflow_10')>0.9,'分點集中度top15 20日 top10%':rb('conc15_20')>0.9,'最大買超分點占比 20日 top10%':rb('top1_20')>0.9,
    '買賣家數差20日 BOT10%':rb('nbal_20')<=0.1,'賣壓集中(單一大賣)&多賣 20d':(E.dsell1_20>=2)&(E.sellconc_20>0.6),'主力20日 top10%':rb('main_r20')>0.9}
for k,m in bs.items():
    ev=bt.cooldown(E[m]); ev=ev.assign(q=ev.d.dt.to_period('Q'))
    for c in ['ex_r240','ex_win7_240','ex_hit7']:
        mean,t2,ts,tt=two_way_t(ev,c,'q'); rows.append({'signal':k,'metric':c,'n':len(ev),'mean':mean,'t_2way':t2,'t_stock':ts,'t_year':tt})
R=pd.DataFrame(rows); R.to_csv('run8g_results.csv',index=False)
print(R.pivot_table(index='signal',columns='metric',values=['mean','t_2way'],sort=False).round(3).to_string())
print(R[R.metric=='ex_r240'][['signal','n','mean','t_2way','t_stock','t_year']].round(3).to_string())
