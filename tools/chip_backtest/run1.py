import numpy as np, pandas as pd, bt, sys
pd.set_option('display.width',250); pd.set_option('display.max_columns',30)
P=pd.read_parquet(f'{bt.S}/feat.parquet').replace([np.inf,-np.inf],np.nan)
feats=['main_r','main_r5','main_r20','f_r','f_r5','f_r20','t_r','t_r5','t_r20','s_r5','s_r20','big400_d4','big400_d13','big1000_d4','big1000_d13','holders_ch4','mb_ch20','sb_ch20','sr','mom20','mom60','mom120','hi250']
E=P[P.elig].copy()
rows=[]
for f in feats:
    x=E[['code','d',f]+[c for c in E.columns if c.startswith(('ex_','r2','r6','r1','hit','win','w7','maxret','elig'))]].dropna(subset=[f]).copy()
    if f in ('big400_d4','big400_d13','big1000_d4','big1000_d13','holders_ch4'):
        ok=E.loc[x.index,'tot_ch4'].abs()<0.02   # exclude capital-change contaminated
        x=x[ok]
    x['rk']=x.groupby('d')[f].rank(pct=True)
    n=x.groupby('d')[f].transform('size'); x=x[n>=100]
    for name,mask in [('TOP10',x.rk>0.9),('BOT10',x.rk<=0.1)]:
        ev=bt.cooldown(x[mask])
        r=bt.evaluate(ev,f'{f}|{name}'); r['span']=f"{ev.d.min():%Y-%m}~{ev.d.max():%Y-%m}"; rows.append(r)
R=pd.DataFrame(rows)
cols=['signal','span','n','r60_mean','r60_ex','r60_t','r240_mean','r240_ex','r240_t','hit7_ex','win7_240_mean','win7_240_ex','win7_240_t','r240_pos_months']
print(R[cols].round(3).to_string())
R.to_csv(f'{bt.S}/run1.csv',index=False)
