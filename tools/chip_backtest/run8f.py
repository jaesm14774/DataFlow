import numpy as np, pandas as pd, bt
pd.set_option('display.width',250)
exec(open('run6.py',encoding='utf-8').read().split('rows=[]')[0])
E=prep(pd.read_parquet(f'{bt.S}/E_brk.parquet').drop(columns=[c for c in pd.read_parquet(f'{bt.S}/E_brk.parquet').columns if c.startswith('ex_') or c=='tp']))
W=pd.read_parquet('wfeat.parquet'); E=E.merge(W,on=['code','d'],how='left')
for c in [c for c in E.columns if c.startswith(('dbuy','dsell'))]: E[c]=E[c].fillna(0)
# stock fixed effect: mean matched-excess of each stock over the whole window
for c in ['r60','r240','win7_240','hit7']:
    E['dm_'+c]=E['ex_'+c]-E.groupby('code')['ex_'+c].transform('mean')
rk=lambda f:E.groupby('d')[f].rank(pct=True)
S={'dbuy1_20>=5 (多分點大買)':E.dbuy1_20>=5,'dsell1_20>=5 (多分點大賣)':E.dsell1_20>=5,'dbuy1_10>=3':E.dbuy1_10>=3,
   '買盤總流量 top10% (10d)':rk('buyflow_10')>0.9,'活躍度 act20>=8':(E.dbuy1_20+E.dsell1_20)>=8,
   '(對照)投信5日 top20%':rk('t_r5')>0.8,'(對照)投信&外資':(rk('t_r5')>0.8)&(rk('f_r20')>0.8)}
rows=[]
for k,m in S.items():
    ev=bt.cooldown(E[m]); ev=ev.assign(mo=ev.d.dt.to_period('M'))
    r={'signal':k,'n':len(ev),'stocks':ev.code.nunique()}
    for c,L in [('r60',3),('r240',11)]:
        mo=ev.groupby('mo')['ex_'+c].mean(); r[f'{c}_ex']=ev['ex_'+c].mean(); r[f'{c}_ntT']=nw_t(mo.values,L)
        mo2=ev.groupby('mo')['dm_'+c].mean(); r[f'{c}_FEdemeaned']=ev['dm_'+c].mean(); r[f'{c}_FE_T']=nw_t(mo2.values,L)
    st=ev.groupby('code').ex_r240.mean(); r['stock_clustered_t_r240']=st.mean()/(st.std(ddof=1)/np.sqrt(len(st)))
    st2=ev.groupby('code').dm_r240.mean(); r['stock_clustered_t_FE']=st2.mean()/(st2.std(ddof=1)/np.sqrt(len(st2)))
    rows.append(r)
print(pd.DataFrame(rows).round(3).T.to_string())
