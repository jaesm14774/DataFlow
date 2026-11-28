import numpy as np, pandas as pd, bt
pd.set_option('display.width',250); pd.set_option('display.max_columns',30)
P=pd.read_parquet(f'{bt.S}/feat.parquet').replace([np.inf,-np.inf],np.nan)
E=P[P.elig].copy()
E['mq']=E.groupby('d').mom60.transform(lambda s:pd.qcut(s.rank(method='first'),5,labels=False,duplicates='drop'))
E['lq']=E.groupby('d').adv20.transform(lambda s:pd.qcut(s.rank(method='first'),3,labels=False,duplicates='drop'))
for c in bt.OUTCOMES:   # matched baseline (date x momentum quintile x liquidity tercile) -> overwrite ex_
    E['ex_'+c]=E[c]-E.groupby(['d','mq','lq'])[c].transform('mean')
def rk(f): return E.groupby('d')[f].rank(pct=True)
ok_cap=E.tot_ch4.abs()<0.02
E['A']=rk('main_r20')>0.8
E['B']=rk('t_r5')>0.8
E['C']=(rk('big400_d13')>0.8)&ok_cap
E['D']=(rk('holders_ch4')<=0.2)&ok_cap
E['F']=rk('f_r20')>0.8
E['Hlow']=E.hi250<0.8
E['Hhigh']=E.hi250>0.95
sig={'A main20 top20':E.A,'B trust5 top20':E.B,'C big400_13w top20':E.C,'D holders4w bottom20':E.D,'F foreign20 top20':E.F,
 'B&C':E.B&E.C,'A&B':E.A&E.B,'A&C':E.A&E.C,'B&F':E.B&E.F,'B&C&D':E.B&E.C&E.D,'C&D':E.C&E.D,'A&B&C':E.A&E.B&E.C,
 'B&C & Hlow':E.B&E.C&E.Hlow,'C & Hlow':E.C&E.Hlow,'B & Hlow':E.B&E.Hlow,'A & Hlow':E.A&E.Hlow,'B&C & Hhigh':E.B&E.C&E.Hhigh,
 'ctrl hi250>0.95':E.Hhigh,'ctrl mom60 top20':rk('mom60')>0.8}
rows=[]
for k,m in sig.items():
    ev=bt.cooldown(E[m])
    r=bt.evaluate(ev,k,min_n=40); mid=pd.Timestamp('2020-01-01')
    for nm,sub in [('pre2020',ev[ev.d<mid]),('post2020',ev[ev.d>=mid])]:
        rr=bt.evaluate(sub,'',min_n=40); r[nm+'_r240ex']=rr.get('r240_ex'); r[nm+'_n']=rr['n']
    rows.append(r)
R=pd.DataFrame(rows)
cols=['signal','n','r60_ex','r60_t','r240_mean','r240_ex','r240_t','win7_240_mean','win7_240_ex','win7_240_t','hit7_ex','r240_pos_months','pre2020_n','pre2020_r240ex','post2020_n','post2020_r240ex']
print(R[cols].round(3).to_string())
