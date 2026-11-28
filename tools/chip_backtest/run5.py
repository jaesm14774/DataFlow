import numpy as np, pandas as pd, bt
pd.set_option('display.width',250); pd.set_option('display.max_columns',30)
P=pd.read_parquet(f'{bt.S}/feat.parquet').replace([np.inf,-np.inf],np.nan)
E=P[P.elig].copy()
E['mq']=E.groupby('d').mom60.transform(lambda s:pd.qcut(s.rank(method='first'),5,labels=False,duplicates='drop'))
E['lq']=E.groupby('d').adv20.transform(lambda s:pd.qcut(s.rank(method='first'),3,labels=False,duplicates='drop'))
for c in bt.OUTCOMES: E['ex_'+c]=E[c]-E.groupby(['d','mq','lq'])[c].transform('mean')
# date-only baseline too
base_d=E.groupby('d')[['r240','win7_240','hit7']].mean().add_prefix('bd_')
E=E.join(E.d.map(base_d['bd_r240']).rename('bd_r240')).join(E.d.map(base_d['bd_win7_240']).rename('bd_win'))
# --- precomputed 地緣券商推薦 table
g=pd.read_parquet(f'{bt.S}/geo.parquet'); g['d']=pd.to_datetime(g.d)
for side in ['買','賣']:
    for how in sorted(g.how.dropna().unique()):
        s=g[(g.side==side)&(g.how==how)][['code','d']].drop_duplicates()
        ev=bt.cooldown(E.merge(s,on=['code','d']))
        r=bt.evaluate(ev,f'地緣推薦 {how}|{side}',min_n=30)
        print({k:(round(v,3) if isinstance(v,float) else v) for k,v in r.items() if k in('signal','n','r240_mean','r240_ex','r240_t','win7_240_mean','win7_240_ex','win7_240_t','hit7_ex')})
# --- headline signals, year-by-year, costs
ok=E.tot_ch4.abs()<0.02
rk=lambda f:E.groupby('d')[f].rank(pct=True)
B=rk('t_r5')>0.8; F=rk('f_r20')>0.8; C=(rk('big400_d13')>0.8)&ok; A=rk('main_r20')>0.8
heads={'投信5日買超 top20%':B,'投信&外資20日':B&F,'投信&大戶13週':B&C,'主力20日 top20%':A}
COST=0.006
for k,m in heads.items():
    ev=bt.cooldown(E[m]); y=ev.groupby(ev.d.dt.year)
    t=pd.DataFrame({'n':y.size(),'r240':y.r240.mean(),'base_r240':y.bd_r240.mean(),'ex_matched':y.ex_r240.mean(),'win7':y.win7_240.mean(),'base_win7':y.bd_win.mean()})
    t['win_lift_pp']=(t.win7-t.base_win7)*100
    print('\n##',k,'| overall n',len(ev),'r240',round(ev.r240.mean(),3),'base',round(ev.bd_r240.mean(),3),'net-of-cost ex',round(ev.r240.mean()-ev.bd_r240.mean()-COST,4),
          '| P(r240>0)',round((ev.r240>0).mean(),3),'vs base',round((E.loc[E.r240.notna()&E.elig,'r240']>0).mean(),3))
    print(t.round(3).to_string())
