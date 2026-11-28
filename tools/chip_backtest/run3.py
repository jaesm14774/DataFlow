import numpy as np, pandas as pd, bt
pd.set_option('display.width',250); pd.set_option('display.max_columns',30)
P=pd.read_parquet(f'{bt.S}/feat.parquet').replace([np.inf,-np.inf],np.nan)
B=pd.read_parquet(f'{bt.S}/brk_feat_2124.parquet'); B['d']=pd.to_datetime(B.d)
P=P[(P.d>='2021-07-01')&(P.d<='2024-12-31')].merge(B,on=['code','d'],how='left',suffixes=('','_b'))
P=P.sort_values(['code','d']).reset_index(drop=True)
have=P.vol.notna()
def rs(col,w):
    x=P[col].fillna(0).where(have,np.nan)
    return x.groupby(P.code).transform(lambda s:s.rolling(w,min_periods=w).sum())
for w in (1,5,20):
    vol=rs('vol',w)
    P[f'conc15_{w}']=(rs('top15b',w)+rs('top15s',w))/vol
    P[f'conc5_{w}']=(rs('top5b',w)+rs('top5s',w))/vol
    P[f'top1_{w}']=rs('top1b',w)/vol
    P[f'geo3_{w}']=rs('geo3',w)/vol
    P[f'geo1_{w}']=rs('geo1',w)/vol
    P[f'nbal_{w}']=(rs('nb',w)-rs('ns',w))/rs('nbr',w)
# XQ rule: geo3 net >200 lots 3 consecutive days & MA3 crosses above MA10
P['geo3_pos3']=(P.geo3.fillna(-1)>200).groupby(P.code).transform(lambda s:s.rolling(3).sum()==3)
ma3=P.groupby('code').c.transform(lambda s:s.rolling(3).mean()); ma10=P.groupby('code').c.transform(lambda s:s.rolling(10).mean())
P['xq']=P.geo3_pos3&(ma3>ma10)&(ma3.groupby(P.code).shift(1)<=ma10.groupby(P.code).shift(1))
P['geo3_pos3_only']=P.geo3_pos3
E=P[P.elig&have].copy()
E['mq']=E.groupby('d').mom60.transform(lambda s:pd.qcut(s.rank(method='first'),5,labels=False,duplicates='drop'))
E['lq']=E.groupby('d').adv20.transform(lambda s:pd.qcut(s.rank(method='first'),3,labels=False,duplicates='drop'))
for c in bt.OUTCOMES: E['ex_'+c]=E[c]-E.groupby(['d','mq','lq'])[c].transform('mean')
E.to_parquet(f'{bt.S}/E_brk.parquet')
rows=[]
def run(label,mask):
    ev=bt.cooldown(E[mask]); r=bt.evaluate(ev,label,min_n=40); rows.append(r)
for f in [c for c in E.columns if c.startswith(('conc15_','conc5_','top1_','geo3_','geo1_','nbal_')) and c!='geo3_pos3']:
    x=E[f]; rk=E.groupby('d')[f].rank(pct=True)
    run(f+'|TOP10',(rk>0.9)); run(f+'|BOT10',(rk<=0.1))
run('XQ geo3>200x3 & MA3x10',E.xq); run('geo3>200 x3 only',E.geo3_pos3_only)
run('main_r20 top10 (same window)',E.groupby('d').main_r20.rank(pct=True)>0.9)
run('trust5 top20 (same window)',E.groupby('d').t_r5.rank(pct=True)>0.8)
R=pd.DataFrame(rows)
cols=['signal','n','r60_ex','r60_t','r240_mean','r240_ex','r240_t','win7_240_mean','win7_240_ex','win7_240_t','hit7_ex','r240_pos_months']
print(R[cols].round(3).to_string()); R.to_csv(f'{bt.S}/run3.csv',index=False)
