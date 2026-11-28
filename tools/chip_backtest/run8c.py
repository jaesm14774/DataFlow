import numpy as np, pandas as pd, bt
pd.set_option('display.width',280); pd.set_option('display.max_columns',40); pd.set_option('display.max_rows',200)
exec(open('run6.py',encoding='utf-8').read().split('rows=[]')[0])
E0=pd.read_parquet(f'{bt.S}/E_brk.parquet'); E0=E0.drop(columns=[c for c in E0.columns if c.startswith('ex_') or c=='tp'])
W=pd.read_parquet('wfeat.parquet'); E0=E0.merge(W,on=['code','d'],how='left')
for c in [c for c in E0.columns if c.startswith(('dbuy','dsell'))]: E0[c]=E0[c].fillna(0)
E0['tp']=np.where(E0.hit7==1,0.07,E0.r240); E0.loc[E0.hit7.isna()|E0.r240.isna(),'tp']=np.nan
E0['mq']=E0.groupby('d').mom60.transform(lambda s:pd.qcut(s.rank(method='first'),5,labels=False,duplicates='drop'))
E0['lq']=E0.groupby('d').adv20.transform(lambda s:pd.qcut(s.rank(method='first'),3,labels=False,duplicates='drop'))
mid=pd.Timestamp('2023-01-01'); OUT=bt.OUTCOMES+['tp']
def with_act(N):
    E=E0.copy(); act=E[f'dbuy1_{N}']+E[f'dsell1_{N}']
    E['ab']=pd.cut(act,[-1,0,1,2,4,7,12,1e9],labels=False)
    for c in OUT: E['ex_'+c]=E[c]-E.groupby(['d','mq','lq','ab'])[c].transform('mean')
    return E
def full(ev,label):
    r=stat(ev,label); ev=bt.cooldown(ev)
    for nm,sub in [('h1',ev[ev.d<mid]),('h2',ev[ev.d>=mid])]: r[nm+'_r240ex']=sub.ex_r240.mean() if len(sub)>30 else np.nan
    return r
rows=[]
for N in (5,10,20):
    E=with_act(N); rk=lambda f:E.groupby('d')[f].rank(pct=True)
    db,ds=E[f'dbuy1_{N}'],E[f'dsell1_{N}']
    S={f'買賣分點數差 buy-sell>=2 ({N}d)':(db-ds)>=2, f'買賣分點數差 sell-buy>=2 ({N}d)':(ds-db)>=2,
       f'買賣分點數差 buy-sell>=4 ({N}d)':(db-ds)>=4, f'買賣分點數差 sell-buy>=4 ({N}d)':(ds-db)>=4,
       f'多分點大買 dbuy>=3 ({N}d)':db>=3, f'多分點大賣 dsell>=3 ({N}d)':ds>=3,
       f'有人買>3%量 ({N}d)':E[f'dbuy3_{N}']>=1, f'有人賣>3%量 ({N}d)':E[f'dsell3_{N}']>=1,
       f'前5累計淨集中 top10% ({N}d)':rk(f'cc5_{N}')>0.9, f'前5累計淨集中 bot10% ({N}d)':rk(f'cc5_{N}')<=0.1,
       f'買盤總流量 top10% ({N}d)':rk(f'buyflow_{N}')>0.9, f'賣壓總流量 最大10% ({N}d)':rk(f'sellflow_{N}')<=0.1,
       f'買超分散(分批買)&多買 ({N}d)':(db>=2)&(E[f'buyconc_{N}']<=0.35), f'買超集中(單一大買)&多買 ({N}d)':(db>=2)&(E[f'buyconc_{N}']>0.6),
       f'賣壓分散(分批賣)&多賣 ({N}d)':(ds>=2)&(E[f'sellconc_{N}']<=0.35), f'賣壓集中(單一大賣)&多賣 ({N}d)':(ds>=2)&(E[f'sellconc_{N}']>0.6)}
    if N<20:
        E['bp']=E[f'bavg_{N}']/E.c-1; r2=E.groupby('d').bp.rank(pct=True)
        S[f'買方均價高於收盤 top10% ({N}d)']=r2>0.9; S[f'買方均價低於收盤 bot10% ({N}d)']=r2<=0.1
    for k,m in S.items(): rows.append(full(E[m],k))
R=pd.DataFrame(rows); R=R[R.n>=150]; R.to_csv('run8c_results.csv',index=False)
cols=['signal','n','r60_ex','r60_ntT','r240_ex','r240_ntT','yrs_pos','h1_r240ex','h2_r240ex','win7_240_ex','win7_240_ntT','hit7_ex','hit7_ntT','tp_ex','tp_ntT']
print('K =',len(R)); print(R.sort_values('r240_ntT',ascending=False)[cols].head(14).round(3).to_string()); print(R.sort_values('r240_ntT')[cols].head(10).round(3).to_string())
