import numpy as np, pandas as pd, bt
pd.set_option('display.width',280); pd.set_option('display.max_columns',40); pd.set_option('display.max_rows',200)
src=open('run6.py',encoding='utf-8').read(); exec(src.split('rows=[]')[0])   # nw_t, prep, stat
E=prep(pd.read_parquet(f'{bt.S}/E_brk.parquet').drop(columns=[c for c in pd.read_parquet(f'{bt.S}/E_brk.parquet').columns if c.startswith(('ex_',)) or c in('tp',)]))
W=pd.read_parquet('wfeat.parquet'); E=E.merge(W,on=['code','d'],how='left')
for c in [c for c in E.columns if c.startswith(('dbuy','dsell'))]: E[c]=E[c].fillna(0)
E['bp5']=E.bavg_5/E.c-1; E['bp10']=E.bavg_10/E.c-1
mid=pd.Timestamp('2023-01-01')
def full(ev,label):
    r=stat(ev,label); ev=bt.cooldown(ev)
    for nm,sub in [('h1',ev[ev.d<mid]),('h2',ev[ev.d>=mid])]:
        r[nm+'_r240ex']=sub.ex_r240.mean() if len(sub)>30 else np.nan; r[nm+'_n']=len(sub)
    return r
rk=lambda f:E.groupby('d')[f].rank(pct=True)
S={}
for N in (5,10,20):
    for k in (2,3,5):
        S[f'多分點大買 dbuy1_{N}>={k}']=E[f'dbuy1_{N}']>=k
        S[f'多分點大賣 dsell1_{N}>={k}']=E[f'dsell1_{N}']>=k
        S[f'多買無大賣 dbuy1_{N}>={k} & dsell1=0']=(E[f'dbuy1_{N}']>=k)&(E[f'dsell1_{N}']==0)
        S[f'多賣無大買 dsell1_{N}>={k} & dbuy1=0']=(E[f'dsell1_{N}']>=k)&(E[f'dbuy1_{N}']==0)
    S[f'買賣分點數差>=3 ({N}d)']=(E[f'dbuy1_{N}']-E[f'dsell1_{N}'])>=3
    S[f'賣買分點數差>=3 ({N}d)']=(E[f'dsell1_{N}']-E[f'dbuy1_{N}'])>=3
    S[f'dbuy3_{N}>=1 (有人買>3%量)']=E[f'dbuy3_{N}']>=1
    S[f'dsell3_{N}>=1 (有人賣>3%量)']=E[f'dsell3_{N}']>=1
    S[f'前5累計淨集中 top10% ({N}d)']=rk(f'cc5_{N}')>0.9
    S[f'前5累計淨集中 bot10% ({N}d)']=rk(f'cc5_{N}')<=0.1
    S[f'買盤總流量 top10% ({N}d)']=rk(f'buyflow_{N}')>0.9
    S[f'賣壓總流量 最大10% ({N}d)']=rk(f'sellflow_{N}')<=0.1
    # split vs single within populations
    pb=E[f'dbuy1_{N}']>=2; ps_=E[f'dsell1_{N}']>=2
    S[f'買盤集中(單一大買占比高) 多買母體 ({N}d)']=pb&(E[f'buyconc_{N}']>0.6)
    S[f'買盤分散(分批買) 多買母體 ({N}d)']=pb&(E[f'buyconc_{N}']<=0.35)
    S[f'賣壓集中(單一大賣) 多賣母體 ({N}d)']=ps_&(E[f'sellconc_{N}']>0.6)
    S[f'賣壓分散(分批賣) 多賣母體 ({N}d)']=ps_&(E[f'sellconc_{N}']<=0.35)
for N in (5,10):
    S[f'買方均價高於收盤 top10% ({N}d, 追價)']=rk(f'bp{N}')>0.9
    S[f'買方均價低於收盤 bot10% ({N}d, 低接)']=rk(f'bp{N}')<=0.1
rows=[full(E[m],k) for k,m in S.items()]
R=pd.DataFrame(rows)
# --- null: random signals of similar density to calibrate NW t spread
rng=np.random.default_rng(0); nulls=[]
for q in range(40):
    p=rng.choice([0.01,0.03,0.08]); m=rng.random(len(E))<p
    r=stat(E[m],'null'); nulls.append((r['r240_ntT'],r['r60_ntT'],r['hit7_ntT'],r['win7_240_ntT']))
nl=np.array(nulls,float); print('NULL (random signals) NW-t: sd r240=%.2f r60=%.2f hit7=%.2f win=%.2f ; max|t| r240=%.2f'%(np.nanstd(nl[:,0]),np.nanstd(nl[:,1]),np.nanstd(nl[:,2]),np.nanstd(nl[:,3]),np.nanmax(abs(nl[:,0]))))
print('K tests =',len(R))
R=R[R.n>=150]
cols=['signal','n','r60_ex','r60_ntT','r240_ex','r240_ntT','yrs_pos','h1_r240ex','h2_r240ex','win7_240_ex','win7_240_ntT','hit7_ex','hit7_ntT','tp_ex','tp_ntT']
R.to_csv('run8_results.csv',index=False)
print(R.sort_values('r240_ntT',ascending=False)[cols].head(18).round(3).to_string())
print(R.sort_values('r240_ntT')[cols].head(12).round(3).to_string())
