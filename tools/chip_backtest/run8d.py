import numpy as np, pandas as pd, bt
exec(open('run6.py',encoding='utf-8').read().split('rows=[]')[0].split('def prep')[0])
E=pd.read_parquet(f'{bt.S}/E_brk.parquet'); W=pd.read_parquet('wfeat.parquet'); E=E.merge(W,on=['code','d'],how='left')
for c in [c for c in E.columns if c.startswith(('dbuy','dsell'))]: E[c]=E[c].fillna(0)
E['imb10']=E.dbuy1_10-E.dsell1_10; E['imb20']=E.dbuy1_20-E.dsell1_20
cheap=['t_r5','f_r20','big400_d13','holders_ch4','mb_ch20','mom20','mom60','hi250']
brk=['buyflow_10','sellflow_10','cc5_10','imb10','imb20','conc15_20','top1_20','nbal_20','geo3_20','main_r20']
brk_only_pure=[b for b in brk if b!='main_r20']
dates=np.sort(E.d.unique())[::5]; Es=E[E.d.isin(dates)]
def ranks(df,cols): 
    return df.groupby('d')[cols].rank(pct=True).fillna(0.5)-0.5
def fm(y,cols,L):
    X=ranks(Es,cols); out=[]; r2=[]
    Y=Es.groupby('d')[y].rank(pct=True)-0.5; ok=Y.notna()
    for d,idx in Es[ok].groupby('d').groups.items():
        x=X.loc[idx].values; yy=Y.loc[idx].values
        if len(yy)<200: continue
        A=np.c_[np.ones(len(yy)),x]; b,res,_,_=np.linalg.lstsq(A,yy,rcond=None); out.append(b[1:])
        r2.append(1-((yy-A@b)**2).sum()/((yy-yy.mean())**2).sum())
    B=np.array(out); return pd.DataFrame({'slope':B.mean(0),'t':[nw_t(B[:,j],L) for j in range(B.shape[1])]},index=cols),np.mean(r2)
for y,L in [('r60',12),('r240',48)]:
    a,ra=fm(y,cheap,L); b,rb=fm(y,cheap+brk_only_pure,L); c,rc=fm(y,cheap+brk,L)
    print(f'\n== target {y}: avg cross-sectional R2  cheap={ra:.5f}  +broker(pure)={rb:.5f}  +broker+主力={rc:.5f}  (gain pure={rb-ra:.5f})')
    print(pd.concat([a.add_prefix('cheap_'),b.add_prefix('plusBrk_')],axis=1).round(4).to_string())
