import numpy as np, pandas as pd, bt, time
t0=time.time()
T=pd.read_parquet('T10m.parquet')
px=pd.read_parquet(f'{bt.S}/px.parquet',columns=['d','code','v']); px['d']=pd.to_datetime(px.d); px=px[(px.d>='2021-07-01')&(px.d<='2024-12-31')]
days=np.sort(px.d.unique()); di={d:i for i,d in enumerate(days)}
T['i']=T.d.map(di).astype('int32'); px['i']=px.d.map(di).astype('int32')
codes=np.sort(T.code.unique()); T['b']=pd.factorize(T.b)[0].astype('int32')
# rolling window volume per code (lots)
pv=px.pivot_table(index='i',columns='code',values='v',aggfunc='sum').reindex(range(len(days)))
out_all=[]
def window_feats(Tc,N,th):
    R=Tc[Tc.sh.abs()>=th][['code','i','b','net','avgp','sh']].copy()
    R=R[np.isfinite(R.sh)]
    offs=np.arange(N,dtype='int32')
    X=R.loc[R.index.repeat(N)].copy(); X['tgt']=X.i.values+np.tile(offs,len(R)); X=X[X.tgt<len(days)]
    X['nav']=X.net*X.avgp
    g=X.groupby(['code','tgt','b'],sort=False).agg(cum=('net','sum'),nav=('nav','sum')).reset_index()
    return g
def feats(g,N,vol_n):
    g=g.merge(vol_n,on=['code','tgt'],how='left'); g['cs']=g.cum/g.vol_n
    pos=g[g.cum>0]; neg=g[g.cum<0]
    o=pd.DataFrame(index=pd.MultiIndex.from_frame(g[['code','tgt']].drop_duplicates()))
    o[f'dbuy1_{N}']=pos[pos.cs>=0.01].groupby(['code','tgt']).size()
    o[f'dsell1_{N}']=neg[neg.cs<=-0.01].groupby(['code','tgt']).size()
    o[f'dbuy3_{N}']=pos[pos.cs>=0.03].groupby(['code','tgt']).size()
    o[f'dsell3_{N}']=neg[neg.cs<=-0.03].groupby(['code','tgt']).size()
    ps=pos.sort_values(['code','tgt','cum'],ascending=[True,True,False]); ps['r']=ps.groupby(['code','tgt']).cumcount()
    ns=neg.sort_values(['code','tgt','cum']); ns['r']=ns.groupby(['code','tgt']).cumcount()
    tb=ps[ps.r<5].groupby(['code','tgt']).cum.sum(); ts=ns[ns.r<5].groupby(['code','tgt']).cum.sum()
    vn=g.drop_duplicates(['code','tgt']).set_index(['code','tgt']).vol_n
    o[f'cc5_{N}']=(tb.reindex(o.index).fillna(0)+ts.reindex(o.index).fillna(0))/vn.reindex(o.index)
    o[f'buyconc_{N}']=ps[ps.r<1].groupby(['code','tgt']).cum.sum().reindex(o.index)/ps.groupby(['code','tgt']).cum.sum().reindex(o.index)   # share of biggest buyer in material buying
    o[f'sellconc_{N}']=ns[ns.r<1].groupby(['code','tgt']).cum.sum().reindex(o.index)/ns.groupby(['code','tgt']).cum.sum().reindex(o.index)
    o[f'buyflow_{N}']=pos.groupby(['code','tgt']).cum.sum().reindex(o.index)/vn.reindex(o.index)
    o[f'sellflow_{N}']=neg.groupby(['code','tgt']).cum.sum().reindex(o.index)/vn.reindex(o.index)
    o[f'bavg_{N}']=pos.groupby(['code','tgt']).nav.sum().reindex(o.index)/pos.groupby(['code','tgt']).cum.sum().reindex(o.index)  # buyers' avg price
    for c in o.columns:
        if c.startswith(('dbuy','dsell')): o[c]=o[c].fillna(0)
    return o.reset_index()
res={}
for N,th in [(5,0.01),(10,0.01),(20,0.02)]:
    ch=[]
    for k in range(0,len(codes),250):
        cs=codes[k:k+250]; Tc=T[T.code.isin(cs)]
        g=window_feats(Tc,N,th)
        vn=pv[cs].rolling(N,min_periods=N).sum().stack().rename('vol_n').reset_index(); vn.columns=['tgt','code','vol_n']
        ch.append(feats(g,N,vn)); print(N,k,f'{time.time()-t0:.0f}s',flush=True)
    res[N]=pd.concat(ch,ignore_index=True)
W=res[5]
for N in (10,20): W=W.merge(res[N],on=['code','tgt'],how='outer')
W['d']=pd.Series(days)[W.tgt.values].values
W.drop(columns='tgt').to_parquet('wfeat.parquet'); print(W.shape,f'{time.time()-t0:.0f}s')
