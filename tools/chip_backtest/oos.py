import numpy as np, pandas as pd, glob, os, bt, time
pd.set_option('display.width',250)
exec(open('run6.py',encoding='utf-8').read().split('rows=[]')[0].split('def prep')[0])   # nw_t
t0=time.time(); LO,HI=pd.Timestamp('2025-04-01'),pd.Timestamp('2026-08-31')
def shards(top):
    out=[]
    for f in glob.glob('brk_shards/*.parquet'):
        if f.endswith('_top.parquet')!=top: continue
        if os.path.basename(f)[:4] in ('2025','2026'): out.append(f)
    return out
A=pd.concat([pd.read_parquet(f) for f in shards(False)],ignore_index=True); A=A[A.code.notna()]; A['d']=pd.to_datetime(A.d); A=A[(A.d>=LO)&(A.d<=HI)]
T=pd.concat([pd.read_parquet(f) for f in shards(True)],ignore_index=True); T['d']=pd.to_datetime(T.d); T=T[(T.d>=LO)&(T.d<=HI)]
px=pd.read_parquet(f'{bt.S}/px.parquet',columns=['d','code','v']); px['d']=pd.to_datetime(px.d); px=px[(px.d>='2025-01-01')&px.code.str.match(r'^[1-9]\d{3}$')]
cal=np.sort(px[(px.d>=LO)&(px.d<=HI)].d.unique()); di={pd.Timestamp(d):i for i,d in enumerate(cal)}; n=len(cal)
hv=np.zeros(n,bool)
for d in pd.to_datetime(A.d.unique()):
    if d in di: hv[di[d]]=True
def contiguous(N): return pd.Series(hv).rolling(N).sum().eq(N).values
A['i']=A.d.map(di); T['i']=T.d.map(di); T=T[T.i.notna()]; A=A[A.i.notna()]; T['i']=T.i.astype(int); A['i']=A.i.astype(int)
codes=np.sort(A.code.unique())
def piv(col): return A.pivot_table(index='i',columns='code',values=col,aggfunc='sum').reindex(range(n))
nb,ns,nbr,vol=piv('nb'),piv('ns'),piv('nbr'),piv('vol')
def roll(df,N): return df.rolling(N,min_periods=N).sum()
nbal=((roll(nb,20)-roll(ns,20))/roll(nbr,20)).stack().rename('nbal_20').reset_index(); nbal.columns=['tgt','code','nbal_20']
T=T.merge(A[['code','i','vol']],on=['code','i'],how='left'); T['sh']=T.net/T.vol; T=T[np.isfinite(T.sh)]
T['b']=pd.factorize(T.b)[0].astype('int32')
def wfeat(N,th):
    R=T[T.sh.abs()>=th][['code','i','b','net']]; out=[]
    for k in range(0,len(codes),250):
        cs=codes[k:k+250]; Rc=R[R.code.isin(cs)]
        X=Rc.loc[Rc.index.repeat(N)].copy(); X['tgt']=X.i.values+np.tile(np.arange(N),len(Rc)); X=X[X.tgt<n]
        g=X.groupby(['code','tgt','b'],sort=False).net.sum().reset_index()
        vn=vol[cs].rolling(N,min_periods=N).sum().stack().rename('vol_n').reset_index(); vn.columns=['tgt','code','vol_n']
        g=g.merge(vn,on=['code','tgt'],how='left'); g['cs']=g.net/g.vol_n
        pos=g[g.net>0]; neg=g[g.net<0]; key=['code','tgt']
        o=pd.DataFrame(index=pd.MultiIndex.from_frame(g[key].drop_duplicates()))
        o[f'dbuy1_{N}']=pos[pos.cs>=0.01].groupby(key).size(); o[f'dsell1_{N}']=neg[neg.cs<=-0.01].groupby(key).size()
        s_=neg.sort_values(key+['net']); s_['r']=s_.groupby(key).cumcount()
        o[f'sellconc_{N}']=s_[s_.r<1].groupby(key).net.sum().reindex(o.index)/neg.groupby(key).net.sum().reindex(o.index)
        vnn=g.drop_duplicates(key).set_index(key).vol_n
        o[f'buyflow_{N}']=pos.groupby(key).net.sum().reindex(o.index)/vnn.reindex(o.index)
        for c in o.columns:
            if c.startswith(('dbuy','dsell')): o[c]=o[c].fillna(0)
        out.append(o.reset_index())
    return pd.concat(out,ignore_index=True)
w10=wfeat(10,0.01); w20=wfeat(20,0.01)
F=w10.merge(w20,on=['code','tgt'],how='outer').merge(nbal,on=['code','tgt'],how='outer'); F['d']=pd.to_datetime(cal[F.tgt.values])
P=pd.read_parquet(f'{bt.S}/feat.parquet',columns=['code','d','elig','r60','mom60','adv20','t_r5','f_r20']).replace([np.inf,-np.inf],np.nan)
P=P[(P.d>=LO)&(P.d<=HI)&P.elig]; P=P[P.code.isin(set(codes))].merge(F.drop(columns='tgt'),on=['code','d'],how='left').reset_index(drop=True)
dayidx=P.d.map(di).astype(int).values; P['ok10']=contiguous(10)[dayidx]; P['ok20']=contiguous(20)[dayidx]
for c in [c for c in P.columns if c.startswith(('dbuy','dsell'))]: P[c]=P[c].fillna(0)
P['mq']=P.groupby('d').mom60.transform(lambda s:pd.qcut(s.rank(method='first'),5,labels=False,duplicates='drop'))
P['lq']=P.groupby('d').adv20.transform(lambda s:pd.qcut(s.rank(method='first'),3,labels=False,duplicates='drop'))
bins=[-1,0,1,2,4,7,12,1e9]
P['ab10']=pd.cut(P.dbuy1_10+P.dsell1_10,bins,labels=False); P['ab20']=pd.cut(P.dbuy1_20+P.dsell1_20,bins,labels=False)
P['ex']=P.r60-P.groupby(['d','mq','lq']).r60.transform('mean')
P['ex_a10']=P.r60-P.groupby(['d','mq','lq','ab10']).r60.transform('mean'); P['ex_a20']=P.r60-P.groupby(['d','mq','lq','ab20']).r60.transform('mean')
def pr(col,mask):
    r=pd.Series(np.nan,index=P.index); r[mask]=P[mask].groupby('d')[col].rank(pct=True); return r
S={}
S['benchmark 投信5日 top20%']=(pr('t_r5',P.t_r5.notna())>0.8,'ex')
S['benchmark 投信&外資']=((pr('t_r5',P.t_r5.notna())>0.8)&(pr('f_r20',P.f_r20.notna())>0.8),'ex')
bf=P.ok10&P.buyflow_10.notna()
S['(1) 10日買盤總流量 top10% [活躍度配對]']=(pr('buyflow_10',bf)>0.9,'ex_a10')
S['(1b) 同上 [僅動能流動性配對]']=(pr('buyflow_10',bf)>0.9,'ex')
nbm=P.ok20&P.nbal_20.notna(); S['(2) 買賣家數差20日 BOT10% [活躍度配對]']=(pr('nbal_20',nbm)<=0.1,'ex_a20')
S['(3) 賣壓集中且多賣 20d [活躍度配對]']=(P.ok20&(P.dsell1_20>=2)&(P.sellconc_20>0.6),'ex_a20')
rows=[]
for k,(m,ex) in S.items():
    ev=bt.cooldown(P[m.fillna(False)].assign(elig=True))
    if len(ev)<30: rows.append({'signal':k,'n':len(ev)}); continue
    ev=ev.assign(mo=ev.d.dt.to_period('M'),q=ev.d.dt.to_period('Q')); e=ev[ex].dropna(); ev=ev.loc[e.index]
    mo=ev.groupby('mo')[ex].mean(); mm=e.mean(); nn=len(e)
    gs=(e-mm).groupby(ev.code).sum(); gq=(e-mm).groupby(ev.q).sum(); gsq=(e-mm).groupby(ev.code.astype(str)+ev.q.astype(str)).sum()
    v=((gs**2).sum()+(gq**2).sum()-(gsq**2).sum())/nn**2
    rows.append({'signal':k,'n':nn,'stocks':ev.code.nunique(),'months':len(mo),'r60_ex':mm,'t_NWmonth':nw_t(mo.values,3),'t_2way':mm/np.sqrt(max(v,1e-18)),'months_pos':(mo>0).mean()})
print('OOS window',LO.date(),HI.date(),'days with data',int(hv.sum()),'of',n,'| universe stocks',len(codes),f'{time.time()-t0:.0f}s')
print(pd.DataFrame(rows).round(3).to_string())
