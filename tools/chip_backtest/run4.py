import numpy as np, pandas as pd, bt, glob, os
pd.set_option('display.width',250)
E=pd.read_parquet(f'{bt.S}/E_brk.parquet')[['code','d','vol','ex_r60','ex_r240','ex_hit7','ex_win7_240','r60','r240','win7_240','hit7','elig','ex_w7_60','mom60']]
fs=[f for f in glob.glob(f'{bt.S}/brk_shards/*_top.parquet') if os.path.basename(f)[:4] in('2021','2022','2023','2024')]
T=pd.concat([pd.read_parquet(f) for f in fs],ignore_index=True); T['d']=pd.to_datetime(T.d)
T=T[T.net>0].sort_values(['d','code','net'],ascending=[True,True,False])
T['rk']=T.groupby(['d','code']).cumcount(); T=T[T.rk<5]
T=T.merge(E,on=['code','d'],how='inner'); T=T[T.elig]
T['share']=T.net/T.vol   # share of the day's volume
T=T[(T.share>=0.05)&(T.net>=50)]    # meaningful accumulation only
print('rows',len(T),'brokers',T.b.nunique())
cut=pd.Timestamp('2022-07-01'); gap=pd.Timestamp('2022-10-15')
tr=T[T.d<cut]; te=T[T.d>=gap]
def score(df): 
    g=df.groupby('b').agg(n=('ex_r60','size'),m60=('ex_r60','mean'),m240=('ex_r240','mean'))
    return g
# persistence: split train period in two halves -> use train vs test
a=score(tr); b=score(te)
j=a.join(b,lsuffix='_tr',rsuffix='_te',how='inner'); j=j[(j.n_tr>=60)&(j.n_te>=60)]
print('brokers with enough events in both:',len(j))
print('rank corr train-vs-test ex_r60:',j.m60_tr.corr(j.m60_te,method='spearman').round(3),' ex_r240 (te):',j.m60_tr.corr(j.m240_te,method='spearman').round(3))
# select top/bottom 15 by train score
sel=j.sort_values('m60_tr',ascending=False)
for name,brs in [('TOP15 train brokers',sel.index[:15]),('BOTTOM15 train brokers',sel.index[-15:]),('ALL brokers (control)',sel.index)]:
    ev=te[te.b.isin(brs)].drop_duplicates(['code','d'])
    ev=bt.cooldown(ev)
    ev=ev.assign(elig=True)
    r=bt.evaluate(ev,name,min_n=40); print({k:(round(v,3) if isinstance(v,float) else v) for k,v in r.items()})
print(sel.head(10).round(3)); print(sel.tail(5).round(3))
