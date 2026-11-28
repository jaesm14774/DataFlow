import numpy as np, pandas as pd, bt
pd.set_option('display.width',260); pd.set_option('display.max_columns',40)
def nw_t(x,L):
    x=np.asarray(pd.Series(x).dropna(),float); n=len(x)
    if n<8: return np.nan
    u=x-x.mean(); g0=(u*u).sum()/n; v=g0
    for l in range(1,min(L,n-1)+1): v+=2*(1-l/(L+1))*(u[l:]*u[:-l]).sum()/n
    return x.mean()/np.sqrt(v/n) if v>0 else np.nan
def prep(E):
    E=E.copy()
    E['tp']=np.where(E.hit7==1,0.07,E.r240); E.loc[E.hit7.isna()|E.r240.isna(),'tp']=np.nan
    E['mq']=E.groupby('d').mom60.transform(lambda s:pd.qcut(s.rank(method='first'),5,labels=False,duplicates='drop'))
    E['lq']=E.groupby('d').adv20.transform(lambda s:pd.qcut(s.rank(method='first'),3,labels=False,duplicates='drop'))
    for c in bt.OUTCOMES+['tp']: E['ex_'+c]=E[c]-E.groupby(['d','mq','lq'])[c].transform('mean')
    return E
def stat(ev,label):
    ev=bt.cooldown(ev); ev=ev.assign(m=ev.d.dt.to_period('M')); r={'signal':label,'n':len(ev)}
    for c,L in [('r60',3),('r240',11),('hit7',11),('win7_240',11),('tp',11)]:
        mo=ev.groupby('m')['ex_'+c].mean(); r[c+'_ex']=ev['ex_'+c].mean(); r[c+'_ntT']=nw_t(mo.values,L)
    r['yrs_pos']=f"{(ev.groupby(ev.d.dt.year).ex_r240.mean()>0).sum()}/{ev.d.dt.year.nunique()}"
    r['win7']=ev.win7_240.mean(); r['hit7']=ev.hit7.mean(); return r
rows=[]
# ---- non-broker signals
P=pd.read_parquet(f'{bt.S}/feat.parquet').replace([np.inf,-np.inf],np.nan); E=prep(P[P.elig])
rk=lambda f:E.groupby('d')[f].rank(pct=True); ok=E.tot_ch4.abs()<0.02
B=rk('t_r5')>0.8; F=rk('f_r20')>0.8; C=(rk('big400_d13')>0.8)&ok; A=rk('main_r20')>0.8
for k,m in {'投信5日 top20':B,'投信&外資20日':B&F,'投信&大戶13週':B&C,'外資20日 top20':F,'大戶13週 top20':C,'主力20日 top20(2021-)':A,'ctrl 近250日新高>0.95':E.hi250>0.95,'ctrl 動能60日 top20':rk('mom60')>0.8}.items():
    rows.append(stat(E[m],k))
base=E.groupby('d')[['win7_240','hit7','tp','r240']].mean().mean(); print('baseline pooled-by-date means',base.round(3).to_dict())
# ---- broker signals (2021-08..2024-12)
EB=prep(pd.read_parquet(f'{bt.S}/E_brk.parquet'))
rkb=lambda f:EB.groupby('d')[f].rank(pct=True)
for k,m in {'分點集中度top15 20日 top10':rkb('conc15_20')>0.9,'最大買超分點占比20日 top10':rkb('top1_20')>0.9,'買賣家數差20日 BOT10':rkb('nbal_20')<=0.1}.items():
    rows.append(stat(EB[m],'[分點'+'] '+k))
# ---- geo: within non-zero population, top vs bottom spread
for f in ['geo3_5','geo3_20','geo1_5','geo1_20']:
    pop=EB[EB[f].notna()&(EB[f]!=0)].copy()
    pop['rk']=pop.groupby('d')[f].rank(pct=True); pop=pop[pop.groupby('d')[f].transform('size')>=40]
    top=bt.cooldown(pop[pop.rk>0.9]); bot=bt.cooldown(pop[pop.rk<=0.1])
    # baseline = non-zero population mean on same date (not whole market)
    for c in ['r240','r60','win7_240','hit7']:
        pm=pop.groupby('d')[c].mean()
        top[c+'_pe']=top[c]-top.d.map(pm); bot[c+'_pe']=bot[c]-bot.d.map(pm)
    out={'signal':f'[geo spread] {f} top10 - bottom10 (nonzero pop.)','n':f'{len(top)}/{len(bot)}','pop_share':round(len(pop)/len(EB),2)}
    for c,L in [('r60',3),('r240',11),('win7_240',11),('hit7',11)]:
        a=top.groupby(top.d.dt.to_period('M'))[c+'_pe'].mean(); b=bot.groupby(bot.d.dt.to_period('M'))[c+'_pe'].mean()
        s=(a-b).dropna(); out[c+'_spread']=s.mean(); out[c+'_T']=nw_t(s.values,L); out[c+'_top_vs_pop']=top[c+'_pe'].mean(); out[c+'_bot_vs_pop']=bot[c+'_pe'].mean()
    rows.append(out)
R=pd.DataFrame(rows); R.to_csv(f'{bt.S}/run6.csv',index=False)
c1=['signal','n','r60_ex','r60_ntT','r240_ex','r240_ntT','win7_240_ex','win7_240_ntT','hit7_ex','hit7_ntT','tp_ex','tp_ntT','yrs_pos','win7','hit7']
print(R[R.signal.str.startswith('[geo')==False][c1].round(3).to_string())
g=R[R.signal.str.startswith('[geo')]; print(g.drop(columns=[c for c in g.columns if c in c1[2:] or c.endswith(('_ex','_ntT'))]).round(3).to_string())
