import numpy as np, pandas as pd, re, glob, os, bt, ast
pd.set_option('display.width',250)
a=pd.read_parquet('branch_addr.parquet'); co=pd.read_parquet('co.parquet'); gi=pd.read_parquet('geoinfo.parquet')
def loc(s):
    try: x,y=ast.literal_eval(s); return float(x),float(y)
    except Exception: return (np.nan,np.nan)
a[['blon','blat']]=a.geometry.apply(lambda s:pd.Series(loc(s)))
cog=pd.read_sql if False else None
import db; from sqlalchemy import text
cg=pd.read_sql(text("select 證券代號 code,geometry g from 公司資訊"),db.eng()); cg[['clon','clat']]=cg.g.apply(lambda s:pd.Series(loc(s)))
# 1) validate: recompute haversine for a sample vs 地緣券商資訊.dist
s=gi[gi.code.isin(cg.code.sample(40,random_state=1))].merge(cg[['code','clon','clat']],on='code').merge(a[['bname','blon','blat']].rename(columns={'bname':'b'}).drop_duplicates('b'),on='b')
R=6371; p=np.pi/180
s['hav']=2*R*np.arcsin(np.sqrt(np.sin((s.blat-s.clat)*p/2)**2+np.cos(s.clat*p)*np.cos(s.blat*p)*np.sin((s.blon-s.clon)*p/2)**2))
print('geometry coverage: branches',a.blon.notna().mean().round(3),'companies',cg.clon.notna().mean().round(3))
print('dist(table) vs haversine: corr',round(s.dist.corr(s.hav),4),' median abs diff km',round((s.dist-s.hav).abs().median(),3))
# 2) administrative-area definition
def adm(x):
    m=re.match(r'^\s*(?:台|臺)?(.{1,2}?[縣市])(.{1,3}?[區鄉鎮市])?',str(x).replace('臺','台')); 
    return (m.group(1) if m else None, (m.group(1)+(m.group(2) or '')) if m else None)
a[['bcity','bdist']]=a.addr.apply(lambda x:pd.Series(adm(x))); co[['ccity','cdist']]=co.addr.apply(lambda x:pd.Series(adm(x)))
print('city parsed: branches',a.bcity.notna().mean().round(3),'companies',co.ccity.notna().mean().round(3), '| district:',a.bdist.str.contains('區|鄉|鎮').mean().round(3),co.cdist.str.contains('區|鄉|鎮',na=False).mean().round(3))
bmap=a.drop_duplicates('bname').set_index('bname')[['bcity','bdist']]
fs=[f for f in glob.glob(f'{bt.S}/brk_shards/*_top.parquet') if os.path.basename(f)[:4] in('2021','2022','2023','2024')]
T=pd.concat([pd.read_parquet(f) for f in fs],ignore_index=True); T['d']=pd.to_datetime(T.d)
T=T.join(bmap,on='b').merge(co[['code','ccity','cdist']],on='code')
FOREIGN='港商麥格理|東方匯理|美林|台灣摩根士丹利|美商高盛|瑞士信貸|港商德意志|香港上海匯豐|新加坡商瑞銀|花旗|摩根大通|野村|大和國泰|法商巴黎|荷銀|日商'
T=T[~T.b.astype(str).str.contains(FOREIGN,na=False)]
T['same_city']=T.bcity==T.ccity; T['same_dist']=(T.bdist==T.cdist)&T.cdist.str.contains('區|鄉|鎮',na=False)
g3=gi[gi.dist<=3][['code','b']].drop_duplicates().assign(near3=True)
T=T.merge(g3,on=['code','b'],how='left'); T['near3']=T.near3.fillna(False).astype(bool)
agg=pd.concat([T[T.near3].groupby(['code','d']).net.sum().rename('rad3_net'),T[T.same_dist].groupby(['code','d']).net.sum().rename('dist_net'),T[T.same_city&~T.same_dist].groupby(['code','d']).net.sum().rename('city_only_net')],axis=1).reset_index()
EB=pd.read_parquet(f'{bt.S}/E_brk.parquet').merge(agg,on=['code','d'],how='left').sort_values(['code','d'])
for c in ['rad3_net','dist_net','city_only_net']:
    EB[c]=EB[c].fillna(0)
    for w in (5,20): EB[f'{c}{w}']=EB.groupby('code')[c].transform(lambda s:s.rolling(w,min_periods=w).sum())/EB.groupby('code').vol.transform(lambda s:s.rolling(w,min_periods=w).sum())
exec(open('run6.py',encoding='utf-8').read().split('rows=[]')[0].split("def prep")[0])   # nw_t
EB['tp']=np.where(EB.hit7==1,0.07,EB.r240); 
rows=[]
for f in ['rad3_net5','rad3_net20','dist_net5','dist_net20','city_only_net5','city_only_net20']:
    pop=EB[EB[f].notna()&(EB[f]!=0)].copy(); pop['rk']=pop.groupby('d')[f].rank(pct=True); pop=pop[pop.groupby('d')[f].transform('size')>=40]
    top=bt.cooldown(pop[pop.rk>0.9]); bot=bt.cooldown(pop[pop.rk<=0.1]); out={'feature':f,'pop_share':round(len(pop)/len(EB),2),'n':f'{len(top)}/{len(bot)}'}
    for c,L in [('ex_r60',3),('ex_r240',11),('ex_win7_240',11),('ex_hit7',11)]:
        pm=pop.groupby('d')[c].mean(); ta=top[c]-top.d.map(pm); bo=bot[c]-bot.d.map(pm)
        A=ta.groupby(top.d.dt.to_period('M')).mean(); B=bo.groupby(bot.d.dt.to_period('M')).mean(); sp=(A-B).dropna()
        out[c+'_topM']=top[c].mean(); out[c+'_spread']=sp.mean(); out[c+'_t']=nw_t(sp.values,L)
        if c=='ex_r240':
            yy=(top.groupby(top.d.dt.year)[c].mean()-bot.groupby(bot.d.dt.year)[c].mean()).dropna(); out['yrs_pos']=f"{(yy>0).sum()}/{len(yy)}"
    rows.append(out)
print(pd.DataFrame(rows).round(3).to_string())
