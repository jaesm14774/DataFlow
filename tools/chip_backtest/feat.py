import numpy as np, pandas as pd, bt
S=bt.S
def rs(df,col,w,fn='sum'):
    return df.groupby('code')[col].transform(lambda s: getattr(s.rolling(w,min_periods=max(2,w//2)),fn)())
def build():
    P=pd.read_parquet(f'{S}/panel.parquet')
    P=P[['code','d','entry','c','v','adv20','mom20','mom60','mom120','hi250','elig','gap']+[c for c in P.columns if c.startswith(('r','hit','win','w7','ex_','maxret','fhit','min60'))]]
    P=P.sort_values(['code','d']).reset_index(drop=True)
    vol=P.v*1000
    # 主力買賣超 (lots)
    m=pd.read_parquet(f'{S}/main.parquet'); m['d']=pd.to_datetime(m.d); m['m']=m.m.astype(float)
    P=P.merge(m,on=['code','d'],how='left')
    P['main_r']=P.m*1000/vol
    P['main_r5']=rs(P.assign(x=P.main_r.fillna(0)),'x',5); P['main_r20']=rs(P.assign(x=P.main_r.fillna(0)),'x',20)
    P.loc[P.m.isna(),['main_r','main_r5','main_r20']]=np.nan
    # institutions (shares)
    i=pd.read_parquet(f'{S}/inst.parquet'); i['d']=pd.to_datetime(i.d)
    P=P.merge(i,on=['code','d'],how='left')
    for k in 'fts':
        P[f'{k}_r']=P[k]/vol
        P[f'{k}_r5']=rs(P.assign(x=P[k].fillna(0)/vol),'x',5); P[f'{k}_r20']=rs(P.assign(x=P[k].fillna(0)/vol),'x',20)
        P.loc[P[k].isna(),[f'{k}_r5',f'{k}_r20']]=np.nan
    # holders weekly (available as-of date +4d)
    h=pd.read_parquet(f'{S}/holder.parquet')
    h['d']=pd.to_datetime(h['資料日期'].astype(str),format='%Y%m%d',errors='coerce'); h=h.dropna(subset=['d'])
    h=h.rename(columns={'證券代號':'code','>400張大股東持有百分比':'big400','>1000張大股東持有百分比':'big1000','總股東人數':'holders','平均張數/人':'avg_lots','集保總張數':'tot'})
    h=h[['code','d','big400','big1000','holders','avg_lots','tot']].drop_duplicates(['code','d']).sort_values(['code','d'])
    for c in ['big400','big1000','holders','tot']: h[c]=pd.to_numeric(h[c],errors='coerce')
    g=h.groupby('code')
    h['big400_d4']=h.big400-g.big400.shift(4); h['big400_d13']=h.big400-g.big400.shift(13)
    h['big1000_d4']=h.big1000-g.big1000.shift(4); h['big1000_d13']=h.big1000-g.big1000.shift(13)
    h['holders_ch4']=h.holders/g.holders.shift(4)-1
    h['tot_ch4']=h.tot/g.tot.shift(4)-1   # capital increase filter
    h['avail']=h.d+pd.Timedelta(days=4)
    h=h.sort_values('avail')
    P=P.sort_values('d')
    hh=h.drop(columns=['d']).rename(columns={'avail':'d'})
    P=pd.merge_asof(P,hh.sort_values('d'),on='d',by='code',tolerance=pd.Timedelta(days=10),direction='backward')
    # margin
    mg=pd.read_parquet(f'{S}/margin.parquet'); mg['d']=pd.to_datetime(mg.d)
    P=P.merge(mg,on=['code','d'],how='left').sort_values(['code','d']).reset_index(drop=True)
    P['mb_ch20']=P.groupby('code').mb.transform(lambda s:s/s.shift(20)-1)
    P['sb_ch20']=P.groupby('code').sb.transform(lambda s:s/s.shift(20)-1)
    return P
if __name__=='__main__':
    P=build(); P.to_parquet(f'{S}/feat.parquet'); print(P.shape); print(P[['main_r','main_r5','f_r5','t_r5','big400_d4','holders_ch4','mb_ch20','sr']].describe().T)
