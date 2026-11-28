import sys, os, time, numpy as np, pandas as pd
import db
from sqlalchemy import text
start,end,part=sys.argv[1],sys.argv[2],int(sys.argv[3]); nparts=int(sys.argv[4])
S=os.environ.get('CHIP_DATA',os.path.join(os.path.dirname(os.path.abspath(__file__)),'data'))
e=db.eng()
px=pd.read_parquet(f'{S}/px.parquet',columns=['d']); days=sorted(pd.to_datetime(px.d.unique()))
days=[d for d in days if pd.Timestamp(start)<=d<=pd.Timestamp(end)]
days=days[part::nparts]
geo=pd.read_parquet(f'{S}/geoinfo.parquet'); geo=geo[geo.dist<=3][['code','b','dist']].drop_duplicates(['code','b'])
FOREIGN=['港商麥格理','東方匯理','美林','台灣摩根士丹利','美商高盛','瑞士信貸','港商德意志','香港上海匯豐','新加坡商瑞銀','花旗','摩根大通','野村','大和國泰','法商巴黎','荷銀','日商']
for d in days:
    fn=f'{S}/brk_shards/{d:%Y%m%d}.parquet'
    if os.path.exists(fn): continue
    for attempt in range(3):
        try:
            df=pd.read_sql(text("select 證券代號 code,券商名稱 b,買量 bv,賣量 sv,買賣超 net,均價 avgp from 券商進出總表 where 資料日期=:d"),e,params={'d':f'{d:%Y-%m-%d}'}); break
        except Exception as ex:
            print('retry',d,ex); time.sleep(10); df=None
    if df is None: continue
    if df.empty:
        pd.DataFrame({'code':[]}).to_parquet(fn); continue
    df['net']=pd.to_numeric(df.net,errors='coerce').fillna(0)
    df=df.sort_values(['code','net'],ascending=[True,False])
    g=df.groupby('code')
    df['rb']=g.cumcount(); df['rs']=g.cumcount(ascending=False)
    vol=g.bv.sum()
    out=pd.DataFrame({'vol':vol,'nb':g.net.apply(lambda s:(s>0).sum()),'ns':g.net.apply(lambda s:(s<0).sum()),'nbr':g.size()})
    out['top5b']=df[df.rb<5].groupby('code').net.sum(); out['top15b']=df[df.rb<15].groupby('code').net.sum()
    out['top5s']=df[df.rs<5].groupby('code').net.sum(); out['top15s']=df[df.rs<15].groupby('code').net.sum()
    out['top1b']=df[df.rb<1].groupby('code').net.sum()
    m=df.merge(geo,on=['code','b'],how='inner')
    m['fgn']=m.b.str.contains('|'.join(FOREIGN))
    m=m[~m.fgn]
    out['geo3']=m.groupby('code').net.sum(); out['geo1']=m[m.dist<=1].groupby('code').net.sum()
    out['geo3_buy']=m.groupby('code').bv.sum(); out['geo_n']=m.groupby('code').size()
    out=out.reset_index(); out['d']=d
    top=df[(df.rb<10)|(df.rs<10)][['code','b','net','avgp','bv','sv']]; top['d']=d
    out.to_parquet(fn); top.to_parquet(fn.replace('.parquet','_top.parquet'))
print('done',start,end,part)
