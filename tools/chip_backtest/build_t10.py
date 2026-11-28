"""Step 3b: merge top-10 broker rows (from extract.py shards) with volume -> T10m.parquet (input of run8a.py). Needs E_brk.parquet from run3.py."""
import pandas as pd, numpy as np, glob, os, bt
fs=[f for f in glob.glob(f'{bt.S}/brk_shards/*_top.parquet') if os.path.basename(f)[:4] in('2021','2022','2023','2024')]
T=pd.concat([pd.read_parquet(f) for f in fs],ignore_index=True); T['d']=pd.to_datetime(T.d)
E=pd.read_parquet(f'{bt.S}/E_brk.parquet',columns=['code','d','vol','c'])
T=T.merge(E,on=['code','d'],how='inner'); T['sh']=T.net/T.vol
T[['code','d','b','net','avgp','vol','c','sh']].to_parquet('T10m.parquet')
