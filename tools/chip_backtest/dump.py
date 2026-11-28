"""Step 1: export the small tables to parquet under $CHIP_DATA (default ./data). Read-only SELECTs."""
import time, pandas as pd, bt, db
from sqlalchemy import text
e = db.eng()
def dump(name, sql):
    t = time.time(); d = pd.read_sql(text(sql), e); d.to_parquet(f'{bt.S}/{name}.parquet'); print(name, d.shape, f'{time.time()-t:.0f}s')
dump('px', "select 資料日期 d,證券代號 code,開盤 o,最高 h,最低 l,收盤 c,成交量 v from 個股成交資訊 where 資料日期>='2010-01-01' and 資料日期<'2027-01-01'")
dump('main', "select 資料日期 d,證券代號 code,主力買賣超 m from 主力買賣超")
dump('holder', "select * from 大戶比例")
dump('inst', "select 資料日期 d,證券代號 code,外資買賣超股數 f,投信買賣超股數 t,自營商買賣超股數 s from 三大法人買賣 where 證券代號 regexp '^[0-9]{4}$'")
dump('margin', "select 資料日期 d,證券代號 code,當日融資變化 mc,當日融券變化 sc,融資剩餘額 mb,融券剩餘額 sb,券資比 sr from 融資融券")
dump('geo', "select 資料日期 d,證券代號 code,券商名稱 b,距離 dist,買量 bv,賣量 sv,推薦方式 how,買賣 side from 地緣券商推薦")
dump('geoinfo', "select 證券代號 code,公司名稱 name,券商名稱 b,距離 dist from 地緣券商資訊")
dump('co', "select 證券代號 code,公司名稱 name,公司地址 addr,實收資本額 cap from 公司資訊")
dump('branch_addr', "select 證券商代號 bid,證券商名稱 bname,開業日,地址 addr,geometry from 全券商分行地址")
