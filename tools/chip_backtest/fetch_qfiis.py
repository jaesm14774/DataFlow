"""Read-only research crawl: TWSE MI_QFIIS (外資及陸資投資持股統計) weekly snapshots 2018-2026.
Polite: 2s between requests, retries, resumable. Output: $CHIP_DATA/qfiis.parquet (gitignored).
Columns by position: 0 code,1 name,2 ISIN,3 發行股數,4 尚可投資股數,5 外資持有股數,6 尚可投資比率,7 外資持股比率,8 法令上限,9 陸資上限."""
import os, time, json, datetime as dt, requests, pandas as pd, bt
out = f'{bt.S}/qfiis.parquet'
done = pd.read_parquet(out) if os.path.exists(out) else pd.DataFrame()
have = set(done.d.astype(str).unique()) if len(done) else set()
rows = []
d = dt.date(2018, 1, 5); end = dt.date(2026, 11, 27)
s = requests.Session(); s.headers['User-Agent'] = 'DataFlow-research/1.0'
n = 0
while d <= end:
    for off in range(0, 4):  # Friday, else walk back to the last trading day of that week
        day = d - dt.timedelta(days=off)
        ds = day.strftime('%Y%m%d')
        if ds in have: break
        j = None
        for attempt in range(3):
            try:
                r = s.get('https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS', params={'date': ds, 'selectType': 'ALLBUT0999', 'response': 'json'}, timeout=40)
                j = r.json(); break
            except Exception as ex:
                time.sleep(5 * (attempt + 1))
        time.sleep(2)
        if j and j.get('stat') == 'OK' and j.get('data') and str(j.get('date')) == ds:
            for x in j['data']:
                rows.append((ds, str(x[0]).strip(), x[3], x[4], x[5], x[6], x[7]))
            n += 1; have.add(ds)
            if n % 20 == 0:
                print('fetched', n, ds, flush=True)
                pd.concat([done, pd.DataFrame(rows, columns=['d', 'code', 'issued', 'room', 'held', 'room_pct', 'held_pct'])]).to_parquet(out)
            break
    d += dt.timedelta(days=7)
res = pd.concat([done, pd.DataFrame(rows, columns=['d', 'code', 'issued', 'room', 'held', 'room_pct', 'held_pct'])])
res.to_parquet(out); print('DONE', len(res), res.d.nunique(), flush=True)
