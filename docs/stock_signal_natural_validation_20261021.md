# 台股訊號自然收集驗證 — 2026-10-21

觀察時間：台北時間 20:49；資料只讀自目前 Airflow 與資料庫。沒有手動觸發／重跑 DAG、發 Discord 或交易。

## 自然來源 runs

| DAG | 最新自然 run | 回報 / 預期 | raw / accepted / quarantine / duplicate | 結果 |
|---|---|---:|---:|---|
| daily | `scheduled__2026-10-19T20:00:00+00:00` | 23 / 23 | 13,828 / 13,823 / 5 / 0 | tasks 成功、contract drift 0；資料需審核 |
| after-close | `scheduled__2026-10-20T12:00:00+00:00` | 32 / 32 | 85,550 / 84,496 / 1,054 / 0 | tasks 成功、contract drift 0；資料需審核 |
| intraday | `scheduled__2026-10-21T12:30:00+00:00` | 10 / 10 | 2,144 / 2,136 / 8 / 0 | tasks 成功、contract drift 0；資料需審核 |
| monthly | `scheduled__2026-08-24T20:00:00+00:00` | 0 / 2 可歸因 | 無可歸因列 | `unknown`；source_runs 沒有 `dag_id`，9 列 legacy 結果不按重疊 run ID 猜測來源 |

after-close 的 1,054 隔離列中，TWSE margin 1,042、TPEx equity quotes 12。另有 TWSE trading-attention HTTP 200 但 0 rows；兩市場董監持股不足額 feed 最新 effective date 仍為 2026-08-19。這些狀態解釋 `data_review_required`，不等同所有 32 個任務失敗。

### 重要來源日期與可用性

- TWSE／TPEx quotes 的本次行情日為 2026-10-21。TWSE 1,053 accepted、1,052 有效收盤、1 個無效收盤、3 個沒有行情列。TPEx 891 raw、879 accepted、12 quarantined；主檔 893 檔中 869 有效收盤、10 明確 no-trade、14 沒有行情列。隔離、no-trade、未列不合併為零量。
- TWSE margin 1,042 raw 全 quarantine、0 accepted、無 effective date。最新自然保存列的 `Date` 欄為空；維持缺日期 fail-closed，不合成採集日，不把空值解讀為零餘額。TPEx margin 801 accepted、effective 2026-10-20。
- TAIFEX 大額交易人 HTTP 200、1,386/1,386 accepted、effective 2026-10-20，較股價行情落後一個交易日。前次 JSON 解碼失敗本次未重現；來源不是 10/21 同日背景。成功回應 Content-Type 仍未保存在 source_runs，無法確認經過哪個 MIME 分支。
- 八個季損益／EPS feed 的可用季度仍只有 effective 2026-06-30。上市 profitability accepted 1,050、隔離 5、上市普通股主檔配對 1,019/1,056（96.5%）；上櫃 profitability 885/885 accepted、主檔配對 885/893（99.1%）。point-in-time 資料沒有可比期，Q2 單季同比或季獲利率不補造，也不放寬現行方向門檻。

## 日線訊號、覆蓋和通知

日線 run `ec19c2c3e73ad3a5602620f8bb371d252c32d9aace6e1dfa63b9193f15b3fb6c`：`ready/partial`、as-of 2026-10-21、universe 1,949、price 1,921、flow 1,824、revenue 1,948、financial 1,904、borrow 1,797、insider 1,939、dividend 1,945、candidate 15。

市場普通股主檔分母上市／上櫃為 1,056／893。法人可判讀 flow 1,043／781；達法人方向條件 538／219。上市法人 natural PIT 0、stored capture time 1,044；上櫃 TPEx 法人 same-day PIT 782。上市分點本次 513 same-day、510 accepted panels、167 可判讀、44 達條件，12 panels 被隔離（10 duplicate/invalid/mixed、2 unbalanced）；上櫃分點 source unavailable。`flow_count` 是可判讀覆蓋，不等於合格方向數，也不能把上櫃分點缺收說成法人訊號缺收。

候選詳細計數：價格／量能／法人／上市券商分點方向家族觸發 203／47／757／44；來源間會重疊，不可相加。1,932 個標的進入規則評估；1,917 因不足 3 個方向家族未達條件，15 個形成候選。候選方向由資料列聚合為上市 7 偏多、2 偏空、1 分歧；上櫃 3 偏多、2 偏空。沒有因一日結果降低門檻。

每日通知 payload 有 8 張 candidate card，摘要 footer 寫另 7 檔未逐張列出，與 15 候選相符；9 筆 notification ledger、9 HTTP 2xx、9 receipt、0 error。`notified_count=1` 是 daily digest flag，並非只通知 1 檔。沒有人工已讀證據。最新 intraday run 的 candidate 0、notified 0、ledger 0；這只代表該 run 沒有新訊號通知。

## 盤中 0 候選根因與改善

根因：`_load_intraday_candidates` 對非 key 公告、重複財測語意及已存在 `sent` ledger 的項目使用 `continue`，原 XCom 只留下最後 `candidate_count`，沒有列出各階段數量。因而 0 不能區分來源缺列、政策排除、重複資料或已處理事件。

以 10/21 20:30:24 台北原 cutoff、程式實際 7 日事件窗、1 日 revision lookback、10,000 ID overlap，唯讀重跑既有 candidate loader：選入 121 source rows（TWSE／TPEx 公告 53／59、TWSE 財測差異 7、TPEx 未完成轉讓 1、TWSE 預定轉讓 1）。112 則公司公告分類為 7 key、70 review、35 routine；105 非 key 公告排除。財測差異 7 列中 2 列是重複語意；其餘與 2 個轉讓事件共 14 個候選 fingerprints。

這 14 個 fingerprint 全命中 `sent` ledger，因此本 run 沒有新待送候選。7 個公司事件與 2 個內部人事件各有 HTTP 2xx 和 receipt；5 個財測差異 ledger 雖標 `sent`，但沒有 HTTP status 或 receipt，實際傳輸未知。未重送以免重複；ledger `sent` 不等於可證實回執，也不代表人工已讀。

窄幅改善只增加 `_load_intraday_candidates` 的 XCom 診斷計數及快照讀取欄位：SQL 選入列、公告 materiality、政策／品質排除、候選 fingerprint、已 sent 去重、去重列的 HTTP／receipt，及新候選數。沒有改方向規則、候選門檻、通知或游標行為。針對性 unittest 1/1 通過；兩檔 Python 檔 `py_compile` 通過；使用 10/21 原 cutoff 的 read-only DB replay 得到上述數量。20:45 自然 run 的 XCom 尚未包含新增診斷欄位，因此之後的自然 run 才能驗證該欄位已由 Airflow 任務寫出；不手動觸發 DAG。

## 新聞 freshness、來源 reject 與 outcomes

新聞 `news_ch_collect_process / scheduled__2026-10-21T08:30:00+00:00` 自然執行 18:30–18:40 台北，15 feeds、1,141 URLs、1,136 parsed／valid-time／accepted、5 fetch/parse rejects、0 invalid-time quarantine、0 content/duplicate reject。拒絕按來源：iThome 1、中央社 1、鉅亨網 3；post-time filter reconciliation 成功。freshness cutoff 10/19 18:40:43：938 個原記錄 timestamp 在精確 48h 內、51 個 date-only 僅知晚於 cutoff 日期、11 個 cutoff 日期無時刻、136 個按記錄精度較舊、future 0。date-only 不當精確 48h；這些是 parser 的發稿時間，不是逐篇原頁核對或 revision vintage。

10/20 signal-close H1 剛成熟：TWSE 14 rows／8 directional hits／平均超額 +1.396233pp；TPEx 3／3／+7.982774pp。H5/H10 尚未成熟；這只有一個獨立訊號日期，結果未扣交易成本／公司行動，僅記錄，不調分數或門檻。
