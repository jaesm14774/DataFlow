# 既有法人、分點與行情判讀修正 — 2026-10-16

記錄時間：2026-10-16 07:31 Asia/Taipei。使用者本日明確要求既有資料參與訊號、修正模糊用詞，並讓每次排程持續改善判讀合理性。此指示取代先前「legacy 法人一律排除／法人方向永久等成效驗證」的政策；新增外部資料訂閱的權利確認仍分開處理。

## 已確認的根因與修正

1. `equity_order_flow.py` 原本完全不查 `stock.三大法人買賣`，上市固定回傳 `source_not_authorized`。現已讀取既有上市櫃表；上櫃優先用有版本的官方來源，缺官方列時可用既有表。保留 cutoff、普通股對碼、股數加總、缺值與重複檢查；已知官方隔離版本不以舊好值覆蓋。
2. 法人原本算出條件仍固定 `vote_enabled=False / direction=0`。現依既有 1／3／5 交易日條件實際判讀：淨額、相對成交量、持續性、外資投信一致性及短長視窗分歧；沒有為增加候選放寬門檻。規則可執行與預測成效未驗證是兩件事。
3. 原本 `flow_count` 只計已觸發多空的股票，卻當作資料覆蓋率。v2 改計同日可對齊行情的有效法人資料；已收、可判讀、達條件各自列示。中性或未達門檻不再冒充缺資料。
4. 日線只讀新行情來源，完全忽略既有 `個股成交資訊`，兩市場 21 日完整歷史皆被判為 0。已接回截止前已入庫的既有歷史；當日只用目前來源，不覆蓋任何已有或隔離的來源列。也修復新來源完全缺少 10/05 上市行情時，既有有效歷史被略過的情況。成交量由張轉股，保持市場日曆、零成交、重複與公司行動檢查。
5. 通知改為具體的訊號數、資料日、可用股票與缺項；刪除現在仍稱「法人未接通／方向未啟用」的文字。歷史 v1 快照明確標示舊政策。法人與分點同群、價格與量能同群，不重複加權；卡片修正先前把群票誤寫為家族票的描述。

## 相同截止時間的唯讀前後比較

標的自然日線：資料日 2026-10-15、cutoff 台北 20:59:28（UTC 12:59:28），run key `3dda2dd5ce2c9df44dd52b7188444cc1010cad9d2691ab5cdeca706e2e8cf58a`。修正後是重算預覽，沒有覆寫此自然 run。

| 項目 | 修正前 | 修正後 |
|---|---:|---:|
| 法人可判讀覆蓋 | 0 | 1,820 |
| 法人達多空條件 | 被全域關閉 | 834 |
| 價格條件觸發 | 未進合併候選 | 329 |
| 量能條件觸發 | 0（歷史被排除） | 77 |
| 上市完整 21 交易日行情 | 0 | 1,021 |
| 上櫃完整 21 交易日行情 | 0 | 760 |
| 合併候選 | 0 | 20（5 偏多、13 偏空、2 分歧） |

| 市場 | 同日已收／普通股主檔 | 同日行情對齊可判讀 | 達法人多空條件 | 五日完整 |
|---|---:|---:|---:|---:|
| 上市 | 1,044/1,054 | 1,043 | 578 | 1,027 |
| 上櫃 | 777/894 | 777 | 256 | 685 |

這 20 檔符合價格、量能、法人三項既有條件，實際屬於行情與交易籌碼兩個資料群；不能當三個獨立來源或上漲機率。財報、估值、營收、新聞及公司行動等既有資料仍在卡片提供支持與反證；缺比較期不補造同比，也不把營收成長直接當 EPS 成長。

## 精確缺項與限制

- TPEx 券商分點買賣仍未收集；只略過上櫃分點，法人、價量及其他資料照常判讀。
- TWSE 分點來源與歷史資料存在，最新實際保存日為 10/14、796 檔；10/15 同日列為 0。自然收集任務顯示 success 不能證明它有保存 10/15 資料，不以 10/14 冒充當日，也不稱整個來源未接通。
- 上市法人同日已收 1,044；1,043 檔可對齊有效行情，另 5 檔有成交卻缺法人列。上櫃同日 777 檔均可判讀，95 檔有成交但缺法人列；缺列不補零。
- 既有表只提供截止前的入庫時間，沒有不可變歷史修訂版本；自然官方版本證據獨立列示。使用目前普通股主檔不代表已消除歷史倖存者偏差。
- [單位比對](order_flow_units_20261016.json) 核 10/14 2330 與 3485：既有張數轉股後與官方股數差分別 40／431 股，反映儲存精度；歷史量能倍數含此精度限制，未宣稱逐股精確。

## 驗證及自然運行邊界

- 新法人回歸在舊程式上實際為 4 failed / 1 passed，修正後通過；舊行情回歸為 3 failed / 1 passed，修正後通過。合計針對性測試 **59 passed**，保留缺值、加總、重複、單位、future cutoff、隔離、交易日曆、無交易、公司行動與同源分組檢查。
- 最終 worker DagBag 兩個檔案 import errors 均空；四個實作檔案與本機 SHA 相同；AST 通過。未提交／push。
- 所有 20 候選卡均通過欄位與序列化限制；摘要預覽 8 批、最大 8,802 bytes（上限 10,000）。這是未發送的預覽，沒有人工 Discord 或人已讀證據。
- 原本本 chat heartbeat 已原位更新，仍 ACTIVE／台北平日 20:30。每次須評估判讀改善假說、有證據就實作與驗證；不能只盤點或永久以未驗證為由全域關閉。
- 下一個自然日線才驗收新規則的候選持久化與平台回執。禁止為此手動觸發／重跑 DAG、回寫舊候選、補造自然證據或改訂閱。

證據：[修正前](order_flow_recompute_20261016_before.json)、[最終完整重算與預覽](order_flow_recompute_20261016_verified.json)、[既有資料盤點](order_flow_repair_probe_20261016.json)、[與本輪修改前相比的 patch](order_flow_repair_20261016.patch)。規則版本 `equity_order_flow_v2`／`group_calendar_risk_order_flow_v4`，後續 outcomes 須分版本比較，不用本次候選數證明獲利。


## 2026-10-16 20:46 台北追補：TAIFEX 媒體類型閘門與自然批次狀態

唯讀快照：[20:33:37 source/run snapshot](stock_signal_inventory_snapshot_20261016_2034.json)。最新 daily source run `scheduled__2026-10-14T20:00:00+00:00` 於 10/16 04:00–04:02 台北完成，23/23、13,817 raw／13,812 accepted／5 quarantine／0 duplicate。Intraday run 10/16 20:30–20:31 完成，10/10、2,116／2,110／6／0。Monthly lineage 仍 unknown：最新 scheduled run 8/24 logical、9/25 實際執行，9 個 `source_runs` 缺 `dag_id`；下一個 scheduler earliest create time 是 10/25 04:00 台北。

### After-close source run 與 parser 根因

- After-close `scheduled__2026-10-15T12:00:00+00:00` 自 10/16 20:00 台北執行；snapshot 時仍 `running/partial`。32/32 預期及回報、無 runtime contract drift；31 success、1 failed，總計 83,840 raw／82,787 accepted／1,053 quarantine／0 duplicate。普通股主檔分母上市／上櫃 1,055／893。TWSE quotes 1,051 accepted；TPEx quotes 886 raw、874 accepted、12 quarantine；兩市場有效日均 10/16。TPEx institutional flow 890 accepted，768 codes 配對、122 unmatched；unmatched 不補零。
- TAIFEX natural `source_run_id=9361`／map index 26 try 2：HTTP 200，raw／accepted／quarantine／duplicate 都是 0，無 effective date，`ValueError`。安全診斷為 `application/octet-stream`、宣告長度 unknown、拒收前 0 bytes。錯誤記錄沒有保留 `Content-Disposition` 或 body，只能確認 MIME 沒通過舊 exact-filename gate；不能判定檔名標頭是缺少或不同值，也不能判定未讀取 body 是否為有效 CSV。前一個成功自然 run 8638 是 1,386 accepted、有效日 10/14；本次仍是來源失敗，未宣稱恢復。
- TWSE margin 1,041 raw、0 accepted、1,041 quarantine、無 effective date；source task success 不等於有可用 margin 資料，也不以採集日補成行情日。

根因是 `adapters.py::_fetch_source_csv_rows` 只有在 `Content-Disposition` filename 完全等於官方 CSV 檔名時，才允許 TAIFEX 的 `application/octet-stream`；這個 run 在讀本文前就被 MIME gate 擋下。修正只對 allowlisted `taifex_futures_large_trader_positions_daily` 開放 octet-stream，仍受 response byte/row 限制；讀 body 後依 JSON 形狀分派並要求全部官方 JSON keys，否則按 CSV 解析並要求完整 allowlisted header。其他來源仍不接受 octet-stream。官方 OAS：[TAIFEX Swagger](https://openapi.taifex.com.tw/swagger.json)。

以相同 synthetic CSV／JSON fixtures 實跑 red/green：舊 gate 對無 exact filename 的有效 TAIFEX CSV 在讀取前失敗；更新後 TAIFEX 和隔離測試 7 passed，錯誤 CSV 欄位仍拒收。`py_compile` 成功，worker DagBag `import_errors={}`，本機與 worker 的 adapter SHA-256 相同。自然失敗 body 沒有保存，不能對 10/16 做同 body 重播；只能等下一個自然 after-close 驗 accepted、effective date、欄位持久化及 freshness。TAIFEX 僅是市場背景，不投個股方向票；未改候選門檻、權重、資料列或通知。

### 訊號、等待與回執

- 最新日線仍是 10/15 修正前自然列，`flow_count=0`／candidate 0；不能當新規則的當日輸出。10/15 20:59:28 同 cutoff 的唯讀修正比較仍是法人可判讀 0→1,820、法人達既有條件 834、合併候選 0→20；這是先前完整重算，並非 10/16 自然驗收。
- 10/16 20:30 intraday 是事件增量模式，`universe=0`／candidate 0。該自然來源批次 TWSE company events 99 accepted（62 matched、2 unmatched），TPEx company events 31 accepted（28 matched）；月營收 1,079／893 accepted（有效月 9/01），財測差異 7／1 accepted；董監轉讓 planned/untransferred 共 5 raw 全 quarantined。0 event candidate 只表示沒有列入候選，不能表示無事件或整體資料不足；未觸發列的逐來源 rule-rejection 計數沒有保存，不能臆造未達哪個事件門檻。
- 20:44 查核時 after-close 的 `wait_existing_equity_order_flow_collection` 為 `up_for_reschedule`。它等候當日 `stock_three_main_investor_collect` 與上市分點第二次收集進 terminal；法人 task 已在 16:26–16:27 台北完成，`stock_broker_inout_twse_collect_second_time` 自 19:21 起仍 running。sensor 的程式界線是 21:30 台北放行，因此目前是有界等待，不是已證實無限卡住；未到 cutoff，保留現行等待條件。`trigger_market_signal_notify` 尚未執行，今日 after-close daily child、候選卡及回執尚不存在。
- 較早 14:15 台北 intraday scan 有 3 candidates，ledger 有 3 rows sent、HTTP 200 且各有平台 receipt id；這是事件掃描，不是日線方向卡，也無人已讀證據。20:30 intraday candidate 0 與 earlier receipt 分開。
- Outcomes 最新 settled date 到 10/12 H1：signal-close 16 rows／5 hits／mean excess +1.324pp；next-close 16 rows／6 hits／+0.371pp（TWSE quote source）。兩個 basis 是同一 signal date，不是 32 個獨立日期；10/12 其他 horizon 尚未成熟，單一 16-row 日期不調門檻／權重。

### 新聞 18:30 台北自然批次及實際落庫

`news_ch_collect_process / scheduled__2026-10-16T08:30:00+00:00` 18:30–18:40 台北三個 tasks 均 success。15 feeds 有 1,133 URLs、1,129 parsed／valid-time／accepted，4 fetch/parse rejects（鉅亨 145/149 accepted）；invalid-time、content/duplicate rejects、future time 均 0。Freshness observation 18:40:41、48 小時 cutoff 10/14 18:40:41：933 exact recorded-datetime rows、40 date-only 在 cutoff 日期之後、23 cutoff-date 時刻未知、133 按記錄精度較舊。iThome 60 篇全是 date-only；TheNewsLens 80/90、報導者 7/10、泛科學 8/10、環境資訊中心 38/48 按精度較舊。

`delete_and_insert` XCom 回執為 1,129 rows、`create_time_tpe=18:40:46`；news 表主鍵倒序 5,000 列有界視窗逐來源對帳，15/15 accepted 數與實際 persisted rows 相符，source 內 distinct article_id 也相符，合計 1,129。一次按 `create_time` 掃全表的唯讀查詢耗時過久，已取消本次自己發起的查詢；後續改用有界 PK 視窗，沒有留下活躍讀取，也未更改新聞資料。這驗證批次寫入與時間品質欄位，不代表人工核對原頁內容或盈利事實。
