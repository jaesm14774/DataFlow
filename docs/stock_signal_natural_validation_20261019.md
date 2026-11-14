# 台股訊號自然批次核驗 — 2026-10-19

核驗截止：2026-10-19 20:40 Asia/Taipei。沒有手動觸發 DAG、重跑收集、發送通知或交易。工作樹原有修改與未追蹤檔案均保留。完整唯讀快照：

- [20:34 執行與通知對帳快照](stock_signal_inventory_snapshot_20261019_2034.json)
- [20:37 after-close 子流程快照](stock_signal_inventory_snapshot_20261019_2037.json)
- [20:43 日線完成與回執快照](stock_signal_inventory_snapshot_20261019_2043.json)

## 可自動化資料及來源品質

| 自然來源流程 | Run 與狀態 | raw / accepted / 隔離 / duplicate | 覆蓋判讀 |
|---|---|---:|---|
| Daily source | `scheduled__2026-10-17T20:00:00+00:00`；10/19 04:00–04:02 台北；23/23 success；contract drift 0 | 13,824 / 13,819 / 5 / 0 | 任務成功仍要按來源分別看有效日與代碼配對 |
| After-close source | `scheduled__2026-10-16T12:00:00+00:00`；10/19 20:00 開始；32/32 source task success；contract drift 0 | 85,516 / 84,463 / 1,053 / 0 | 上市／上櫃普通股主檔分母 1,055／893；DAG 父流程於約 20:36 完成，之後才觸發日線子 run |
| Intraday source | `scheduled__2026-10-19T12:00:00+00:00`；10/19 20:00–20:01 台北；10/10 success；contract drift 0 | 2,001 / 1,997 / 4 / 0 | 來源收集成功不代表有方向掃描母體 |
| Monthly source | 最新 scheduled logical run 仍是 8/24；9/25 實際執行；audit `unknown` | 不可歸因 | 9 筆 legacy `source_runs` 缺 `dag_id`；下一次最早排程 10/25 04:00 台北，不能以相似 logical date 補造歸因 |

### TAIFEX 大額交易人：post-fix 自然解析成功；成功回應 MIME 未留存

- 10/19 20:00:49–20:00:52 台北自然 source_run `10141`，HTTP 200，`row_count=1,386`、`accepted=1,386`、quarantine／duplicate 均 0，有效日 10/16。與同批 TWSE、TPEx 行情有效日 10/19 不同，因此這次證明 post-fix 的 TAIFEX 解析與入庫路徑成功，**不**證明資料已更新至當日。
- `source_record_fields` 有 13,860 筆 canonical 欄位、1,386 個 distinct records、10 個目標欄位；contract、交易人類別、月份、前五／前十大買賣未平倉量等均已落庫。這驗證了 parser、欄位正規化與持久化整條路徑。
- 結論：10/16 HTTP 200 `application/octet-stream` 在讀 body 前被 exact-filename MIME gate 擋下，是已確認的程式准入邏輯缺陷；新 allowlist + 嚴格 JSON keys／CSV header gate 有 red/green fixture 驗證。10/19 成功的 source_run 沒有保留成功回應的 Content-Type，故不能證明該次實際走到 octet-stream 分支；只能證明修改後的 adapter 可解析並持久化本次實際回應。10/07–09 的 JSON char-0 decode 失敗仍沒有保留 body，不能倒推全是同一根因，也不能排除 upstream 其他回應型態。
- TAIFEX 是市場期貨背景，不是個股方向票。市場訊號格式器會把資料日與掃描日不一致標出；保留 freshness 判讀，不以 10/16 資料冒充 10/19。

### TWSE 融資融券：成功下載但仍不可用

10/19 after-close 有 1,042 raw、0 accepted、1,042 quarantined，沒有 effective date。隔離記錄中彙計出 1,042 列 `Date` 為 null/空，與本次 run 的隔離列數一致；但 `source_records` 沒有 source_run_id，不能宣稱逐列有直接 run lineage。adapter 不再合成本機日期，因此符合 registry 必填日期條件而隔離。這是有資料但缺來源日期，不是零融資量。現有證據不支持由收集日或行情日代填日期，維持隔離，不改日期規則。

### 同批行情與法人來源

- TWSE quotes：1,052 accepted，effective 10/19；TPEx quotes：891 raw／880 accepted／11 quarantined，effective 10/19。TPEx accepted codes 880／893；未對上的代碼或隔離列不轉成零量。
- TPEx institutional flow：888 accepted、有效日 10/19；768 檔可配對至 OTC 普通股主檔，120 筆未配對保持 unknown，不作零買賣。TWSE 既有法人資料另由歷史儲存讀取，與 TPEx 自然 source_run 分開追蹤。
- TPEx margin 與 SBL 有效日仍是 10/16；TWSE／TPEx quotes 則是 10/19。對日期較舊的家族分別保留日期，不因來源 task 綠燈就當同日資料。

### 8 個季損益／EPS feeds

10/19 04:00 台北 natural daily run 中新增的 8 個 income-statement 批次來源全數 HTTP 200、成功且沒有本次 source-run quarantine／duplicate；目前有效期均只有 2026Q2（effective date 6/30）：

| Source keys | 本次 raw／accepted | 主檔配對與限制 |
|---|---:|---|
| TPEx `tpex_income_statement_bd`, `tpex_income_statement_ci` | 7／7、885／885 | CI 885／893；另一來源是不同報表子集，不能當額外市場覆蓋直接相加 |
| TWSE `twse_income_statement_basi`, `twse_income_statement_bd`, `twse_income_statement_ci`, `twse_income_statement_fh`, `twse_income_statement_ins`, `twse_income_statement_mim` | 10／10、3／3、1,050／1,050、13／13、5／5、4／4 | CI 1,019／1,055 配對，31 未配對；其他來源是不同報表子集 |

按目前已持久化、effective date 為 2026-06-30 的這 8 個 feeds 自動檢查基本 EPS 欄位，均無 null／missing；所有記錄目前也只有 6/30 一個季度，不能在缺少可比期時推同比。TPEx CI 的 source-run accepted 是 885，但 source_records 存量有 886 列；source_records 無 run-level row lineage，故保留這 1 列差異，不把存量列數冒充本次 run 計數。

## 日線訊號、法人覆蓋與通知對帳

10/16 自然 daily scan（`run_key=bbe205…`）是目前最新完成的日線掃描：`ready/partial`、universe 1,948、有效價格 1,911、法人可判讀 `flow_count=1,804`、候選 16。這證明 10/16 使用者要求接回既有法人資料的規則已進入自然日線流程：

- 上市主檔 1,055：1,036 direction-usable、551 檔達原有方向條件；另有 10 檔有成交但缺 flow。上市 flow source 是既有 `twse_t86_stored`，`natural_pit_same_day_count=0`、保有 collection time 1,037；列入資料判讀，但不能宣稱完整自然 PIT。
- 上櫃主檔 893：768 direction-usable、237 檔達原有方向條件；另有 97 檔有成交但缺 flow。768 檔 TPEx 法人列具 same-day natural PIT。未收集的 TPEx 券商分點仍只影響該家族，不能把這些缺列說成全部法人資料未接通。
- 上市分點是 `available_subset`，該掃描同日有效 panel 0、20 筆因 duplicate/invalid/mixed capture、5 筆因 unbalanced 排除，最新日期 10/14；TPEx 分點為 `source_unavailable_skipped`。因此不把券商分點 0 個合格 panel 說成法人方向 0，也不改門檻。
- 16 個候選與日摘要的 delivery 是不同層。程式確認 daily `notified_count=1` 是整批日摘要已送的旗標；通知 ledger 有 9 列 `sent`、9 個 HTTP 2xx、9 個 receipt id、0 error。這些資料不能證明使用者已讀。

10/19 20:30 intraday run 是 `ready/partial` 但 `universe=0, price=0, flow=0, candidate=0`，也沒有通知列。這表示本次 run 沒有有效掃描母體，不能解讀成全市場無方向訊號。20:40 snapshot 時 daily child 尚在執行，當時的暫存空計數未作結論；20:43 後續自然 run 完成結果見下節。當時最新成熟 H1 仍是 10/12（16 rows；signal-close 5 hits、+1.324pp；next-close 6 hits、+0.371pp），同一日期不同 entry basis 不是獨立樣本，未調分數或門檻。

### 20:43 自然日線完成

After-close parent `scheduled__2026-10-16T12:00:00+00:00` 於 20:36:57 台北成功結束；`market_signal_notify / market_signal__daily__source_extension_collect_after_close__scheduled__2026-10-16T12:00:00+00:00` 於 20:36:50–20:43:01 成功完成。10/19 daily run `ready/partial`：universe 1,948、price 1,917、flow 1,810、revenue 1,948、financial 1,904、borrow 1,796、insider 1,939、dividend 1,945、candidate 7。

| 市場 | 主檔 | 有效價格 | 可判讀法人流 | 達既有法人條件 | 有成交但缺 flow | same-day natural PIT |
|---|---:|---:|---:|---:|---:|---:|
| 上市 | 1,055 | 1,047 | 1,042 | 489 | 5 | 0；stored collection time rows 1,045 |
| 上櫃 | 893 | 870 | 768 | 208 | 102 | 768 |

上市券商分點為 `available_subset`，同日有效 panel 0，15 筆 duplicate/invalid/mixed capture、4 筆 unbalanced，最新有效日 10/14；上櫃券商分點 `source_unavailable_skipped`。flow_count 1,810 代表可判讀覆蓋，489／208 才是符合原有法人條件的檔數；缺同日列另列，不按零買賣處理。

通知對帳：7 candidate rows、run `notified_count=1`（daily digest 旗標）、7 ledger rows sent、7 HTTP 2xx、7 receipt ID、0 error；沒有人工已讀欄位。10/16 H1 `signal_close` 已成熟：TWSE 12 rows／4 directional hits／mean excess +1.1524pp；TPEx 4 rows／1 hit／+0.3002pp。這是單一 signal date、分市場 benchmark 的樣本；沒有據此更動門檻或權重。10/19 H1 尚待後續 session outcome。

## 新聞自然批次

10/19 18:30 台北 `news_ch_collect_process / scheduled__2026-10-19T08:30:00+00:00` 15 feeds；1,132 URLs、1,126 parsed／valid-time／accepted，6 fetch/parse reject（中央社 1、聯合報 3、鉅亨網 2），invalid-time／content/duplicate／future 均 0。`delete_and_insert` task success 並回報 1,126 inserted rows（18:46:30 TPE）。

Freshness observation 18:46:27、cutoff 10/17 18:46:27：874 筆有精確時間且落在 48 小時內；28 筆只有日期且晚於 cutoff 日期、1 筆日期落在 cutoff 日但時刻未知、223 筆依來源記錄精度較舊、future 0。date-only 不計作精確 48 小時；被收錄也不表示它是公司事件或獲利驗證。

## 本輪改善與驗證

判讀假說是「監控報表把 daily `notified_count` 當成已送訊息數，容易和候選卡／實際 receipt 混淆」。查到程式定義後確認它是 daily digest flag，故沒有改訊號、權重或門檻；窄幅修正只改唯讀 `stock_signal_inventory_snapshot.py`，加入最新 signal run 的 notification-ledger 分組欄位：候選數、ledger row、HTTP 2xx、receipt ID、notified timestamp、error row 各自呈現。

`python -m py_compile` 通過；worker 執行新的唯讀 snapshot 後，daily run 顯示 16 candidates、`notified_count=1`，delivery group 顯示 9 ledger rows／9 HTTP 2xx／9 receipts／0 errors，與逐列查詢一致。欄位不聲稱 human-read。沒有改生產 DAG、收集 adapter 或交易策略，因此不需要訊號前後門檻比較；可核對的前後差異是舊快照無 run-level delivery 分組，新快照可與候選 run 直接對帳。

研究來源台帳新增 TAIFEX 自然通過證據；因這是可重用的 parser/data freshness 分界，也新增一份小型 ad-hoc memory note。下一次自然回看需確認 TAIFEX 新有效日、10/19 H1/H5/H10 後續 outcome、monthly lineage 與來源新鮮度；不手動觸發或重跑。
