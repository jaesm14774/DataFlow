# 台股訊號自然收集驗證 — 2026-10-22

觀察時間：台北時間 20:33。資料來自唯讀 inventory snapshot、source audit 與資料庫查詢。未手動觸發／重跑 DAG、未手動發 Discord、未交易、未寫入業務資料。

## 自然來源執行

| DAG | 最新排程 run | 回報／預期 | raw / accepted / quarantine / duplicate | 結果 |
|---|---|---:|---:|---|
| daily source collection | scheduled__2026-10-20T20:00:00+00:00 | 23 / 23 | 13,834 / 13,829 / 5 / 0 | 全部 task 回報、contract drift 0；需資料審核 |
| after-close | scheduled__2026-10-21T12:00:00+00:00 | 32 / 32 | 85,543 / 84,485 / 1,058 / 0 | 全部 task 回報、contract drift 0；需資料審核 |
| intraday | scheduled__2026-10-22T12:15:00+00:00 | 10 / 10 | 2,107 / 2,102 / 5 / 0 | 全部 task 回報、contract drift 0；需資料審核 |
| monthly | scheduled__2026-08-24T20:00:00+00:00 | 0 / 2 可歸屬 | 無可歸屬列 | audit unknown；source row lineage 沒有 dag_id，不按重疊 run ID 猜測 |

快照於 20:33:02 台北產生：[唯讀快照](stock_signal_inventory_snapshot_20261022_2033.json)。三個每日排程的綠燈不代表有效日、對碼或資料完整。Monthly 最新 Airflow scheduled run 實際於 9/24 執行；下一次 scheduler earliest create 為 10/25 04:00 台北，仍等自然排程。

## 判讀假說：日線評估數是否混用了行情分母

日線 run 24e02cad74ebf99023a8bc05f742fb4304a53b4527236caa286b181cf4595634：as-of 10/22、主檔 1,950、price_count 有效收盤 1,915、候選 22。XCom 的 evaluated_stocks=1,929 看似比有效收盤多 14。

唯讀核對後，這不是分母錯置，而是兩個不同口徑：

| 市場 | 普通股主檔 | 有當日接受行情代碼列 | 有效收盤 | 無效收盤 | 明確 no-trade | 未進接受代碼覆蓋 |
|---|---:|---:|---:|---:|---:|---:|
| 上市 | 1,056 | 1,053 | 1,047 | 6 | 0 | 3 |
| 上櫃 | 894 | 876 | 868 | 0 | 8 | 18 |
| 合計 | 1,950 | 1,929 | 1,915 | 6 | 8 | 21 |

程式的候選評估數等於有當日接受行情代碼列數 1,929；price_count 只計有效收盤，因此差額 14 正好是 6 個無效收盤及 8 個 no-trade。未進接受代碼覆蓋的 21 檔不是零量推論；上櫃 18 檔還可能包含隔離列，不能都稱為來源無列。無效／no-trade 不會因此成為有效價格方向票，其他家族仍按各自資料判讀。保留現行計算，不因數字看似不等而改門檻；後續文件須持續把「當日代碼列」「有效收盤」「主檔分母」分開標示。

TWSE 行情 1,053/1,053 accepted、effective 10/22；TPEx 行情 892 raw、876 accepted、16 quarantined、effective 10/22，876/894 進入接受代碼覆蓋，868 有效收盤、8 no-trade。隔離列不與未匹配或 no-trade 合併。

## 日線訊號與通知

1,929 個有當日行情代碼列進入規則評估，1,907 個不足三個方向家族，22 個達既有條件。候選方向 11 偏多、9 偏空、2 分歧（上市 8/9/1，上櫃 3/0/1）。家族觸發：法人 777、價格 171、上市券商分點 77、量能 56，來源會重疊，不可相加。

法人：上市 1,039/1,056 可判讀、557 達方向條件、natural PIT 0；上櫃 787/894 可判讀、220 達方向條件、788 列有自然 PIT，81 檔有成交但沒同日來源列，仍為未知。上市分點 706 accepted panels、245 可判讀、77 達條件；TPEx 分點尚未收集。

每日摘要實際序列化 8 張個股卡，footer 標示另 14 檔；22 候選與 8+14 相符。通知 ledger 10 rows、10 HTTP 2xx、10 receipts。notified_count=1 是 daily digest flag，不是卡片數；receipt 不證明人工已讀。

## 盤中零新增候選：自然 XCom 診斷已落地

10/22 20:30:22 TPE 的 intraday child 對應自然 scheduled source run scheduled__2026-10-22T12:15:00+00:00。此 child 是事件增量掃描，universe_count=0 不代表全市場日線掃描；本次 candidate_count=0 表示沒有新候選，不能單獨解讀為沒有收集到來源。

本次 XCom 已自然寫入前次加入的 intraday_candidate_diagnostics 欄位，驗證了自然持久化。選入 2,255 source rows：

| 項目 | 列數 | 判讀 |
|---|---:|---|
| TWSE／TPEx 月營收 | 1,088 / 893 | 合計 1,981；目前缺預期基準，作為資料脈絡，不投方向票 |
| 公司公告 | 191 / 60 | 共 251：key 10、review 155、routine 86；241 個非 key 依政策排除 |
| TWSE 財測差異 | 14 | 7 個舊版本被新版取代、2 個重複語意；形成 5 個候選指紋 |
| 內部人轉讓 | planned 7、untransferred 2 | 形成 9 個候選指紋 |

最終 24 個 fingerprint 全部命中先前 sent ledger，因此本次沒有新待送候選。19 筆有 HTTP 2xx 和 receipt；5 筆財測差異 ledger 只有 sent、沒有 HTTP status 或 receipt，傳輸未知。沒有重送，以免產生重複通知；ledger 狀態不等於可確認送達或已讀。

## 來源 freshness、季財報與新聞

- TAIFEX 大額交易人 HTTP 200、1,386/1,386 accepted，effective date 10/21；比 10/22 行情落後一個交易日。先前 JSON 解碼錯誤未重現，成功回應 Content-Type 未持久化，故不能證明走過哪個 MIME 分支。
- TWSE margin 1,042 raw 全隔離、0 accepted、無 effective date；不合成本機日期、不推成零餘額。TPEx margin 801 accepted、effective 10/21。TWSE 注意交易 HTTP 200 但 0 rows；兩市場董監持股不足額來源最新有效日仍為 8/19。
- 八個季損益／EPS feeds 本批合計 1,978 accepted，全部 effective 2026-06-30。最新存量 1,979 列中基本 EPS 均非空，但比自然 source-run accepted 多 1 列（TPEx CI 885 vs stored 886），額外列未能歸因到本次 run。TWSE CI 配對 1,020/1,056（96.59%），TPEx CI 885/894（98.99%）。只有 2026Q2，沒有可比季度，不推造同比或方向票。
- 新聞自然 run 15 feeds、1,134 URLs、1,128 parsed、1,127 valid-time/accepted、6 fetch/parse rejects、1 invalid-time quarantine、0 content/duplicate reject；post-time rejection reconciliation 成功。拒絕來源：iThome 1、聯合報 1、鉅亨網 4；三立 1 列時間無效隔離。Freshness cutoff 10/20 18:43:21 TPE：930 個精確時間在 48 小時內、57 個 date-only 僅知晚於 cutoff 日期、7 個 cutoff 日期無時刻、133 個依記錄精度較舊、future 0。這是 publisher/parser recorded time，不是逐篇核原頁或 revision vintage；date-only 不算精確 48 小時。

## 成熟 outcomes 與決策

10/21 signal-close H1 新成熟：TWSE 10 rows、9 directional、6 hits、方向調整平均超額 +2.5013pp；TPEx 5 rows、5 directional、5 hits、+3.1727pp。這是單一 signal date，沒有扣交易成本／公司行動，不能據此調權重或門檻。10/22 當日訊號的 H1 尚未成熟；H5/H10 也未成熟。

本次完成一項判讀合理性核對：評估代碼數與有效收盤數的差額由無效/no-trade 狀態完整解釋，沒有證據支持改動計算或門檻。既有盤中診斷 XCom 已由自然 run 寫出，無需再改程式。snapshot JSON 通過解析；Airflow list-import-errors 回 No data found。無手動 DAG、Discord 或交易操作。
