# Airflow 啟用中的 DAG 效能檢視

## 檢視範圍

2026-11-01 從 Airflow 2.10.5 metadata DB 查詢未暫停的 DAG，共 24 個 DAG、89 個 task。paused DAG 未納入程式檢視。執行時間取最近 14 天的 task instance；資料庫索引以目前設定的 MySQL `INFORMATION_SCHEMA` 與 `EXPLAIN` 確認。

| DAG | task 數 | 實作位置 |
|---|---:|---|
| `another_stock_calculate_everyday_process` | 4 | `dags/another_stock_collect_everyday.py`, `dags/stock/lottery_notify.py`, `dags/stock/treasury_stock.py` |
| `game_knowledge_collect` | 3 | `dags/game_knowledge_collect.py`, `dags/game/` |
| `game_new_releases_collect` | 3 | `dags/game_new_releases_collect.py`, `dags/game/new_releases.py` |
| `game_pre_registration_collect` | 5 | `dags/game_pre_registration_collect.py`, `dags/game/pre_registration.py` |
| `market_signal_notify` | 2 | `dags/market_signal_notify.py`, `dags/source_extension/market_signals.py` |
| `money101_collect` | 3 | `dags/money101.py`, `dags/credit_cards/money101_crawler.py` |
| `news_ch_collect_process` | 3 | `dags/main_news_ch_collect.py`, `dags/news_ch/` |
| `news_ch_notify` | 2 | `dags/news_ch_notify.py`, `dags/news_ch/all_news.py` |
| `portfolio_data_notify` | 2 | `dags/portfolio_data_notify.py` |
| `ptt_collect_process` | 1 | `dags/ptt_collect.py`, `dags/ptt/collect_process.py` |
| `save_money_ch_collect_process` | 3 | `dags/save_money_ch.py`, `dags/news_ch/` |
| `source_extension_collect_after_close` | 6 | `dags/source_extension/dag.py`, `dags/source_extension/adapters.py` |
| `source_extension_collect_intraday` | 6 | 同上 |
| `source_extension_collect_monthly` | 4 | 同上 |
| `source_extension_collect_process` | 5 | 同上 |
| `source_extension_collect_weekend` | 5 | 同上 |
| `stock_calculate_everyday_process` | 5 | `dags/basic_stock_calculate_everyday.py`, `dags/stock/calculate_logic.py` |
| `stock_calculate_weekday_process` | 3 | `dags/basic_stock_calculate_weekday.py`, `dags/stock/calculate_logic.py` |
| `stock_collect_everyday_process` | 13 | `dags/basic_stock_collect_everyday.py`, `dags/stock/collect_logic.py` |
| `stock_collect_weekday_process` | 7 | `dags/basic_stock_collect_weekday.py`, `dags/stock/collect_logic.py` |
| `stock_international_stock_process` | 1 | `dags/international_stock.py` |
| `stock_major_holder_monday_refresh_process` | 1 | `dags/stock_major_holder_refresh.py` |
| `taiwan_holiday_collect_process` | 1 | `dags/collect_holiday_info.py` |
| `youtube_collect_process` | 1 | `dags/youtube_collect.py`, `dags/youtube/collect_process.py` |

## 已調整

- 上市／上櫃日成交抓取重用 HTTP session，並把每檔股票各自寫 staging 表改為收集完後批次寫入。資料仍依相同日期與證券代號刪除後重寫；只把主檔讀取欄位縮至實際使用的欄位。
- 當沖 CSV 改在記憶體解析，不再覆寫 `dags/stock/temp/day_trader.csv`。
- 融資融券收集重用 HTTP session，且只讀取證券代號。
- 證交所券商進出每檔股票仍維持獨立 cookie session；改用 context manager，讓錯誤與早退路徑也關閉 session，並為 CAPTCHA 圖片請求補上 60 秒 timeout。
- 每日股票流程的 20:00 台北時間等待改成 Airflow `TimeSensor(mode="reschedule")`，保留 10 分鐘檢查間隔，等待期間釋放 Celery worker。
- YouTube API 共用 session 並為所有請求設定 20 秒 timeout；channels API 回傳錯誤或缺少 `pageInfo.totalResults` 時，改報明確錯誤，不再落成不具體的 `KeyError`。
- 修正 README 股票回補說明：正常值是空字串 `''`，不是 `None`；移除已暫停的 Google Trends 作業描述，補上目前啟用的 PTT、YouTube、Bahamut 知識收集與 Money101 工作。

## 執行時間與資料庫索引

最近 14 天最耗時的股票 task 是 `stock_day_trade_collect`（10 次，平均約 2 小時 18 分）、`stock_margin_trade_collect`（2 次，平均約 69 分）、`stock_twse_all_code_daily_deal_info_collect`（10 次，平均約 68 分），以及兩輪 TWSE 券商進出（平均約 67 分與 53 分）。目前抓取方式是逐檔呼叫公開來源並遵守既有等待節奏；本次只減少連線與 staging 寫入成本，沒有增加並行度或改排程。當沖資料依來源發布時間輪詢，是主要等待因素。

檢查到股票表已有符合 upsert/delete 條件的索引，沒有加重複索引：

- `個股成交資訊`、`當沖資訊`、`融資融券`：`(資料日期, 證券代號)` 唯一索引。
- `券商進出總表`：`(資料日期, 證券代號, 券商名稱)` 唯一索引。
- `全證券代碼相關資料表`：`證券代號` 主鍵。

`game_knowledge_board_candidates` 有 53,168 筆候選；其中 `game_directory` 有 882 筆已核准映射、300 筆待辨識，另有 51,986 筆 sitemap-only 候選。每 10 分鐘執行的自動映射查詢只需要前兩類，但原查詢計畫會從主鍵估掃 26,328 列。新增 `(source_key, discovery_method, review_status, mapping_source)` 索引到 schema 並套用至目前 `game` DB 後，`EXPLAIN` 改用新索引，估掃 1,183 列；這只縮小映射查詢範圍，不改變哪些看板會被核准或收集。

目前遊戲知識 DAG 是啟用狀態；正文仍只會從啟用來源、已啟用且已核准的遊戲看板收集。sitemap-only 候選仍待辨識，不會因新增索引自動開始抓取。

## 其他觀察

- 五個 `source_extension` DAG 共用相同 ingestion 實作；目前已使用共用 `source_records_write` pool 與 bounded HTTP retry，本次沒有重複調整。
- PTT 最近一次最終失敗發生在來源回傳 HTTP 500，程式已有 bounded GET retry 與 Airflow retry。Save Money 新聞來源曾讀取 timeout；這是來源故障，沒有以增加重試掩蓋來源問題。
- YouTube 最近一次失敗是 API 錯誤回應缺少 `pageInfo`；已修正錯誤診斷。遊戲知識收集有正文完整性檢查失敗與 HTTP 525，未將缺文標成成功。
- 官方 TWSE OpenAPI 列有全市場 `STOCK_DAY_ALL`，可作為逐檔抓取的後續評估方向；但切換前仍需確認指定資料日、發布時間、上市櫃覆蓋與欄位換算一致。此處不直接替換目前支援指定日期與回補的抓取方式。[TWSE OpenAPI](https://openapi.twse.com.tw/)

## 檢查結果

對本次檢視涵蓋的啟用 DAG 定義執行 DagBag import，涵蓋股票每日／平日、YouTube、五個共用實作的 source_extension DAG，以及遊戲知識 DAG，均為 0 import errors。11/01 檢視當日沒有觸發 DAG 或執行測試；來源實際連線、資料列相等性與正式排程成效仍需由後續自然 run 觀察。11/02 來源品質政策修正的局部測試結果記在下方。

## 2026-11-02 來源品質警示根因

核對頻道收到的 `scheduled__2026-10-30T14:45:00+00:00` run。這是
2026-10-30 22:45 台北時間的邏輯區間；`source_extension_collect_intraday`
排程為平日 07:00–22:45 每 15 分鐘，下一次排程邊界是 2026-11-02 07:00。
因此 run ID 的日期與實際啟動時間不同，是週末跨區間的 Airflow cron
語意，不是隨機時間或重試證據。

該次三項提示是兩個未轉讓來源各有 1 列正規化、0 接受、1 隔離，以及
TWSE 財測差異來源成功但回傳 0 列。隔離列 payload 的日期有效，公司代號、
名稱、身分及股數皆空；唯一非空欄位是固定的 `市場別`（上市／上櫃）。舊
正規化器將這個分類標籤當成事件內容，因此把「沒有申報」的日期占位列誤判
為壞資料。TWSE／TPEx 財測差異 API 是設有差異門檻的事件清單，成功零列可
表示當次沒有達標公司。原 10/30 14:45Z 批次的 response body 未留存；因
14:30Z 同來源曾有 7 列，無法回溯證明該次 0 列是正常空清單或來源短暫空回應，
但來源合約允許合法零事件，故不再單憑零列告警。

以 `crawler.source_runs` 查詢該 DAG 自 2026-10-29 起的成功批次：TPEx 與
TWSE 未轉讓來源分別有 127/127 批次帶隔離列；計畫轉讓來源為 TPEx 124/127、
TWSE 60/127；TWSE 財測差異來源 127 批次中有 3 批零列。這證明提示源於
可重現的來源列格式與過寬的通用品質規則，不是隨機告警。該次通知紀錄為
`source_warning_status=sent`、候選數 0；下一個 07:15 run 為 `unchanged`，
fingerprint 依問題類別去重，不會每 15 分鐘重複送同一狀態。它是來源完整性
提示，不代表有個股候選訊號。

修正後的第一個自然 run 為 `scheduled__2026-11-01T23:30:00+00:00`（11/02
07:45 台北時間）。TWSE、TPEx 未轉讓來源均回傳 0/0/0/0，TWSE 財測差異
來源也為 0/0/0/0；同輪計畫轉讓來源仍接受 TPEx 1 列、TWSE 2 列有效事件。
來源 DAG 與下游 `market_signal_notify` 均成功。
下游 XCom 顯示 `source_warning_status=cleared`、`candidates=0`、`sent=0`，
沒有新的候選通知或來源警示收據。這是修正後的正式自然 run 證據。

修正後，正常化器只會略過有有效日期、無公司代號且除了日期／市場標籤外
沒有內容的列；任何其他非空欄位仍走原品質檢查。登錄設定只對 insider
disclosure 與 TWSE／TPEx 財測門檻事件宣告空清單合法；月營收等需有覆蓋的
來源若成功回傳 0 列仍要求人工確認。source audit 與訊號警示共用這份來源
空清單政策。19 個局部測試通過，涵蓋占位列、非空缺漏列、合法／非法零列與
列數核帳邊界；`git diff --check` 通過。改動後仍須由自然排程確認正式來源
回應。
