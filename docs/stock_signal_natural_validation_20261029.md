# 2026-10-29 自然批次與訊號品質驗證

## 根因假說與先行驗收情境

假說：source collection DAG 的 4-task concurrency 讓 mapped adapters 同時對共用 `source_records` 索引批次 upsert；固定 2 分鐘 retry 又讓首輪 deadlock 的 mapped tasks 接近同時重試。這比上游 API／JSON 格式問題更符合已觀察錯誤。

**User** Given 多個全市場來源任務並行寫入共用資料表，When 同一自然 batch 發生資料庫 deadlock 後重試，Then collection DAG 將同時執行的 source tasks 限制在 2 個以降低鎖競爭，並保留既有 retry 與來源契約。

第一次修正只把 `max_active_tasks` 降到每個 DAG 2，實際檢查後發現它無法約束不同 collection DAG 彼此重疊。追加驗收情境：**User** Given daily、after-close、intraday、monthly 或 weekend DAG 可能同時寫入共用 source table，When 其 mapped source ingestion tasks 重疊執行，Then 所有這些 tasks 必須共用同一個最多 2 slots 的 Airflow pool，且 pool 由專案啟動設定建立，毋須人工逐次補設。

## 自然證據、根因與修正

唯讀 inventory snapshot 擷取於 2026-10-29 20:35:53 台北：[20:35 快照](stock_signal_inventory_snapshot_20261029_2035.json)。latest daily／after-close／intraday／monthly source audits 的 source key 回報數為 23/23、32/32、10/10、2/2；raw／accepted／quarantine／duplicate 分別為 13,847/13,842/5/0、85,440/84,385/1,055/0、2,225/2,220/5/0、45,074/43,971/0/1,103，contract drift 均 0。每日 source process root DAG state 是 failed，其餘三批成功但保留 `data_review_required`，所以不能以 23/23 row reports 宣稱完整成功。

Airflow log 定位到 `source_extension_collect_process / scheduled__2026-10-27T20:00:00+00:00 / ingest_approved_source / map_index=9`，對應 `twse_balance_sheet_ins`。第 1 次與第 2 次在 `registry.py` 的 `source_records` bulk upsert 都拋出 MySQL 1213 `Deadlock found when trying to get lock`；沒有 HTTP status，source-run row_count 為 0。日批 10/25、10/26、10/27 的 mapped source logs 都有同類首輪 deadlock；after-close 10/23、10/26 也有。這是共用資料表的寫入競爭，不是 TWSE API 空資料或 JSON parser error；一次自然成功不能排除它。

根因假說通過檢查：source collection DAG 原本允許 4 個 DAG tasks 同時執行，而 mapped adapters 共用 `source_records` 及其索引批次寫入。先把 `_build_collection_dag` 的 `max_active_tasks` 降至 2，但這只是每個 DAG 各自限流，不能限制多個 DAG 同時寫入；已追加跨 DAG pool 驗收情境並將它完成。現在每個 `ingest_approved_source` mapped task 都使用全域共用的 `source_records_write` pool，容量 2；`docker-compose.yaml` 的 `airflow-init` 也會建立／更新此 pool。保留既有一次 Airflow retry 與 2 分鐘 delay。pool 在 Airflow metadata DB 中生效，跨所有 collection DAG 共用。

驗收情境先寫入上節並先紅後綠：unittest 在尚未加 pool 時因 mapped tasks 仍屬 `default_pool` 而失敗，指派共用 pool 後通過。Airflow scheduler 的 DagBag 已實際載入五個 collection DAG：各 DAG `max_active_tasks=2` 且 `ingest_approved_source.pool=source_records_write`；metadata DB 中 pool 容量核為 2。`airflow dags list-import-errors` 為 `[]`，Docker Compose 設定檢查與 DAG Python compile 成功。修改前後以同一最新 10/29 signal run `ec89f2d2...` 和相同已落庫 source-run cutoff 作唯讀比較：日線仍 0 candidates／1,935 evaluated 均低於三家族，daily source process 仍是同一歷史 failed run；沒有回寫訊號資料。沒有手動觸發或重跑 DAG。下一次自然 source collection run 才能驗證 deadlock 率與總耗時是否改善，尚未宣稱修復已在線驗收。

## 訊號、來源覆蓋與通知判讀

10/29 after-close 自然 run `scheduled__2026-10-28T12:00:00+00:00` 32/32 keys 成功、資料品質仍待檢視。TAIFEX 大額交易人 1,386/1,386、期貨／選擇權法人各 6/6、TPEx OAS 17/17 均接受，最新有效日 10/28、age 1；TAIFEX 解析錯誤本次沒有重現。來源日期比 10/29 掃描日早一日，reader 不把它們當同日個股方向票。TWSE margin 1,042 全隔離且沒有 effective date；TPEx quote 892 rows 中 879 accepted、13 quarantined。

10/29 daily scan `ec89f2d2c8fe186cdb79f87637188d25d19f06600f94b7cbe565ce33edf3e753` 為 `ready/partial`，普通股主檔上市／上櫃 1,057／896。1,935 檔進入方向評估且全部 `below_three_directional_families`；candidate count 0。有方向觸發為 institution 576（上市 420、上櫃 156）與 listed broker 64。上櫃 broker 維持 unavailable；沒有 price／volume／profitability 方向觸發。故沒有證據支持降低三家族門檻。

行情：上市 1,056 quote code rows、1,051 有效收盤、5 invalid close；上櫃 879 quote rows、868 有效收盤、11 no-trade。相對主檔沒有 quote row 的是上市 1、上櫃 17 檔，與已標示 no-trade 及 invalid close 分開。法人：上市 1,043 可判讀、420 達方向條件、natural PIT 0（collection-time records）；上櫃 760 可判讀、156 達條件、natural PIT 760。有效資料未達方向門檻不屬缺列。

日線沒有個股卡；自然摘要通知 ledger 2 列、HTTP 200 2 列、receipt 2 列，不能推為已讀。Intraday 選入 446 個 source rows，routine/review/key 125/280/22；405 非 key events、7 舊財測版本與2 重複語意被排除。32 fingerprints 均已有 sent ledger，27 有 HTTP 2xx+receipt，5 receipt 未確認；新候選 0，未重送。

最新新聞自然 run 15 feeds／1,119 URLs／1,116 accepted／2 fetch-parse rejects／1 invalid-time quarantine。記錄精度為 930 精確 timestamp 在48h內、57 date-only 晚於 cutoff 日期、4 cutoff 日期時刻未知、125 較舊、future 0；自由時報隔離 1 個無效時間，鉅亨網有2個 fetch/parse reject，報導者無 URLs。最新 monthly scheduled run 仍是 `scheduled__2026-09-24T20:00:00+00:00`，有效月 2026-09-01；八個季損益/EPS feeds 仍只有 2026Q2，不推同比。10/28 H1 outcomes 顯示為 `stock_evidence / subthreshold_evidence`，是單一 signal date，不能當已送候選績效或據以調參。

