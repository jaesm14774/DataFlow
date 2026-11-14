# 台股訊號自然執行與匯入診斷 — 2026-10-26

## 唯讀證據

- 最終快照：[20:38 台北唯讀快照](stock_signal_inventory_snapshot_20261026_2038.json)，registry 67 keys。
- 最近 daily、after-close、intraday、monthly source runs 均為 `all_expected_tasks_succeeded_data_review_required`，runtime contract drift 皆為 0。raw/accepted/quarantine/duplicate 分別為 13,845/13,840/5/0、85,469/84,411/1,058/0、1,995/1,992/3/0、45,074/43,971/0/1,103。任務成功仍需逐來源品質審核。
- 修正 package import 後，scheduler 自然啟動 overdue 的 after-close run `scheduled__2026-10-23T12:00:00+00:00`（10/26 20:33–20:36 台北）與 intraday `scheduled__2026-10-26T12:15:00+00:00`；兩個 DAG run 均成功，沒有手動觸發。

## 根因與判讀

### Source-extension 套件語法錯誤

`dags/source_extension/__init__.py` 第三行是字面值 `\\n`，導致 package import `SyntaxError`，Airflow DagBag 對引用該 package 的 source-extension DAG 顯示 import errors。建立 BDD package contract 後先跑紅：使用者情境在 import 時以此 SyntaxError 失敗。移除多餘字面行後，同一情境通過，`airflow dags list-import-errors` 回 `No data found`；自然排程隨後啟動並完成積壓的 scheduled run。

### DB deadlock 首次嘗試自動恢復

本輪 5 個 mapped source 首次寫入遇 MySQL 1213 deadlock：after-close 的 `tpex_market_index_daily`、`twse_market_index_daily`、`tpex_trading_attention_daily`、`twse_margin_balance_daily`，以及 intraday 的 `tpex_monthly_revenue`。DAG 原有一次 retry，5 個第二次嘗試皆 exit 0；最終 32/32 after-close 與 10/10 intraday source rows 成功，沒有 duplicate。這次證據支持暫時 DB 寫入競爭，尚不足以證明來源 parser 或資料契約缺陷；保留現有重試，不手動重跑。後續若多次自然 run 同樣重試，需再評估並行寫入/交易邊界。

### 有效日期、來源覆蓋與訊號結果

- after-close TWSE／TPEx 行情與 TPEx 法人流有效日均為 10/22。TWSE quote 1,053/1,053 accepted；TPEx quote 892 raw、876 accepted、16 quarantined，另 8 檔為 no-trade。TPEx flow 909 accepted，其中 788 對上櫃普通股主檔、121 unmatched；unmatched 不視為零。TAIFEX 大額交易人 1,386/1,386 accepted、有效日 10/22。
- TWSE margin HTTP 200 但 1,042/1,042 quarantined，沒有有效日。TWSE／TPEx 月營收 feeds 的有效月份為 2026-09：1,088/1,088 與 893/893 accepted；TWSE 1,055 對碼、33 筆在普通股主檔外。季獲利率仍只有 2026Q2，沒有同比補造。
- Monthly scheduled run 的兩個月持股來源都有可歸屬 lineage、effective 2026-09-01、PIT version coverage 100%：TPEx 17,580 raw／17,152 accepted／428 duplicate；TWSE 27,494／26,819／675，另 31 列在普通股主檔外。
- after-close 觸發的 10/26 daily signal child 因 `market_closed` 跳過，沒有 daily candidate scan；不可將其當成 0 candidates。修正後 10/22 digest 沒有新的自然 delivery 證據，通知 byte-budget 修正仍待有交易日的自然 scan 驗證。
- Intraday signal scan 有 11 個選入事件列；2 個 routine company events 與 2 個重複財測語意排除。7 個指紋全在舊 sent ledger，2 筆有 HTTP 2xx／receipt、5 筆傳輸未知；新候選為 0，未自動重送。這是 deduplication/policy 結果，不是未收資料。
- 最新成熟 outcome 仍是 10/21 signal-close H1，只有一個獨立日期且未扣成本；`market_signal_followups`、`market_signal_horizon_reviews` 仍無新列。daily child 被市場行事曆 gate 跳過，故上次欄位修正尚無自然 lifecycle 驗收。

## News freshness

10/26 自然新聞 run：15 feeds、1,125 URLs、1,120 accepted、4 fetch/parse rejects、1 invalid-time quarantine；拒絕對帳成功。發布時間精度：832 筆有精確時刻且在 48 小時內、8 筆 date-only 晚於 cutoff 日期、1 筆 date-only cutoff 日期時刻未知、279 筆依記錄精度較舊、future 0。date-only 不當作精確 48 小時；報導者此次沒有 URLs，三立有 1 筆無效發布時間，鉅亨 4 筆 fetch/parse reject。

## 驗證與後續界線

- `tests/test_source_extension_package_contract.py`：修正前失敗、修正後 1 passed。`py_compile` 與 Airflow DagBag/import-error check 通過。
- 五個 DB deadlock 由排程既有 retry 自動恢復，沒有手動重跑。以後續自然執行觀察重試頻率；本輪不改並行度或來源契約。
- 尚待一個正常交易日自然 daily child 驗證通知 digest 修正與 horizon/follow-up rows 寫入。未手動發 Discord、交易或暴露憑證。
