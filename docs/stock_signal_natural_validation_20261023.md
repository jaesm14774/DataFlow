# 台股訊號自然執行與判讀驗證 — 2026-10-23

## 時間與來源

- 唯讀快照：`stock_signal_inventory_snapshot_20261023_2030.json`，2026-10-23 20:30:35 Asia/Taipei。
- daily、after-close、intraday 最近自然批次各回報 23、32、10 個來源列；raw/accepted/quarantine/duplicate 分別為 13,835/13,830/5/0、85,266/84,208/1,058/0、2,177/2,174/3/0。來源 registry/runtime contract drift 均為 0。全部預期來源任務回報成功，但資料仍需品質審核。
- monthly 最新可歸屬記錄仍為 `scheduled__2026-08-24T20:00:00+00:00`，狀態 unknown；有 9 筆舊 source-run 缺 dag lineage，不以 run ID 猜測歸屬。

## 訊號日期、覆蓋與實際判讀

- 10/23 的 daily child 讀到的官方行情有效日是 10/22，故它是 10/22 收盤訊號，不是 10/23 收盤訊號。日線 `ready/partial`：主檔 1,952、有效收盤 1,915、flow 1,826、營收 1,948、財務 1,905、借券 1,798、內部人 1,939、股利 1,948；22 個候選，`notified_count=0`。
- 觸發方向家族：量能 56、法人 777、上市券商分點 77、價格 171（家族可重疊，不可相加）。1,929 檔進入候選評估，1,907 檔因不足三個方向家族未達條件；這是策略條件計數，不是來源缺失計數。
- TWSE quote 1,053/1,053 accepted，TPEx quote 876/892 accepted、16 quarantined、8 檔明確 no-trade；TPEx 法人流 909 accepted，其中 788 對碼、121 unmatched，未對碼不補零。TWSE margin HTTP 200 但 1,042/1,042 quarantined，沒有有效日期；不得解讀成零融資融券活動。TAIFEX 大額交易人此次 HTTP 200、1,386/1,386 accepted、effective 10/22，JSON decode 失敗未在此次自然 run 重現。
- TWSE/TPEx 季獲利率 feeds 分別 1,051/1,056、885/885 accepted，均只有 effective 2026-06-30；TWSE 31 筆未匹配主檔。不能由目前單一 2026Q2 期間補造同比。
- 盤中事件掃描選入 2,293 列，含月營收 1,981、公司事件 293、內部人轉讓 5、財測差異 14。排除包含月營收缺少預期基準 1,981、非 key 重大度公司事件 276、舊財測版本 7、重複財測語意 2。27 個候選指紋全已存在 sent ledger，其中 22 筆有 HTTP 2xx/receipt、5 筆沒有已確認 receipt；新候選 0、cursor 延後 0。故 0 表示此 cutoff 無新的可送事件，並非沒有收集到資料；對 5 筆傳輸未知事件不重送。
- 最新成熟 outcome 為 10/21 signal-close H1：TPEx 5 rows／5 directional hits／平均超額 +1.4585pp，TWSE 10／6／+1.9097pp。這只有一個獨立訊號日且未扣交易成本，不能據此調參。`market_signal_followups` 和 `market_signal_horizon_reviews` 目前無新列；1w/1m/3m/6m/1y review lifecycle 尚未證明自然落地。
- 新聞自然批次：15 feeds、1,129 URLs、1,125 accepted、4 fetch/parse reject、時間隔離 0、內容/重複拒絕 0、reject reconciliation 成功。記錄時間分類為 925 exact timestamps within 48h、62 date-only 晚於 cutoff 日期但沒有精確時刻、3 筆 date-only cutoff 日期時間未知、135 筆較舊、future 0。date-only 不算精確 48 小時。

## 具體改善假說與根因

### 每日 digest 超過 Discord 單則 payload 上限

自然 daily signal capture 成功並產生 22 個候選，但摘要 embed 連同背景欄位達 10,700 bytes，大於 10,000 bytes request budget。通知 task 因確定性的 formatter 預算錯誤失敗；`market_signal_runs.notified_count=0`，候選 digest 沒有送出。另有失敗通知 HTTP 200/receipt，該回執只證明失敗通知送達，不證明候選 digest 已送達。

修正將四個非方向市場背景欄位移至獨立 embed，並以每個 request 的 embed 限制切批，維持原本 8 張候選卡展示上限。對同一 10/22 cutoff 唯讀重播 formatter：8 張候選卡全保留，輸出 10 批、最大 request 9,244 bytes，全部低於 10,000 bytes；未送出 Discord。BDD contract test 先在舊 formatter 上失敗，再於修正後通過。

### Follow-up / horizon review SQL 與已部署資料庫欄位不一致

自然 capture log 出現 `Unknown column 'max_high_excursion_pct' in 'field list'`；後續 lifecycle 例外被診斷層捕捉，所以 Airflow capture task 顯示 success，但 `market_signal_followups` 和 `market_signal_horizon_reviews` 沒有新列。只讀 `SHOW COLUMNS` 確認 DB 欄位為 `max_favorable_excursion_pct`、`max_adverse_excursion_pct`。修正程式、schema contract 和 summary reader 使用現有欄位名，沒有加 migration。部署 DB read-only column probe、4 個 lifecycle tests、py_compile 及 Airflow DagBag/import-error check 通過。下一個自然 daily run 尚未發生，故不可稱 lifecycle 已自然恢復。

## 結論與驗證界線

- TAIFEX 先前 JSON decode 失敗在 10/23 的自然 HTTP 200/1,386-row ingest 中未重現；本次證據不足以再宣告 parser regression。保留 payload/格式差異的監控，不以單次成功推論長期穩定。
- 已修的通知與 follow-up 問題都有可重現的程式/契約證據，不是單純偶發傳輸失敗。修正屬同一部署資料庫契約上的程式調整，不需 schema migration。
- 相同 cutoff formatter replay、BDD test、4 個 lifecycle tests、Python compile 和 DagBag import check 通過。這些是靜態/離線驗證；尚無 post-fix 自然 digest 送達或 horizon rows 落地證據。
- 未手動觸發/重跑 DAG，未發候選 Discord 訊息，未交易。
