# 2026-10-30 自然批次：來源寫入 pool 首輪驗證

## 結果

唯讀快照於 2026-10-30 20:30:33 台北擷取：[snapshot](stock_signal_inventory_snapshot_20261030_2030.json)。source registry 67 keys。修改後的 daily source process、after-close source ingest、intraday source ingest 都實際記錄在 `source_records_write` pool，且映射來源任務均成功、try 1，未見 MySQL 1213 重試：

| 批次 | Airflow run | Source tasks | raw / accepted / quarantine / duplicate | DAG 狀態 |
|---|---|---:|---:|---|
| daily process | `scheduled__2026-10-28T20:00:00+00:00` | 23/23 | 13,857 / 13,852 / 5 / 0 | success |
| after-close | `scheduled__2026-10-29T12:00:00+00:00` | 32/32 | 85,487 / 84,434 / 1,053 / 0 | 20:30 snapshot 時仍 running |
| intraday source | `scheduled__2026-10-30T12:15:00+00:00` | 10/10 | 2,249 / 2,246 / 3 / 0 | success |
| monthly | `scheduled__2026-09-24T20:00:00+00:00` | 2/2 | 45,074 / 43,971 / 0 / 1,103 | success，尚無新月批次 |

四批 source contract drift 都是 0。Airflow metadata 的 task instances 證實 daily 23、after-close 32、intraday 10 個 mapped ingest task 全部指定 `source_records_write`，各批最大 try number 為 1；pool 容量是 2。這是修正後的自然執行證據，不能單憑一次無 retry 宣稱死鎖永久消失。這幾批實際 source task 時段未互相重疊，因此跨 DAG 同時爭用的壓力情境仍未被自然排程覆蓋。

After-close 的 32 個來源映射已完成，但 parent DAG 在快照時間仍等待 `stock_collect_everyday_process` 的既有工作；因此目前只能確認來源收集部分，不把 parent 說成已完成。這是自然等待，不手動觸發或重跑 DAG。

## 新鮮度、覆蓋及訊號判讀

- After-close 的 TWSE quotes 為 1,057/1,057 accepted、effective 10/30；TPEx quotes 893 raw、882 accepted、11 quarantined、matched 882 OTC codes。TPEx institutional flow 917 accepted、796 codes matched to current OTC common-stock master、121 unmatched；這些未配對列不轉成零量。TAIFEX large-trader 1,386/1,386、期貨及選擇權法人各 6/6 accepted，effective 10/29。TPEx OAS 有 18 筆市場彙總，最晚 effective 10/29，不是個股流量。TWSE margin 1,042 raw 全 quarantine，無 accepted row／effective date；不把它當零融資或零融券。
- 最新 daily signal 仍是 10/29 scan，`ready/partial`、1,953 universe、1,935 evaluated，0 candidates；institutions 576、listed broker branches 64，1,935 檔均低於三個方向家族。TPEx broker family unavailable。相同 10/29 cutoff 前後仍是 0 candidates；沒有證據降低門檻。
- 10/30 intraday scan `ready/partial`、0 新候選：502 selected event rows 中 127 routine、336 review、23 key；463 非 key、7 筆舊財測版本、2 筆重複語意排除。30 fingerprints 均已在 sent ledger，25 有 HTTP 2xx 與 receipt，5 receipt 未確認；不重送。Receipt 不代表人已讀。
- 成熟 horizon review 最新截至 10/29：1w 及 1m 顯示 `outcome_data_incomplete`；1w 仍有 125 pending rows，選中候選的確認 receipt 為 0，政策決策維持 `no automatic policy changes`。10/28 的 H1 settled rows 屬 `stock_evidence / subthreshold_evidence`，不是已送出候選績效，不據此調權重。

## 新聞與限制

10/30 news natural run：15 feeds、1,127 URLs、1,122 accepted、5 fetch/parse rejects、0 invalid-time quarantine；時間精度分布為 935 exact timestamps within 48h、47 date-only after cutoff date、13 cutoff-date time unknown、127 older、0 future。Date-only 不當精確 48 小時。Monthly source run 尚為 9/24 logical run、effective month 2026-09；八個季損益/EPS feeds 目前仍只到 2026Q2，沒有補造同比。

本輪沒有改訊號門檻、權重或來源解讀。只讀 inventory script 沒有觸發 DAG、寫業務資料或送通知。Pool 的跨 DAG 限流已在自然 task instances 生效；死鎖率的持續改善與同時寫入情境，仍須由後續自然批次觀察。
