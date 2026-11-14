# 2026-10-28 自然批次與訊號品質驗證

## 驗收情境與處理

**User** Given 當日沒有達標個股候選，但 `market_signal_outcomes` 同日仍含成熟列；When 唯讀快照彙整這些 outcome；Then 使用者應能區分正式候選與未達候選門檻的 `stock_evidence`，避免把次門檻樣本誤讀為已送出訊號的績效。

在相同 2026-10-27 signal-date／signal-close／H1／TPEx 價格來源分組上，更新前後列數相同：settled 857、directional hit 59、平均超額報酬 -2.596541 pp。唯讀快照 SQL 現在連接候選列並輸出 `entity_type` 和 `evaluation_cohort`；重跑後該組明確為 `stock_evidence / subthreshold_evidence`。`market_signals.py` 對每日全市場評估保存 evidence snapshot，且 outcomes 結算查詢包含這些 evidence；10/27 掃描的 1,937 檔均未達三個方向家族，故這 857 列不能被讀作 857 個已選候選或通知績效。變更只改善盤點分類，未改訊號規則、評分或 outcomes。

## 自然收集與品質證據

唯讀快照時間為 2026-10-28 20:33:56 台北：[20:33 快照](stock_signal_inventory_snapshot_20261028_2033.json)。Airflow 最新 daily、after-close、intraday、monthly 自然 source runs 分別回報 23/23、32/32、10/10、2/2，registry/runtime contract drift 均 0；各批仍標 `data_review_required`。raw／accepted／quarantine／duplicate 分別是 13,846/13,841/5/0、85,499/84,443/1,056/0、2,219/2,190/29/0、45,074/43,971/0/1,103。完整回報不是當日完整度證明。

### TAIFEX 解碼失敗與來源有效日

10/28 after-close 自然批次中，TAIFEX 大額交易人 HTTP 200、1,386/1,386 accepted；期貨與選擇權法人資料各 6/6 accepted；TPEx OAS 11627 當沖市場彙總 16/16 accepted。四者皆為零隔離／重複、無品質旗標，最新有效日均前進到 10/27，距收集日 1 日。這次自然執行未重現 JSON 解碼失敗，證明該日成功解析與入庫，不足以判定先前空／錯格式回應的根因已永久消失。OAS 僅為市場彙總。對 10/28 掃描而言，上述 effective date 仍不是同日，TAIFEX 與 OAS 不能因此被當成當日個股方向票；5 日 audit budget 只作資料品質警示，signal reader 仍 fail closed。

### 10/28 日線候選判讀

日線 run `bce74231fc671e931278443b56a9b0426627442668628933b2f6a6a00058c734` 為 `ready/partial`，主檔上市 1,057、上櫃 896，candidate count 0。1,937 檔進入候選評估，全部記為 `below_three_directional_families`。有方向觸發的只有 `institutions=482`（上市 369、上櫃 113）與 `broker_branches=73`（上市券商）；price、volume、profitability 均未產生方向觸發。TPEx 券商分點狀態仍是 `source_unavailable_skipped`，不能解讀成零。現有結果支持維持三家族規則；不能因零候選下修門檻。

行情覆蓋須分開看：上市 quote code rows 1,056、有效收盤 1,054、無效收盤 2；上櫃 quote rows 881、有效收盤 872、已標示 no-trade 9。相對主檔有 1 檔上市、15 檔上櫃沒有 quote code row；這 16 檔缺列與 9 檔 no-trade、2 檔無效收盤不是同一狀態。法人：上市 1,048/1,057 同日可判讀、達方向門檻 369、natural PIT same-day 0，保存時間是 collection time；上櫃 785 筆同日列、782 可判讀、3 排除、113 達門檻，natural PIT 785。有效資料未達方向門檻不是資料缺失。

10/28 日線摘要沒有候選卡；自然摘要 1 個，notification ledger 2 列、HTTP 2xx 2 列、receipt 2 列。回執證明平台接受摘要，不證明人已讀。Intraday 自然掃描選入 232 筆來源事件，materiality 為 routine 67、review 138、key 8；排除 205 非 key 事件、7 舊財測版本、2 重複語意。18 個 fingerprint 均已有 sent ledger，13 有 2xx/receipt、5 個回執未確認，沒有新候選且未重送。

### 新聞、財報與月頻邊界

最新新聞自然批次有 15 個 source、1,129 URLs、1,125 accepted、4 fetch/parse rejects、0 invalid-time quarantine。依來源所記精度：937 筆精確時間在 48 小時內、59 筆 date-only 僅晚於 cutoff 日期、2 筆只有 cutoff 日期而時刻未知、127 筆較舊、future 0。中央社 1 reject、鉅亨網 3 rejects；報導者沒有 URLs。這是收集 feed 中的時間精度，不等同逐篇核對原始頁或獨立佐證。

TWSE margin 1,042 列全隔離、無 effective date；不得視為零融資融券。TWSE／TPEx 季損益與 EPS feeds 目前有效日均為 2026-06-30（2026Q2），沒有可比期就不推同比。最新月頻 scheduled run 為 `scheduled__2026-09-24T20:00:00+00:00`，於 10/24 執行，兩來源均有本次 run lineage、effective date 2026-09-01、PIT version coverage 100%；合計 45,074 rows、43,971 accepted、1,103 duplicates。這只驗證最新 scheduled run，不外推所有舊列 lineage。

## 驗證界線

本次重跑的是有執行時間的新唯讀 inventory snapshot，沒有手動觸發／重跑 DAG、沒有發通知或交易。Worker 上的資料庫查詢成功，SQL `py_compile` 成功；同一 outcome 分組的列數／結果值在更新前後相同，新增 cohort 分類使次門檻證據樣本不再混同於正式候選。沒有證據支持改三家族門檻或訊號權重。

