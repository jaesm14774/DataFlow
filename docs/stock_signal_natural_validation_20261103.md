# 2026-11-03 台股自然訊號與 TAIFEX 選擇權修正驗收

唯讀快照於 2026-11-03 20:32:17（Asia/Taipei）擷取：[inventory snapshot](stock_signal_inventory_snapshot_20261103_2033.json)。本輪只檢查自然排程結果、資料列與既有通知回執；沒有手動觸發／重跑 DAG、發通知、交易或改資料庫。

## 自然來源批次

Source registry 共 67 keys。最新自然批次的 Airflow run metadata、source_runs 與 audit 如下；`data_review_required` 是資料品質/完整性檢查狀態，不代表來源任務失敗。

| DAG | logical run | 台北實際執行 | 預期/回報 | raw / accepted / quarantine / duplicate | 結果 |
|---|---|---|---:|---:|---|
| daily source | `scheduled__2026-11-01T20:00:00+00:00` | 11/02 20:00:00–20:00:45 | 23/23 | 8,207 / 8,207 / 0 / 0 | Airflow success；無缺 key |
| after-close source | `scheduled__2026-11-02T12:00:00+00:00` | 11/03 20:00:00–20:29:07 | 32/32 | 83,758 / 82,707 / 1,051 / 0 | Airflow success、32 個 source task 均 success；audit `data_review_required`；contract drift 0、無缺 key |
| intraday source | `scheduled__2026-11-03T12:15:00+00:00` | 11/03 20:30:00–20:30:34 | 10/10 | 2,140 / 2,140 / 0 / 0 | Airflow success；無缺 key |
| monthly source | `scheduled__2026-09-24T20:00:00+00:00` | 10/24 20:00:00–20:00:56 | 2/2 | 45,074 / 43,971 / 0 / 1,103 | Airflow success；資料有效月份 2026-09；next run create-after 為 11/24 20:00 TPE |

Daily source 的 source date 為 11/02；after-close 主要行情、法人及 TAIFEX rows 已涵蓋 11/03 或最新可發布日。Monthly latest run 的 43,971 accepted rows PIT version coverage 為 100%，本次 DAG/run source attribution 的 unattributed legacy rows 為 0；這不替更早的歷史月份背書。該批 1,103 列是重複列計數，沒有列入 accepted。

## TAIFEX 修正已由自然批次驗證

`taifex_index_options_positions_daily` 在自然 after-close map index 24、try 1 以 HTTP 200 成功；本次 DB registry 與 runtime 均為 `taifex_csv`，contract drift 0。6 raw 全數 accepted、quarantine 0、duplicate 0，有效日 2026-11-02，6/6 accepted records 有 source-feed PIT version，coverage 100%。再唯讀查核 `source_record_fields`：6/6 records 有欄位資料，出現 8 種 canonical target fields，共 48 field rows。這確認前次修正不只將 HTTP 200 的 CSV 解碼成功，也自然 bootstrap registry 並落入可判讀欄位。

這是市場彙總選擇權資料；上市/上櫃個股分母不適用，不加入個股方向分數。相對 11/03 日線日期晚一個有效交易日，是上游目前可用日期，不表示個股資料當日同時點或訊號新鮮度已對齊。根因為可重現的 JSON/CSV parser contract mismatch，這次自然成功已解除「等待自然持久化」缺口；後續仍監看有效日是否前進，不宣稱上游契約永不變動。

## 日線、涵蓋率與通知

11/03 日線 signal 是 after-close DAG 的自然 downstream child，20:29:02 啟動；parent root 在 20:29:07 成功結束，child 隨後完成通知回執。該 signal run 使用的 source DAG/run ID 正是本輪 after-close run。Universe 1,955（上市 1,058、上櫃 897），source status `partial`；資料列計數為 price 1,924、flow 1,841、revenue 1,948、financial 1,912、borrow 1,796、insider 1,943、dividend 1,952。這些覆蓋數與可達方向條件數分開讀取。

| 市場 | 有效價格 / 主檔 | 法人可判讀 / 主檔 | 法人達多空條件 | 上市分點面板 | 其他缺列語義 |
|---|---:|---:|---:|---:|---|
| 上市 | 1,051 / 1,058 | 1,046 / 1,058 | 510 | 750 accepted、307 可判讀、53 達集中度條件 | 6 個無效收盤、1 個缺行情列；1,046 筆上市法人列為 collection-time、natural PIT same-day 為 0 |
| 上櫃 | 873 / 897 | 795 / 897 | 211 | `source_unavailable_skipped` | 行情 11 個明確 no-trade、13 個缺行情列；78 個有成交但缺法人來源列，保留 unknown；122 法人代碼 unmatched 不當零 |

方向 family trigger counts：法人 721、上市分點 53、價格 487；同一證券可出現在多個 family。1,941 檔進入合併評估，1,929 檔低於至少 3 個方向 family，因此 12 檔達候選條件。candidate direction 分布為偏空 11、偏多 1、分歧 0；這是當日規則產物，不代表上漲機率或可外推的命中率。現有上市/上櫃法人和上市分點都實際進入判讀；唯一尚未收集的方向家族是 TPEx 分點。

日線 digest 有 12 個候選：3 個個股卡片 embed 有 HTTP 2xx receipt，摘要列出偏多 1、偏空 11 的分布及「另 9 檔未逐張列出」；摘要 embed 1。ledger 5 rows 全數有 HTTP 2xx 與 platform receipt。這些代表傳輸回執，不是人已讀；run 欄 `notified_count=1` 為每日通知批次標記，不能解讀為只有一檔候選送達。現行摘要亦分列上市/上櫃覆蓋、方向 family 數與資料狀態，故本輪沒有證據支持改通知或放寬三家族門檻。

Intraday 10/10 sources 自然接受。選入 166 個來源事件列，其中公司公告 materiality 為 routine 60、review 94、key 7；12 個 candidate fingerprints 均已存在 sent ledger 且各有 HTTP 2xx receipt，因此新卡 0。這是重複抑制後的 0 新卡，不是無來源訊號。不能把同一 11/03 的候選/outcome 用來調分數或門檻。

## 仍需明確保留的來源缺口

- `twse_margin_balance_daily`：1,042 raw、0 accepted、1,042 quarantine、無 effective date；不可視為融資融券為 0，也不填本機日期。
- `twse_trading_attention_daily`：HTTP/task 成功但 0 raw/accepted；audit 要求依來源語義核對，不推論當日無注意股。
- TWSE/TPEx director-shareholding-shortfall feeds 最新有效日均 2026-08-19（12/15 rows）；顯示舊資料/稀疏揭露，不代表當天新發生。
- TPEx `tpex_institutional_flow_daily`：917 accepted，795/897 主檔配對、122 unmatched。TPEx quotes HTTP 206 回應完整性通過本次收集，但 893 raw 中 884 accepted、9 quarantine；明確 no-trade、缺列和 quarantine 分開。
- TPEx OAS 11627 當沖資料本次僅 1 個市場彙總列、有效日 11/02；必須 freshness gate，不作個股當沖成交訊號。TWSE TWTB4U 是資格名單，仍與 TWTB5U 個股成交資料分開。
- Monthly latest run 雖有完整當次 source_runs/PIT attribution，資料期仍為 2026-09；重複列 1,103 不算新列。歷史 panel 全期間 lineage 不在本次快照的證明範圍內。

## 新聞與成熟 outcomes

新聞自然 run `scheduled__2026-11-03T08:30:00+00:00` 於台北 18:30:00–18:44:07 完成：15 feeds、1,120 URLs、1,115 accepted、5 fetch/parse rejects、0 invalid-time quarantine、0 duplicate/content rejects。freshness 記錄觀測時間 11/03 18:44:02、48 小時 cutoff 11/01 18:44:02；精確記錄時間在窗內 930 篇、日期精度且 cutoff 日後 48 篇、cutoff 日時刻未知 1 篇、較舊 136 篇、未來 0。10 feeds recent、4 mixed、1 no accepted articles。中央社 98/100、鉅亨網 145/148 為 partial；報導者本批無 URL。這些是 feed 記錄時間的批次統計，不代表逐篇核對原頁發稿時間或新聞已證實公司獲利。

成熟 outcome review：H1/1w due 1,528 rows，1,403 settled、125 pending，只有 81 rows source-complete；1m due 131，6 settled、125 pending、81 source-complete。兩者都是 `outcome_data_incomplete`、policy `no automatic policy changes`，confirmed delivery receipt rows 為 0。不可將未連結通知、未結算樣本或單日候選當預測成效。

## 本輪改善假說與決策

假說一：11/02 parser 修正可能只通過唯讀解析，尚未自然持久化。自然 after-close 已以 6/6 accepted、零隔離/重複、DB/runtime `taifex_csv` 一致、6/6 PIT versions 和 48 個 canonical field rows 否證此缺口；修正的驗收條件完成。

假說二：11/03 0 intraday 新卡或 digest `notified_count=1` 可能被讀成「沒有訊號」或「只送出一檔」。自然資料是 12 個日線候選、11 偏空/1 偏多，三張卡都有 receipt、另 9 檔在摘要明示未逐張列出；intraday 的 12 fingerprints 則早已有 12 個 receipt。這些數字口徑足以解釋結果，沒有支持改既有候選門檻或通知欄位的證據；保持現行規則。

本輪只增加追蹤文件與唯讀快照，不更動程式、資料庫、通知或交易狀態，也不補造新聞/價格/法人資料。

## 活躍程序

查核時四個 source DAG 最新 scheduled runs 與 intraday child 均已完成；Airflow metadata 沒有 running 的 source-extension 或 stock-signal DAG。唯一仍 running 的 DAG 是無關本任務的 `game_knowledge_collect`，未干預。
