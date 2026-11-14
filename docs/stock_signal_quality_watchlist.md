# 台股訊號品質追蹤 — 2026-11-03

## 2026-11-03：TAIFEX 選擇權修正自然恢復；日線 12 檔達合併條件

詳見[自然驗收報告](stock_signal_natural_validation_20261103.md)與[20:32 唯讀快照](stock_signal_inventory_snapshot_20261103_2033.json)。最新 after-close logical run `scheduled__2026-11-02T12:00:00+00:00` 於 11/03 20:00–20:29 台北執行，32/32 source tasks 成功、raw/accepted/quarantine/duplicate 為 83,758/82,707/1,051/0，contract drift 0。先前失敗的 `taifex_index_options_positions_daily` 現由 registry/runtime 一致使用 `taifex_csv`，6/6 accepted、0 quarantine/duplicate、effective date 11/02、PIT version 6/6；唯讀欄位查核為 6/6 records、8 canonical target fields、48 field rows，確認自然 bootstrap 與持久化成功。

11/03 日線 universe 1,955、source status partial、候選 12（偏空 11、偏多 1）；1,941 檔進 gate，1,929 檔低於三個方向 family。方向觸發為法人 721、上市分點 53、價格 487。上市法人/分點和上櫃法人已實際判讀；上櫃分點仍 `source_unavailable_skipped`。上市法人 1,046 列自然 PIT same-day 仍 0，上櫃 795 列有 PIT，同期版本限制照實保留。不可把 flow coverage 和達條件數混為一談。

通知假說檢查：3 張個股卡片均有 HTTP 2xx receipt，摘要顯示偏多/偏空分布且宣告另 9 檔未逐張列出；5 個 ledger rows 全有 receipt。`notified_count=1` 是 run-level batch flag，不是候選筆數。Intraday 12 fingerprints 均已送達且有 receipt，新卡 0 是去重結果。未發現證據需改摘要或放寬門檻。成熟 1w outcome 1,403/1,528 settled、125 pending；1m 6/131 settled、125 pending，均 `outcome_data_incomplete`，不調政策。

TWSE margin 1,042 raw 全隔離且無 effective date、TWSE attention 空回應、兩市場 shortfall feed 最新仍 8/19、TPEx flow 122 unmatched、TPEx quote 9 quarantine 均保留為各自缺口/限制，不當零值。Monthly latest run仍只到 2026-09；該 run 43,971 accepted rows PIT 100%、1,103 duplicates、當次 run attribution 未見 unassigned legacy rows，不能外推成整段歷史 lineage 已全核。沒有更動程式或交易門檻。

## 2026-11-02：修正 TAIFEX 選擇權來源 JSON/CSV 契約錯配

詳見[根因與修正驗證](stock_signal_natural_validation_20261102.md)及[20:40 唯讀快照](stock_signal_inventory_snapshot_20261102_2040.json)。10/30 logical after-close run 於 11/02 20:01–20:03 台北實際執行，32/32 source keys 有回報、31 成功／1 失敗；失敗的 `taifex_index_options_positions_daily` 在 HTTP 200、`application/octet-stream`、2,789 bytes 後由 `taifex_json` 對 CSV 第 1 字元解碼失敗，task retry 後相同。官方端點現況直接回傳 16 欄 CSV；這是程式 adapter/schema 錯配，不是零部位或單次暫時故障。已改為嚴格 CSV header 契約並加入官方中文欄名 aliases。

修正後唯讀呼叫官方 endpoint 得 6 個有效 10/30 市場彙總列；BDD 測試先在舊契約紅燈、修正後 19 個 adapter/HTTP targeted tests 通過，兩檔 `py_compile` 通過。這只證明現行 endpoint 可解析；沒有執行或重跑 DAG，也沒有持久化此次唯讀回應。11/02 20:40 快照仍看到舊失敗 run；因 DB registry 尚未由自然 `ensure_source_extension_schema` bootstrap，該歷史 row 對新版 adapter/config 顯示 1 個預期 drift。等待下一個自然 after-close 驗證 registry 更新、逐列 effective date、accepted/quarantine/duplicate 與 PIT fields，不手動改 DB。

同一 11/02 daily run 的前後唯讀快照保持 run key 不變、universe 1,955、candidate 0；1,931 檔各自仍未達三個方向家族，已觸發法人 635、券商分點 29。TAIFEX 是市場彙總背景，不改個股 direction score。1w outcome review 967 settled／125 pending、1m 6 settled／125 pending，均為 `outcome_data_incomplete` 且 policy decision 是 no automatic changes；不調整門檻。日線 digest ledger 2 列均有 HTTP 200 receipt，與 0 個候選卡分開。

本輪其他自然證據：daily source 23/23、8,201/8,201/0/0；intraday 10/10、1,990/1,990/0/0；monthly 最新仍是 9/24 logical run、45,074/43,971/0/1,103。新聞 15 feeds、1,115 URLs、1,113 accepted、2 fetch/parse rejects、0 invalid-time quarantine；freshness cutoff 10/31 18:41:32 台北。逐來源資料及缺列語意見[來源盤點](stock_signal_source_inventory.md)與[驗證報告](stock_signal_natural_validation_20261102.md)。

## 2026-10-30：跨 DAG 寫入 pool 首輪自然批次驗證

詳見[自然驗證紀錄](stock_signal_natural_validation_20261030.md)及[20:30 唯讀快照](stock_signal_inventory_snapshot_20261030_2030.json)。修正後 daily source process 23/23、after-close source tasks 32/32、intraday source tasks 10/10 均以 `source_records_write` pool、try 1 完成，沒有 1213 deadlock；monthly 仍是舊 9/24 logical run。After-close mapped ingest 已完成，但 parent 在快照時仍等待既有 `stock_collect_everyday_process`，不視為全 DAG 完成。三個 source batches 實際沒有跨 DAG 寫入時間重疊，因此限流生效已確認，死鎖永久消失或壓力下效果尚未證明。

10/29 daily scan 仍 1,935/1,935 低於三家族、0 candidates；10/30 intraday 502 event rows 中排除 463 非 key、7 superseded forecast、2 duplicate semantics，30 fingerprints 全為既有 sent，25 有 HTTP 2xx+receipt、5 receipt unknown，沒有新候選。不能因 0 候選調低門檻。最新 H1 新結算樣本仍屬 `stock_evidence/subthreshold_evidence`；1w/1m reviews 未完整，沒有策略調參依據。

10/30 after-close 的 TWSE margin 仍 1,042 全隔離且無 effective date；TPEx institutional flow 917 accepted、796 對碼、121 unmatched，未配對不當零值。News 15 feeds、1,127 URLs、1,122 accepted、5 fetch/parse rejects、0 invalid-time quarantine；時間精度細節見驗證紀錄。沒有手動觸發 DAG、重跑、發 Discord 或交易。

## 2026-10-29：定位重複 DB deadlock 並限制來源 DAG 併發

詳見[根因與自然驗證報告](stock_signal_natural_validation_20261029.md)及[20:35 唯讀快照](stock_signal_inventory_snapshot_20261029_2035.json)。Daily source process 的 `twse_balance_sheet_ins` 在共用 `source_records` batch upsert 第 1、2 次均遇 MySQL 1213；前幾個 daily run 和部分 after-close run 亦有同類首輪 deadlock。這是共享寫入鎖競爭，非來源 HTTP／解碼失敗。除每 DAG 的 `max_active_tasks=2` 外，已讓五個 collection DAG 的 mapped 寫入任務共用 2-slot `source_records_write` pool，並納入 `airflow-init` 啟動設定。現有 Scheduler 已載入 pool 指派且 metadata pool 容量核為 2；Compose/DagBag/unittest/compile 檢查通過。同一 10/29 cutoff 的來源與候選結果不變，未回寫訊號資料或觸發 DAG；死鎖率與耗時仍待下一個自然批次驗證。

10/29 掃描 1,935 檔全未達三家族條件，候選 0；方向觸發法人 576、上市分點 64，上櫃分點 unavailable。沒有證據支持降門檻。After-close TAIFEX 1,386/1,386 與三個 context feed 自然接受、有效日 10/28；是當日解析成功，不證明先前失敗根因永久消失。

## 2026-10-28：TAIFEX 自然解析恢復，並區分未達門檻 outcome

詳見[自然驗證報告](stock_signal_natural_validation_20261028.md)與[20:33 唯讀快照](stock_signal_inventory_snapshot_20261028_2033.json)。TAIFEX 大額交易人 1,386/1,386、期貨／選擇權法人各 6/6、TPEx OAS 市場彙總 16/16 自然接受，最新有效日均 10/27；這次未重現先前 JSON 解碼失敗，但只證明當日恢復，不能宣稱根因已永久排除。有效日仍早於 10/28 掃描日；5 日門檻是 audit 警示，signal reader 不沿用非同日值。

日線 1,937 檔進入評估，全部未達三個方向家族，0 個候選；方向觸發為法人 482、上市分點 73，TPEx 分點仍 unavailable。行情缺 quote code 16 檔、上櫃 no-trade 9 檔、上市無效收盤 2 檔分開保留；上市法人 natural PIT 0、上櫃 785。無證據支持降門檻。

合理性檢查發現 mature outcome 聚合原先未顯示 `stock_evidence` 次門檻樣本：10/27 TPEx signal-close H1 的 857 settled rows 是 `subthreshold_evidence`，不是已送出個股候選的績效。只在唯讀快照新增 `entity_type`／`evaluation_cohort` 分組；相同 signal-date 分組結果值不變。驗收情境與詳細批次數字見報告。

## 2026-10-27 20:58 台北：修正 freshness 日曆門檻並判讀自然日線結果

詳見[20:58 唯讀快照](stock_signal_inventory_snapshot_20261027_2057.json)與[來源盤點](stock_signal_source_inventory.md)。after-close 32/32 source rows 回報，raw/accepted/quarantine/duplicate 為 85,480/84,427/1,053/0、contract drift 0；TWSE 第二次分點收集 20:53:54 成功，等待感測器、通知觸發與驗證步驟隨後自然成功，parent 已完成。由它建立的 10/27 日線掃描在 20:55:30 執行，沒有候選，但這次不是休市 skip。intraday 10/27 收 10/10 source rows（2,009/2,006/3/0），33 個事件列中 6 個 fingerprints 均已在 sent ledger；1 有 HTTP 2xx receipt、5 receipt 未確認，無新候選。

判讀假說：TAIFEX 期貨法人／選擇權法人／大額交易人三個 feed 為 6/6、6/6、1,386/1,386 accepted；TPEx OAS 11627 上櫃當沖彙總為 15/15 accepted；最新 effective date 均為 10/22。20:48 的暫定 3 日 audit budget 把 5 日曆差標成 stale。官方 TAIFEX 公告確認 10/23 期貨市場因薔琵颱風休市，TWSE 日曆列 10/26 補假；TPEx 當沖資料說明另有 T+2 最終更新。這些證據說明日曆日差不等於交易日數，也不能用一個市場日曆推定其他市場的開休市或解釋 TPEx 當日缺列。[TAIFEX 公告](https://www.taifex.com.tw/cht/11/announcement)、[TWSE 2026 開休市日期](https://accessibility.twse.com.tw/zh/trading/holiday.html)、[TPEx OAS 11627](https://data.gov.tw/dataset/11627)、[TPEx 當沖 T+2 更新說明](https://www.tpex.org.tw/storage/zh-tw/web/stock/trading/intraday_stat/intraday_trading_statD.htm)。

已把四個 daily market-context sources 的 audit-only age budget 修正為 5 個日曆日。以同一 after-close source run 作唯讀前後比較：舊 3 日上限會把 age=5 標 stale；新上限在 age=5 邊界不報 stale，effective date、accepted/quarantine 列數、runtime contract 與 signal reader 輸入均未改。這只是 freshness 診斷門檻，不表示資料已是當日資料：reader 仍要求 effective date 等於 scan date 才引用數值；TPEx OAS 仍只有上櫃市場彙總、沒有個股覆蓋。若後續自然 run 仍未前進且 age>5，才報 stale。四個 BDD 測試、`py_compile`、Airflow import-error `[]` 和 source-file whitespace check 通過；repo-wide `git diff --check` 另報既有使用者修改的 JPG 檔有尾端空白，本輪未動該檔。

10/27 日線掃描已完成：主檔 1,953 檔，1,940 檔進入方向評估；上市/上櫃價格覆蓋分別為 1,055/1,057、872/896，有效法人方向資料 1,049/1,057、771/896。家族觸發數為法人 434（上市 326、上櫃 108）與上市分點 25；broker 子集 513 accepted panels 中 194 可判方向、25 達門檻。沒有 price、volume、profitability 方向觸發；上櫃券商分點為 `source_unavailable_skipped`，這個缺口可能藏有第三家族訊號，不能寫成上櫃分點為零。當日已見的兩類方向家族不足三類，1,940/1,940 評估標的都未達合併條件，因此 0 候選是已觀察到的家族條件結果，同時保留上櫃分點缺口；不把它寫成整體資料缺失，也不因此降門檻。主檔有 13 檔缺同日行情列（上市 1、上櫃 12）；上櫃另有 12 個 no-trade 行情列，仍與缺列分開。TWSE 同日法人資料 1,049 列為 collection-time 儲存、自然 PIT same-day 計數 0；TPEx 772 列有自然 PIT same-day。兩市場法人預測成效仍未驗證。

掃描 source status 為 `partial` / `data_review_required`，但各類已有實際計數：universe 1,953、price 1,927、flow 1,820、revenue 1,948、financial 1,908、borrow 1,721、insider 1,943、dividend 1,949。其候選 filter 明列 1,940 evaluated、1,940 below three directional families。0 候選日線仍送出一則摘要通知，ledger 2 rows、HTTP 200 與 receipt 各 2；這是摘要送達，不代表有個股卡片。日線 no-candidate 結果、來源部分狀態、摘要回執分開記錄。

同日新聞 15 feeds、1,118 URLs、1,112 accepted、6 fetch/parse rejects、0 invalid-time quarantine；記錄時間 freshness 為 915 精確時間在 48h、35 date-only 僅晚於 cutoff 日期、162 較舊。最新 monthly run 2/2 success，兩市場 2026-09 effective rows 的本次 DAG/run lineage 可核、PIT 100%；此結論不外推至歷史月頻列。成熟 outcomes 須按 signal date、market、entry basis 和 horizon 分開；H5/H10 獨立日期仍少，不調分數或門檻。

## 2026-10-26 20:38 台北：修復 package import；自然重試恢復來源寫入

詳見[最終唯讀快照](stock_signal_inventory_snapshot_20261026_2038.json)與[自然驗證報告](stock_signal_natural_validation_20261026.md)。`dags/source_extension/__init__.py` 含一行字面 `\n`，造成 Airflow DagBag 對 source-extension modules 的 SyntaxError。移除後 package BDD contract 綠燈、Airflow import-error 清空，scheduler 自然啟動 overdue 的 after-close 與 intraday runs，均成功。

最新 daily／after-close／intraday／monthly source raw/accepted/quarantine/duplicate 為 13,845/13,840/5/0、85,469/84,411/1,058/0、1,995/1,992/3/0、45,074/43,971/0/1,103；contract drift 0。五個 mapped source 首次遇 MySQL 1213 deadlock，既有一次 retry 全數成功；不視為 feed parser 失敗，暫不改並行度，追蹤後續頻率。

10/22 行情日的 TWSE／TPEx quotes 與 TPEx 法人流已收：TWSE 1,053 accepted，TPEx 892 raw／876 accepted／16 quarantine（8 explicit no-trade），TPEx flow 909 accepted／788 對碼／121 unmatched；TAIFEX 1,386 accepted。TWSE margin 1,042 全隔離且無 effective date。月頻來源本次兩市場均有 lineage、有效月份 2026-09、PIT 100%；八個季 EPS／損益批次的可用期仍只有 2026Q2。

日線 child 10/26 因 `market_closed` 跳過，沒有日線掃描；不能稱 0 candidates。Intraday 選入 11 事件列，7 指紋皆已在 sent ledger（2 有 receipt、5 傳輸未知），新候選 0 是去重／策略篩選結果。最新成熟 H1 仍只有 10/21 一個日期；horizon/follow-up 表無新列。因此 10/23 的 digest byte-budget 與 follow-up 欄位修正尚未取得自然端到端驗收。

新聞 10/26 自然批次 15 feeds、1,125 URLs、1,120 accepted、4 fetch/parse rejects、1 invalid-time quarantine；832 exact timestamp within 48h、8 date-only 晚於 cutoff 日、1 日期時間未知、279 較舊。報導者無 URLs；date-only 不作精確 48h。

## 2026-10-23 20:30 台北：修正 digest byte-budget 與 horizon DB 欄位契約

詳見[唯讀自然快照](stock_signal_inventory_snapshot_20261023_2030.json)與[自然驗證報告](stock_signal_natural_validation_20261023.md)。daily／after-close／intraday 來源列 23／32／10，raw/accepted/quarantine/duplicate 為 13,835/13,830/5/0、85,266/84,208/1,058/0、2,177/2,174/3/0；runtime contract drift 0。最新 daily child 使用有效行情日 10/22，產生 22 candidates，但 digest 因單則 10,700-byte embed 超過 10,000-byte Discord request 上限失敗；`notified_count=0`。失敗通知的 HTTP 200 receipt 不代表候選 digest 已送達。

同 cutoff formatter 修正後唯讀重播保留全部 8 張個股卡，拆成 10 requests，最大 9,244 bytes；未實際送出。另一個 deterministic failure 是 follow-up SQL 選取不存在的 `max_high_excursion_pct`；部署 DB 使用 `max_favorable_excursion_pct`／`max_adverse_excursion_pct`。程式與 reader 已改用既有欄位，未加 migration；4 個 lifecycle tests、BDD formatter contract、compile、DagBag/import-error check 通過。下一個自然 run 的 review/follow-up rows 尚待驗證。

盤中選入 2,293 個事件列，27 個指紋皆已在 sent ledger，22 個有 HTTP 2xx/receipt、5 個沒有確認回執；月營收 1,981 列缺預期基準，非 key 事件 276、舊財測版本 7、重複財測 2 被排除。0 new candidates 是此 cutoff 沒有新可送事件，不是資料未收。TAIFEX 大額交易人自然 run 回復為 HTTP 200、1,386 accepted、effective 10/22；先前 JSON decode 失敗此次未重現。TWSE margin 1,042 全隔離、無有效日。季度獲利率只有 2026Q2，無同比推論。月頻來源 run lineage 仍 unknown。

最新成熟 H1 是 10/21 signal-close：TPEx 5/5 directional hits、平均超額 +1.4585pp；TWSE 10/6、+1.9097pp。只有一個訊號日期、未扣交易成本。Follow-up/horizon review 表本次仍空，故只報告 outcome，不升級方法或聲稱預測成效。

## 2026-10-22 20:33 台北：盤中診斷自然落地；日線分母差異已核清

## 2026-10-22 20:33 台北：盤中診斷自然落地；日線分母差異已核清

最新[唯讀快照](stock_signal_inventory_snapshot_20261022_2033.json)與[自然驗證報告](stock_signal_natural_validation_20261022.md)：daily / after-close / intraday 分別 23/23、32/32、10/10 回報且 contract drift 0，raw/accepted/quarantine/duplicate 為 13,834/13,829/5/0、85,543/84,485/1,058/0、2,107/2,102/5/0。任務成功仍有資料審核項；monthly lineage 仍 unknown，9 列舊 source-run 不按 run ID 猜測。

改善假說檢查：日線 evaluated_stocks=1,929 比有效 price_count=1,915 多 14。逐市場比對發現 1,929 等於接受的當日行情代碼列；1,915 是有效收盤，差額正是 6 個無效收盤和 8 個 no-trade。主檔分母 1,950，另 21 檔沒有進入接受代碼覆蓋（其中上櫃18檔仍可能含隔離，不能一律稱沒來源列）。兩種口徑用途不同，保留計算；未把 no-trade 或隔離列補零。

日線 22 個候選（11偏多／9偏空／2分歧），1,907/1,929 不足三個方向家族。通知摘要 8 張卡＋footer 14 檔；ledger 10 rows、10 HTTP 2xx、10 receipts。法人與上市分點已有實際判讀；TPEx 分點仍缺收。成熟 10/21 H1 只有一個獨立訊號日期，不調參。

盤中 20:30 自然 child 的 XCom 已首次確認保存 intraday_candidate_diagnostics：2,255 source rows 分階段排除後 24 fingerprints 全命中舊 sent ledger，19 有 HTTP/receipt，5 筆財測差異無 transport 證據，沒有新候選。1,981 個月營收列目前缺預期基準，不投方向票；未重送 5 筆未知傳輸資料。

本次來源待確認：TWSE margin 1,042 列全隔離且無有效日期；TPEx 行情 16 列隔離；TAIFEX 大額交易人 1,386 accepted、effective 10/21，比行情落後一個交易日；董監持股不足額來源仍停在 8/19。季損益／EPS 八來源本次 accepted 1,978，只有 2026Q2；存量比自然 run 多一筆 TPEx CI、未歸因本次 run。新聞 15 feeds 接受 1,127，6 fetch/parse reject、1 時間隔離；date-only 不當精確48小時。

## 2026-10-21 20:50 台北：盤中零候選根因、回執落差與來源 freshness

唯讀快照：[20:49 inventory](stock_signal_inventory_snapshot_20261021_2049.json)；逐來源驗證：[自然批次報告](stock_signal_natural_validation_20261021.md)。Daily source run 23/23：13,828 raw／13,823 accepted／5 quarantine／0 duplicate；after-close 32/32：85,550／84,496／1,054／0；intraday 10/10：2,144／2,136／8／0；runtime contract drift 均 0。任務全 success 仍有來源需資料審核。

10/21 日線 `ready/partial`、universe 1,949、候選 15（10 偏多／4 偏空／1 分歧）。1,932 進入規則評估，1,917 因少於 3 個方向家族未過；15 達既有條件。方向族觸發：法人 757、價格 203、上市券商分點 44、量能 47，彼此重疊不相加。上市／上櫃法人可判讀 1,043／781，達法人方向條件 538／219；上市存量資料 natural PIT 0、上櫃 TPEx 法人 782 列有 natural PIT。上市券商分點 510 accepted panels、167 可判讀、44 達方向條件；上櫃分點仍未收集。兩市場既有法人和上市分點均已參與判讀。

行情主檔分母上市／上櫃 1,056／893。TWSE 有效收盤 1,052（99.62%），另 1 個代碼收盤無效、3 個無行情列；TPEx 有效收盤 869（97.31%），另 10 檔明確 no-trade、14 個無行情列，quote adapter 另隔離 12 列。缺列、隔離與 no-trade 保持分開，不補零。TPEx 同日法人可判讀 781；88 個有成交代碼未見同日來源列，仍是未知。

通知對帳：15 candidates 中 8 張個股卡、摘要 footer 明列另 7 張，正好 15；9 筆 ledger rows、9 HTTP 2xx、9 receipts、0 errors。`notified_count=1` 是 daily digest flag；回執不代表人工已讀。

改善假說與驗證：盤中 `candidate_count=0` 可能是缺來源，也可能是規則過濾或已送去重。按 10/21 20:30:24 原 cutoff、程式實際 7 日事件窗／1 日 revision lookback／10,000 ID overlap 唯讀重算：選入 121 列（TWSE／TPEx 公司公告 53／59、TWSE 財測差異 7、內部人轉讓 2）；公告分類 7 key、70 review、35 routine，105 列因非 key 排除；另 2 個財測列為重複語意。14 個候選指紋均命中 `sent` ledger，沒有新待送候選；其中 9 有 HTTP 2xx 與 receipt，5 個財測差異列只有 ledger `sent`、缺 HTTP／receipt，故實際傳輸未確認。未重送，不能把 5 列標成已確認通知。已窄幅補上 intraday XCom 的選入／排除／去重／回執計數，不改訊號、門檻或傳送；單元測試及相同 cutoff 唯讀重算通過，下一個自然 run 才能驗證 XCom 落地。

after-close 1,054 個隔離列由 TWSE margin 1,042 與 TPEx 行情 12 組成。TWSE MI_MARGN 本次 1,042 raw 全隔離、0 accepted；保存樣本的來源 `Date` 為空，沒有有效日。保持 fail-closed，不補本機日期，也不推為零信用餘額。TAIFEX 大額交易人本次 HTTP 200、1,386/1,386 accepted、effective date 10/20，較 10/21 行情落後一個交易日；自然 JSON 解析錯誤未重現，但資料尚非同日。TPEx margin 801 accepted、effective 10/20；TWSE 注意交易資訊 200/0 仍不能當全市場無注意股。上市／上櫃董監持股不足額資料最新有效日仍停在 8/19，須視為過舊背景。

新聞自然 run `scheduled__2026-10-21T08:30:00+00:00` 於 18:30–18:40 台北完成：15 feeds、1,141 URLs、1,136 parsed／有效時間／accepted、5 fetch/parse reject、invalid-time 0、content/duplicate 0，拒絕列核帳相符。5 rejects：iThome 1、中央社 1、鉅亨網 3。依來源記錄精度：938 個精確 timestamp 在 48h、51 個 date-only 僅能證明晚於 cutoff 日期、11 個 cutoff 日期沒有時刻、136 個較舊、future 0。所有 feeds 的 freshness/reject 分布已寫入快照；這是 parser 記錄的發稿時間，不等同逐篇核原頁或歷史 revision vintage。date-only 不算精確 48h。

月報 audit 仍 `unknown`：最新成功排程 run 的 source rows 沒有 `dag_id`，9 列不可按重疊 run ID 歸因；兩個 monthly source 沒有可歸屬的 source-run 回報。scheduler 下次 earliest create 為 10/25 04:00 台北，不手動重跑。季損益／EPS 可用資料仍只有 effective 2026-06-30；沒有可比期不推同比或方向票。新成熟 10/20 signal-close H1：TWSE 14 rows／8 hits／平均超額 +1.3962pp；TPEx 3／3／+7.9828pp。只有一個訊號日期，未扣成本／公司行動，不據此調權重或門檻。

## 2026-10-20 20:45 台北：候選篩選與摘要卡片對帳；季獲利率缺歷史可比期

10/20 自然日線 `ready/partial`：universe 1,949、行情列代碼 1,931、候選 17（12 偏多／5 偏空）；1931 個有行情列的標的均進入評估，1,914 檔明確因不足 3 個方向家族未達候選條件。其餘 18 檔恰為上市 3／上櫃 15 檔沒有當日行情列，未當成零成交。方向家族觸發數：價格 202、量能 41、法人 707、上市券商分點 35；族間重疊，不可相加。上市／上櫃法人可判讀 1,045／760，分別 503／204 符合法人方向條件。上市存量資料 1,045 列仍無自然 PIT；上櫃同日 TPEx 法人 761 列有自然 PIT；上櫃券商分點缺收。既有法人與上市券商資料均已讀入，沒有因舊來源名而全域排除。

通知審核確認：17 個合格候選中，日摘要逐檔展示 8 張（偏多最多 6、偏空／分歧最多 2），另 9 張由摘要 footer 明確標為未逐張列出；8 張卡與摘要共有 9 筆 ledger rows，9/9 HTTP 2xx、有 receipt、0 error。daily `notified_count=1` 是 digest flag。沒有把回執當人已讀，也沒有改訊號規則；目前 cap 與 footer 語意一致，避免單日為多展示而增加通知量。唯讀快照已新增 per-run XCom 的候選篩選計數，以及序列化卡片與省略數 reconciliation，讓下次可直接看出通知覆蓋。

`profitability` 家族沒有方向觸發；point-in-time 24 個月資料只找到 2026-06-30 的季獲利率列，沒有同年 Q1 累計值可推 Q2 單季率，也沒有 2025Q2 同季比較。現行規則要求同季同比、變化至少 3 個百分點或跨零才投票；此處是可比歷史缺口，並非 1,904 筆財務脈絡全缺或門檻失準，不放寬門檻、不補造同比。八個季損益／EPS feed 共 1,977 accepted、均 effective 2026-06-30；持久化檢查 1,978 筆（TPEx CI 多 1 列，沒有 source-run row lineage）之基本 EPS 全非空，只有 Q2，上市 CI 31 列未對上市櫃普通股主檔。

TAIFEX natural source_run 10843 HTTP 200、1,386/1,386 accepted、effective 10/19；落後 TWSE／TPEx 報價 10/20 一個交易日。成功 Content-Type 未持久化，不能確認 octet-stream 分支。TWSE margin 1,042 全部因沒有來源有效日隔離；TPEx margin 801 列 effective 10/19。詳見[10/20 自然驗證](stock_signal_natural_validation_20261020.md)與[同時點唯讀快照](stock_signal_inventory_snapshot_20261020_2045.json)。

## 2026-10-19 20:40 台北：TAIFEX post-fix 自然解析成功（MIME 分支未確認）；日線 delivery 對帳補強

10/19 after-close TAIFEX source_run 10141 在 post-fix adapter 下自然成功：HTTP 200、1,386/1,386 accepted、13,860 canonical fields 已存；有效日 10/16，落後同批 TWSE／TPEx 行情 10/19。成功回應 Content-Type 未寫入 source_run，故只能證明當次格式可解析，不能證明走過 octet-stream 分支；10/16 exact-filename MIME 誤拒仍是已確認的程式缺陷，10/07–09 JSON char-0 是否同因仍未知。資料新鮮度未恢復，市場期貨資料只作背景。TWSE margin 的 1,042 raw 列無有效日並全部隔離，不補本機日期。

10/16 自然日線 `flow_count=1,804`（上市 1,036 usable／551 qualified；上櫃 768 usable／237 qualified）及 16 candidates 已持久化；上市存量法人列無 natural PIT，TPEx 768 same-day PIT；上市分點該日 0 個有效 panel、TPEx 分點缺收，兩者不等於整體法人缺失。日摘要 `notified_count=1` 是 daily digest flag，實際有 9 sent ledger rows、9 HTTP 200／receipt，不能推成人已讀。唯讀快照新增 per-run delivery rollup 並已和 DB 逐列對平。

10/19 after-close source 32/32 tasks success（85,516 raw／84,463 accepted／1,053 quarantine／0 duplicate，0 contract drift）；父流程於 20:36 自然觸發 daily child。20:40 子 run 仍在計算，當時 `as_of_date` 未寫入，暫時零值不是掃描結果；20:30 intraday 確為 0 universe，亦不能稱全市場無訊號。新聞 18:30 自然批次 15 feeds、1,126 accepted／6 fetch-parse rejects；874 exact timestamps within 48h，28 date-only after cutoff date，1 cutoff-date time unknown，223 older by recorded precision。細節見 [10/19 自然批次核驗](stock_signal_natural_validation_20261019.md)。

### 2026-10-19 20:43 台北：after-close 子流程完成，兩市場法人覆蓋進入自然日線

10/19 daily scan `ready/partial`，universe 1,948、price 1,917、flow 1,810、candidate 7；上市／上櫃可判讀 flow 1,042／768，分別 489／208 達既有方向條件。上市自然 PIT 0（stored capture 1,045），TPEx 768 same-day PIT；上市券商分點同日 panel 0、上櫃分點缺收。7 candidates 對應 7 sent ledger、7 HTTP 200／receipt、0 errors；日線 `notified_count=1` 是整批摘要 flag，不是 1 張卡。10/16 H1 成熟：TWSE 12 rows／4 hits／+1.1524pp、TPEx 4／1／+0.3002pp，僅一個日期，不據此調參。詳見 [自然批次核驗與快照](stock_signal_natural_validation_20261019.md)。

## 2026-10-16 20:46 台北：修正 TAIFEX octet-stream 假拒收；自然驗收仍待 after-close

新發現：10/16 after-close TAIFEX HTTP 200 但 parser 在讀 body 前因 `application/octet-stream` 未符合 exact filename gate 拒收；來源記錄未保存檔名標頭與本文，不能判定原文格式。僅對該 allowlisted feed 開放 octet-stream，後續仍須符合嚴格 JSON key 或 CSV header。紅燈重現、綠燈 7 targeted tests、py_compile 與 worker DagBag 通過；下一個 natural source_run 才能證明恢復，當前仍 0 accepted／無有效日。詳細自然資料及新聞對帳見 [source inventory](stock_signal_source_inventory.md) 和 [repair report](stock_signal_order_flow_repair_20261016.md)。

另核 10/16 after-close 31/32 source keys success、TAIFEX 1 failed；父 DAG 正在等 19:21 起仍執行中的 TWSE 分點第二次收集，sensor 有 21:30 TPE 截止，先保留現行 bounded wait。最新 20:30 intraday event scan 0 candidate，但來源有 99+31 company events 及 5 個 insider-transfer sentinel 全隔離；因此不寫「無訊號」或「整體資料不足」。10/12 新成熟 H1 只有 16 rows，兩種 basis 都不據此調門檻。新聞 18:30 natural run 1,129/1,129 已按 15 source 與實際 DB rows 對平； freshness 中 133 rows 按來源精度偏舊，date-only 不計作精確 48h。

## 2026-10-16 07:31 台北：依使用者校正，既有法人與行情恢復實際訊號判讀

目前政策取代下方「legacy 法人不讀／方向須先等成效驗證」的舊限制。已修正資料被排除、方向固定關閉、觸發數被誤當覆蓋率，以及既有行情歷史未讀取四項根因。固定 10/15 20:59:28 cutoff 的唯讀比較：法人可判讀 0 → 1,820，834 檔達法人條件；接回行情後量能 77 檔，合併候選 0 → 20（5 多／13 空／2 分歧），門檻未放寬。上市／上櫃法人已收 1,044／777，可判讀 1,043／777，達條件 578／256。TPEx 券商分點未收集只影響該項；TWSE 分點來源存在，最新保存 10/14、796 檔，10/15 缺同日列要精確標日期，不能概稱未接通。

59 targeted tests passed；新回歸已實跑 red/green；AST 與 worker DagBag 通過，20 張卡與 8 批預覽均符合限制。新規則與舊自然結果分開，未改舊 run、未觸發 DAG、未發 Discord。既有平日 20:30 heartbeat 已原位更新，每次須檢驗判讀合理性並完成有證據的窄幅改善；無證據不硬調門檻。詳見 [本次修正與驗證](stock_signal_order_flow_repair_20261016.md) 及 [完整唯讀重算](order_flow_recompute_20261016_verified.json)。

## 2026-10-16 06:53 台北：新聞鎖競爭修正通過自然落庫驗收；月頻時間標籤釐清

- 新聞修正後兩個自然 run 均一次成功：10/15 23:30–23:40 收錄 1,139/1,140 URLs；10/16 05:30–05:37 收錄 1,036/1,040 URLs。後者 `collect_news` accepted、`delete_and_insert` receipt 與新聞表實際 15 來源逐一對平，沒有 source/article_id 重複；主鍵倒序 3,000 筆視窗涵蓋較早批次邊界，足以核對本次 1,036 筆。06:52 唯讀 processlist 與 `article_url` metadata lock 均無相關 active/waiting statement。這驗收了 DELETE／單一 active run 修正後的自然恢復；先前長讀者身份仍未知，兩批成功不代表長期零故障。證據：[自然 run/XCom](stock_signal_natural_followup_20261016.json)、[實際落庫/鎖](stock_signal_news_storage_20261016.json)。
- 最新新聞有 837 筆精確時間在其 48h 窗內、24 筆僅日期在 cutoff 日後、34 筆 cutoff 日時刻未知、141 筆按記錄精度較舊；4 fetch/parse rejects（三立 1、鉅亨 3），無 invalid-time/content/duplicate reject。iThome 60 篇全部只有日期；TheNewsLens 82/90 較舊。這些是自然 parser/XCom 的原發稿欄位與精度分類，本輪未逐篇重核原頁，未宣稱公司 mention 是獲利事件或轉載是獨立佐證；新聞正文仍沒有不可變歷史版本。
- [06:53 唯讀快照](stock_signal_inventory_snapshot_20261016_065331.json) 已新增 67 來源的最後嘗試，以及四個來源 DAG 的 logical date、實際開始/結束、active/paused、下次最早可建 run 時間。worker 實讀成功、3 份盤點 Python AST 通過；未改生產 DAG 或資料、未手動觸發、未發 Discord。本 chat 原 heartbeat 已核 ACTIVE／台北平日 20:30，沒有另建工作。
- 月頻 logical run `scheduled__2026-08-24T20:00:00+00:00` **實際在 9/25 04:00–04:00:53 台北執行**；scheduler 的下一個 `next_run_create_after` 是 **10/25 04:00 台北**。因此不是「8 月後停跑」或「10 月漏跑」證據。9 筆 legacy source-run rows 仍缺 dag_id、不能只靠重疊 logical ID 歸屬；monthly audit 繼續 unknown，須等 10/25 自然 batch 驗收可歸因 lineage，禁止補造或手動重跑。
- 10/15 after-close parent 已於 20:59 完成；daily scan as-of 10/15 `ready/partial`、universe 1,948、有效價格 1,920、flow 0、candidate 0。兩個 HTTP 200/receipt 的實際 payload 分別是「資料不足」與「法人與分點來源覆蓋」，沒有方向候選卡；receipt 不等於人已讀。TPEx 777/894 同日對碼/PIT、685 檔五日完整、716 檔三日完整，95 檔有成交卻缺同日 flow；方向仍 `context_only_validation_pending`。上市授權 bulk 缺口維持 unknown。
- TWSE E-Shop 官方商品契約新增釐清：[三大法人買賣超檔](https://eshop.twse.com.tw/zh/product/detail/c4c87ac184e44896a05fcab5a9d544ec) 有 18:00 不含鉅額 `TWT86UC` 與 20:00 含鉅額 `TWTAIUC` 兩版本，不能混作相同 vintage 或以產製時刻冒充可得時間。列有 TEXT/Excel、線上/電郵交付及內外部用途區分，但未取得 entitlement、穩定機器介面與本專案儲存/回測/外顯許可，仍不接入、不訂購。來源台帳保存契約與缺口，沒有把公開報告樣本接成方向 feed。


## 2026-10-15 20:46 台北：TAIFEX 首個自然恢復驗收；新聞批次鎖競爭修正待自然驗證

- TAIFEX CSV 中文欄名 aliases 已由自然 after-close source_run 8638 驗收：HTTP 200、1,386/1,386 accepted、0 quarantine/duplicate、effective date 10/14，且 1,386 records 持久化 13,860 個 source_record_fields。原本 10/13 隔離列不回寫；期貨彙總不投個股方向票。TWSE quote 有效日 10/15、1,050/1,054 listed master；TPEx quote 878/894 OTC master、9 隔離。TPEx flow 898 accepted、777/894 matched、121 codes off-master，未開方向票；TDCC 69,496 accepted 且雙市場完全配對，但 effective date 10/08。
- after-close DAG 32/32 sources success、85,240 raw / 84,191 accepted / 1,049 quarantined/rejected / 0 duplicate，但 parent 尚在等自然 daily order-flow process，scan/notification child 未觸發。TWSE margin 仍 1,040 全隔離、無 effective date；daily scan `ready/partial`、flow 0、candidate 0 不當作零流量或無訊號。
- 新聞 10/15 三個連續排程 run 都在 `TRUNCATE article_url` 等待 exclusive metadata lock 後 timeout。讀鎖 owner 是一筆執行數小時、關聯 `article_url` 與 `news` 的 SELECT；六個 TRUNCATE 重試排隊，顯示是程式使用 DDL 清表遇到長讀者的鎖競爭。Airflow 對應 runs 已全部 terminal failed，因此只取消六個仍等待、未執行的 TRUNCATE statements；pending TRUNCATE 降為 0，reader 的 shared-read lock 保留。已改 DML DELETE 並限制 DAG 單一 active run；2 個針對性測試通過。下一自然 run 再驗收，未手動觸發或重跑。
- 最近成功新聞批次為 12:30–12:40 台北：15 sources、1,130 URLs、1,126 accepted、4 fetch/parse rejects。Freshness 不能只看 source `complete`：iThome 全 60 篇只有日期精度；TheNewsLens 81/90、報導者 9/10、環境資訊中心 40/48 依記錄精度較舊。逐來源數字與 MDL 證據見 [source inventory](stock_signal_source_inventory.md)。

## 2026-10-12 20:38 台北：驗證自然批次並關閉未核准法人方向票

- 唯讀快照：[20:38 source/run inventory](stock_signal_inventory_snapshot_20261012_2038.json)。67 registry keys；daily process 23/23、after-close 32/32、intraday 10/10 都無 missing key／contract drift；monthly lineage 繼續 unknown。
- After-close logical run `scheduled__2026-10-09T12:00:00+00:00` 實際 10/12 20:00–20:32 台北成功完成。TAIFEX large-trader HTTP 200、1,386 accepted，但 effective date 仍 10/08；JSON 解碼可恢復，資料新鮮度未恢復。TWSE margin 1,040 raw 全數 quarantine、無 effective date，證實不再用本機日期補標。TPEx flow 908 accepted、787/893 上櫃普通股配對、121 accepted codes 在主檔外，版本覆蓋 100%。
- 10/12 自然日線 child `ready/partial`，1,947 universe、1,917 price rows、1,828 flow rows、16 candidates。追查發現舊 `equity_order_flow.py` 仍讀 `stock.三大法人買賣`，把 legacy rows 當 `twse_t86_stored`；上市 natural PIT 為 0 仍開啟方向票。TPEx 787 列雖有同日 natural PIT，尚有多日期成熟成效驗證缺口；此 run readiness 對兩市都標 `direction_enabled`，16 個候選都帶法人方向（11 正、5 負）。
- 已移除 legacy 法人流輸入；上市來源維持未授權／unknown。TPEx 官方 flow 保留 accepted/PIT/coverage 與影子檢查，但方向家族關閉，未進候選分數。舊 run 的候選與通知不重算，下一個自然日線 run 才能驗證新路徑。修正前的新回歸檢查 5 項失敗；修正後法人流、PIT、政策測試 32 passed，py_compile 成功、Airflow DagBag import errors `[]`，worker hash 與本機一致。詳見 [source inventory](stock_signal_source_inventory.md)。
- 同日新聞 12 feeds 持久化 1,265/1,289 URLs；4 fetch/parse reject、20 空白內容 reject、0 invalid-time quarantine、duplicate 0；8 complete／4 partial。1,112 筆精確時間在 48h 窗內；date-only 34 筆晚於 cutoff 日期但不是精確 48h、2 筆 cutoff 日時刻未知、117 筆依記錄精度較舊、future 0。
- 自然 daily digest 有 16 candidates；ledger 9/9 batch rows `sent`、HTTP 200、每筆有 platform receipt，payload 共 10 embeds。`notified_count=1` 是 run flag；receipt 不代表人已讀。成熟 outcomes 仍僅 9/24、10/02 兩個 signal dates；10/02 TPEx H1 next-close 44/12、-1.119pp，TWSE H5 signal-close 僅 6/2、-1.562pp；不據此調分數或門檻。

## 2026-10-09 23:26 台北：TAIFEX 解碼失敗與 TWSE margin 日期根因

## 2026-10-09 23:26 台北：TAIFEX 解碼失敗與 TWSE margin 日期根因

- TAIFEX `source_runs` 的 10/06 自然 after-close attempt 1 成功（1,386 accepted）；10/07–10/09 每次自然 run 的 attempt 1/2 均 HTTP 200，錯誤為 `JSONDecodeError: Expecting value: line 1 column 1 (char 0)`，row/accepted 為 0。解碼在欄位正規化前失敗，現有程式不是 TAIFEX row schema mismatch。當日稍晚由 Airflow worker 對同一官方端點唯讀取得 1,386 rows，`fetch_and_normalize` 全數接受、有效日 10/08；代表端點可恢復，支持排程時段上游空／非 JSON 回應的判斷。原失敗 body 未保存，仍無法區分空 body 與 HTML/其他非 JSON；程式已補寫 status 以外的 content-type、Content-Length、實收 bytes（不記錄 body），等待下一次自然 failure 提供確證。未手動重跑 DAG。
- TWSE MI_MARGN OpenAPI 清單沒有日期欄；對同一端點分別帶 `date=20261008`、`date=20261009` 的唯讀查詢得到相同 1,298 rows/body hash。舊 adapter 在日期缺失時補上本機台北 `snapshot_date`，所以 10/09 `effective_date` 是合成標籤，不是來源欄位。官方歷史報表核對 10/08 有 1,298 rows、10/09 無資料，且最新報表標 10/08；但 TWSE 使用條款禁止未經同意以腳本下載網站資料，所以沒有把該報表頁接進排程。已保留核准的 `openapi.twse.com.tw` 來源，移除無效 `date` query 與本機日期 fallback；缺來源日期的列 fail closed 為 quarantine。10/09 已保存列數值與官方 10/08 報表的非零餘額一致，差異只在 OpenAPI 空欄對報表明示 `0`；官方報表也說授信機構會於次日調帳。即時唯讀核准 OpenAPI adapter 結果為 HTTP 200、1,040 rows、0 accepted／1,040 quarantined、無 effective date，上市普通股配對 1,040/1,054；這不是自然 source_run 或 DB receipt。signal reader 另要求 margin 日期出現在觀察到的官方行情交易日，避免舊 10/09 合成列在休市日期被當新鮮脈絡。沒有回寫／刪除歷史錯標列，日期欄來源授權仍是缺口。
- 離線 `py_compile` 與 `test_source_extension.py`、`test_source_http_quality.py`、`test_signal_policy.py`：21 passed。沒有手動觸發 DAG、寫入 source tables 或發通知。

## 2026-10-09 20:33 台北：自然批次、訊號與通知回看

- 唯讀快照 [20:33 source/run snapshot](stock_signal_inventory_snapshot_20261009_2033.json) 擷取於 20:32:48；registry 67 keys（daily 23、after-close 32、intraday 10、monthly 2）。各來源有效日、raw/accepted/quarantine/duplicate、上市/上櫃對碼、PIT 與 quality flags 以快照為準；成功任務不等於完整。
- Daily source process 最新自然 run `scheduled__2026-10-07T20:00:00+00:00` 完成 23/23，無 missing key 或 contract drift。10/03 新增的 8 個 `income_statement_*` EPS feeds 合計 1,976 raw/accepted、0 EPS 缺值、0 quarantine/duplicate，全部 effective 2026-06-30，source-feed versions 1,976/1,976。去重後目前普通股主檔配對上市 1,053/1,054、上櫃 892/893；每 feed 僅有 2026Q2，API 無可比期就不算同比，負 EPS 只作反向脈絡。另 `cbc_fx_daily` 任務成功且 3,660 accepted/0 quarantine/duplicate，但儲存的日期只到 2026-09-30；程式未將它讀入 `market_signals.py`，先標為有效日新鮮度待核，不當成當前匯率訊號。
- 最新 after-close `scheduled__2026-10-08T12:00:00+00:00` 有 32/32 key 回報、31 success/1 failed、無 missing/contract drift。TAIFEX 大額交易人 map 26 再次 HTTP 200 但 `JSONDecodeError`、0 accepted、無 effective date；來源失敗不等於部位為零，保留 HTTP 狀態只改善診斷。TWSE/TPEx 收盤行情有效日均為 10/08：TWSE 1,051 accepted、普通股配對 1,051/1,054；TPEx 886 raw、874 accepted、12 quarantined，配對 874/893，其中 7 明確 no-trade、19 檔沒有 accepted 行情列。TPEx 法人流 892 accepted、配對 770/893、122 列在主檔外；資料到達不代表目前方向家族已投票。TWSE margin 當時回報的 10/09 是 adapter 在缺來源日期時補的執行日（1,040 raw、992 accepted、48 quarantined），不能當成官方有效日；細節見上方 23:26 根因追查。
- after-close 的 `trigger_market_signal_notify` 成功，下游 daily child 也成功執行；但 10/09 市場休市，XCom 明確為 `skipped / taiwan_market_closed`，無當日市場覆蓋或方向掃描。盤中來源 10/10 成功；20:30 scan 為 `ready/partial`、universe 0、candidate 0、`candidate_batches_sent=0`，source warning unchanged 且無新 receipt。0 候選不能解讀為當日無訊號。最新 ledger 十筆回執屬兩個較早 run keys，狀態 sent 且 HTTP 200；平台回執不等於人已讀。
- 新聞自然 run `news_ch_collect_process / scheduled__2026-10-09T08:30:00+00:00` 於 18:30–18:41 台北完成：12 feeds、1,208 URLs、1,204 parsed/valid-time、1,178 accepted 並持久化；4 fetch/parse rejects、0 invalid-time quarantine、26 content rejects，重複 0、拒收 reconciliation 通過。9 complete／3 partial（SETN、自由時報、鉅亨）；精確時間在 48h 窗內 1,028，date-only 在 cutoff 日後 49、cutoff 日期但時刻未知 23、依日期精度較舊 78、future 0。date-only 不當精確 48h；文章版本仍非不可變歷史 PIT。逐 feed 的拒收與 freshness 分解見 source inventory。
- TPEx 官方 OAS 11627/當沖摘要本批 6 筆、有效日 10/01–10/08，只是市場彙總且要過 freshness gate；TWTB4U 資格與 TWTB5U 個股成交的契約/授權仍分開處理，上櫃個股 endpoint 空回應及授權未知，不接入。Monthly audit 仍 `unknown`：9 筆 legacy source-run rows 缺 dag_id，不能歸屬；最新 scheduled run 是 8/24，9/23 manual initial run 不能替代新自然 monthly lineage。
- Outcomes 無新增成熟 horizon：settled H1 仍只有 9/24 與 10/02 兩個 signal dates，10/02 只有 TPEx next-close 44 rows/12 directional hits、平均超額 -1.119pp；沒有成熟 H5/H10。單日及不同 entry basis/價格來源不作調權依據。
- 本輪沒有新券商原文或批次授權契約可加入研究來源台帳；沒有逐家公司手工研究。只保存唯讀 snapshot 並更新追蹤文件，未改程式、未手動觸發/重跑 DAG、未手動發 Discord、未交易、未新增測試。

## 2026-10-08 20:33 台北：自然來源、新聞與通知回看

## 2026-10-08 20:33 台北：自然來源、新聞與通知回看

- 唯讀快照 [20:33 source/run snapshot](stock_signal_inventory_snapshot_20261008_2032.json) 顯示 registry 67 keys：daily 23、after-close 32、intraday 10、monthly 2。Daily 自然 run 23/23、Airflow success、audit `all_expected_tasks_succeeded_data_review_required`；intraday 自然 run 10/10 成功。After-close 32/32 keys 有回報、無缺鍵／契約漂移，但 31 success、1 failed，audit `partial`；Airflow parent 尚在等待既有法人／分點收集 sensor，尚未觸發本次 daily signal child。Monthly 仍 `unknown`，9 筆 source-run rows 因缺 `dag_id` 不可歸屬。
- 10/03 新增的 8 個季損益/EPS feeds 在 10/08 daily 自然批次再收：1,976 accepted、0 quarantine／duplicate／null basic EPS、accepted versions 1,976/1,976；有效日全為 2026-06-30。八 feed 去重後上市 1,053/1,054、上櫃 892/893；仍只有 2026Q2，沒有同比所需同期。負 EPS 僅反向脈絡。
- 10/08 TPEx 法人流自然批次 892 accepted，770 代碼對上櫃普通股主檔 893 分母，122 代碼在主檔外，effective date 10/08、PIT versions 100%。這不是 892 檔均為普通股訊號。最新日線 signal run 仍 as-of 10/07、`flow_count=0` 且 `flow_readiness_json` 為空；等待中的 after-close child 尚未驗證新流資料的 readiness、計票或通知。
- 收盤行情 effective date 10/08：TWSE 1,051 accepted、0 quarantine，匹配 1,051/1,054；TPEx 886 raw、874 accepted、12 quarantine、0 duplicate，匹配 874/893，其中 7 筆有明確 no-trade，另 19 檔沒有 accepted row。缺列與隔離不補成零成交。兩市場交易限制 feed 的完整快照分別 1,054／893 rows；它們包含明示名單缺席狀態，不代表所有股票都受限。TWSE 注意股來源 HTTP 200 但 0 accepted、無有效日，仍是未知。
- TAIFEX 大額交易人 map index 26 第 2 次仍 `JSONDecodeError`、HTTP 200、0/0/0/0、無 effective date；保留 HTTP 狀態改善診斷，不代表來源恢復。20:43 台北只讀重查 after-close DAG 仍為 `running`，`wait_existing_equity_order_flow_collection` 為 `up_for_reschedule`，設計上等待既有收集完成或到 21:30 TPE 後才放行；本次不手動解除或觸發。
- 最新 intraday run 20:30 台北為 `ready/partial`、0 candidates，沒有新 receipt。較早 10/08 intraday run 有 3 candidates，通知 ledger 有 3 筆 HTTP 200 receipt；receipt 不代表人已讀。最新 daily scan 仍是 10/07 的 0 candidates、`source_status=partial`；不得把資料不足解讀為無交易機會。
- 最新新聞自然 run `news_ch_collect_process / scheduled__2026-10-08T08:30:00+00:00` 於 18:30–18:42 台北完成：12 feeds、1,316 URLs、1,312 parsed、1,311 valid time、1,288 persisted；4 fetch/parse rejects、1 invalid-time quarantine、23 content rejects；8 complete／4 partial，逐來源拒收差額已對平。以原發稿欄位觀察時刻 18:42:25、48h cutoff 10/06 18:42:25 計，1,138 精確 timestamp 在窗內、67 date-only 僅能確認日期在 cutoff 後、8 筆位於 cutoff 日期但時刻不明、75 按日期精度較舊、0 future。日期精度不等於精確 48h，也未把轉載當獨立佐證。signal reader 用 `create_time <= scan cutoff` 擋併發新列，但舊文章重抓會覆寫該欄；新聞表沒有不可變正文版本，故無法聲稱有完整歷史 PIT/revision lineage。
- Outcomes 沒有新增成熟 horizon：H1 仍只有 9/24 與 10/02 兩個信號日期；10/02 TPEx next-close 44 rows/12 directional hits、平均超額 -1.119pp，其他 10/02 basis/上市列與 H5 仍 pending，無成熟 H10。維持固定門檻，不用單日結果調權。
- 新增一份可讀的國泰投顧公開市場評論樣本，方法台帳記錄於 `stock_signal_research_sources.json`：ROE／應計比率／槓桿構成的品質分數可作研究假說，但這是全球市場／基金評論，不是台股個股 EPS 或估值模型，不能投方向票或接成 feed。
- 本次只讀 Docker/Airflow/DB/XCom 狀態並保存 snapshot；未手動觸發／重跑 DAG、發 Discord、交易、調分或改 OAuth。未改無關既有 dirty worktree。

## 2026-10-07 22:40 台北：研究台帳、持續範圍與失敗診斷改善

- 使用者明確要求持續搜尋/優化、可瀏覽券商報告、整合新聞及把盤點寫記憶。沿用既有平日 20:30 heartbeat，新增公開原文方法抽查；全市場批次仍為重心，不把個別報告抽查當批次 feed 或方向投票。
- 重新執行唯讀自然 run inventory：65 approved/enabled sources（intraday 10/daily 23/after-close 30/monthly 2），日線仍 `ready/partial`、1,920/1,947 價格、方向 flow 0、0 candidates；22:30 盤中仍 0 候選。flows_by_code 明確未載入 legacy 表，新的 TPEx 官方 flow 作背景，不能解除這個驗證邊界。月頻兩來源缺 dag_id 可歸因性，audit `unknown` 而非成功。
- 收盤後 TAIFEX 大額交易人兩次 JSONDecodeError；22:38 左右唯讀 preflight 回 HTTP 200、可解析 1,386 rows，僅證明當次 endpoint 可讀，未回填自然失敗批次。修正 generic JSON fetch：JSON/編碼解析失敗保留 `exc.http_status`，沿用 registry 持久化/通知路徑，不保存 response body。11 個針對性與既有來源測試通過；生產自然驗收仍待下一次排程。
- 五家券商官方入口抽查及 TEJ 方法/API 證據已記 `stock_signal_research_sources.json`；凱基 PDF 日期/題目與入口摘要不同、永豐 2023 PDF 僅作歷史方法、富邦需登入、國泰 detail 重導、元大入口未得原始研報。論據品質和自動准入分開，不以題目猜作者動機或宣稱有效。未保存受限全文。
- 完整交付/缺口順序見 `stock_signal_research_brief_20261007.md`；精確 read-only query/audit snapshot 見 `stock_signal_inventory_snapshot_20261007_2245.json`。沒有手動 DAG/Discord/交易/門檻改動。

## 2026-10-07 20:30 台北：自然收集、日線/盤中訊號與新聞回看

- 本次僅回看自然排程與其資料庫/XCom/平台回執。After-close `source_extension_collect_after_close / scheduled__2026-10-06T12:00:00+00:00` 在 10/07 20:00 自然執行；30/30 source keys 均回報、無 missing key/contract drift，但 audit 是 `partial`。`taifex_futures_large_trader_positions_daily` 的 map index 24 兩次嘗試都在 JSON 解析失敗（`JSONDecodeError: Expecting value`，HTTP status 未記錄），raw/accepted/quarantine/duplicate 為 0/0/0/0、無有效日；驗證 task 因 `24:failed` 失敗。其餘有 accepted rows 的成功來源 PIT version coverage 為 100%；TWSE 空回應來源沒有 accepted rows，PIT 為 N/A。這是來源回應解析缺口，不能推成大額交易人部位為零。
- 收盤行情有效日 10/07，上市行情 1,051 raw/accepted，上市普通股對碼 1,051/1,054；日線可用價格 1,047/1,054（99.34%），另 4 列收盤無效。上櫃行情 887 raw、878 accepted、9 quarantine，對碼 878/893（98.32%）；可用價格 873/893（97.76%），另 5 個 accepted row 有明確 no-trade 標記，15 個普通股沒有 accepted quote row。缺列/隔離都保留 unknown，不補零量。兩市場指數與總報酬指數均對齊 10/07。
- After-close child 雖由 parent trigger 成功啟動，仍是 `ready/partial`，as-of 10/07：universe 1,947；price 1,920；flow 0；revenue 1,945；financial 1,903；borrow 1,795；insider 1,939；dividend 1,944；candidate 0。上市/上櫃 flow 覆蓋皆 0，故 0 候選不是無訊號或偏空結論。自然通知 ledger 有 1 筆 HTTP 200 回執；這是資料不足/來源品質通知，不是方向卡，也不是本次手動發送。
- 盤中 parent `source_extension_collect_intraday / scheduled__2026-10-07T12:15:00+00:00` 10/10 tasks 成功、audit `data_review_required`；兩市公告有效日 10/06。月營收仍是 2026-08-01；TPEx revenue 892 accepted、891/893 對碼，TWSE 1,086 accepted、1,054/1,054 上市對碼。三個空白 transfer sentinel 均隔離，不能推為零事件。盤中 child 為增量掃描，0 candidates、0 候選卡批次、source warning `unchanged` 且沒有新 receipt 確認。
- 10/03 新增的 8 個季損益/EPS feeds 已有第四次自然 daily run：1,976 raw/accepted、0 quarantine、0 duplicate；基本 EPS 欄位缺值 0，8 feed source-feed versions 1,976/1,976（100%），有效日皆 2026-06-30。合併目前普通股主檔對碼為上市 1,053/1,054、上櫃 892/893。各來源只有 2026Q2，API 沒有可比同季時不作同比；負 EPS 仍只作反向脈絡，不新增方向家族。
- 新聞自然 run `news_ch_collect_process / scheduled__2026-10-07T08:30:00+00:00` 實際 18:30–18:40 台北：12 feeds、1,385 URLs、1,383 parsed/valid-time、1,369 accepted 並持久化；2 fetch/parse rejects、0 invalid-time quarantine、14 content/duplicate rejects；9 feeds complete、3 partial（中央社 2 fetch rejects、自由時報 13 content rejects、鉅亨網 1 content reject）。48h observed/cutoff 為 10/07 18:40:44 / 10/05 18:40:44：1,219 精確時間在窗內，另有 56 date-only 在 cutoff 日後、18 cutoff 日時刻不明、76 按記錄精度較舊、0 future。date-only 不視為精確 48h。
- 當沖 eligibility 最新 effective date 是 TWSE 10/06（1,227 accepted；971/1,054 對碼，256 unmatched）及 TPEx 10/07（841 accepted；725/893 對碼，116 unmatched）；資格清單不是成交量。TPEx OAS 11627 本次 4 筆市場彙總，日期 10/01–10/06，仍需 freshness gate，不能當個股訊號。TWTB4U/TWTB5U 契約/授權缺口、TPEx 個股 endpoint 空資料與授權未確認仍未解，沒有接入未核准個股查詢路徑。
- Mature outcomes 仍只有 H1 settled：9/24 legacy signal-close 81 列/50 hits、平均超額 +1.776pp；next-close 81/37、+0.987pp。10/02 TPEx-only next-close 44/12、-1.119pp，仍是單一日期；同日 signal-close 44 列仍 pending-data。沒有成熟 H5/H10；不因單日 outcome 改分數或門檻。
- 本次沒有逐家公司券商報告調查、沒有手動觸發/重跑 DAG、沒有手動發 Discord、交易、程式或分數/門檻修改，也沒有跑測試。完整逐來源 effective date、列數、配對與 PIT 明細見 `docs/stock_signal_source_inventory.md` 本節。

## 2026-10-06 20:30 台北：自然日線、盤中與自動來源回看

- 本次只檢查已排程的批次來源、資料庫回執和掃描快照，沒有逐家公司搜尋或手工評比券商報告。After-close parent `source_extension_collect_after_close / scheduled__2026-10-05T12:00:00+00:00` 於 10/06 20:00–20:02 台北自然執行；30/30 預期來源回報、無 missing key、無契約漂移，accepted 列 PIT version coverage 為 100%，但整體 audit 仍是 `data_review_required`，不能以 Airflow success 宣稱全市場完整。
- 同日行情備援已由自然批次證實：TWSE CSV fallback effective 10/06，1,052/1,052 accepted、0 quarantine，上市普通股 1,052/1,054（99.81%）；TPEx byte-range 回應 HTTP 206 且完整性驗證後正常解析，889 raw、872 accepted、17 quarantine，接受代碼 872/893（97.65%）。掃描可用收盤 865/893（96.86%），另 7 列有明確零成交標記；21 檔普通股沒有接受行情列，仍是 unknown，不能補成零量或無交易。上市與上櫃市場基準都 effective 10/06 並與掃描日對齊。
- After-close child `market_signal__daily__source_extension_collect_after_close__scheduled__2026-10-05T12:00:00+00:00` 為 `ready/partial`、as-of 10/06：universe 1,947；price 1,917；flow 0；revenue 1,945；financial 1,902；borrow 1,795；insider 1,939；dividend 1,944；candidate 0。上市價格覆蓋 99.81%，上櫃 96.86%；法人 flow 為 0，所以 0 候選不能解讀為沒有方向訊號。系統自然送達 1 筆「資料不足」通知且 receipt HTTP 200；這是 pipeline 自動回執，不是方向卡或本次手動發送。
- 最新 intraday parent `scheduled__2026-10-06T12:15:00+00:00`（台北 20:30–20:32）10/10 source tasks 成功、audit `data_review_required`；child 為 `ready/partial`、0 candidates、候選批次送出 0、來源品質提醒狀態 unchanged 且沒有確認新 receipt。它仍是事件增量掃描，不代表日線無訊號。
- 10/03 新增的 8 個季損益/EPS feed 已經歷三次自然 daily 收集；10/06 04:00 第三批合計 1,975 accepted、0 quarantine、0 duplicate、effective 2026-06-30，8 feed 的 source-feed version lineage 全數 100%。基本 EPS 缺值 0；TWSE `fh` 有一次 0 列 failed attempt，重試成功 13/13。合併目前普通股主檔為上市 1,053/1,054、上櫃 891/893；主檔外代碼仍 unknown。每 feed 只有 2026Q2，無可比前年同期，不作同比；負 EPS 只列反向脈絡，不加方向家族。
- 成熟 outcomes 有新增的上櫃 H1 `next_close` 44 列，來自單一訊號日 10/02；12/44 directional hits、平均超額報酬 -1.119pp，僅作單日描述且未扣交易成本。9/24 legacy H1 仍有 signal-close 81 列/50 hits、next-close 81 列/37 hits；仍無成熟 H5/H10。樣本日期少且 outcome 價格來源不一，不因單日表現調分數或門檻。
- 當沖 eligibility 今日自然收集為 TWSE 1,228 accepted（972 個上市普通股對碼，256 unmatched）及 TPEx 843 accepted（727 個上櫃普通股對碼，116 unmatched）；資格名單不是成交量。TPEx OAS 11627 僅 3 筆市場彙總、有效日 10/01–10/05，尚未對齊 10/06 掃描；不作個股訊號。上市 TWTB5U entitlement/機器交付使用權仍未確認，上櫃 individual endpoint 仍空資料且授權不明，均未接入。
- 最新新聞自然批次 logical 16:30、實際 18:30–18:39 台北：12 feeds、1,372 URLs、1,365 parsed/valid-time、1,339 accepted 並持久化；7 fetch/parse reject、26 空白內容 reject、0 invalid-time quarantine；8 complete、4 partial。48h cutoff 為 10/04 18:39 台北；1,189 篇有精確時間落在窗內，另 56 篇只有日期精度、不能當小時級新鮮度，94 篇依已記錄精度較舊。新聞仍只作有時間界限的背景，和來源事實、分析、未知事項分開。
- 本次只更新追蹤文件與授權的 ad-hoc memory note；沒有改程式、分數、門檻或方向家族，沒有手動觸發/重跑 DAG、沒有手動發 Discord、交易或跑測試。逐來源有效日、列數、配對及 PIT 明細見 `docs/stock_signal_source_inventory.md` 本節。

## 2026-10-05 22:37 台北：補同日官方指數與分清 TPEx 零成交

- 已將官方資料集 11549、11755、11871 的 CSV 備援接到既有平日 20:00 after-close 收集來源；TWSE 個股日行情、TAIEX 收盤指數和總報酬指數只有在主端點失敗或日期落後時才取同日 CSV。保留來源授權、欄位映射與有效日檢查，非同日資料不會混入當日掃描。
- 只讀呼叫現行 adapter：三個 TWSE 備援 HTTP 200、有效日 10/05，收盤行情 1,052 列、TAIEX 指數 3 列、總報酬指數 3 列；adapter 正規化均接受。沒有寫 DB、沒有手動觸發 Airflow。
- TPEx 10/05 即時唯讀預檢 888 列：現行正規化可接受 876、隔離 12；已分出 5 列官方明確零成交、1 列有效收盤但變動欄為「除息」、12 列收盤/變動都缺值。已知零成交不當成壞資料或有效收盤；有收盤但沒有數字變動時不猜方向；其餘缺值仍未知。這是新程式的記憶體預檢，資料庫自然 run 目前仍是舊的 870 accepted/18 quarantined。
- 下一次自然 after-close 才會用新契約收錄到 PIT/version/source-run，且完成上市/上櫃行情與基準日對齊後重跑訊號及 pending outcome。H5/H10 尚未成熟的訊號不會提前結算。TWSE 個股法人/外資持股的正式授權仍未具備；TPEx 缺 close 的 12 列、4 檔未回傳代碼不能由其他欄位推造。細節見 docs/stock_signal_source_inventory.md 最新盤點。
- TradingAgents 只參考 point-in-time 資料與按固定持有期對市場基準結算的驗證方式，沒有移植其 LLM 分析或交易決策。[TradingAgents repository](https://github.com/TauricResearch/TradingAgents)

## 2026-10-05 21:07 台北：修正 TWSE 同日行情缺列與 0 候選提示

- 根因：TWSE `STOCK_DAY_ALL` 在 10/05 21:07 仍回 HTTP 200，但資料日期停在 10/02；舊 adapter 只檢查傳輸與列型別，沒有檢查有效日，故把舊行情當成功。最新 daily snapshot 因此上市行情 0/1,054、上櫃 870/892、候選 0、`source_status=partial`，不能解讀成沒有訊號。
- `adapters.py` 改用資料集 11549 明列的授權 CSV 資源 `STOCK_DAY_ALL?response=open_data` 作 TWSE 同日備援，只在 OpenAPI 主端點錯誤或有效日落後時查詢；核對 CSV 欄位與最新有效日等於掃描日後，才正規化目前上市普通股。若當日無資料或備援失敗，留用舊列並維持 partial，不補零。先前檢查的另一個官方網頁查詢端點不納入程式來源。
- 授權 CSV 唯讀核對：OpenAPI 主路徑 10/05 21:07 仍為 10/02；資料集列出的 CSV 資源回 HTTP 200、1,381 列、有效日 `1151005`。adapter 記憶體內重跑得到 1,052 accepted、0 quarantine、有效日 10/05，覆蓋 1,052/1,054（99.81%）；2 檔無列仍是 unknown。未寫 DB、未回填舊 snapshot。
- `market_signals.py` 將來源不完整的零候選摘要改成「資料不足，無法確認；0 檔不代表沒有訊號或偏空」，並在標題標示資料不足。其他候選門檻、方向分數和家族均未改。TradingAgents 參考僅沿用 PIT、分項證據、固定持有期基準結算的評估原則，不加入模型自行產生買賣建議。[TradingAgents repository](https://github.com/TauricResearch/TradingAgents)
- TWSE 的同日補收尚未由自然 `source_run` 驗證，10/05 既有 daily snapshot 仍為 partial；下一次自然 after-close 收集時才會 upsert 新 registry contract 並持久化資料。沒有手動觸發 DAG、送 Discord、交易或改門檻；未跑測試套件。靜態解析、adapter 唯讀預檢及摘要函式輸出檢查完成。

## 2026-10-05 20:53 台北：確認八個官方季損益/EPS feed 首次自然收集

- 交付維持全市場、可排程、可重複執行的批次訊號；本次沒有逐家公司搜尋或人工比較券商報告。10/03 新增的 8 個 TWSE/TPEx 季綜合損益與基本 EPS feed，首次自然 daily 收集是 `source_extension_collect_process / scheduled__2026-10-02T20:00:00+00:00`，10/04 04:00 台北開始。8 個 source_runs 全成功，合計 1,975 raw、1,975 accepted、0 quarantine、0 duplicate；各 feed effective date 為 2026-06-30，正規化 EPS 必填欄位缺值 0。逐 feed 列數、目前普通股主檔配對及第二輪版本化狀態見 `docs/stock_signal_source_inventory.md`。
- 首次 run 的 accepted rows 有 persisted `created_at`，但沒有任何 `source_record_versions` 以該 run lineage 捕捉；不把後一輪版本倒填成首次 run 的 PIT 證據。下一次自然 daily run `scheduled__2026-10-03T20:00:00+00:00`（10/05 04:00 台北）才為 1,975 accepted rows 建立 source-feed versions，逐來源版本覆蓋 100%。八個來源各只有 2026Q2 一個已存比較期；暫不計同比。卡片中的負 EPS 維持反向獲利脈絡，不另立方向家族、不加分。
- 10/05 20:00 after-close parent Airflow success，但 audit 為 30 預期/28 回報、`partial`；缺 `twse_director_shareholding_shortfall_daily`、`tpex_director_shareholding_shortfall_daily`，留待 10/06 下一次自然收集。daily child 20:02 success、as-of 10/05，source `partial`：上市價格 0/1,054（TWSE 行情有效日 10/02）、上櫃 870/892，法人方向列 0、候選 0；此快照不能解讀成沒有訊號。排程 child 自有通知 ledger 有一筆 platform receipt、HTTP 200；不是本次手動發送。
- 最新 intraday parent `scheduled__2026-10-05T12:30:00+00:00` 20:45:00–20:47:51 台北完成，10/10 task success、audit `data_review_required`。TWSE 月營收 1,086/1,086、1,054/1,054 上市普通股；TPEx 892/892、891/892 上櫃普通股、1 筆 unmatched。兩市公司事件有效日 10/04；內部人預定轉讓各 1 筆接受、effective 10/02；未轉讓來源各 1 筆 sentinel quarantine，不能解讀成零事件。20:47:46–20:47:56 intraday child success、`ready/partial`、0 candidates，通知 `sent=0`。
- 最新新聞仍為 18:30–18:39:19 台北自然批次：12 feeds、1,329 URLs、1,327 parsed/valid-time、1,304 accepted、2 fetch/parse rejects、23 blank-body/content rejects、0 invalid-time quarantine；9 feeds complete、3 partial，入庫回執 1,304 rows、`create_time_tpe=18:39:17`。48 小時 cutoff 10/03 18:39:14；日期精度邊界維持 unknown，不合成時分。
- 成熟 outcomes 仍只有 9/24 H1 已結算訊號日：signal-close 81 settled、50 筆 10/02 資料仍 pending；next-close 81 settled；無成熟 H5/H10。已結算列仍來自單一 `legacy_stock_table`，不據單日 cohort 調門檻。TPEx 當沖最近只有 10/01–10/02 兩列市場彙總，不能當個股訊號；TWTB5U 授權與自動交付、TPEx 個股當沖 endpoint 授權/空資料缺口維持原紀錄。
- 未手動觸發/重跑 DAG，未發 Discord，未交易；本次只做唯讀 runtime 稽核並更新追蹤文件與 ad-hoc memory note。

## 2026-10-05 20:23 台北：自然收盤回執與全自動來源擴充

- 重新校正交付範圍：只做可排程、可批次收集、能反覆量化驗收的市場訊號；不做逐家公司券商報告調查。官方 TWSE/TPEx 稀疏「董監持股不足法定成數」批次端點已新增到來源契約、正規化、`daily_after_close` registry 與收盤候選卡背景。資料只會附在已有市場候選上，不單獨發事件卡、不增加方向分數；缺席仍為 unknown。60 日門檻只代表資料過舊須 review。
- 兩個官方資料集標示免費與政府開放授權第 1 版；TWSE OpenAPI 路徑為 `/v1/opendata/t187ap08_L`，TPEx 為 `/openapi/v1/mopsfin_t187ap08_O`。匿名唯讀端點預檢各回 12、15 列，兩邊出表日均為 2026-08-19；這是原始 endpoint 預檢，不是 Airflow 接受列或 PIT 收集回執。TWSE 開放資料中文/英文頁更新頻率文字不一致，TPEx 頁列不定期更新；因此設每日輪詢、60 日 review gate，不聲稱每日新鮮或完整市場覆蓋。[TWSE dataset 20767](https://data.gov.tw/dataset/20767)、[TPEx dataset 20768](https://data.gov.tw/dataset/20768)、[TWSE OpenAPI](https://openapi.twse.com.tw/)、[TPEx OpenAPI](https://www.tpex.org.tw/openapi/)
- 今日自然 after-close `source_extension_collect_after_close / scheduled__2026-10-02T12:00:00+00:00` 20:00–20:02 台北執行、Airflow parent success。用目前 registry 定義唯讀重稽核為 30 預期、28 回報，缺兩個新 statutory-shortfall keys，狀態 `partial`；今天這輪沒有收進新 feed。20:15 的 intraday schema/catalog 工作已令 scheduler catalog 現有 30 keys，但不回頭補跑收盤 DAG。明天 10/06 20:00 才是新來源下一次自然收集。
- 已回看 28 個來源各自 raw/accepted/quarantined/duplicate、effective date、對碼與適用覆蓋分母；逐來源表在 `docs/stock_signal_source_inventory.md`。關鍵品質點：TWSE 日行情 1,051/1,051 accepted、有效日 10/02，普通股主檔覆蓋 1,051/1,054（99.72%）；TPEx 日行情 888 raw、870 accepted、18 quarantine、有效日 10/05，覆蓋 870/892（97.53%）。台股兩市場行情尚未同日對齊；指數 TWSE 10/02、TPEx 10/05。TPEx 個股法人流 916 accepted、有效日 10/05，795/892（89.13%）對碼，121 筆 unmatched 保持 unknown；TPEx 當沖 2 列只是市場彙總，不作個股訊號。TWSE 注意股 endpoint 是 0 列並標 `empty_response_requires_source_semantics`，不可推成零注意股。
- After-close 子 run `market_signal_notify` 成功，as-of 10/05；daily XCom 為 universe 1,946、上市有效價格 0/1,054、上櫃 870/892、法人流 0、候選 0、`source_status=partial`、benchmark partial。XCom 記 `sent=1/failed=0`，但這不是本次手動發送，也未將該欄位當成外部收件回執。成熟 outcomes 唯讀查詢仍只有 9/24 的 H1（signal-close / next-close 各 81 列），無 H5/H10；沒有用這一筆新結果改分數或門檻。
- 最新新聞自然 run `news_ch_collect_process / scheduled__2026-10-05T08:30:00+00:00` 18:30–18:39：12 feeds、1,329 URLs、1,327 parsed/valid-time、1,304 accepted、2 fetch/parse rejects、23 空白內文隔離、0 invalid-time quarantine；post-time reconciliation 通過。48 小時精確時間窗內 1,141 列；另 30 列只有 cutoff 後日期、5 列是 cutoff 日但無法確認時刻，128 列依來源時間精度屬舊資料，未來時間 0。Feed 主要缺口是中央社 1 fetch/parse reject、自由時報 23 blank-body reject、鉅亨網 1 fetch/parse reject。
- 未手動觸發/重跑 DAG、未查券商單家公司、未執行測試、未手動發 Discord 或交易。程式改動已做 AST/registry 靜態驗證，等待下一自然收盤批次驗新 feed 接受/隔離/重複、PIT、對碼和日期。

## 2026-10-05 16:51 台北：盤中來源、收盤 catalog 與新聞最新自然批次

- 最新盤中 parent `source_extension_collect_intraday / scheduled__2026-10-05T08:15:00+00:00`（16:15 logical；實際 16:30:00–16:30:36）10/10 tasks 成功，但來源 audit 仍是 `data_review_required`。TPEx/TWSE 公司事件為 2/2、3/3，資料有效日 10/04；TWSE 有 1 筆代碼未配普通股主檔，保留 unknown。planned insider transfers 各 1/1、有效日 10/02；untransferred 各 1 raw、0 accepted、1 blank-sentinel quarantine，不能解讀為零事件。月營收有效日 08/01：TWSE 1,086 accepted、上市普通股 1,054/1,054；TPEx 892 accepted、上櫃 891/892，1 筆 unmatched。均有 100% accepted-row PIT versioning。TWSE forecast variance 7/7、有效日 06/30、5 筆匹配；TPEx endpoint 僅 metadata，無公司事件列。
- 對應 `market_signal_notify` child 成功，但為 event-only；market run `ready/partial`，universe/price/flow/candidates/notified 均為 0。這不代表全市場日線無訊號。最近完整 daily scan 仍為 10/02：universe 1,946、price 1,938、flow 1,891、revenue 1,945、financial 1,902、borrowing 1,795、insider 1,944、dividend 1,053、50 candidates、0 notified。成熟 `market_signal_outcomes` 仍只有 9/24 H1：signal-close 81 rows/80 directional/50 hits，next-close 81/80/37；無 H5/H10，不據此改方向分數或門檻。
- 唯讀呼叫 scheduler `discover_approved_sources("daily_after_close")` 現回傳 28 個核准 keys，含 TDCC 與上市/上櫃大股東新增來源；這只證明 runtime catalog 已有登記。10/05 20:00 after-close 還未到期，最新實際 parent 仍是 10/02 的 failed run。舊 `source_runs` 有 17 筆、15 keys 缺 `dag_id`，所以那些列的 DAG 歸屬 audit 為 unknown；不拿它們推斷本次收集狀態，也不手動觸發 DAG。
- 最新新聞自然 run `news_ch_collect_process / scheduled__2026-10-05T06:30:00+00:00`（16:30 logical，實際 16:30:02–16:39:13）：12 feeds、1,247 URLs、1,246 parsed、1,216 有有效時間、1,197 accepted；1 fetch/parse reject、30 invalid-time quarantine、19 content/duplicate rejects，reconciliation 與 post-time filter 均通過。Freshness observed at 16:39:08，48 小時 cutoff 為 10/03 16:39:08 台北。SETN 30/30 因無法以列表相對時間佐證時間而 quarantine；CNA 100 URLs/99 accepted、1 fetch/parse reject；LTN 96 parsed/78 accepted、18 blank-body rejects；CNYES 150 parsed/149 accepted、1 blank-body reject且另有34列較舊；iThome 與 TNL 多為舊資料或日期精度，其他 feeds 整體大致完整。不可只用 parent success 宣稱所有 feed 完整。
- SETN 的 8 篇自然批次文章頁唯讀核對中，JSON-LD `datePublished` 都帶 `+00:00`，但換算成真正 UTC 會落在未來；以台北 wall time 解讀後，8 篇列表相對年齡都比 JSON-LD 年齡少約 3.47 分鐘。此批證據支持只把分鐘標籤 corroboration 上限由 +3 放到 +5 分鐘，保留列表佐證、下限與其他時間規則；修正尚待下一個自然新聞批次驗證，不因單批調整訊號分數或新聞品質門檻。
- 本次只有唯讀檢查、窄幅 parser 改動與文件更新；未跑測試、未觸發/重跑 DAG、未寫業務 DB、未發 Discord 或交易。

## 2026-10-05 16:25 台北：新增官方上市/上櫃大股東名單批次來源

- 從官方開放資料目錄與 TWSE/TPEx OpenAPI 找到兩個匿名可讀、無查詢參數的每日批次名單端點：上市 `GET /v1/opendata/t187ap02_L`（資料集 [18422](https://data.gov.tw/dataset/18422)）與上櫃 `GET /openapi/v1/mopsfin_t187ap02_O`（資料集 [18423](https://data.gov.tw/dataset/18423)）。官方欄位含出表日、公司代碼/名稱、大股東名稱；資料集標示政府開放授權第 1 版。10/05 唯讀實收：TWSE 174 列、日期 10/04；TPEx 386 列、日期 10/05。日期不一致照實保留，不推測發布時刻或補成同日。
- 兩個來源只列官方揭露持股超過 10% 的股東名稱，沒有股數或精確持股比例。已在 allowlist/registry 加入 `twse_major_shareholders_daily`、`tpex_major_shareholders_daily`，每日收盤排程、5 日有效日 freshness gate、保留 PIT 快照。新事件型別 `major_shareholder_over_10pct` 僅作名單身分脈絡；名單缺席是 unknown，不代表持股為零或股東出場；未進方向分數/通知。
- 這是稀疏 disclosure，不計算普通股全市場覆蓋百分比，也不把列數當全市場覆蓋。每次自然收集需核 effective date、raw/accepted/quarantined/duplicate、接受代碼與主檔 unmatched 數；確認兩市場當日名單是否全量的來源契約仍須依官方欄位/發布行為持續核對。當前 Airflow DB catalog 尚待 10/05 20:00 自然收盤流程自動 bootstrap；程式定義預期收盤來源由 26 增至 28，尚非當日實收回執。
- 改動僅加批次來源登記、正規化與稽核 freshness，不改訊號權重/門檻。待自然 run 驗 endpoint 實收、PIT versions 和品質列；沒有手動觸發 DAG、寫業務 DB、發 Discord 或交易。

## 2026-10-05 16:20 台北：TDCC 官方產品文件支持週變化特徵假說

- TDCC e 手掌握官方手冊描述「股權分散數據連增排行榜」，依指定持股區間看持股比例連續增加週數；同手冊也描述近 4 期分布圖和近 12 期趨勢圖。這為全市場週資料可衍生的特徵提供產品依據，但不是 alpha 證據。[官方手冊](https://epassbook.tdcc.com.tw/ufile/o568abppxfmh9p9de3ukdxidqpp9nds6.pdf)
- 可檢驗假說：用 TDCC OpenAPI 全表和 PIT 週快照，自行計算同一證券指定級距的 `share_of_custody` 週增減及連續上升週數，再和週後 1/5/10 交易日的成本後超額報酬比較；先核對級距完整映射、除權息/股本變化和證券存續偏差。只用正式授權全市場 API，不調用單檔網站查詢路徑；累積多週自然 versions 和成熟 outcomes 前不投分、不調門檻。
- 手冊允許選擇/調整持股區間，但 OpenAPI 只公開分級序號及各級人數、股數、集保比例，沒有完整級距對照；不能將級距編號簡化成已核准的「散戶/大戶」分類。

## 2026-10-05 16:11 台北：新增 TDCC 官方週頻全市場持股分布自動來源

- 校正工作焦點為自動化來源與可驗證結果，不做逐家券商人工調查。TDCC OpenAPI 規格列 `GET /v1/opendata/1-5`，無查詢參數/分頁；匿名唯讀實收 HTTP 200、69,326 列、4,078 個代碼，effective date 2026-10-02，各代碼目前觀察到 17 個分級列。政府資料集標示免費及政府開放授權第 1 版。
- 本地普通股主檔覆蓋：上市 1,054/1,054；上櫃 890/892。兩檔沒有官方快照列，維持 unknown/coverage gap，不補零。資料內非普通股或未匹配代碼保存為 security，不讓其混入股票覆蓋或評分。
- 已修改程式 allowlist/source definition、欄位正規化及收盤來源稽核，新增 `tdcc_shareholder_distribution_weekly`。Airflow DB registry 尚待下次自然 run 的 catalog task 自動 bootstrap。每個成功自然收集須核 effective date、raw/accepted/quarantined/duplicate、PIT versions、普通股分級完整性與上市/上櫃覆蓋；10 日 effective-date freshness 超期會標 review；兩市場任一未滿 100% 會標 coverage incomplete。欄位分級僅保存原始序號/數值，尚未作方向訊號。
- 更新節奏有官方文件差異：data.gov.tw metadata 寫每月，TDCC 個股說明寫每週最後營業日收盤後；採每日收盤排程檢查的方式觀察官方週快照，但有效日 gate 仍須通過，不能把重複舊快照當新日資料。分級門檻未由 OpenAPI schema 說明，需再核官方定義並累積多週 PIT/outcomes 後再評估特徵。
- 最近完整日線仍為 10/02、成熟 outcomes 仍只有 9/24 H1；after-close 10/05 20:00 run 尚未到期。等待自然 run 核 registry bootstrap 與新 key 實收，不手動觸發/重跑，不發 Discord 或交易。

## 2026-10-05 15:55 台北：官方批次法人流與當沖量能來源核查

- 重新載入 Airflow after-close registry，25 個 keys 仍只有上櫃個股法人 `tpex_institutional_flow_daily`、上櫃外資持股 `tpex_foreign_investor_holdings_daily`，以及市場彙總當沖 `tpex_daytrade_market_summary_daily`；沒有 TWSE 個股法人 flow 或個股當沖量能 source key。
- 官方目錄確認 TPEx 個股法人日資料 [11856](https://data.gov.tw/dataset/11856) 為免費、每日、Government Open Data License v1；上市個股法人 [136987](https://data.gov.tw/suggests/136987) 和 [137032](https://data.gov.tw/suggests/137032) 仍是開放資料建議，並非可排程 feed。TWSE OpenAPI [Swagger](https://openapi.twse.com.tw/) 列有產業外資持股與前 20 名彙總；TWTB4U 是 eligibility/restriction 欄位，不能替代個股法人買賣或當沖成交量。
- 找到 TWSE E-Shop [TWTB5U 個股當沖成交統計](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)作為可量化缺口候選：個股當沖股數及買賣金額、約 20:00 的每日 XLS；T/T+1 數可變更，T+2 才是最終數。費用內用 NT$1,500/月、外部資訊用戶 NT$3,000/月。官方[訂購條款](https://eshop.twse.com.tw/zh/shopping/finishOrder?show=true&showTIP=)限制使用和轉傳，且不保證正確性/完整性；公開商品資訊未確立機器 API、email 自動交付 SLA 或私有 Discord 的使用權。本專案尚無授權/自動交付證據，故不購買、不接入；如取得授權，保留 T/T+1/T+2 vintages，先作量能脈絡並用成熟成本後 outcomes 評估方向性。
- TPEx 官方 OAS [11627](https://data.gov.tw/dataset/11627) 是上櫃市場當沖彙總，每日資料但沒有個股列，仍需 freshness gate 且不得當個股訊號。Airflow 目前仍未建立 10/05 20:00 after-close run；待自然排程後核 25 個來源及下游日線/通知，不手動觸發。

## 2026-10-05 15:49 台北：盤中回執欄位已由自然 run 驗證

- 最新自然盤中 run `source_extension_collect_intraday / scheduled__2026-10-05T07:30:00+00:00`（15:30 logical、15:45–15:47 台北實際）10/10 source tasks 成功，source audit 仍為 `data_review_required`，missing/contract drift 皆 0。有效日與前批相同：TWSE 公司事件 10/04、TPEx 公司事件 10/04、planned transfers 10/02、營收 08/01；TWSE 普通股月營收覆蓋 1,054/1,054，TPEx 為 891/892，未配對 1 列保留 unknown。
- 先前新增的 XCom 欄位已在這個自然 child run 出現：`candidate_batches_sent=0`、`source_warning_status=unchanged`、`source_warning_receipt_confirmed=false`；`market_signal_runs` 同 run 是 `ready/partial`、universe/price/flow/candidates/notified 均 0，`market_signal_notifications` 沒有該 run 的 ledger 列。這驗證欄位能區分「本輪沒有候選卡」與「沒有新 warning receipt」；child 是事件增量掃描，不代表全市場無訊號。
- 最新完整日線仍為 10/02、成熟 outcomes 仍只有 9/24 H1（signal-close 50/80 directional hits；next-close 37/80），不調整權重或門檻。after-close 的 10/05 20:00 台北自然 run 尚未建立；預期 run ID `scheduled__2026-10-02T12:00:00+00:00`，留待 20:30 按 mapped source 行數、有效日、上市櫃覆蓋、daily scan/outcomes 與真實 receipt 查核。本次只讀 Airflow/XCom/訊號 DB，未手動觸發或重跑 DAG、未跑測試、未送 Discord 或交易。

## 2026-10-05 15:39 台北：自然批次資料狀態與收盤前基線

- 今日 04:00 台北自然 `source_extension_collect_process` run `scheduled__2026-10-03T20:00:00+00:00` 已完成。唯讀 source audit 為 `all_expected_tasks_succeeded_data_review_required`：23/23 預期及回報來源、missing 0、contract drift 0；這只證明收集任務完成。最新有效日分布仍是季財報 2026-06-30、月營收 2026-08-01、CBC 匯率至 2026-09-30、TWSE/TPEx 當沖資格 2026-10-05、TPEx 股利至 2026-10-02、TWSE 股利至 2026-09-21。TWSE 季獲利率 1,054 raw/1,049 accepted/5 quarantined，1,018/1,054 上市普通股可配對；當沖資格未匹配主檔的 256 上市、116 上櫃代碼維持 unknown。
- 最新盤中來源 run `source_extension_collect_intraday / scheduled__2026-10-05T07:15:00+00:00`（15:15 台北邏輯時間、15:30:25 結束）為 10/10 sources 成功、missing/contract drift 0，但 audit 仍是 `data_review_required`。TWSE 月營收 1,086 accepted、上市普通股覆蓋 1,054/1,054；TPEx 892 accepted、上櫃覆蓋 891/892，1 列未配對保留 unknown；兩市場有效日均為 2026-08-01。兩個未轉讓申報來源各有 1 個空白 sentinel quarantine，不能解讀為零事件。觸發的盤中掃描 universe/price/flow 均為 0、候選 0、notified 0；這是公告增量掃描，不代表全市場沒有訊號。該 run 的通知 XCom 仍只有舊 `sent=0/failed=0` 指標，新 warning 欄位尚待後續自然 run 驗證。
- 最新完整日線 market-signal run 仍是 10/02 `ready/partial`、universe 1,946、price 1,938、flow 1,891、50 candidates、0 notified。`market_signal_outcomes` 只到 9/24 H1：signal-close 81 列/80 有方向/50 hits；next-close 81/80/37；無 H5/H10，不據此調整分數或門檻。
- 最新新聞 run 仍是 `news_ch_collect_process / scheduled__2026-10-05T04:30:00+00:00`（12:30 logical、14:30–14:39 台北實際）：12 feeds、1,180 URLs、1,178 parsed/有效時間、1,159 accepted、2 fetch/parse rejects、19 blank-content rejects、0 invalid-time quarantine/duplicates。48 小時窗內 956 列有精確時間；另有 19 列日期精度較新、5 列 cutoff 日時分未知、179 列較舊，不能將日期精度記錄當小時級 PIT。
- After-close 今日 20:00 台北自然批次尚未建立；最新仍是 10/02 執行的失敗批次。預期 run `scheduled__2026-10-02T12:00:00+00:00` 到期後再核 TPEx 行情分段完整性、25 個 sources 的 effective date/raw/accepted/quarantined/duplicate、普通股覆蓋、daily scan、outcomes 與 receipt。本次查核只讀 Airflow/XCom/訊號 DB，未手動觸發或重跑 DAG、未執行測試、未發 Discord 或交易。

## 2026-10-05 15:31 台北：區分候選卡和來源品質提醒回執

- 新增 [自動訊號來源盤點](stock_signal_source_inventory.md)：以 scheduler runtime registry 核實 25 個收盤 source keys，整理 10 個盤中來源、12 個新聞 feeds、資料/授權缺口，以及「已登記不等於已實收」的限制。
- 唯讀核 `market_signal_notifications` 與關聯 run：最新 event-only natural scan 的 `sent=0` 不等於沒有 HTTP 200 回執。10/05 週末後 interval `source_extension_collect_intraday / scheduled__2026-10-02T14:45:00+00:00` 對應 child XCom 為 0 候選，`source_collection_status=data_review_required`；notification ledger 有一筆 status `sent`、HTTP 200 的來源品質提醒，`market_signal_runs.notified_count=0`。這是來源 warning，不是股票卡。
- 10/04 `source_extension_collect_weekend / scheduled__2026-10-04T00:00:00+00:00` 有 5 個事件候選（3 `company_announcement`、2 `insider_transfer_planned`），5 個方向皆 unknown、`notified_count=5`。通知 ledger 6 rows、2 個不同平台回執（皆 HTTP 200）：一筆來源 warning，另一筆是含 5 個事件候選的批次卡。證明事件卡已送達，不證明交易方向預測有效。
- 找到 XCom 指標語意缺口：舊 `sent` 只計候選卡批次，來源 warning 經獨立 `_send_and_record` 寫入 ledger，不計入 `sent`。已在 `market_signals.py` 做窄幅觀測改良：intraday XCom 加上 `candidate_batches_sent`、`source_warning_status`、`source_warning_receipt_confirmed`；不變更 Discord 發送、去重或 source cursor 行為。scheduler AST parse 通過，workspace/scheduler/worker 檔案 SHA-256 完全一致；新欄位待下次自然 run 驗證，未手動觸發或發通知。

## 2026-10-05 15:15 台北：上市個股法人 flow 的授權機器來源仍缺

- 這輪只查整個資料集/API 的官方目錄與規格，沒有逐家公司查券商材料。官方開放資料平台的 [11856 上櫃個股三大法人明細](https://data.gov.tw/dataset/11856) 標示每日更新、免費、政府資料開放授權第 1 版，並連到 TPEx OpenAPI 規格；本專案 registry 將它登記為 `tpex_institutional_flow_daily`、每日收盤來源、上櫃背景資料，須通過 PIT/覆蓋/成熟 outcome 才能投方向票。今日 after-close run 尚未執行，這次未宣稱來源已收新資料。
- [137032 上市個股法人明細](https://data.gov.tw/suggests/137032) 是資料開放「建議」頁，不是已發布 dataset/API；頁面理由也標示「上櫃有開放，但上市沒開放」。重新讀取 TWSE 官方 [OpenAPI Swagger](https://openapi.twse.com.tw/v1/swagger.json)：共 143 paths，法人相關命中僅 `/fund/MI_QFIIS_cat`、`/fund/MI_QFIIS_sort_20`（產業分類/持股前 20），沒有 T86 個股法人批次路徑。
- 結論是上市法人 flow 仍為授權的市場級來源缺口，資料開放建議的「已回覆/已分派」不等於 feed 已開放。不得接入 T86 網站逐股查詢或推定授權；持續等官方批次資料集/正式授權出現，再檢查全市場有效日、覆蓋、修訂版本與自然 run。現有官方整體市場/期貨部位可作背景，但不冒充上市個股法人訊號。未改程式、訊號權重、通知或 DAG。

## 2026-10-05 15:11 台北：最新盤中自然 run、成熟結果與收盤傳輸根因

- 唯讀查 `source_extension_collect_intraday / scheduled__2026-10-05T06:45:00+00:00`（資料區間 14:45–15:00，實際 15:00:00–15:00:34）：parent 成功，10 個預期/registry/runtime/reported source map 全部成功，缺列、契約漂移、未歸屬列皆 0；source audit 仍為 `all_expected_tasks_succeeded_data_review_required`。逐源 accepted/effective date/隔離/重複和上市櫃配對與 14:41 表相同；TPEx company-events/monthly-revenue 的 HTTP 206 來自現行允許的完整 byte-range 拉取，不能單看 HTTP 206 判成資料截斷。新鮮 XCom 的 revenue 覆蓋仍為 TWSE 1,054/1,054（100%，effective 8/01）、TPEx 891/892（99.89%，effective 8/01），一筆未配對維持 unknown。
- 對應 `market_signal_notify` child 成功，但仍是公告增量：`status=ready/source_status=partial`、來源收集 `data_review_required`、universe/price/flow=0、候選=0、通知 sent/failed=0；沒有股票通知 receipt，不能解讀成全市場沒有交易訊號。直接查 `market_signal_runs`，最新完整 daily scan 仍為 10/02：universe 1,946、prices 1,938、flow 1,891、50 candidates、0 notified；沒有該舊 run 的市場覆蓋快照列。
- 直接查 `market_signal_outcomes`，仍只有 9/24 legacy H1：signal-close 81 列/80 可計方向/50 hits；next-close 81/80/37。尚無 H5/H10 列；不可把未建立列當作成熟結果。今晚 daily 收盤掃描需先驗官方行情和台灣加權/櫃買 benchmark 日期對齊，再結算成熟期數。
- 最新新聞自然 run 仍是 `news_ch_collect_process / scheduled__2026-10-05T04:30:00+00:00`（實際 14:30–14:39）：12 feeds、1,180 URLs、1,178 parsed/有效時間、1,159 accepted；2 fetch/parse reject、19 空白內文、0 invalid-time quarantine/duplicate、post-time reconciliation 全過。48 小時 freshness 分解為 956 精確時間內、19 日期精度 cutoff 後、5 cutoff 日時刻不明、179 舊、0 未來；自由時報空白內文 19/95，未改新聞權重。
- 只讀查 latest after-close parent，仍是 `scheduled__2026-10-01T12:00:00+00:00` failed（10/02 20:00–20:04 實際執行）。map 9 是 `tpex_equity_quotes_daily`，log 顯示連線在 4,193,897 bytes 後中斷，尚差 472,744 bytes；其他 10 個 map 成功，驗收因該 map 失敗。這是自動抓取傳輸問題，不是新訊號。檢視現行 adapter，該 key 已走 allowlist 的 64-KiB byte-range fetch，驗 Content-Range、representation validator，分段最多重試 3 次；workspace/scheduler/worker 的 `adapters.py` SHA-256 一致。今晚自然 run 預期 ID `scheduled__2026-10-02T12:00:00+00:00`，data interval 終點為 10/05 20:00 台北；scheduler `next_dagrun_create_after` 為 20:00，15:11 尚未到期。今晚須以自然回執驗證範圍抓取是否解決傳輸中斷，未手動觸發或重跑。

## 2026-10-05 15:01 台北：再次校正為自動化訊號交付

- 使用者再次指出，逐家公司線上調查券商報告再人工評比彙整，偏離目標。後續只把可排程、可授權、可全市場批次取得的來源與訊號納入建置/驗收；券商材料若沒有已確認授權的機器介面、覆蓋及 PIT 發布版本，只登記為來源缺口，不以人工瀏覽代替自動化進度。
- 工作重心保持在自然排程回執、來源有效日與接受/隔離列、上市/上櫃覆蓋、點時版本、成熟 outcome、候選形成和通知 receipt；成功狀態不等於完整訊號。新聞僅從既有排程來源自動取用並保留發布時間/來源事實，不靠逐篇人工閱讀創造方向票。
- 15:01 台北尚早於今日 20:00 after-close 自然批次及 20:30 檢視時點；本次沒有新收盤資料可驗，未手動觸發/重跑 DAG、未改訊號門檻、未發 Discord 或交易。

## 2026-10-05 14:58 台北：scheduler/worker DAG 載入無錯誤

- Airflow scheduler 與 worker 各自執行唯讀 `airflow dags list-import-errors --output json`，兩邊皆 0 import errors；範圍包含 `source_extension` 和 `market_signal_notify`。結合 14:56 七個核心檔案 hash 一致與 registry 25-key contract 預檢，目前沒有已觀察到的 DAG 載入或主機/worker 版本障礙；外部來源在自然執行時是否完整仍待回執驗證。
- 14:58 仍早於 scheduler 的 10/05 20:00 `next_dagrun_create_after`，預期週五至週一 interval 的 scheduled run 尚未建立。20:30 按 logical run ID/interval/實際執行時刻檢查，不手動觸發。

## 2026-10-05 14:56 台北：收盤 collector 主機與執行容器版本一致

- 唯讀計算 `registry.py`、`adapters.py`、`dag.py`、`audit_runs.py`、`market_signals.py`、`schema.py`、`market_signal_notify.py` 七個檔案 SHA-256；workspace、Airflow scheduler、Airflow worker 三處逐檔完全相同。排除這些核心檔案部署不同版造成今晚 mapped task 行為不一致的可能，但不證明上游資料可用或收集成功。
- 目前仍未到 20:00 建立時間；今晚預期 run logical ID `scheduled__2026-10-02T12:00:00+00:00`、資料區間結束 10/05 20:00 台北。20:30 再核自然 parent/child 與來源品質，不手動觸發。

## 2026-10-05 14:53 台北：釐清 after-close 週末資料區間與 run ID

- 直接在 Airflow scheduler 載入 `source_extension_collect_after_close`：timetable 是 `CronDataIntervalTimetable`、時區 `Asia/Taipei`、DAG 未暫停。最新 run `scheduled__2026-10-01T12:00:00+00:00` 的資料區間為 10/01 20:00 至 10/02 20:00，實際於 10/02 20:00 開始且失敗。scheduler metadata 的下一 logical run 是 10/02 20:00，`next_dagrun_create_after` 為 10/05 20:00；這是週五至週一的資料區間，不是 scheduler 多延遲一天。
- 因此今晚排程建立的 parent run 預期 logical ID 以 10/02 20:00 為起點，資料區間終點才是 10/05 20:00。14:53 尚未到建立時間，尚無該區間 run。20:30 驗收需以 run ID、`data_interval_start/end`、實際 start/end 及 mapped task/child lineage 定位，不要只用 execution date=10/05 過濾。未手動觸發。

## 2026-10-05 14:49 台北：確認今日成熟 outcome 的行情日期閘門

- Outcome 程式使用實際行情日期並支援 H1/H5/H10；既有交易日核對顯示 9/24 `signal_close` H5 預計 10/05 收盤成熟、`next_close` H5 預計 10/06。10/02 的 TWSE 基準指數有效日仍落後 TPEx 一個交易日；今晚結算前需確認兩市場報價與基準指數有效日對齊，否則保留 `pending_data`，不把缺行情轉成輸贏。

## 2026-10-05 14:46 台北：確認收盤 registry 的即時來源映射

- 只讀呼叫 Airflow scheduler 內的 `discover_approved_source_keys(schedule_hint="daily_after_close")`，目前回傳 25 個已核准來源，並通過 runtime contract／必要憑證存在性檢查。映射包含 TWSE/TPEx 個股行情、TPEx 個股法人、TAIFEX 期貨/選擇權法人部位與大額交易人 OI 等；這只證明排程映射可解析，不能代替來源實收、日期/覆蓋/PIT 驗收。
- Airflow 查詢截至 14:46 尚無 10/05 after-close run；最新仍是 10/02 failed。等 20:00 自然排程再驗 mapped tasks 和子 run，不手動觸發。

## 2026-10-05 14:41 台北：自然盤中批次與新聞新 run 驗收

- 唯讀查 Airflow/XCom：`source_extension_collect_intraday / scheduled__2026-10-05T06:15:00+00:00`（14:15 台北邏輯時間、14:30 左右實際收集）10 個預期/註冊/runtime/reported 來源全部成功，missing、契約漂移、未歸屬列皆 0；來源 audit 為 `all_expected_tasks_succeeded_data_review_required`。普通股分母是上市 1,054、上櫃 892。各源 accepted/隔離/重複、effective date、配對如下； accepted PIT 版本覆蓋皆 100%，但這不代表公告源有市場級行情覆蓋：

  | source_key | raw / accepted / quarantine / duplicate | effective date | 普通股配對/覆蓋 |
  |---|---:|---|---|
  | `tpex_company_events` | 2 / 2 / 0 / 0 | 2026-10-04 | OTC 2 |
  | `tpex_insider_transfer_planned` | 1 / 1 / 0 / 0 | 2026-10-02 | OTC 1 |
  | `tpex_insider_transfer_untransferred` | 1 / 0 / 1 / 0 | NULL | 空白 sentinel 隔離，不代表零事件 |
  | `tpex_monthly_revenue` | 892 / 892 / 0 / 0 | 2026-08-01 | OTC 891/892（99.89%）；1 個未配對保留 unknown |
  | `tpex_quarterly_forecast_variance_events` | 1 / 1 / 0 / 0 | 2026-10-05 | 僅 snapshot metadata，沒有公司事件列 |
  | `twse_company_events` | 3 / 3 / 0 / 0 | 2026-10-04 | listed 2；另 1 個代碼在普通股主檔外 |
  | `twse_insider_transfer_planned` | 1 / 1 / 0 / 0 | 2026-10-02 | listed 1 |
  | `twse_insider_transfer_untransferred` | 1 / 0 / 1 / 0 | NULL | 空白 sentinel 隔離，不代表零事件 |
  | `twse_monthly_revenue` | 1,086 / 1,086 / 0 / 0 | 2026-08-01 | listed 1,054/1,054（100%）；32 個主檔外代碼不進普通股覆蓋分子 |
  | `twse_quarterly_forecast_variance` | 7 / 7 / 0 / 0 | 2026-06-30 | listed 5 個匹配事件列（非市場覆蓋）；最新事件時間 10/04 |

- 觸發的 market-signal 子 run 成功，但它是公告增量掃描：`ready/partial`、來源 `data_review_required`、價格/法人/全市場宇宙皆 0、候選 0；新聞背景 `no_candidates`，通知 `sent=0/failed=0`。無股票通知 receipt。最新完整日線掃描仍為 10/02：universe 1,946、price 1,938、flow 1,891、50 candidates、0 notified。官方 TWSE 報價仍 1,051 列、最新有效日 10/01；TPEx 報價/個股法人資料仍無 source records，官方行情/法人 PIT 版本仍未形成。
- `market_signal_outcomes` 唯讀聚合仍只有 2026-09-24 legacy H1：`signal_close` 81 列、80 筆可判方向、50 hits；`next_close` 81 列、80 筆可判方向、37 hits；無 H5/H10。沒有可用的新官方來源績效樣本，維持門檻不變。
- 新聞自然 run `news_ch_collect_process / scheduled__2026-10-05T04:30:00+00:00`（12:30 邏輯時間，14:30–14:39 實際完成）三個 task 全成功。12 feeds 共 1,180 URLs、1,178 parsed/有效時間、1,159 accepted；fetch/parse reject 2、空白內容 19、invalid-time quarantine 0、重複 0，逐來源 post-time reconciliation=true。觀察時刻 10/05 14:39、48 小時 cutoff 10/03 14:39：956 精確時間列在 48 小時內、19 日期精度列在 cutoff 日期後、5 列僅能確認 cutoff 日期（時分未知）、179 列按來源時間精度已舊、未來列 0。自由時報空白內文 19/95（20%），仍與前幾批約 20% 的持續缺口相符；不因單批放寬過濾或改新聞權重。iThome/TNL 多為舊文或日期精度不確定，不能當成嚴格即時證據。
- After-close 仍無 10/05 自然 run，最新仍是 10/02 實際執行的 failed run；官方日線與新 TAIFEX/TPEx 收盤來源待今日 20:00 自然批次。未觸發/重跑 DAG、未寫業務 DB、未發 Discord、未交易，未改程式或分數。

## 2026-10-05 14:27 台北：使用者再次校正工作範圍為自動化訊號

- 使用者明確要求以可排程、可授權、可批次抓取的市場級訊號為交付；逐家公司閱讀券商報告、人工評分彙整，或把人工瀏覽供應商頁面當作進度，都偏離目標。後續以既有 registry/自然收集、全市場來源覆蓋與新鮮度、PIT/修訂版本、成熟 outcome、通知 receipt 及可驗證的窄幅程式改善為主。
- 券商共識/預估只在具明確機器介面與本專案授權、全上市櫃覆蓋、可回溯發布版本及使用權後，才列入自動化候選；任一條件未確認就記來源缺口，不以人工逐家公司查報告替代。新聞沿用已排程來源作時間受限背景，未經 outcome 驗證不加方向票。
- 本次只有工作範圍校正，沒有新市場訊號或自然批次回執。10/05 20:00 台北 after-close 自然批次尚未到期；不手動觸發、不改分數或門檻。沿用既有未提交工作樹內容，無其他檔案清理或覆寫。

## 2026-10-05 14:20 台北：確認新版收盤來源集尚未自然驗收

- 唯讀比對 Airflow mapped task/XCom 與現行 registry：最後一個收盤 run（10/02 實際執行、failed）當時只有 11 個 `ingest_approved_source` map instances；目前 after-close registry/audit 預期 25 個來源。舊 task XCom 只保存 source key、row/accepted/rejected 數，map 9 TPEx 行情失敗，其餘舊列又缺 `dag_id`，故不能用它補造完整的有效日、上市櫃覆蓋或 run 歸屬。
- 10/05 04:00 自然 daily process 已更新/核對目前 registry，但 migration 14 之後尚沒有 after-close 自然 run；對現行 25 個 after-close source keys 查 `source_record_versions` 目前皆 0。這是尚待第一個新版自然收盤批次驗收的狀態，不能先宣稱 PIT 寫入成功，也不能僅憑舊版本為 0 判定新版 adapter 壞掉。
- 新增的可批次候選包括 TAIFEX 上市/上櫃指數期貨與臺指選擇權法人淨未平倉，以及期貨大額交易人前五/前十名未平倉集中度。來源 registry 已限定為官方 OpenAPI/開放資料、market-context-only；買賣兩側分開保留，不相減成個股多空票。若今晚收集成功，先驗 raw/accepted/quarantine/duplicate、effective date、行別/市場覆蓋與 PIT，再累積跨日樣本；在價格/期貨結果對齊及多期 outcomes 前不升成方向訊號。
- 今日 20:00 台北自然 run 的關鍵驗收是 25 個預期來源與 task map 對齊、TPEx quote 完整傳輸、各源有效日及列品質/覆蓋、source-run lineage、版本表寫入、daily scan/outcomes 和平台 receipt。未手動觸發或重跑，也未調整分數、門檻或通知規則。

## 2026-10-05 14:15 台北：盤中自然批次、官方事件訊號與收盤待驗

- 最新自然 `source_extension_collect_intraday` logical run `scheduled__2026-10-05T05:45:00+00:00`（台北 13:45）於 14:00–14:02 完成；10/10 預期來源都有回執、契約無漂移，但稽核仍是 `data_review_required`。上市/上櫃普通股主檔分母為 1,054/892。TWSE 月營收 1,086 accepted、1,054/1,054 普通股匹配（32 個非普通股代碼留作 unknown）；TPEx 月營收 892 accepted、891/892 匹配；TWSE/TPEx 重大訊息為 3/3（2 個上市匹配，1 個未匹配）及 4/4（4 個上櫃匹配）。兩市場未完成轉讓來源各 1 列空白 sentinel 隔離，不能當成零事件。
- 同一批 TWSE 公司財測差異源有 7 accepted、5 個上市普通股匹配，來源有效日 2026-06-30、最新發布事件時間 10/04。`market_signals.py` 已將 `company_forecast_variance` 納入增量候選器與獨立事件通知；訊息要求核對公司原始財測/財報，方向不明且不進多空分數。TPEx 簡式財測來源只有 1 列 snapshot metadata、0 公司事件列。這一輪下游 `market_signal_notify` success，但它是事件型掃描：universe/價量/法人列均 0、候選/股票通知 0；不能稱為全市場日線掃描，也不能從 0 候選推論當日沒有交易訊號。
- 最新完整 daily scan 仍是 10/02：universe 1,946、price 1,938、flow 1,891、50 candidates、0 stock notification。官方收盤資料仍未形成：TWSE quote source_records 1,051 筆、有效日只有 10/01，TPEx quote/TPEx flow 沒有 source_records，三個官方 quote/flow keys 沒有 PIT versions。成熟 outcomes 仍只含 2026-09-24 的 legacy H1（signal-close 81 rows/50 hits；next-close 81/37），尚無 H5/H10；不改分數或門檻。
- After-close Airflow 最新仍為 `scheduled__2026-10-01T12:00:00+00:00` failed（10/02 實際執行）；唯讀稽核回 `unknown`，因 17 筆舊 `source_runs` 列缺 `dag_id`，拒絕跨 DAG 歸屬。今日 20:00 台北自然批次尚未到期，待其自然完成後核 TWSE/TPEx effective date、逐源 raw/accepted/quarantined/duplicate、普通股覆蓋、PIT、daily scan/outcome 與 receipt，不手動觸發/重跑。
- 今日通知 ledger 唯一 HTTP 200 receipt 仍連到舊 `scheduled__2026-10-02T14:45:00+00:00` intraday source run；關聯 market-signal run candidate 0、notified 0，是來源品質提醒而非交易方向訊號，不重送。最新新聞自然收集仍是 10/05 10:30 logical / 12:30–12:39 實際完成；下個新聞批次未到期，延續追蹤自由時報空白內文率與逐來源 freshness。

## 2026-10-05 14:08 台北：新聞來源缺列趨勢與上市法人批次來源缺口

- 依最新自然 `news_ch_collect_process` XCom（`scheduled__2026-10-05T02:30:00+00:00`，10:30 logical / 12:30–12:39 實際執行），1,124 URLs 有 1,121 parsed、1,121 有效發布時間、1,107 accepted；invalid-time quarantine 與重複皆 0。4 個 `partial` source 不是同一種故障：三立 2、中央社 1 筆 fetch/parse rejected；自由時報 13、鉅亨網 1 筆空白內文遭過濾。各來源拒收原因 reconciliation 都為 true。
- `dags/news_ch/all_news.py` 將 `partial` 定義為 accepted 小於 URL 數，因此它會混合抓取/解析失敗與預期的空標題/內文過濾，不能直接當成來源中斷。近 5 個有細項 XCom 的自然批次，自由時報空白內文分別為 13/98、19/93、24/100、17/100、27/100，共 100/491 URLs（20.4%）；顯示持續性的擷取覆蓋缺口。尚未證實是頁面結構變更或來源文章本身無內文，先記錄並觀察後續批次，不放寬空內容過濾、不將缺文當訊號，也不因單日改通知權重。
- 以機器解析 TWSE 官方 OpenAPI Swagger（143 paths）檢查法人路徑，僅見 `/fund/MI_QFIIS_cat` 與 `/fund/MI_QFIIS_sort_20`（產業彙總/持股前 20），沒有全市場個股 T86 endpoint。政府開放資料目錄本次找到的是上櫃個股法人明細 dataset 11856（每日、免費、政府資料開放授權第 1 版）；上市個股法人明細僅找到開放建議紀錄，非可收集資料集。TWSE T86 查詢/CSV 網頁不等同已確認的機器批次授權，故不接入舊個股網站查詢路徑；上市個股法人 flow 仍列為授權來源缺口。官方規格與目錄：[TWSE OpenAPI](https://openapi.twse.com.tw/v1/swagger.json)、[TWSE T86](https://wwwc.twse.com.tw/zh/trading/foreign/t86.html)、[TPEx 法人資料集 11856](https://data.gov.tw/dataset/11856)、[上市法人明細開放建議](https://data.gov.tw/suggests/137032)。
- 後續改善次序：觀察自由時報空白內文比率是否跨多個自然 run 持續，再針對單一來源 parser 證據修補；待 TWSE 有可核准的全市場批次來源或正式授權後，才納入 source registry。今日沒有改程式、分數、門檻或通知規則；after-close 仍待 20:00 自然 run。

## 2026-10-05 13:56 台北：從新聞 XCom 讀取逐來源新鮮度

- 使用 `news_ch_collect_process` 最新自然 run 的 `collect_news` XCom `sources` 欄位；run logical time 是 10:30 台北、實際執行 12:30–12:39，cutoff/觀察時刻分別是 10/03 12:39 與 10/05 12:39。這些統計來自批次解析回執，不需逐篇閱覽或掃整張新聞表。
- 1,107 accepted articles 按來源發布時間精度分布為：889 筆精確時間落在 48 小時內、16 筆日期精度在 cutoff 日期之後、5 筆日期精度恰在 cutoff 日而無法確認小時先後、197 筆依來源時間精度已舊；未見未來時間。合計與 accepted rows 一致。日期精度列仍是較弱新鮮度證據，不能當成精確 48 小時窗口。
- 各來源仍分別保留 `complete/partial`、解析/有效時間/接受數及 `freshness_status`：iThome 與 The News Lens 有較多日期精度舊文或 cutoff 日不確定列；公視與鉅亨網同時含近期與較舊文章；三立、中央社、自由時報、鉅亨網為 partial。新聞只作時間受限的候選脈絡，不因來源數或文章數增加方向票；沒有調整新聞權重或候選門檻。
- 此前僅讀總數而稱未確認逐來源新鮮度，現在以既有 XCom 補齊該缺口。下一次自然新聞 run 按同一欄位比較來源級新鮮度變化；不查單篇新聞、不手動重跑 DAG。

## 2026-10-05 13:51 台北：自然盤中、新聞與績效現況

- 唯讀檢查確認 schema migration 14 已套用（migration 表共 14 版）。目前官方行情落庫仍不足：`twse_equity_quotes_daily` 1,051 列、最新有效日 2026-10-01；TPEx 行情及 TPEx 法人流尚無 source records，三個官方行情/法人 key 都沒有 PIT versions。最新 daily market scan 仍是 10/02：1,946 universe、1,938 價格、1,891 法人、50 candidates、`notified_count=0`；不能把它視為新官方報價掃描。
- 10/05 04:00 台北 `source_extension_collect_process` 自然 run 為 23/23 task 成功、無 registry/runtime 缺源或契約漂移，但 `data_review_required`。`twse_quarterly_profitability` 有 1,054 raw、1,049 accepted、5 quarantine、0 duplicate；普通股主檔匹配 1,018/1,054（96.58%）。日當沖資格、股利等來源含非普通股代碼，匹配數按普通股用途解讀，未匹配列仍 unknown。
- 最新盤中 collection 是 13:15 logical run，實際 13:30 台北完成：10/10 預期來源回報，source audit 為 `data_review_required`。隔離列為上市/上櫃未轉讓回報各 1 列空白 sentinel；上櫃月營收 892 accepted、891/892 配對；上市重大訊息 3 accepted、2 配對。當日 27 次 intraday market scan 都是 0 candidates、0 stock notifications，`ready/partial`；這表示沒有符合的增量事件候選，不代表沒有市場交易訊號。
- 通知 ledger 新增一筆有 HTTP 200 receipt 的來源品質警示，實際時間 10/05 07:00 台北；它關聯的 intraday source run id 是 `scheduled__2026-10-02T14:45:00+00:00`，signal run 的 `candidate_count=0`、`notified_count=0`。因此它是批次品質警示而非股票訊號。保留 receipt 和來源 run lineage；不據此重發或調整規則。
- 最新 `news_ch_collect_process` 三個 task 均 success。Airflow logical run 時間為 10/05 10:30 台北，實際收集 12:30–12:39；批次回報 1,124 URLs、1,121 parsed、1,121 有可解析時間、3 fetch/parse rejected、1,107 persisted。新聞發布時間使用來源 `created_at`，落庫時間以 `create_time` 記錄 12:39；本次未彙總各來源最新發布時刻，因此不能僅憑 task success 認定新聞新鮮。
- `market_signal_outcomes` 仍只有 2026-09-24 H1：signal-close 81 rows/50 directional hits，next-close 81 rows/37 hits；無 H5/H10，價格來源均為 `legacy_stock_table`。沒有足夠新官方來源成熟績效來校準門檻。最新 after-close 仍是 10/02 失敗 run，map index 9 的 TPEx 大回應短讀；現行 local/worker `adapters.py` SHA256 相同，已用 64 KiB byte ranges、單段最多 3 次，待 10/05 20:00 自然批次驗證。未手動觸發 DAG、未寫業務 DB、未發 Discord 或交易。

## 2026-10-05 13:28 台北：把執行重心固定在自動化批次訊號

- 使用者再次校正：目標是可排程、全市場批次抓取、可用成熟樣本持續驗收的訊號管線；逐家公司瀏覽券商報告、人工評分比較與彙總不屬於交付。券商觀點只有在已確認授權且有可排程全市場 feed/API 時才列為來源候選；條件未確認就記錄來源缺口，不做手工替代。
- 修正 `docs/source_extension.md` 的每日掃描契約：收盤方向使用已接入的官方上市/上櫃日行情批次及其他 PIT 資料；不載入舊逐股網站查詢法人表。TPEx 法人官方批次仍待自然 run、PIT/上市櫃覆蓋與多日期 outcome 驗收，TWSE 個股法人授權批次仍是缺口；兩者目前都不投方向票。
- 修正收盤摘要中的法人覆蓋文字，明確表示是「可供方向分析」或「已接入法人來源」的列數。零可用列表示目前沒有通過來源准入的方向資料，不能解讀為法人淨買賣超為零；仍保留缺列 unknown。
- 沒有調整候選門檻、分數、新聞方向權重或 outcome 方法。10/05 after-close 仍待自然排程；待 20:00 後只讀核對 effective date、raw/accepted/quarantined/duplicate、上市/上櫃覆蓋、PIT versions、成熟績效與 receipt，不手動重跑 DAG。

## 2026-10-05 13:19 台北：日線方向與績效改用官方批次行情

- 新日線掃描 `_load_daily_candidates` 現在只從 PIT-aware `source_record_versions`／`source_records` 讀 `twse_equity_quotes_daily`、`tpex_equity_quotes_daily` 的收盤價與成交股數；候選價量特徵不再讀 `stock.個股成交資訊`。個股結果結算亦改讀同一組官方行情來源，基準仍是官方 TAIEX/TPEx 指數；缺列、缺值和未匹配代碼保持 unknown，既有上市櫃 coverage/freshness gate 不變。
- 為避免舊結算與新來源混算，`market_signal_outcomes` 新增 `outcome_price_source_key`（schema migration 14）；既有列以 `legacy_stock_table` 標記，新列按市場記錄 TWSE/TPEx 官方 quote key。自動 family diagnostics 與 `stock_signal_family_history_queries.py` 依來源分組。
- `audit_runs.py` 的收盤 effective-date alignment 改以官方上市/上櫃 quote records 的最新有效日作基準，不再查 stock DB legacy 日行情。舊 `stock_collect_everyday_process` 對 `stock_calculate_everyday_process` 的自動觸發已移除：保留舊函式與歷史資料，但 legacy 券商/主力查詢路徑不再自動產生推薦結果；新訊號由 `source_extension` 排程管線處理。
- 唯讀基線（13:00 台北）：TWSE 官方行情仍只有 1,051 列、有效日 2026-10-01；官方 TWSE/TPEx quote 與 TPEx flow PIT versions 均為 0，成熟結果只有 2026-09-24 H1（舊行情來源）。所以新官方來源尚未形成當日完整行情或可比較的新成熟績效。
- 靜態驗證：6 個修改 Python 檔 AST parse 通過；Airflow worker `airflow dags list-import-errors --output json` 為 `[]`；`stock_collect_everyday_process` task 清單不再包含 `calculate_trigger_everyday_dagrun`；新訊號掃描和稽核器已無 `個股成交資訊`／逐股法人查詢 SQL。未跑測試、未手動觸發 DAG、未寫業務 DB、未發 Discord 或交易。
- 10/05 20:00 自然收盤後核對官方兩市場行情有效日、raw/accepted/quarantined/duplicate、覆蓋與 PIT versions，以及 migration 14 是否經自然初始化生效。官方日行情累積足夠 sessions 後，才可形成 5-session 動能/20-session 量能特徵；不回填未保留版本的舊行情。

## 2026-10-05 13:05 台北：隔離未確認授權的舊法人推薦排程

- 使用者再次校正目標：重點是可排程、全市場批次擷取與自動驗證，不是逐家券商報告線上研究。檢視舊 `stock_calculate_everyday_process` 後確認，它仍排程執行法人比、法人累積推薦，以及混合法人與券商資料的推薦分支；這些分支會讀取由網站查詢 collector 填入、尚未確認可作訊號的 `三大法人買賣`。
- 已從每日計算 DAG 排除五個依賴該來源的 task：`calculate_outer_and_investment_ratio`、`calculate_outer_and_investment_recommendation`、`make_outer_and_investment_recommendation_list`、`make_outer_and_investment_cumulate_recommendation_list`、`make_another_broker_inout_recommendation_list`。獨立的券商進出與主力買賣流程保留；舊歷史資料與實作未刪除，避免破壞既有資料和其他用途。
- 唯讀 DB 核對（13:00 台北）：官方 TWSE 日行情現有 1,051 列、有效日 2026-10-01；`twse_equity_quotes_daily`、`tpex_equity_quotes_daily`、`tpex_institutional_flow_daily` 的 PIT version row 目前皆為 0，TPEx 法人來源也尚無持久化 run。成熟 `market_signal_outcomes` 仍只有 2026-09-24 H1，故不以目前資料恢復法人方向票。
- 驗證：修改檔 Python AST parse 成功；Airflow worker `airflow dags list-import-errors --output json` 回傳 `[]`。未跑測試、未觸發/重跑 DAG、未寫業務 DB、未發 Discord 或交易。
- 下一步等 10/05 20:00 自然收盤批次，核對行情/法人來源的 effective date、raw/accepted/quarantined/duplicate、上市櫃覆蓋與 PIT versions；待官方 TPEx 批次累積可用日期後，再建立 OTC 專屬 shadow 特徵與 1/5/10-session outcomes。TWSE 全市場法人授權批次來源仍是缺口。

## 2026-10-05 12:25 台北：自動盤中批次與訊號子 run 回看

- 使用者再次明確校正範圍：交付物是可排程、可批次抓取、能重複量化驗收的市場訊號；逐家公司線上調查與人工券商報告評分不屬於工作流程。只讀核對 DataFlow 自然 run，沒有手動觸發或重跑 DAG。
- `source_extension_collect_intraday / scheduled__2026-10-05T04:00:00+00:00`（12:00 台北 logical time）於 12:15 完成，Airflow parent success；source audit 為 `all_expected_tasks_succeeded_data_review_required`，10/10 預期與回報、missing/runtime missing/contract drift 皆 0。上市普通股主檔 1,054、上櫃 892。逐來源的來源列/接受/隔離/重複、有效日及匹配數：

  | source_key | rows / accepted / quarantined / duplicate | effective date | listed / OTC matched; coverage | quality |
  |---|---:|---|---:|---|
  | `tpex_company_events` | 4 / 4 / 0 / 0 | 2026-10-03 | 0 / 4 | — |
  | `tpex_insider_transfer_planned` | 1 / 1 / 0 / 0 | 2026-10-02 | 0 / 1 | — |
  | `tpex_insider_transfer_untransferred` | 1 / 0 / 1 / 0 | NULL | 0 / 0 | 空白列隔離，不推成零事件 |
  | `tpex_monthly_revenue` | 892 / 892 / 0 / 0 | 2026-08-01 | 0 / 891；99.89% of 892 | 1 個代碼未匹配，保持 unknown |
  | `tpex_quarterly_forecast_variance_events` | 1 / 1 / 0 / 0 | 2026-10-05 | 0 / 0 | metadata-only，無公司事件列 |
  | `twse_company_events` | 3 / 3 / 0 / 0 | 2026-10-04 | 2 / 0 | 1 個代碼未匹配，保持 unknown |
  | `twse_insider_transfer_planned` | 1 / 1 / 0 / 0 | 2026-10-02 | 1 / 0 | — |
  | `twse_insider_transfer_untransferred` | 1 / 0 / 1 / 0 | NULL | 0 / 0 | 空白列隔離，不推成零事件 |
  | `twse_monthly_revenue` | 1,086 / 1,086 / 0 / 0 | 2026-08-01 | 1,054 / 0；100% of 1,054 | 32 個來源代碼在普通股主檔外，保持 unknown |
  | `twse_quarterly_forecast_variance` | 7 / 7 / 0 / 0 | 2026-06-30 | 5 / 0 | 事件列數不是全市場覆蓋率 |

- 10 個來源的 accepted rows 均有本 run PIT 版本觀測；這不等於有效日新鮮或全市場完整。`market_signal_notify` 子 run 也為 Airflow success，但快照 `ready/partial`、source collection `data_review_required`、candidate 0、news context `no_candidates`，通知 sent/failed 均 0。這次是事件/營收盤中批次，不是新的全市場日行情掃描；零候選不代表零交易活動，也沒有新增通知回執。
- 最新新聞自然批次仍為 10/05 10:30–10:39 台北；成熟 `market_signal_outcomes` 仍只有 2026-09-24 H1，無 H5/H10；最新完整 daily market scan 仍是 10/02。10/05 20:00 after-close 尚未到期，留待自然 run 驗上市/上櫃日行情、TPEx 法人流及其他收盤來源的有效日、隔離列、覆蓋、版本與通知回執；不調門檻、不手動重跑。

## 2026-10-05 12:40 台北：成熟期查核與上櫃法人流研究假設

- 直接查核 `crawler.market_signal_outcomes` 與 10/02 daily scan log：成熟資料只有 2026-09-24 的 H1，`signal_close` 與 `next_close` 各 81 列、80 列有方向評分。H1 的 `signal_close` 是 9/24 收盤至 9/29 收盤；`next_close` 是 9/29 至 9/30。10/02 daily capture review 131 個舊候選，新增 `settled_count=0`、`pending_data_count=0`，最新可見行情日 10/02。這不是 outcome 寫入失敗的證據：當時 9/24 之後只有 9/29、9/30、10/01、10/02 四個交易日；本地官方休市曆列 9/25–9/28 為休市，因此 `signal_close` H5 最早要等 10/05 收盤資料，`next_close` H5 最早為 10/06；H10 更晚。今晚自然 daily child 只可能首次產生 `signal_close` H5 outcome。
- 自動研究檔 `docs/institutional_flow_intensity_research_20261005.json` 提出待驗、非生產的 OTC 假設：五日法人淨買超以過去 20 日平均成交量標準化、同市場/日期取前十分位，再要求五日相對大盤動能同向。H5 的年別結果為 2023：12 日、平均 signed excess -0.3913pp、正向日期率 50.0%；2024：11 日、+0.5198pp、72.7%；2025：11 日、+1.1138pp、63.6%；2026：8 日、+1.9622pp、62.5%。加權總計 42 個 signal dates、平均 +0.690pp、正向日期率 61.9%；2023 為負，且不是獨立留出樣本。
- 此研究共只有 88 個達 70% coverage 的 market dates，另 1,698 日因 PIT 覆蓋不足排除；各市場/年份價格可用率中位數皆 0%，資料使用現行證券主檔、未復原下市成分股，報酬未調整公司行動或交易成本，且比對多個重疊假設。資料來自 legacy `stock.三大法人買賣` 與僅有 `create_time` 的可變價格表，不能據此改生產分數或證明 alpha。
- 後續只用具批次授權的 TPEx 官方來源驗證該假設。政府資料開放平臺 dataset 11856 標示每日更新、免費、政府資料開放授權第 1 版，欄位含上櫃逐檔三大法人分項；目前 registry 將它設為 after-close context-only。公開 metadata 未證實歷史回查深度，因此今晚自然收集先驗本日 effective date、列數、隔離/重複與 892 檔普通股覆蓋，再按 PIT 日資料持續累積；上市法人全市場授權來源仍是缺口。來源：[政府資料開放平臺 dataset 11856](https://data.gov.tw/dataset/11856)。
- 未改候選門檻、分數或通知；沒有重跑 DAG、送 Discord 或交易。下一個自然 after-close 批次要同時核 TPEx 行情完整傳輸、TWSE/TPEx effective date 與覆蓋、新法人來源落庫，以及 9/24 H5 是否成熟。

## 2026-10-05 12:52 台北：TPEx 法人流 OpenAPI 歷史能力核對

- 唯讀取得 TPEx 官方 `https://www.tpex.org.tw/openapi/swagger.json`（HTTP 200，476,923 bytes），機器解析 7 個 institutional GET operations；`/tpex_3insti_daily_trading` 的 `parameters=[]`，其他 6 個 institutional endpoints 也都沒有日期參數。這證實此 OAS 契約能提供可排程的每日全市場快照，沒有契約化的日期回補參數；不把目前快照宣稱為歷史資料。
- 官方 TPEx 網站另有日期查詢頁，但不是該 OAS operation，且尚未確認該頁歷史匯出路徑的自動批次授權/速率契約，因此不把它接入排程。政府資料開放平臺 dataset 11856 仍是目前確認可用的每日免費 Open Data License v1 來源；registry 的 `tpex_institutional_flow_daily` 保持 `daily_after_close` 和 context-only。
- 若 10/05 自然 after-close 回執成功，從其 effective date 開始保存每日版本；等至少 5 個 flow sessions 和 20 個可點時成交量 sessions，且 OTC coverage/freshness gate 通過，再離線計算前述 flow-intensity shadow 假設並累積 1/5/10-session outcomes。它仍不投方向票；TWSE 全市場法人流未有已確認的免費批次來源，除非取得另一個授權批次介面。
- 本次只讀 OAS 規格，未呼叫日期查詢頁、未寫業務 DB、未改分數/門檻、未重跑 DAG、未發 Discord 或交易。今晚 20:00 自然 after-close 再核新來源 effective date、raw/accepted/quarantined/duplicate、892 普通股覆蓋與 PIT 版本。

## 2026-10-05 11:31 台北：官方日行情日期對齊納入自動稽核

- 範圍維持自動化、可排程、全市場來源；券商研究僅在具合法授權且可批次/API 重複取得時才列入訊號管線，逐家公司人工查閱不屬於此交付物。
- 依已觀察到的自然 run 缺口，`dags/source_extension/audit_runs.py` 新增 after-close TWSE/TPEx 日行情 `effective_date_max` 與掃描價格快照日期的對齊檢查。成功來源若缺有效日、查不到價格快照或日期不一致，會記錄 `effective_date_alignment` 與品質旗標，將來源稽核列為 review；不改訊號分數，也不把缺列視為零量。
- 觸發依據：10/02 自然來源紀錄中的 TWSE 報價 task 為 HTTP 200、1,051 列接受，但有效日僅 10/01；同日 legacy 價格快照已到 10/02。10/05 11:27 對官方 TWSE `STOCK_DAY_ALL` 的唯讀 preflight 回 HTTP 200、1,380 列，解析出 1,051 個上市普通股報價並配對 1,051/1,054 主檔代碼；這只是即時端點檢視，不是已持久化的 Airflow 收集回執，也不證明端點何時更新。
- 驗證：本機 Python AST parse 成功；Airflow worker `airflow dags list-import-errors --output json` 回傳空清單。尚待下一個 20:00 after-close 自然 run 驗證新旗標和 TPEx 分批來源狀態，不手動觸發/重跑 DAG。價格方向家族仍讀 legacy 行情；官方批次行情還未升格為方向輸入。

## 2026-10-05 11:47 台北：確認 after-close 失敗根因與 PIT 版本自然落庫

- 唯讀 Airflow run/task state 與 worker `source_runs` 查核確認：最近自然 `source_extension_collect_after_close` 是 `scheduled__2026-10-01T12:00:00+00:00`，10/02 20:00 台北執行後失敗。`ingest_approved_source` 的 `map_index=9` 對應 `tpex_equity_quotes_daily`；兩次都因 HTTP 回應短讀而失敗，最後一次讀到 4,193,897 bytes，仍短 472,744 bytes。`verify_collection_and_triggers` log 的原因是 `ingestion failures=['9:failed']`；market-signal trigger task 為 success，但這只證明子 run 被觸發，不等於訊號通知已送達。
- 失敗發生時的舊 quote adapter 以完整 response body 讀取，無法承受 TPEx 大型回應中斷。現行 `adapters.py` 已改用固定 byte-range、校驗 `Content-Range` 與 ETag/Last-Modified，單段最多重試三次；官方端點的 72 段完整 preflight 已在 10/05 早前記錄，但不是 DAG 回執。今晚 20:00 自然 run 是第一次可驗證現行 adapter、TWSE/TPEx effective date、列品質/覆蓋及行情版本寫入的機會；不手動觸發或重跑。
- 同一舊 run 的 `source_runs` 有 17 列、15 個來源 key 缺 `dag_id`；目前稽核器因此回 `unknown` 並拒絕跨 DAG 推定歸屬。這是舊 lineage 資料限制，與 map 9 的行情傳輸失敗分開記錄；不回寫推定 lineage。相較之下，10/05 04:00 自然 `source_extension_collect_process` audit 為 23/23 回報、`data_review_required`、無未歸屬列。
- PIT 版本自然寫入已由 DB 唯讀查證：上述 04:00 process run 在 `source_record_versions` 共寫入 20,864 列、涵蓋 23 個來源 key，其中 `source_feed` 13,808 列、`pre_ingest_current` 7,056 列；前者含 13,803 筆 `ok` 與 5 筆隔離列，後者含 7,051 筆 `ok` 與 5 筆隔離列。DB `captured_at` 約為 2026-10-04 20:00–20:02（對應 Airflow run 10/05 04:00–04:02 台北）。兩個 official daily quote key 目前仍是 0 versions，因最近 after-close quote run 未成功；不得用其他日頻 sources 的版本列代替報價歷史。
- 10/02 下游 daily scan row 為 `ready/partial`、1,946 universe、1,938 prices、1,891 flows、50 candidates、`notified_count=0`。其 notification ledger 仍有一筆 `failed` 與一筆標成 `sent` 的列，但都沒有 platform receipt ID 或 HTTP 200；不能當實際通知送達。成熟 outcomes/門檻仍不變。
- 最新 news collection `scheduled__2026-10-05T00:30:00+00:00` 的三個 task 均 success，10/05 10:30–10:39 台北完成；最新 intraday market scan 至 11:45 為 0 candidates、無 news join。outcome table 唯讀聚合仍只有 2026-09-24 H1：signal-close/next-close 各 81 rows、80 scored，沒有新增成熟日期或 H5/H10 證據。
- 範圍遵循追蹤文件既有界線：CMoney 已是先前記錄的批次候選，沒有新增授權/介面存取證據，不重做供應商頁面調查、不接 API。此輪沒有改訊號分數、重跑 DAG、寫業務 DB、送 Discord 或交易。

## 工作範圍校正：自動化、全市場訊號優先

- 目標是持續擴充與改善可排程、可批次抓取的台股訊號管線，不是逐家公司上網找券商報告、人工評分，再彙總成報告。每個候選來源都先判斷是否有官方或已授權、可重複取得的全市場資料介面；若只能逐檔登入查閱或權限/完整性不明，記為來源缺口，不列入自動訊號主流程。
- 工作順序以「來源盤點/授權與契約 → 自然排程收集 → effective date、raw/accepted/quarantined/duplicate、上市/上櫃覆蓋與 freshness → PIT 合併 → 可重複計算的訊號族 → 多日期成熟 outcome 與實際通知 receipt」為準。成功 task 或一次命中都不是訊號有效的證據；不完整資料不可補零，也不以單一日期改門檻。
- 新聞只納入可自動收集且保留發布時間精度、來源與 PIT 邊界的背景/事件特徵；先與官方事實及未知事項分欄，不因新聞與候選同時出現便加方向分。券商觀點只有在合法授權且能以排程/API/批次取得可追溯版本與全市場或明確覆蓋範圍時，才評估自動化接入；人工逐份研究不作為這個目標的交付物。
- 後續來源探索要提出可機器化接入的來源、可量化欄位、更新頻率/歷史深度、權限與覆蓋限制、所需驗收及 outcome 設計；沒有新增可行來源或可行動證據時，維持監控，不為了產出而做逐家公司報告彙整。

## 2026-10-05 12:02 台北：自動化來源稽核加入 lineage-aware PIT 覆蓋

- 根因是 `source_runs.finished_at` 為秒精度、`source_record_versions.captured_at` 為微秒精度；同一成功 run 寫入的版本可能顯示在 `finished_at` 之後。只用 `captured_at <= finished_at` 會誤報版本缺漏。
- `dags/source_extension/audit_runs.py` 現在對每個成功來源計算 run window 內觀察到的 accepted records、能以歷史 `source_feed` 版本或同一 `dag_id`/`dag_run_id` lineage 找到版本的列數、缺版本數與覆蓋率。計數不一致或有缺版本會加品質旗標；`dags/source_extension/dag.py` 將欄位帶入自動化 audit issues 與 source summaries。沒有改方向分數或門檻。
- 對最新自然 `source_extension_collect_process` 作唯讀驗證：23/23 來源有稽核結果，accepted、observed、versioned 各合計 13,803，unversioned 0，無列數不一致；整批仍為 `all_expected_tasks_succeeded_data_review_required`，不代表來源全新或全市場完整。版本表同一 run 的 20,864 列已拆明為 13,808 `source_feed` + 7,056 `pre_ingest_current`，各含 5 筆 quarantine，避免將所有版本列誤稱為 accepted feed observations。
- 驗證只有 Python AST parse 成功及 Airflow `list-import-errors` 為空；沒有執行測試或觸發 DAG。今晚 20:00 台北 after-close 自然批次仍待觀察，尤其 TPEx 個股日行情的完整回應、effective date、覆蓋及版本落庫。

## 2026-10-05 12:13 台北：逐股法人流找到上市市場授權缺口

- 來源目錄 60 個來源中有合法批次的 `tpex_institutional_flow_daily`（data.gov.tw 11856，每日、免費、政府資料開放授權第 1 版），沒有 TWSE 每檔三大法人買賣超資料的對應開放資料來源。TPEx feed 仍要等自然 after-close run 驗收，其設定目前明確為 context-only，未經 PIT/覆蓋/多日期成熟 outcome 驗證前不得投方向票。
- 追查到目前 legacy collector `dags/stock/collect_logic.py` 仍以 HTTP GET 呼叫 TWSE RWD `/rwd/zh/fund/T86`，並呼叫 TPEx 舊的 `/www/zh-tw/insti/dailyTrade`；daily scanner `dags/source_extension/market_signals.py` 讀 legacy `三大法人買賣` 表，近 7 日資料可以在候選計算中成為 `institutions` 方向家族（5 日窗至少 4 日同向、淨買賣至少 100,000 股）。因此這是現行訊號路徑的來源授權/批次口徑問題，不只是待新增的候選源。
- TWSE 使用條款寫明未經同意不得用自動化裝置、指令碼、爬蟲或擷取程式下載網站資料，例外是官方已授權到政府開放資料平台的項目。TWSE T86 頁可查詢或下載 CSV 本身不構成自動抓取授權；不要再把公開查詢頁視為免費批次 API。
- 可授權的全市場批次候選是 TWSE Data E-Shop「三大法人買賣超檔」：每日、資料起始日 2004-09-09；`TWT86UC` 18:00 產製且不含鉅額，`TWTAIUC` 20:00 產製且含鉅額；欄位涵蓋外陸資（不含外資自營商）、外資自營商、投信、自營商自行/避險及合計的每檔買賣股數。頁面列內部使用 NT$1,500/月、外部資訊用戶 NT$3,000/月，可選線上下載或 email 檔案，歷史單筆最多 5 年。購買本身不等於准許自動下載；TWSE 網路資訊商店條款另限制未經同意的自動化下載。尚未確認 Email/線上商品能否授權自動匯入、留存歷史、使用於衍生訊號或私人 Discord 通知，也未確認現有 T86 JSON 路徑是否等同任一商品版本。
- 在授權與交付方式確定前，不新增 TWSE T86 網頁端點、不購買商品、不宣稱 legacy 表等同正式批次版本；同時把「現行掃描仍可能用 legacy 法人流投方向票」列為需決策的風險。若已有商品 entitlement，先核對授權範圍與可自動接收方式，再比較 18:00/20:00 樣檔、鉅額交易口徑、逐列欄位恆等式、上市普通股主檔覆蓋、PIT 版本與自然 run；若無授權，應停用該 legacy 投票路徑，保留缺值為 unknown，待合法批次累積並經成熟 outcome 驗證再談重啟。
- 已先在 `dags/source_extension/market_signals.py` 停止讀 `三大法人買賣` legacy 表，避免未確認授權的流量繼續成為方向票；缺少正式 TWSE 批次時 scanner 保留法人資料為缺值並將 source status 標為 partial。這次只切斷新訊號 scanner 的讀取，舊 collector 仍存在且其他 legacy DAG 可能使用該表；沒有在未盤點下游依賴前停用整個 collector。
- 驗證：worker 對 `market_signals.py` AST parse 成功、Airflow `list-import-errors` 為 `[]`，程式碼掃描確認新訊號 scanner 不再含 `FROM 三大法人買賣`；未跑測試、未手動觸發 DAG。今晚自然收盤/掃描 run 待確認 family、coverage、候選與通知狀態。
- 下游依賴盤點發現舊路徑仍會自動使用同一來源：`basic_stock_collect_everyday.py` 是平日排程，執行 `three_main_inverstor_collect` 後會觸發 `stock_calculate_everyday_process`；雖然 calculate DAG 自身 `schedule=None`，它由前述每日 DAG 觸發。`calculate_logic.py` 的法人比例/券商推薦計算仍讀 legacy 表並用法人數值篩選，因此本次沒有停用舊 collector 或整組推薦任務；停用會改變其他舊輸出，需要先界定授權或確認退場範圍。新 `market_signals.py` 已隔離，舊推薦路徑仍是待處理來源風險。
- 一手依據：[政府開放資料 11856](https://data.gov.tw/dataset/11856)、[政府資料平台的上市/上櫃開放差異建議](https://data.gov.tw/suggests/137032)、[TWSE 使用條款](https://www.twse.com.tw/zh/terms/use.html)、[TWSE T86 查詢頁](https://wwwc.twse.com.tw/zh/trading/foreign/t86.html)、[TWSE Data E-Shop 商品與費率](https://eshop.twse.com.tw/zh/product/detail/c4c87ac184e44896a05fcab5a9d544ec)。

## 2026-10-05 11:24 台北：盤中事件掃描不讀舊日線行情

- 11:15 台北自然 `source_extension_collect_intraday`（`scheduled__2026-10-05T03:00:00+00:00`）Airflow success；10 個來源與前一批相同，source audit 仍為 `data_review_required`。子 `market_signal_notify` 是 `ready/partial`、0 candidates、`news_context_status=no_candidates`、sent=0/failed=0。
- 檢查實際執行路徑：`capture_market_signal_snapshot` 的 intraday 分支以 source-record id cursor 加重疊/修訂時間窗呼叫 `_load_intraday_candidates`；它只讀該增量/修訂視窗內符合條件的 `company_event`、`monthly_revenue`、財測差異及內部人移轉事件。它不呼叫日線 `_load_daily_candidates`，也不讀 legacy `stock.個股成交資訊` 價格，故不會把 10/02 收盤行情誤當 10/05 盤中訊號。
- 解讀界線：這是增量事件通知器，不是即時價格選股器；候選為 0 只表示本批沒有符合條件的新/更新事件可送，並不表示當日沒有價格型訊號或沒有新聞。盤中個股報價/法人流 feed 缺口仍在；日線價量掃描待 20:00 after-close 自然批次。

## 2026-10-05 11:22 台北：官方日行情批次來源仍未進入方向價格家族

- 官方目錄確認 TWSE 11549「上市個股日成交資訊」為每日、免費、政府資料開放授權第 1 版，欄位含成交股數及開高低收/漲跌；TPEx 11370「上櫃股票行情」同樣每日、免費、政府資料開放授權第 1 版，含股票、ETF、ETN 盤後行情與成交股數。自動來源已在 registry 掛這兩個批次 feed；若要作普通股方向輸入，仍須限定 ES* 普通股並驗證數值欄。
- 唯讀查詢目前持久化狀態：`twse_equity_quotes_daily` 最新自然 source run 為 10/02 HTTP 200、1,051/1,051 接受、0 隔離/重複，但資料有效日只有 10/01；目前 1,054 檔上市普通股主檔中配對 1,051，另 3 檔本批沒有資料，保持 unknown。`tpex_equity_quotes_daily` 最新兩次嘗試在 10/02 因回應短讀失敗，0 raw/accepted；目前沒有 TPEx quote rows。
- `source_record_versions` 對這兩個行情來源目前都是 0 列；`source_records` 只有上述 1,051 列 TWSE 10/01。最新 10/02 after-close parent 仍 failed，10/05 的自然 20:00 run 尚未到期。因此即使目錄授權與日更新頻率合適，當前 persisted evidence 仍不夠新，且尚未開始累積 quote vintage。
- 程式路徑核對：`registry.py` 將兩來源定義成 `market_quote_daily`；`market_signals.py::_load_daily_market_breadth` 目前只把它們作同日漲跌家數背景。方向 price/volume 家族的日期、收盤、量能仍從 legacy `stock.個股成交資訊` 讀取，法人家族仍從 `stock.三大法人買賣` 讀取；這解釋為何新官方 feed 尚未修復歷史行情 PIT 覆蓋。不能僅因資料集可下載就宣稱已接入可驗證的方向訊號。
- 可自動延伸的下一步是先讓自然 after-close 批次證明兩市場 effective date、新鮮度、逐欄非空/可解析率、raw/accepted/quarantine/duplicate 與普通股母體覆蓋，再從 `source_record_versions` 累積同一口徑的每日 close/volume 歷史；達到至少 70% 雙市場覆蓋與可回放 PIT 後，才以 walk-forward 比較既有 legacy price/volume family，避免把兩種來源或成交量單位混接。未改 production score 或 signal families。

## 2026-10-05 11:12 台北：自然盤中批次與法人流 PIT 重算

- 最新自然 `source_extension_collect_intraday` logical run `scheduled__2026-10-05T02:45:00+00:00` 在 11:00 台北完成。Airflow 與 source audit 均成功，但品質狀態仍為 `all_expected_tasks_succeeded_data_review_required`；10/10 expected、registered、runtime、reported，missing/contract drift 均為 0。來源清單只有公司事件、月營收、財測差異與內部人事件，並非盤中報價或個股法人流批次。
- 逐來源 raw/accepted/quarantine/duplicate 與 effective date：TPEx 公司事件 4/4/0/0（10/03）；TWSE 公司事件 3/3/0/0（10/04）；TPEx/TWSE 月營收 892/892/0/0、1,086/1,086/0/0（皆 2026-08-01，分別匹配上櫃 891/892、上市 1,054/1,054 普通股）；TWSE 季財測差異 7/7/0/0（2026-06-30）；TPEx 季財測差異事件 metadata 1/1/0/0（10/05）；planned insider transfer 兩市場各 1/1/0/0（10/02）；untransferred insider 兩市場各 1/0/1/0（effective date NULL，空白列隔離，不推為零事件）。
- 對應 `market_signal_notify` child 為 `ready/partial`、source collection `data_review_required`、0 candidates、`news_context_status=no_candidates`、sent=0/failed=0。這只表示該 scan 沒有候選可做 news join；不能解讀為沒有市場訊號或新聞。10/05 07:00 自然 source-quality warning 已有 HTTP 200 receipt。
- 在同一 Airflow worker 對既有 `institutional_flow_intensity_research.py` 做唯讀重算，資料截止 2026-10-02，歷史窗共 1,786 個上市/上櫃市場日期窗；符合 next-session-open 前價格與法人各自 70% PIT 覆蓋的只有 88 窗，1,698 窗排除。各市場/年度的價格 PIT 覆蓋中位數均為 0.00%，法人列入庫時間在次日開盤前的比例約 99.40%–100%。因此只能以 88 窗視為嚴格 PIT 子樣本；480 個彙總格與少數正值不足以證明流量強度因子有效，不升格訊號、不改權重或門檻。重算鍵重複/衝突排除仍為 0；現行主檔存續者偏差、公司行動與成本未處理等限制仍在。
- 追蹤文件 02:47 初版把 1,706/80 寫成通過/排除，數字錯誤；後續 03:26 修正版記載 88/1,698，本次 worker 重算再次確認該修正版。初版數字已標記作廢，避免後續以錯誤分母評估訊號。
- 對自動化的直接需求是可追溯歷史 vintage：legacy 價格/法人 current table 的 `create_time` 不能普遍證明歷史日當時資料可得，且舊值可能被覆蓋。後續需依合法全市場批次來源保存 append-only source/effective/observed versions，再累積自然日期做 PIT outcome；歷史無版本日一律排除，不回填推估。最新 after-close natural parent 仍是 10/02 failed；等 10/05 20:00 自然 run 驗證 TPEx 分段行情與通知回執，不手動重跑。

## 2026-10-05 10:44 台北：盤中個股行情與法人流的自動化來源缺口

- 最新自然盤中批次 `source_extension_collect_intraday / scheduled__2026-10-05T02:15:00+00:00` 的 XCom 顯示 expected/registered/runtime/reported 都是 10/10，無 missing、contract drift 或 unattributed rows；整批仍是 `all_expected_tasks_succeeded_data_review_required`、completeness `unproven`。10 個來源摘要與 10:15 批次相同：有公告、月營收、財測/董監事件資料，沒有當日個股報價或法人流。
- 同一自然 child `market_signal_notify` 回傳 `ready/partial`、`source_collection_status=data_review_required`、`news_context_status=no_candidates`、0 candidates；通知回執為 sent=0/failed=0。這是掃描沒有市場方向輸入時的空候選結果；因 candidate set 為空，不能解讀為新聞沒有相關內容，也不能解讀為市場沒有訊號。
- 最新自然新聞批次 `news_ch_collect_process / scheduled__2026-10-05T00:30:00+00:00` 已於 10:39 台北完成：12 feeds、1,006 URLs、1,004 parsed/valid-time、981 accepted、2 fetch/parse rejects、0 invalid-time quarantines、23 content/duplicate rejects；來源拒收計數已 reconciliation，981 篇於 10:39:38 台北寫入。accepted 文章中 768 篇有精確發布時間且落在 48 小時內，7 篇只知道 cutoff 日期後、5 篇日期落在 cutoff 當天但時分未知、201 篇依可用時間精度已舊；日期精度不能當成小時級新鮮度。盤中 child 因 0 candidates 沒有可核對的候選新聞 join，本次不把新聞資料量當成訊號效果。
- 目前自動來源 registry 將 TWSE `STOCK_DAY_ALL` 與 TPEx `tpex_mainboard_daily_close_quotes` 排在 `daily_after_close`，分別對應政府資料集 [11549](https://data.gov.tw/dataset/11549) 與 [11370](https://data.gov.tw/dataset/11370)；它們是每日/盤後行情，不能為 07:00–22:00 的盤中掃描提供最新個股價格。政府資料集 [11747](https://data.gov.tw/dataset/11747) 的正式中文名稱是「興櫃股票當日行情表」，每日更新，市場範圍不是上市或上櫃普通股，不能拿來補 1,946 檔核心 universe 的盤中覆蓋。
- 官方盤中行情路徑是交易所授權 feed：TWSE 說明即時行情須直接或經已簽約使用者連線，並列有資料/授權費用分類；TPEx 說明即時資訊需簽「供給使用交易資訊契約」，規格提供個股即時成交資訊與 5 秒快照。TWSE 網站條款也禁止未獲同意的網站自動抓取。官方參考：[TWSE 即時交易資訊與申請/費用](https://wwwc.twse.com.tw/en/products/information/real-time.html)、[TWSE 使用條款](https://www.twse.com.tw/en/terms/use.html)、[TPEx 即時交易資訊購買說明](https://www.tpex.org.tw/zh-tw/service/data/product/real-time.html)、[TPEx 盤中逐筆/5 秒行情說明](https://www.tpex.org.tw/zh-tw/mainboard/trading/rules/continuous.html)。
- 下一步只查可合法批次接入的既有券商/資訊商行情授權或交易所 feed：確認 feed/API 端點、上市櫃普通股全市場覆蓋、延遲/時間戳、資料使用/外顯權利、歷史重播或版本保存，再設 source run raw/accepted/quarantine、雙市場覆蓋、PIT 與多交易日 outcome 驗收。授權取得前不把網站查詢或未確認 OAS 路徑接入訊號；若要新購 feed，先取得明確費用與使用權再決定。

## 2026-10-05 11:00 台北：自然候選新聞 join 與 PIT 更新後驗收邊界

- 10:45 台北自然盤中 run `source_extension_collect_intraday / scheduled__2026-10-05T02:30:00+00:00` 仍是 10/10 expected/reported、無 missing/contract drift；來源摘要與 10:15 詳表相同，兩市場「未轉讓」來源各 1 raw/0 accepted/1 quarantine。子 run `ready/partial`、0 candidates、`news_context_status=no_candidates`，通知 sent=0/failed=0；這是當次掃描沒有方向候選輸入，不表示沒有新聞或交易訊號。
- 最近一個有候選的自然 run 是 10/04 08:04 台北週末盤中掃描：5 張事件觀察候選，掃描時 `news_context_status=matched`；2 個候選配到 3 篇新聞（中央社 1、鉅亨網 2；標題比對 2 篇、內文前段比對 1 篇），另外 3 個候選在有限樣本無匹配。PIT 候選池有 4,638 篇、實際抽樣 4,638 篇、未截斷；3 篇匹配的 `created_at` 都是 datetime 欄位，但發稿頁面與時區尚未逐項驗證，仍只作背景，不當方向票。通知 task 回報 sent=1/failed=0，平台 receipt ledger 有 HTTP 200。
- 這 10/04 候選 run 早於 10/05 05:38 台北的新聞入庫/PIT 修正；現行 `market_signals.py` 已用 `news.create_time <= observed_at` 限制掃描截止前已入庫文章，但 10/05 盤中至 10:45 都沒有候選，因此更新後的候選新聞 PIT join 尚無新的自然候選案例可驗。下次自然掃描若有候選，核對文章入庫時間不晚於掃描 cutoff、發布時間精度、來源、匹配方式與通知 receipt；不回填舊候選。
- 10/05 07:00 自然盤中流程另送出「來源批次品質待確認」警示，列出 TWSE/TPEx 未轉讓來源各 1 raw/0 accepted/1 quarantined；ledger 有平台 receipt 且 HTTP 200。這是品質警示，不是個股候選通知；10:45 子 run 沒有再送重複警示。
- 10/05 04:00 daily process 自然 run 經唯讀 `audit_source_run` 以 run-scoped `source_runs` 重建 23 個來源摘要：23/23、無 missing/contract drift，整批 `data_review_required`。已確認兩個未轉讓內部人來源各 1/0/1/0，空白列不作零事件；TWSE 季營益 1,054/1,049/5/0、有效日 2026-06-30、上市配對 1,018/1,054（96.58%）；TPEx 季營益 884/884/0/0、同有效日、上櫃配對 884/892（99.10%）。TWSE/TPEx 當沖資格有效日 10/05，分別 1,228/1,228 與 841/841 接受；配對 972/1,054、725/892，其餘代碼不併入個股方向訊號。
- 最新 daily scan 仍為 10/02 `ready/partial`：1,946 universe、1,938 價格、1,891 法人流、50 candidates、0 notified；`market_signal_run_market_coverage` 仍 0 rows。成熟 outcome 只有 2026-09-24 H1：signal-close 81 rows/80 directional、50 hits、平均超額 +1.776pp；next-close 81/80、37 hits、+0.987pp；尚無 H5/H10，不調分數或門檻。after-close 最新自然 parent 仍為 10/02 failed，10/05 20:00 尚未到期。

## 2026-10-05 10:20 台北：收盤來源失敗根因與修正載入狀態回核

- 唯讀查 Airflow task state、原始 task log 及該 run 的 `discover_approved_sources` XCom：最新 `source_extension_collect_after_close` 自然 parent 仍是 `scheduled__2026-10-01T12:00:00+00:00`（10/02 20:00 台北），failed。map index 9 明確對應 `tpex_equity_quotes_daily`；`ChunkedEncodingError / IncompleteRead` 在串流讀到 4,193,897 bytes 時中斷，尚缺 472,744 bytes。其餘 10 個 mapped source 成功，`verify_collection_and_triggers` 因 `9:failed` 失敗；這是大型回應短讀，不是來源 schema 或代碼配對拒收。
- 現行 `adapters.py` 已為 `tpex_equity_quotes_daily` 導向 allowlisted byte-range reader：逐段核 `Content-Range`、`Content-Length`、representation validator（ETag/Last-Modified），每段最多嘗試 3 次，並限制總回應大小。主機、scheduler、worker 的檔案 SHA-256 同為 `96555b104c4f5576d194695f0d46afb2f74835821120e14f7a6be5235592e45d`；worker AST parse 為 `AST_OK`，Airflow import errors 為 `[]`。`tests/test_source_extension.py` 目前沒有這個串流短讀/分段重試的回歸案例，本輪沒有修改程式或執行測試。
- 截至 10:20 台北，尚未到 10/05 20:00 自然 after-close；目前無 runtime 證據證明此 reader 已由自然 DAG 完整取回個股日線。舊端點 72 段唯讀預檢不能代替 `source_runs` 回執。今晚按自然批次核 effective date、accepted/quarantined/duplicate、上櫃普通股覆蓋、日線 scan coverage/outcome 與實際 notification receipt，不手動觸發或重跑。

## 2026-10-05 10:15 台北：自然盤中批次覆蓋與 outcome 分母回核

- 唯讀核對最新自然 `source_extension_collect_intraday / scheduled__2026-10-05T02:00:00+00:00`（10:15 台北完成）：10/10 expected、registered、runtime、reported sources，missing/contract drift/unattributed rows 均為 0；整批為 `all_expected_tasks_succeeded_data_review_required`，`data_completeness=unproven`。所有列依來源 schema 與普通股主檔核對；事件列不是市場覆蓋率，未配對保留 unknown。

  | source_key | rows / accepted / quarantine / duplicate | effective date | 上市 / 上櫃配對與解讀 |
  |---|---:|---|---|
  | tpex_company_events | 4 / 4 / 0 / 0 | 2026-10-03 | 0 / 4 事件列 |
  | tpex_insider_transfer_planned | 1 / 1 / 0 / 0 | 2026-10-02 | 0 / 1 事件列 |
  | tpex_insider_transfer_untransferred | 1 / 0 / 1 / 0 | NULL | 空白列隔離，不推為零事件 |
  | tpex_monthly_revenue | 892 / 892 / 0 / 0 | 2026-08-01 | 0 / 891；上櫃主檔 891/892（99.89%），1 個接受代碼未配對 |
  | tpex_quarterly_forecast_variance_events | 1 / 1 / 0 / 0 | 2026-10-05 | metadata-only，沒有公司事件；覆蓋不適用 |
  | twse_company_events | 3 / 3 / 0 / 0 | 2026-10-04 | 2 / 0 事件列，另 1 代碼在主檔外 |
  | twse_insider_transfer_planned | 1 / 1 / 0 / 0 | 2026-10-02 | 1 / 0 事件列 |
  | twse_insider_transfer_untransferred | 1 / 0 / 1 / 0 | NULL | 空白列隔離，不推為零事件 |
  | twse_monthly_revenue | 1,086 / 1,086 / 0 / 0 | 2026-08-01 | 1,054 / 0；上市主檔 1,054/1,054（100%），32 個接受代碼未配對 |
  | twse_quarterly_forecast_variance | 7 / 7 / 0 / 0 | 2026-06-30 | 5 / 0 財測差異事件列，不是覆蓋率 |

- 對應 10:15 的自然 `market_signal_notify` child 為 `ready/partial`、`source_collection_status=data_review_required`、`news_context_status=no_candidates`、0 candidates；notification XCom 為 `sent=0, failed=0`。沒有當日個股價格/法人方向輸入，不能解讀成沒有市場訊號或新聞影響。
- 唯讀查 `market_signal_runs`：最新 daily 仍為 10/02 `ready/partial`，universe 1,946、price 1,938、flow 1,891、50 candidates、0 notified。`market_signal_run_market_coverage` 表存在但目前 0 rows，雙市場 daily scan coverage 尚無自然落庫證據。
- 重新核對 `market_signal_outcomes`：9/24 `signal_close` 與 `next_close` 各 81 rows、81 筆有超額報酬，但只有 80 筆有 `directional_hit`；各有 1 筆 `訊號分歧` 不納入方向命中分母。方向命中為 50/80 與 37/80，平均未加方向符號的超額報酬分別 +1.7760pp、+0.9868pp。尚無 H5/H10，不以單一日期調整門檻。
- 最新自然 after-close 仍是 10/02 failed；10/05 20:00 排程尚未執行。等待自然收盤批次驗證 TPEx 個股行情 byte-range reader、所有來源有效日/接受與隔離列/雙市場覆蓋，以及日線 coverage、成熟 outcome 和實際 receipt；不手動重跑。

## 2026-10-05 10:09 台北：自然盤中批次、日線缺口、新聞 PIT 與成熟度回核

- 唯讀 Airflow/XCom 核對最新自然 `source_extension_collect_intraday / scheduled__2026-10-05T01:45:00+00:00`（10:00 台北完成）：10/10 expected、registered、runtime、reported sources；missing、contract drift、unattributed legacy rows 均為 0，總狀態 `all_expected_tasks_succeeded_data_review_required`。這 10 個來源都是公告/基本面資料，沒有即時個股行情或盤中法人 flow feed；逐來源摘要與 09:45 run 相同。
- 逐來源有效日/接受與上市櫃配對：TWSE 月營收 1,086/1,086 accepted、effective date 2026-08-01、上市 1,054/1,054（100%）；TPEx 月營收 892/892 accepted、同日、上櫃 891/892（99.89%）。兩市場 accepted 中主檔外代碼分別 32/1，保持 unknown。TPEx company events 4/4、effective date 10/03、上櫃配對 4；TWSE events 3/3、10/04、上市配對 2 且 1 個主檔外。兩市場未轉讓來源各 1 raw/0 accepted/1 quarantined；不是零事件。TWSE 財測差異 7/7、有效期間 6/30、5 列匹配（事件列不是覆蓋率）；TPEx 財測來源為 10/05 snapshot metadata、沒有公司事件列。
- 唯讀查 `stock.個股成交資訊` 與 `stock.三大法人買賣`：兩表最新有效日均為 2026-10-02，2026-10-05 均 0 列。10/02 個股日線 1,938 個代碼；依現行普通股主檔分市場，上市 1,050/1,054（99.62%）、上櫃 888/892（99.55%）。10/02 法人表同日代碼配對為上市 1,043/1,054（98.96%）、上櫃 788/892（88.34%）；這是當日原始代碼配對，不等於訊號掃描的近 7 日 flow_count，也不把缺列補零。
- 10:00 自然 intraday 子 run `market_signal_notify` 為 `ready/partial`，universe/price/flow/revenue/financial/borrow/insider/dividend/candidate/notified 全為 0；這表示 10/05 行情/方向輸入缺失，非市場沒有訊號。最新 daily scan 仍為 10/02 `ready/partial`、1,946 universe、1,938 price、1,891 flow、50 candidates、0 notified；成熟 outcomes 只有 9/24 H1（兩個 entry basis 各 81 列、各 80 列可評分），無 H5/H10。9/24 signal-close H5 要等 10/05 收盤，next-close H5 要等 10/06 收盤；單一 signal date 不調門檻。
- `market_signal_run_market_coverage` 目前仍無資料列，10/02 daily capture 的 XCom 也沒有分市場 coverage payload；主機、scheduler、worker 的現行 `market_signals.py` SHA-256 一致，程式現行 daily path 會持久化 coverage。等下一個自然 daily scan 驗證 runtime 落庫後再宣稱此監測已生效；盤中 early-return 不負責寫 daily coverage。
- 最新新聞自然 `news_ch_collect_process / scheduled__2026-10-04T21:30:00+00:00` 的 `collect_news` XCom：12 feeds、968 URLs、965 parsed/valid-time、938 accepted、3 fetch/parse rejects、0 invalid-time quarantine、27 content/duplicate rejects，reconciliation=true；時間觀測 08:41，48h cutoff 為 10/03 08:41 台北。TNL 88/90 accepted 日期早於 cutoff，另 2 筆只有 cutoff 當日日期、時分未核；iThome 54/60 早於 cutoff，3 筆僅日期且時分未知。此類舊/日期精度新聞不得當成即時證據；新聞仍依 `create_time <= observed_at` 與來源發稿時間精度 PIT 篩選，不加方向票。
- 最新有 receipt 的通知仍是 07:00 來源品質提醒（HTTP 200，receipt `1556440879248711802`），不是交易方向訊號；目前無 10/05 direction-candidate receipt。After-close DAG active、未 pause，metadata `next_dagrun_create_after=2026-10-05T12:00:00+00:00`（20:00 台北）；最新自然 run 仍為 10/02 failed。待今晚自然收盤 run 核 effective date、source counts、兩市場 coverage、日線/XCom coverage 落庫、outcome 及 receipt，不手動觸發/重跑。

## 2026-10-05 09:46 台北：MOPS 全市場財報更正事件批次候選

- 唯讀資格預檢官方 MOPS `t56sb31_q1` 財報更(補)正查詢，以市場別 `all`、公司代號空白、年度 115、季別 02 查詢；HTTP 200、回應 29,424 bytes，取得 49 筆事件列、47 個不同代碼，有效公告日期介於 2026-08-10 至 2026-10-02，49/49 日期可解析。列粒度是更正事件，不是公司；不可只按代碼去重。
- 同站 `t163sb01` 財務報告公告頁以全市場/空白代碼查詢，回應要求輸入公司代號；目前不能提供全市場原始財報申報時間。這讓季報 XBRL 整批封存檔的首見時間、修訂版本及報告公告時間仍是 PIT 缺口。
- 財報更正是可程式化的官方品質/修訂事件候選，可能用於標示舊季報數值已更新、避免財報因子沿用過時版本；所有更正不可一概視為重大或看空，需保留更正內容並分類格式補件與數值/會計內容異動，成熟 outcome 前不加方向票。
- 這是官方網頁的一次性 POST 預檢，不是 OpenAPI、source_registry 註冊、自然 Airflow run 或持續授權證明。此次未找到該查詢的官方開放授權、排程/速率/保存與再利用條款；先列資格候選，不自動接入、不逐家公司查詢。若取得明確授權，驗收應以全市場/季別批次、事件鍵、公告時間、首次觀測時間、修訂版本、上市櫃覆蓋、重複/隔離及自然 run 回執為準。
- 官方入口：[財報更(補)正查詢](https://mopsov.twse.com.tw/mops/web/t56sb31_q1)｜[XBRL 案例文件整批下載](https://mopsov.twse.com.tw/mops/web/t203sb02)。細節與接入閘門見 [source_extension.md](source_extension.md#official-mops-quarterly-report-corrections-full-market-event-candidate)。

## 2026-10-05 09:40 台北：盤中個股行情仍無授權批次來源

- 唯讀查自然 `source_extension_collect_intraday / scheduled__2026-10-05T01:15:00+00:00`：10/10 mapped tasks 成功，但只有公司事件、月營收、財測差異與內部人轉讓等公告/基本面來源，沒有即時個股報價 feed。其 market-signal 子 run 於 09:30 `ready/partial`，`as_of_date=2026-10-05`，universe/price/flow/revenue/financial/borrow/insider/dividend/candidate/notified 全為 0；這次是沒有當日盤中行情輸入，不代表零成交或市場沒有訊號。
- `stock.個股成交資訊` 最新有效日仍為 2026-10-02，10/05 列數為 0。當前盤中掃描可處理新公告，但無法產生基於當日個股價格/量能的方向判讀；通知 ledger 本次沒有新 signal receipt，最近回執仍是 07:00 的來源品質提醒 HTTP 200。
- 官方交易所說明：TWSE 將即時/延遲個股與指數交易資訊列為契約及費用管理的資訊產品；TPEx 也提供需契約的即時/延遲交易資訊。專案沒有已確認的相關授權或批次供應商，因此不把公開網頁瀏覽等同自動化授權，也不接未核准的逐檔查詢路徑。保留現有開放資料收盤行情作為方向訊號基礎，盤中只處理官方公告；授權 feed 若日後到位，再依有效日、行情報價覆蓋和延遲驗收。[來源及界線](source_extension.md#intraday-quote-access-boundary)
- 不手動觸發 DAG、不實發 Discord、不交易。待 10/05 20:00 自然 after-close 批次核對正式日線、TPEx 法人資料與 outcome 更新。

## 2026-10-05 09:29 台北：核實 9/24 outcome 尚未成熟的原因

- 唯讀查 `market_signal_outcomes`：2026-09-24 的 `signal_close` 與 `next_close` 各有 81 筆 settled H1，尚無 H5/H10。H1 的 `signal_close` entry/exit 為 09-24/09-29；`next_close` entry/exit 為 09-29/09-30。
- 對照本地 `holiday.taiwan`：2026-09-25、09-28 為休市，09-29 開市；TWSE 115 年官方行事曆及 TPEx 休市公告亦列 09-25 中秋與 09-28 教師節休市。因此 09-29 是 09-24 收盤訊號後第一個交易日，H1 exit 日期沒有錯位。
- 最新 ready daily run 的資料 as-of 為 2026-10-02；`signal_close` H5 須到 10-05 收盤、`next_close` H5 須到 10-06 收盤才成熟。現在尚未有 10-05 收盤價，不把 H5/H10 缺列稱為失敗或零報酬。等待 10/05 自然 after-close run 及後續自然 daily scan 回報成熟 outcome；不調門檻、不手動補算寫庫。
- 官方日曆：[TWSE 115 年市場開休市日期](https://wwwc.twse.com.tw/zh/trading/holiday.html)｜[TPEx 2026/09/25、09/28 休市公告](https://www.tpex.org.tw/storage/eb_data/11509/11503027221.html)。

## 2026-10-05 09:26 台北：補 TDCC 成功批次品質計數與重複鍵閘門

- 針對 09:00 自然 TDCC run 未提供 accepted/quarantine/duplicate 欄位的缺口，更新 `dags/stock/collect_logic.py`：收集時正規化持股級距後，檢查完整官方回應的 `(資料日期, 證券代號, 持股分級)` 重複鍵；目標主檔列的空白代碼、無法解析或越界數值會明確記為 quarantined，並 fail closed；重複自然鍵也會帶出 key 數及受影響列數後 fail closed。
- 成功 task 的 batch log 現在列 `raw_rows`、`accepted_rows`、`quarantined_rows`、`duplicate_keys`、`duplicate_rows`、`outside_stock_master_rows`、目標列/摘要列、有效日、市場日 lag 與上市/上櫃 coverage。主檔外列獨立標示為 scope exclusion，不冒稱 rejected 或零持股；隔離列仍僅在 task log 計數，沒有聲稱另存隔離明細表。
- 09:00 run 發生在此程式更新之前，仍只證明當時的 raw/target/summary/coverage 計數；沒有回溯補造 quarantine/duplicate 計數。要等下一個適用的自然 TDCC 批次驗證新 log，本輪不手動觸發排程。
- 靜態驗證：Airflow worker 上 `collect_logic.py` AST parse 與 `stock.collect_logic` module import 成功，`git diff --check` 通過（僅提示工作樹既有 LF/CRLF 轉換）。未跑測試、未手動觸發/重跑 DAG、未寫業務 DB、未發 Discord、未交易。

## 2026-10-05 09:18 台北：TDCC 全市場週批次首次自然收集

- 唯讀查 Airflow 自然 run：`stock_major_holder_monday_refresh_process / scheduled__2026-10-05T01:00:00+00:00` 於 09:00:01–09:00:16 台北執行成功。Task log 記錄官方 TDCC OpenAPI `1-5` 的 `effective_date=2026-10-02`，相對個股日行情 `market_date_lag_days=0`；raw 69,326 列、篩入現行證券代碼 33,048 列、轉換摘要 1,944 檔並 insert 1,944。上市 raw/摘要匹配 1,054/1,054；上櫃 890/892（99.78%），缺的 2 檔維持 unknown，不補零。
- 此 legacy collector 的 task log 沒有 quarantine/rejected/duplicate 計數欄位；不可把未回報當成 0。另一次唯讀 live API 結構預檢（非 Airflow 入庫計數）為 69,326 列、4,078 代碼、17 個級距、(資料日期, 證券代號, 持股分級) 重複鍵 0、必要欄位解析失敗 0；這不能替代來源任務未提供的隔離/拒收統計。資料中級距 17 是總持股列，需與 1–16 級分開解讀；非目前普通股主檔的 2,134 個代碼不納入普通股摘要。
- TDCC 官網說明資料按各週最後營業日收盤集保餘額、經 ID 歸戶後按級距編表；政府資料開放平臺 metadata 卻仍標示每月更新。採官方 API 的實際有效日作 freshness gate，不按成功狀態推定週週有新資料。這是集保帳戶歸戶分布，不能稱為可識別受益人或內部人持股。
- 今天新入庫的 `2026-10-02` 快照目前只證明自動化來源可用；既有最新歷史有效日為 `2025-04-25`，與新快照不在 14 日比較窗內。既有訊號程式要求兩個有效日相隔不超過 14 日、兩市場各至少 95% 覆蓋且候選掃描開始前已入庫，因此本次不形成集中度變化訊號、不加方向分。等下一週自然批次再收一個鄰近有效日快照後，才恢復 PIT 週變化比較；仍需多日期 matured outcomes 才能評估預測性。
- 未手動觸發/重跑 DAG，未跑測試，未手動寫業務 DB，未發 Discord，未交易。來源契約與每週排程說明見 [source_extension.md](source_extension.md)；今天 20:00 after-close 仍另依自然 run 稽核。

## 2026-10-05 09:06 台北：新增法人來源收盤有效日對齊稽核

- 收盤來源稽核此前會對齊四個大盤指數的有效日與股票日線 as-of date，尚未對新增的 `tpex_institutional_flow_daily` 做相同核對。已在 `audit_runs.py` 加入 report-only freshness alignment：以收盤 run 可見的最新個股日線日期作基準，將法人來源 `effective_date_max` 與之比較；來源失敗、有效日缺失、價格基準不可用或日期不一致時，附上來源專屬 quality flag 和 alignment details。
- 該檢查只附 quality flag，不丟棄 unmatched 列、不把缺列補零、不改訊號分數或通知；若所有收集 task 成功，既有 reducer 會將此類 freshness flag 標為 `data_review_required`，task 失敗時仍維持 `partial`。等自然 20:00 run 驗證 XCom/source audit 中的 effective date、對齊狀態與兩市場 coverage。
- `audit_runs.py` AST parse 與 Airflow worker module import 通過；未跑測試、未觸發/重跑 DAG、未寫業務 DB、未發 Discord、未交易。收盤 DAG 目前未 pause，排定自然建立時間為 20:00 台北。

## 2026-10-05 09:02 台北：自然目錄 bootstrap、盤前訊號與新聞批次回看

- 09:00 台北自然 `source_extension_collect_intraday / scheduled__2026-10-05T00:45:00+00:00` 成功，source audit 10/10 expected/reported，狀態 `all_expected_tasks_succeeded_data_review_required`。最新逐來源摘要延續：TWSE 月營收 1,086/1,086 accepted、有效日 2026-08-01、上市普通股匹配 1,054、32 unmatched；TPEx 月營收 892/892 accepted、有效日 2026-08-01、上櫃匹配 891、1 unmatched；兩市場未轉讓來源各 1 raw/0 accepted/1 quarantined；TPEx 財測差異為 metadata-only。所有 unmatched/空列仍為 unknown。
- 此自然 run 的 `ensure_source_extension_schema` 已將 `tpex_institutional_flow_daily` 建入 catalog（enabled、approved、`daily_after_close`）；但截至 09:02 該 source 的 `source_runs=0`、`source_records=0`，因為 20:00 收盤批次尚未到。catalog bootstrap 只證明定義進入目錄，不等於來源收集成功。
- 最新 `market_signal_notify` intraday 子 run 成功、`as_of_date=2026-10-05`、`ready/partial`，universe/price/flow/revenue/financial/borrow/insider/dividend/candidate 各為 0/0/0/0/0/0/0/0/0。掃描發生在開盤前後且當時尚無新行情列；這些 0 是快照缺列，不是零交易或無方向訊號。最新 daily scan 仍為 10/02 `ready/partial`：universe 1,946、price 1,938、flow 1,891、revenue 1,945、financial 1,902、borrow 1,795、insider 1,944、dividend 1,053、50 candidates、通知 0；其 market-coverage 子表仍無列。
- 成熟 outcome ledger 仍只有 2026-09-24 H1：signal-close 81 rows/50 hits/平均超額 +1.776pp；next-close 81/37/+0.987pp；無 H5/H10，不調整訊號門檻或權重。
- 最新新聞自然 run `news_ch_collect_process / scheduled__2026-10-04T21:30:00+00:00` 於 08:30–08:41 台北成功：12 feeds、968 URLs、965 parsed/valid-time、938 accepted、3 fetch/parse rejects、0 invalid-time quarantine、27 content/duplicate rejects，post-time reconciliation true。TNL freshness 為 cutoff-date 時分未核；iThome、公視及鉅亨網為 mixed recent/old。新聞仍依來源發布時間精度做 PIT 背景，不因收集成功就加方向分。
- 通知 ledger 最新有回執的來源品質提醒為 10/05 07:00：HTTP 200、receipt `1556440879248711802`。10/04 08:04 有 6 列帶 receipt；10/03 的 `sent` 列無 receipt，10/02 `failed` 無 receipt。10/02 50 候選 daily scan 仍沒有可驗證的通知送達回執。
- 最新自然 after-close parent 仍為 10/02 20:00 的 failed run；`ingest_approved_source` map index 9 與 `verify_collection_and_triggers` failed。10/05 20:00 台北 after-close 尚未執行，留待自然 run 檢查新法人來源的 raw/accepted/quarantine/duplicate、effective date、OTC master coverage、版本留存、收盤行情與通知 receipt；未手動觸發或重跑。

## 2026-10-05 08:54 台北：接入 TPEx 上櫃個股法人買賣批次來源

- 工作方向依使用者再次校正：交付物是可排程、可批次、可全市場驗收的訊號資料管線，不是人工逐家公司瀏覽券商報告。券商共識/研究只有在取得可重複、合法授權的批次介面及 PIT 版本欄位後才會自動化接入。
- 新增官方 TPEx OpenAPI 契約 `tpex_institutional_flow_daily`，排入既有 `daily_after_close` 註冊與來源版本留存路徑。政府資料開放平臺 dataset 11856 列明每日更新、免費、政府資料開放授權第 1 版、原始資料；欄位涵蓋上櫃股票三大法人買賣明細。官方 OAS 操作 `tpex_3insti_daily_trading` 的說明亦為上櫃股票三大法人買賣明細。欄位按股數保存；以「外資及陸資不含外資自營商 + 投信 + 自營商」核對三大法人合計，外資自營商分項另存、不重複併入合計。來源不提供發布時刻，保留資料有效日，不虛構發布時間。
- 唯讀官方端點預檢與新 parser 映射預演：HTTP 回應 JSON 可讀；910 raw、910 normalized、910 accepted、0 quarantined、0 duplicate，唯一 effective_date 為 2026-10-02；910 個證券代碼唯一，三大法人分項與來源合計 910/910 相符。這是當前 API 樣本預演，不是 Airflow 自然 run 或資料庫寫入證據。
- 對照目前 common-equity master：上櫃普通股母體 892 個；910 個來源代碼中 788 個匹配，來源代碼 unmatched 122 個，母體相對覆蓋 788/892=88.34%；另有 104 個母體代碼未出現在本次來源列中。兩邊差額均保留 unknown，不補零、不宣稱完整上櫃覆蓋。非標準代碼仍保留來源列，交由 coverage audit 顯示 unmatched，不因格式不同直接刪除。
- 新來源明確限於上櫃，沒有把 TWSE T86 接入未核授權的路徑；也未將 TPEx 資料冒稱上市櫃全市場。這個來源目前是自動化收集與 PIT 候選，尚未替換既有法人訊號讀取、尚未加入方向分數或通知；等自然 after-close run 證實來源有效日、實際接受/隔離/重複數、788/892 類型的普通股匹配覆蓋與資料庫版本，再累積多個 signal dates 的成熟 outcome 後才考慮接上訊號引擎。
- 靜態驗證：`adapters.py`、`registry.py` AST parse OK；worker import 可載入定義，allowlisted contract 成立，source priority 97.75，高於自動啟用門檻 75；after-close source definitions 共 25。未跑測試、未手動觸發/重跑 DAG、未寫業務 DB、未發 Discord、未交易。
- 官方來源：[政府資料開放平臺 dataset 11856](https://data.gov.tw/dataset/11856)｜[TPEx OpenAPI Swagger](https://www.tpex.org.tw/openapi/swagger.json)｜[TPEx API endpoint](https://www.tpex.org.tw/openapi/v1/tpex_3insti_daily_trading)。下一個自然收盤批次再核實實際 run ledger；API 預檢不替代自然 run。

## 2026-10-05 08:33 台北：盤中批次與通知回執自然驗收

- 唯讀重查 04:00 process 自然 run scheduled__2026-10-03T20:00:00+00:00：Airflow success、23/23 expected/reported、missing/runtime missing/contract drift 皆 0，整批仍 all_expected_tasks_succeeded_data_review_required。TWSE 季獲利有效日仍 2026-06-30、1,054 raw/1,049 accepted/5 quarantined、上市普通股匹配 1,018/1,054（96.58%）；其餘保持 unknown。
- 新的 08:15 台北盤中自然 run source_extension_collect_intraday / scheduled__2026-10-05T00:15:00+00:00 於 08:30:00–08:30:27 執行成功；source audit 為 10/10 expected/reported、missing 0、contract drift 0，批次狀態 all_expected_tasks_succeeded_data_review_required。逐來源如下（raw / accepted / quarantine / duplicate）：

  | source_key | HTTP | rows / accepted / quarantine / duplicate | effective date | listed / OTC 覆蓋或列數 | quality |
  |---|---:|---:|---|---|---|
  | tpex_company_events | 206 | 4 / 4 / 0 / 0 | 2026-10-03 | 上櫃事件 4 列；非覆蓋率 | — |
  | tpex_insider_transfer_planned | 200 | 1 / 1 / 0 / 0 | 2026-10-02 | 上櫃事件 1 列；非覆蓋率 | — |
  | tpex_insider_transfer_untransferred | 200 | 1 / 0 / 1 / 0 | NULL | 0 accepted | 空白列隔離，不推為零事件 |
  | tpex_monthly_revenue | 206 | 892 / 892 / 0 / 0 | 2026-08-01 | 上櫃 891/892（99.89%）；1 個代碼未配對 | unmatched 保持 unknown |
  | tpex_quarterly_forecast_variance_events | 200 | 1 / 1 / 0 / 0 | 2026-10-05 | metadata only，無公司事件 | 不視為事件列 |
  | twse_company_events | 200 | 3 / 3 / 0 / 0 | 2026-10-04 | 上市事件列；1 代碼未配對 | unmatched 保持 unknown |
  | twse_insider_transfer_planned | 200 | 1 / 1 / 0 / 0 | 2026-10-02 | 上市事件 1 列；非覆蓋率 | — |
  | twse_insider_transfer_untransferred | 200 | 1 / 0 / 1 / 0 | NULL | 0 accepted | 空白列隔離，不推為零事件 |
  | twse_monthly_revenue | 200 | 1,086 / 1,086 / 0 / 0 | 2026-08-01 | 上市 1,054/1,054（100%）；32 個代碼在普通股主檔外 | 保持 unknown |
  | twse_quarterly_forecast_variance | 200 | 7 / 7 / 0 / 0 | 2026-06-30 | 事件資料；無全市場覆蓋分母 | 不把列數當覆蓋率 |

- 對應 market_signal_notify child 成功（08:30:24–08:30:29）；snapshot as_of_date=2026-10-05、ready/partial、候選 0、此 run 通知列 0。當時尚未開盤，價格及方向家族為 0 是盤前快照，不能解讀為全市場零覆蓋。
- 07:00 台北自然 child 曾送出來源品質提醒，原因只列兩市場未轉讓來源各 1 列隔離；notification ledger 有 Discord receipt 1556440879248711802、HTTP 200。08:00 與 08:15 仍為相同 partial fingerprint，沒有重複提醒。這是來源品質提醒，不是交易方向訊號。
- 最新 daily scan 仍是 10/02 ready/partial、50 候選；通知 ledger 有 sent 與 failed 各一筆但都沒有平台 receipt。失敗 log 為 HTTP 40005 request-too-large；既有 no-send 分批預覽不是實際送達證據。
- 成熟 outcome ledger 仍只有 2026-09-24 H1：signal-close 81 列/50 命中/平均超額 +1.776pp；next-close 81/37/+0.987pp；沒有 H5/H10。單日且未扣公司行動/成本，不改任何權重或門檻。
- 最新新聞自然 run 仍為 05:30 台北成功：12 feeds、1,307 parsed、1,287 accepted、1 invalid-time quarantine、19 content rejections、0 article-id duplicates。iThome 與 TNL 各 2 列只有日期精度，不能作小時級 PIT。
- 09:00 TDCC Monday refresh run list 仍為空，DagModel 未 pause、預計 09:00 台北建立自然 run；上次唯讀表查仍見 persisted effective date 2025-04-25。官方端點預檢不代替 ingest receipt；等自然 run 核 effective date、接受/隔離/拒收、上市櫃覆蓋。
- After-close 最新自然 parent 仍為 10/02 20:00 failed；task-state 唯讀定位 ingest_approved_source map index 9 失敗，verify_collection_and_triggers 隨後失敗。等 10/05 20:00 自然 run 驗現行 range adapter、逐來源帳目、覆蓋與通知 receipt，不手動重跑。
## 2026-10-05 08:13 台北：自然批次、訊號成熟度與通知回執

- 唯讀 Airflow 自然 run 稽核：04:00 `source_extension_collect_process`（`scheduled__2026-10-03T20:00:00+00:00`）23/23 source 有回報、missing/contract drift 均 0，但整批 `data_review_required`。TWSE 季營業分析 1,054 raw、1,049 accepted、5 quarantined、0 duplicate；effective date 2026-06-30，accepted 中 1,018/1,054 個上市普通股代碼配對（96.58%）。其餘未配對代碼保持 unknown。
- 同一自然批次日當沖資格來源有效日 2026-10-05：TWSE 1,228/1,228 accepted、0 quarantine/duplicate，其中 972 個代碼配對現行上市普通股主檔；TPEx 841/841 accepted、0 quarantine/duplicate，其中 725 個配對現行上櫃普通股主檔。這是資格名單會員資料，未配對證券種類尚未核清；不把配對差額解讀為個股方向或零量。
- 08:00 實際執行的盤中自然 run（logical run `scheduled__2026-10-04T23:45:00+00:00`）10/10 expected source、0 missing、0 contract drift，仍為 `data_review_required`。8 月月營收有效日：TWSE 1,086 raw/accepted、上市配對 1,054/1,054，32 個 accepted codes 在主檔外且維持 unknown；TPEx 892 raw/accepted、上櫃配對 891/892（99.89%），1 個未配對不補零。兩市場「未轉讓」來源各 1 raw/0 accepted/1 quarantined；不當作零事件。TPEx 財測差異來源只有有效日 10/05 的空 sentinel/metadata，沒有公司事件列。
- after-close 最新自然 run 仍是 10/02 20:00 台北執行的失敗 run `scheduled__2026-10-01T12:00:00+00:00`。Airflow map index 9 由 discover XCom 證實是 `tpex_equity_quotes_daily`；舊版整體串流兩次都未收完 4,666,641 bytes（分別收到 704,105、4,193,897 bytes），task 重試後仍失敗，驗收器正確讓整批失敗。現行 host/worker `adapters.py` 已同 hash，該來源走 64 KiB 有界 Range、validator/Content-Range 驗證及每段最多 3 次重試；這只是現行程式狀態，等 10/05 20:00 自然 after-close run 驗 effective date、接受/隔離/重複、兩市場覆蓋與 run receipt，不手動重跑。
- 10/02 daily child 的 capture XCom 為 `ready/partial`：universe 1,946、price 1,938、flow 1,891、revenue 1,945、financial 1,902、borrow 1,795、insider 1,944、dividend 1,053、TPEX-only benchmark partial、50 candidates；新聞背景狀態為 matched。通知 log 明確回 HTTP 40005 `Request entity too large`。歷史失敗 ledger 的摘要 payload 為 9 embeds/13,624 UTF-8 bytes，沒有平台 receipt；現行 formatter 對這份已存 payload 做 no-send 重切，結果為 3 批、每批 3 embeds，估算 request bytes 8,384/7,479/7,616（均低於 10,000）。這只證明格式切分，不證明 Discord 已收件；本次沒有發送或重送。
- 截至 10/05 的成熟 outcome ledger 只有 2026-09-24 一個 signal date：81 個候選、162 筆 H1 settled outcome（signal-close/next-close 兩種 basis）、506 個重疊 family observations；沒有 H5/H10 成熟樣本。單日上市 volume family 的 next-close 觀察為 15/20 正向、平均 signed market-excess +3.817pp、中位數 +2.538pp；上櫃為 6/21 正向、平均 -1.197pp。只列作後續跨日期驗證假設，樣本只有一日且未扣公司行動與成本，不改門檻/權重。
- 05:30 新聞自然收集 run 成功：12 feeds、1,307 URLs/parsed、1,306 valid-time、1,287 accepted、1 invalid-time quarantine、19 content rejection、0 article-id duplicate，DB 持久化 1,287 列，`create_time_tpe=05:38:11`。iThome 與 The News Lens 各有 2 列落在 cutoff 日期但只有日期精度，無法判定 cutoff 小時前後；仍不可作小時級 PIT 證據，也不增加方向分。

## 2026-10-05 07:59 台北：官方現金流量品質訊號候選與批次授權缺口

- scheduler 目前 59 個 `SOURCE_DEFINITIONS` 有季損益表及資產負債表來源，沒有現金流量表/營業現金流來源或欄位。現有財報 context 只能以 EPS、利潤率及資產負債比提示獲利與槓桿，不能驗證帳面盈餘的現金轉換。
- 新的可延伸因子候選為「營業活動淨現金流量 ÷ 稅後淨利」現金轉換率，及營業現金流/負債的償債脈絡；先按已公告時間作 PIT、以單季或可比較 TTM 計算，淨利為零/負值、非標準業別及缺報表列保持 NA。MOPSFIN 官方說明列有這些指標，並明示金融、保險、證券期貨、金控及異業合併等不適用情形。此為公式/欄位候選，不是預測力結論。
- 官方 MOPSFIN 的資料涵蓋條款包含上市、上櫃等公司；列出 IFRS 財報四大表、營業現金流等資料，資料庫每日更新且比 MOPS 已申報財報晚一天。頁面沒有在目前可見說明中提供可排程 API/批次檔案契約；不能把互動式財務比較頁視為已授權 API。TWSE OpenAPI 公開路由目前看得到損益表與資產負債表，但沒有找到個股現金流量表 endpoint。
- 官方 MOPS XBRL 資訊平台說明有按季「案例文件整批下載」，操作資料稱可下載該季所有公司申報檔；TWSE Data E-Shop 另列 IFRS 財報/XBRL Push package，當前公開價 NT$40,000/月。批次檔存在不等於允許這個專案自動重複抓取/留存或對外呈現，也不證明目前帳號已有授權；本次未呼叫批次下載、未購買、未加 adapter。
- 接入前確認免付費批次端點的官方下載契約/使用限制，或確認既有 Push/XBRL 授權；核對上市/上櫃普通股覆蓋、報表更正版本、申報可得時間、單季/累計口徑及科目映射。自然收集後用 run-scoped effective date、接受/隔離與覆蓋驗收，再先做 context-only PIT 留存；跨多個成熟 signal dates 評估後才考慮方向票。財務報告不以單一低現金比率直接下買賣結論。
- 官方參考：[MOPSFIN 財報涵蓋、更新與現金流指標定義](https://mopsfin.twse.com.tw/terms)｜[TWSE XBRL 批次下載操作文件](https://dsp.twse.com.tw/public/static/downloads/announcement/official/R-1130603962-1.pdf)｜[TWSE Data E-Shop 財報套裝及資料項目](https://eshop.twse.com.tw/zh/mops/detail/000000007af53455017af57a15fe0007)｜[TWSE Data E-Shop 現行價目](https://eshop.twse.com.tw/zh/mops/list)。自動化授權/介面待核。

## 2026-10-05 07:52 台北：新增券商共識的批次來源候選（待授權與契約核實）

- 盤點 scheduler 載入的 59 個 `SOURCE_DEFINITIONS`，source key/name/purpose/endpoint 均無 TEJ、analyst、broker、consensus 或 EPS forecast feed；目前 DataFlow 沒有自動收集券商 EPS 共識/修正歷史。這是明確來源缺口，不代表使用者沒有站外授權。
- 新候選優先項是「EPS 預估修正 breadth / revision magnitude + 分析師間分歧度」，由上市櫃批次估值修正資料導出；和現有月營收、已公告季財報相比屬前瞻資訊。這只是來源優先級判斷，不是已驗證因子或推薦交易策略。
- TEJ E-Shop 研究索引列出 2026-09-05「台灣股票市場量化因子實證--券商共識資料：財務預測相關因子」、2026-07-05「目標價及投資建議相關因子」；另有 2025-09-05 EPS 預測變動及分析師分歧度研究。可公開讀到的研究摘要提及按 EPS 預測變動幅度分組，並以分析師預估的變異係數衡量分歧；全文頁標為付費文章。**作者/日期目前可確認的是 2025 文章作者羅靖霖、2025-09-05；2026 列表條目只顯示標題/日期。**全文不可讀，資料截止日、股票池與覆蓋率、預估修正定義、估值期間、交易成本、完整績效結果/反方風險皆未知；不推測效果，也不以標題/摘要當證據。
- TEJ 官方 API 文件公開 REST/Python/R/.NET 介面，列免費與付費資料庫及 API key；說明可用多代碼條件和 cursor 分頁。相關說明頁版本為 2019–2021，僅證明有程式化資料介面的一般能力；尚未證明「券商共識」特定表在目前 API 套餐可用、覆蓋範圍、更新頻率、歷史 as-of revisions、逐分析師/券商識別欄位、留存與外顯權利或現行限額。DataFlow 未加入 API key/adapter，也未呼叫端點。
- 接入前驗收條件：確認實際帳號表格授權與允許用途；取可機器批次的 schema/metadata 樣本；核對上市/上櫃對 1,946 普通股 universe 的覆蓋及無估值公司處理；要求每筆有可得時間、估計期、前後版本/修正日、來源分析師數及分歧計算欄位。達標後先以 PIT 快照作 context-only 累積歷史，再按既有 H1/H5/H10 成熟 outcomes、交易成本與公司行動限制評估；尚未證明前不加入 directional family、通知門檻或權重。
- 自動來源探索參考：[TEJ 共識因子研究目錄](https://eshop.tej.com.tw/E-Shop/journal?category_id=7&subcategory_id=27)｜[TEJ 2025 EPS 預測變動研究摘要與付費全文標記](https://eshop.tej.com.tw/E-Shop/journal_detail?journal_id=2025090511)｜[TEJ REST API 說明](https://api.tej.com.tw/documents.html)。不逐公司搜尋券商報告；未有授權/欄位證據前維持 gap。

## 2026-10-05 07:45 台北：逐來源 XCom 摘要自然批次驗收

- 自然 run `source_extension_collect_intraday / scheduled__2026-10-04T23:30:00+00:00` 於 07:45:00–07:45:16 台北成功。XCom 的 `source_audit.source_summaries` 已實際包含 10 個來源，expected/reported 10/10、missing 0、contract drift 0；整批仍標示 `all_expected_tasks_succeeded_data_review_required`，不可只看 DAG 綠燈。每源帳目如下：

  | source_key | HTTP | rows / accepted / quarantine / duplicate | effective date | matched / coverage | quality note |
  |---|---:|---:|---|---|---|
  | tpex_company_events | 206 | 4 / 4 / 0 / 0 | 2026-10-03 | OTC 4（事件列，非覆蓋率） | — |
  | tpex_insider_transfer_planned | 200 | 1 / 1 / 0 / 0 | 2026-10-02 | OTC 1（事件列，非覆蓋率） | — |
  | tpex_insider_transfer_untransferred | 200 | 1 / 0 / 1 / 0 | NULL | 0 | 空白列隔離；不可推為零事件 |
  | tpex_monthly_revenue | 206 | 892 / 892 / 0 / 0 | 2026-08-01 | OTC 891/892，99.89% | 1 個代碼未配對主檔，保留 unknown |
  | tpex_quarterly_forecast_variance_events | 200 | 1 / 1 / 0 / 0 | 2026-10-05 | 0 | 只有 snapshot metadata，沒有公司事件列 |
  | twse_company_events | 200 | 3 / 3 / 0 / 0 | 2026-10-04 | listed 2（事件列，非覆蓋率） | 1 個代碼未配對主檔 |
  | twse_insider_transfer_planned | 200 | 1 / 1 / 0 / 0 | 2026-10-02 | listed 1（事件列，非覆蓋率） | — |
  | twse_insider_transfer_untransferred | 200 | 1 / 0 / 1 / 0 | NULL | 0 | 空白列隔離；不可推為零事件 |
  | twse_monthly_revenue | 200 | 1,086 / 1,086 / 0 / 0 | 2026-08-01 | listed 1,054/1,054，100% | 32 個代碼未配對主檔，保留 unknown |
  | twse_quarterly_forecast_variance | 200 | 7 / 7 / 0 / 0 | 2026-06-30 | listed 5（事件列，非覆蓋率） | — |

- 對應 market-signal child 自然成功，`as_of_date=2026-10-05`、`ready/partial`、source collection `data_review_required`。此刻為開盤前：方向家族尚無盤中行情列，candidate 0；通知 candidates/sent/failed = 0/0/0。這只代表盤前快照，不代表當日全市場訊號不存在，也沒有發 Discord。
- 此次確認逐來源摘要已經自然 runtime 落地。下一步仍以自然 20:00 after-close 驗收個股行情串流修復、兩個新官方來源及收盤訊號快照；不手動觸發 DAG。

## 2026-10-05 07:38 台北：逐來源批次稽核保存與盤中 XCom 驗收

- 最新自然盤中 run `scheduled__2026-10-04T23:15:00+00:00` 於 07:30:00–07:30:21 台北成功；verify XCom 為 10/10 expected/reported、無 missing key/contract drift，整批狀態仍 `all_expected_tasks_succeeded_data_review_required`。XCom 中上市月營收有效日 2026-08-01、1,054/1,054 matched；上櫃月營收同日、891/892 matched（99.89%）；兩市場未轉讓來源各 1 列隔離、0 accepted；TPEx 財測門檻來源為 metadata-only。這是 run 當時的稽核結果，不把未配對或隔離列當零。
- 07:30 market-signal child 自然完成：`as_of_date=2026-10-05`、`ready/partial`，開盤前所有價格/方向家族 count=0、coverage 無列、候選 0；通知 candidates/sent/failed=0/0/0。
- 事後於 07:30 後重查 07:15 的 `audit_source_run(run_id=...)` 時，current `source_records.observed_at` 已由下一批覆寫至 07:30，該 helper 因只按舊 run 的 observed_at 時窗查 mutable current table，錯回有效日 NULL/配對 0；07:15 的 verify XCom 卻保留正確的營收有效日與市場 coverage。故歷史回看以 run-scoped verify XCom 為準，不把事後 current-table 查詢結果當成零覆蓋。
- 已修改 `dags/source_extension/dag.py`：未來 `verify_collection_and_triggers` XCom 除異常列及全市場 coverage 外，另保存每個來源的 compact summary（HTTP、normalized/accepted/quarantined/duplicate、有效日、事件時間、市場配對、quality flags 與 contract match），供逐批次回看。無 scoring/門檻/通知變動，也未寫業務資料表。下一個 07:45 自然盤中 run 用來確認新 XCom 欄位實際落地。
- 驗證：host 與 scheduler `dag.py` SHA-256 相同；兩端 AST parse OK；Airflow DAG import errors 0。`source_extension_collect_intraday` active/unpaused，下一次 create-after 為 07:45 台北；after-close 也 active/unpaused，下一次為 20:00 台北。未跑測試、未手動觸發 DAG、未發 Discord、未交易。

## 2026-10-05 07:27 台北：新聞 PIT 與成熟 outcome 交叉核對

- 最新自然 `news_ch_collect_process` run `scheduled__2026-10-04T15:30:00+00:00` 於 05:30–05:38 台北成功：12 feeds、1,307 URLs/parsed、1,306 valid-time、1,287 accepted、1 invalid-time quarantine、0 fetch/parse rejects、19 content/duplicate rejects；reconciliation delta/failure 均為 0，05:38:11 實際寫入 1,287 列。
- iThome 與 TNL 各有 2 列 cutoff 日期的時分未核（分別 58/88 列按日期精度屬較舊）；PTS 有 7 列較舊，鉅亨網有 56 列較舊。訊號卡將午夜/日期精度列顯示為「僅日期」，明示時分與時區未核、不代表嚴格 48 小時；新聞只作背景、不加方向票。查詢以 `create_time <= observed_at` 限定掃描前已收錄內容。本次未發現需要放寬或改寫 PIT 的證據。
- 最新 daily signal 仍為 `as_of_date=2026-10-02`、`ready/partial`：universe 1,946、price 1,938、flow 1,891、50 candidates、0 notified。10/05 07:00 自然盤中 snapshot 是 pre-open：行情與方向家族 count 全為 0，coverage table 無列，candidate 0；不將缺列解讀為 0% coverage。
- `market_signal_outcomes` 當前只含 2026-09-24 H1：signal-close 81 rows / 50 hits / 平均 excess +1.776pp；next-close 81 / 37 hits / +0.987pp。H5/H10 尚無成熟列；H5 待 10/05 收盤後 daily scan 第一次可結算，next-close H5 還要等下一交易日。單一 H1 cohort 不用來調分數或門檻。`market_signal_run_market_coverage` 目前 0 rows，待新的自然 daily run 寫入。

## 2026-10-05 07:21 台北：收盤個股行情失敗根因與自然修復待驗

- 最新 after-close 自然 run 仍是 `scheduled__2026-10-01T12:00:00+00:00`（10/02 20:00 台北）failed。Airflow task audit 定位到 `ingest_approved_source` map index 9；該 run 的 discovery XCom 對應 `tpex_equity_quotes_daily`。第二次 attempt 的錯誤為 `ChunkedEncodingError / IncompleteRead`：已讀 4,193,897 bytes，但串流尚有 472,744 bytes 未收到；堆疊在 payload streaming 時中止，不是 parser/schema reject。
- 現行 host 與 scheduler container 的 `adapters.py` SHA-256 相同（`bd084dd9f61f9236fb4e7542c6625aa000d82c1fb8393b8018f6c6c83bfae079`）。目前 `tpex_equity_quotes_daily` 已走 allowlisted fixed-byte-range reader：identity encoding、每段驗證 `Content-Range`/`Content-Length` 及 ETag/Last-Modified，64 KiB 分段且每段最多 3 次讀取。這表示修正已載入 scheduler，但仍未由 after-close 自然批次 runtime proof；不以靜態程式碼宣稱端點已恢復。
- After-close DAG active/unpaused，Airflow metadata `next_dagrun_create_after=2026-10-05 12:00:00+00:00`（20:00 台北）；尚未到期。自然 `ensure_schema_and_catalog` 已使下一輪收盤 discovery 含 24 keys，當中包括新加入的 TPEx 持股快照與 TAIFEX 大額交易人部位 feed。今晚只檢查自然 run 的每來源 HTTP/task 狀態、effective date、accepted/quarantined/duplicate、上市櫃配對及通知 receipt，不手動觸發。
- 靜態檢查：scheduler 內 `adapters.py` AST parse `AST_OK`，Airflow DAG import error count 0。未改程式、未跑測試、未手動觸發 DAG、未寫業務 DB、未送 Discord、未交易。

## 2026-10-05 07:00 台北：自然盤中批次稽核與收盤來源自動登錄

- 自然 `source_extension_collect_intraday` run `scheduled__2026-10-02T14:45:00+00:00` 於 10/05 07:00:00–07:00:18 台北執行完成；10 個動態來源均回報成功，無 missing key 或 contract drift，但 audit 是 `all_expected_tasks_succeeded_data_review_required`，所以不能把 DAG 綠燈當成資料完整。按 source audit 的 normalized rows / accepted / quarantined / duplicate，以及有效日與目前普通股主檔配對如下；未配對維持 unknown。

  | source_key | rows / accepted / quarantine / duplicate | effective date | listed / OTC matched | quality note |
  |---|---:|---|---:|---|
  | tpex_company_events | 4 / 4 / 0 / 0 | 2026-10-03 | 0 / 4 | 無旗標 |
  | tpex_insider_transfer_planned | 1 / 1 / 0 / 0 | 2026-10-02 | 0 / 1 | 無旗標 |
  | tpex_insider_transfer_untransferred | 1 / 0 / 1 / 0 | normalized date NULL；原始日期 2026-10-02 | 0 / 0 | 空白證券/股數欄隔離；不可解讀成零事件 |
  | tpex_monthly_revenue | 892 / 892 / 0 / 0 | 2026-08-01 | 0 / 891 | 上櫃 891/892（99.89%），1 未配對 |
  | tpex_quarterly_forecast_variance_events | 1 / 1 / 0 / 0 | 2026-10-05 | 0 / 0 | 僅 snapshot metadata，沒有公司事件列 |
  | twse_company_events | 3 / 3 / 0 / 0 | 2026-10-04 | 2 / 0 | 1 accepted code 不在普通股主檔 |
  | twse_insider_transfer_planned | 1 / 1 / 0 / 0 | 2026-10-02 | 1 / 0 | 無旗標 |
  | twse_insider_transfer_untransferred | 1 / 0 / 1 / 0 | normalized date NULL；原始日期 2026-10-02 | 0 / 0 | 空白證券/股數欄隔離；不可解讀成零事件 |
  | twse_monthly_revenue | 1,086 / 1,086 / 0 / 0 | 2026-08-01 | 1,054 / 0 | 上市 1,054/1,054（100%），32 accepted codes 不在普通股主檔 |
  | twse_quarterly_forecast_variance | 7 / 7 / 0 / 0 | 2026-06-30 | 5 / 0 | 事件 feed 的匹配列數不是市場覆蓋率 |

- 最新盤中 market-signal child run 自然成功，`as_of_date=2026-10-05`、`status=ready`、`source_status=partial`；universe、price、flow、revenue、financial、borrow、insider、dividend 均為 0，coverage table 無列，候選 0。通知 child 的結果是 candidates/sent/failed = 0/0/0；沒有產生 Discord 通知。此時為開盤前快照，不能當成當日行情訊號或覆蓋率為 0%。
- 兩個新官方批次鍵 `tpex_foreign_investor_holdings_daily`、`taifex_futures_large_trader_positions_daily` 已由 07:00 自然 `ensure_source_extension_schema` 寫入 source catalog；資料列為 approved、enabled、`daily_after_close`，updated_at 07:00:01 台北。唯讀 `discover_approved_source_keys("daily_after_close")` 現在返回 24 keys，兩個新鍵均在其中；目前兩鍵 `source_runs`/`source_records` 仍為 0，表示已自動登錄、尚未收集。下一驗收點是 10/05 20:00 台北自然 after-close run，不手動觸發或補 bootstrap。
- 兩市場的未轉讓來源在自然批次各回一列：日期與市場欄有值，但證券代號、公司名、未轉讓股數及理由欄空白，故目前保留隔離。TWSE 官方 OpenAPI 當前端點也回一列相同空白形狀（[TWSE OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap13_L)）；TPEx 的自然批次 payload 形狀相符，但本次網頁讀取工具無法開啟其 endpoint。缺少 TPEx 明確「無資料占位列」契約前，不改成 accepted no-event、不計零量，也不移除隔離旗標。
- after-close DAG 目前 active/unpaused，metadata 的 `next_dagrun_create_after` 為 2026-10-05 20:00 台北；最新自然 run 仍是 10/02 failed。沒有改程式、門檻或 scoring；本輪只有讀取自然 XCom/metadata/SQL 與官方 endpoint，沒有手動觸發 DAG、寫業務 DB、發 Discord、交易或跑測試。

## 2026-10-05 06:50 台北：兩個官方批次來源尚待自然註冊與覆蓋驗證

- 現行 `registry.py` 已定義並核准/啟用 `tpex_foreign_investor_holdings_daily` 與 `taifex_futures_large_trader_positions_daily`，但 crawler `source_registry` 目前 57 rows，兩個 key 都不在 catalog；`source_runs`/`source_records` 也都尚無這兩個 key 的列。host 與 Airflow scheduler 的 `registry.py` SHA-256 相同；檔案於 05:37 台北修改，而 catalog 最新更新為 10/05 04:02 台北，故缺列符合「最近自然 bootstrap 早於 source definition 修改」的時序，不表示來源拒收或端點失敗。
- 在 scheduler 容器以唯讀 `discover_approved_source_keys("daily_after_close")` 核現況：本機 24 個 runtime definitions 對 catalog 22 個 approved keys，唯一缺少的正是上述兩 key。DAG 順序為 `ensure_schema_task >> discover_task`，`_bootstrap_catalog()` 遍歷 source definitions；兩項來源分數都高於 75 的自動登錄門檻，因此今晚自然 bootstrap 預期會使 discovery 對齊 24；回執到來前不當成已註冊或已收集。
- TPEx 政府資料集 [20214](https://data.gov.tw/dataset/20214) 標示每日更新、免費、政府資料開放授權第 1 版，欄位含排行、代碼、發行股數、僑外資及陸資持有股數/持股比例。這是上櫃持股結構快照，不是當日外資買賣流；來源未提供發稿時間，程式應要求有效日等於候選日並保持 context-only。資料資源連結本輪讀取工具回 403，尚無 live rows，完整度須等自然收集後與上櫃普通股主檔 892 檔核對；未核前不宣稱全市場覆蓋。
- TAIFEX 政府資料集 [11337](https://data.gov.tw/dataset/11337) 標示免費、政府資料開放授權第 1 版，欄位含契約、到期月份、前五/前十大交易人買賣方未平倉數及全市場未平倉數；平台中繼資料標示「每 1 月」，而程式註記官方 OpenAPI 曾回傳日資料，頻率語義衝突尚待自然 `effective_date` 驗證。這是期貨市場彙總，不能映射成個股外資流或個股方向票，先只供市場背景。
- Airflow metadata 於 06:47 顯示 after-close 下一排程為 10/05 20:00 台北；DAG 明確依序跑 `ensure_schema_and_catalog()` 再 `discover_approved_source_keys()`，bootstrap 會遍歷全部 source definitions。兩來源現行 priority 分數都高於 75 自動登錄門檻，因此今晚自然 run 可新增 catalog row，毋須手動寫 DB。屆時檢查兩 keys 的註冊、task/HTTP、effective date、raw/accepted/quarantined/duplicate、上市櫃配對；TPEx holdings 若非完整市場快照，保留 ranking-context 語義。不可手動 bootstrap/觸發來製造回執，不因欄位多就加入 directional score。

## 2026-10-05 06:47 台北：新聞 PIT join 的有界存取路徑

- 對 `_attach_recent_news_context()` 兩段 SQL 做唯讀 `EXPLAIN`：查詢先以 `create_time <= observed_at` 封住掃描時點，沿 `PRIMARY(id)` 反向走最新列並設 `LIMIT 10,000`，外層再依 `created_at` 取樣；optimizer 預估 `rows=10,000`、`filtered=6.92%`，不是對 690 萬列整表掃描。兩段查詢均使用相同有界路徑，現有訊號新聞 join 無需因上一輪自製彙總查詢而改程式。
- 上一段被取消的三個查詢是按來源/時間彙總整個新聞表，表上沒有 `create_time`/`created_at` 索引；這項限制只適用於該類 ad-hoc 彙總。巡檢繼續優先使用自然批次 XCom/回執及主鍵尾端小量抽樣，不能把被取消的彙總誤判為生產 PIT join 的全表掃描。
- 日期精度新聞仍不能推斷小時級發稿時間；`created_at` 保留作文章來源時間欄，`create_time` 只作 ingest/PIT 上界。沒有程式、DB、scoring 或門檻變更，也沒有跑測試。
- 06:47 後再次讀 Airflow metadata：after-close 和 intraday 均 active、未 pause，DAG import errors 為空；after-close 尚未到 10/05 20:00 台北排程，最新 run 仍為 10/02 20:00 failed；intraday 預計 07:00 才建立今日排程。這是目前時刻的待到期狀態，不當成今日自然 run 失敗。

## 2026-10-05 06:44 台北：成熟 outcome horizon 的自然交易日界

- 檢查 `market_signals.py` 後確認 outcome horizons 是 1/5/10 個交易日，且只在 daily scan 中以該次 `as_of_date` 結算；尚未到期的 horizon 會略過，只有已出現 exit 交易日但缺個股/基準價格時才寫 `pending_data`。
- 用 `(交易日, 證券代號)` 索引查 legacy 個股日線表的日期索引，9/24 訊號後截至最新 daily `as_of_date=10/02` 僅有 9/29、9/30、10/01、10/02 四個後續交易日。故 9/24 horizon-5 在 10/02 scan 尚未到期，缺少 h5 rows 不是目前證據能指向的結算漏跑；h10 更未到期。
- 若今晚自然批次含 10/05 有效收盤資料，signal-close h5 將首次達到五個後續交易日；next-close h5 依序列最早要等 10/06 有效收盤資料。等自然 daily scan 核 effective date、outcome basis/horizon、settled/pending_data counts；不手動結算、不因尚未成熟改門檻。

## 2026-10-05 06:35 台北：批次排程與訊號新鮮度核對

- 本次沿用全市場、自動化範圍：核對自然 DAG、來源有效日/接受與隔離、上市櫃覆蓋、新聞發稿時間、PIT 關聯、成熟 outcomes 與通知回執；不做逐家公司券商報告評比。Airflow `list-import-errors` 無結果，after-close、process、intraday DAG 均 active 且未 pause。
- 06:35 台北時 after-close 尚未到今天排程。Airflow `next_dagrun_create_after` 對應 10/05 20:00 台北；intraday 下一排程為 10/05 07:00，process 下一排程為 10/06 04:00。上一個 after-close 自然 run 仍是 10/02 20:00 失敗批次，TPEx 個股日線下載的有界 Range 修正尚待今晚自然 run 驗證；不手動觸發。
- 最近成功 process 批次為 10/05 04:00 台北，23/23 runtime sources 回報、無缺 key/contract drift；每來源有效日、接受/隔離與普通股主檔上市櫃配對見本文件 06:10 表。成功 task 仍不等於完整市場覆蓋，未匹配代碼維持 unknown。
- 最新 daily signal 為 10/02 `ready/partial`：universe 1,946、price 1,938、flow 1,891、revenue 1,945、financial 1,902、borrow 1,795、insider 1,944、dividend 1,053、50 candidates、0 notified。最新 intraday snapshot 10/04 08:04 僅事件背景、5 candidates，個股行情/法人/營收/財務 family 均為 0。該 snapshot 最近 6 筆通知列均有平台 receipt 且 HTTP 200；市場分項覆蓋表仍是 0 rows，等下一次自然 daily scan。
- `market_signal_outcomes` 仍只有 2026-09-24 的 horizon-1 成熟樣本：signal-close 81 rows / 80 scored / 50 hits；next-close 81 / 80 / 37。尚無成熟 h5/h10，也未納入公司行動及交易成本；不依單日調分或門檻。
- 最近新聞自然 run 仍是 10/05 05:30–05:38 台北：12 feeds、1,307 URLs/parsed、1,306 有效時間、1,287 accepted；來源級 counts 與 reject 細節見 06:10 段落。唯讀抽樣最新寫入的 TNL rows 顯示 ingest 為 05:38:11，但 `created_at` 僅有 2026-01 至 03 的日期時間；這類日期級時間不作發稿小時或即時 freshness。`news_ch.news` 約 6.9M rows，索引只有主鍵與 `(source, article_id)`；三個按來源/時間彙總的唯讀查詢因長掃描已取消，後續巡檢改用 Airflow XCom/自然 run 回執及有索引的小量抽樣，不做全表 freshness 掃描。
- 本次只有 Airflow/MySQL 唯讀查詢及文件紀錄；未手動觸發或重跑 DAG、未寫業務 DB、未發 Discord、未交易、未改 scoring/threshold、未跑測試。沒有新且可量化的全市場方向訊號可納入。

> **固定工作範圍（使用者再次校正）**：目標是全市場、可排程、可批次取得、可驗證覆蓋與有效日、可累積 outcomes 的訊號系統。工作優先順序是巡檢自然收集、修復批次來源、接入官方或明確授權的批次 feed、改善新聞/市場資料的點時關聯及自動驗證。不得把逐家公司搜尋券商報告、人工評分或彙總當成工作主線。新來源資格檢查只確認批次端點、授權、時間戳、歷史版本與市場覆蓋是否具備；無法確認就記缺口並停止，不延伸成手動報告調查。券商研究只有在存在授權批次 feed 時納入。

## 2026-10-05 06:10 台北：自然批次、新聞與訊號新鮮度核對

- 唯讀查 Airflow、source-run audit、crawler DB 與新聞 XCom。`airflow dags list-import-errors` 為空，相關 collect/scan DAG 未 pause。
- `source_extension_collect_process` 自然 run `scheduled__2026-10-03T20:00:00+00:00` 於 10/05 04:00 台北成功。source-run audit 為 `all_expected_tasks_succeeded_data_review_required`：runtime/registry 預期 23、回報 23，無缺 key 或 contract drift；所有來源 task 成功，但輸出仍有隔離列或主檔外代碼品質標記。上市普通股主檔 1,054、上櫃 892。下表的列數依序為 normalized / accepted / quarantined / duplicate；「配對」是目前普通股主檔代碼命中數，不代表來源本身的市場完整率。未配對代碼保持 unknown。

  | source_key | rows / accepted / quarantine / duplicate | effective date | listed / OTC matched | unmatched |
  |---|---:|---|---:|---:|
  | cbc_fx_daily | 3,660 / 3,660 / 0 / 0 | 2025-09-30–2026-09-30 | n/a | 0 |
  | tpex_balance_sheet_bd | 7 / 7 / 0 / 0 | 2026-06-30 | 0 / 7 | 0 |
  | tpex_balance_sheet_ci | 884 / 884 / 0 / 0 | 2026-06-30 | 0 / 884 | 0 |
  | tpex_daytrade_eligibility_daily | 841 / 841 / 0 / 0 | 2026-10-05 | 0 / 725 | 116 |
  | tpex_dividends | 963 / 963 / 0 / 0 | 2026-01-29–2026-10-02 | 0 / 890 | 2 |
  | tpex_income_statement_bd | 7 / 7 / 0 / 0 | 2026-06-30 | 0 / 7 | 0 |
  | tpex_income_statement_ci | 884 / 884 / 0 / 0 | 2026-06-30 | 0 / 884 | 0 |
  | tpex_quarterly_profitability | 884 / 884 / 0 / 0 | 2026-06-30 | 0 / 884 | 0 |
  | twse_balance_sheet_basi | 10 / 10 / 0 / 0 | 2026-06-30 | 10 / 0 | 0 |
  | twse_balance_sheet_bd | 3 / 3 / 0 / 0 | 2026-06-30 | 3 / 0 | 0 |
  | twse_balance_sheet_ci | 1,049 / 1,049 / 0 / 0 | 2026-06-30 | 1,018 / 0 | 31 |
  | twse_balance_sheet_fh | 13 / 13 / 0 / 0 | 2026-06-30 | 13 / 0 | 0 |
  | twse_balance_sheet_ins | 5 / 5 / 0 / 0 | 2026-06-30 | 5 / 0 | 0 |
  | twse_balance_sheet_mim | 4 / 4 / 0 / 0 | 2026-06-30 | 4 / 0 | 0 |
  | twse_daytrade_eligibility_daily | 1,228 / 1,228 / 0 / 0 | 2026-10-05 | 972 / 0 | 256 |
  | twse_dividends | 1,228 / 1,228 / 0 / 0 | 2026-01-30–2026-09-21 | 1,053 / 0 | 59 |
  | twse_income_statement_basi | 10 / 10 / 0 / 0 | 2026-06-30 | 10 / 0 | 0 |
  | twse_income_statement_bd | 3 / 3 / 0 / 0 | 2026-06-30 | 3 / 0 | 0 |
  | twse_income_statement_ci | 1,049 / 1,049 / 0 / 0 | 2026-06-30 | 1,018 / 0 | 31 |
  | twse_income_statement_fh | 13 / 13 / 0 / 0 | 2026-06-30 | 13 / 0 | 0 |
  | twse_income_statement_ins | 5 / 5 / 0 / 0 | 2026-06-30 | 5 / 0 | 0 |
  | twse_income_statement_mim | 4 / 4 / 0 / 0 | 2026-06-30 | 4 / 0 | 0 |
  | twse_quarterly_profitability | 1,054 / 1,049 / 5 / 0 | 2026-06-30 | 1,018 / 0 | 31 |

- 當沖來源是每日資格名單，不是全市場母體，也不是多空量訊號。TWSE/TPEx 分別有 972/725 個上市/上櫃普通股代碼命中；256/116 個接受代碼不在目前普通股主檔，不能據此推成不具資格或零量。財報列外代碼同樣保留 unknown；TWSE 季營益 5 列隔離、1,018/1,054 上市普通股可配對，TPEx 季營益 884/892 上櫃普通股可配對。TWSE 股利來源最新有效日 09-21，雖 task 成功仍需視為較舊資料。
- 最新日線 signal 仍是 10/02 `ready/partial`：1,946 universe、1,938 price、1,891 flow、50 candidates、0 notified。最新盤中 run 是 10/04 08:04 event-only：5 candidates、universe/price/flow/revenue/financial 都為 0；notification ledger 6/6 有 platform receipt、HTTP 200。`market_signal_run_market_coverage` 現已存在但仍 0 rows，需等新的自然 daily scan 填入上市/上櫃覆蓋。`market_signal_outcomes` 仍只有 2026-09-24 horizon-1（signal-close 50/80、next-close 37/80）；無成熟 h5/h10，不調門檻。
- 新聞自然 run `scheduled__2026-10-04T15:30:00+00:00` 於 10/05 05:30–05:38 台北成功：12 feeds、1,307 URLs/parsed、1,306 有效時間、1,287 accepted 並於 05:38:11 寫入、0 fetch/parse rejects、1 時間隔離、19 內容/重複拒收；post-time rejection reconciliation delta/failure 都為 0。TVBS、自由時報、鉅亨網為 partial；iThome/TNL 為日期級 cutoff 時間未驗，公視/鉅亨網 freshness 混有新舊。新批次時間晚於目前盤中快照，僅供下一次 PIT scan，不回填舊卡；新聞仍是背景，不另算方向票。
- `source_extension_collect_after_close` 最新自然 run 仍是 10/02 20:00 台北的 `scheduled__2026-10-01T12:00:00+00:00`，Airflow failed；舊 `source_runs` 17 列缺 `dag_id`，audit 維持 `unknown`。目前 after-close runtime 預期 24 keys、catalog 22，缺 `taifex_futures_large_trader_positions_daily` 和 `tpex_foreign_investor_holdings_daily`；今晚 20:00 自然 run 再核來源回執、effective date、隔離/重複與雙市場配對，不手動觸發。
- 本輪僅 SELECT/metadata/XCom 唯讀並更新追蹤紀錄；未手動觸發 DAG、寫業務 DB、發 Discord、交易或跑測試。

## 2026-10-05 06:14 台北：after-close 舊失敗根因與新版批次程式待驗

- 根因已由 10/02 Airflow mapped task states 和 `source_runs.error_message` 確認：TPEx 個股日行情 map index 9 的兩次下載都在 HTTP body 傳輸中斷，分別只收到 704,105/4,666,641 bytes 及 4,193,897/4,666,641 bytes，錯誤為 `ChunkedEncodingError`，因此兩次都是 0 rows、0 accepted，mapped task 失敗並使 `verify_collection_and_triggers` 失敗。`tpex_company_events` 另有一次 MySQL 1213 deadlock，但第二次成功；它不是 parent 最終失敗原因。舊 `source_runs.dag_id` 缺值只妨礙 audit lineage，不是下載失敗的原因。
- 現行 `adapters.py` 在 10/05 05:39 修改，host、Airflow worker、scheduler SHA-256 一致。程式已把 TPEx quotes 加入官方端點 byte-range allowlist，使用 64 KiB ranges、每段最多 3 次嘗試，僅接受完整 HTTP 206 range 並檢查表示檔 validator；這代表修正已在當前容器程式碼，不代表自然資料批次已成功。尚無修正後自然 after-close 回執，今晚只以自然 run 核最終 HTTP、range bytes、effective date、接受/隔離/重複與上市/上櫃配對。
- 今日 04:00 process schema/bootstrap run 早於 `registry.py` 05:37 修改。現行 `ensure_schema_and_catalog()` 會在自然 DAG 中呼叫 `_bootstrap_catalog()` 並遍歷 `SOURCE_DEFINITIONS`；因此 after-close audit 的兩個缺 catalog keys（`taifex_futures_large_trader_positions_daily`、`tpex_foreign_investor_holdings_daily`）與定義晚於最近 bootstrap 的時間吻合。這個解釋待今晚自然 schema/bootstrap 和 source audit 驗證，不手動 bootstrap 或補跑。
- 本次只讀 task metadata、source-run error 欄與目前程式；沒有更改 collector 程式或執行測試。

## 2026-10-05 01:01 台北：最新自然批次、新聞與通知回執交叉核對

- 唯讀查 Airflow `DagModel`、`DagRun`、crawler `source_runs`/`source_records`、訊號、outcomes、通知與新聞 XCom。訊號、新聞、TDCC、process、after-close DAG 都未 pause；scheduler `list-import-errors` 為空。尚未到今天 04:00 process、05:30 news、09:00 TDCC、20:00 after-close 台北自然排程，不手動補跑。
- 最新 process logical run `scheduled__2026-10-02T20:00:00+00:00` 於 10/04 04:00 台北執行，Airflow success、稽核 partial：21 個登錄來源對 23 個 runtime 來源，缺上市/上櫃當沖資格兩個 source keys；TWSE、TPEx 季營益來源仍有 registry config drift。所有 21 個 mapped source rows 均逐項檢查；下表欄位為來源列數、接受/隔離/重複、有效日範圍、目前普通股 master 命中上市/上櫃數與未配對數：

  當前 common-equity master 分母為上市 1,054、上櫃 892。以下是來源列與目前主檔配對計數；不同財報子表本來就有不同列範圍，不能把列數差額當成缺報或零值，未配對代碼保持 unknown。

  | source_key | rows | accepted / quarantine / duplicate | effective date | listed / OTC matched; unmatched |
  |---|---:|---:|---|---:|
  | cbc_fx_daily | 3,660 | 3,660 / 0 / 0 | 2025-09-30–2026-09-30 | n/a |
  | tpex_balance_sheet_bd | 7 | 7 / 0 / 0 | 2026-06-30 | 0 / 7; 0 |
  | tpex_balance_sheet_ci | 884 | 884 / 0 / 0 | 2026-06-30 | 0 / 884; 0 |
  | tpex_dividends | 2,483 | 2,483 / 0 / 0 | 2019-01-18–2021-08-03 | 9 / 748; 36 |
  | tpex_income_statement_bd | 7 | 7 / 0 / 0 | 2026-06-30 | 0 / 7; 0 |
  | tpex_income_statement_ci | 884 | 884 / 0 / 0 | 2026-06-30 | 0 / 884; 0 |
  | tpex_quarterly_profitability | 884 | 884 / 0 / 0 | 2026-06-30 | 0 / 884; 0 |
  | twse_balance_sheet_basi | 10 | 10 / 0 / 0 | 2026-06-30 | 10 / 0; 0 |
  | twse_balance_sheet_bd | 3 | 3 / 0 / 0 | 2026-06-30 | 3 / 0; 0 |
  | twse_balance_sheet_ci | 1,049 | 1,049 / 0 / 0 | 2026-06-30 | 1,018 / 0; 31 |
  | twse_balance_sheet_fh | 13 | 13 / 0 / 0 | 2026-06-30 | 13 / 0; 0 |
  | twse_balance_sheet_ins | 5 | 5 / 0 / 0 | 2026-06-30 | 5 / 0; 0 |
  | twse_balance_sheet_mim | 4 | 4 / 0 / 0 | 2026-06-30 | 4 / 0; 0 |
  | twse_dividends | 1,228 | 1,228 / 0 / 0 | 2026-01-30–2026-09-21 | 1,053 / 0; 59 |
  | twse_income_statement_basi | 10 | 10 / 0 / 0 | 2026-06-30 | 10 / 0; 0 |
  | twse_income_statement_bd | 3 | 3 / 0 / 0 | 2026-06-30 | 3 / 0; 0 |
  | twse_income_statement_ci | 1,049 | 1,049 / 0 / 0 | 2026-06-30 | 1,018 / 0; 31 |
  | twse_income_statement_fh | 13 | 13 / 0 / 0 | 2026-06-30 | 13 / 0; 0 |
  | twse_income_statement_ins | 5 | 5 / 0 / 0 | 2026-06-30 | 5 / 0; 0 |
  | twse_income_statement_mim | 4 | 4 / 0 / 0 | 2026-06-30 | 4 / 0; 0 |
  | twse_quarterly_profitability | 1,054 | 1,049 / 5 / 0 | 2026-06-30 | 1,018 / 0; 31 |

  每列未配對均保持 unknown。TPEx 股利雖 2,483/2,483 接受，最新事件時間仍是 2021-08-04；TWSE/TPEx 財報季別為 2026-06-30，TWSE 季營益有 5 列隔離及 31 個接受代碼未配對。
- 最新 after-close logical run `scheduled__2026-10-01T12:00:00+00:00`（10/02 20:00 台北）仍 failed。Airflow `discover_approved_sources` XCom 列出 11 個 map keys，map index 9（TPEx 個股日線行情）failed、parent verifier failed；但舊 `source_runs` 有 17 列、15 個 source keys 缺 `dag_id`，來源 audit 維持 `unknown`，不把這些孤兒列猜配到 DAG。DB catalog 登錄 17、runtime 預期 20，缺 `tpex_daytrade_market_summary_daily` 及兩個新報酬指數 source keys；目前兩個報酬指數新 keys 在 `source_records` 尚為 0 列。
- 此舊 parent 觸發的 daily child `market_signal__daily__scheduled__2026-10-01T12:00:00+00:00` Airflow failed：snapshot task success、notification task 三次後 failed。對應 10/02 daily `ready/partial`：1,946 universe、1,938 價格、1,891 法人流、1,945 營收、1,902 財務、1,795 借券、1,944 董監、1,053 股利、50 候選、0 notified；notification ledger 一列 failed、一列 sent，但兩列都沒有 platform receipt。舊 child 的 `source_dag_id` 為 NULL，後續只按目前新 run-id 契約驗 lineage。
- 最新 intraday snapshot 10/04 08:04：5 個事件觀察候選，行情/法人/營收/財務家族皆 0；對應 6/6 通知有平台 receipt、HTTP 200。最新成熟 outcomes 仍只有 2026-09-24 horizon-1：signal-close 81 rows/80 scored/50 hits，next-close 81/80/37；沒有成熟 h5/h10，不據此調分或門檻。市場覆蓋表 `market_signal_run_market_coverage` 仍不存在；既有普通價格指數僅 TWSE 21 日（最新 10/01）、TPEx 22 日（最新 10/02）。
- 最新新聞自然 run `scheduled__2026-10-04T10:30:00+00:00` 於 10/04 23:30–23:40 台北成功：12 feeds、1,298 URLs、1,297 parsed/有效時間、1,270 accepted、1 fetch/parse reject、0 時間隔離、27 內容拒收、post-time reconciliation delta/failure 均 0。三立 29/30、自由時報 73/100 為 partial；iThome 38 篇較舊、20 篇 cutoff 日時間未驗、2 篇日期較新；TNL 82 篇較舊、6 篇 cutoff 日時間未驗、2 篇日期較新。這些時間精度只作新聞背景，不補回舊候選。
- 本次僅 SELECT/metadata/XCom 唯讀；未手動觸發/重跑 DAG、寫業務 DB、發 Discord、交易或跑測試。等今天自然 04:00 schema/bootstrap 驗 process catalog，20:00 再逐源驗 after-close effective date、列數及覆蓋；新總報酬指數尚無自然落庫/通知證據。

## 2026-10-05 00:52 台北：加入上市/上櫃官方報酬指數的自動市場背景

- 新增兩個 `daily_after_close` 全市場來源：TWSE `twse_market_total_return_index_daily`（OpenAPI `MFI94U`，資料集 [11871](https://data.gov.tw/dataset/11871)）和 TPEx `tpex_market_total_return_index_daily`（OpenAPI `tpex_reward_index`，資料集 [20219](https://data.gov.tw/dataset/20219)）。官方資料頁列每日更新、免費、政府資料開放授權第 1 版；TWSE 資料集明確說明配股配息再投資，TPEx 資料欄位列「櫃買報酬指數」。
- 兩個端點只供最新快照，不具可用的歷史日期查詢；既有日指數表目前僅累積 21/22 個有效日。因此不宣稱已有長期回測樣本；新版自然收盤批次將按有效交易日累積，不人工回補或手動觸發。
- daily notification 只在資料列品質為 `ok`、有效日等於掃描日、`updated_at` 不晚於掃描開始，且有 6 個不同有效交易日值時，才顯示上市/上櫃各自的五日報酬指數變化。尚未累積 6 點、資料過舊、隔日或無列均明確顯示不完整/未對齊，不補零。這是市場背景，不加入個股分數或改變候選門檻。
- 保留現行個股原始收盤價 outcome 與原始價格指數基準；總報酬指數不可直接替換基準，直到個股價格也具可比的公司行動調整。此次不調權重/門檻、不跑測試、不寫 DB、不發 Discord；等待下一次自然 after-close 驗收兩個新來源的有效日、原始/接受/隔離/重複數及台股兩市場 feed 覆蓋，再確認 daily card PIT gate。

## 2026-10-05 00:29 台北：全市場歷史行情可用性與回測風險

- 唯讀掃描 `stock.個股成交資訊` 得 6,050,865 列、4,172 個資料日期。以有效 Gregorian 年（2000–2026）範圍計為 6,050,802 列、4,118 日，日期 2009-01-05 至 2026-10-02；另 63 列落在資料庫年欄 `0114`/`0115`（54 個日期）。不將 ROC 年值自動轉成 Gregorian 年；任何回測先明確排除並查來源轉換規則。
- 依程式目前普通股範圍 `CFICode LIKE 'ES%'`，主檔上市 1,054（其中 ESVUFR 1,053、ESVTFR 1）、上櫃 892。10/02 legacy 行情表 1,938 個代碼列：上市 1,050/1,054（可解析正收盤 1,050、4 代碼當日未列）；上櫃 888/892（20 列收盤為 NULL、4 代碼未列，正收盤 868）。分市場有效正收盤覆蓋是上市 99.62%、上櫃 97.31%；缺列和 NULL 價格都保留未知，不補零。這是 legacy 行情表快照 profile，不是可歸屬的 `source_runs` 接受/隔離回執。
- `_settle_daily_candidate_outcomes` 目前用個股原始收盤價計 close-to-close 報酬，再扣市場指數報酬；沒有公司行動調整或交易成本。6M 筆資料列只足以支持探索性 profile；日期異常 `0114/0115`、歷史上市櫃母體/存續者偏差、公司行動、交易成本與僅 21/22 日的指數歷史都使它不足以支持可靠回測。既有 outcomes 只有一個訊號日，現階段不以它調分或門檻。
- 本次僅讀取行情資料與程式；未回寫 DB、手動觸發 DAG、發通知、交易或跑測試。此發現用來建立後續自動化因子研究的資料准入條件，不等同新訊號已驗證。

## 2026-10-05 00:16 台北：重新核對 live 批次管線、快照與自然排程

- 查詢的四個 Airflow DAG（process、after-close、news、TDCC Monday refresh）均未 pause。最新 `source_extension_collect_process` logical run `scheduled__2026-10-02T20:00:00+00:00` 實際於 10/04 04:00 台北執行並 success；唯讀 audit 為 `partial`：舊 catalog 21/21 有回執、runtime 預期 23，尚缺 `twse_daytrade_eligibility_daily`、`tpex_daytrade_eligibility_daily`。兩個季獲利來源仍有 registry config contract drift：TPEx 884/884 accepted、884/892 OTC 普通股匹配；TWSE 1,054 raw、1,049 accepted、5 quarantine、1,018/1,054 上市普通股匹配，31 個已接受代碼未匹配主檔，維持 unknown。資料有效季度為 2026-06-30。當時整批成功不證明新 runtime contract 已跑過。
- 最新 `source_extension_collect_after_close` logical run `scheduled__2026-10-01T12:00:00+00:00` failed。現行唯讀 audit 對應到 17 筆缺 `dag_id` 的舊 `source_runs`（15 個來源鍵），所以狀態保持 `unknown`、拒絕猜歸屬；runtime 預期 18 個來源，catalog 17，缺 `tpex_daytrade_market_summary_daily`。等 10/05 20:00 台北自然 after-close，核新 lineage、每源有效日與列數後再判讀，不手動重跑。
- Crawler DB 最新 daily snapshot 仍是 as-of 10/02：`ready/partial`，1,946 universe、1,938 price、1,891 flow、1,945 revenue、1,902 financial、1,795 borrow、1,944 insider、1,053 dividend、50 candidates、0 notified。最新 intraday 為 10/04 08:04：5 個事件觀察候選、price/flow/revenue/financial 皆 0；通知帳本 6/6 有平台 receipt、HTTP 200。這不是方向型行情訊號。`market_signal_outcomes` 仍只有 9/24 h1：signal-close 與 next-close 各 81 rows、80 directional rows、命中 50/80 與 37/80；沒有其他訊號日或成熟 h5/h10，不調權重/門檻。通知帳本總計 131 個 `sent` 只有 6 個 receipt，32 個 `failed` 沒有 receipt，狀態字樣不代替平台回執。
- 最新新聞自然 run `news_ch_collect_process / scheduled__2026-10-04T10:30:00+00:00` success，實際 10/04 23:30–23:40 台北：12 feeds、1,298 URLs、1,297 parsed/valid-time、1,270 accepted、1 fetch/parse reject、0 invalid-time quarantine、27 content rejects，拒收 reconciliation delta/failure 均 0；三立與自由時報 partial。TNL/iThome 有日期級時間精度限制；這批時間是 crawler 記錄欄位，不等同逐篇原文頁/時區驗證。它晚於 08:04 intraday snapshot，不回填舊候選。
- Live `DagModel` 顯示自然排程未 pause：process 預計 10/05 04:00 台北建立、news 05:30、TDCC 持股批次 09:00、after-close 20:00。現行程式有 `market_signal_run_market_coverage` DDL/寫入路徑，但 crawler DB 尚無該表；舊日線快照不能證明上市/上櫃分市場覆蓋表已啟用。待 04:00 schema/bootstrap 和 20:00 scan 由自然 run 建立並驗證雙市場列，不手動改 DB。
- 本次以 Airflow ORM/CLI、crawler SELECT 與 runtime source audit 讀取；未觸發 DAG、寫業務資料、發 Discord、交易或跑測試。工作仍只推進全市場自動批次訊號，不逐家公司線上搜尋或評比分報告。

## 2026-10-05 00:13 台北：最新新聞批次可供下一次 PIT 掃描，舊候選不回填

- Airflow 自然新聞 run `news_ch_collect_process / scheduled__2026-10-04T10:30:00+00:00` success，邏輯排程 10/04 18:30 台北、實際 23:30–23:40 台北。XCom freshness cutoff 為 10/02 23:40 台北；12 feeds、1,298 URLs、1,297 parsed/valid-time、1,270 accepted、1 fetch/parse reject、0 invalid-time quarantine、27 content rejects、0 post-time reconciliation delta/failure。三立新聞與自由時報為 partial，其餘 10 feed complete；自由時報 27 列因空內文拒收。
- 時間欄反映 crawler 保存的來源日期/時間與 `Asia/Taipei` 正規化，不等同逐篇原始頁及時區核實。最新盤中快照仍是 10/04 08:04，早於此批次，不回填舊快照；待 10/05 下一次自然盤中掃描按 scan-start cutoff 重查新聞匹配、未匹配、時間精度/驗證標籤與候選方向家族。媒體資料只作來源事實/背景，不單獨計方向票。

## 2026-10-05 00:04 台北：上市/上櫃當沖資格批次來源授權核實

- 政府資料開放平臺資料集 11661（上市）與 137680（上櫃）均列當沖標的的資料日期、證券代號/名稱及「暫停現股賣出後現款買進」註記，更新頻率每日，授權為政府資料開放授權條款第 1 版；兩份資料集的 CSV 資源分別指向 TWSE/TPEx 官方來源。這確認它們是可納入評估的全市場授權批次資料，語義只代表名單資格/限制，不是交易量或多空方向。
- runtime definitions 已有這兩個 source key，但本次 00:04 台北 SELECT 時 crawler `source_registry` 尚無其列；最近自然 daily run 仍是 10/04 04:00 台北，尚未經新版 schema bootstrap。待 10/05 自然 bootstrap/collection 核 registry/source_runs、effective date、原始/接受/隔離/拒收/重複列及上市/上櫃主檔覆蓋。訊號卡只能在同候選資料日、scan cutoff 前有已接受名單列時標示正向 membership/原始限制註記；缺列仍為 unknown，不能推成「不可當沖」。
- 尚未改動訊號邏輯或人工下載資料；來源授權與欄位確認來自官方資料集頁：[TWSE 11661](https://data.gov.tw/dataset/11661)、[TPEx 137680](https://data.gov.tw/dataset/137680)。自然 Airflow 端點/落庫仍待驗收。

## 2026-10-05 00:03 台北：區分舊 run 成功與新版 completeness gate 的自然驗證

- Airflow metadata 顯示最近 `source_extension_collect_process` run 的 verifier task 與 parent 為 success，舊 XCom 回傳 `status=complete`；但現行唯讀 `audit_source_run` 對該舊 run 回 `partial`（runtime 23、catalog 21）。檢查檔案修改時間：`dag.py` 22:31、`audit_runs.py` 22:36 台北，晚於該 run 04:00 執行。因此舊 run 不證明目前工作樹的 fail-closed completeness gate 已在 Airflow runtime 執行；不把回溯稽核結果倒稱成 runtime 失敗。
- 現行 verifier 僅接受完整 audit 狀態；`partial` 會讓 verifier failed。下一個自然 schema bootstrap 會重跑 catalog bootstrap，之後再以新 DagRun 的 mapped source keys、source_runs lineage 和 verifier XCom 驗證目前程式。未手動重跑。

## 2026-10-05 00:02 台北：融資背景的五日比較加上交易日對齊

- 讀目前 `market_signals.py`：融資/融券「近 5 日」背景以最新列和第 6 個 source row 比較，原先沒有確認六個 effective dates 是連續市場交易日。唯讀資料顯示 as-of 10/02 的六個報價交易日為上市 9/23、9/24、9/29、9/30、10/01、10/02；上市融資餘額 accepted 日期只有 9/28–10/02 五日，9/28 不是該日線報價交易日。上櫃六交易日為 9/22、9/23、9/24、9/29、9/30、10/01；上櫃融資餘額僅 9/24、9/29、9/30、10/01 四日。現有樣本不足以產生完整五日比較。
- 已在 `market_signals.py` 將背景變化改為比對市場級個股日線實際日期窗：必須有六個市場報價交易日、最新融資資料日落在該窗最後一日，且六日都有該股 accepted 融資資料，才會輸出五日變化。這只改背景欄位的資料完整性，不改方向家族、分數、排序或通知門檻；缺日不補值。
- 驗證：Airflow worker AST parse、module import 成功；本機與 worker SHA-256 相同 `dcc5987e44c303188be30b4295328aa11cebd2fdb6d4fc1d3ef559b128509f26`。對目前落庫日期唯讀試算，上市/上櫃 `can_emit_5_session_change` 均為 false，符合資料不足時隱藏比較的預期。未跑測試；等待下一個自然 daily scan 驗新分支。未手動觸發 DAG、寫業務 DB、發 Discord 或交易。

## 2026-10-04 23:47 台北：回到自動批次訊號的自然執行驗收

- 唯讀查 Airflow metadata 與 crawler DB：自然 `source_extension_collect_process` logical run `scheduled__2026-10-02T20:00:00+00:00` 於 10/04 04:00 台北執行並 success。收集稽核為 21/21 已登錄來源有回執，但 runtime 期望 23 個來源；缺 `twse_daytrade_eligibility_daily`、`tpex_daytrade_eligibility_daily` 兩個 catalog keys，故不能把 DAG success 或 21/21 解讀為完整。兩者仍需確認官方批次契約/授權，不拿個股查詢路徑補洞。
- 同批來源稽核：TPEx 季獲利有效日 2026-06-30，884/884 accepted，符合普通股主檔 884/892（99.10%）；TWSE 季獲利 1,054 raw、1,049 accepted、5 quarantine，1,018/1,054 主檔代碼配對（96.58%），其中 31 個 accepted 代碼未配主檔、另 5 個隔離。未配對/隔離不當零。TPEx 股利雖 2,483/2,483 accepted，最新有效日仍為 2021-08-03，屬過期資料而非新訊號。
- 最新自然 `source_extension_collect_after_close` logical run `scheduled__2026-10-01T12:00:00+00:00`（10/02 20:00 台北）仍 failed。17 筆舊 `source_runs` 缺 `dag_id`，稽核拒絕猜測歸屬；runtime 期望 18、DB catalog 17，缺 `tpex_daytrade_market_summary_daily`。等待下一次平日自然 bootstrap/after-close，以新 lineage、來源列與日期回執重驗。
- 最新 daily 訊號仍是 10/02 `ready/partial`（1,946 universe、1,938 price、1,891 flow、50 候選）。10/04 08:04 intraday 保存 5 張事件觀察卡，但 universe/price/flow/revenue/financial counts 均為 0，不能稱為方向型訊號。新聞自然批次 10/04 18:30 台北晚於該 snapshot，不回填舊卡；待下一自然 scan 依 cutoff 驗 PIT 關聯。`market_signal_outcomes` 仍只有 9/24 h1：signal-close、next-close 各 81 rows/80 directional scores，沒有成熟 h5/h10；不據此調門檻。
- 工作方向依使用者再次校正：只把自動化全市場收集、自然 run 品質、雙市場覆蓋、PIT 關聯和 outcomes 當主線。券商研究限於確認是否有可授權的批次 feed；不逐家查報告或人工評比。本次全為 SELECT/metadata 讀取，未手動觸發 DAG、寫業務 DB、發 Discord、交易或跑測試。

## 2026-10-04 18:47 台北：全市場漲跌廣度仍缺可驗證的歷史序列

- 唯讀查詢 `source_records`：目前只有 TWSE 行情有效日 2026-10-01，共 1,051 個代碼均為 accepted、0 quarantine；漲 411、跌 533、平 107。TPEx 尚無已接受的行情廣度列。對應最新 `source_runs` 顯示 TWSE 報價批次成功 1,051/1,051、0 rejected/duplicate；TPEx 報價兩次失敗，各 0 raw/accepted/rejected/duplicate。
- `market_signal_runs` 最新 daily 仍為 2026-10-02 `ready/partial`、1,946 universe / 1,938 price / 1,891 flow / 50 candidates；10/04 08:04 最新 intraday 是 5 張事件觀察候選，行情與法人方向家族均為 0。`market_signal_outcomes` 目前只有 2026-09-24 一個成熟訊號日、horizon 1（每種 entry basis 81 rows、80 directional scores）；因此沒有足夠多日資料可驗證漲跌廣度或調整任何方向門檻。
- 現有程式已把同日官方個股漲跌家數當作候選通知的非方向背景。可由既有全市場行情延伸多日 advance/decline breadth，但先恢復上市與上櫃日線自然收集，逐日核 effective date、接受/隔離、雙市場母體覆蓋及 scan 前可用時間，再累積與 TAIEX/TPEX 後續報酬對齊的 PIT 樣本；在有多個獨立訊號日及成熟 outcomes 前，維持背景資訊，不接候選分數。
- 本次僅查 Airflow metadata 與 crawler DB `SELECT`，未觸發/重跑 DAG、改寫資料庫、發 Discord、交易或跑測試。下一個相關驗收點為 2026-10-05 自然日線來源 bootstrap 和 20:00 after-close 行情批次。

## 2026-10-04 18:56 台北：新聞背景已自動點時關聯，但歷史候選欄位需以新版自然掃描驗證

- 唯讀讀取最新盤中候選快照（10/04 08:04）：5 張事件觀察卡中 2 張自動匹配到 3 篇新聞（中央社 1、鉅亨網 2）；3 張在當次樣本中未匹配。6,000 篇文章抽樣未截斷。3 篇來源時間欄都有時分，但程式明確標為 `unverified_against_article_page_and_timezone`；匹配只供新聞背景，不是方向票。
- 10/04 18:30 的新聞自然批次晚於上述 08:04 掃描，不能回填到舊候選。下一個自然候選快照才可依掃描開始時間，點時納入已收錄的新聞。
- 10/02 daily 50 張候選中 10 張有新聞匹配、共 16 篇/5 個發布來源，樣本未截斷；但這批已保存的 match JSON 沒有 `source_time_field`、精度及時間驗證欄位，故舊卡無法回溯補出其當時新聞時間證據。不可把這批歷史匹配計入新的時間品質驗證。
- 新版關聯程式已保存資料庫 `created_at` 時間欄、精度及「未對原文頁與時區核實」狀態。以 10/05 首個自然日線掃描驗收其 PIT 輸出：掃描前 ingest cutoff、匹配/未匹配數、樣本截斷旗標、每篇時間精度和驗證狀態。未匹配不代表市場沒有相關新聞；不因單次 match rate 改候選門檻。
- 本次唯讀查 market signal/news DB；未觸發 DAG、寫 DB、發 Discord、交易或跑測試。

## 2026-10-04 18:39 台北：自然新聞批次拒收分類核帳通過

- `news_ch_collect_process` 自然 run `scheduled__2026-10-04T08:30:00+00:00`（台北 18:30:02–18:38:56）成功，`collect_article_url`、`collect_news`、`delete_and_insert` 均 success。XCom：12 feeds、1,220 URLs、1,219 parsed/有效時間、1,197 accepted、1 fetch/parse reject、0 invalid-time quarantine、22 content/duplicate rejects；空標題 0、空內文 22、重複 0、post-time rejection delta 0、reconciliation failure sources 0。
- Feed 狀態為 9 complete、3 partial：三立 30 URL / 29 accepted / 1 fetch-parse reject；自由時報 98 parsed / 77 accepted / 21 content rejects；鉅亨網 150 parsed / 149 accepted / 1 content reject。接受列的記錄時間分布：1,007 有精確時分且在 48 小時內、4 為日期級晚於 cutoff、26 是 cutoff 日期但時分未知、160 按記錄精度較舊；分類加總為 1,197。時間分類依資料庫記錄欄位精度，不代表逐篇原頁及時區已複核。
- 這驗證拒收 reconciliation 會由自然排程產生且本批核帳一致；任務 success 不抹去 3 個 partial feed。新聞收集源是媒體 publisher，不具上市/上櫃個股母體覆蓋，故不把 1,197 篇當個股 signal 或市場 coverage，也不改方向分數/門檻。下一個工作日盤中/收盤自然 run 再看 news_context 是否與最新候選點時關聯。

## 2026-10-04 18:27 台北：收盤 daily trigger 與盤中 run 時間碰撞

- 唯讀查 Airflow metadata 與 crawler DB：成功 daily run 目前只有 as-of 2026-09-24（fresh、81 candidates）及 2026-10-02（ready/partial、50 candidates）；另有 9/25 failed run。成熟 outcomes 仍只有 9/24 horizon-1，signal_close/next_close 各 81 列。10/02 訊號的首個後續交易日為 10/05，當時尚未成熟，不能算缺失結算。
- 9/28、9/29、9/30 三個自然 `source_extension_collect_after_close` run 的 `trigger_market_signal_notify` 都被 `skip_when_already_exists` 跳過。Airflow log 明確記為 `DagRunAlreadyExists`；同一 `market_signal_notify` DAG 已存在相同 20:00 UTC logical date 的 `market_signal__scheduled__...` run，conf 為 `scan_mode=intraday`。因此那些日期沒有建立 daily signal child；這解釋 daily candidates/outcomes 歷史斷層，不是來源列數為零。
- 本地 `source_extension/dag.py` 現以 daily child logical date 加 1 秒避開盤中 run。10/01 自然收盤流程實際建立 daily child，logical date `2026-10-01T12:00:01Z`；child capture task success，持久化 10/02 `ready/partial`、50 candidates。這是偏移修正的一次自然 runtime 驗證。該 child notifier 仍 failed；parent 另有 1 個 mapped source failure，verify task 因此 failed。下一個 10/05 自然收盤 run 要逐項核 trigger 不再撞時、source/effective date/上市櫃品質與 notification receipt。
- 同一 10/02 daily digest ledger 有 2 段：13,624-byte 失敗列與 429-byte `sent` 列；兩列均無 Discord receipt ID/HTTP 狀態。故舊的 `sent`/「failure notice delivered」字樣都不算已驗證送達。現行分段器與 receipt-aware 狀態機需等完整 after-close 自然 child 驗證。
- 本輪只讀查 Airflow ORM/logs 與 crawler market-signal DB；沒有回補缺失日期、手動觸發/重跑 DAG、寫 DB、發 Discord、交易或跑測試。不依此調整訊號分數或門檻。

## 2026-10-04 18:11 台北：再次校正為可自動化、可抓取訊號

- 使用者指出近期針對券商網站/API 產品頁的人工查閱仍偏離目標；即使沒有逐檔讀個股報告，也不應把人工供應商網頁調查當成自動化訊號進度。先前 17:32 的 CMoney 網頁資格核查只保留為未接入來源線索，不視為新增訊號、已授權或接入決策，不延伸做更多手動供應商/券商網頁評比。
- 後續只以可自然排程、全市場批次抓取和可重跑資料為交付：修復現有 feeds/registry，驗 source run receipts、effective date、raw/accepted/quarantined/rejected/duplicate 計數、上市櫃 coverage、PIT lineage、跨來源自動 join、成熟 outcomes、新聞時間與通知 receipt。單檔公司資料僅能作為批次 adapter 的代表性技術檢查，不做人工個股研究。
- 券商訊號只有在專案已有可用授權與全市場機器 feed/API 時才處理；若缺授權、端點、歷史 vintage、時間戳、上市櫃覆蓋或回測使用權，只記錄缺少的准入條件並停止。不得搜尋或整理單家公司報告來替代批次訊號。後續 heartbeat 已加上這項明確限制。
- 本輪唯讀確認工作樹含多項既有修改，予以保留；週末自然 source run 仍為 failed/partial，新聞最近自然批次 `scheduled__2026-10-04T06:30:00+00:00` success。沒有手動觸發 DAG、寫 DB、發 Discord、交易或跑測試。

## 2026-10-04 17:39 台北：再確認交付是訊號自動化

- 使用者再次指出，交付必須是自動化、可抓取的市場訊號，不是逐家公司線上查券商研究再人工評比彙總。後續以自然排程和批次資料管線為主：檢查 effective date / PIT、接受與隔離數、上市櫃覆蓋、跨來源自動關聯、成熟 outcomes 及通知 receipts，並持續補可全市場重跑的高品質 feed。
- 券商資料只作為「批次來源候選」：先確認全市場批次端點、授權、有效時間、歷史版本與市場覆蓋；若其中任一關鍵條件未確認，就記為來源缺口，不逐家公司人工調查或評分。CMoney 公開 feed 資格筆記不擴展成券商報告評比流程。

## 2026-10-04 17:59 台北：新增自動訊號家族 outcome 歸因

- `docs/stock_signal_family_history_queries.py` 現會從已結算 outcome 連回候選當時保存的 `family_directions`，按訊號家族、上市/上櫃、`source_status`、進出場基準與持有期彙總候選數、訊號日數、家族命中率及方向調整後超額報酬均值/中位數。它是描述性診斷；同一候選可有多個重疊家族，報酬仍未調整除權息與交易成本，不能當因果或調參依據。
- 即時唯讀批次仍只有 2026-09-24 一個成熟訊號日、僅 h1。量能家族 `next_close`：上市 15/20 命中（75.00%），平均家族方向調整後超額報酬 +3.817 個百分點；上櫃 6/21（28.57%），-1.197 個百分點。相同候選以事後 `signal_close` 計則上市 16/20、+4.554 個百分點；上櫃 17/21、+2.540 個百分點。這形成待多日期/多持有期追蹤的市場別及可執行進場假設，不改分數、權重或門檻。
- `docs/stock_signal_family_history_quality.ipynb` 加入家族歸因表與方法 caveat，移除特定公司報告評析，改為全市場批次 feed 授權/PIT/歷史版本/雙市場覆蓋准入條件。helper 的 Airflow scheduler 唯讀 SELECT 執行成功，host AST 與 `nbformat.validate` 通過；整本 nbconvert 兩次均在 kernel 啟動/通訊遇 `ZeroMQ Bad file descriptor (.../epoll.cpp:73)`，舊 cell outputs 未刷新，不能宣稱整本 notebook 執行成功。未跑測試。
- 重新核自然批次：10/04 08:00 `source_extension_collect_weekend` failed、coverage partial，registry 9 keys / runtime 10 keys，runtime 缺 `tpex_quarterly_forecast_variance_events`；TPEx 公司事件/月營收 registry contract 仍為 `tpex_csv`，本輪兩者各兩次 MOPS read timeout、0 raw/accepted；目前程式定義已是 `tpex_json`，須等 10/05 04:00 自然 bootstrap 與收集回執後再判新版端點，舊批次不代表新 JSON reader 失敗。TWSE 月營收 effective month 2026-08、1,086/1,086 accepted；TWSE 重大訊息 10/03 effective、5/5 accepted，2 codes 未配普通股主檔仍 unknown。
- 同日盤中子 run 建立 5 個事件觀察候選，沒有價格/法人等方向家族；`market_signal_notifications` 為 6/6 有平台 receipt。最新日線仍 10/02 `ready/partial`、1,946 universe、1,938 prices、1,891 flows、50 candidates。最新 after-close logical 10/01 run failed；mapped source 第二次出現不完整串流（4,193,897 bytes 已收、尚缺 472,744 bytes），新版範圍續傳需由下一個平日自然批次驗收。
- 沒有手動觸發 DAG、寫業務資料庫、送 Discord、交易或改方向門檻。

## 2026-10-04 18:05 台北：將家族 outcome 摘要接入每日自然掃描回傳

- `dags/source_extension/market_signals.py` 新增 `_summarize_family_outcomes`，在每日 `_settle_daily_candidate_outcomes` 已提交後，唯讀彙總最近 120 日已成熟 outcome，並以 top-level `family_outcome_diagnostics` 放入 daily capture XCom。輸出含來源狀態、上市/上櫃、進出場基準、持有期、家族方向命中/平盤、均值/中位數、候選數及訊號日數；只作描述性品質回饋，不寫額外資料表、不影響候選分數、門檻或 Discord 通知。摘要失敗有獨立 unavailable 狀態，不會回滾已提交的 outcome。
- 對目前 worker runtime 直接呼叫新 helper 作唯讀 SELECT，as-of 2026-10-02 回傳 81 個候選、162 筆成熟 outcome、506 筆 family-outcome 觀察、無候選缺家族；量能 `next_close` 仍是上市 15/20、+3.817pp，與上櫃 6/21、-1.197pp。只涵蓋 9/24 一個訊號日，保留小樣本 caveat，沒有調參。
- Host 與 Airflow worker AST parse 通過；helper 已在 worker import 後讀現有資料執行成功。還沒有下一個自然 daily scan 驗收 XCom 回傳；等 10/05 after-close 下游子 run 檢查 `family_outcome_diagnostics` 欄位、樣本日期數與組別。沒有跑測試、手動觸發 DAG、寫 DB、送 Discord 或交易。

## 2026-10-04 17:17 台北：新聞拒收分類不平衡時 fail closed

- 在改寫 `news_diff` / `news_temp` 前檢查所有來源的拒收分類 delta；任一來源非 0 就以來源與 delta 明確拋錯，避免無法解釋的內容拒收分類仍進入下一個 `delete_and_insert` task。分類平衡時仍保留原收錄資料與 legacy aggregate 欄位。
- Host/worker AST parse、`git diff --check` 通過。最新 16:30 自然新聞 run 早於此閘門；下次預定 18:30 批次將同時驗證 delta、failed-source count、fetch/parse、時間隔離與 accepted 數。沒有手動觸發 DAG、寫業務資料庫、送 Discord、交易或跑測試。

## 2026-10-04 17:14 台北：新聞拒收分類加上自動加總校驗

- 在 per-source/aggregate XCom 新增 `post_time_filter_rejection_count_delta`、`post_time_filter_rejections_reconciled` 及整批 reconciliation failure source count；比較既有 `content_or_duplicate_rejected` 與空白標題、空白內文、重複及殘差分類總數。如果分類錯算不會被殘差欄遮住，差額會保留正負值供批次稽核。原先總數與收錄行為仍保留。
- Host 與 worker AST parse、`git diff --check` 通過。最新自然新聞 run 仍是 10/04 16:30，早於 telemetry 修改；下一自然批次預計 18:30 建立，屆時核每來源及 aggregate `delta=0`、failure count=0，並核接受/拒收數。未手動觸發、測試、寫 DB、發通知或交易。

## 2026-10-04 17:12 台北：TDCC 週一 trigger DAG 已自然登錄

- Airflow scheduler 現已將最終 DAG `stock_major_holder_monday_refresh_process` 登記為 active、未 pause、無 import errors；DagModel 的 `next_dagrun` 與 `next_dagrun_create_after` 都是 `2026-10-05 01:00 UTC`（台北 09:00）。此版本使用 `CronTriggerTimetable`，可在週一排程時刻執行，不等待下一個週資料區間。
- 最終 DAG 的 `list-runs` 目前為空；舊暫用 id `stock_major_holder_refresh_process` 已 inactive/paused，沒有手動觸發或 unpause。下一步等明天既有自然排程，核 TDCC 回執及 `stock.大戶比例` 寫入後的最新有效日；新 API preflight 尚不是正式收集證據。

## 2026-10-04 17:07 台北：提早恢復 TDCC 全市場週持股快照

- 由 Airflow worker 對官方 TDCC `1-5` OpenAPI 做唯讀批次預檢：HTTP 200、9,793,266 bytes、69,326 rows、單一 effective date `20261002`、4,078 unique codes、持股分級 1–17 全部存在、`(資料日期, 證券代號, 持股分級)` 重複鍵 0、4,078 檔皆有第 17 總持有級距。與普通股 master 比對，上市 1,054/1,054（100%），上櫃 890/892（99.78%）；未匹配兩檔仍為 unknown。最新行情有效日亦為 10/02，lag 0。這是端點 preflight，不是落庫 receipt。
- 以 worker 的唯讀 SELECT 核 legacy `stock.大戶比例`：最新有效日仍是 2025-04-25（1,851 rows / unique codes），下一期是 2025-04-18。既有 `stock_collect_weekday_process` 最近週六 run 在台北 10/03 09:00–09:58 success，但發生在 TDCC endpoint 修復前；該 DAG 每週六 09:00 才跑，下一次為 10/10。Airflow success 及新 API preflight 都不能代替修復後的正式批次回執。
- 新增單一來源的 `stock_major_holder_monday_refresh_process`，每週一 09:00 台北呼叫既有 `major_holder_collect`，不執行融資券、月營收、法說會或後續計算流程；週六舊批次保持不變。採專案既有 `CronTriggerTimetable`，避免一般週 cron 將首次資料區間延至下一週；`is_paused_upon_creation=False` 使新排程建立時啟用。目的為先將目前已發布的 10/02 全市場快照寫入既有持股表，再由之後的週六批次取得下一週快照，縮短兩期 TDCC 點時背景可用前的等待。持股變化仍只附於已成立候選卡，不加分或建候選；兩期及覆蓋 freshness gate 保持不變。
- Airflow worker `DagBag` 找到新 DAG：start `2026-10-05 09:00 Asia/Taipei`、trigger schedule `0 9 * * 1`、`catchup=False`、`max_active_runs=1`、DagBag import errors 0；host/worker AST parse 通過。初版 id 已在 scheduler metadata 建成 paused row，故改用新的最終 id，讓 Airflow 按 `is_paused_upon_creation=False` 自然建立啟用中的模型，沒有呼叫 pause/unpause 或觸發命令。待 scheduler 自然解析後核最終 id 未 pause、next run 為 10/05 09:00 且未提前建立 run。10/05 自然回執才驗收 effective date、raw/accepted/duplicate、雙市場覆蓋及 `大戶比例` 最新日期。

## 2026-10-04 16:54 台北：新聞空白標題/內文收錄條件 fail-closed

- 靜態核 `collect_news` 發現舊判斷只濾掉恰好單一空格標題；空字串、全空白標題及 null/全空白內文可能通過內容篩選。這是 parser 輸出品質邊界，不是已證明來源實際發生的拒收筆數。
- 收錄條件改用 `fillna`、字串化及 `strip` 判斷標題/內文是否至少含非空白字元；保留文章排序、`(source, article_id)` 去重及其餘收錄語義。拒收 telemetry 分別計算標題空白、內文空白、重複文章，另列殘差並保留舊 aggregate 欄位。
- Host 與 Airflow worker AST parse、`git diff --check` 通過。10/04 16:30 最近自然新聞 run 早於本次修改；等待下一次自然收集驗證 accepted/reject 與分類總和，不能據靜態檢查宣稱實際來源已出現空白列。未跑測試或手動觸發 DAG，未寫 DB、送 Discord、交易或改訊號分數。

## 2026-10-04 16:51 台北：拆解新聞批次拒收原因 telemetry

- 最新已知自然新聞批次（10/04 16:30–16:38）有 17 筆被合併記為 `content_or_duplicate_rejected`，只能知道有效時間列與收錄列的差額，無法由自動回執分辨空白/單空格標題、空白內文或同來源重複文章。
- 在 `collect_news` 保留現有收錄條件與去重順序，新增互斥 per-source 計數：單空格標題 placeholder、空白/單空格內文、重複 `(source, article_id)`，及未分類的殘差；原有 `content_or_duplicate_rejected` 欄位不變，以利舊 consumer 相容。XCom 也回傳各類 aggregate，讓未來每批自然收集可以比較來源品質，而非人工逐篇盤點。
- Host 與 Airflow worker 對修改檔 AST parse 通過，`git diff --check` 通過。最新 16:30 自然收集早於本次改動，不能驗新欄位；等下一次既有自然新聞 run 檢查 12 個來源的分類計數是否加總回原拒收數。未手動觸發 DAG、未跑測試、未寫 DB、未發 Discord、未交易或改方向分數。

## 2026-10-04 16:39 台北：16:30 新聞自然批次與點時品質

- `news_ch_collect_process` logical run `scheduled__2026-10-04T06:30:00+00:00` 於台北 16:30:00–16:38:44 parent success；`collect_news` 和自然 `delete_and_insert` task 均 success。XCom 為 12 feeds、1,153 URLs / parsed、1,152 valid publication times、1,135 accepted、0 fetch/parse rejects、1 invalid-time quarantine、17 content/duplicate rejects。觀測時刻 10/04 16:38:36；48 小時 freshness cutoff 為 10/02 16:38:36。
- 三立 30 URLs 中 29 accepted、1 筆因無效發稿時間 quarantine，因此 source partial；自由時報 98 URLs 中 81 accepted、17 content/duplicate rejects，因此 source partial。其他來源 parent 子項均無 fetch/parse rejects。
- 精確時刻較新來源包含 ETtoday 280、TVBS 16、新頭殼 30、中央社 100、中時 196、公視 43、聯合報 60；這是來源時間 freshness，不代表內容方向品質。鉅亨 150 筆中 22 筆舊於 48 小時 cutoff。TNL 90 筆中 82 筆舊、6 筆為 cutoff date-only / cutoff 時刻未知、2 筆只有日期且晚於 cutoff date；iThome 60 筆中 38 筆舊、20 筆 date-only / cutoff 時刻未知、2 筆只有日期且晚於 cutoff date。日期級時間維持不確定，不補造時分，也不因日期較新直接當成精確「近 48 小時」。
- 新聞仍只作 PIT 背景，通知需保留來源事實、媒體分析及未知事項的區別；不把單一報導直接加成方向票。此次只觀察自然 run / XCom，未手動觸發 DAG、未手動寫 DB、未發 Discord、未交易或調整分數。
- 新聞到候選卡的自動 handoff 已核碼：掃描只會把 48 小時範圍內、`create_time <= observed_at` 的既有新聞附到當時已成立的候選；每檔最多 2 篇，另受 10,000 id scan / 6,000 row sample 上限約束。卡片明列 `news.created_at` 是來源時間欄、precision 與時區尚未逐篇核實；TNL/iThome date-only 維持日期精度。10/04 16:30 新聞批次晚於 08:04 event-only signal scan，不回填舊卡；下次自然市場掃描再核是否有匹配候選及 source-time 標籤。新聞不另開方向分數。

## 2026-10-04 16:23 台北：自動化來源優先與 ETF 批次候選評估

- 再次依使用者更正把工作固定在「全市場可排程 feed、自然回執、點時資料、成熟結果」；不逐家公司線上查券商報告、人工打分或彙總。這輪只評估可批次取得的官方市場資料。
- 最新日線 snapshot 仍為 10/02 `ready/partial`：1,946 檔 universe、1,938 price、1,891 flow、50 candidates、0 notified。最新盤中 observation 是 10/04 08:04：`source_status=partial`、5 個事件觀察 candidates；universe/price/flow 均為 0，不能當作方向訊號。該批 6/6 ledger rows 有平台 receipt、HTTP 200；10/03 批次雖有 11 列 `sent`，0 receipt，不能證實送達。成熟 `market_signal_outcomes` 仍僅 9/24 horizon-1：signal-close 50/80 directional hits、avg excess +1.7760pp；next-close 37/80、+0.9868pp；未扣成本，無成熟 h5/h10，維持門檻。
- Airflow 自然狀態：`source_extension_collect_process` 最近於 10/04 04:00 台北 `success`，但 source registry/runtime contract audit 尚有缺漏；`source_extension_collect_weekend` 10/04 08:00 `failed`；`source_extension_collect_intraday` 最近 logical time 為 10/02 22:30 台北、實際於 22:45 完成；after-close 最近 10/02 20:00 `failed`，TPEx quote 新 reader 尚未有自然回執。保留前文的 4 個缺 catalog source key 與 2 個 TPEx CSV/JSON contract drift，未手動觸發或重跑 DAG。
- 16:27 再核當前 Airflow metadata 與 worker DB：10/04 process `ensure_source_extension_schema`、`discover_approved_sources` 均 success，但 DB catalog 仍 49 keys、runtime 53 keys。主機 runtime 檔最後修改時間為 `registry.py` 13:30、`adapters.py` 15:50、`market_signals.py` 15:55，晚於 04:00 process 及 08:00 weekend run；因此這兩次自然 bootstrap 都早於目前來源/reader 變更，不能據此判成 bootstrap bug。等 10/05 04:00 自然 bootstrap 驗 source registry 是否增至當前定義、更新兩個 TPEx JSON contract，再逐源核收集回執。
- worker 唯讀靜態預檢：當前 7 個 `source_extension` Python 檔皆 AST 可解析；實際載入 worker 的 53 個來源定義全部通過 `validate_source_contract`，且 priority 均高於自動啟用門檻。這只證明新版 runtime definitions 合法，不能取代自然 bootstrap 對 DB catalog 的更新，也不能取代 source 收集回執。
- 新聞最新自然批次 `news_ch_collect_process` 10/04 14:30–14:39 台北為 parent `success`，collect XCom：12 feeds、1,092 URL、1,090 parsed/valid-time、1,069 accepted、2 fetch/parse rejects、0 invalid-time quarantine、21 content/duplicate rejects；freshness cutoff 10/02 14:39 台北。三立為 partial（30/28/28 accepted）；自由時報 100 parsed、79 accepted、21 content/duplicate rejects。TNL 有 82 筆較舊、6 筆日期精度不足、2 筆只有日期且晚於 cutoff date；iThome 有 38 筆較舊、20 筆日期精度不足、2 筆只有日期且晚於 cutoff date。日期級資料不補造發稿時分，新聞維持 PIT 背景並保留來源事實/分析/未知的區別。
- 批次候選 A：TWSE 官方 OpenAPI [`/ETFReport/ETFRank`](https://openapi.twse.com.tw/v1/swagger.json) 可自然 HTTP 200 JSON 批次取回，但目前只有 20 個排名列、包含股票與 ETF 的定期定額交易戶數，schema 沒有報告日期/資料年月。服務 `Last-Modified` 只代表回應資源修改時間，不能代替報告有效日。先列為觀察度/零售關注度候選；在可取得明確 effective period、歷史 vintage、完整 universe/coverage 前，不做 PIT 回測或方向分數。
- 批次候選 B：官方 TDCC [dataset 58207](https://data.gov.tw/dataset/58207)「集中保管 ETF 月分析表」[批次資源](https://opendata.tdcc.com.tw/getOD.ashx?id=2-41)唯讀預檢 HTTP 200，CSV 359 rows、359 unique codes、0 duplicate；資料期全為 202608，含當月底/前月底保管數、增減、發行單位數、集保戶數，授權為政府資料開放授權第 1 版、月更新。以 TWSE `t187ap47_L` 10/03 master 的 271 codes 作單邊比對有 240 matched、31 master codes 未出現在這期資料、119 TDCC codes 不在該 TWSE master；未建立 TPEx ETF master，這些差異不等於覆蓋缺漏。此來源是 ETF 層級的慢速持有人/規模背景，無成分股曝險映射，也只有目前 202608 snapshot；不升格為個股方向訊號。若後續納入月頻收集，需先建立上市+上櫃 ETF 主檔、歷史版本與生效期 gate，再累積樣本驗證。
- 盤中 ETF 單位數變化是更及時的潛在資金流訊號，但 [TWSE 官方格式說明](https://dsp.twse.com.tw/public/static/downloads/tradingDepartment/ETF%20%E7%94%B3%E8%B4%96%E8%B3%87%E8%A8%8A%E5%8F%8A%E5%8D%B3%E6%99%82%E6%B7%A8%E5%80%BC%E6%8F%AD%E9%9C%B2%E5%B0%88%E5%8D%80%E4%BB%8B%E6%8E%A5%E6%A0%BC%E5%BC%8F%E8%AA%AA%E6%98%8E_20250109142554.pdf) 記載資料由各投信提供固定 JSON URL，再由交易所於交易日約每 15 秒彙整公開；目前未確認可自動擷取之批次 endpoint、歷史 archive 或機器再利用授權。公開頁面可見不等於批次擷取/外部通知授權，不接投信逐站爬蟲或非官方頁面 scrape；待找到明確官方 feed/授權才重新評估。
- 本輪沒有新且足以安全進入分數或通知的 ETF feed，故未改訊號分數、門檻或程式。下一自然檢查點是 10/05 04:00 bootstrap 及 20:00 after-close；逐 source 核 effective date、raw/accepted/quarantined/duplicate、上市/上櫃 coverage、契約同步、日線有效收盤覆蓋、h5 maturity 和每筆通知 receipt。沒有寫 DB、手動觸發 DAG、發 Discord、交易或跑測試。

## 2026-10-04 16:03 台北：最新自然週末 source audit 核對

- Airflow 最新 `source_extension_collect_weekend` 為 10/04 08:00 台北，父 run `failed`；唯讀執行既有 `audit_source_run` 得 `partial`、9/9 catalog sources reported、目前 runtime 預期 10 個來源，缺 `tpex_quarterly_forecast_variance_events`。兩個舊 catalog 契約 `tpex_company_events`、`tpex_monthly_revenue` 仍為 MOPS CSV，與 runtime TPEx JSON 不符；兩者第二次嘗試均在舊 MOPS host 讀取逾時 60 秒，0 raw/accepted，不能視為空事件或零營收。
- 7 個最終成功來源的回執及接受列：TWSE 重大訊息 5/5、有效 10/03，3 代碼匹配上市普通股、2 未匹配；TWSE 月營收 1,086/1,086、有效月 2026/08，1,054/1,054 上市普通股匹配、32 未匹配；TWSE 財測差異 7/7、5 個 distinct issuers 且 5/5 匹配上市普通股、財報期末 2026/06/30；上市及上櫃預定轉讓各 1/1 接受、各匹配本市場 1 檔、有效 10/02。
- 上市及上櫃未完成轉讓來源各 1 raw、0 accepted、1 quarantined，沒有有效日，兩者都保留為未知，不能當作無未完成事件。兩個 TPEx 內部人來源首試連線失敗，但第二次已成功；重試回執須與最後嘗試分開看。
- 本 audit 只讀 DB source_runs/source_records 與 runtime definitions；沒有寫 DB、手動觸發 DAG、送 Discord 或交易。10/04 08:00 run 早於後續 TPEx 行情 reader/contract 變更，下一自然 bootstrap/after-close 才能驗收新版目錄及 TPEx quote 自動收集。

## 2026-10-04 15:58 台北：自然收集等待期 outcome 與回執盤點

- 15:58 台北唯讀核 Airflow：process 尚無 10/05 04:00 之後的新自然 run；after-close 尚無 10/02 20:00 台北失敗 run 之後的新 run。未手動觸發 DAG。所以上一節 TPEx adapter 預檢仍未升格為自然 source receipt。
- 只讀 crawler DB：目前僅有 9/24 與 10/02 兩個 ready daily signal snapshot（10/02 有 50 張 stock candidates；9/24 的另一筆失敗 run 962 rows 不列入 ready sample）。`market_signal_outcomes` 目前仍只含 9/24 horizon-1 settled 結果：signal-close 50/80 hit、平均超額 +1.7760pp；next-close 37/80、+0.9868pp；皆未扣成本。沒有後續 ready 日線掃描，也沒有成熟 horizon-5/10 結果，不能調門檻或推論預測力。
- 通知帳本聚合為 131 `sent`（其中只有 6 筆有平台 receipt）及 32 `failed`（0 receipt）。只將有 receipt 的 6 筆視為已證實送達；其餘 `sent` 列維持未證實，不作為真實投遞率。最新有 receipt 的仍為 10/04 08:04 盤中批次 6/6 HTTP 200。
- 下一自然 process/bootstrap 及 after-close 批次再核目錄 53/49 差異、TPEx JSON 合約、effective date、接受/隔離/重複、上市櫃覆蓋、daily snapshot、outcome maturity 及逐筆通知 receipts。此次只查 Airflow metadata 和 crawler SELECT；沒有 DB 寫入、測試、發通知或交易。

## 2026-10-04 15:52 台北：修復 TPEx 全市場行情批次截斷

- 目前最新 after-close run（logical `scheduled__2026-10-01T12:00:00+00:00`，實際台北 10/02 20:00）失敗在 `ingest_approved_source` map index 9；Airflow worker log 顯示 TPEx 日行情 HTTP body 讀到 4,193,897 bytes，仍短 472,744 bytes，第二次嘗試仍遭 `ChunkedEncodingError`。同日 `market_signal_notify` child 有執行，但收盤來源 run 未完整，不能把通知/日線候選視為完整覆蓋。
- 對官方 TPEx endpoint `https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes` 做唯讀 Range 預檢：首段 `bytes 0-65535` 與中段 `bytes 4193897-4259432` 均 HTTP 206，精確 `Content-Range`/`Content-Length` 一致，兩段的 ETag、Last-Modified 相同；總量 4,666,641 bytes，官方回應最新日 2026-10-02。
- 將 TPEx 行情 source key 加入既有 64 KiB 分段 JSON reader；每段最多 3 次、核 Content-Range/長度/總位元組/穩定 ETag 或 Last-Modified，完整後才 parse。唯讀完整 adapter 預檢 72 requests、HTTP 206、11,928 JSON rows；普通股 master 篩選後 888 unique TPEx code，867 accepted、21 quarantined、0 duplicate hash，effective date 2026-10-02，888/892 上櫃普通股主檔匹配（99.55%，另 4 檔為當日來源未列，不視為零量）。同次傳輸也核得 `Last-Modified=2026-10-04 14:00:06` 台北；來源版本時間與行情有效日分開保留。
- 覆蓋分母拆開解讀：888/892 是來源代碼出現/主檔覆蓋率；可供漲跌廣度使用的接受列只有 867/892（97.20%）。21 筆都有代碼與有效日，但官方 `Change` 欄無可解析數值而隔離；4 個主檔代碼未列於回應仍為 unknown。不能將代碼匹配率當作可用訊號覆蓋，也不能把缺值轉成零漲跌。
- 通知摘要保留舊 `IncompleteTPExQuoteResponseError` 分類相容性，並把新共用 reader 的 `IncompleteTPExOpenAPIResponseError` 同樣顯示為本文不完整；後者的例外型別與 HTTP 狀態能延續寫入來源回執。
- 修補後唯讀比對仍見 runtime 53 個來源、DB catalog 49 個：缺 `twse_daytrade_eligibility_daily`、`tpex_daytrade_eligibility_daily`、`tpex_daytrade_market_summary_daily`、`tpex_quarterly_forecast_variance_events`；`tpex_company_events` 與 `tpex_monthly_revenue` 仍是舊 MOPS CSV 合約。最新 process 執行仍是 10/04 04:00 台北，尚無後續自然 bootstrap。下一 process 自然排程 10/05 04:00，after-close 20:00；逐源核目錄同步、回執與覆蓋，不能把 adapter 預檢當落庫證明。
- 預檢不寫 DB，也不產生 `source_runs`；不是自然 Airflow receipt。等下一個自然 after-close run 核實這個來源在 `source_runs` 的 effective date、raw/accepted/quarantined/duplicate、普通股匹配覆蓋及 parent DAG 狀態，再檢查日線有效收盤覆蓋表、signal outcome maturation 與通知 receipts。未手動觸發/重跑 DAG、未送 Discord、未交易、未跑測試；程式 import/source contract 在唯讀 adapter 預檢中通過。

## 2026-10-04 13:30 台北：接入上櫃官方財測差異門檻事件批次

- 依自動化優先目標，將政府開放資料目錄 [20761](https://data.gov.tw/dataset/20761)「上櫃公司當季綜合損益經會計師查核(核閱)數與當季預測數差異達百分之十以上者，或截至當季累計差異達百分之二十以上者(簡式)」列為全上櫃官方事件 feed；目錄標示免費、政府資料開放授權第 1 版、更新頻率不定期。官方 CSV 批次端點為 `https://www.tpex.org.tw/www/zh-tw/api/mopsfin?response=csv&type=t187ap16_O`。
- Airflow worker 唯讀預檢 HTTP 200、`application/csv`、78 bytes、無 Content-Length/ETag/Last-Modified；CSV 解析成 1 列、欄位為出表日期/年度/季別/公司代號/公司名稱，但唯一資料列只有出表日期 `20261004`，0 個公司代號、0 個可用季度公司事件。這個日期是資料快照出表日，不是公司事件有效日；目前預檢無可計算的上市/上櫃代碼匹配率，也沒有自然收集任務的 accepted/quarantined 計數。不得把哨兵列解讀為個股零值、零事件或完整市場覆蓋。
- 新增 `tpex_quarterly_forecast_variance_events` 的 allowlisted `tpex_csv` 來源，掛既有 `intraday` 排程；明確檢核官方五欄 CSV header。有內容但日期/期別/代碼不完整的公司列隔離。出表日及其他公司/期別欄皆空白的哨兵記成 `entity_type=source_snapshot`、無代碼、無數值的中性快照 metadata；run audit 會明確標示 metadata-only，訊號查詢只讀 stock 列。有效公司列的 `event_time` 保持空值，不把快照日期冒充發稿時間；候選只以首次收錄時間進入新事件掃描，並以來源/代碼/期別去重。通知僅顯示快照出表日與系統首次收錄 UTC，作獨立中性事件觀察，不補造實績/預測數值、不計入多空分數。
- 尚未有有效公司列或自然來源回執。後續自然 collection 必須逐源驗收 HTTP/metadata row/公司事件列數、輸出日期與公司財報期別、accepted/quarantined/duplicate、上櫃普通股主檔匹配覆蓋及 source registry contract；metadata-only 或任務綠燈都不代表可用個股事件 feed 完整。現有 TWSE 財測差異來源與分數門檻不變。
- 靜態檢查：4 個修改 Python 檔 AST parse；adapter/registry import 與 source contract allowlist validation 通過；intraday 已核准來源 10 個，低於 32 個 mapping cap。未跑測試、未手動觸發/重跑 DAG、未寫 DB、未送 Discord、未交易。

## 2026-10-04 13:00 台北：將全市場券商預估修訂列為授權 API 候選

- 針對自動化研究而非逐檔瀏覽，重查 CMoney 官方 M037「月機構預估盈餘與 EPS」及 API/獵報商品頁。[M037](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M037)涵蓋上市櫃代號表；頁面列每日 09:50、19:54 更新，明示 EPS 以近半年報告中每機構最新有效估值計算，並提供預估盈餘較上月變化、平均/最高/最低 EPS、預測機構數、最新研究報告更新日等欄位。頁面觀察到的資料更新時間是 2026-10-03 19:37:42，並非本輪當下資料快照。
- 這是可形成全市場「盈餘預估修訂廣度、平均修訂幅度、EPS 分歧度、預估機構數變化、估值更新年齡」候選的彙總資料；它不是原始券商報告。M037 對外說明採近半年最新值，未在公開表格契約中確認逐分析師/逐報告原文、精確發布時刻、歷史每日 vintage 或更新列的 append-only 軌跡。若正式授權批次可用，需由本專案逐日保存 PIT 快照，並與之後 MOPS 官方實績及成熟 outcomes 驗證，不能把共識上修直接當買進票。
- CMoney 官方[數據貓頭鷹 API](https://www.cmoney.com.tw/product/api)說明其 API 為標準化 RESTful、支援多股等查詢模式與歷史資料；但公開商品頁未確認 M037/M188 的商品 key、方案授權、全市場批次配額、可保存的歷史 vintage 或可否將衍生訊號送入外部通知。平台[授權說明](https://owl.cmoney.tw/)目前限制資料用於個人/企業內部研究，不得未經書面同意重製、轉售或散布；外部通知/衍生輸出權利未確認前不得接入 Discord 或寫入生產訊號。
- 官方[獵報](https://www.cmoney.com.tw/product/hunter-report)頁面描述已將券商報告評價、目標價、預估與每日變化/歷史軌跡量化整合，但只提供聯絡購買，未公開可排程 batch API、檔案交付格式、PIT 修訂時間或通知再利用授權。保留為優先供應商資格候選，不登入、不抓資料端點、不爬公開個股表格、不聯絡供應商、不加入 source registry。先補齊書面 API 商品範圍與內外部使用授權，再判斷能否批次抓取；未確認前不將不可達視為零或假設已有權限。

## 2026-10-04 12:42 台北：接入可批次取得的 TPEx 個股當沖資格 feed

- 依使用者再次校正，主線固定為全市場、可排程/批次抓取與可驗收的訊號，不做逐家公司券商報告線上調查與人工評比。只把授權批次 feed、官方盤後/日線、可 PIT 關聯的新聞與成熟 outcomes 納入自動化；來源候選需核可達性、effective date、raw/accepted/quarantined、上市/上櫃覆蓋與可行的歷史驗證。
- 找到可用的官方 TPEx 批次路徑：政府資料集 [137680](https://data.gov.tw/dataset/137680)，每日免費、Government Open Data License v1。由 Airflow worker 透過現有 allowlisted adapter 唯讀預檢，CSV HTTP 200、841 rows / 841 unique、0 quarantine、effective date 2026-10-02、duplicate keys 0；上櫃普通股主檔 892 檔中 724 個 feed code 命中，另有 117 feed codes 不在此普通股主檔，168 個主檔 code 未列於快照。未匹配與未列代碼皆為 unknown，不解讀為零量或不合格。前次 Windows transport/browser 403 是環境存取差異，不能再記成目前來源不可讀。
- runtime 已增加 `tpex_daytrade_eligibility_daily`，與 `twse_daytrade_eligibility_daily` 同屬既有 04:00 daily batch；TWSE worker preflight 為 HTTP 200、1,228/1,228 unique/accepted、0 quarantine、effective date 2026-10-05、972/1,054 上市普通股主檔命中。兩個 feed 都只有資格/暫停註記，不是成交量、方向或獲利訊號；exact-effective-date 與 scan-start 前可得 gate 不允許沿用舊日期。尚無已核歷史 vintage，暫只作執行背景，不加入分數/候選門檻。
- 唯讀自然 run audit 最新仍是 10/02 `scheduled__2026-10-02T20:00:00+00:00`：舊 DB catalog/source set 21/21 reported，現 runtime 預期 23 個來源，兩個新增資格 feed 均尚未自然 bootstrap 登錄/收集。下個驗收點為 10/05 04:00 自然 process/bootstrap，逐源核 effective date、raw/accepted/quarantined/duplicate、上市/上櫃 join、DB catalog 及來源回執；10/02 TPEx snapshot 到時若仍舊，freshness gate 必須排除。綠燈本身不代表當日來源完整。
- 本次 worker request 為官方公開 CSV 的唯讀 adapter preflight，未寫 DB、未製造 `source_runs`、未手動觸發 DAG、未送 Discord、未交易、未跑測試；程式驗證已在來源實作階段完成。維持現有分數/通知門檻，等自然批次及後續 outcomes，不以單日結果調整。

## 2026-10-04 12:55 台北：自然來源、訊號結果、通知與新聞 freshness 交叉核對

- 重新從 Airflow scheduler/worker metadata 與既有 DB 唯讀核對。`source_extension_collect_process` 最新仍為 `scheduled__2026-10-02T20:00:00+00:00`，Airflow success 但 audit partial：舊登錄 21/21、runtime 預期 23，缺 `twse_daytrade_eligibility_daily` / `tpex_daytrade_eligibility_daily`。沒有自然 DB receipt 可驗這兩個 feed。
- 10/04 08:00 台北週末自然 collection 父 run failed，9/9 有 mapped attempts；TPEx 重大訊息與月營收當次 catalog adapter/endpoint 仍為 `tpex_csv`，runtime 已是 `tpex_json`，contract drift 且 HTTP NULL、raw/accepted/quarantined 均 0。這是該次來源契約/環境 failure，不是空 feed。官方 TWSE 月營收 1,086/1,086 接受、effective 2026/08；其餘成功/隔離列依 source semantics 和 freshness 看待，不以綠燈推論全市場完整。
- 最新 after-close Airflow parent 為 `scheduled__2026-10-01T12:00:00+00:00`，台北實際 10/02 20:00，父 run failed：TPEx `tpex_equity_quotes_daily` map index 9 第 2 次嘗試 `ChunkedEncodingError`，4,193,897 bytes 已讀、尚缺 472,744 bytes，raw/accepted/quarantined 為 0；verifier 因該來源失敗。現 audit 仍因 17 筆 `source_runs` 缺 `dag_id` 而拒絕錯配 lineage，runtime after-close 18 個、DB 登錄 17 個，缺 `tpex_daytrade_market_summary_daily`。本日不改寫舊來源血緣。
- 上述 after-close run 觸發的日線子 run 確實存在：10/02 `ready/partial`，1,946 universe、1,938 price、1,891 flow、1,945 revenue、1,902 financial、1,795 borrow、1,944 insider、1,053 dividend、50 candidates、`notified_count=0`。通知 attempt 3 的 worker log 明確回 Discord `Request entity too large` / 40005；ledger 一列 sent、一列 failed，兩列均無平台 receipt。這次是先前程式版本的失敗，待 10/05 20:00 台北自然 after-close 驗收目前 byte-bounded split 的實際 payload、每批 receipt、HTTP 狀態與計數。
- 10/04 08:04 週末盤中 child run `ready/partial`：5 張為事件觀察候選，price/flow/revenue/financial 家族各 0，不解讀成行情方向訊號；通知 task success，ledger 6 列 sent、6/6 有 receipt、HTTP 200。成熟 outcomes 仍僅有 9/24 的 horizon-1：signal-close 81 列/80 有方向結果/50 hit/平均超額 +1.7760pp，next-close 81/80/37/+0.9868pp；都未扣成本，沒有成熟 h5/h10，不調分數或門檻。
- 最新新聞自然 run `scheduled__2026-10-04T02:30:00+00:00`，實際台北 12:30–12:39 success：12 feeds、1,047 URL、1,042 parsed/valid-time、1,019 accepted、5 fetch/parse rejects、0 invalid-time quarantine、23 content/duplicate rejects。三立 28/30、自由時報 76/99、鉅亨網 147/150 為 partial；TNL 82 old、6 日期級 cutoff-time unknown、2 日期級較新；iThome 38 old、20 日期級 unknown、2 日期級較新。精確時分與日期精度分開保留，文章頁時區/原始發稿時分未由此摘要確認；新聞只作 PIT 背景，不加方向票。
- 本輪只查 Airflow metadata、XCom 摘要與 SELECT/日誌，沒有 DB 寫入、手動觸發 DAG、發 Discord、交易或門檻修改。下一個驗收點是 10/05 04:00 process/bootstrap（兩個新資格 feed）及 10/05 20:00 after-close（TPEx 行情與日線通知修正）；並核既有 source contract 是否自然更新。

## 2026-10-04 12:33 台北：TWSE 新來源 adapter 唯讀批次預檢

- 使用 Airflow worker 以現行 allowlisted `fetch_and_normalize` 對官方 TWSE `TWTB4U` 來源做一次唯讀 preflight，HTTP 200、1,228 normalized / 1,228 quality accepted、0 quarantine、0 duplicate keys、1,228 unique codes，effective date 全為 2026-10-05；上市普通股 master 命中 972/1,054。這驗證新 normalizer 可解析本次官方回應，未寫 DB、未建立 `source_runs`，不等於自然排程收集成功。
- 同步唯讀 collection audit 確認 latest natural process 仍是 `scheduled__2026-10-02T20:00:00+00:00`：Airflow success、舊 DB catalog 21/21 reported，而目前 runtime expected 22 個，缺 `twse_daytrade_eligibility_daily` 登錄/回執。新來源等 10/05 04:00 自然 bootstrap/collection，再核 raw/accepted/quarantined/duplicate、資料日、公司代碼 join 及 DB catalog；不可手動觸發來製造驗收。
- `TWTB4U` 仍只是官方資格清單及 `Suspension` 限制註記，沒有成交量/買賣金額，也沒有歷史 vintage；不接方向分數或候選卡。TPEx 137680 官方開放授權 CSV 在目前環境無法取回 body，維持可達性缺口，不接其他網站路徑。

## 2026-10-04 12:17 台北：新增上市當沖資格清單批次，確認上櫃官方檔案目前不可讀

- 官方 TWSE `TWTB4U` 是政府資料集 [11661](https://data.gov.tw/dataset/11661) 的上市當沖標的批次；metadata 標示免費、每日更新、政府資料開放授權第 1 版。官方 OpenAPI GET 唯讀回 HTTP 200、1,228 rows/1,228 unique codes，欄位 `Date, Code, Name, Suspension`，全數 effective date 為 ROC 1151005（2026-10-05）；請求不同日期參數不改變回傳最新快照。這是資格名單與限制註記，不是個股當沖量值或方向訊號。
- 本次實際讀取上市普通股主檔 1,054 檔，1,228 筆名單中 972 個代碼匹配上市普通股（92.22%）；其餘 256 筆未匹配目前上市/上櫃普通股主檔，可能是非普通股範圍，沒有直接判錯。另有 82 檔上市普通股未出現在這次名單，但不以缺列判為零、不合格或訊號反向；只使用官方明列列作肯定資格證據。
- 已在 adapter/registry 增加 `twse_daytrade_eligibility_daily` allowlisted 批次來源，掛既有 `daily` 04:00 排程。保存 effective date、代碼/名稱及 `Suspension`，空回應 fail-closed、無效/矛盾列隔離；未接入方向分數或通知卡，也沒有歷史 vintage。host AST 解析通過；scheduler、worker 都可 import，runtime source priority 為 89（高於 75 的自動收集門檻）。尚未有自然 run 落庫；等 10/05 04:00 bootstrap/collection 驗收 effective date、raw/accepted/quarantined/duplicate 及 972/1,054 主檔 join 基線。
- 修正來源盤點：政府資料集 [137680](https://data.gov.tw/dataset/137680) 也列有每日、免費、Government Open Data License v1 的 TPEx 上櫃當沖標的 CSV，metadata 欄位為資料日、證券代碼/名稱、暫停先賣後買註記；官方資源 URL 是櫃買中心 `intraday_trading_list_result.php?l=zh-tw&o=data`。本機只讀請求收到 transport error，browser fetch 為 HTTP 403，因此 body/effective date/rows/上櫃 coverage 均未核；這是可達性缺口，不是空資料或授權不存在。未接入該 URL、未改用個股查詢頁或付費交易資訊商店。
- 最新自然批次唯讀重查：`source_extension_collect_weekend` 10/04 08:00 台北 run failed，9/9 有 mapped 回執；TPEx 重大訊息及月營收 `ReadTimeout`、HTTP NULL、0 raw/accepted/quarantined；未轉讓來源仍各 1 sentinel/0 accepted/1 quarantine。最新 process 自然 run 10/04 04:00 台北為 21/21 舊登錄來源 success；依新 runtime definitions 稽核為 21/21 reported、22 runtime expected，差的正是尚未自然 bootstrap 的 TWSE 當沖資格 feed。latest after-close Airflow run 10/02 20:00 台北 mapped index 9 第 2 次失敗，log 明確為行情傳輸 `ChunkedEncodingError`（已收 4,193,897 bytes，尚缺 472,744 bytes）；該 run 早於 TPEx range-resume adapter，不能當新 adapter 驗收。日線子 run 仍 capture 了 10/02 50 張候選，但 notification task 失敗且無 receipt。DB registry 尚未自然登錄 TPEx 當沖彙總。
- 訊號快照最新 daily 仍為 10/02：1,946 universe、1,938 price、1,891 flow、1,945 revenue、1,902 financial、50 candidates、`notified_count=0`；daily ledger 對該 run 有 1 sent/1 failed，兩筆都無平台 receipt。最新 intraday 為 10/04 08:04，5 個事件觀察候選，price/flow/revenue/financial 各 0；對應 6 筆通知全 sent、6/6 有 receipt、HTTP 200，仍不能算行情方向訊號。成熟 outcomes 仍只有 9/24 h1：signal-close 81 rows/81 stock returns/80 directional rows/50 hits；next-close 同為 81/81/80，37 hits；沒有 h5/h10 成熟樣本，不調門檻。
- 最新新聞自然 run `scheduled__2026-10-04T00:30:00+00:00` 於 10/04 10:30–10:38 台北 success：12 feeds、971 URL/parsed/valid-time、940 accepted、31 內容/重複淘汰、無時間隔離或抓取/解析失敗。自由時報 99→70、鉅亨網 150→148 為 partial。TNL 82 舊、6 個 cutoff 日時間不確定、2 個較新；iThome 38 舊、20 個 cutoff 日時間不確定、2 個較新；兩者雖 complete 仍是 mixed freshness，只按記錄時間精度判讀，不因文章數量加方向票。
- 本輪未跑測試、未觸發/重跑 DAG、未寫 DB、未發 Discord、未交易；保留其他工作樹變更。

## 2026-10-04 11:56 台北：自然批次與程式更新時序界線

- 唯讀重查 10/04 08:00 台北週末 source run：Airflow parent failed，9 個來源都有 mapped 回執；TPEx 重大訊息及月營收各 `ReadTimeout`、HTTP NULL、raw/accepted/quarantined 均 0，DB registry 仍是 `tpex_csv`，與目前 runtime `tpex_json` 不符。主機檔案時間顯示 adapters.py 10:40、registry.py 10:44 才更新，因此 08:00 run 早於新契約，不能當作新 JSON feed 驗收失敗或成功；等待下一次自然 bootstrap/collection 核 registry、HTTP、effective date、列數與上市櫃覆蓋。
- 同一自然 run 觸發的 10/04 盤中 child `ready/partial`：行情 universe、價格、法人流、營收與財務家族均為 0，5 張候選是事件觀察卡；`notified_count=5`，通知 ledger 6 筆 sent 均有 platform receipt、HTTP 200。記為事件通知送達，不算行情方向訊號。
- 最新 daily snapshot 仍是 10/02 `ready/partial`，1,946 檔母體、1,938 價格、1,891 法人流、50 候選、`notified_count=0`；daily digest ledger 舊送達/失敗列均無平台回執。`market_signal_outcomes` 仍只有 9/24 horizon-1：signal-close 80 筆有方向結果、50 hit；next-close 80 筆有方向結果、37 hit；h5/h10 尚無成熟列，不調門檻。
- Airflow 三項來源 DAG active 且未 pause；下一個 weekday 自然檢查點依序是 10/05 04:00 process/bootstrap、07:00 intraday、20:00 after-close。沒有手動觸發/重跑、寫 DB、發 Discord、交易或跑測試。

## 2026-10-04 11:42 台北：讓稽核在來源血緣缺失時仍檢查 runtime catalog

- 核實每個 source-extension DAG 都先執行 `ensure_schema_and_catalog`；`_bootstrap_catalog` 使用 `ON DUPLICATE KEY UPDATE` 更新 adapter/endpoint/config，但不覆寫既有 approval/enabled 狀態。所以上櫃重大訊息/月營收的 `tpex_csv`→`tpex_json` 會由下一自然 bootstrap 套用，無需資料庫手動修改。
- 唯讀盤點 18 個 runtime `daily_after_close` approved/enabled 來源，DB 目前僅登錄 17 個；唯一 runtime-only key 是官方 TPEx 市場當沖彙總 `tpex_daytrade_market_summary_daily`。另有 7 個來源尚無任何 `source_runs`：TAIFEX 指數期貨部位、臺指選擇權部位、TPEx 市場當沖彙總、TPEx 董監注意/處置、TWSE/TPEx 每日估值。訊號卡已有 TAIFEX 與 TPEx 當沖的非方向背景讀取；需先等批次自然收集，不把缺資料當零值或方向票。
- `audit_runs.py` 先前遇到無精確 DAG 歸屬來源列時會提前回 unknown，無法同時指出 runtime/DB catalog 漂移。現改為無論 source rows 是否存在都計算註冊預期與 runtime 預期來源鍵；orphan 分支明確列出 17 個無歸屬列、15 個來源鍵、17 個 DB 登錄來源、18 個 runtime 預期來源，以及缺登錄的 `tpex_daytrade_market_summary_daily`。仍回 `unknown`，不猜測 orphan 屬於該 DAG；若未來 run 完全沒有 orphan，才列可歸屬的缺少來源鍵。
- scheduler AST/import、worker AST/import 通過；唯讀 after-close audit 即時顯示上述數量。最新 process 仍 21/21 回報且無 contract drift、`data_review_required`；週末 10/04 run 仍有兩個 TPEx source contract drift。等待 10/05 04:00 自然 bootstrap 驗證 registry，20:00 自然 after-close 驗收 effective date、接受/隔離/重複、上市櫃覆蓋與新 feed 首次回執；不手動觸發 DAG。
- 10/04 11:44 唯讀查 Airflow `DagModel` 確認 process/after-close/intraday 三 DAG 均 active 且未 pause；下一次建立時刻分別為 10/05 04:00、20:00、07:00 台北。沒有把尚未到期的排程當成漏跑；10/04 10:30 新聞批次會留待 10/05 07:00 自然盤中掃描按既有時間精度規則關聯。
- 未跑測試、未寫 DB、未發 Discord、未交易，保留既有無關工作樹變更。

## 2026-10-04 11:29 台北：擴充自然來源 run 預設稽核血緣

- 最新可見 Airflow `source_extension_collect_after_close` scheduled run 為 `scheduled__2026-10-01T12:00:00+00:00`，狀態 failed；`ingest_approved_source` map index 9 與 verifier 第 2 次嘗試失敗。先前 audit CLI 只從 `source_runs.dag_id` 找最新 run，因此回報「找不到」，漏掉實際存在的失敗 Airflow run。
- 已修改 `audit_runs.py` 的預設 run 選擇：先查 Airflow metadata 的最新 scheduled `DagRun`，再查該 run 的精確來源列；只有 Airflow metadata 查詢失敗/無排程 run 才退回 `source_runs`。audit 回報 run id 來源與 Airflow 狀態；如來源列全是 dag_id=NULL，仍回 unknown 並列 orphan 數，不按重複 run ID 猜 DAG。唯讀重查 now correctly reports failed run、17 orphan rows/15 source keys。
- 最新 process logical run `scheduled__2026-10-02T20:00:00+00:00`（10/04 04:00 台北）21/21 來源有回執、無 contract drift，但狀態 `all_expected_tasks_succeeded_data_review_required`；`tpex_dividends` 2,483/2,483 接受仍只有 2021-08-03 effective date，freshness 旗標 1,886 天。週末 10/04 08:00 自然 run 為 partial：`tpex_company_events`、`tpex_monthly_revenue` 都因 DB registry `tpex_csv` 與 runtime `tpex_json` 不一致而失敗，0/0/0、HTTP NULL；其餘不可用/隔離列不解讀成零活動。待 10/05 自然 bootstrap/collect 驗證 registry 契約是否更新。
- 最新新聞自然 run 10/04 10:30–10:38 台北 success：12 feeds、971 URL/parsed/valid-time、940 accepted、0 fetch/parse reject、0 invalid-time quarantine、31 content/duplicate reject。自由時報 99→70、鉅亨網 150→148 為 partial；iThome 有 38 篇超出 48 小時及 20 篇 cutoff 當日時間不確定，thenewslens 為 82 篇超時及 6 篇時間不確定。只將精確時間和日期級時間依各自精度呈現。
- 最新 daily signal snapshot 仍是 10/02 `ready/partial`：1,946 universe、1,938 price、1,891 flow、50 candidates、0 notified。10/02 daily child snapshot capture success，但 notify 第三次仍 failed。`market_signal_outcomes` 目前僅 9/24 horizon-1：signal-close 81 列/80 scored/hit 率 62.5%，next-close 81/80/46.25%；沒有成熟 h5/h10，維持分數與門檻。週末 intraday 仍是 0 價格/0 法人流/5 事件候選，屬事件觀察，不是新盤中行情訊號。
- 讀取並遮蔽 URL/敏感字串後檢查 daily notify attempt 3 log，明確錯誤為 Discord `Request entity too large`（code 40005）；不是只從 task failed 推測。現行 `market_signals.py` 已按 Discord payload 的實際 JSON UTF-8 bytes 將 daily embeds 拆批，每批含 part 標籤仍須低於 10,000 bytes，單張超界即 fail closed；worker AST/import 通過。舊失敗 run 不重送，等 10/05 自然日線通知驗證每批 `receipt_id`、HTTP 狀態及 `notified_count`。
- Airflow scheduler AST/import 與 worker 對修改後 `audit_runs.py` AST/import 通過；現場唯讀 audit 確認新增的 Airflow metadata fallback 與 orphan 診斷。未跑測試、未觸發/重跑 DAG、未寫資料庫、未送 Discord、未交易，保留所有既有無關變更。

## 2026-10-04 09:50 台北：新增自然來源 run 唯讀稽核器

- 新增 `dags/source_extension/audit_runs.py`，可按 DAG 選最新 `scheduled__` source run 或指定 run ID，以唯讀方式報告每個來源的最新 attempt、raw/accepted/quarantined/duplicate、accepted effective-date 範圍、上市/上櫃主檔匹配數，以及 DB registry 與目前程式 contract drift。輸出不含 endpoint URL；市場主檔匹配是 join 數，不當作完整市場覆蓋率，成功任務或缺列也不當作零值。
- 在 Airflow scheduler container 對當日自然 run 實際執行：9 個預期來源/9 個有回執，整體 `partial`；上櫃重大訊息與月營收兩來源各兩次逾時、HTTP NULL、0/0/0，且 DB registry 仍是 `tpex_csv`，與目前程式定義 `tpex_json` 不一致。下一個自然 schema bootstrap 應更新 registry；接著要核自然收集是否真的用新合約、有效日、列數及上市櫃匹配，不能以本地程式已改或 audit 成功代替。
- 其餘成功列：TWSE 重大訊息 5/5 accepted、有效日 10/03、5 個代碼中 3 個匹配上市普通股主檔、2 個保留未匹配；上市月營收 1,086/1,086 accepted、有效月 2026-08、1,054 個代碼匹配上市普通股主檔；TWSE 財測差異 7 列/5 個代碼，effective quarter end 2026-06-30 且 5 個均匹配上市主檔。上市/上櫃預定轉讓各 1/1 accepted、effective 10/02、各匹配本市場普通股；未轉讓來源各 1 raw/0 accepted/1 quarantine、無可用 effective date，不能解讀成沒有未轉讓資料。
- 執行方式：`PYTHONPATH=/opt/airflow/dags python -m source_extension.audit_runs --dag-id source_extension_collect_weekend`。另已將只讀 audit 接到既有 `verify_collection_and_triggers`，未來每次 source-extension 自然 collection run 都會在 Airflow task log 留完整 JSON、在 XCom 留精簡 issue 摘要；audit 故障只標 unavailable，不改寫來源列，也不取代現有 ingestion/trigger 驗收。audit 狀態仍明確標示 all-task success 不證明 freshness/完整性。本輪只讀既有 DB/run；沒有寫資料庫、手動觸發 DAG、發 Discord、交易或跑測試。

## 2026-10-04 09:35 台北：發現可自動化券商預估候選，尚未證明 API 可接

- CMoney 公開 `M037` 說明為台股上市櫃機構盈餘/EPS 彙總，列出每日 09:50、19:54 更新、近半年各機構最新有效預估及預估機構數、最高/最低 EPS、更新日等欄位；`M188` 是外資機構子集。[M037 欄位/排程](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M037)、[M188](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M188)。這是比逐公司手動看報告更符合目標的全市場候選；它是彙總估值，不含原始假設與完整逐分析師修訂鏈。
- CMoney 一般 API 商品頁宣稱台股全市場、每日自動更新並支援 HTTPS/JSON；「獵報」商品頁宣稱每日整理預估變化與歷史預估軌跡。但公開資料尚未證明 M037/M188 或獵報有本專案可授權的批次 API、完整上市櫃覆蓋、歷史 PIT vintage、首次發布/修訂時間、查詢限制及儲存/回測重用權。列為**待資格確認**，未爬頁、未接入、未產生訊號。[API 商品](https://cmoney.com.tw/product/api)、[獵報](https://www.cmoney.com.tw/product/hunter-report)。
- `M866` 公開說明是每檔股票「最新一季前十名」券商預測誤差橫斷面，不能直接當歷史分析師品質因子或回測資料；除非可用 PIT 封存版本消除前視/選擇偏誤。[M866](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M866)。
- 若未來授權與合約核實，才評估自動收集日快照，試算共識 EPS/營收修訂、分歧度、預估家數變化，對照 MOPS 官方實績與市場調整後 outcomes。原始估值報告仍不做逐家公司人工搜尋評比。

## 2026-10-04 08:40 台北：恢復上櫃事件與月營收的官方批次可達性

- 最新自然週末來源 run `source_extension_collect_weekend / scheduled__2026-10-04T00:00:00+00:00` 仍記錄兩個 MOPS CSV source 各 0 raw/accepted/rejected、HTTP NULL，因容器當時連線逾時而 failed；不可解讀為上櫃沒有資料。其餘自然來源及 10/04 intraday 子 run 的結果見下方 08:09 紀錄。
- 同一全市場官方資料已有 TPEx OpenAPI JSON 批次端點：重大訊息 [OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)，月營收 [OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)；所對應的政府開放資料目錄為[每日重大訊息 dataset 18418](https://data.gov.tw/dataset/18418)及[每月營收 dataset 56510](https://data.gov.tw/dataset/56510)。在 Airflow worker 對兩端點以 256 KiB range 分段讀取，驗證 HTTP 206、Content-Range、總長、穩定 ETag/Last-Modified、If-Range 與 10 MiB 上限後才解 JSON。預檢全程唯讀，沒有 source_runs 或 DB 寫入。
- worker 的唯讀 `fetch_and_normalize` 預檢：上櫃重大訊息 42/42 normalized quality `ok`、0 重複、34 個不同證券代碼均匹配目前市場主檔；最新有效日 2026-10-01，OpenAPI Last-Modified 為 2026-10-02 23:30 台北。上櫃月營收 892/892 quality `ok`、0 重複，892 個代碼中 891 匹配市場主檔、1 個未匹配；有效月份均為 2026-08，Last-Modified 為 2026-10-02 23:34 台北。未匹配代碼沒有改寫為零值或零量。
- 新 OpenAPI 重大訊息有效日 10/01 與稍早讀到的 MOPS CSV 預檢 10/03 不一致。OpenAPI 可達不等於比 CSV 更新，也不代表 10/04 全日公告完整；先保留有效日差異，等自然批次回執與來源新鮮度核實，不調訊號權重、不將單日 feed 當成高品質方向票。
- Docker 預設 DNS 曾令 scheduler/worker 對 `www.tpex.org.tw` 解析失敗；scheduler 與 worker 的只讀檢查確認當時無 running/queued TI 後，僅在 `x-airflow-common` 加入 8.8.8.8/1.1.1.1 並重建這兩個服務。重建後 scheduler 與 worker 均健康，worker 使用一般 DNS（未 pin IP）完成上述批次預檢。未重建 webserver/triggerer/postgres/redis，未手動觸發或重跑 DAG、通知或交易。
- `adapters.py` 限定只有這兩個已核准 source key 使用分段 JSON retrieval，registry contract 改為官方 TPEx JSON URL。下一次自然來源 run 才能驗收 Airflow source registry/bootstrap、來源 run effective date、接受/隔離列、整體市場覆蓋與 downstream 子 run；目前不能宣稱新 contract 已自然 run 綠燈。
- 同次唯讀刷新：10/04 08:00 週末來源 parent failed，10/04 intraday 子 run success/partial、5 個公司事件候選且 5 個已通知；最新 daily snapshot 仍為 10/02 ready/partial（1,946 universe、1,938 價格、1,891 流、1,945 月營收、1,902 財務、50 候選、0 通知）。最新 after-close parent 仍是 10/02 failed。`market_signal_outcomes` 仍僅 9/24 signal date：signal-close/next-close 1-session 各 81 settled、各 81 個股報酬、各 80 direction hits；沒有成熟 5/10-session 視窗，維持分數與門檻。
- 新聞最新自然 run `scheduled__2026-10-03T21:30:00+00:00` 的 Airflow `collect_news` XCom 回執確認 08:30–08:38 台北完成：12 sources、967 URLs、966 parsed/valid-time、945 accepted、1 fetch/parse reject、21 content/duplicate reject、0 invalid-time quarantine；10 sources complete、自由時報與鉅亨網 partial。新聞仍只作有發布時間的背景脈絡，不自動加方向票。修改後 `adapters.py`、`registry.py` AST 解析通過、`docker compose config --quiet` 通過；未跑測試。

## 2026-10-04 08:55 台北：DNS 修復後檢查雙市場官方行情與內部人批次列

- Airflow worker 使用一般 DNS 對上市[官方日行情 API](https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL)及上櫃[官方日收盤 API](https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes)唯讀批次預檢；兩源均 HTTP 200、資料日 2026-10-02。上市普通股主檔 1,054 檔，API 回列 1,051 檔、1,051 quality `ok`（99.72%）；上櫃普通股主檔 892 檔，API 回列 888 檔，其中 867 quality `ok`、21 quarantine（回列覆蓋 99.55%，可用方向價格 97.20%）。預檢不寫 source_runs 或 DB，非自然收集回執。
- 上櫃 21 個 quarantine 中，20 列的漲跌與收盤欄皆為官方 `---` 標記，另有 1 列漲跌欄為 `除息`、收盤 24.50。沒有把前者補成 0 成交/平盤，也沒有由除息標籤推算方向報酬；先保留隔離，避免跨除息日把未調整價格當作訊號。TWSE/TPEx API 的欄位差異和未回列主檔代碼仍以分市場覆蓋數呈現。
- 四個官方內部人轉讓批次 feed 同次預檢均 HTTP 200：TWSE/TPEx 預定轉讓各 1 row/1 accepted、effective date 10/02；兩市場未轉讓 feed 各回 1 個只有日期欄的 sentinel row，均 0 accepted/1 quarantine/無 entity。不可將其解讀為「無未完成轉讓」。這與 08:00 自然 run 的 raw/accepted/quarantined 形狀一致；DNS 修復後的自然來源回執仍待下一排程驗收。
- 目前收盤通知 renderer 已有上市/上櫃分開的行情接受數、主檔分母、隔離數與未分類漲跌欄，整體卡也會列出來源品質問題。本次沒有足夠證據改分數或卡片規則；待自然 run 後依實際 `source_runs` 與 daily child 的上市/上櫃 coverage、通知 receipt 再決定是否需要窄幅調整。

## 2026-10-04 08:09 台北：自然週末來源 run 驗收批次路由與訊號子 run

- 自然 run `source_extension_collect_weekend / scheduled__2026-10-04T00:00:00+00:00` 於台北 08:00–08:06 執行，schema bootstrap 已將上櫃公司事件/月營收映射到新 MOPS CSV contract。這兩個 source 均兩次在 `mopsfin.twse.com.tw` 讀取 60 秒逾時，HTTP status 為 NULL、每次 0 raw/accepted/rejected，effective date 無資料；run 最終 failed。不是上櫃當日沒有公告或營收，且 direct preflight 曾回 HTTP 200 不等於自然 worker 當日可用。
- 其餘 7/9 sources 成功：TWSE 重大訊息 5/5 accepted、effective 10/03，其中 3/5 證券代碼匹配現行上市普通股主檔；TWSE 月營收 1,086/1,086 accepted、effective month 2026-08，其中 1,054 符合現行上市普通股主檔；TWSE 財測差異 7/7 accepted、effective quarter end 6/30、5/5 實體匹配普通股主檔。上市/上櫃內部人預定轉讓各 1/1 accepted、effective 10/02 且各匹配 1 個普通股；兩市場未轉讓報表各 1 raw/0 accepted/1 quarantine，缺實體列不可解讀為無未轉讓紀錄。上櫃內部人兩 key 第一次遇舊 TPEx host DNS error，第二次才 HTTP 200；不是經 MOPS CSV 驗證的欄位契約。
- source collection 的失敗仍觸發盤中 signal 子 run；子 run Airflow success，但 `market_signal_runs` 為 10/04 `intraday ready/partial`、1,946 universe 中行情/方向家族皆 0、5 個公告觀察候選。通知 ledger 有 6 筆 `sent`、每筆有平台 receipt、HTTP 200，候選計數/已通知數均為 5；這是收到官方事件觀察卡，不是行情或全市場方向訊號。Latest after-close parent remains 10/02 failed；其 10/02 日線 child 是 50 candidates、快照 success、通知因 Discord 40005 failed，現版日線切批還沒經新的自然 close run 驗收。
- `market_signal_outcomes` 仍僅 9/24 一個 signal date：signal-close/next-close horizon 1 各 81 settled rows、各 81 有個股報酬、各 80 有方向 hit；沒有 5/10-session 成熟窗或成本後表現，不調分數/門檻。

## 2026-10-04 05:30 台北：批次新聞時間 parser 自然驗收

- 自然新聞 run `news_ch_collect_process / scheduled__2026-10-03T15:30:00+00:00` 於 05:30–05:41 完成，三個 Airflow tasks success。12 個已設定來源合計 1,364 URLs、1,359 parsed、1,359 valid publication-time records、1,346 accepted；5 個 fetch/parse reject、13 個內容/重複篩除、0 個無效時間隔離。來源 coverage 為 9 complete、3 partial，run success 不代表每源都完整。
- TVBS 16/16、新頭殼 25/25、公視 43/43 已解析、有效時間、接受，驗收了新版批次列表/發布時間 parser；三立 26/30，4 篇解析拒絕仍待追；自由時報 88/100（12 篩除）、鉅亨 148/150（1 fetch/parse reject、1 內容/重複篩除），均 partial。時間精度較差來源不可因 `complete` 當作近稿充足：iThome 60 accepted 中 38 依日期精度已舊、20 筆落在 cutoff 當日而時刻未知、2 筆日期較新；TNL 90 accepted 中 82 舊、6 筆 cutoff 當日時間未知、2 筆日期較新。新聞仍僅提供有來源時間的背景脈絡，不加個股方向票。

## 2026-10-04 07:52 台北：TPEx 內部人轉讓批次 CSV 在目前環境無法讀取

- 為補足已登錄、但近期沒有成功回執的上櫃內部人轉讓/未轉讓批次來源，從官方開放資料列出的 MOPS 全市場 CSV 路徑唯讀讀取 `t187ap12_O.csv`（轉讓日報）與 `t187ap13_O.csv`（未轉讓日報）。Windows 主機對兩個 endpoint 各等待 15 秒均逾時；先前 Airflow scheduler 對轉讓日報也曾讀取逾時。兩次本輪請求均沒有 HTTP status/body，無法核欄位、筆數、日期、上市櫃覆蓋或正規化結果。
- 這是目前執行環境的傳輸逾時證據，不代表官方檔案為空、當日沒有轉讓，或資料本身失效。未將零列寫入 source_runs，未切換 contract、未推定接受/隔離數，也未產生個股方向訊號。要接入必須先由可排程 runtime 實際讀到 CSV，再核必要欄位、有效日、stock-master 匹配及未配對列處置。
- 本輪通知程式檢視確認 10/02 的 40005 發生於舊版單一 payload 路徑；現版每日通知已按 JSON request bytes/Discord embed 數上限切批，卡片彙總也限制總 embed 字元數。尚無新自然 daily run 與 Discord 平台 receipt 驗證新版投遞；不重送舊卡、不手動發送。

## 2026-10-04 07:30 台北：上櫃月營收與重大訊息改接官方 MOPS 批次 CSV

- 最新週末自然來源 run `source_extension_collect_weekend`（10/03 08:00 台北實際啟動）中，`tpex_company_events` 與 `tpex_monthly_revenue` 的 JSON API 在兩次 attempt 均回 HTTP 530；兩項 source_runs 都是 0 raw/accepted/rejected，DB registry 仍保存舊 JSON endpoint。這是交易所來源存取失敗，不是上櫃沒有事件或營收列。
- 官方開放資料平台列有[上櫃公司每日重大訊息 dataset 18418](https://data.gov.tw/dataset/18418)（每日更新、政府資料開放授權第 1 版）與[上櫃公司每月營業收入 dataset 56510](https://data.gov.tw/dataset/56510)（每月更新、同授權），兩個資料資源分別提供 MOPS CSV `t187ap04_O.csv` 與 `t187ap05_O.csv`。新增這兩條固定 URL 的 `tpex_csv` source contract，按 source key 驗證必要 CSV 欄位；既有上限、MIME、禁止 redirect、UTF-8 BOM 與只重試 HTTP 5xx 規則不變，正規化仍分別使用公司事件發言時間/日期與月營收所屬月份。
- Airflow scheduler 的唯讀 `fetch_and_normalize` 預檢：上櫃月營收 HTTP 200、892 列/892 accepted quality、0 隔離、0 重複，effective month 2026-08；892 feed code 中 891 匹配目前 892 檔上櫃普通股，1 個 feed code 不在主檔，另 1 檔主檔沒有列。4 個 accepted 列沒有可用 YoY metric，值保留缺漏，未補零。重大訊息 CSV HTTP 200、4 列/4 accepted quality、0 隔離/重複，有效日 2026-10-03，4 個代碼均匹配上櫃普通股。這些是唯讀格式/覆蓋預檢，不是 source_runs、落庫或訊號通知回執。
- 兩個修改模組在 Airflow scheduler/worker 均通過 AST 解析；worker 可 import 新 contract 並通過 allowlist 驗證。尚待 10/04 08:00 週末自然 run 核 source registry 是否由 schema bootstrap 更新，以及 source_runs 的 HTTP、effective date、接受/隔離/重複列、上市櫃範圍與後續掃描結果。未跑測試、未手動觸發 DAG、未寫 DB、未發 Discord 或交易。
- 其他自動化品質狀態仍是阻礙：最新 daily snapshot 為 10/02 `ready/partial`、1,946 主檔/1,938 價格/1,891 法人流/1,945 月營收/1,902 財務/50 候選；10/03 週末 intraday snapshot 是 0 個行情家族、10 個事件候選。最新 after-close 父 run（10/02 台北）因 TPEx 行情 mapped source 失敗而 failed；其 daily child 的 snapshot task success、通知 task failed。`market_signal_outcomes` 仍只有 9/24 的 signal-close/next-close 各 81 列、各 80 列有方向值，沒有成熟 5/10-session 窗。單一訊號日不足以調分數或門檻。

## 2026-10-04 07:13 台北：訊號卡加入有覆蓋門檻的產業相對表現脈絡

- 現有日線候選已有對 TAIEX/TPEx 指數的五日相對報酬與全市場上漲/下跌家數，未顯示個股相對同市場、同產業表現。已用既有官方個股日線和同日指數視窗，為證券主檔每個「市場別＋產業別」計算五個交易日原始收盤報酬的等權中位數；不新增資料源或方向家族、不改分數/門檻/排序，也不回填舊快照。
- 個股所屬「市場別＋產業別」分組在計算中排除個股本身；僅在至少有 10 檔其他同組證券、至少 10 個有效同日同窗 peer return 且 peer 覆蓋至少 70% 時顯示。卡片列出 leave-one-out peer 中位數、個股相對產業百分點及有效 peer/peer 母體數。缺價列不補零，覆蓋不足時明示脈絡暫缺。使用掃描當下的現行產業分類與未作公司行動調整的原始收盤價，故僅作近期觀察背景，不是獨立高品質訊號或歷史 PIT 因子。
- 以目前保存的 10/02 行情和指數在記憶體唯讀重算：as_of_date=10/02、1,946 個主檔、1,938 價格、1,891 法人流、1,945 月營收、1,902 財務、50 個候選，來源狀態仍 partial、基準僅 TPEx；本次預覽未指定 DAG lineage，source_collection_status=unknown。50 張 no-send 卡片均有市場/產業比較欄，43 張有合格 leave-one-out 產業脈絡，7 張明示資料/分組門檻未達；最大單卡 1,403 字元，低於 5,500 上限。這是程式與格式預覽，不是新自然收盤 run、資料庫寫入或通知。
- scheduler/worker 對 `market_signals.py` 與 SETN parser 通過 AST 靜態解析，`git diff --check` 無 whitespace error；未跑測試、未觸發 DAG、未寫 DB、未發通知或交易。新卡片需等待下一次自然 after-close run 才有 runtime/回執驗收。

## 2026-10-04 07:05 台北：修正三立娛樂子網域文章正文 selector

- 唯讀核對 10/04 05:30 台北自然新聞 run（21:30:00–21:41:04 UTC）。12 個來源中 9 個 complete、3 個 partial。三立 30 URLs 中 26 篇解析、有效時間及接受，4 篇 parser reject；逐篇錯誤都是 SETN 娛樂子網域頁面缺少原 selector `article_content_area`/`Content1`。另鉅亨 150 URLs 有 149 parsed、148 accepted：1 次 `ChunkedEncodingError`、1 列內容/重複淘汰，保留為另一種來源狀態。
- 對四個失敗頁做唯讀形狀檢查：HTTP 200、有標題與 NewsArticle JSON-LD，正文在 `article.printdiv`；各頁正文含可解析段落。已增加此精確 fallback，仍只抽文章段落，沒有放寬發稿時間驗證或去重。
- `all_news.py` 的 coverage return/XCom 程式修改時間為 05:51，晚於這次自然 run；該次 `collect_news` log 回傳 `None`，因此沒有 XCom source counters。不是把舊 run 當成新 instrumentation 已驗收；下一次自然新聞 run 須核對 XCom 計數。
- Airflow scheduler 與 worker 對 SETN parser 均通過 AST 靜態解析；主機沒有 `py` launcher，未完成主機端解析。未跑測試、未手動觸發 DAG、未寫 DB、未發通知、未交易。此 fallback 等下一自然新聞批次驗收 URL/解析/有效時間/接受/拒絕與 XCom counters。

## 2026-10-04 06:49 台北：淘汰過時上櫃股利批次路徑，改接官方當前 CSV

- 使用者再次校正目標為自動化全市場訊號。本輪只盤點自然執行與官方批次 feed，未逐家公司研究券商報告。
- 10/04 04:00 台北 `source_extension_collect_process` 自然 run 於 04:02:32 完成、21 個 mapped sources 最終皆 success；兩個來源曾遇 MySQL 1213 deadlock，重試後成功。上市/上櫃季損益及資產負債端點的有效期間都是 2026-06-30。季損益 source raw/accepted/rejected：TWSE 1,054/1,049/5，TPEx 884/884/0；與目前普通股主檔匹配分別為 1,018/1,054、884/892。完整損益表與資產負債表來源聯集匹配為 TWSE 1,053/1,054、TPEx 891/892；未匹配列不補零。
- 此自然 run 的 `tpex_dividends` 舊路徑 HTTP 200、2,483/2,483 接受，但 payload 中最大股利年度僅民國 110 年、最大出表日 ROC 1100804（2021-08-04）。事件通知另有既存 45 日發稿日期 gate，因此舊列不會成為近期股利卡；DAG success 仍掩蓋了上櫃股利 feed 已過時。
- 官方資料平台的[上櫃股利分派情形資料集 58285](https://data.gov.tw/dataset/58285)標示每日更新與政府資料開放授權第 1 版，提供官方 MOPS CSV；平台[下架公告](https://data.gov.tw/news/29113)說明較舊的「經股東會確認」資料集由 58285 的完整資料取代。對 CSV 資源唯讀 GET 得 HTTP 200、`text/csv`、212,865 bytes、963 列，最新出表日 ROC 1151003（2026-10-03）、股利年度 ROC 115；892 個 distinct code 中有 890 個匹配目前上櫃普通股主檔，兩個 feed code 不在主檔，另有兩個主檔代碼無資料列。缺列不視為零股利。
- 已將固定 contract 切換到 `https://mopsfin.twse.com.tw/opendata/t187ap45_O.csv`，新增有 10MB/100,000 列上限、禁止 redirect、僅重試 HTTP 5xx、檢查 CSV MIME/必要欄位、以 UTF-8 BOM 解碼的 `tpex_csv` adapter，更新股利欄位名稱。保留原 45 日事件發稿 gate，不加方向票或調分數。這是官方批次 CSV 的直連唯讀形狀檢查，不是 DB ingest 回執。
- 主機 AST 解析通過；Airflow scheduler/worker 均可 import adapter 並驗證新來源固定 contract。未跑測試、未寫 DB、未手動觸發 DAG、未發 Discord、未交易。新來源等待 10/05 04:00 台北自然 daily run 核 raw/accepted/rejected/effective-date 與 890/892 覆蓋。
- 同日 05:30 台北新聞批次於 05:41 完成、DAG success；12 個來源 coverage 有 9 complete、3 partial。三立 26/30、自由時報 88/100（12 列內容/重複淘汰）、鉅亨 148/150（1 次連線截斷，另 1 列淘汰）。iThome 與 TNL 雖為 complete，date-precision 舊稿分別 38/60、82/90，且 cutoff 日時間不確定 20、6 列；不把 complete 當新稿充足。
- 訊號現況仍未改善：最新 daily snapshot 為 10/02 `ready/partial`、1,946 主檔、1,938 價格、1,891 法人流、1,945 月營收、1,902 財務、50 候選、notified_count=0；最新週末 10/03 是 event-only、10 張卡且行情家族計數為 0。after-close 最新父 run 10/01 logical failed（10/02 20:00 台北執行）；outcomes 仍只有 9/24 的 81 筆 signal-close 與 81 筆 next-close 一日結果，沒有成熟 5/10 日視窗。通知 ledger 32 failed、125 sent，但 receipt id/http status 均為 0，不宣稱已送達。

## 2026-10-04 03:36 台北：TPEx 行情本文截斷時保留最後 HTTP 狀態

- 依 10/02 `tpex_equity_quotes_daily` 兩次自然 attempt 的 `ChunkedEncodingError` 追到 registry catch：現有 bounded byte-range adapter 在所有續傳嘗試仍不完整時會丟 `ValueError`，錯誤字串雖含 `last_http_status`，`source_runs.http_status` 卻只能讀 `exc.response.status_code`，因此記為 NULL。
- 新增 `IncompleteTPExQuoteResponseError`，攜帶最後收到的 HTTP code；registry 在標準 response status 不存在時讀取明確的 `exc.http_status`。這保留傳輸層錯誤與內容未完整的原錯誤文字，不改重試次數、資料接受/隔離、coverage 或訊號分數。HTTP 200 只代表伺服器回應，不等於行情 payload 完整。
- host、Airflow scheduler、worker 對 `adapters.py` 與 `registry.py` AST 解析及模組 import 均通過；尚未經自然 run 驗收。本輪時間為 03:36 台北，04:00 日常收集、05:30 新聞、08:00 週末來源均尚未到；after-close 下一平日為 10/05 20:00。
- 未跑測試、未手動觸發/重跑 DAG、未寫 DB、未發 Discord、未交易；未更動其他工作樹變更。

## 2026-10-04 03:29 台北：批次行情覆蓋預檢與新聞來源時效分層

- 唯讀以現有 TPEx 個股日行情 adapter 查官方全市場端點，contract allowlist 通過、HTTP 200、單次收完整 4,666,641 bytes。原始 11,928 列中，6,442 列可正規化且有有效日（皆為 2026-10-02），5,486 列被正規化器隔離。符合目前 CFI ES* 上櫃主檔並去重後 867 列；主檔分母 892，按列數為 97.2%，另外 25 個主檔代碼在這次回應中沒有匹配列，原因未確認、不當作零量。另 5,575 個正規化列不在目前 CFI ES* 主檔，屬範圍外過濾，不算隔離或接受。這是唯讀預檢，沒有資料庫 source_run/落庫回執，未觸發 Airflow。
- 回看 10/02 自然 after-close 的 map index 9 為 `tpex_equity_quotes_daily`：兩次 Airflow attempt 都因預期 4,666,641-byte 回應中途斷線而失敗，source_run 的接受/隔離為 0/0、HTTP status NULL；當時尚未部署現有 byte-range resume adapter。這次 API 以完整 200 回應，未觸發 resume 分支；仍須等 10/05 after-close 自然 run 驗證實際落庫與上市/上櫃覆蓋。
- 最新新聞自然 run 是 10/03 23:30–23:39 台北，12 個來源 coverage log 有 6 complete、2 partial、1 `empty_after_parse`、3 `no_urls`。TNL 90/90、iThome 60/60 因時間欄可解析而標 `complete`，但按同批資料庫 `created_at` 日期分組：TNL 有 75 篇早於 10/01、7 篇落在 10/01 日期精度邊界、8 篇日期為 10/02–10/03；iThome 分別為 13、25、22。這些舊來源欄只有日期，邊界列實際發稿時刻未知；不能把 `complete` 解讀為新稿充足。最近寫入的 TNL 文章也有大量 2026/01–03 的舊稿。
- 已在 `dags/news_ch/all_news.py` 增加獨立於抓取/解析 `status` 的 `freshness_status` 與分層計數：記錄時間精度列近 48 小時、日期精度且晚於 cutoff 日期、cutoff 當日時間不確定、依目前記錄精度可確定較舊、未來時間。只增加來源 coverage log，不刪除歷史文章、不改方向分數或新聞接受條件。host、scheduler、worker AST 解析通過；下一個新聞自然排程預計 10/04 05:30 台北，尚待 runtime 驗收。
- 唯讀重查 market signals 仍最新 daily 10/02 `ready/partial`、50 候選，最新 intraday 10/03 `ready/partial`、10 張事件卡；outcomes 仍只有 9/24 的 1-session signal-close/next-close，沒有成熟 5/10-session。after-close 最新父 run 10/01 logical failed，10/02 日線 child snapshot 有 50 候選，但舊 digest 因 Discord 40005 payload 過大而 failed；現有分批/receipt 修改尚待下一次自然驗證。沒有新 outcomes 或實際 Discord 收件證據。
- 本輪未跑測試、未手動觸發/重跑 DAG、未寫 DB、未發 Discord、未交易；direct endpoint 僅唯讀全市場預檢，並保留其他既有工作樹變更。

## 2026-10-04 03:10 台北：通知揭露跨媒體同標題，避免把轉載誤讀成獨立佐證

- 新聞交叉配對原本會將不同來源的完全相同標題去重，但卡片沒有說明另有媒體刊載同標題。已在自動 matcher 對完整已載入樣本計算同標題的不同來源數（同一來源重複列不重複計）；即使通知只展示最多兩篇，仍掃完最多 6,000 篇樣本再完成計數。通知只有在 count 大於 1 時才註明「相同標題見 N 個來源，不視為獨立佐證」。不替新聞加方向、不改候選分數或門檻。
- host、Airflow scheduler、worker 對 `market_signals.py` 的 Python AST 解析均通過。這是程式靜態驗證；尚待自然掃描以實際資料確認，不代表已完成新聞批次或收件驗收。現有新聞證據仍受最近 10,000 個 ID / 最多 6,000 篇樣本、exact-title 去重及 `created_at` 時區/原頁時間未核限制；近似標題改寫不會被合併。
- 本輪未跑測試，未觸發/重跑 DAG，未寫 DB，未發 Discord，未交易；保留其餘既有工作樹變更。

## 2026-10-04 03:03 台北：避免每日掃描 trigger 碰撞被當成成功

- 讀 Airflow metadata 與自然 task log：9/28、9/29、9/30 logical after-close runs 的 `trigger_market_signal_notify` 都 `skipped`，log 明確記錄 `skip_when_already_exists`/`DagRunAlreadyExists`。相同邏輯時刻已有 `market_signal__scheduled__...` 子 run，其 `conf.scan_mode=intraday` 且狀態 success；這些既有盤中 run 不能代替日線掃描。9/25 trigger task failed；10/01 logical after-close run 的 daily trigger 成功，唯一可見的 daily 子 run 是 `market_signal__daily__scheduled__2026-10-01...`，快照寫入 10/02，但通知 task 最後 failed。
- `verify_collection_task` 原本把任意 downstream `skipped` 當可接受冪等結果，不查被跳過的實際 target；會把錯誤 scan mode 的同時段 run 誤認成每日掃描已存在。已加入唯讀驗收：從來源 DAG/run 組出預期 target run ID，查 `market_signal_notify` 的該筆 DagRun，並核對 `scan_mode`、`source_dag_id`、`source_dag_run_id`；缺少或 lineage 不合就讓來源驗證失敗，且把 validation error 寫進錯誤訊息。成功與 skipped 都須通過 target 驗證。
- Airflow metadata 現顯示 `market_signal_notify` 未暫停、schedule 為 external-trigger-only；`dag.py` 對 daily trigger 使用包含模式/來源的 run ID 並將 logical date 往後 1 秒。這些是避免重複的現行契約，仍須等 10/05 after-close 自然 run runtime 驗證；不重跑 9 月歷史 run、不回補缺失 daily snapshot。
- 同輪將 `_settle_daily_candidate_outcomes` 的 settled/pending 計數從錯誤 tuple index 19 改為 status index 20；DB 列插入邏輯未變。`market_signals.py` 與 `dag.py` 在 host、scheduler、worker AST 解析通過，兩個 Airflow module import 通過、scheduler DAG import-error 清單未列該檔；未跑測試。
- Airflow 下次自然排程為日常來源 04:00、新聞 05:30、週末來源 08:00（台北）；本輪未手動觸發/重跑、未寫 DB、未發 Discord、未交易。既有工作樹變更均保留。

## 2026-10-04 02:55 台北：確認 outcomes 樣本稀疏原因並修正結算計數

- 唯讀查 `market_signal_runs`，目前只有三筆 daily 記錄：9/25 scheduled run failed；9/26 手動建立的訊號快照 `as_of_date=9/24` ready/fresh、81 候選；10/02 自然快照 ready/partial、50 候選。故 9/24 之後沒有其他成功日線候選日；`market_signal_outcomes` 只含 9/24 的 1-session 結果，這不是僅因報酬命中偏低而少樣本。
- 結算程式支援 1/5/10-session。股票資料目前只查到 9/24、9/29、9/30、10/01、10/02 五個有效交易日；截至 10/02，對 9/24 訊號尚未達到 5-session 出場日期，10-session 更未成熟。後續自然日線快照和行情資料更新後再觀察成熟，不手工補寫 outcome。
- 找到 outcome ledger 的回報計數索引錯誤：插入 tuple 的 `status` 在 index 20，舊程式卻用 index 19（`directional_hit`）計算 settled/pending 數，因而 Airflow payload 的 ledger 計數會錯，但 DB outcome rows 的 status 計算/插入不受此計數讀取影響。已窄幅改為 `row[20]`；host、Airflow scheduler 與 worker AST 靜態解析通過。尚無下一次自然 snapshot 驗證新的 runtime 計數，未跑測試。
- 本輪唯讀查 DB/檔案與程式碼，沒有 DB 寫入、手動觸發/重跑 DAG、Discord 發送、交易或更動 outcome 樣本。來源 run lineage 為 NULL 的舊列不回填。

## 2026-10-04 02:50 台北：辨識券商共識的批次資料供應商線索

- 這輪按「市場級批次來源」搜尋，未逐檔找券商報告。TEJ 公開 E Journal 在 2026/07 與 2026/09 分別刊出以台灣上市櫃券商共識的目標價/投資建議、財務預測作量化因子研究；作者列羅靖霖、韓政均。公開頁只給研究摘要，全文付費，故只能確認 TEJ 目前公開展示這類共識研究，不能由摘要推測因子績效、原始報告品質或研究動機。[2026/07 共識目標價與建議研究](https://eshop.tej.com.tw/E-Shop/journal_detail?journal_id=2026070561)、[2026/09 共識財務預測研究](https://eshop.tej.com.tw/E-Shop/journal_detail?journal_id=2026090543)
- TEJ REST 文件描述可用 API key 以 JSON/XML 擷取 datatables，支援欄位篩選與游標分頁（每頁上限 10,000 列、總計上限 1,000,000 列）；因此具備自動批次取數的介面型態，但沒有證明本專案持有券商估值表的授權或 API entitlement。[TEJ REST API 文件](https://api.tej.com.tw/document_rest.html)
- TEJ 公開的歷史資料庫清單將 `PS` 列為券商分析師預估營收/盈餘，`TQ2` 列為外界分析師預估；該清單標示日期為 2011/01/01，且所列頻率為年資料。這是辨識舊表代碼的線索，不是當前產品目錄/刷新頻率/台灣全市場覆蓋或即時修訂保存的證據。[TEJ 歷史資料庫清單](https://www.tej.com.tw/tejplus/images/tw.pdf)
- 待向可取得的正式產品目錄/授權文件核對：目前可用表代碼與上市/上櫃覆蓋；原始逐分析師、券商、預測期間、EPS/營收/目標價/評等欄位；首次發布/修訂的時間戳及歷史快照；更新頻率、回補/修正語義；API/檔案批次交付、保存/顯示權利、價格與 entitlement。缺少這些欄位就無法做 point-in-time revision/breadth/dispersion 訊號，也不可從共識值倒推出單份報告的估值法與假設。
- 現況仍是「高優先候選供應商、授權和技術契約未核」：crawler `source_registry` 沒有 TEJ 券商估值來源，本輪未呼叫需 API key 的商用資料端點、未接入 feed、未更動訊號分數。若日後授權/時間戳契約核實，先保存逐分析師修訂與有效時間，再於上市/上櫃全市場計算估值修正幅度、分歧、覆蓋廣度；先累積成熟 outcomes，再決定是否能成為方向家族。
- 同時刷新待自然 run 基線（只讀，台北 02:49）：DB 尚無 10/04 當地日期 `source_runs`；Airflow next-create-after 為 source daily 04:00、新聞 05:30、週末來源 08:00。10/03 週末來源最新父 run 為 failed，當批 TPEx 事件/月營收各 0/0/0，TWSE 事件 70/70（effective 10/02，53 發行人）、營收 1,086/1,086（effective 2026/08）、財測差異 7/7（期末 2026/06/30）。因此尚不能驗收新內部人轉讓 feeds、修訂後新聞解析、registry timestamp 修正或新來源 lineage。
- 最新快照仍為 10/02 daily `ready/partial`（候選 50；行情 1,938、法人流 1,891、營收 1,945、季財務 1,902；內部通知計數 0）及 10/03 intraday `ready/partial`（10 個公司事件候選，行情/方向家族計數 0）。成熟 `market_signal_outcomes` 仍只有 9/24、1-session 的兩種 entry basis 各 81 列、各 80 列有方向結果；尚無成熟 5/10-session 樣本。通知 ledger 是 125 sent/32 failed，當前 schema 仍沒有平台 message receipt 欄；內部 sent 不足以證明送達。
- 未手動觸發/重跑 DAG、未寫資料庫、未呼叫商用 API、未發 Discord、未交易、未跑測試。工作樹所有既有變更保留。

## 2026-10-04 01:34 台北：新增全市場內部人轉讓官方批次來源

- 使用者再次明確校正：主線是自動化、批次抓取與持續驗證，不是逐家公司人工查券商報告。此次以整批官方 API 新增 TWSE/TPEx 預定轉讓與未完成轉讓四個 feed，接入 SOURCE_CONTRACTS、SOURCE_DEFINITIONS 與現有 15 分鐘 intraday collector。新 feed 的上市櫃路由數為 9/32，靜態 catalog 與 URL allowlist 驗證通過。
- 只讀 HTTP probe 四個官方端點均回 200。兩個預定轉讓端點各回 1 列，證券代號、申報日期、轉讓方式、正股數及有效期間欄均有值，日期為 10/02；兩個未完成轉讓端點各回 1 列，但證券代號與未轉讓股數為空白 sentinel。這是端點探查，不是本地 collection 回執；不能記成資料庫接受 1 列。Normalizer 對沒有有效代號/正股數的 sentinel 隔離，禁止解讀成零量。
- 新 source record 僅保留市場、公司代號、身分、方法、股數、持股、有效期間與官方理由等研究欄；不保存申報人姓名或受讓人。intraday 卡片明確標示為官方事實觀察、資料只有日期精度、事前申報不等於成交，且不加到多空分數。若自然 run 通過，這批事件候選可累積各自的後續 outcomes；樣本成熟前不調門檻或宣稱預測力。
- 本機、Airflow scheduler 與 worker 的三個 Python 檔 AST 靜態解析通過；scheduler/worker 靜態載入 SOURCE_DEFINITIONS 與 validate_source_contract 均確認四來源存在且 URL allowlist 相符，intraday map 9/32。Airflow 排程唯讀預覽：週末來源下一排程為 10/04 08:00 台北；新聞下一個有效排程點為 10/04 05:30 台北。尚待自然 run 核各 source effective date、raw/accepted/quarantined、上市櫃事件列數、新聞發布時間與 source-level coverage；沒有手動觸發或重跑 DAG、DB 寫入、Discord 發送、交易或跑測試。
- 同時唯讀刷新目前 runtime 基線：最新 daily as_of=10/02、ready/partial，母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、候選 50、notified_count=0。最新 intraday as_of=10/03、ready/partial，市場/行情家族全為 0、10 個公告候選、ledger notified_count=10；該內部欄位不等於平台收件回執。
- 成熟 market_signal_outcomes 仍只有 9/24 訊號日；signal-close 與 next-close 各 81 列、80 列有方向命中欄，只有 1-session，不含成熟 5/10-session 或成本調整結果。after-close mapped run scheduled__2026-10-01T12:00:00+00:00 的 source log dag_id 仍為 NULL，按執行時窗辨識為 11 個來源中 10 成功、TPEx 行情重試後仍 0/0/0；TWSE 行情 1,051/1,051 但有效日 10/01，TWSE 融資融券 1,039/994/45（10/02），兩市可借券 1,717/1,717（10/02），上櫃融資融券 801/801（10/01）、借券餘額 814/814（10/01）。daily scan 子 run 的 snapshot task success、notification task failed，非平台回執。
- 最新自然新聞仍是 10/03 23:30–23:39 台北，執行早於 TVBS/Newtalk/PTS/SETN parser 修改；12 個來源共 1,281 parsed、1,238 valid-time、1,220 accepted：6 complete、2 partial、PTS 43 parsed/0 valid-time/0 accepted、3 個 no-URL。下一輪 10/04 05:30 台北後再核 parser 與發稿時間品質。所有市場 coverage、effective date 和接受/隔離列需逐 feed 報告；工作成功不代表市場完整。

## 2026-10-04 00:55 台北：修正週末來源 DAG 的 cron interval 延遲

- 唯讀查 Airflow metadata：`source_extension_collect_weekend` 最近 scheduled run 的 logical date 為 9/27 08:00 台北，實際啟動於 10/03 08:00 台北並 failed；`DagModel.next_dagrun_create_after` 是 10/04 08:00 台北。原因是 `0 8 * * 6,0` 被預設 cron data-interval timetable 解讀成相鄰排程點之間的資料區間；週日排程點的區間要等到下週六結束，來源 DAG 實際不能在週六、週日各早上執行。
- 將週末 DAG 改用 Airflow 2.10.5 的 `CronTriggerTimetable`，並以 10/04 08:00 台北作首次排程下界，讓排程器在該週日時間點執行、之後每個週末排程點分別執行，避免回補舊週末區間。唯讀 timetable 計算得到首次 10/04 08:00、其後 10/10 08:00；再下一點按 cron 為 10/11 08:00。尚待自然 scheduler run 驗收，不手動觸發。
- `source_extension_collect_after_close` 仍為平日 20:00 自然排程，下一次 10/05；新聞最近 run 為 10/03 23:30–23:39 台北且早於新聞 parser 修正。這輪尚無新的新聞或訊號快照，不捏造 runtime 成效。修改後待核週末各 run 的 effective_date、原始/接受/隔離列、上市櫃涵蓋、快照完整性及下游回執。
- 依 Airflow scheduler 容器的 2.10.5 `CronTriggerTimetable.next_dagrun_info` 與目前 `catchup=False` 做唯讀排程計算；未手動觸發或重跑 DAG、未寫 DB、未發 Discord、未交易。新增修改只限 weekend DAG schedule 和文件。

## 2026-10-04 01:03 台北：排程改動驗證與訊號/新聞自然資料基線

- Airflow 已載入修改後 weekend DAG，`DagModel` 顯示未暫停、下次 run/create-after 均為 10/04 08:00 台北；排程預覽為 10/04、10/10、10/11、10/17、10/18 08:00。下一 run 尚未開始，這只驗證排程，不代表收集成功。
- 唯讀訊號 DB 最新 daily run 仍為 10/02 `ready/partial`：母體 1,946、行情 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、50 候選、內部 `notified_count=0`。最新 weekend intraday/event snapshot 為 10/03 `ready/partial`，行情/市場家族全為 0、10 個事件觀察候選；`notified_count=10` 是內部欄位，不能證明 Discord 收件。
- 成熟 outcomes 仍僅 9/24 一個訊號日：signal-close 1 日 81 列/80 有方向結果，next-close 1 日 81/80；尚無成熟 5/10 日或成本調整樣本，不調門檻。after-close child run `market_signal__daily__scheduled__2026-10-01T12:00:00+00:00` 於 10/02 20:02–20:08 台北結束 failed：快照 task success、通知 task 第 3 次 failed。該 run 不是已確認外部通知。
- 10/03 08:00 台北週末來源 mapped run 的 TWSE 重大訊息 70/70、53 個證券實體、effective 10/02；TWSE 月營收 1,086/1,086、effective 8/01；TWSE 財測差異 7/7、5 個實體、期末 6/30。TPEx 重大訊息與月營收各重試兩次仍 0/0/0。DB 中 after-close run `scheduled__2026-10-01T12:00:00+00:00` 的舊 `source_runs.dag_id` 全為 NULL；截至該輪，TPEx 個股行情 0/0、上市行情 1,051/1,051 但有效日僅 10/01；上市融資融券 1,039 raw/994 accepted/45 rejected（10/02）；可借券股數兩市場合計 1,717/1,717（10/02）；上櫃融資融券 801/801、借券餘額 814/814（10/01）；TWSE 指數只到 10/01、TPEx 指數到 10/02；TWSE 財測達成列 8/0/8（6/30），注意交易列 0，處置列 8/8（公告至 10/02）。
- 最新新聞自然 run 仍為 10/03 23:30–23:39 台北，早於 TVBS/Newtalk/公視/三立 parser 修改；共 1,281 parsed、1,238 valid-time、1,220 accepted：6 個 complete、自由時報 84/100 與鉅亨 148/150 partial、公視 43 parsed/0 valid-time/0 accepted，TVBS/Newtalk/三立 0 URL。新 parser 未經自然 run 驗收；新聞下一自然 run 預計 10/04 05:30。所有 count/status 均按實際來源/有效日分開，不將任務綠燈等同完整。
- 本次 DB 查詢使用只讀交易；Airflow DagBag 排程預覽與 host AST parse 通過。沒有手動觸發 DAG、寫資料庫、發 Discord 或交易。

## 2026-10-04 01:07 台北：官方財報 API 批次目錄搜尋

- 對 TWSE/TPEx 官方 Swagger JSON 的全部 operation paths、summary、description 和 operationId 作系統性關鍵字掃描，TWSE 143 條、TPEx 225 條，未找到名稱/描述指向現金流量表的 route。此結果限於公開規格描述，不推論所有官方入口都不存在。
- 官方 TPEx 政府資料集 94865 是上櫃一般業財報 CSV，頁面列出營收、成本、毛利、營業利益、稅前/稅後損益及基本 EPS，更新頻率每月、政府資料開放授權第 1 版；沒有現金流量欄位。保留現金流品質為正式批次來源缺口，不由資產負債比率或損益欄位替代。暫不加新方向訊號。
- 本次為全目錄／整個產業批次來源檢查，未逐家公司瀏覽或評估券商報告；來源定義和官方頁面依據補入 `docs/source_extension.md`。

## 2026-10-04 00:42 台北：確認自動化範圍並修復三立批次 crawler 漂移

- 使用者再次釐清目標是可排程、批次抓取與自動驗收的全市場訊號；不做逐家公司券商報告上網調查或人工評分。券商資料僅在授權批次原始報告 feed 存在時才納入。
- 唯讀查看三立批次列表 `/viewall?p=1`：HTTP 200、30 張 `.news_list_item`；文章連結為 `/news/{id}`，發布相對時間位於 `.time_box .time`。抽樣八篇的 JSON-LD `datePublished` 都帶 `+00:00`，但其牆上時分與「幾分鐘前／幾小時前」吻合；當成 UTC 轉台北會得到未來約八小時的時間。文章正文目前在 `.article_content_area`，舊 crawler 的 `ViewAll.aspx`、`.newsItems`、`NewsID=` 與 `#Content1` selector 均已漂移。
- 後續批次探查 p=1..4 均 HTTP 200，但四頁完全重覆相同 30 個文章 ID，且回應是 CloudFront cache hit。crawler 現在遇到第一個完整重覆頁即停並在 log 記錄，不把四次重覆抓取冒稱為 120 篇或全站覆蓋；目前只能證實最新 30 個列表項，其他分頁契約仍未知。
- `setn.py` 已改為批次讀新列表與 `/news/{id}`；解析新版 NewsArticle JSON-LD、正文、作者、分類、圖片及文章代碼。只把列表的相對發布時間和擷取觀測時刻放在暫存 `keyword` 作為核時證據，不冒充發布時間寫入 `created_at`。零偏移或無時區 JSON-LD 時間，只有在與列表相對時間區間吻合才收；缺證據或不吻合就留空並由 valid-time gate 排除。來源 offset 衝突仍會寫入 task log，後續自然批次需驗收。
- `all_news.py` 的自動 per-source coverage log 新增 `fetch_or_parse_rejected`、`invalid_time_quarantined`、`content_or_duplicate_rejected`，可直接區分 URL 未解析、時間隔離及內容/去重篩除；既有 URL、parsed、valid-time、accepted 與 coverage status 保留。
- 目前最新自然新聞 run 仍是 10/03 23:30–23:39 台北，早於本次修改；1,281 parsed、1,238 valid-time、1,220 accepted。TVBS、新頭殼、三立該輪 0 URL，公視 43 篇全因舊時間 parser 未收。下一個新聞自然 run 預計 10/04 05:30 台北；不手動觸發。
- 10/03 08:00 台北週末來源批次 parent failed：TWSE 公司事件 70/70（53 個證券實體，effective 10/02）、月營收 1,086/1,086（effective 8/01）、財測差異 7/7（5 個實體，期末 6/30）；TPEx 公司事件與月營收失敗，各 0/0。週末盤中快照 `as_of=10/03`、ready/partial，但行情/其他方向家族 0 檔、10 個公告觀察候選；10 筆內部通知 ledger 標成 sent，但 ledger 沒有平台 receipt 欄，不能據此確認 Discord 已收件。沒有手動發送。
- 目前最新日線快照仍為 10/02 `ready/partial`：母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、50 候選、通知計數 0。`market_signal_outcomes` 仍只有 9/24 一日成熟樣本，signal-close/next-close 各 81 列、80 列有方向結果，沒有成熟 5/10 日或成本調整結果，不調門檻。
- 最新 after-close 仍是 10/02 20:00 台北 run failed；11 個來源 map 中 10 個成功，TPEx 個股報價 0/0/0；daily child 快照 task success，但通知 task 第三次仍 failed。週末 source-extension 下次預計 10/04 08:00，after-close 下次預計 10/05 20:00。舊 `source_runs.dag_id` 仍為 NULL，lineage 不能單靠 run_id 消歧；TAIFEX 尚未註冊成 runtime source。
- 本次只做公開列表/文章批次契約唯讀探查、改 crawler/coverage log 與追蹤文件；不跑 DAG、不寫 DB、不送 Discord、不交易。`setn.py`、`all_news.py` 的 host/scheduler/worker AST 與 `git diff --check` 均通過；自然 run 才能確認 accepted/quarantine 數、來源時間及後續快照。

## 2026-10-04 00:13 台北：修復兩個新聞批次來源的頁面結構漂移

- 使用者目標維持自動化：修的是全來源批次 crawler 的列表/時間解析，不依公司個案人工找報導或評券商。先以現有 10/03 23:30 台北自然 run 的 TVBS、newtalk、三立 0 URL 覆蓋作為根因線索，再用官方網站目前批次列表頁唯讀核 selector。
- TVBS 舊 `/realtime/news/{date}` 列表路徑當前回 404/串流截斷；新的 `/realtime` 回 HTTP 200，頁面 JSON-LD 有 16 個 `NewsArticle`，含官方文章 URL 與 ISO `datePublished`。已改成從 JSON-LD 的批次 `NewsArticle` 列表讀 URL，並支援 `@graph` 文章資料、offset 時間及 `.article-editor-content` 正文。新 selector 尚未經 crawler runtime 執行。
- Newtalk 當前 `/news/summary/{台北日期}/1` 回 HTTP 200，含 23 個 `/news/view/{日期}/{id}` 文章卡；文章 detail 的 `<time datetime>` 和 JSON-LD `datePublished` 均為一致的 Z 時間。本來的 `.news-list-item`/`ul.category-list` selector 找不到新版卡片。已改用新版 URL selector 去重收集，並從文章頁 `<time>` 或 JSON-LD 取發布時間；列表日期在呼叫時用台北時區計算，避免 worker 跨日後沿用模組載入日。等自然 run 驗收接受數與時區。
- 三立目前 `/viewall?p=1` 回 HTTP 200 並有 30 個 `.news_list_item`，而既有 crawler 找舊 `.newsItems`，所以此次 0 URL 可歸因於 selector 漂移；但文章 JSON-LD `datePublished` 帶 `+00:00`，其時分又與頁面顯示的台北本地時分相同。這是時區語義衝突，未改 parser 轉時區或放行資料，避免產生未來時間；待更多同文頁面證據/發布契約確認後再處理。
- TVBS 與 Newtalk 的程式修改只做本機/ scheduler / worker Python AST 靜態解析，均通過；未跑測試、未手動觸發 DAG、未寫 DB、未發 Discord、未交易。Airflow metadata 顯示下一新聞自然 run 預計 10/04 05:30 台北；週末來源 DAG 預計 10/04 08:00 台北，自然建 catalog 後再看 TAIFEX 是否登錄；TAIFEX 仍未出現在 source_registry/source_runs，after-close 收集尚未 runtime 驗證。
- 市場訊號 runtime 尚未有新快照：日線仍 10/02 `ready/partial`、50 候選、通知回執 0；週末盤中仍 10/03 `ready/partial`、零行情家族、10 個公告候選。Outcome 仍只有 9/24 一日樣本，門檻不調整。

## 2026-10-03 23:55 台北：釐清 after-close 回執的 DAG lineage 碰撞

- 依使用者再次校正，工作只聚焦可排程、可批次抓取及可自動驗收的訊號；不逐家公司上網調查券商報告。券商研究只在有授權且可批次取得原始報告時納入。
- 讀 Airflow metadata 與本機 task logs：`source_extension_collect_after_close / scheduled__2026-10-01T12:00:00+00:00` 於 2026-10-02 20:00–20:04 台北自然執行，11 個 mapped source 中 10 個任務成功、TPEx 個股行情任務兩次 body 截斷後失敗；`verify_collection_and_triggers` 因 map index 9 failed 而失敗。TPEx 回應宣告 4,666,641 bytes，兩次分別讀到 704,105 與 4,193,897 bytes，沒有有效行情列；本輪 TWSE 行情有效日 10/01，1,051/1,051 接受，而 TPEx 行情 0/0/0，故上櫃行情覆蓋缺失。
- `source_runs` 並非未寫入。唯讀查得這個 run ID 有 17 列，但舊列的 `dag_id` 全為 NULL；其中 10/01 20:15 台北的 `source_extension_collect_process` 也使用相同 logical run ID，另有其月營收/重大訊息列。因此只按 `dag_run_id` 會混入不同 DAG；按 `(dag_id, dag_run_id)` 查不到舊資料是 lineage 欄位缺失，不是來源回執未持久化。新版本 `_airflow_lineage` 已保存 `dag_id`，仍需等待自然排程確認資料庫回執與通知能正確按 DAG/run 關聯。
- 隔離 10/02 20:00–20:05 台北的 after-close 實際批次（不把同 run ID 的前一個 process run 混入）後，來源有效日期/列數如下：上櫃借券餘額 10/01 814/814；上櫃融資融券 10/01 801/801；上市融資融券 10/02 1,039 原始/994 接受/45 隔離；跨上市櫃可借券賣出餘額 10/02 1,717/1,717，其中上市 980、上櫃 737；上市行情 10/01 1,051/1,051；TPEx 指數 10/01、10/02 各 1 列，TWSE 指數只到 10/01 1 列；TWSE 財測達成差異期末日 6/30，8 原始/0 接受/8 隔離；TWSE 注意交易資訊本輪 0 原始/接受列；上市處置公告 8/8，公告日期 9/22–10/02。這些成功 HTTP/任務不能抹去有效日落後、隔離或個股行情缺口；公告空列也不代表個股成交量為零。
- after-close 的 downstream trigger task 成功啟動每日掃描；子 run `market_signal__daily__scheduled__2026-10-01T12:00:00+00:00` 的快照 task 成功，但通知 task 第 3 次仍 failed。資料快照為 10/02 `ready/partial`、母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、50 候選、`notified_count=0`；這不是可確認的外部通知。
- 最新週末盤中快照仍為 10/03 `ready/partial`、母體/個股行情等市場資料家族皆 0、10 個公告候選；不可稱為盤中行情訊號。成熟 outcomes 仍只有 9/24 一個訊號日，signal-close/next-close 各 81 列、80 列有方向結果，無成熟 5/10 交易日或成本調整樣本，不改門檻。
- 10/03 23:30–23:39 台北自然新聞批次的 12 個來源記錄為 1,281 URL/parsed、1,238 valid-time、1,220 accepted：6 complete、2 partial、1 公視 43/43 parsed 但 0 valid-time/accepted、3 no-URL；TVBS 列表失敗已在前一輪 log 見 HTTP 404。這批早於公視 metadata 時間 fallback 與細分 coverage status 修改，不代表修改已 runtime 驗證；下一次自然新聞 run 再核。新聞仍只作背景脈絡，不作方向票。
- 本輪唯讀檢查並更新追蹤，沒有程式/門檻改動、測試、手動 DAG、DB 寫入、Discord 發送或交易。等 10/04 週末自然批次驗證新版 source lineage，再等下一個平日 after-close 驗證 TPEx range-resume、effective date、接受/隔離、雙市場覆蓋、通知回執與後續 outcomes。

## 2026-10-03 23:47 台北：自然新聞批次驗收並修復公視發稿時間解析

- Airflow 自然 run `news_ch_collect_process / scheduled__2026-10-03T10:30:00+00:00`（台北 logical 18:30、實際 23:30–23:39）之 `collect_article_url`、`collect_news`、`delete_and_insert` 均 success。`collect_news` 的來源 coverage log 共 12 個已設定來源：9 個有網址、合計 1,281 URL/1,281 parsed/1,238 valid_time/1,220 accepted；狀態為 6 complete、2 partial、1 empty_after_parse、3 no_urls。不能以 DAG 綠燈聲稱 12 個來源完整。
- 分來源：complete 為 ETtoday 480/480、iThome 60/60、thenewslens 90/90、中央社 100/100、中時 198/198、聯合報 60/60；partial 為自由時報 100 URL/100 parsed/100 valid_time/84 accepted、鉅亨網 150/150/150/148；公視新聞 43 URL/43 parsed/0 valid_time/0 accepted；TVBS、新頭殼、三立新聞均 0 URL。唯讀看 `collect_article_url` log，TVBS 列表 URL 回 HTTP 404；新頭殼及三立該次無網址但沒有記錄 request error，暫不能判定是無新稿或 selector/頁面問題。
- 用批次抓取清單內 3 篇公視頁面核 parser 根因：HTTP 200，頁面 `<time>` 文字是空值，但 `article:published_time` 與 JSON-LD `datePublished` 都有帶 `+08:00` 的發布時間。現有 parser 只讀空的 `<time>.text`，因此 43 篇時間全被有效時間清洗器排除。已在 `pts.py` 改成依序讀 `<time datetime/text>`、Open Graph publication metadata、JSON-LD `datePublished`，將 ISO offset 轉為 Asia/Taipei；仍無法解析的文章繼續不入庫。沒有逐家公司搜尋或手工評比報告。
- `all_news.py` 的來源 coverage 狀態也改為分開標示 `no_urls`、`no_parsed_articles`、`no_valid_time`、`empty_after_filter`、`partial`、`complete`，避免把「有解析但完全缺時間」錯報成空解析。這兩項修改都晚於本次自然 run，尚待下一次自然新聞收集 runtime 驗收；不表示本批 43 篇已回填。本新聞 DAG 不觸發市場掃描，本輪沒有新增市場候選或 outcome。
- 公視頁面時間探查為唯讀；本機、scheduler、worker 的 `all_news.py`/`pts.py` Python AST 均通過。未跑測試、未手動觸發/重跑 DAG、未寫 DB、未發 Discord、未交易；保留其他既有工作樹變更。

## 2026-10-03 23:14 台北：修正自動新聞配對樣本截斷盲點

- 唯讀查詢新聞 DB：`news` 目前只有 `id` 主鍵與 `(source, article_id)` 唯一索引，沒有 `created_at` 索引；因此配對仍限定最近寫入 10,000 個 ID，避免每次訊號掃描都按發稿時間掃全表。以 2026-10-03 23:10 台北時間為觀察點，該 ID 視窗中有 4,445 篇落在 48 小時／日期精度邊界規則內；套用 matcher 的非空標題或內文前段條件後，有 4,441 篇可配對，原 4,000 筆樣本上限會截掉至少 441 篇（約 9.9%）。時間欄時區及該 ID 視窗外是否另有晚到文章仍未核。
- 將 `_attach_recent_news_context` 的樣本上限由 4,000 提高至 6,000，並在同一 10,000-ID 視窗計算符合時間且有標題/內文的總數；matched/no-match context 都記錄 eligible count、實際樣本數及截斷旗標。通知在樣本截斷時加明確警語；無匹配時分清合格文章數與實際比對數，不把樣本內無匹配說成沒有相關報導。維持既有新聞非方向背景，不改任何分數、方向家族或候選門檻。
- 同資料時間範圍的唯讀 SQL 核對：4,441 篇合格列全部落入新 6,000 上限，未觸發樣本截斷。Python AST 在本機、Airflow scheduler 與 worker 均通過。未跑測試、未手動觸發/重跑 DAG、未寫 DB、未送 Discord、未交易；下一次自然訊號掃描再核通知中的實際計數與截斷標籤。

## 2026-10-03 20:52 台北：確認董監月頻來源自然驗收時間

- 唯讀查詢 Airflow metadata：`source_extension_collect_monthly` 未暫停；最新 scheduled run 於 2026-09-24 20:00 UTC 啟動（台北 9/25 04:00），該 run 仍只收進 2026-08-01 報表期別。`next_dagrun_create_after=2026-10-24 20:00 UTC`，即台北 10/25 04:00，為下一次自然月頻收集時間；目前沒有到期的月頻 run。依限制不手動觸發。
- 因此剛才的持股去重修正可先等 10/25 批次是否新增不同 effective month；之後第一個符合 PIT cutoff 的自然日線 run 才能計算月變化。若來源仍只發布同一期，繼續標示沒有前期基準，不產生方向票。

## 2026-10-03 20:48 台北：董監持股訊號按人去重，保留衝突隔離

- 唯讀檢查 source_runs：TWSE 最新董監持股 mapped run 為 27,475 原始列、26,804 接受、0 隔離、671 上游重複；TPEx 為 17,568 原始列、17,139 接受、0 隔離、429 重複。兩 feed 的 source_records 目前都只有 effective_date=2026-08-01；接受代碼數分別為 TWSE 1,084、TPEx 891。這些是來源列/代碼計數，不代表已有多月可比歷史。
- 對該全市場快照按「公司、月份、姓名」計數後，TWSE 26,804 列對應 22,520 個人鍵（4,284 多職稱重複列）；TPEx 17,139 列對應 13,747 個人鍵（3,392 多職稱重複列）。TWSE 有 5 個公司期別中，同一姓名的目前持股值互相不一致；TPEx 此快照沒有此類衝突。此證據限於目前唯一可用月份，不能外推為跨月穩定比例。
- 既有日線讀法直接 `SUM(metric_value)`，會把同一人因多個職稱重複列出的持股再次相加。已改成在 PIT 條件下按公司/期別/姓名去重，值一致時只計一次；若同一公司期別任何一人有衝突值，整個公司期別不供董監方向家族使用，避免任選值或把衝突視為零。方向門檻 5%、其他家族、候選門檻與排序未改。同步更新 source-extension 文件說明。
- 本機 Python AST parse 通過；把新 SQL 以唯讀查詢套用至目前已收集資料，得到 1,970 個可用公司期別（全部為 2026-08-01），5 個 TWSE 衝突公司期別被排除。沒有執行測試、沒有重跑 DAG、沒有寫資料庫、沒有發 Discord 或交易。由於目前僅一個 effective month，董監持股變化尚無前期基準；下一個自然月批次後再核兩期、有效日、接受/隔離、上市櫃覆蓋及是否有候選卡方向票。

## 2026-10-03 20:36 台北：確認工作主軸為自動化批次訊號

- 使用者再次澄清，主要交付是可排程、可批次抓取、能把市場資料/新聞自動關聯並持續回看成效的訊號系統；逐家公司上網搜尋、人工評比券商報告不是主要工作流程。券商研究只有在可授權、可批次取得原始內容的來源存在時，才作為自動資料源補充，不因個股候選而逐家查網頁。
- 本次檢視確認已有自動化基礎：`source_extension` 以 allowlist/registry 排程收集交易所批次行情、營收、財務、法人/持股、借券與公司公告；`market_signals` 對官方記錄作點時配對、形成日線/盤中候選並保存 outcome；新聞卡片由新聞資料庫近 48 小時樣本批次匹配公司名稱/代號，最多顯示兩則。新聞目前明確只列背景、不計方向家族；`news.created_at` 的原頁發稿時間與時區仍未核，匹配樣本也受最近寫入 ID 與樣本上限限制，不能把「無匹配」解讀為沒有新聞。
- 後續以自動化驗收為優先：每批檢查有效日期、接受/隔離、上市/上櫃覆蓋與新聞逐來源收集狀態；再使用既有批次欄位建立一般化、點時正確的跨訊號脈絡，等 1/5/10 交易日 outcomes 成熟後評估。只在有新增且可量化、可全市場重跑的證據時改規則；不因單檔故事或單一日期調門檻。此輪只校正工作範圍與追蹤方式，未改程式、未觸發 DAG、未送 Discord、未交易。

## 2026-10-03 20:27 台北：資產負債表背景點時覆蓋回算

- 重新核對最近日線資料可用時間：10/02 日線掃描 `started_at=2026-10-02 20:02:35` 是台北時間，對應 UTC 12:02:35。最近財報批次在 Airflow 的 source-extension process run `scheduled__2026-10-01T20:00:00+00:00` 實際於 10/02 20:00–20:04 UTC 執行（台北 10/03 04:00–04:04）；八個 balance-sheet mapped feeds 成功，但同輪 CBC FX map 失敗、parent verification failed。
- 八個財報 feed 的記錄 `updated_at` 為 10/02 20:02:xx UTC，即台北 10/03 04:02，比 10/02 日線掃描 cutoff 晚約 7 小時 59 分。以程式 `_source_updated_at_cutoff_utc` 的 UTC cutoff 正確回算，10/02 的 50 檔候選有 0 檔可在當時使用該批資產負債表；若將 DB 時刻誤當台北時間，則會錯把 50 檔都納入，構成前視。故本輪不改 cutoff/helper、不回填舊卡；這確認先前的點時排除是正確行為，不是新脈絡缺漏。
- 這批記錄在 10/03 04:02 台北已落庫，報表有效期別 6/30；10/05 下一次自然收盤掃描時，若股票仍入選且 source row 未隔離，理應可進入 180 日財報背景。屆時只核新 run 的候選卡期別、比率欄位、官方 URL、上市櫃覆蓋與通知回執，不回寫 10/02 歷史候選。未手動觸發 DAG、未送 Discord、未交易。
- 成熟度回算：程式已定義 1/5/10 個交易日 outcomes。10/02 時點的個股價格／指數日期集合只有 9/24、9/29、9/30、10/01、10/02；唯一訊號日 9/24 之後目前僅 4 個可用交易日，所以 5 日視窗尚未成熟、10 日更未到。且該批 TAIEX 缺 10/02、TPEX 有 10/02；結算仍須個股與對應市場指數的 entry/exit 都存在。下次自然結算先核 9/24 五日窗和缺基準日的 pending 狀態；目前樣本不足以調門檻。

## 2026-10-03 20:03 台北：季度資產負債表批次脈絡接進日線候選卡

- 依使用者更正，工作重點是可排程、可批次驗證的來源與通知，不做逐家公司線上蒐集券商報告。TWSE/TPEx 已有 6 個上市與 2 個上櫃季度資產負債表 OpenAPI feed，收集欄位含流動資產/負債與總負債/權益；官方 published Swagger 中可見損益表、資產負債表路徑，未找到命名為公司現金流量表的股票級 OpenAPI route。這只代表已發布 OpenAPI 目錄未列該批次路徑，不推論目錄外不存在資料。[TWSE OpenAPI](https://openapi.twse.com.tw/v1/swagger.json)、[TPEx OpenAPI](https://www.tpex.org.tw/openapi/swagger.json)
- 唯讀資料庫盤點最近一次自然季度財報來源收集（Airflow source-extension process 實際於 2026-10-02 20:00–20:02 UTC 執行，即台北 10/03 04:00–04:02）：8 個資產負債表 feed 最後一次 mapped attempt 均成功，正規化列合計 1,975/1,975 接受、0 隔離；資料有效期別最新為 2026-06-30。上市 feed 1,084 列、上櫃 feed 891 列；依當前證券主檔比對該期別可對上上市 1,053/1,054、上櫃 891/892，另有 32 個接受代碼不在當前主檔。此為現行主檔交集，不是期末 point-in-time 母體；未匹配代碼不當作零或隔離列。各 source run 的 row/accepted/rejected 與 `source_records` 中跨次累積代碼數是不同粒度。
- 已在 `dags/source_extension/market_signals.py` 的日線候選建置中點時查詢 `quarterly_balance_sheet`：要求 quality `ok`、期末日不晚於訊號日、官方發布 event_time 不晚於掃描開始、updated_at 不晚於 source 截止，且期末距掃描日不超過 180 天。非金融產業才顯示有效分母下的流動比率、負債/權益比，帶期別和交易所原始資料連結；分母缺漏/為零不計算，金融業不套一般比率解讀。卡片明示期末存量不等於營業現金流；不計方向家族、不加分、不改排序或候選門檻。故這是既有自動訊號的量化風險背景，不冒稱新預測訊號。
- 當前 runtime 盤點：daily 最新 2026-10-02 `ready/partial`，母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、候選 50；`market_signal_outcomes` 仍僅 2026-09-24 signal-close/next-close 各 81 筆一交易日 settled，無成熟 5/10 日樣本。週末 intraday 最新 2026-10-03 08:02 的掃描是 10 筆事件候選、無日線價格母體，狀態 `partial`；資料表 `notified_count=10` 是系統記數，非外部收件回執。
- 最近 after-close parent 的自然 run 仍失敗：TPEX 個股日行情兩次 `ChunkedEncodingError`，raw/accepted/quarantine 皆 0；TWSE 行情 1,051/1,051 接受、有效日 10/01。其 daily signal child snapshot 可產生 50 候選但 Discord 主摘要回 `Request entity too large`，資料庫 `notified_count=0`；僅失敗告警有送達紀錄，不把它算候選通知。非同步週末 collect 最新 task 狀態 `partial`，TPEx 公司事件及月營收 mapped attempts 失敗、TWSE 公司事件/月營收與預測差異收集成功；沒有來源級日誌就不把綠燈 parent 當完整新聞覆蓋。legacy 新聞管線最新 10/03 16:30 台北 run 的三個 task 為 success，但該次執行早於 source-by-source coverage 記錄版本，不能由 Airflow 綠燈推定 13 個來源都完整。以上皆唯讀檢查，未重跑 DAG、未送 Discord、未交易。
- 靜態工作完成後仍須等下一次自然日線 run 驗證欄位是否可呈現及 Discord 分批是否成功；本次只做 Python AST / diff whitespace 靜態驗證，不跑測試、不觸發 DAG。不要用單一日期結果調整方向門檻。

## 2026-10-03 19:43 台北：日線市場廣度補上上市櫃分母與隔離列

- 靜態盤點發現，日線摘要原先顯示上市/上櫃漲跌家數與行情代碼數，卻沒有分市場主檔分母，也把隔離列和缺漲跌值合併成「未分類」，讀者難判斷該廣度列的資料範圍。
- 已在 market_signals.py 將當次日線掃描的上市/上櫃主檔數經 task payload 傳到 digest；同日官方行情按 distinct 證券代碼分開列總代碼、接受列/現行主檔數、隔離列、已接受但漲跌值缺漏列，以及上漲/下跌/平盤數。沒有同日行情時明示「缺資料，不當作零漲跌」。
- 分母是當前證券主檔，不是訊號日 point-in-time 母體；差額未區分停牌、無交易和採集缺漏，故只作來源範圍說明、不視為品質分數或方向訊號，也不改候選排序/閾值。本機 Python AST 語法解析通過；未跑測試、未觸發 DAG 或送 Discord，需等自然 daily digest 驗收實際欄位與 Discord payload 長度。

## 2026-10-03 19:35 台北：用官方重大訊息批次欄位補強庫藏股進度訊號

- 直接唯讀讀 [TWSE](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)/[TPEx](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O) 已登記官方 OpenAPI（不是逐家公司搜尋）：本次 TWSE 回 70 則，相關公告日 1151003；其中 5 則正文含「庫藏股／買回股份」字樣。TPEx 回 36 則，相關公告日 1151002；其中 1 則含相關字樣。字詞命中不是事件接受數：TWSE 其中一則是併購註腳提到庫藏股、另一則是子公司買回，不是代碼所屬上市公司買回；資料庫 source-run 的接受/隔離及上市櫃母體覆蓋未由這次直讀取得，仍等自然收集驗收。
- 本批官方正文可機器解析出三個上市公司及一個上櫃公司的量化進度：2474 內文目前買回金額 NT$299,604,810、累積庫藏股占股本 5.76%，標題明示達新台幣三億元門檻；3708 實際買回 NT$301,447,236、3,000,000 股、累積 3.34%；TPEx 3684 計畫 1,500,000 股、實際 987,000 股、累積 3.26%，公告稱買回期間屆滿且未執行完畢；1437 完成買回 750,000 股、累積 0.62%，未達現行 2%/NT$300M 過濾門檻。這是事件資料示例，不是報酬驗證。
- 現有事件分類只抓「三億元」中文標題或狹窄的「達 2%」寫法，會漏掉正文數值 301,447,236 元與欄位「比率(%):3.26」。已在 dags/source_extension/market_signals.py 改為由有買回事件的標題限定範圍，解析官方欄位數值；只在標題明示門檻、實際金額達 NT$300M 或計畫/公告股數比例達 2% 時產生中性事件卡。公司子公司買回會排除；僅在併購正文註腳提到庫藏股不會觸發庫藏股卡。官方樣本的併購正文還含公開收購法律範本用語，故公開收購事件分類也收窄為標題需明示「公開收購」，避免把正文範本當成公開收購事件。卡片摘要列出有提供的計畫股數、實際買回股數/金額、累積占股本比率與價格區間，不投方向票、不加每日家族、不宣稱有股價支撐。
- 本次官方 endpoint 直讀不是 Airflow ingestion：沒有呼叫 ingest、沒有 source_runs，也沒有實際送 Discord。本機 market_signals.py 通過 Python AST 語法解析；沒有執行測試或 runtime classifier。需等下一次自然 intraday 收集/掃描確認有效日、接受/隔離列、TWSE/TPEx coverage 及新卡片去重/欄位呈現；不手動重跑 DAG。

## 2026-10-03 19:24 台北：TPEx 官方監理批次來源端點與欄位驗證

- 從 Airflow worker 直接唯讀讀取兩個已登記官方 OpenAPI；兩端點本次皆 HTTP 200，先前紀錄的 worker `ConnectionError` 本次未重現。這是端點／adapter probe，沒有呼叫 `ingest_source`、沒有新增 `source_runs` 或持久化資料。
- 注意交易端點原始 22 列、22 個代碼、資料日 `1151002`（2026-10-02）；透過現行 adapter 與 1,946 筆上市/上櫃 stock-master codes 配對後為 16 列、16 接受、0 隔離，皆為上櫃，6 列未匹配主檔而過濾。
- 處置端點原始 25 列、21 個代碼、公告日涵蓋 `1150922` 至 `1151001`；adapter 配對後 11 列、11 接受、0 隔離，皆為上櫃。日期區間解析和 registry 必填欄在這次目前資料樣本均可通過；不同日期／處置區間維持分列，不合併成單一當日股票訊號。
- DB 仍顯示兩來源 `approved/enabled`, `daily_after_close`, `last_success_at=NULL` 且無 `source_runs`；本次 probe 不是來源收集成功，並未解決持久化覆蓋。現有 daily renderer 已把這兩種官方資料作公告與交易限制背景，不加入方向家族。下次自然 after-close 預計 10/05 20:00，要驗收 source_runs、effective date、raw/accepted/quarantined、上櫃 stock-master 覆蓋及候選卡是否按公告／有效期間呈現。

## 2026-10-03 19:15 台北：修復 TPEx 官方全市場行情截斷的恢復流程

- 依資料庫最近 after-close `source_runs` 查到 TPEx 日行情同一批兩次 task attempt 都因 `ChunkedEncodingError` 失敗、HTTP status 空值、raw/accepted/quarantined 均 0；錯誤中的 Content-Length 是 4,666,641 bytes，兩次分別只收到 704,105 與 4,193,897 bytes。對照同批 TWSE 報價為 HTTP 200、1,051/1,051 接受。這造成上櫃 892 檔缺少此來源報價，不可解讀成無行情或零交易。
- 對官方 TPEx endpoint 做唯讀協定探測：HEAD 回 HTTP 200、Content-Length 4,666,641、Accept-Ranges=bytes；`Range: bytes=0-1023` 回 HTTP 206、`Content-Range: bytes 0-1023/4666641`，1,024 bytes。證據支持用 byte-range 續傳，而不是不斷重抓完整大檔。
- Adapter 已針對這個官方批次 endpoint 使用 identity encoding；串流未完整時從已收位元組 offset 發 Range，並驗證 HTTP 206、起點、總長、分段大小及 10 MiB 上限，完整收齊後才 JSON parse，最多 6 次 request；伺服器忽略 Range、改變總長或不符合 Content-Range 即 fail closed。修改在 `dags/source_extension/adapters.py`；操作契約寫入 `docs/source_extension.md`。
- 目前只有唯讀協定證據與靜態程式修改，尚未實際跑續傳後完整資料或自然 ingestion；下一次自然 after-close 仍須確認 effective date、raw/accepted/quarantined、上市/上櫃主檔覆蓋和 daily snapshot 由 partial 是否改善。不手動觸發、不把 HTTP 206 部分回應當作完整成功，不重送通知或交易。

## 2026-10-03 19:05 台北：新增可排程的期交所選擇權法人批次來源

- 依使用者更正，持續聚焦自動化批次來源與可稽核資料品質，不做逐家公司線上調查。把官方 TAIFEX `MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate` 加入 `daily_after_close` registry，篩選臺指選擇權 CALL/PUT × 三類法人，完整日預期 6 列。直連官方端點核對：原始 30 列、最新有效日 2026-10-02，目標契約 6 列且淨未平倉全有值。這是端點形狀檢查，不是排程 ingestion 成功證據；尚無 accepted/quarantined task 統計。
- Adapter 以契約＋買賣權＋法人＋有效日去重並保留 source payload；daily digest 只呈現同有效日資料，缺列不補零。5 交易日變化須六組有 6 個共同日期才顯示。CALL/PUT 位置可能屬多腿策略，三類法人也是多機構彙總，故只當市場背景，不加個股多空 family 或分數。[TAIFEX OpenAPI](https://openapi.taifex.com.tw/)；[選擇權買權／賣權統計口徑](https://www.taifex.com.tw/cht/3/callsAndPutsDate)
- 本來源只覆蓋臺指選擇權市場彙總，不代表上市/上櫃個股覆蓋；上市期貨與櫃買指數期貨維持既有另一 feed。原始 API 有 30 列、預期 adapter 接受 6 列、其餘 24 列因非目標契約而過濾（不是 quarantine）；實際 ingestion raw/accepted/quarantined 只能等自然 after-close task log。新增定義目前尚未由下一次自然排程驗證，週末不手動觸發。
- 19:11 台北唯讀複查：最新 daily snapshot 仍為 10/02 `ready/partial`（母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、候選 50、通知欄 0）；10/03 最新掃描是週末 intraday `partial` 事件批次 10 筆，不是價格掃描。`market_signal_outcomes` 共 162 列，全部仍來自 9/24 的 signal-close/next-close 一交易日結算，沒有成熟 5/10 日樣本。10/02 20:00 after-close parent 仍 failed，週末無新 after-close run；DB `source_registry` 尚無舊期貨或新選擇權 TAIFEX 定義，須等自然排程 bootstrap。新聞最近成功 run 仍是 10/03 16:30 台北時間（18:38:48 完成），在本次 source_coverage 程式與選擇權修改前；新的 per-source coverage 尚未 runtime 驗證。
- 修改範圍：`dags/source_extension/adapters.py`、`registry.py`、`market_signals.py`、本文件與 `docs/source_extension.md`。下次自然 after-close 預期 2026-10-05 20:00 台北時間，需對照 DB registry 是否已 bootstrap、effective date、mapped source 狀態、接受/隔離數及通知卡同日覆蓋；任何缺資料都不解讀為零部位或方向訊號。

## 2026-10-03 18:28 台北：新增官方全市場期貨部位背景來源

- 依自動化方向擴充一個可排程的市場級來源：期交所官方 OpenAPI `MarketDataOfMajorInstitutionalTradersDetailsOfFuturesContractsBytheDate`，批次抓取後只保留臺股期貨、櫃買指數期貨兩項契約及三類法人，完整日期預期 6 列。獨立直連檢查最新有效日為 2026-10-02；原始回應共 66 列、22 種契約，篩選後為 6 列。來源端點以 `Accept: application/json` 回 JSON；這是內部採集驗證，不代表已確認對外再散布授權。
- 新來源已登記到 `daily_after_close` registry；adapter 保留官方淨交易與淨未平倉欄位，daily digest 顯示與掃描日對齊的淨未平倉。只有六組皆有共同有效日且累積至少六個同步日期時才顯示近五交易日變化；缺列不補零。類別是多機構彙總，只當市場背景，不加方向家族、投票或門檻；未有樣本外驗證前不把它宣稱為預測訊號。[期交所 OpenAPI](https://openapi.taifex.com.tw/)；[期交所法人交易資料口徑](https://www.taifex.com.tw/cht/3/futContractsDate)
- 修改範圍：`dags/source_extension/adapters.py`、`registry.py`、`market_signals.py`，以及本文件和 `docs/source_extension.md`。接下來等自然 after-close 收集核對 effective date、raw/accepted/quarantined 列數及六組完整度，再確認通知卡能呈現日期與缺列；不手動觸發 DAG、不送 Discord、不交易。
- 既有市場狀態仍須分開看：截至前次盤點，10/02 daily snapshot 為 partial；當晚 after-close 因 TPEx 個股行情 HTTP 傳輸錯誤未完成，Discord digest 曾因 payload 過大失敗；outcomes 僅有 9/24 一日樣本且沒有成熟 5/10 日視窗。這次接入不會掩蓋上述缺口，也不會調訊號門檻。

## 2026-10-03 18:50 台北：自然批次重查、新聞覆蓋與來源診斷

- 唯讀查得最新日線仍是 10/02 `ready/partial`：母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、候選 50。TWSE 官方行情有效日到 10/01（1,051/1,051 接受）；TPEx 行情在兩次 mapped attempt 都是 0/0/0。TAIEX 來源有效日僅到 10/01、TPEx 指數到 10/02，不可把不同基準日當同日相對報酬。10/02 daily digest ledger 一筆 `failed`、無 notified_at；另一筆短訊息為 `sent`，不計為 digest 收件回執。`market_signal_outcomes` 仍只有 9/24 的 signal-close/next-close 各 81 筆一日結果，無成熟 5/10 日成本調整證據。
- 10/03 04:00 台北自然 `source_extension_collect_process` 對 13 個舊 daily sources 收集：12 個來源最後一次 attempt 成功；`cbc_fx_daily` 第一次遇 MySQL 1213 deadlock，第二次為 `JSONDecodeError`，0/0/0，造成 parent verify 失敗並使持股通知上游失敗。這次 run 早於下午新增的 8 個 EPS 來源，不能作為新 EPS feed 的測試；截至此查，DB catalog 尚未 bootstrap 新 EPS/TAIFEX 定義與其 source_records。待 10/04 04:00 自然 daily run 驗證 21 個 daily definitions 的實際 mapped 數、有效期間、接受/隔離列和上市/上櫃覆蓋。
- 10/03 08:00 台北週末 source-extension run：TWSE 公司事件 70/70、TWSE 月營收 1,086/1,086、TWSE 財測差異 7/7；TPEx 公司事件與月營收兩次均 HTTP 530、0/0/0。觸發的 16:02 event-only scanner 是 `ready/partial`、10 個公告候選、行情/法人/營收/財務母體皆 0；不得當成新的全市場價格掃描，也不得將 TPEx outage 解讀成沒有事件/營收。
- 10/03 16:30 台北新聞收集 run 18:38:48 完成。12 個設定 crawler 中，網址表只有 10 個來源、1,212 個網址；TVBS 記錄 `ChunkedEncodingError` 並略過，三立新聞沒有網址且舊 log 沒記空回應狀態。`news_temp` 有 1,146 篇有效文章、9 個來源：公視新聞 43 個網址最後 0 篇入庫，自由時報 99→79、鉅亨網 150→147。任務仍成功，DAG 綠燈沒有說明來源完整度；公視剔除階段仍未知。`iThome` 與 TNL 在該批最大 `created_at` 都是 10/03 00:00，只有日期精度；不可視為盤中發布時刻。
- 已在 legacy `news_ch/all_news.py` 加入只讀批次 coverage log：列出全部設定來源（包含零網址來源），輸出 URL、解析列、有效時間列、最終接受列與狀態，不改抓取、篩選或訊號方向。下一次自然新聞收集看 `source_coverage` log，可區分三立/TVBS 網址階段缺口與公視/自由/鉅亨文章解析或篩選落差。新聞仍只作具來源、時間精度標記的個股背景，不投方向票。
- 本機、Airflow scheduler 與 worker 容器中的四個 Python 模組 `all_news.py`、`adapters.py`、`registry.py`、`market_signals.py` 均通過 AST 解析；worker/scheduler 也確認讀到列出全部設定 crawler 的新 coverage 程式。`git diff --check` 未報空白錯誤（僅有既存 LF/CRLF 提示）。沒有跑測試、沒有手動觸發 DAG、重送 Discord 或交易。後續自然驗收：10/04 daily EPS feeds、下一次自然新聞批次的 per-source coverage、10/05 平日 after-close 的 TAIFEX/TWSE/TPEx 行情與 digest 實際回執。
- 18:58 台北再次唯讀查 Airflow；最新新聞自然 run 仍為 16:30 logical run、18:38:48 完成，coverage 程式更新後尚無新 run，因此新 log 仍待下一次自然收集 runtime 驗證。

## 2026-10-03 18:08 台北：自動化執行檢查（以批次來源與通知管線為主）

- 工作方向依使用者更正：優先改善可排程、可批次抓取的市場級來源、有效日/接受率/市場覆蓋、訊號快照、通知收件與 outcome 校準；不新增逐家公司網搜或券商逐篇評比。歷史研究資料保留為上下文，不代替可重複的來源管線。
- 最新 daily 市場快照仍是 10/02：ready/partial、母體 1,946、價格 1,938、法人流 1,891、營收 1,945、季財務 1,902、候選 50。10/03 16:02 的週末 event-only run 是 ready/partial、10 個事件候選，但價格/法人/營收/財務母體均為 0，不視為新日線市場掃描。
- 10/02 20:00 after-close parent failed at verify_collection_and_triggers。TWSE 個股行情 1,051/1,051 接受、0 隔離，最大有效日 10/01；上市普通股主檔 1,054，少 3 檔。TPEx 行情 map 9 兩次因 ChunkedEncodingError 失敗，0/0/0；892 檔上櫃普通股因此沒有這批個股行情。單一 mapped task 成功不等於雙市場完整。
- after-close 觸發的 10/02 daily child 有 50 個候選，但 Discord digest 3 次後收到 API 40005 Request entity too large；訊號 digest 0 筆成功收件，另有失敗通知成功記錄。失敗 ledger 的訊息為 13,624 UTF-8 bytes。現行程式已設每個 request 最多 10 個 embeds 並限制序列化 JSON 10,000 bytes，但修改晚於這次 runtime；等 10/05 自然收盤掃描驗證，不重送舊通知、不手動觸發。
- 同輪 TWSE quarterly_forecast_attainment 成功抓 8 列但 accepted 0 / quarantined 8，涵蓋 6 個代碼，effective period 2026-06-30。保存 payload 的 7 個必填欄皆有值、event/effective date 均存在；在目前 worker 以現行 adapter 重解析同一批 payload 得到 8/8 ok、必填欄缺值 0。歷史 run log 沒有隔離原因，故確切舊版 parser/contract 原因仍未知；下次自然收盤批次再核驗，不先改語意或計分。
- 10/03 週末 source-extension run scheduled__2026-09-27 failed at verification：TPEx company-event 與 monthly-revenue endpoint 回 HTTP 530、各 0/0/0；TWSE 同批來源成功（company events 70/70、monthly revenue 1,086/1,086）。這是收集服務可用性差異，不能當成上櫃無事件/無營收。
- 最新新聞收集 logical 10/03 14:30、實際 16:30–16:40 台北成功；新聞解析表 1,071 列/9 個來源，URL 階段 10 個來源；程式設定 12 個 crawler。一次鉅亨網文章因 ChunkedEncodingError 略過；來源覆蓋在 URL 與解析兩階段不同。iThome/TNL 的 created_at 只有日期精度並落在 00:00，不能作盤中即時新鮮度；其他來源時間亦是 crawler 存檔欄位，未逐篇核原頁時不等於已驗證發稿時刻。
- market_signal_outcomes 仍只有 2026-09-24 的 81 筆 signal-close 與 81 筆 next-close 一交易日觀察，尚無成熟 5/10 交易日或成本調整樣本；不調門檻。新增 8 個 TWSE/TPEx 官方季損益 EPS 批次端點仍待 10/04 起自然 daily collection 核對有效期間、接受/隔離列與上市/上櫃主檔覆蓋。
- 本輪僅唯讀查 DB/Airflow 與本機 log；未觸發 DAG、未重送 Discord、未交易、未跑測試，亦未改訊號程式。

## 2026-10-03 17:44 台北：工作方向改為全市場批次訊號自動化

- 使用者明確更正：目標是可自動抓取、可重複運作的市場級訊號管線，不是逐家公司上網研究、人工評比後彙總。後續以 source registry、批次 API、資料品質門檻、point-in-time 組合和 outcomes 驗證為主；單家公司只可作程式驗證樣本，不另寫人工個股研究摘要。
- 目前資料庫唯讀盤點：33 個核准啟用來源，schedule 為 daily 13、daily_after_close 13、intraday 5、monthly 2。最新 10/03 08:02 weekend run 是 `ready/partial`、行情/法人/營收/季財務母體皆 0、公司事件 10 筆；內部 `notified_count=10` 不是 Discord 收件回執，也不是行情掃描。最新日線仍為 10/02 `ready/partial`，universe 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、候選 50、通知欄 0。
- 最新 10/02 20:00 台北 after-close Airflow parent run 仍為 `failed`。TWSE 個股報價 1,051/1,051 接受、0 隔離、有效日 10/01；對 1,054 檔上市普通股差 3。TPEx 報價兩次失敗，0/0/0；892 檔上櫃普通股沒有該批行情。成功 task 不能代替有效日和市場覆蓋檢查。啟用來源中仍有 3 個 `last_success_at=NULL`：TPEx 報價、注意交易與處置資料。
- `market_signal_outcomes` 仍只有 9/24 訊號日，signal-close/next-close 各 81 列（方向命中各 50/37，均各有 1 列無方向）；沒有成熟 5/10-session 窗，不能據此調門檻。新聞 DAG 最新一批 10/03 logical 14:30 台北、實際 16:30–16:40 成功；該次文章列數與來源覆蓋尚未在本輪即時重查，不以 DAG 綠燈代替來源品質核驗。
- **新增可批次官方財務來源。** TWSE OpenAPI 六個產業別綜合損益表及 TPEx OpenAPI 兩個產業別端點，本輪從官方端點無認證 GET 均回 HTTP 200；資料出表日 `1151003`、年度/季別 `115Q2`，八個端點均有基本EPS欄。原始列數加總 TWSE 1,084、TPEx 891，這是不同產業端點的全市場原始代碼量，尚非經證券主檔配對後的接受覆蓋數。[TWSE OpenAPI 規格](https://openapi.twse.com.tw/)、[TPEx 季報資料說明](https://www.tpex.org.tw/web/regular_emerging/statistics/financial/list.php?l=zh-tw)
- 已在 source registry/adapter 加入 8 個官方全市場季損益來源，安排既有 `daily` 自然批次收集，來源鍵留在 TWSE/TPEx allowlist。daily map 來源數由 13 預計增為 21，仍低於每輪 32 個 mapped-source 上限；資料庫尚未 bootstrap 新定義，需等自然日批次確認 effective date、raw/accepted/rejected、EPS 缺值和上市/上櫃普通股匹配覆蓋。
- 候選卡只讀最新 180 日官方財報列報 EPS；負 EPS 與現有正向方向家族同時出現時，增加反向風險文字，不新增方向家族、不改分數或門檻。現行全市場 API 回應只含最新季別，沒有同季去年值，所以明確標「未作年對年比較」；不得把單一期 EPS 當作 EPS 成長 signal。所有查詢仍依掃描時間截斷，避免前視。
- 三個修改模組 `adapters.py`、`registry.py`、`market_signals.py` 靜態 AST 解析成功；沒有跑測試、沒有手動觸發 DAG、沒有送 Discord 或交易。首輪 runtime 驗證等下一次自然 `daily` 收集與 10/05 平日 after-close；只有拿到歷史可比期、穩定覆蓋和成熟 outcomes 後，才評估是否升級為方向訊號。
- 既有 heartbeat 已更新為「台股自動訊號與來源品質監控」，狀態仍為 ACTIVE、平日 20:30 排程不變；後續 prompt 明確禁止逐家公司人工研究評比，改以可授權的全市場批次來源、自然收集驗證與 outcome 成熟度為主。

## 現行資料與掃描狀態

- 2026-10-04 07:34 台北即時盤點：資料庫 registry 有 47 個 approved/enabled 來源（daily 21、daily_after_close 15、intraday 9、monthly 2），9 個尚無 `last_success_at`，含兩個 TAIFEX 市場倉位、TPEx 個股行情/監理公告，以及上市櫃內部人轉讓批次。10/04 04:00 natural daily source run 的 21 個 mapped source 最終全成功，共有 23 次 attempt（季獲利分析與 TWSE 股利各在第 2 次成功）；上市/上櫃季損益及資產負債資料期間皆為 2026-06-30，四類財務來源聯集匹配普通股主檔 TWSE 1,053/1,054、TPEx 891/892。TWSE 季獲利分析為 1,054 raw/1,049 accepted/5 quarantined；TPEx 為 884/884/0。舊 `tpex_dividends` 仍回傳 2,483/2,483 accepted，但最晚事件日僅 2021-08-03；新版 MOPS CSV contract 尚待自然 schema bootstrap 後驗收。券商原始研究目前沒有已授權、可排程的批次 feed/source registry 列，記為來源缺口；不以逐家公司人工搜尋代替。`market_signals` 使用官方公告、月營收、日線、法人流、季財務與持股資料，新聞自動匹配作背景。最新 daily signal snapshot 仍是 10/02 `ready/partial`、50 候選；outcomes 只有 9/24 的 horizon-1 signal-close/next-close 結果，無成熟 5/10-session 樣本。
- 2026-10-02 17:27 台北時間唯讀驗證：17:15 logical 盤中 child run 於 17:15:15 成功；`market_signal_runs` 為 `ready/fresh`、`as_of_date=10/2`，候選與通知皆 0。這是盤中 mapped-source 新鮮度，不是日線收盤完成。自然 `stock_collect_everyday_process` 仍在跑：法人流 10/2 已有 24,797 列、TPEx 收盤行情 task 已完成；正式 `個股成交資訊` 現有 888 列且全為上櫃，TWSE 行情與當沖 task 在 17:23 仍 `running`。四個新來源（TWSE/TPEx 日線報價、注意交易、處置股票）均為 approved/enabled，但 `last_success_at` 仍 NULL。每日訊號最新成功樣本仍是 9/24（81 候選），9/25 run failed；`market_signal_outcomes` 仍為 0。after-close DAG 最新仍是 10/1 20:00 failed，10/2 20:00 自然 run 尚未開始；不得以盤中 `fresh` 推定日線、TWSE 行情或新來源已完成。
- 2026-10-02 17:39 台北時間進度/來源契約檢查：TWSE 上市行情 task 的 live log 到 17:34:50 仍逐檔寫入 diff，已到代碼 6541，確認還在推進。當沖 task 在 16:57、17:14、17:31 的自然重試皆因三個成交量/買賣金額欄位缺失而報 `KeyError`；當時保留的官方 CSV 只有代碼、名稱和當沖資格註記。`StockDayTradeCollectProcess` 本身註明要等晚間資料並每 15 分鐘重試；TWSE 網路商店的 TWTB5U 日沖統計檔約 20:00 產製，且 T 日資料可修訂至 T+2，內用價 NT$1,500/月；免費資料集 11661 只有當沖資格欄位，沒有成交量值。因此 17:31 的欄位缺失與資料尚未發布相符，但還不能證明目前抓的 TWTB4U 端點在 20:00 後會提供相同統計欄位；等自然重試實際回應再判斷，不手動重試、不改程式，也不把資格註記當成成交量信號。`market_signals` 目前沒有讀取當沖表，故它尚未進入候選評分。[TWSE 日沖統計商品與更新說明](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)、[免費上市當沖資格資料集](https://data.gov.tw/dataset/11661)。
- 2026-10-02 17:49 台北時間再查同一個自然 run：17:47:44 重試仍報相同三個成交量欄位 `KeyError`；`day_trader.csv` 更新時間 17:47:44，標頭仍只有證券代號、名稱與當沖資格。TWSE 上市行情 task 在 17:48:50 已推進至代碼 9925，task 仍 `running`。因此行情採集確有進度，當沖統計則仍待官方晚間資料/端點契約核實；未手動觸發或改程式。
- 2026-10-02 17:51 台北時間後續唯讀驗證：legacy TWSE 行情 task 於 17:51:25 標為 `success`，但最後重試仍有 1589、2323、2601 的 `KeyError: 'fields'`；collector 在代碼第三次失敗後移出重試清單，仍保存其餘結果。DB 的 10/2 快照為 1,938 個唯一代碼，與 10/1 的 1,938 相同；主檔上市 1,054、上櫃 892，10/2 有行情上市 1,050、上櫃 888。8 檔缺列是上市 1589/2323/2601/3311、上櫃 3085/4527/4804/5301；3311 雖記錄「已寫入 diff」仍無最終日列。這 8 檔在現有價格表的最後資料日分別為：1589=4/2、4804=4/13、2601=9/22、4527/5301=9/29、2323/3085=9/30、3311=10/1；缺列不全由當日重試錯誤造成，還需區分停牌/無交易、仍在主檔的舊證券和 collector 漏取。每日掃描只把當日有價的股票列為候選；其 `source_status` 可在行情覆蓋達主檔 70% 時仍為 `fresh`，但 Discord 卡會顯示實際行情分子/分母與百分比，並說明 fresh 不代表 100% 覆蓋。需在 20:00 後將核准 OpenAPI 的 TWSE/TPEx 行情來源逐檔對照這 8 檔；不先改 70% 門檻或方向分數。舊 DAG 及當沖 task 仍 `running`，不手動觸發。
- 2026-10-02 18:05 台北時間檢查前一個自然 after-close run（10/1 20:00）：`twse_market_index_daily` HTTP 200、20/20 列接受且無 reject，但最新有效日期停在 9/30；同一輪 `tpex_market_index_daily` 只回 1 列、有效日期 10/1。故 mapped task 的 `success` 不代表 TWSE 基準含當日收盤。daily scanner 要求 TAIEX 與 TPEX 各有連續 6 個交易日且最後日等於 `as_of_date` 才算 benchmark complete；若 10/2 20:00 的自然收集仍只有 T-1 TAIEX，relative-price 家族不可用、`market_signal_runs.source_status` 應為 `partial`，雖然其他資料家族仍可能產生候選。待本日自然收盤 run 核對 feed 是否補上 10/1/10/2 及 summary 狀態；不手動呼叫來源或觸發 DAG。
- 2026-10-02 18:04 台北時間當沖自然 retry：同一個 `stock_day_trade_collect` 再次缺少成交股數與買/賣金額三欄而 `KeyError`，`day_trader.csv` 仍只含證券代碼、名稱與資格註記；10/2 DB 資料仍未更新。worker task 仍存活並每 15 分鐘重試。TWTB5U 是不同日沖統計產品，不能由資格檔欄位推回成交額/股數；持續等本日 20:00 後的自然 response 和正式來源口徑，不將 eligibility 當量能訊號。
- 2026-10-02 18:21 台北時間唯讀回查：自然 legacy run `scheduled__2026-10-01T08:10:00+00:00` 仍為 `running`；TWSE 日行情任務已於 17:51:25 成功，但 TWSE 券商進出任務仍 `running`（17:51:27 起），`stock_day_trade_collect` 仍 `running`（16:57:51 起）。最新 task log 在 18:21:05 再次缺少「當日沖銷交易成交股數／買進成交金額／賣出成交金額」三欄並進入 15 分鐘 sleep/retry；`day_trader.csv` 同秒更新，標頭仍只有證券代號、名稱與資格註記。故資格名單沒有轉成成交量資料，不能把欄位空缺當成零量；20:00 以前仍符合晚間統計尚未發布的可能性，但若晚間 retry 仍相同，需將目前 TWTB4U 介面視為 schema/產品不符待核，而不是無限延長等待。10/2 20:00 after-close DAG 尚未到排程時間；不手動觸發舊任務或 notifier。
- 2026-10-02 18:26 台北時間官方欄位契約交叉核對：TWSE Data E-Shop 把 TWTB4U「可現股當沖標的證券主檔」定義為代碼/指數成分股/先買後賣與先賣後買資格旗標，產製時間 22:30；另一個 TWTB5U「當日沖銷交易標的及統計」產品才明列成交股數、買賣成交金額，約 20:00 產生且 T 日資料可修訂至 T+2。[TWTB4U 主檔規格](https://eshop.twse.com.tw/zh/product/detail/793e7c3c28e04a7c9c9675583c624f03)、[TWTB5U 統計規格](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)。但 TWSE OpenAPI Swagger 仍把 `/exchangeReport/TWTB4U` 描述為「標的及統計」；故 E-Shop 商品檔與 OpenAPI endpoint 命名/欄位說明存在不一致。當前 collector 請求 TWTB4U 卻選取三個統計欄；18:21 回傳只有資格欄位與其中一種來源定義一致，仍須等晚間自然 response 或正式欄位文件釐清，暫不斷言 OpenAPI endpoint 一定錯，也不購買付費資料或修改程式。[TWSE OpenAPI Swagger](https://openapi.twse.com.tw/)
- 截至 2026-10-02 15:32 台北時間，最新 15:15 logical intraday collection 與 child scan 已於 15:30:15 完成，`ready/fresh`、候選 0；15:00 logical run 於 15:15:12 完成，候選 0。Airflow 今日盤中排程截至 15:15 logical run 均為 success。兩輪皆走正常路徑，沒有觸發新設的 2 分鐘 retry。14:00 logical run 候選 2；12:00、12:15 各有 5 候選，12:30 至 13:45 logical runs 均為 0。`sent` 代表 notifier 呼叫成功，不是外部平台回執或預測有效證據。日線仍只有 1 次 ready（資料日 2026-09-24、81 檔候選）；9/25 日線 run failed，之後尚無日線 run。候選表共 1,331 筆，混有盤中公告候選，不能視為日線回測樣本。`market_signal_outcomes` 在 20:00 close 前仍是 0 rows；首批結算結果待自然收盤 DAG 驗證。
- 2026-10-02 15:46 台北時間唯讀驗證：Airflow 15:30 logical intraday run（`scheduled__2026-10-02T07:30:00+00:00`）於 15:45:12 完成 `success`；最新 `market_signal_runs` 為 `ready/fresh`、候選 0、通知 0。兩個新個股報價來源都已 `approved/enabled`，但 `last_success_at` 仍為 NULL，且目前完全沒有 `source_runs`；`market_signal_outcomes` 仍為 0 rows。after-close DAG 目前最新仍是 10/1 logical run failed，10/2 20:00 自然 run 尚未到時。故新來源和 daily-run 修復尚未 runtime 驗證；未手動觸發 DAG 或 notifier，也未跑測試。
- 2026-10-02 11:40 台北時間新增兩個 TWSE 收盤監理背景來源：當日注意交易資訊與處置股票。程式已將它們加入 OpenAPI allowlist/registry；應用 DB 已見 29 個核准啟用來源，但截至 14:15 這兩個來源都沒有 `source_runs` 紀錄。因此註冊完成，mapped ingestion、sentinel 排除與有效期間判斷仍待 10/2 20:00 自然收盤 DAG 驗證。它們只作日線風險背景，不加入方向家族、候選門檻、分數或排序。未手動觸發，也未跑測試。
- 截至 2026-10-02 14:49 台北時間回查近 7 日 `source_runs`：1,487 次嘗試中 60 次首試失敗（55 次 MySQL 1213 deadlock、5 次 `ChunkedEncodingError`），60 次均有同來源/同 run/map index 的後續成功 retry。最新保留的 InnoDB deadlock 在 14:30 台北時間，`twse_quarterly_forecast_variance` 與 `tpex_monthly_revenue` 兩個不同來源同時 `INSERT INTO source_records`，都在同一個 `PRIMARY` 頁尾 supremum 等待 insert-intention X lock；此表 clustered primary key 是自增 `id`。這支持共享 append 寫入路徑有競爭，但單一死鎖快照不足以證明全部 55 次的共同根因。已將單次 retry 間隔從 10 分鐘降為 2 分鐘，重試次數維持 1；14:45 與 15:00 自然 run 都沒有 retry，因此縮短 backoff 的 runtime 效果仍待首次自然重試驗證。先保留現狀，不依此單一快照調降併發或改資料表。來源均已恢復，未見未恢復來源。
- 收盤收集 DAG 最近 3 個台北時間收盤排程（9/29、9/30、10/1 20:00）均在驗證階段 failed；Airflow 的 UTC logical dates 是 9/28–9/30 12:00Z。特別是 `scheduled__2026-09-30T12:00:00+00:00` 對應台北 10/1 20:00，不是漏掉 10/1 的 run。該輪來源 schema、discovery 與 6 個 mapped ingestion 都成功；`trigger_market_signal_notify` 因舊 trigger run ID `market_signal__scheduled__2026-09-30T12:00:00+00:00` 已存在而被 `skip_when_already_exists` 跳過，verify log 明確報 `DagRunAlreadyExists`／trigger `skipped`，所以當輪沒有 daily snapshot。現行程式已採 `market_signal__daily__{dag_run.run_id}` 並把子 run logical date 加 1 秒；Airflow 對既有 scheduled parent run 的 `tasks render` 確認產生 daily 專屬 ID。10/2 20:00 台北時間的真實收盤 run 尚待執行，runtime 修復未確認。

- 2026-10-02 16:15:28 台北時間唯讀檢查：當日自然排程 `stock_collect_everyday_process`（run `scheduled__2026-10-01T08:10:00+00:00`）已啟動；代碼主檔與備份任務成功，`stock_three_main_investor_collect` 第 1 次嘗試於 16:12:21 失敗並進入 `up_for_retry`。TWSE T86 請求後，`dags/stock/collect_logic.py:607` 在 `a['fields']` 發生 `KeyError: 'fields'`。`process()` 先 `collect()` 再 `save()`，所以此錯誤發生於寫入前；唯讀 DB 查詢確認 `三大法人買賣` 的 10/2 rows=0、最近資料日 10/1，當日 `個股成交資訊` rows=0。任務設定 2 次重試、間隔 15 分鐘；Airflow 自動重試仍待觀察。日誌沒有保存 HTTP status 或 JSON body，不能判斷是暫時錯誤、回應格式變動、空資料還是存取問題；不能把它歸因為先前 MySQL deadlock。

  這會影響 20:00 訊號驗證：收盤掃描用 `個股成交資訊` 最大日期決定 `as_of_date`；法人流在近 45 日取數，但只要最新資料距 `as_of_date` 不超過 7 日便視為可用，再以近 7 日最多 5 列且至少 4 個有效觀測日形成方向家族。因此 10/1 的流量仍可進入 10/2 掃描；通知文字只顯示有效觀測日數，不顯示最新法人資料日。after-close source-extension DAG 只等待自身核准來源 ingestion，沒有等待 legacy stock DAG；若 20:00 前 legacy retry 未成功，必須直接核對 daily scan 的 `as_of_date` 和個股價格/法人日期，不可把 `ready/fresh` 當成今日資料完整。現階段不改方向分數；若確認出現日期錯置，優先讓摘要明示最新法人資料日/落後日數並阻止過期收盤價冒充當日快照。無手動觸發或重跑。

- 2026-10-02 16:30:31 台北時間後續驗證：同一 DAG 的自動 attempt 2 於 16:27:53 成功；log 記錄 TWSE T86 18,607 列、TPEx 6,190 列，合併後 `insert=24,797`。唯讀 DB 確認 `三大法人買賣` 已有 24,797 筆 10/2 rows（`create_time` 約 16:27:50）。這證明既有重試機制本輪恢復，但 attempt 1 的 JSON/status 未保存，遠端原因仍未確定；暫不改 collector。下游 `stock_twse_all_code_daily_deal_info_collect`、`stock_tpex_all_code_daily_deal_info_collect` 於約 16:27:55 啟動；16:30:31 仍為 `running`，且 `個股成交資訊` 的 10/2 rows=0。上一個成功的 legacy run（logical 9/30 08:10Z）中，TWSE 行情於 10/1 18:02、TPEx 行情於 17:15 台北時間完成，均早於 20:00；今日下游任務約早 17 分鐘啟動，預期可在 20:00 前完成，但在 DB 出現當日行情列前不得視為已完成。after-close DAG 截至 16:30 尚無 10/2 run；仍只觀察自然執行。

- 2026-10-02 17:19 台北時間唯讀檢查自然 legacy run `scheduled__2026-10-01T08:10:00+00:00`：三大法人任務已成功，TPEx 個股收盤價於 16:57:48 完成（`insert=1,775`、`delete=887`）；TWSE 收盤價仍 `running`，17:19:17 live log 已處理至代碼 4119；`stock_day_trade_collect` 自 16:57:51 起仍 `running`。整個 DAG 仍 `running`。after-close DAG 最新仍是 10/1 20:00 對應的失敗 run；10/2 20:00 尚未排程。不得把 TPEx 完成等同全市場完成，也不手動觸發。

- 2026-10-02 18:40 台北時間唯讀追查自然 `source_extension_collect_after_close`：9/25、9/28、9/29、9/30 run 均 failed，但 9/28–9/30 六個 mapped source ingestion 全成功。9/28–9/30 的 `trigger_market_signal_notify` 因 `skip_when_already_exists=True` 遇到既有 DAG run 而標記 `skipped`；`verify_collection_task()`（`dags/source_extension/dag.py`）把非 `success` 一律列為 `failed_triggers`，所以父 DAG 失敗。9/30 同時段既有 `market_signal_notify` run 在 20:15 台北時間成功，支持這幾次是重複觸發造成父流程假失敗，而非來源 ingestion 失敗。已在 verifier 將 `skipped` 視為可接受的冪等結果；未跑測試，等待 10/2 20:00 自然排程 runtime 驗證。9/25 是另一種失敗：Airflow `DagRun.find_duplicate(...).one_or_none()` 遇到 `MultipleResultsFound`；不清理/刪除 metadata，後續獨立調查是否再現。範圍搜尋確認同一 verifier 同時檢查 source-extension 的持倉通知與市場掃描兩個 skip-on-existing trigger；兩者都適用同一冪等語意。10/2 after-close run 尚未到 20:00 排程，不手動觸發。

- 2026-10-02 18:52 台北時間 legacy 收盤採集仍未結束：`stock_collect_everyday_process` 的 10/1 logical run 還在 running；上市券商進出任務自 17:51 起未結束，`stock_day_trade_collect` 自 16:57 起 running。當沖最新 log 18:37:46 仍因 TWTB4U 回應缺少成交股數及買/賣成交金額三欄而 `KeyError`，`day_trader.csv` 最新資料仍只有代碼/名稱/資格旗標。10/2 20:00 自然 after-close run 尚未到期；等自然重試和官方來源，不手動觸發、不把欄位缺失當零量。
- 2026-10-02 19:00 台北時間唯讀回查：Airflow CLI 顯示同一 legacy logical run 仍為 `running`，`stock_broker_inout_twse_collect` 與 `stock_day_trade_collect` 兩項仍在跑；當沖 task log 最新自然 retry 是 18:54，仍報三個統計欄位不在 TWTB4U 回應 schema。磁碟 CSV 標頭仍只有證券代號、名稱與「暫停現股賣出後現款買進當沖註記」，與官方 TWTB4U 主檔欄位契約一致，不是統計量為零；不要讓資格旗標替代成交股數/金額。10/2 20:00 after-close run 當時尚未排程。前一個 after-close run 的 6 個 mapped ingestion 都成功，但 `trigger_market_signal_notify` 是 skip、verifier failed；已修正後需等今日自然排程確認是否解決。未手動重跑或觸發。

2026-10-02 19:22 台北時間：Airflow 唯讀狀態確認 `stock_collect_everyday_process` 的 10/1 logical run 仍在執行；TWSE 券商進出首輪已完成，第二輪與 `stock_day_trade_collect` 仍為 running。當沖 worker 最新一次失敗 log 是 19:11:07，仍缺成交股數及買/賣金額三欄；此 task 目前 attempt 1、每次失敗後等 15 分鐘，最多內部重試 24 次。重新核對 TWSE 官方商品契約：TWTB4U 是次日當沖資格主檔，僅證券代號及資格/成分股註記、22:30 產製；TWTB5U 才包含市場與個股當沖量值，約 20:00 產製，T+2 才是最終數據，內用 NT$1,500/月、外用 NT$3,000/月且外用限制轉傳給資訊用戶。legacy collector 現正讀 TWTB4U 卻選取 TWTB5U 才有的欄位，屬來源產品/欄位契約不符；`market_signals` 未讀取當沖表，故不把它當訊號或零成交。尚待 20:00 後同一自然 run 的回應核實 OpenAPI 描述與 E-Shop 商品文件差異；未訂購、未改碼、未手動觸發。[TWSE TWTB4U 主檔](https://eshop.twse.com.tw/zh/product/detail/793e7c3c28e04a7c9c9675583c624f03)、[TWSE TWTB5U 統計](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)

## 一次性日線候選結算

以唯一一輪 2026-09-24 日線候選為訊號日，計算該日收盤至未來收盤的未調整股利報酬，再扣除所屬市場指數同期報酬：

| 後續交易日 | 候選數 | 候選相對市場報酬中位數 | 全市場中位數 | 候選超額中位差 | 相對方向命中率 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-09-29（第 1 日） | 81 | +1.05 個百分點 | +0.38 個百分點 | +0.67 個百分點 | 62.5% |
| 2026-09-30（第 2 日） | 81 | +1.01 個百分點 | +0.30 個百分點 | +0.71 個百分點 | 55.0% |
| 2026-10-01（第 3 日） | 34 | -0.25 個百分點 | -0.97 個百分點 | +0.72 個百分點 | 48.5% |

此結果只是單一訊號日、未扣交易成本、未調整股利或公司行動。基準股票清單取自目前 stock master，並非訊號日 point-in-time 清單。第 3 日只剩 TPEx 樣本：TWSE 加權指數最新有效日停在 9/30，雖然 10/1 的 TWSE source run 回報 success 且 accepted 20 筆；TPEx 指數已到 10/1。市場休市表確認 9/25–9/28 休市，所以 9/29 是 9/24 後第一個交易日。單一樣本不足以判定訊號有效或失效。

已新增 `market_signal_outcomes` 追蹤表與日線結算流程，採 1、5、10 個交易日持有窗並以所屬市場指數扣除大盤變動。`signal_close` 是事後描述值，因日線通知在收盤後發送，不可當成可成交績效；`next_close` 從訊號日後第一個交易日收盤進場，再持有指定交易日數，作為保守可執行代理。交易日按 stock 價格日期與合格指數日期聯集，並記錄候選快照的 `source_status`，以便日後分層檢視 partial 樣本。尚未成熟的視窗不寫入；已成熟但缺個股或基準收盤資料者先記 `pending_data`，日後日線掃描會補算。報酬使用未調整收盤價、未扣成本，`directional_hit` 僅表示超額報酬方向是否符合訊號方向。schema migration 7 已建立 `source_status` 欄；截至 2026-10-02 14:03，outcome 表仍為 0 rows。靜態程式檢視確認 `capture_market_signal_snapshot` 只在 daily branch 呼叫 `_settle_daily_candidate_outcomes()`，結算選取近 120 日 ready 日線候選；因此 9/24 候選預期會納入，但實際 UPSERT 與成熟視窗數量待 10/2 20:00 自然收盤 run 驗證。

## 盤中公告品質抽查

- 2026-10-02 07:00 的最新非空盤中快照有 10 筆候選；通知 ledger 對該 run 記錄 10 筆 `sent`。此為程式呼叫通知成功的內部證據，不代表市場後續反應或訊號有效。
- 截至 2026-10-02 14:15，盤中 `ready/fresh` 新增 2 個 TPEx 官方公告候選：4744 是既有董事異動公告的新任者履歷更正；1259 是委外平台疑似個資遭第三方存取，公司稱截至公告時無重大財務/營運影響。兩者均在 10/1 公告，10/2 14:15 才首次入庫，分別晚約 24 小時 13 分、20 小時 34 分。延遲格式用 `source_records.created_at`（首次入庫、UTC）對 `event_time`（公告時間、台北）換算；`observed_at` 每輪會更新，不能當 first-seen。後續公司事件通知已加入公告時間與首次入庫延遲（超過 1 小時才顯示）；4744 現分類為 `review`、標籤「董事履歷更正｜既有事件」，不再當作新董事異動推播，仍保留在官方 source record。1259 保留為資安風險背景；後續查受影響範圍、補救與通報進度，不把公司目前無重大影響的說法當作已驗證結果或獨立方向訊號。延遲欄位與新分類仍待後續自然公告驗證，沒有重送既有通知。
- 12:00 與 12:15 各抽查一批 5 張卡，內容是 2412 中華電、2515 中工、3036 文曄、3702 大聯大、4904 遠傳的 TWSE 公司財測差異事件。每張卡只有 `company_forecast_variance` 一個中性家族，不進多空分數；同一批的 5 筆通知列各保存相同整批 JSON，投遞程式則按 batch 呼叫一次 notifier，所以 5 列不是 5 次獨立送出。`t187ap16_L` 原始 JSON 沒有金額單位欄，舊卡又直接串接未加逗號的數字。已修改後續卡片：金額加千分位、區間改為「至」、清楚標示 API 未提供單位；不推算差異百分比或多空方向，不重送舊卡。修改尚未等到自然通知驗證。
- 2026-10-02 16:53 台北時間回查這兩輪的通知 ledger：12:00 與 12:15 各是一次五卡 batch，兩輪 serialized payload hash 完全相同，但五組 `candidate_hash` 與衍生 dedupe key 都不同；兩輪都各發生一次成功的 notifier 呼叫，ledger 記為 `sent`。12:15 的 hash 符合現行語意指紋，12:00 的 hash 與現行指紋不符；差異成因與切換時間未證實。現行指紋以來源、公司、財報期別、涵蓋期間與實績/財測值為識別，略過來源列 ID、財測序號及發布時間。12:30–14:00 後續輪次皆為 0 候選/0 通知，故證據只支持一次識別鍵差異造成的重複呼叫，不支持持續性重複。未做去重行為變更；只補註該語意指紋是持久 ledger 相容契約，避免無遷移地更改後重播舊訊號。`sent` 不構成 Discord 外部回執。
- 下列公告內容來自 [TWSE 官方重大訊息 API](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)，資料庫保留每筆公告時間、正文與來源網址供回查。
- 3016 的 TWSE 官方財務業務資訊列出最近一月營收年增 +58%、最近一季營收 +37%，最近一季稅前淨利 +532%、EPS +533%。此為值得追蹤的基本面事件，但月資料標示自結、季度資料為核閱數，巨大增幅仍須檢查基期與後續月營收，不能單獨當買進結論。
- 6957 同日有原始公告與 16:11 更正公告：更正後最近一月營收、稅前淨利、EPS 年減 31.15%、61.58%、62.67%，但最近一季三項仍年增 32.91%、124.41%、77.17%。舊通知把月與季百分比用斜線並列、方向標為轉弱，且同批把舊公告與更正各推一次。已修改判讀，讓月弱季強標為「訊號分歧」、卡片保留「最近一月/最近一季」期別，並在同批只留更新版；尚未有新更正事件驗證改版 runtime。
- 2024 搜索調查公告同時明載「營運正常，對本公司財務業務無重大影響」。舊分類因「搜索」一詞直接標偏空。現在保留為重要檢調事件，但呈現公司自述並標中性，等待調查進度或補充公告；未經新事件自然執行驗證。

## 新聞資料品質與整合

- `news_ch.news` 欄位有來源、標題、發稿時間、內文及文章 URL，但沒有 `created_at` 索引；禁止用全表 MAX/排序掃描來判 freshness。
- 2026-10-02 12:58 台北時間只讀主鍵最近 2,000 筆樣本：1,825 筆的 `created_at` 落在台北時間近 48 小時；該樣本的標題、時間、URL、內文欄位都非空，但鉅亨網有 117 筆標題實際值為 `...`、內文有文字，占該樣本近期鉅亨網 267 筆的 43.8%。因此原本只查標題會漏掉內容完整的公司報導。The News Lens 仍會重寫較舊文章，最新主鍵列可能有較舊發稿時間但 `updated_at` 已更新；資料庫寫入時間不能代替文章發稿時間。
- `news_ch.utils.clean_time_format` 已防空白時間解析崩潰，`all_news.collect_news` 會在正規化後移除無效時間列。修補後首輪自然執行於 2026-10-02 台北時間 08:30 開始，三個 Airflow 任務皆成功。`article_url` 有 922 筆，其中 402 筆發稿時間空白；`news_temp` 寫入 861 筆且 `created_at` 空值為 0，最新發稿時間到 10/2 08:30。這證明本輪含空白欄位時管線沒有再崩潰；尚未逐篇歸因 61 筆 URL 與 staging 的差額。
- 對同一個台北時間近 48 小時樣本，發現 39 組有效標題重複（其中 38 組跨來源）、額外 49 筆轉載；沒有相同文章 URL。`market_signals` 已更新日線候選新聞背景：仍只查主鍵最近 2,000 筆；以標題或內文首 320 字配對公司全名，兩字公司名另要求同一股票代碼在該文字區段附近。精確重複標題與 URL 只留一則；原標題為 `...` 時顯示標記為「內文摘錄」的首 140 字，否則標示命中標題或內文。每檔仍最多 2 則並保留日期、來源、HTTPS 連結。新聞仍不影響方向、家族數、分數或排序；讀取失敗不阻擋原掃描。這項程式改動尚未等到自然日線執行驗證；媒體內文仍屬二手資料，不能代替公司公告、財報原文或券商模型。
- 已把 TAIEX/TPEX 都具備截至行情日的六日基準窗加入日線新鮮度條件。只要一市場指數落後，掃描仍可產出候選，但收盤摘要會標 partial。

## 券商報告追蹤
研究快照更新至 2026-10-02 18:28 台北時間。中信投顧與國泰部分個股報告、KGI 10/2 Daily Navigator 點入會員登入；新光 10/2 盤前稿與永豐期貨 10/1 晨報可公開閱讀；另取得永豐 3/6 富世達原始報告並在下方加入假設/實績對照。新光、KGI、統一證券、國票及元大證券網頁上的 10/2 盤中稿已做交叉檢視：多數屬盤勢觀察或媒體摘要，仍缺期限/失效條件/統一資料批次，未升格為高品質交易訊號，詳見下方。中信 10/2 新增兩檔個股報告，但完整內文導至登入頁；本輪以公司公開營運資料建立可驗證項目，不將標題與目標價直接當訊號。另查凱基官方入口：10/2 台股市場報告題名和日期可確認，但點入會員登入；TWSE 收盤只核對指數數值，權值股歸因與訊號時點仍不可核實。玉山 10/1 公開第四季策略文章的宏觀數字已抽查官方原始來源，評為市場背景而非個股交易訊號；富邦投顧公開盤後快訊有族群觀點，個股報告內文仍受會員限制。另回查台北富邦銀行 6/10 AI PC 報告，部分產品事實有 NVIDIA 一手佐證，但測量口徑與獲利橋接不足，列歷史產業背景而非個股訊號。

2026-10-02 19:18 台北時間來源新鮮度回查：搜尋摘要將犇亞證券「每日分析」顯示為 2026-10-02，但打開證券公司頁面時，正文資料日期仍為 2026-09-24、報告標題為 23-Sep-26。搜尋摘要與目前可見原頁不一致，沒有可核對的 10/2 原始版本或固定日期文章；先標為來源日期未核實，不納入 10/2 券商報告或交易訊號。[犇亞證券每日分析頁](https://www.estockking.com/day.aspx)

| 券商 / 日期 | 公開可見線索 | 品質判讀狀態 |
| --- | --- | --- |
| 摩根大通，2026-10-02 台積電（2330）法說前展望（二手報導） | 媒體轉述台灣研究主管 Gokul Hariharan 維持「優於大盤」、TP 由 NT$3,200 升至 3,300；預估 3Q26 美元營收 US$45.8B（季增 14%）、毛利率 66.8%，並稱 Q4 營收季增 11%。未見 JP Morgan 原始報告。 | 台積電官方 3Q 指引為營收 US$44.6–45.8B、毛利率 65–67%、營業利益率 56–58%；JPM 被轉述的 3Q 營收正落在公司指引上緣、毛利率接近上緣。公司 8 月營收年增 53.3%，1–8 月年增 39.3%，支持需求強但不能驗證 3Q 預估。3Q 實績將在 10/15 公布，屆時按公司口徑評 forecast error；Q4 +11% 目前缺管理層指引。以 10/2 收盤 NT$2,500 計，TP NT$3,300 隱含約 32% 名目上檔，但未見目標期限、EPS bridge 或估值法；故「近季營運預估可量化、來源屬二手、TP 合理性不可評」，先建 scorecard，不作買進訊號。[二手報導](https://www.capitalfutures.com.tw/zh-tw/financial/globalarticle?category=all&contentid=c26100200380)、[TWSE 10/2 台積電收盤](https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20261002&stockNo=2330&response=json)、[台積電 3Q 官方指引](https://investor.tsmc.com/chinese/quarterly-results/2026/q3)、[台積電 8 月營收](https://pr.tsmc.com/chinese/news/3340)、[法說日期](https://investor.cld.tsmc.com/english/financial-calendar)。 |
| 凱基投顧，2026-10-02 台股盤勢報告 | 官網把「凱基台股分析：權值電子穩健推升、台股收盤歷史新高」列在晨會報告，日期可確認但沒有公開發布時分，點入原文導向會員登入。TWSE 10/1 收 48,353.49（+0.86%），10/2 收 48,475.74（+0.25%）；標題可能是在回顧 10/1 前收，不能把 10/2 收盤直接算成報告命中。另 10/2 上漲股票 483 檔、下跌 506 檔；臺灣50 -0.02%、半導體類 -0.18%，中型100 +1.00%、小型股300 +0.86%、電子零組件類 +2.21%，指數與市場廣度/產業表現不一致。 | 公開頁只有日期、類別、題名，沒有原文、具體觸發條件、期限或發布時間；因此「權值電子推升」的指涉交易日和成因仍不明，不評預測命中。10/2 的廣度/產業差異只列為可回查的中性市場背景，不升格方向訊號。[凱基官方研究入口](https://investment.kgisia.com.tw/Portal/Report/Index)、[TWSE 10/1 指數資料](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html&type=ALLBUT0999)、[TWSE 10/2 指數與市場廣度](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)。 |
| KGI，2026-08-21，台股盈餘展望（歷史背景） | KGI 新聞公告將 2026 整體盈餘成長率由 42% 上修至 50%、2027 年由 24% 至 26%；AI 族群 2026 年由 53% 至 60%、非 AI 由 17% 至 30%。目前公開的是摘要，完整模型/產業拆分與估值假設不可見。 | 官方公告清單確認這篇發布於 8/21，而不是 10/2 新報告；其引用的 2Q26 結果與宏觀觀點仍可追蹤，但不得混入最新晨報或重複計為新的訊號。非 AI 盈餘上修是值得用 Q3 報表驗證的假設，單篇摘要無範圍定義、底層公司清單、EPS bridge 或失效條件，暫作歷史情境，不作交易信號。[KGI 公告](https://www.kgi.com.tw/zh-tw/product-market/news-and-announcement/announcement/announcement-detail?id=48873c5829d34f5ca95f3613f5c99715)、[KGI 公告清單及日期](https://www.kgi.com.tw/zh-tw/product-market/news-and-announcement/announcement?category=press&p=2)。 |
| 新光證券，2026-10-02，台股盤前晨報 | 以隔夜半導體股上漲、台指期夜盤下跌作多空交錯；列台積電 2,500 元、加權 48,000 點條件，分別給出守穩延續高檔與同步失守轉區間的情境；對川普未具名「台灣晶片公司投資 5,000 億美元」聲明標示未確認，不直接認定為台積電。 | 期交所 10/1 日盤台指期最後成交 48,685、結算 48,698，夜盤收 48,475、較結算跌 223 點（-0.46%），與報告一致；報告以 10/1 現貨收盤計出的 331.5 / 121.5 點價差也可重算。台積電官網現列 Arizona 計畫總投資 US$265B（2026 年 7 月增加 US$100B 後），所以報告的 2,650 億美元不是誤植；對未具名 5,000 億美元說法的保留是合理的。[新光 10/2 盤前稿](https://www.skis.com.tw/Report/market/TSM20261002.html)、[期交所 10/1 日盤](https://www.taifex.com.tw/cht/3/futDailyMarketExcel?commodity_id=TX)、[期交所 10/1 夜盤](https://www.taifex.com.tw/enl/eng3/futDailyMarketReport)、[台積電 Arizona 官方說明](https://www.tsmc.com/static/abouttsmcaz/index.htm)。TWSE 10/1 官方股票家數為上漲 422 檔、下跌 551 檔，報告沒列這項廣度反證；加入後更能呈現指數創高但多數股票未同步。成交額 8,740.46 億對應 TWSE 總計(1–15)；一般股票為 8,147.94 億，評估量能門檻時需標清口徑。具備可核對資料與條件式風險控管，列為可檢驗的盤勢觀察；僅一日；10/2 收盤條件初步對照見下文，尚不評預測能力。[TWSE 10/1 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html)。 |
| KGI，2026-10-02，DAILY Navigator | 官方入口列出當日台灣市場策略報告，作者標示 Taiwan Research；點入正文要求會員登入。 | 只能確認日期、分類、研究團隊與報告存在，無法檢查數據、條件、風險或模型，不作品質評分。[KGI 官方報告入口](https://investment.kgisia.com.tw/Portal/Report/Index/En/R)。 |
| 永豐期貨顧問事業部，2026-10-01，台指期晨報（18 頁 PDF） | 封面列期交所、CMoney、DQII 為資料來源。第 2 頁列夜盤期權籌碼（單日交易淨部位）：外資大台 -788、小台 -2,200、微台 -6,979 口；依報告公式「大台 + 小台/4 + 微台/20」正規化為約 -1,687 口。 | 期交所 2026/10/01 夜盤查詢中的外資三項單日成交淨口數逐一相同，報告公式也可重算；來源可追溯性與數據透明度高。這是單夜成交流，不是未平倉部位，也不是方向預測。期交所提醒法人數字是多家機構互抵後的彙總，不代表某一機構或整類法人策略；報告沒有交易期限、支撐/壓力、確認或失效條件。PDF 通用免責亦稱不保證完整/精確且可能不更新。故列為「高品質觀察資料、預測能力未驗證」，暫作風險上下文而不單獨推送。[永豐 10/1 台指期晨報 PDF](https://www.spf.com.tw/upload/sinopac/researchContent/1a0f4c76cbc000002e7e.pdf)、[期交所 10/1 夜盤三大法人資料](https://www.taifex.com.tw/cht/3/futContractsDateAh?doQuery=1&queryDate=2026%2F10%2F01&queryType=1)。 |
| 國泰證期，2026-10-02，兆豐金 2886 | 「中立(+2.4%)」；標題稱非銀業務貢獻使獲利上修，但殖利率不具吸引力。官方歷史清單另有 5/28 兆豐金報告。 | 至少不是該部門首次公開兆豐金報告；但無法判斷是否同一分析師續更。原文需登入，無法核對 EPS 上修幅度、目標價前後差異、估值法或風險假設。標題表面理由自洽，仍不能評高品質。 |
| 國泰證期，2026-10-02，台股晨訊 | 公開正文回顧 10/1 大盤收 48,353.49、創收盤新高；選股偏向被動元件、ABF、AI 電子等，並提醒高檔震盪、避免全面追價。 | TWSE 官方三大法人資料可核對外資及陸資 9/30 買超約 NT$30.86B、10/1 買超約 NT$21.94B，至少支持「連日回補」最近兩個交易日；TWSE MI_INDEX 同日股票類別上漲 422 檔、下跌 551 檔（上漲/下跌比 0.77）；中央社同日報導則列 428 檔上漲、560 檔下跌，計數口徑略有差別，但兩者方向一致，都是下跌家數較多。指數創高但廣度偏弱，與外資買超同時出現，較合理解讀是指數強、廣度未確認的分歧，不足以判定廣泛風險偏好。這只證明兩日外資買超和單日 breadth，不能推論資金持續性或個股受惠；後續要看廣度是否擴大、外資流是否延續。族群建議沒有明確期限/估值條件，仍只當盤勢背景。[國泰公開晨訊](https://research.cathayfut.com.tw/FRRPortal/index.do)、[TWSE 9/30 法人金額](https://www.twse.com.tw/zh/trading/foreign/bfi82u.html)、[TWSE 10/1 法人金額 API](https://www.twse.com.tw/rwd/zh/fund/BFI82U?type=day&dayDate=20261001&response=json)、[TWSE 10/1 大盤分類統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html)、[中央社漲跌家數](https://www.cna.com.tw/news/afe/202610010164.aspx)。 |
| 國泰證期，2026-10-02，研華 2395 新聞評析 | 公開標題稱 Customer360「導入 27 國、商機成交率增 2 成」；10/1 新聞稱驗證 5,185 筆商機、成交率增 20%、業務準備時間由 30 分鐘降為 5 分鐘。 | 比泛 AI 故事多了實際採用與營運 KPI；但數值來自公司受訪說法，沒有對照組、期間、成交率分母定義、訂單金額或獲利貢獻。國泰類別是「新聞評析」，不是估值/財測報告；個別報告要登入。[國泰公開報告標題](https://research.cathayfut.com.tw/FRRPortal/index.do)、[自由財經原始採訪](https://ec.ltn.com.tw/article/breakingnews/5592204)。研華官方 2Q26 營收年增 46%、營業利益年增 64%、EPS 5.20 元可作公司基本面背景，但財報 7/31 已公告，早於此 Customer360 KPI，不能歸因於 AI 助手；追蹤後續月營收、毛利/營業利益率和 KPI 持續性。[研華 2Q26 官方結果](https://www.advantech.com/en-eu/resources/news/advantech-reports-2q26-eps-of-nt520?eventDate=19&eventType=physical)。 |
| 新光證券，2026-10-01，盤前晨報 | 盤前稿把條件寫成數值：台積電站回 2,500 元、加權指數站回 48,000；若開盤後台積電站回 2,500 且成交額維持 8,500 億以上，較有利挑戰前高；若台積電低於 2,450 且指數跌破 47,600，則減碼。另提 48,379.71 與 48,601.53 壓力、47,600–47,632 支撐。 | 10/1 台積電盤中高 2,510、收 2,510 元，加權收 48,353.49，價位條件達成；指數最低 47,893.42，未破 47,600。指數高點/收盤仍比 9/30 前高 48,379.71 低約 26 點，故「挑戰前高」接近但未突破。成交額門檻會因統計範圍翻轉：新光盤後稿列 8,388.73 億、中央社 8,374.31 億及 TWSE 一般股票 8,147.94 億低於 8,500 億；TWSE 全證券總計 8,740.46 億則高於，且 FMTQIK 與 MI_INDEX 兩端點一致。標記為「價位條件達成、量能口徑未定」，不計二元命中。此稿具時間、確認條件、失效條件與風險控管，比只有方向標題可檢驗；但目前只有單一交易日，且部分基本面引用自公司自述，不能推論預測能力。[新光盤前稿](https://www.skis.com.tw/Report/market/TSM20261001.html)、[TWSE 台積電 10/1 日線](https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20261001&stockNo=2330&response=json)、[TWSE 加權 10/1 區間](https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date=20261001&response=json)、[TWSE 10/1 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html)。 |
| 新光證券，2026-10-01，盤後檢視 | 同公司盤後稿記錄加權收 48,353.49、上漲 0.86%、收盤/盤中創高；成交額 8,388.73 億，低於其盤前稿設定的 8,500 億；另指出權值股支撐指數但個股漲跌家數分歧，提醒台積電 2,500 元支撐與題材股追高風險。 | 可用來檢查同一研究團隊是否回報條件：按其自身量值未達 8,500 億；TWSE 一般股票列也未達，但全證券總計已達。因來源未定義成交值範圍，不作二元命中；這是同一研究團隊盤後更新，不是獨立佐證。[新光 10/1 盤後稿](https://www.skis.com.tw/Report/market/TSM20261001_after.html)、[中央社 10/1 收盤與漲跌家數](https://www.cna.com.tw/news/afe/202610010164.aspx)。 |
| 凱基，2026-10-02，盤前/晨訊/台股分析 | 官網當日列表有盤前解析、投資早報及「權值電子穩健推升、台股收盤歷史新高」 | 個別報告點開要求會員登入。收盤新高標題描述前一交易日結果，屬回顧訊息；看不到盤前區間、量價條件或可驗證的方向預測，不能把標題當新訊號。 |
| 凱基，2026-10-01，台股晨會分析 | 完整正文公開於 KGI MoneySchool：「台股站回所有均線，惟量能有限將橫盤整理」。文中回顧 9/30 長上影、47,940.13 收盤、成交額 8,756.73 億，稱 48,300 以上壓力沉重、短線在 5 日線附近震盪，並提台積電 2,500 元上方壓力。 | 10/1 加權收 48,353.49（+0.86%），當日收在高點、創收盤新高，且高於 48,300；但當日高點仍比前一日盤中高點 48,379.71 低約 26 點。故「橫盤整理」對 10/1 單日走勢偏保守，對前高附近壓力與量能較弱的描述有部分對應。TWSE FMTQIK 與 MI_INDEX 都確認全證券總額由 9/30 的 9,214.93 億降至 10/1 的 8,740.46 億（-5.15%），同口徑量縮方向成立；但 KGI 9/30 的 8,756.73 億及新光/中央社 10/1 的 8,388.73/8,374.31 億均不等於 TWSE 一般股票列或全證券總計，公開報告未給出可重現這些數值的範圍定義。不能用混合來源驗證量能門檻。此 MoneySchool 全文可公開閱讀，修正先前「全文需登入」的紀錄；KGI 其他個股報告仍可能要求登入。該文未明訂整理區間與預測期間，單日只作觀察、不計正式命中率。[KGI 10/1 晨訊全文](https://moneyschool.kgi.com.tw/articles/stockcheck-stockbrief-advisory-20261001-68ea67d63499419ca4f0e4c9da15052d-1245)、[TWSE 9/30 成交總額](https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date=20260930&response=json)、[TWSE 10/1 成交總額](https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date=20261001&response=json)、[TWSE 加權 10/1 區間](https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date=20261001&response=json)。 |
| 中信投顧，2026-10-02，每日盤勢分析（評述 10/1） | 公開正文稱加權指數 +413.36 點至 48,353 點、成交額約 8,374 億元，量價中性偏多；以 KD 金叉、油價回落和亞股氣氛轉佳為背景，並條件式預期若美國非農穩健，指數有機會再挑戰前高。 | 比只有方向標題多了當日行情、技術面和一項可檢查的條件因子；但沒有量化「前高挑戰」期限/價位、非農門檻、失效條件或下行情境。成交額 8,374 億與 TWSE FMTQIK、MI_INDEX 全證券總計 8,740.46 億不一致約 4.2%；與中央社/多家盤後稿一致，但也不等於 TWSE 一般股票列 8,147.94 億。公開報告沒有交代成交值範圍；TWSE 外幣換算註記本身不足以解釋差額，不據此認定券商錯誤。未調和前不把重複轉載算成獨立佐證；10/2 盤中尚未到可評估其條件預測的期限。[中信投顧每日盤勢分析](https://www.ctbcsis.com.tw/)、[TWSE 10/1 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html)、[中央社 10/1 收盤報導](https://www.cna.com.tw/news/afe/202610010187.aspx)。
| 中信投顧，2026-10-02，仁寶 2324 個股報告 | 官網公開標題「增加持股、TP 42.2 元」，稱跨越 PC 轉型門檻、伺服器是營運轉折點；分析師徐子捷。 | 個別報告連結導向會員登入，仍只有題目/評等/目標價，無法核對 EPS/營收預估、估值方法、期限、失效條件或利益揭露。公司 2Q26 合併營收 NT$238.295B、年增 32%；AI 伺服器約占 server revenue 70%（公司自述），但毛利率 4.6%，毛利額年增 4%、營業利益年增 9%，稅前獲利另受金融資產評價利益增加帶動。後續月營收未延續同等年增：7 月 NT$81.006B、年增 38.3%/月減 15.5%；8 月 NT$67.337B、年增 14.5%/月減 16.9%；1–8 月累計年增 18.3%，月營收為公司內部自結未查核數。7–8 月月均 NT$74.17B，較 2Q 月均 NT$79.43B 低約 6.6%，但未作季節調整；月營收無伺服器分項，不能證明 AI 伺服器收入或利潤轉折。現階段論點只獲部分基本面背景支持；原報告品質仍不可評為高，維持「券商標題線索」。仁寶 9 月營收排定 10/8 公布，再看營收延續與正式季報毛利率、營業現金流，不以單月營收驗證 TP。[中信投顧報告清單](https://www.ctbcsis.com.tw/)、[仁寶 2Q26 官方營運結果](https://www.compal.com/zh-hant/media/344/ren-bao-dian-nao-min-guo-115nian-di-er-ji-ying-yun)、[仁寶 7 月營收](https://www.compal.com/zh-hant/media/342/ren-bao-dian-nao-min-guo-115nian-qi-yue-fen-ying)、[仁寶 8 月營收](https://www.compal.com/zh-hant/media/347/ren-bao-dian-nao-min-guo-115nian-ba-yue-fen-ying)、[仁寶營收公告時程](https://www.compal.com/zh-hant/investor-relations/events-announcements/)。 |
| 中信投顧，2026-10-02，晟銘電 3013 個股報告 | 官網公開標題「買進、TP 117.5 元」，稱 2H26 成長動能明確、公司逐步拓展 L11 整機櫃；分析師鄭子凡。 | 個別報告連結導向會員登入。公司 2026-08-06 公布的上半年合併營收 61.48 億元、營業利益 8.21 億元、歸母淨利 5.71 億元、EPS 2.78 元，可作已實現基線；本輪查閱到的官方公開資料沒有足以量化 L11 機櫃出貨/收入或 2H26 預估的細節。目標價缺少模型、評價基準日、上/下行情境與風險，現階段標為「券商標題線索」，不送交易通知。待取得可合法檢視的研究正文或新的公司營收/重大訊息，再核對成長假設。[中信投顧報告清單](https://www.ctbcsis.com.tw/)、[晟銘電官方財務/重大訊息](https://www.uneec.com/tw/major)。 |
| 凱基，2026-10-01，台積電 2330 | 「HBM4e 客製化基底晶粒將為 N3 需求挹注新動能」；研究員潘俊宏 | 官方標題與研究對象可確認，點開要求會員登入。沒有原始模型/估值，暫不評高品質或方向正確性。 |
| 凱基，2026-09-30，TPU v8 產業報告 | 題名稱 2027 AI 成長動能，列多檔台灣供應鏈 | 產業主題與受惠名單可作追蹤線索；未取得各公司營收/EPS 假設與敏感度。 |
| 國泰證期，2026-10-01，金居 8358 | 公開標題「買進」，標示目標價上漲空間 +25.3% | 原報告要求會員登入。目標價和標題不夠檢驗估值與利益揭露。 |
| 中信投顧，2026-10-01，大江 8436 | 公開標題「中立」，稱 2Q26 低於預期並下修財測 | 「預估修正」比單純初次覆蓋更值得追蹤，但原文須登入，還不能評估數字、風險情境或假設品質。 |

券商原文入口：[KGI](https://investment.kgisia.com.tw/Portal/Report/Index/Zh/R)、[國泰期貨研究平台](https://research.cathayfut.com.tw/FRRPortal/index.do)、[國泰證期歷史下載清單](https://consultant.cathayfut.com.tw/include/research_d9.aspx)、[中信投顧](https://www.ctbcsis.com.tw/)。截至 2026-10-02 10:08 台北時間，KGI 官網 10/2 個別研究點入仍要求會員登入；但 10/1 台股晨訊全文可在 MoneySchool 公開取得。國泰公開晨訊正文及多篇研究標題，個股/產業報告點入也要求登入。研華報告標題與前一日媒體採訪重合，適合歸為公司營運案例新聞整理，不應當作券商財測、目標價調整或新評等證據。公開資訊足以檢查部分標題論點和指數/成交數據，但無法校驗兆豐金研究模型，或把 Customer360 效率 KPI 換算成研華 EPS 影響。

KGI 官方免責說明表示報告資料及意見不保證完整/精確、依特定日期判斷且未必更新，集團及員工可能持有標的或與標的公司有業務關係。這是平台通用揭露，不代表上述單篇研究已揭露具體利益關係；可取得原文時，應連同分析假設和個別利益揭露一起評估。[KGI 會員/免責頁](https://investment.kgisia.com.tw/Portal/Login/LoginReport)

### 2026-10-02 13:20 成交值口徑覆核

直接查詢 TWSE FMTQIK 與 MI_INDEX：兩個端點在 9/30 都回傳 NT$921,492,702,613（9,214.93 億），在 10/1 都回傳 NT$874,046,252,416（8,740.46 億），互相一致。MI_INDEX 同時列出 10/1 一般股票 8,147.94 億、證券合計（1+6+14+15）8,163.32 億、總計（1–15）8,740.46 億。新光盤後稿與多家媒體的 8,388.73/8,374.31 億，以及 KGI 9/30 的 8,756.73 億，不等於全市場總計或一般股票列；可見來源未提供足以重現差額的口徑。故「價漲量縮」方向以 TWSE 同口徑比較仍成立（-5.15%），但券商 8,500 億條件可能翻轉，量能門檻先標「來源口徑未定」，不評二元命中，也不推斷是券商錯誤或外幣換算造成。[TWSE 9/30 MI_INDEX](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20260930&response=html)、[TWSE 10/1 MI_INDEX](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html)、[新光 10/1 盤後稿](https://www.skis.com.tw/Report/market/TSM20261001_after.html)、[中央社 10/1 收盤報導](https://www.cna.com.tw/news/afe/202610010187.aspx)。

### 2026-10-02 15:46 法人流快照差異

新光 10/1 盤後稿列三大法人買超 NT$292.53 億（外資 219.37、投信 56.57、自營 16.59）；新光 10/2 盤前稿回顧同一資料日則列 NT$294.62 億（投信 58.65）。這是 NT$2.09 億差異，幾乎全在投信列；後一稿公布的三個已四捨五入分項相加為 294.61 億，與總計差 0.01 億可由分項四捨五入解釋。TWSE 商品規格明定每日 14:50 產製一版（不含綜合帳戶及鉅額），19:40 再產製一版（含兩者）。兩個券商數字可能分別引用不同批次，但公開報告沒有標資料擷取時間/範圍，尚不能將此推定當成已證實，也不能據此判定哪篇錯誤。實務上，券商流量比較和 DataFlow 特徵至少應記錄資料日、擷取/公布時間、批次與納入範圍；同一家券商的盤後稿與隔日盤前稿不是兩份獨立確認。若來源介面無批次欄位，先標 `snapshot_scope=unknown`，避免精確流量訊號被版本差異污染。[新光 10/1 盤後稿](https://www.skis.com.tw/Report/market/TSM20261001_after.html)、[新光 10/2 盤前稿](https://www.skis.com.tw/Report/market/TSM20261002.html)、[TWSE 三大法人統計產品產製時間與範圍](https://eshop.twse.com.tw/zh/product/detail/d31c1b9570ae47058ec83a0bb1ffa419)。

### 2026-10-02 15:48 DataFlow 個股法人流收集時點缺口

Airflow `stock_collect_everyday_process` 的 10/1 run 記錄顯示，`stock_three_main_investor_collect` 在台北時間 16:45:03–16:45:23 執行。程式當時以交易日日期讀 TWSE `rwd/zh/fund/T86`、TPEx `insti/dailyTrade`，整理成逐股票的法人買進/賣出/淨買股數；存檔依「證券代號＋資料日期」刪除再重插，沒有保留原始回應、官方批次名稱或明確觀測時點欄。日線訊號讀該表時以 `資料日期 <= as_of_date` 取最多 45 天，因此 20:00 收盤掃描有可能把 16:45 抓到的當日值當成完整收盤法人流。這是程式/時程上的可信度缺口，但尚未證明 16:45 API 回應缺資料或與最終值不同。

證交所正式 Data E-Shop 的逐股商品規格為 `TWT86UC`（18:00 產製、不含鉅額）及 `TWTAIUC`（20:00 產製、含鉅額），欄位為各法人每檔證券買進、賣出與淨買股數；內用 NT$1,500/月、外部資訊用戶 NT$3,000/月，遞送方式列線上下載及 Email 檔案，未見 API/自動匯入規格。TWSE FAQ 說商品頁時點是平均預估產製時間，交易量增加或券商延遲申報時可能再延後，所以不可把 20:00 排程等同檔案已就緒。這與市場總額商品 BFI82U 不同（14:50/19:40、買賣金額）。現有 T86 JSON 查詢端點與付費檔案的版本/範圍等價性、該端點自動擷取授權都未查明。故先把當日個股法人流標記為「批次與完整性待驗」，不因其來源官方而自動視為最終值；先比對 16:45 資料、18:00/20:00 商品樣檔或 TWSE 書面說明，再決定是否採用正式授權 feed、延後收集或排除當日未確認批次。[TWSE 逐股法人商品規格、時程及費率](https://eshop.twse.com.tw/zh/product/detail/c4c87ac184e44896a05fcab5a9d544ec)、[TWSE Data E-Shop 產製時間 FAQ](https://eshop.twse.com.tw/zh/page/faq.html)。

### 2026-10-02 16:02 新光盤前條件核對與官方市場廣度

TWSE 官方 10/2 收盤資料顯示，加權指數收 48,475.74 點、上漲 122.25 點（+0.25%），高於新光列出的 48,000 點觀察線；官方主板「股票」漲跌家數為上漲 483（漲停 24）、下跌 506（跌停 1）、持平 91，股數廣度略偏負。這表示指數收紅與多數成分未同步，應在訊號通知中作中性廣度背景，不能從權值指數單獨推成普遍風險偏好。查找過程中，Investing.com 香港與中國語系頁面對同日加權收盤分別列 48,437.45、48,431.49，均與 TWSE 官方 48,475.74 不同；行情訊號應以交易所原始收盤為準，第三方頁差異先保留而不平均。[TWSE 10/2 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)、[Investing.com 香港頁](https://hk.investing.com/indices/taiwan-weighted-historical-data)、[Investing.com 中國頁](https://cn.investing.com/indices/taiwan-weighted-historical-data)

同一官方「股票」母體比較 10/1 與 10/2：上漲/下跌/持平由 422/551/107 變成 483/506/91，淨上漲家數由 -129 改善至 -23（+106），上漲/下跌比約由 0.77 升至 0.95；TAIEX 單日漲幅則從 +0.86% 收斂到 +0.25%。初步讀法是市場參與廣度正在改善但仍未轉正，屬值得追蹤的中性趨勢背景；只有兩日資料，不能推定後續方向或預測力。[TWSE 10/1 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html&type=ALLBUT0999)、[TWSE 10/2 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)

TWSE 官方台積電月日成交頁列出 10/2 高 2,515、低 2,495、收 2,500 元、下跌 10 元。新光盤前稿的「續站 2,500 元之上」若按嚴格大於解讀，盤中曾跌破且收盤恰等於門檻；若把「守住」解讀為收盤不低於 2,500，則收盤貼線。報告沒有明確說明盤中觸價、收盤確認或觀察期限，所以標記「台積電條件部分符合／判準不明」，而非二元命中或預測失敗。指數收盤條件高於 48,000 成立，但僅一日樣本仍不足以評估預測能力。[TWSE 台積電 10/2 官方行情](https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&stockNo=2330)、[新光 10/2 盤前稿](https://www.skis.com.tw/Report/market/TSM20261002.html)

今天官方盤後表是一次性人工對照，不代表 `twse_equity_quotes_daily` 已完成 DataFlow 自動採集；截至 15:46 該來源尚無 `source_runs`。等自然 20:00 收盤 DAG 後，再核對收集日期、接受/隔離列與摘要輸出，維持廣度只作市場背景、不進個股分數或方向通知。

品質判讀：新光這篇有明確指數/權值門檻、條件式情境，並把未確認的 US$500B 傳言標成未證實，作為盤勢觀察稿的可檢驗性不錯；但未交代採盤中或收盤值、預測期限、失效後續，也未把偏弱股票廣度納入。今天只得到指數條件達成、台積電條件貼線且判準不明，因此列「中等品質觀察／交易預測力未驗證」，不列高品質交易訊號。

### 2026-10-02 16:34 券商稿件事後核對

- 新光 10/2 盤前稿列出加權 48,000 點觀察線及台積電「續站 2,500 元之上」的條件。TWSE 收盤為 48,475.74（+0.25%），台積電高 2,515、低 2,495、收 2,500（-10 元）：指數收盤高於觀察線；台積電盤中跌破、收盤恰等於門檻。報告沒有說明採盤中或收盤判定、觀察期限及失效規則，仍標「部分符合／口徑不明」，不計命中。[新光 10/2 盤前稿](https://www.skis.com.tw/Report/market/TSM20261002.html)、[TWSE 10/2 指數與廣度](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)、[TWSE 台積電日行情](https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&stockNo=2330)
- KGI 10/1 盤後稿稱短線可望挑戰 48,601，若要突破則成交額需達約 8,600 億，否則較可能震盪；另列 47,578 支撐。10/2 收盤未越過 48,601；TWSE「股票」成交金額約 8,637.98 億，高於報告所稱約 8,600 億。因量能條件只寫成突破的必要條件、基準成交額口徑和預測期限未明，這一天既不能當作成功，也不足以判失敗；保留為可檢驗觀察，不列高品質交易訊號。[KGI 10/1 盤後分析](https://moneyschool.kgi.com.tw/articles/stockcheck-stockbrief-advisory-20261001-12ed4570dbff43d9815ae54c03b245d4-1260)、[TWSE 10/2 市場摘要](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)
- 元大證券網頁刊載的 10/2 10:38 盤中解析標示《時報》記者，摘要權值股偏弱、被動元件/面板等題材接棒；這是券商頁面的媒體即時新聞，非可識別分析師署名的原始研究。沒有明確期限、價格確認或失效條件，只適合作盤中背景；收盤時 TAIEX 小漲而台積電下跌，也不能把「題材接棒」解讀成可執行訊號。[元大證券網頁的 10/2 盤中解析](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202610020103820)
- 16:34 搜尋時，KGI 即時情報頁仍列 10/1 台股盤後分析為最新台股盤後稿；這是當下搜尋快照，不代表 10/2 稿件之後不會發布。[KGI 即時情報](https://moneyschool.kgi.com.tw/sub-category/stockbrief)
- SinoTrade stockchannel 10/2 08:37 刊出標示「統一證券」的盤勢觀點，稱高檔震盪盤堅，列出台指期夜盤 -223 點、外資現貨買超 NT$21.937B（219.37 億）及 TX 淨空單增加 1,389 口等混合線索。TAIEX 收 +0.25% 與震盪盤堅相容，但官方股票漲跌家數仍是 483 上漲、506 下跌，且台積電收跌；沒有硬性價位、期限或失效條件。文中三大法人買超 NT$29.462B（294.62 億），與新光 10/2 盤前稿同數，卻不同於新光 10/1 盤後 NT$29.253B（292.53 億）；無批次欄，不能視為獨立確認。頁面未列個別分析師或原始 PDF，故只評為中等品質市場背景，預測力未驗證。[統一證券署名觀點（SinoTrade 頁面）](https://stockchannelnew.sinotrade.com.tw/z/zf/zfz/zfz_500B90B4-5B75-4A4A-8B1F-652CFCB6725F.djhtm)、[TWSE 10/2 指數與廣度](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)
- SinoTrade stockchannel 10/2 08:37 另刊出標示「國票證券」的觀點，列 47,800 為短線結構觀察線，若非農不確定性消化且台積電再創高，指數可望挑戰 48,601。10/2 加權收 48,475.74，高於 47,800、未達 48,601；台積電收 2,500、下跌 10 元，故上檔情境條件當日未觸發。明確價位提高可檢驗性，但沒有期限/失效規則；48,601 是公開歷史高點且其他券商亦引用，不代表獨立 alpha。頁面未列個別分析師或原始 PDF，列中等品質條件式盤勢觀察、預測力未驗證。[國票證券署名觀點（SinoTrade 頁面）](https://stockchannelnew.sinotrade.com.tw/z/zf/zfz/zfz_DA5AC6E8-CF82-4732-B0FD-6538C3C4C4C8.djhtm)、[TWSE 10/2 指數資料](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)、[TWSE 台積電日行情](https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&stockNo=2330)

### 2026-10-02 16:34 DataFlow 行情資料日核對

唯讀 DB 顯示 10/2 `三大法人買賣` 已有 24,797 列，但 `個股成交資訊` 最新仍是 10/1（1,938 列、10/2 為 0）；Airflow 自然 run 的 TWSE/TPEx 收盤價 task 在 16:33 仍為 `running`。16:30 盤中掃描雖為 `ready/fresh`、候選 0，不能當作 10/2 收盤行情已完成或日線掃描已驗證。待 20:00 自然收盤流程後，再比對 `market_signal_runs.as_of_date`、價格資料日與法人流資料日；若不一致，將當日結果標為 partial/stale，不沿用「fresh」字樣作完整性證據。

### 2026-10-02 16:44 收盤行情 task 進度追查

Airflow 兩個行情 task 在 16:44 仍為 `running`，但同一時間的 live task log 持續新增：TWSE 已處理到代碼 1909，TPEx 到 5530。唯讀查詢 `股票模板_diff` 暫存表已有 10/2 219 列，`股票模板_diff2` 有 496 列；正式 `個股成交資訊` 仍無 10/2 列。程式先將每檔回應 append 到 diff staging，完成全部證券後才由 `save()` 合併寫入正式表，因此正式表暫無當日列是預期的中間態，不是 task 卡死或失敗。先等自然收集完成，不清 staging、不手動重跑。

另確認 `market_signal_runs.source_status='fresh'` 在盤中模式表示 source-extension mapped collection 完整，並不代表舊版 `個股成交資訊` 的收盤價已是當日資料；兩種新鮮度必須分開。收盤驗證仍以正式行情表資料日、run 的 `as_of_date` 及法人流日期三者對齊為準。

16:50 再查同一個 live run：兩個 task 仍是 `running`，最新 task log 已更新到台北時間 16:50（TPEx 代碼 6692、TWSE 2345）。staging 10/2列數由 16:44 的 219/496 增至 284/660；正式表仍等整批 `save()`。這是持續進度，不重跑；收盤 DAG 尚未到 20:00 排程時間。

### 2026-10-02 18:14 玉山投顧公開第四季策略文章

玉山證券投顧官網於 10/1 刊載「十月投資策略報告，哪些股票、ETF是第四季關注重點」，正文稱依據玉山投顧十月份投資月報整理；公開頁可讀，但沒有連結底層完整月報或個股估值模型。因此資料集分類為「官方投顧公開策略摘要」，不能記成已取得完整逐股原始研究。[玉山投顧文章](https://www.esunsec.com.tw/article/post?postid=410C0D4129754A6EB0F2B4DE3256ED4F)

抽查宏觀事實大致吻合官方一手資料：Fed 9/16 升息 25bp 至 3.75%–4.00%，9 月 SEP 年底政策利率中位數 2026、2027 年皆 4.1%；BEA 8 月名目 PCE 月增 0.9%、核心 PCE 月增 0.2%/年增 3.0%；央行將 2026 年成長率預測上修至 11.48%；中國 8 月工業增加值 +5.2%、零售 +0.4%、前八月固定資產投資 -7.2%，9 月製造業 PMI 為 50.1%。來源：[Fed 決策](https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm)、[Fed 9 月經濟預測](https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20260916.htm)、[BEA PCE](https://www.bea.gov/news/2026/personal-income-and-outlays-august-2026)、[央行經濟預測](https://www.cbc.gov.tw/tw/cp-302-192864-4319f-1.html)、[中國 8 月經濟數據](https://www.stats.gov.cn/sj/zxfbhjd/202609/t20260915_1965307.html)、[中國 9 月 PMI](https://www.stats.gov.cn/zwfwck/sjfb/202609/t20260930_1965449.html)。這是抽樣核對，不等於逐項審計報告內所有數字；文中外資/投信與費半數據尚未逐筆重算。

品質判讀：利率提高估值門檻、總體成長不等於每家公司獲利、AI 支出須追到供應鏈實際營收/毛利，以及融資升高可能擴大回檔壓力，構成有用的研究框架。報告也承認近十年季節性樣本不能當成今年機率，這點合理。整體列為「中等品質市場背景；低可執行個股訊號」。

限制包括：47,000/45,000/43,815 點及櫃買 396 點沒有公開推導、預測期限、採盤中或收盤觸發、失效條件；相關公司只是概念族群名單，無逐股 EPS/營收/利潤率橋接、目標價期限或風險報酬。文章數據截點也不同：外資/投信截至 9/24、費半截至 9/25、ETF 表截至 9/29、美國 PCE 為 9/30 公布的 8 月資料。十年季節性資料雖計算與其列示樣本一致，N=10 仍不足以作交易方向證據。

不把概念股直接加入方向評分或通知。可延伸為可追溯背景：追蹤報告所列企業與 AI 資本支出/產品規格的可量化曝險，待公司營收、毛利率、現金流及市場價格有新資料後逐項驗證；後續若建立追蹤組合，必須固定報告發布後的 1/5/20 交易日相對報酬與可用時點，且明確標示它是研究清單而非報告明示買進建議。

### 2026-10-02 18:14 富邦投顧公開報告入口抽查

富邦投顧官網公開 10/1 盤後快訊，稱台股收漲 413 點、櫃買收漲 0.4%，成交量低於五日均量，同時均線上揚、KD/MACD 偏多；列出被動元件、網通、MCU、功率半導體為短線強勢族群。10/2 另一篇「每日五檔焦點股」可見日期與標題，但沒有公開內文。官網也列出 10/1 洋基工程（6691）個股報告、9/29 聯茂（6213）報告，內文均要求會員登入。[富邦投顧報告入口](https://fubonresearch.fubon.com/Research/)

品質判讀：官方發布及日期可追溯，10/1 指數漲點與公開市場數據相符；但公開快訊未附分析師署名、固定預測期限、明確買賣條件、價格/量能門檻或失效規則，技術指標計算與成交量均線口徑也未列。10/2 的市場廣度仍略偏負（漲 483、跌 506），所以「指數強」不能推成全市場普遍強勢。列為「中等品質盤勢背景；無可評分的個股交易訊號」。登入牆後的個股報告只保存為待取原文線索，不評標題、目標價或報告數量。

來源盤點新增：富邦投顧有可公開追蹤的盤後/主題摘要，也常刊個股、產業和 ETF 報告；但個股模型需會員登入，當前可用性與 KGI、國泰及中信的 gated reports 相似。後續優先記發布時間、價格假設、EPS 修正、目標價期限、分析師署名和風險條件是否公開；不繞過會員頁面，也不把網站摘錄自動當成逐股高品質訊號。

### 2026-10-02 18:28 台北富邦銀行 NVIDIA/AI PC 產業報告回查

找到台北富邦銀行投資研究部 2026-06-10 16:00 的 4 頁公開報告《主要產業總評：NVIDIA 宣戰 PC》。注意它是銀行投資研究部，不是富邦證券個股研究；頁面標示舉例股票並非單一標的推薦。[公開 PDF](https://www.fubon.com/banking/document/Corporate/Financial_Market/TW/%E4%B8%BB%E8%A6%81%E7%94%A2%E6%A5%AD%E7%B8%BD%E8%A9%95%EF%BC%9ANVIDIA%E5%AE%A3%E6%88%B0PC_20260610.pdf)

官方資料可支持部分事件/規格：NVIDIA GTC Taipei 主題演講稱 RTX Spark 的 N1X 是與 MediaTek 合作設計，官方產品資料列 20 核 Grace CPU、Blackwell GPU、最高 128GB unified memory；NVIDIA 5/31 新聞稿稱 Vera Rubin NVL72 正進入量產，並稱 10x agent throughput at scale 對比 Grace Blackwell。[NVIDIA keynote](https://www.nvidia.com/en-us/on-demand/session/gtctaipei26-stw61044/)、[RTX Spark 規格](https://www.nvidia.com/en-sg/products/rtx-spark/)、[Vera Rubin 量產稿](https://nvidianews.nvidia.com/news/vera-rubin-full-production-agentic-ai-factory)

品質判讀：N1X/Rubin 與供應鏈方向有官方事件支撐；但報告把「AI 推論效能提高 10 倍」進一步寫成單一機櫃、同電力可做 10 倍運算，沒有保留官方所述的 agent throughput / 特定 benchmark / scale 範圍。報告說推出 N1X 與 N1，官方 keynote 本輪只核實到 N1X；N1 單獨產品發布待來源。AI PC TAM 使用 Precedence Research 預估，來源商用預測而非可重現公司訂單；毛利率從傳統代工 3–5% 到系統整合 10–15% 的推論沒有在頁面附公司別模型/資料。整體「中等產業背景、低個股可執行性」，不加進方向分或通知。

報告第 4 頁視覺表格確認欄位為：2026/6/8、2Q26、3Q26、4Q26、2027Q1、2026 YTD、2026 年底。台灣加權指數依序為 44,564、43,503、42,430、45,338、51,802、44,564、45,338；季度變化為 +37.1%、-2.5%、+6.9%、+14.3%，YTD +53.9%、全年 +56.5%。

回查 TWSE 官方市場成交資料：6/8 加權指數收 43,502.78，與報告表內「6/8 44,564」相差 1,061.22 點（約 2.44%）；反而表內 2Q26 的 43,503 幾乎等於當日實際收盤。TWSE 6/30 收盤為 46,125.91，若按正文「2Q26 收盤價預測」解讀，實際收盤高於 43,503 預測 2,622.91 點（6.03%）。但註 1 又說看漲季度值是當季最高點、看跌值是當季最低點，與正文的「收盤價預測」口徑不一致，不能把 2Q close 誤當 quarter high。

一筆季度極值條件可核對：3Q26 報 -2.5%、目標 42,430；註 1 的看跌情境應取該季低點。TWSE 官方 7 月指數歷史資料列 7/29 低點 39,384.85，較預測低 3,045.15 點（預測值高估低點約 7.18%）。方向上出現更深的下探，但點位幅度未覆蓋實際低點；目前只算一筆市場情境觀察，不納入個股交易訊號績效。

綜合：N1X/Rubin 與供應鏈主題有部分官方事件支撐，作產業背景仍有參考價值；但指數基準值不符官方資料、正文與註腳的價格目標定義衝突，且沒有逐股 EPS/營收/利潤率橋接或進出場/失效條件。列為「中等產業背景、低個股可執行性；指數預測品質待查」，不加進方向分或通知。[TWSE 2026 年 6 月市場成交資訊](https://www.twse.com.tw/exchangeReport/FMTQIK?date=20260630&response=html)、[TWSE 2026 年 7 月加權指數歷史資料](https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?date=20260701&response=html)、[NVIDIA 5/31 Rubin 規格與官方措辭](https://nvidianews.nvidia.com/news/vera-rubin-full-production-agentic-ai-factory)

### 報告可檢驗性欄位

新增券商訊號時，逐項保存發布時間與預測期限、可觀察的確認/失效條件、原始數據來源與定義、所屬市場/基準、盤後對照來源，以及基本面數字是公司自述或已公告財報。每個條件分別標記「達成／未達成／口徑不明」，不要把混合成交額序列或同一家券商的盤前與盤後稿當成獨立驗證。條件明確只能提高可檢驗性；須累積多個不同日期的樣本，才評估訊號表現。

## 新資料源評估：期交所大額交易人未沖銷部位

- 期交所每日盤後公布各期貨商品多方、空方排名前五/前十名未沖銷部位的合計口數與占全市場比例；括號內另列這些前五/前十名中屬「特定法人」的部位，並非全體法人部位。資料頁歷史起始可回溯至 2004-07-01，台股指數合約按 TX + MTX/4 + TMF/20 折算。[期交所大額交易人資料](https://www.taifex.com.tw/cht/3/largeTraderFutQry)、[欄位與口徑說明](https://www.taifex.com.tw/cht/3/largeTraderFutQryDetail)
- 2026-10-01 台股期貨「所有契約」列：全市場未沖銷部位 119,549 口；多方前五大 73,819（61.7%），前十大 81,195（67.9%，其中特定法人 80,270/67.1%）；空方前五大 56,700（47.4%），前十大 77,850（65.1%）。多空兩邊各自排名，不是同一批交易人的淨部位。
- 2026-10-02 期交所「所有契約」列：總未沖銷部位 118,531 口，較 10/1 的 119,549 減 1,018（-0.85%）。前十大多方 80,841（68.2%），較前日少 354 口、占比 +0.3 個百分點；前十大空方 78,636（66.3%），多 786 口、占比 +1.2 個百分點。前五大多方 73,291（61.8%）、空方 57,034（48.1%）。兩側各自排名且交易人匿名，不能相減成淨部位或推成方向；單日變動先作部位集中風險背景，等待多日延續及現貨/市場廣度配合再評估。[期交所 10/2 大額交易人資料](https://www.taifex.com.tw/cht/3/largeTraderFutQryTbl)、[欄位口徑說明](https://www.taifex.com.tw/cht/3/largeTraderFutQryDetail)
- 評估：這比單夜淨成交多出未平倉和集中度，可用來觀察持倉集中是否延續；但不揭露交易人身份，也不能只憑集中度判定方向。括號「特定法人」只統計落在前五/前十名內的合資格法人，不能當成全體外資或全體法人持倉。資料源與口徑品質高，預測能力未驗證，先作風險背景，不新增交易通知。
- 若延伸至個股期貨，只對期交所有相應契約且流動性/未平倉足夠的個股建立覆蓋；先固定「近月/所有契約」口徑並控制到期換月，再配對現貨法人流、單夜期貨流與市場廣度，檢視 1/5/10 個交易日相對報酬。覆蓋不足或低流動性契約不得當成負向訊號。
- 找到可合法研究的個股期貨參與度資料：政府資料集 22644 提供股票期貨/ETF期貨契約代碼到標的證券的對照，OGDL v1 免費但不定期更新；資料集 55021 提供每日契約代碼與成交量，OGDL v1 免費、標示每日更新。成交量表沒有買賣方向、交易人類別或未平倉量，所以只能形成「衍生品參與度」候選背景，不能視為多空部位流。合約映射也要保留生效版本並處理展期；在確認 InfoHub 的個股未平倉/大額交易人下載介面及授權、固定流動性篩選、累積多週樣本並做 1/5/10 日相對績效以前，不接入方向評分或通知。[期交所股票期貨標的資料集](https://data.gov.tw/dataset/22644)、[每日個股期貨交易量資料集](https://data.gov.tw/dataset/55021)、[期交所個股期貨資訊查詢](https://www.taifex.com.tw/eventTaifexTradingCenter/cht/ssf.do)。

## 新增 TWSE 注意／處置公告：交易風險背景

- 官方 OpenAPI 提供 `/announcement/notice`（集中市場當日注意股票）與 `/announcement/punish`（集中市場處置股票）。2026-10-02 11:23 台北時間實測：注意端點回傳單筆全空 sentinel，不能解讀為零檔注意股票；處置端點回傳 9 筆，包含處置公告日、代碼、理由、期間與全文措施。新增解析會略過全空/無日期的注意 sentinel，處置期間需成功解析起迄日，且資料日須落在有效期間內才顯示。[TWSE OpenAPI](https://openapi.twse.com.tw/)
- 注意交易資訊是在收盤後依價格漲跌、成交量、週轉率、集中度、本益比、券資比、借券賣出及當沖等異常條件發布；部分類型只有風險提醒，不會因此處置。處置則可能限制撮合頻率、要求預收價金/證券或融資融券保證金，直接影響成交和流動性。故這是高權威的監理/執行背景，適合在符合方向條件的候選卡提醒，不是獨立多空確認，也未驗證有預測力。[TWSE 注意交易作業要點第 4 條](https://twse-regulation.twse.com.tw/TW/law/DOC01_print.aspx?FLCODE=FL007225&FLNO=4)、[第 6 條處置措施](https://twse-regulation.twse.com.tw/TW/law/DOC01_print.aspx?FLCODE=FL007225&FLNO=6)。
- `NumberOfAnnouncement` 的公開 schema 語意未確認，原值保留供追溯、不標成公告累計次數；處置卡會帶公告理由與從全文提取的措施片段。只覆蓋上市股票，且限於日線卡；不會把注意／處置事件另發成多空通知。TWSE 的規則/資料用於監理背景，對外再散布授權條款仍需另外核對。
- 上櫃同類資料目前查到 TPEx E-Data Shop 的每日 `TRADING_ATTENTION.CSV` 與 `DISPOSAL_INFORMATION.CSV`；產製時間 22:00，內部/外部使用均標 NT$3,000/月。現行來源 DAG 20:00 執行，時間早於檔案產製；先記缺口，不購買或繞過訂閱。若未來評估訂閱，還要設計 22:00 後補抓及許可條款，再決定是否納入。[TPEx 商品與費率](https://eshop.tpex.org.tw/zh/product/detail/B67FFF1FA60285D9232DE2C017EDA26C)。

## 已收集季度資產負債表：流動性風險背景候選

- 2026-10-02 11:46 台北時間唯讀查核 `crawler.source_records` 與 `source_record_fields`：最新有效季為 2026-06-30。一般業 TWSE 1,049 筆、TPEx 885 筆都有流動資產及流動負債數字；證券業 TWSE 3 筆、TPEx 7 筆及 TWSE 異業 4 筆也都有兩欄，合計 1,948 筆公司季資料可計算 `流動資產 ÷ 流動負債`。金融業、金控、保險等 28 筆雖有資產負債表列，但這兩欄未提供，不能把空值當成流動性差。
- 目前資料庫只留有 2026 Q2 一個有效季，未有可比前季，故尚不能算變化、檢查趨勢或作候選訊號；來源端目前呈現最新季快照，下一輪季報到來時要確認舊期資料是否仍可取得並保留點時序列。
- 可能的延伸是依公司與產業比較流動比率、營運資金及季變化，作財務風險背景，再與價格/成交、法人流及公司公告交叉判讀。流動比率受產業營運週期和資產分類影響，單一絕對門檻不能跨產業套用；至少需兩個已公告季度、具同業分組及缺漏覆蓋，再以未來流動性/獲利結果檢驗。當前資料權威性高、單期可計算性高，但趨勢覆蓋不足，暫不放入候選卡或多空分數。[TWSE OpenAPI](https://openapi.twse.com.tw/)、[TPEx OpenAPI](https://www.tpex.org.tw/openapi/)。

## 融資券與放空部位：現有覆蓋、通知延伸與上市資料缺口

- 直接查核 `crawler.source_records` 的最新有效資料日（2026-10-02 11:16 台北時間）：上市融資券到 10/1，共 1,039 列/代碼，其中 994 列為可用數字、45 列的融資與融券現值皆空；上櫃融資券到 9/30，801 列全數可用；上市/上櫃合併的「當日可借券賣出股數」到 10/1，1,718 列全可用；上櫃「融券借券賣出餘額」到 9/30，814 列全可用。來源 DAG 在 10/1 20:00 台北時間成功跑完四個來源任務。這是截至最後一次收盤擷取的來源日期，市場時段尚未結束時不可把各來源的最新日視為同一個同步快照。
- 45 列上市融資券隔離記錄抽樣顯示代碼與資料日存在，但融資/融券現餘額、前日餘額和使用率皆為空；先視為沒有信用交易數字的覆蓋差異，不據此判定為壞行情或源端故障。後續若比對 TWSE 合格信用交易標的名單，再確認是否為預期不適用代碼。
- 程式已計算近 5 筆日資料的融資餘額與融券餘額變化；原通知僅顯示融資變化及融資使用率，未呈現已計算的融券變化。已把融券餘額變化補進每日候選卡，明確標「部位背景，非多空訊號」；並將上櫃欄位標成「借券賣出餘額」，與一般借券存量區隔。這次只改通知說明，不改方向、分數、候選門檻或排序，尚未等到有合格候選的自然收盤卡片驗證呈現。
- 三種欄位不能混為一談：融券餘額是信用交易未回補賣空部位；上櫃來源的 `securities_borrowing_balance` 來自櫃買中心「融券借券賣出餘額」資料；TWSE/TPEx 的 `available_short_shares` 是可借券賣出額度/股數，不是已借入或已實際賣出的股數。TWSE 說明一般借券可能用於避險、套利、還券或履約，借券餘額不等同借券放空；真正賣空應看實際借券賣出量與尚未回補餘額。[TWSE 借券名詞說明](https://www.twse.com.tw/zh/products/sbl/disclosures/info.html)、[TPEx 融券借券賣出餘額及計算方式](https://www.tpex.org.tw/zh-tw/mainboard/trading/margin-trading/sbl_hist10110.html)。
- 新缺口與授權核查：上市市場目前收集可借券賣出額度及融券餘額，沒有與上櫃 `tpex_margin_sbl` 同口徑的「實際借券賣出餘額」。TWSE `TWT93U` 官方查詢頁可互動查詢並下載 CSV；欄位含前日/當日融券與借券賣出、還券、調整、餘額及次日限額，資料自 2019-12-23 起、每個交易日 23:30 產製。[TWSE 借券賣出餘額查詢](https://www.twse.com.tw/zh/trading/margin/twt93u.html)、[欄位與產製時間](https://eshop.twse.com.tw/zh/product/detail/000000006e0bbe8d016f181acc1f0321)。但 TWSE 網站使用條款明訂，未經同意不得以自動化裝置、指令碼、爬蟲或擷取程式下載資料；因此「網頁可查/可下載」不等於可排程抓取。網路資訊商店列內部使用 NT$1,000/月、外部使用 NT$1,500/月；正式自動採集需取得 TWSE 認可方式/書面同意，或由使用者授權訂購內用資料。官方條款另只排除已授權至政府資料開放平臺的資料；本次搜尋未找到 `TWT93U` 的 OpenAPI/開放資料版，先不視為開放授權。[TWSE 網站使用條款](https://www.twse.com.tw/zh/terms/use.html)、[商品價格及用途限制](https://eshop.twse.com.tw/zh/product/detail/000000006e0bbe8d016f181acc1f0321)。即使授權，現行收盤採集 DAG 為台北時間 20:00，早於 TWT93U 23:30 產製，應另排晚間採集或隔日補收，不能直接塞進 20:00 snapshot。
- 存取紀錄：閱讀 TWSE 使用條款前，本輪曾以 PowerShell 對 `exchangeReport/TWT93U` 與 `rwd/zh/marginTrading/TWT93U` 兩個端點各做一次 HTTP GET，確認端點可達及回應欄位；未寫入資料庫、檔案或正式流程。發現自動擷取限制後已停止，不再以腳本存取，除非先取得 TWSE 授權。
- 目前 SBL 單日變化提示使用固定股數及百分比雙門檻（可借股數變化至少 50,000 股且 25%；上櫃借券賣出餘額變化至少 100,000 股且 20%），這些是程式過濾條件，尚未以自由流通股數/成交量正規化，也未驗證預測力；保留為背景線索，不作跨股票方向比較。下一輪品質研究應以實際賣出餘額/流通股數、每日借券賣出量/成交量、融券變化和價格相對市場報酬組合，切開上市/上櫃及訊號日，測 1/5/10 個交易日並扣成本後才討論升級。

## TWSE 公司財測與實績差異：官方事件候選及欄位疑點

- 2026-10-02 約 11:51 台北時間直接讀取 TWSE OpenAPI：`t187ap15_L` 回 8 列，`t187ap16_L` 回 7 列。OpenAPI 目錄將前者稱為各季綜合損益財測達成情形；後者明確限定當季實績/財測差異至少 10%，或截至當季累計差異至少 20%。後者來源權威、比較期間清楚，適合作為公司財測偏差事件；它是發行公司自己的財測與查核/核閱實績，不是券商 EPS 預估，也不直接表示利多或利空。[TWSE OpenAPI 目錄](https://openapi.twse.com.tw/)、[公開財測規則第 23 條](https://twse-regulation.twse.com.tw/TW/law/DOC01.aspx?FLCODE=FL021142)。
- 發現 `t187ap15_L` 有重要欄位配對疑點：2412 中華電的「截至該季實績」20,593,145，與 `t187ap16_L` 的累計實績一致，但其「截至該季預測數」9,232,695–9,249,329，卻與 `t187ap16_L` 的**當季**預測相同，而非累計預測 18,224,469–18,260,262。2515 中工亦為「截至當季實績」618,350 搭配 -32,649；後者與 variance feed 的當季預測相同，累計預測則為 128,467。故 `t187ap15_L` 只原樣保存及研究，不計算達成率、不當成可比 actual/forecast 訓練資料，等待 TWSE 欄位說明或跨公司核對。[TWSE 財測達成 API](https://openapi.twse.com.tw/v1/opendata/t187ap15_L)、[TWSE 差異 API](https://openapi.twse.com.tw/v1/opendata/t187ap16_L)。
- 程式新增兩個官方來源：財測達成原始列排入 20:00 盤後採集；差異名單每 15 分鐘輪詢並作獨立 Discord 事件卡，保留季別、涵蓋期間、財測序號，以及當季/累計實績與預測原值。相同季度、期間、數字只通知一次，保留來源列；卡片明示公司財測、差異門檻與方向未定，不納入日線三家族評分或績效結算。10/2 12:15 台北時間自然盤中 run 已完成 schema/catalog bootstrap：source registry 29 筆、差異端點 HTTP 200 回 7 列且 7 列 accepted/0 rejected；同輪盤中分析 `ready/fresh`，產生 5 個事件候選並標記 5 則通知 sent。啟用期間 12:00 與 12:15 兩輪各送出同一批 5 檔，因中間修正過候選去重指紋而造成一次重複；目前指紋已固定為期別、涵蓋期間和實績/預測值。12:30 自然盤中 run 已成功，分析 `ready/fresh`、來源完整，0 候選、通知 sent=0/failed=0；這輪沒有重送，但因沒有重生候選，尚未直接壓測相同候選再次出現時的指紋冪等性。兩輪各入庫 7 列，14 筆中包含兩套 key 版本；盤後 `t187ap15_L` 尚待 20:00 run 確認。
- TWSE 公開財測規則要求按季比較公司綜合損益實際數與預測數；截至當季止差異達 20% 以上時，公司需重新評估假設並說明是否更新。這使差異事件值得回查原因和公司解釋，但規則本身並不證明未來股價方向或券商報告品質。[規則第 23 條](https://twse-regulation.twse.com.tw/TW/law/DAT0201.aspx?FLCODE=FL021142)。
- 12:45 台北時間再查應用資料庫：12:00、12:15 盤中 run 各有 5 候選、5 筆通知 ledger `sent`；12:30、12:45 各 0 候選、0 筆通知。這確認後兩輪沒有重送，但候選為 0，未驗證相同候選重新出現時的冪等性；截至 12:45 `market_signal_outcomes` 仍是 0 列，10/2 收盤後結算尚未發生。

## 大摩半導體通路報告：事後驗證與缺口（2026-10-02）

- 可追溯的公開版本是經濟日報 2026-05-20 對摩根士丹利報告的新聞摘要，不是大摩原始模型。公司投資人專區的「產業研究券商」列出法國農業信貸、瑞信、德意志、摩根士丹利與元大，但該頁只列券商名稱、沒有報告原文或模型。故下列只評公開摘要中可核對的預測，不替大摩完整模型打分。[經濟日報 5/20 摘要](https://money.udn.com/money/story/5607/9515406)、[大聯大研究券商名單](https://www.wpgholdings.com/investors/investor_services/zhtw/analyst-coverage)。
- 大聯大 3702：大摩摘要稱 1Q26 營收符合預估、毛利率與營業利益率各高約 0.2 個百分點、EPS 高 12%；公司同期公告 1Q EPS 3.30 元，並給出 2Q EPS 3.65–4.06 元、營收 3,450–3,650 億元等正式財測。摘要以營收中位數推論 2Q EPS 季增 17%。8/11 公司公布 2Q EPS 5.12 元、營收 4,562.11 億元、稅後淨利 86.2 億元：EPS 較 1Q 增 55.2%，較公司財測高標高 26.1%；營收較公司財測高標高 25.0%。因此公開摘要的方向判讀正確，但可見的 EPS 增幅推論保守許多；這不是大摩完整 EPS 模型誤差，因新聞沒有刊出其 2Q EPS 絕對預估。[公司 1Q 結果與 2Q 財測](https://www.wpgholdings.com/news/detail/zhtw/operation/26032)、[公司 2Q 結果](https://www.wpgholdings.com/news/detail/zhtw/revenue/26047)。
- 文曄 3036：大摩摘要將 2026–28 EPS 預估上修 12%、17%、16%，目標價 299→349 元，並以資料中心到 2028 年可能占營收 64% 支持 AI 通路成長論點。公司 2Q26 營收 5,907 億元、EPS 7.68 元，營收略高於先前 5,600–5,900 億元財測上緣；這支持近期營運強，但不能驗證 2026–28 EPS 上修幅度或 2028 組合假設。公司 9/1 另公告發行 GDR 與可轉債，預計募資約 9.35 億美元以支應外幣購料及償還外幣借款；記為資金與潛在股本變動背景，不據此推斷股價因果。[文曄 2Q 法說/財務更新](https://www.wtmec.com/investor-relations/financials/quarterly-results/)、[文曄公司新聞](https://www.wtmec.com/news/)。
- 價格結果提供反證，但尚不能判定目標價到期命中與否：以 5/20 收盤至 10/1 收盤，3702 為 119→119 元（0%），3036 為 279→212.5 元（-23.8%），加權指數為 40,020.82→48,353.49（+20.8%）。兩股同期都落後大盤；簡單報酬差約 -20.8 與 -44.7 個百分點。這是未調整股利/公司行動的價格比較；報告目標價期限、路徑與原始估值假設未取得，所以只記「截至 10/1 的中途表現」，不稱目標價預測失敗。[TWSE 3702 5 月日線](https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20260501&stockNo=3702&response=json)、[TWSE 3702 10 月日線](https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20261001&stockNo=3702&response=json)、[TWSE 3036 5 月日線](https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20260501&stockNo=3036&response=json)、[TWSE 3036 10 月日線](https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=20261001&stockNo=3036&response=json)、[TWSE 加權指數 5 月日線](https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date=20260501&response=json)、[TWSE 加權指數 10 月日線](https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST?date=20261001&response=json)。
- 目前可給這份公開摘要「中等可核驗度、尚不能評為高品質原始研究」：它提供了季度、同業、EPS 修正及目標價，且部分數據可由公司公告交叉核對；但沒有大摩原文、明確模型表、EPS 絕對值、風險/失效條件與目標價期限。以 3036 為例，目標價是否採高於歷史均值的本益比、資料中心營收占比能否轉成獲利與現金流，都需要原始假設才能判斷估值合理性。不要只把「AI 需求」或標題評等升成交易訊號。
- 重要反向線索：大聯大 1H26 稅後淨利 141.69 億元，但營業活動現金流出 988.99 億元、淨營運資產變動 -1,092.03 億元，融資負債增加 966.63 億元。高速擴張的通路商需要大量營運資金，這不等於財務困境，但營收/EPS 加速應與應收帳款、存貨、現金轉換及短期借款一起看；列為財報品質與風險背景，不能獨立推方向。[大聯大 8/18 法說資料](https://www.wpgholdings.com/news/detail/zhtw/revenue/26050)。

## 永豐投顧原始報告與 6805 富世達預估檢核（2026-10-02）

- 可取得永豐證券投資服務 2026-03-06 原始晨報 PDF。富世達 6805 頁列 2026 EPS NT$52.08、2027 EPS NT$74.30；2026 預估營收 NT$17.65B、毛利率 31.04%，2027 預估營收 NT$23.37B、毛利率 32.44%。NT$2,100 目標價近似 2027 EPS × 28 倍（74.30 × 28 = 2,080.4，再取整）；可重算估值基礎，但報告缺少清楚的 downside/sensitivity 表，也沒有可見的營業現金流、應收帳款或存貨分析。[永豐 3/6 原始研究 PDF](https://scm.sinotrade.com.tw/Article/GetFile/5558020e-5960-4068-98bc-907fd5c73153)。
- 這不是只有初次覆蓋的報告：本次目標價由 NT$2,000 上調至 NT$2,100，2027 EPS 由 NT$72.36 上修至 NT$74.30（+2.7%），2026 EPS 則由 NT$52.50 微降至 NT$52.08（-0.8%）；變更方向與估值基礎可追溯。不過同一頁摘要段落先列 2026/2027 EPS 為 52.08/73.29 元，估值句與預估段落再用 74.30 元；目標價計算吻合 74.30，73.29 的差異仍是原文內部數字不一致，需保留為品質缺陷而非自行假定是哪個值正確。
- 原報告也列 4Q25 營收 NT$3.976B 對預估 NT$4.073B（-2.4%）、EPS NT$11.02 對 NT$11.54（-4.5%），營業費用 NT$286M 對 NT$204M（+40.2%），並提及研發費用增加。成長假設包括 GB300、Rubin、多個 ASIC 水冷專案，以及 2026 年伺服器相關營收倍增且占比超過一半；手機部門則納入 Apple 摺疊機預期。Rubin Switch Tray 設計變更減少連接器數量，原文同時提到原料成本抵銷部分影響，這種雙向因素比只列 AI 題材更可檢查。
- TWSE 月營收列顯示 2026 年 1–8 月累計 NT$10.812B、年增 49.8%；8 月單月 NT$1.528B、年增 29.1%。對照永豐 2026 全年營收估計，前 8 個月已達 61.3%，剩餘 4 個月需 NT$6.838B、月均 NT$1.710B，約比 8 月單月高 11.9%。TWSE 上半年獲利率列則顯示累計毛利率 33.13%、營業利益率 27.31%、稅後淨利率 22.29%；H1 毛利率已高於報告全年 31.04% 估計 2.09 個百分點。這些實績支持營收成長、且毛利率高於原估，但仍未驗證全年 EPS、下半年費用率或 28 倍估值是否合理；不是交易推薦。[TWSE 月營收 API](https://openapi.twse.com.tw/v1/opendata/t187ap05_L)、[TWSE 獲利率 API](https://openapi.twse.com.tw/v1/opendata/t187ap17_L)。
- 品質判讀：有原始報告、絕對預估、可重算目標價和明確營運驅動因素，優於只有評等/目標價標題；仍缺下行情境與營運資金品質分析。待 9 月營收和 Q3 財報公布後，核對營收、毛利率、營業費用、EPS、營業現金流及應收/存貨週轉；多季之前不評估預測能力或升格方向訊號。

### 財報品質因子資料缺口

- 目前 DataFlow normalized TWSE 財報欄位有資產負債表總額、損益及獲利率，但已抽查的 6805 Q2 raw payload 沒有營業現金流、應收帳款或存貨明細；現有 OpenAPI schema/catalog 也沒有現金流 statement source。若要判讀成長是否由現金支持，優先補齊原始財報期別與公告時間，建立營業現金流/淨利、應收與存貨增幅/營收增幅、應付與融資負債等背景欄位；先做風險/品質觀察，不納入多空方向分數。
- 新增免費官方研究入口：金管會督導的 MOPS「財務比較 e 點通」涵蓋上市、上櫃、興櫃及部分公開發行公司，提供四大報表、現金流量趨勢、應收/存貨週轉與現金流比率；TWSE 官方說明可由介面下載 Excel。站方標示每日更新一次，與公司最新 MOPS 申報約有一日落差；公司採用 IFRS 後資料大致自 2013 年起。這比每月付費的 N41 更適合作為第一個現金轉換人工核對來源，但公開說明未提供機器 API 或自動擷取授權；在取得明確資料使用方式前，只用官方 UI 做人工研究，不排入爬蟲/排程。TWSE OpenAPI 目前公開的財務表有綜合損益及資產負債，未見現金流 API；`t187ap08_L` 是董監事持股不足法定成數，不是現金流表。[MOPS 財務比較 e 點通使用說明](https://mopsfin.twse.com.tw/terms)、[金管會上線說明與 Excel 匯出](https://www.fsc.gov.tw/ch/home.jsp?dataserno=201607280002&dtable=News&id=96&mcustomize=news_view.jsp&parentpath=0%2C2)、[TWSE 資訊取得管道](https://shl.twse.com.tw/page/library/tips/6.html)、[TWSE OpenAPI](https://openapi.twse.com.tw/)。
- 資料口徑注意：財務比較站現金流量以各季累計值提供；單季 Q1–Q3 來自公司申報，Q4 以全年累計減 Q3 累計推算。站方列出的營業現金/流動負債、營業現金/負債、營業現金/稅後淨利等比率排除金融、保險、證券期貨、金控及異業合併等不適用產業；因此計算需先還原單季、標明推算期別，且不能把不適用/缺漏記為零。候選特徵先看四季滾動現金轉換、連續負營業現金流、應收/存貨週轉惡化，定位為獲利品質/風險背景，經 point-in-time 與成本調整後的多期驗證前不作方向訊號。[MOPS 期別及公式說明](https://mopsfin.twse.com.tw/terms)。
- 證交所網路資訊商店進一步確認：套裝二每月 NT$40,000，明列 S20 IFRS 資產負債表、綜合損益表、現金流量表、權益變動表；套裝二 Plus 每月 NT$50,000，也含 S20，另加其他公司活動檔。套裝五 XBRL 每月 NT$40,000，產品頁只寫 S21「IFRS 財報檔案」，沒有公開說明欄位、更新節奏及傳送格式。因此就現金流缺口而言，S20 套裝二的公開欄位承諾比 S21 更明確，但兩者都須向 TWSE 問清實際推送方式、歷史補檔/更正版本、上櫃涵蓋及欄位文件，不能僅憑「主動派送」字樣推定已有可接入 API。[套裝價格表](https://eshop.twse.com.tw/zh/mops/list)、[套裝二 S20 欄位](https://eshop.twse.com.tw/zh/mops/detail/000000007af53455017af57a15fe0007)、[套裝五 S21 欄位](https://eshop.twse.com.tw/zh/mops/detail/000000007af53455017af57c6e5c000c)。
- 套裝二 S20 與套裝五 S21 都是高成本付費線；相較之下，N41 為內用 NT$5,000/月、外部資訊用戶 NT$10,000/月，但只給較精簡的上市財務重點與近三期營業現金流/風險旗標。訂購條款把法人/團體內用者限定為董事、監察人、經理人或受雇人；外用可以傳給資訊用戶但須附來源與訂單文字且禁止用戶再散布；未經書面同意不得公開、重製或傳輸商品及衍生內容，違約可能被停權並處 NT$200,000 違約金。私人 Discord 的單人閱覽、摘要/衍生指標是否屬內用或外用仍不明；接入或對外發送前需書面問 TWSE。條款也不保證完整/正確或適合特定用途，故即使取得資料仍要做原始公告交叉驗證。[網路訂購使用條款](https://eshop.twse.com.tw/zh/shopping/finishOrder?show=true)。
- 成本效益順序：先用免費 MOPS 財務比較 e 點通 UI 對 6805、3702、3036 做人工現金流/營運資金比對；只有驗證出既有原始財報源確實缺欄、且這些資料能補足券商假設檢查時，再詢價/索取 S20、S21 與 N41 的樣檔、交付規格及 Discord 授權答覆。現階段不購買、不下載樣檔、不建立自動擷取。
- 新增較精簡的付費候選：TWSE「上市財務重點專區資料檔」N41 每交易日 07:50 產製，CSV/TEXT，自 2020-03-02 起；欄位列出近三年度稅後損益、每股淨值、負債比率、流動比率、近三個財報期的營業活動淨現金流量，以及連續負現金流/低淨值、設質、資金貸與、背書保證等旗標。內部使用 NT$5,000/月；對外傳輸 NT$10,000/月。產品頁列出的遞送方式是線上下載或電子郵件，未見自動 API 規格；需先確認持續交付/自動匯入方式、欄位說明及通知分享授權。公開 CSV 範例連結目前無法由瀏覽工具解析，故格式仍未核實。N41 僅覆蓋上市公司，期別欄可能是年度或季度，須按原檔與欄位說明驗證。可優先評估作財報品質/下行風險背景，不當成方向訊號；未購買、未下載、未加入排程。[TWSE N41 產品規格、遞送與價格](https://eshop.twse.com.tw/zh/product/detail/000000006f6a5e3401702cb5ca18003e)。

## 台股現貨與台指期法人流：官方端點與時段核對（2026-10-02）

- TWSE 三大法人買賣金額頁使用日報表參數 `type=day&dayDate=YYYYMMDD`；只傳 `date=YYYYMMDD` 會回傳頁面預設日期，不能據此標成歷史查詢結果。使用正確參數重查 9/29、9/30、10/1，回應的資料日期分別相符。[TWSE 查詢頁](https://www.twse.com.tw/zh/trading/foreign/bfi82u.html)。TWSE 網路資訊商店說明這個資料產品每日 14:50（不含綜合帳戶及鉅額）與 19:40（含綜合帳戶及鉅額）各產製一次；目前網頁 API 回應未指出所屬產製批次，故研究表先照錄頁面查詢值，未視作不同批次已完成 reconciliation，也不接入 production。[TWSE 資料產品與格式](https://eshop.twse.com.tw/zh/product/detail/d31c1b9570ae47058ec83a0bb1ffa419)。
- TAIFEX 的「依日期」TX 外資交易數包含盤後時段及次一一般時段；夜盤獨立表以交易量歸屬日查詢，例如 10/1 夜盤資料是 9/30 15:00 至 10/1 05:00。依 TAIFEX 說明，盤後交易併入次一一般交易時段；因此同一歸屬日的日盤 TX 淨交易口數可用「依日期總表 − 夜盤表」推回。未平倉部位僅列在一般交易時段表，夜盤淨交易不能代替日終未平倉變化。[TAIFEX 時段與併入規則](https://www.taifex.com.tw/cht/4/aHIntroduction)、[TX 依日期](https://www.taifex.com.tw/cht/3/futContractsDate)、[TX 夜盤](https://www.taifex.com.tw/cht/3/futContractsDateAh)。所有 TAIFEX 法人數字是多機構集合淨額，不代表單一法人或該類法人完整策略。[TAIFEX 表格定義](https://www.taifex.com.tw/cht/3/futContractsDate)。

| 歸屬日 | TWSE 外資及陸資現貨買賣超 | TAIFEX TX 外資合併淨交易 | 夜盤淨交易（同歸屬日） | 推回日盤淨交易 | 日終淨未平倉口數 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2026-09-29 | -NT$62.58B | -1,009 口 | -35 口 | -974 口 | -79,029 口 |
| 2026-09-30 | +NT$30.86B | +846 口 | +1,820 口 | -974 口 | -78,151 口 |
| 2026-10-01 | +NT$21.94B | -1,389 口 | -788 口 | -601 口 | -79,554 口 |
- 2026-10-02 延伸觀察：TAIFEX 依日期表的 TX 外資合併淨交易為 -802 口；同歸屬日夜盤為 -1,377 口，因此推回一般日盤約 +575 口。收盤外資 TX 淨未平倉為 -80,304 口，較 10/1 的 -79,554 再偏空 750 口。當日 TAIEX +0.25%，TWSE 股票上漲/下跌 483/506、淨差 -23；期交所大額交易人「所有契約」前十大空方部位增加 786 口、占比 +1.2 個百分點。讀作混合的跨市場風險背景：指數微漲但廣度仍略負，夜盤賣壓被日盤部分抵銷、日終空方部位集中度增加。TAIFEX 法人流與大額交易人資料彼此有重疊、不同合約口徑與參與者定義，不能當獨立確認；樣本僅一日，不推成方向訊號或交易條件。[TAIFEX 10/2 期貨與法人資料](https://www.taifex.com.tw/cht/3/futContractsDateExcel)、[TAIFEX 10/2 夜盤資料](https://www.taifex.com.tw/cht/3/futContractsDateAhExcel)、[TWSE 10/2 市場廣度](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)

2026-10-02 19:32 新來源盤點：TAIFEX OpenAPI 有 `GET /v1/OpenInterestOfLargeTradersFutures`，registry 目前沒有 TAIFEX 大額交易人來源。官方 OAS 直接回傳 1,386 筆結構化列，欄位包含 Date、Contract、SettlementMonth、TypeOfTraders、Top5/Top10 Buy/Sell、OIOfMarket；本次全部 1,386 筆的有效日期均為 2026-10-01。TX 全契約（SettlementMonth=999912、TypeOfTraders=0）10/1 數值 81,195/77,850/119,549，與官方查詢表相同；但同時 TAIFEX HTML 查詢頁已顯示 10/2 的 80,841/78,636/118,531。故截至 19:32 結構化 API 落後一個交易日，不能據它判斷 10/2 部位。data.gov.tw 中文資料頁標示更新頻率每月、英文頁標示每日，metadata 也與目前 API payload 時效不一致；先列為「高權威、即時性待驗證」的免費官方背景來源，不接入通知或多空分數。20:00 後再用同一官方 API 自然查詢驗證資料日；若仍落後，需在未來接入時保留有效日期並採 freshness gate，不把昨日部位貼成今日。[TAIFEX OpenAPI](https://openapi.taifex.com.tw/swagger.json)、[API 路徑](https://openapi.taifex.com.tw/v1/OpenInterestOfLargeTradersFutures)、[data.gov.tw 資料集 11337](https://data.gov.tw/dataset/11337)、[TAIFEX 10/2 查詢表](https://www.taifex.com.tw/cht/3/largeTraderFutQry)

2026-10-02 19:33 授權補核：資料集 11337 標示政府資料開放授權條款第 1 版、免費。條款允許重製、散布、公開傳輸及改作/衍生服務，但使用時必須明確標示提供機關與資料集名稱。這降低未來將此 OAS 作為 Discord 市場背景的授權不確定性；仍須先解決 API 資料落後與收盤日更新時點，且不把匿名集中度數據轉成交易方向。[政府資料開放授權條款](https://data.gov.tw/license)

這三天已足以驗證「拆開看」會改變解讀：9/30 合併 TX 淨交易為正，但夜盤為 +1,820 口、推回日盤為 -974 口；若只拿合併值與現貨同日比較會把時段混在一起。9/29 至 10/1 現貨法人流連續兩日買超，TX 日盤口數淨額則三日皆為負。這是短樣本的跨市場分歧描述，不證明法人看空、現貨流無效或任何反轉；TAIFEX 外資還可能透過其他契約、選擇權及現貨對沖，先保留作背景特徵，不加多空分數或通知門檻。

## 上市/上櫃市場廣度：來源授權、母體與資料新鮮度（2026-10-02）

- 單次核對證交所 2026-10-01 收盤 `MI_INDEX` 報表的「漲跌證券數合計」：股票欄上漲 422（漲停 23）、下跌 551（跌停 2）、持平 107、未成交 1、無比價 1。上漲/下跌淨差 -129，是指數與大型權值股可能掩蓋個股普遍走弱時有用的反向背景；單日數據不構成反轉訊號或預測力證據。[TWSE 10/1 大盤統計](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261001&response=html)。
- 不能將目前可查的網站頁面直接排入自動抓取：TWSE 使用條款禁止未經其同意以自動裝置、指令碼或擷取程式下載網站資料；條款列明的例外是經政府資料開放平台授權的資料。[TWSE 使用條款](https://www.twse.com.tw/zh/terms/use.html)。
- 政府資料開放平台的免費「集中市場漲跌證券數統計表」(OGDL v1) 更新頻率標示不定期；其 `/opendata/twtazu_od` API 與 CSV 在 2026-10-02 都只有 2026-06-05 兩列（整體市場/股票），距本次查詢 119 天，不適合日線 freshness。TWSE 使用條款禁止未經同意以程式擷取其一般網站頁面，因此未將 `www.twse.com.tw/exchangeReport/MI_INDEX` 直接排入自動化。[政府資料集 18409](https://data.gov.tw/dataset/18409)、[TWSE 使用條款](https://www.twse.com.tw/zh/terms/use.html)。
- 找到更新且有開放資料授權的個股來源：TWSE 個股日成交資訊列每日漲跌價差（資料集 11549），TPEx 上櫃股票行情列每日漲跌（資料集 11370），兩者均標示每日更新、免費及政府資料開放授權第一版。已新增 `twse_equity_quotes_daily`、`tpex_equity_quotes_daily` 來源定義，收盤排程擷取官方 OpenAPI 個股列，按目前證券主檔 CFI `ES*` 篩選，逐筆存日期與官方 `Change`；收盤摘要新增獨立市場背景欄，只接受與掃描 `as_of_date` 同日的資料，缺資料時不沿用前一日。來源不參與候選分數、門檻、排序或個股方向通知。[TWSE 個股日成交資料集](https://data.gov.tw/dataset/11549)、[TPEx 上櫃股票行情資料集](https://data.gov.tw/dataset/11370)。
- 2026-10-02 15:10 台北時間官方 OpenAPI 快照仍為資料日 10/1：目前 CFI `ES*` 主檔交集 TWSE 1,051 檔，Change 計上漲 411／下跌 533／平盤 107；TPEx 887 檔，331／417／114，另 25 檔未分類（Change 空值或非數字）。同日 TWSE `MI_INDEX` 股票欄為 422／551／107，並有未成交/無比價列；這兩組母體不同，不能互相當成精確對帳。早先舊表以 raw close 對前收簡算得到 411／533／106，證明不應以舊表代替官方漲跌欄。
- 15:22 時應用 DB 的 source registry 仍有 29 個來源；其後 15:15 logical 自然盤中 run 在 15:30:15 成功完成 catalog bootstrap，將 `twse_equity_quotes_daily`、`tpex_equity_quotes_daily` 註冊為核准啟用來源。目前總數 31，但兩個來源仍沒有 `source_runs` 或行情列，故只是已註冊，尚未證明收盤擷取。待 10/2 20:00 自然收盤 DAG 驗證是否成功、回傳資料日是否為 10/2、來源接受/隔離筆數及 Discord 摘要欄是否只顯示同日廣度；目前程式接線尚未 runtime 驗證。即使新來源成功，`ES*` 仍是以目前主檔定義的個股截面而非歷史 point-in-time 證券清單，25 筆 TPEx 未分類要保留為缺漏，先累積多日資料與 TPEx 完整性後再考慮回測；目前不作方向分數或通知條件。

## 下一步

1. 10/2 20:00 台北時間檢查收盤 DAG 的新 daily child run ID 是否避開既有 intraday run；UTC logical date 將是 10/1 12:00Z。確認 trigger/verify tasks 成功；核對 `market_signal_runs.as_of_date`、個股價格資料日與三大法人資料日均為 10/2，若仍停在 10/1 則記為 stale/partial；查看 `market_signal_outcomes` 首批結算/UPSERT。另確認注意/處置來源在 registry 之外也確實完成 mapped ingestion、sentinel 被略過且有效期間正確；財測達成 raw feed 是否完整保留欄位、variance feed 是否入庫且首次通知只出現一次；候選卡有合格個股時核對官方背景欄與來源連結。期別標註、同批更正合併、搜索事件中性表述仍等未來合適的自然公告做 runtime 驗證。
2. 找到 TWSE 10/1 的 8,740.46 億、新光盤後的 8,388.73 億、中央社的 8,374.31 億，以及 KGI 對 9/30 的 8,756.73 億各自欄位/範圍差異；統一報告評估的量能基準前，所有比較註明資料來源及口徑。
3. 確認 10/1 TWSE 基準資料補齊後再重算第 3 日相對績效；累積多個獨立訊號日後，補上 point-in-time 股票清單、交易成本與股利/公司行動調整，再判斷是否能支撐任何訊號品質結論。
4. 保留新聞為可點回原文的背景證據；只有公司主體與可量化影響被核實後，才研究能否升級為方向訊號。
5. 持續追蹤報告修正/下修，而非僅收目標價；若沒有合法可檢視的原文，就記成「標題線索、未核假設」，不評分。
6. 10/8 仁寶公布 9 月月營收後，核對中信投顧「伺服器轉折」是否有公司營運數據延續支持；另等晟銘電可核對的 9 月營收與 L11 整機櫃公開資料，暫不把 10/2 目標價標題升格為候選訊號。
7. 時段核對已有 9/29–10/1 的現貨法人流、TX 夜盤/日盤淨交易及日終未平倉快照（見上表）；後續要延長成多週序列，保留公布時間與資料批次，並對齊同日及後續 1/5/10 個交易日相對報酬，再判斷有無增量解釋力。新增的 TWSE/TPEx `ES*` 日線廣度會從 10/2 20:00 起等待自然 run 驗證，之後保存日資料日、上市/上櫃各自有效列與 Change 缺漏；未累積多日、未解決母體與 TPEx 缺漏前不改成方向訊號或通知條件。[TWSE 個股資料集](https://data.gov.tw/dataset/11549)、[TPEx 行情資料集](https://data.gov.tw/dataset/11370)。
8. 若要將 TWSE TWT93U 用於自動採集，先取得 TWSE 認可的機器存取方式/書面同意，或由使用者明確授權 NT$1,000/月內用訂購；未授權前不以腳本抓取公開查詢端點，也不排入 production。授權後先以 1 日與 TPEx 同口徑及 TWSE 人工查詢交叉核對，再依 23:30 產製時間設計晚間/隔日採集。欄位用實際借券賣出餘額及流量，不混入可借額度、一般借券餘額或融券；信號仍須按流通股數、成交量正規化並用 1/5/10 交易日相對報酬檢驗。
9. 季報更新後確認 2026 Q2 舊快照是否仍保留；拿到兩個以上已公告季度後，先按報表季度與產業計算/比較流動比率和營運資金變化，檢查公告時間、分母為零、產業例外及缺漏覆蓋，再決定是否只加財務風險背景。未完成同業與未來結果驗證前不納入多空方向。
10. 取得 `t187ap15_L` 的 TWSE 欄位定義或更多跨公司配對樣本，確認其預測欄為當季、累計或其他期間；確認前保留 raw-only。以 `t187ap16_L` 的明確當季/累計配對追蹤後續出表、公司解釋與價格反應，分開記錄正/負偏差與財測序號，並建立多季樣本後才評估是否有通知以外的預測價值。
11. 追蹤 3702 第 3 季與 3036 第 3 季官方展望的實績兌現：分別保留財測範圍/假設，對照營收、毛利/營業利益率、EPS、營業現金流、應收/存貨及融資負債；現金轉換只先作獲利品質和風險背景，累積多季後再判斷能否成為候選特徵。
12. 後續券商評分固定保存報告日期與原始版本、絕對 EPS/營收/利潤率估值假設、風險條件、目標價期限及修正歷程；將同一期間實績與 TWSE 基準總報酬對齊。若只能取得媒體摘要，就只評摘要中有明確數值且可核對的部分；3702/3036 的 5/20–10/1 僅是中途價格表現。
13. 6805 富世達：待 9 月營收及 Q3 財報公開後，核對永豐 2026 全年營收/毛利率/EPS 假設與已實績、下半年費用及營業現金流；營運資金明細目前未進 TWSE normalized source，故只作待補的獲利品質風險背景，不以 NT$2,100 目標價作交易訊號。
14. 付費來源候選 N41：先取得完整 A49 欄位說明與 CSV 範例、確認交易日 07:50 檔案如何穩定自動交付，再問清內用方案能否供私人 Discord 通知/儲存使用者閱覽；若無自動授權交付或價格不合成本效益，就維持人工核查缺口。若之後獲准訂閱並可合法自動匯入，先核對旗標口徑與 TWSE 公開財報，再把它作上市股財報風險背景，和既有訊號按 1/5/10 日相對報酬分層檢視，暫不加方向分或通知門檻。
15. TAIFEX 個股期貨延伸：利用已授權的期貨標的與每日成交量資料，只建立可回溯的參與度背景；先取得/確認合法個股 OI 與大額交易人資料接口，處理契約更換、單雙月及市場流動性，再看是否對現貨/法人流增加資訊。成交量本身沒有方向，不推成買賣訊號。
16. 財報現金轉換候選先以 MOPS 財務比較 e 點通人工匯出核對 6805、3702、3036 等已追蹤標的，保存報表公告日、報表期別、單季/累計及現金流/淨利、應收/存貨週轉口徑，對照原始季報。若人工匯出可穩定補出缺欄，將其用於券商假設檢查；自動化前另確認正式 API/下載授權，不探測或逆向工程隱藏介面。
17. TWSE 付費財報資料需先比較 S20（套裝二 NT$40,000/月，完整四大報表含現金流）、S21（XBRL 套裝五 NT$40,000/月，但公開商品頁未列詳細欄位/交付節奏）與 N41（NT$5,000/月內用、NT$10,000/月資訊用戶，較精簡且上市股限定）。先向 TWSE 索取樣檔、欄位/更正及歷史補送規格、可自動接收的協定與上櫃覆蓋，並書面確認 owner-only Discord、訊號摘要或衍生指標的內/外用授權；在此之前不購買、不排程、不把付費資料送 Discord。若免費 MOPS 人工核對能補足需求，維持人工路線。
18. 追蹤券商法人流與 TWSE BFI82U 當日的資料批次：保存報告發佈/更新時間、14:50 或 19:40 批次、是否含綜合帳戶與鉅額、分項原值及四捨五入總計；若券商未揭露批次，記 `unknown` 並暫不評量差額。DataFlow 現有來源是否保留 `snapshot_scope` 亦待檢查，未查明之前不可把不同批次串成同一日序列或用於方向分數。
19. 逐股法人流的排程先做自然資料驗證：目前 legacy collection 約 16:45 台北時間完成，早於 TWSE 官方 TWT86UC 18:00 與 TWTAIUC 20:00 產品檔產製。核對當日 API 可用時間、資料筆數/欄位與正式檔的版本差；確認官方端點可自動使用的授權，詢價內用 NT$1,500/月或資訊用戶 NT$3,000/月方案的實際推送/API、歷史補檔、更正規則及 Discord 使用授權。驗證前不把日期相同等同批次相同，也不以 16:45 快照升級方向分數。
20. 10/15 台積電 3Q26 法說後，按公司報告口徑對照市場報導轉述的摩根大通數字：美元營收對預估 US$45.8B（公司指引區間上緣）、毛利率對 66.8%；若公司同日給出 4Q 指引，再比較其季增 11% 預估。另把目標價 3,200→3,300 獨立於營運 forecast 評分，因原始 JP Morgan EPS/估值模型尚不可見。
21. 10/5 後續交易日追蹤凱基 10/2 報告時，先比對晨會報告可能參照的前一交易日（10/1），不可因目錄日期相同便用 10/2 收盤宣稱命中。未取得原文、發布時分、明確觀察窗與失效條件前，維持「官方題名線索、不評命中」。10/2 指數上漲但上漲家數少於下跌家數，且臺灣50/半導體偏弱、中小型股/電子零組件偏強，暫保留為市場背景，等新增 TWSE/TPEx 日線廣度來源自然入庫後再核口徑，不加入個股分數。
22. 10/2 19:44 台北時間自然重試已成功：TWSE TWTB4U 請求回應終於含當沖成交股數與買賣金額欄，DB 當日 1,228 列中 972 列可連到上市主檔、256 列未分類；同一輪 TPEx 統計回應為空，collector 未檢查兩市列數而仍標成功。待自然 after-close 時間後唯讀再核官方欄位/資料日；保留分市場 row count、未分類代碼與 T+2 修訂，先視為上市部分覆蓋資料，不接訊號或方向分數，未確認授權與完整性前不手動重跑。
23. 10/2 after-close 自然 run 要確認 TWSE 及 TPEx 指數來源 `source_runs` 的接受列數、最大 `effective_date` 和資料新鮮度，並對照 `market_signal_runs.benchmark_status/source_status`。10/1 20:00 那輪 TWSE source task 雖 HTTP 200 且 20/20 接受，TAIEX 最新仍為 9/30；若今日仍落後 `as_of_date`，保留 partial 狀態，不把 task success 當當日指數齊備，也不要手動呼叫 endpoint。
24. 10/2 日線行情 8 個無當日列代碼的既有價格資料日已查明：兩檔最後資料在 4 月，三檔在 9/22–9/29，兩檔在 9/30，3311 為 10/1。after-close 核准 TWSE/TPEx OpenAPI ingestion 後逐檔比對;分類為 no-row/停牌或舊主檔、官方 source coverage、legacy 抓取遺漏，保留每市場合格交易母體，勿只按靜態主檔總數改 70% 覆蓋門檻。

本頁是本次盤點與後續驗證清單，持續追蹤；其中的觀察不代表投資建議或已證實的預測能力。



## 全球宏觀日曆與新聞時點核對（2026-10-02）

- 新光與國票晨報都把美國 9 月就業報告列為今晚風險事件。BLS 的 Employment Situation 官方排程目前列 10/2 08:30 ET（台北 20:30），8 月官方報告也寫明同一發布時間；AP 10/1 報導則寫「下週」發布，形成同日來源衝突。該 AP 文章並引 FactSet 預估新增 90,000 職位、失業率 4.1%，但日期句不能覆蓋官方時刻表。[BLS 排程](https://www.bls.gov/schedule/news_release/empsit.htm)、[BLS 8 月官方報告](https://www.bls.gov/news.release/archives/empsit_09042026.htm)、[AP 10/1 報導](https://apnews.com/article/jobs-unemployment-claims-layoffs-6f2362a2320d739dc78210a899ce4edf)
- 在官方實際發布頁出現前，不把「今晚公布」當成已確認的 catalyst。20:30 後查 BLS 當期發布頁與更新排程，保存實際發布時間；若沒有報告，以 BLS 最新公告修訂時間並標註資料延遲/未發布，不用 AP 摘要推定。
- 10/1 after-close `source_extension_collect_after_close` 六個 mapped ingestion 都成功，但 verify task 曾因 `trigger_market_signal_notify=skipped` 將整個 DAG 標紅；執行中的舊版本不接受 idempotent trigger 的 `skipped`。`dags/source_extension/dag.py` 在 10/2 18:50 台北後的現行版本已明確接受 `success` 或 `skipped`；等 10/2 20:00 自然 run 驗證新行為，期間不清除/重跑既有 run。
- 19:44 台北再次直接查 TAIFEX OAS，同樣回傳 1,386 列且唯一 Date=20261001；官方 HTML 查詢頁已有 10/2 數值，因此 API 新鮮度落後的狀況延續，不可供當日通知使用。[TAIFEX OpenAPI](https://openapi.taifex.com.tw/v1/OpenInterestOfLargeTradersFutures)
- 19:15 台北的 `market_signal_notify` intraday 自然 run 於 19:32 完成：`source_status=fresh`、`source_collection_status=complete`、`candidate_count=0`；notify task 回 `sent=0, failed=0`。當時無可發送事件；這是成功空掃描，不是市場看多/看空結論。

## 現行收盤訊號邏輯與驗證邊界（2026-10-02）

- 唯讀檢視 `market_signals._load_daily_candidates`：股票要有至少 3 個非零方向家族才形成候選；方向是家族 ±1 的合計；排序分數是同方向家族數，程式明確註記「不是報酬或上漲機率」。價格家族是個股近 5 交易日絕對漲跌達 ±8%，雖另存對 TAIEX/TPEx 的相對報酬但未用來判價格家族；量價家族要求單日量至少是前 20 日均量 2 倍且單日絕對漲跌至少 3%；法人家族要求近 5 筆至少 4 日同向且累計淨股數至少 ±100,000 股；月營收家族用年增率 +30%/-20% 或相鄰月年增率變化 ±15 個百分點；季營業利益率變化達 ±3 個百分點；董監持股變化達 ±5%。借券、融資/融券、股利、注意/處置及廣度都是背景欄或上下文，不直接形成方向家族；媒體新聞只展示最多兩則匹配，不計分。
- 目前待證的是門檻的增量預測價值，而非把排序分誤讀為勝率。價格與量能來自同一行情序列、可能高度相關；法人固定 10 萬股門檻也未按流通股數/成交額正規化。`market_signal_outcomes` 已設 1/5/10 交易日、收盤價及次日收盤兩種入場基準，績效扣 TAIEX/TPEx 基準，但樣本與成熟 outcome 尚不足；先保留現行規則，累積資料後按家族、母體及市場分層校準，再考慮改閾值。[訊號建構程式](../dags/source_extension/market_signals.py)
- 19:44 runtime 細節：Airflow 自然任務由 16:57 起多次缺三個當沖統計欄，最終於 19:44 成功；DB 的 `當沖資訊` 2026-10-02 有 1,228 列、1,228 個代碼、統計欄 NULL=0；上市主檔 join 972 列、未匹配 256 列。未匹配樣例以 00400A、0050 等 ETF 為主，先標未分類，不能用靜態股票主檔漏列便推論交易量為零。沒有 TPEx 匹配列；同一官方 TPEx endpoint 當時 `tables[1].data` 為空。程式把這個空陣列轉成合法空 DataFrame，因上市 d1 非空仍整體寫入成功；因此目前 Airflow success 只證明有資料寫入，不是上市/上櫃雙市場完整。這個 day-trade table 尚未被 market_signals 使用。[TWSE OpenAPI](https://openapi.twse.com.tw/)、[TPEx 日當沖查詢](https://www.tpex.org.tw/www/zh-tw/intraday/stat)

## TPEx 當沖統計：授權的市場總量背景與個股缺口（2026-10-02）

- 官方資料集 11627「上櫃股票現股當沖交易統計資訊」標示每日、免費、政府資料開放授權第 1 版，但範圍是每日市場彙總；TPEx 官方 OpenAPI `/v1/tpex_intraday_trading_statistics` schema 僅有日期、總成交股數/占市場比、總買進金額與總賣出金額/占市場比，不含證券代號。官方 OpenAPI 於 19:50 回傳 1 列，資料日期 1151001（2026-10-01），成交量 621,642,000 股、占比 22.83%、買進 NT$121.13B、賣出 NT$121.44B。這是高權威且授權清楚的市場參與度背景，但目前落後一日、僅 1 列，需等 10/2 自然更新並累積序列；當沖比率不能單獨解讀為方向。[data.gov.tw 11627](https://data.gov.tw/dataset/11627)、[TPEx OpenAPI schema](https://www.tpex.org.tw/openapi/swagger.json)、[TPEx OpenAPI data](https://www.tpex.org.tw/openapi/v1/tpex_intraday_trading_statistics)
- 另一個官方資料集 137680 僅是上櫃當沖資格主檔；個股成交量/金額未列在免費開放資料欄位。legacy collector 呼叫的 `/www/zh-tw/intraday/stat` 個股明細路徑未見於官方 OpenAPI schema，且 19:44 查詢回應無個股列。未確認個股路徑授權、欄位與市場覆蓋前，不把它當已核准資料源；開放資料總量可先作市場層級活動背景。[data.gov.tw 137680](https://data.gov.tw/dataset/137680)

## 2026-10-02 after-close run, digest delivery, and lineage follow-up

- Natural `source_extension_collect_after_close` run `scheduled__2026-10-01T12:00:00+00:00` started 10/2 20:00 Taipei. The TPEx daily quote map task failed twice on truncated HTTP streams; the collection verifier and parent DAG ended failed. TWSE daily quote/index normalized rows observed through 10/1, while TPEx index had one 10/2 row. The daily signal child captured `as_of_date=2026-10-02`, `source_status=partial`, universe 1,946, valid price rows 1,938, fresh flow rows 1,891, and 50 candidates. Do not treat this run as a complete cross-market scan.
- The daily Discord digest exhausted retries and was not delivered (`market_signal_runs.notified_count=0`). Airflow delivered the short operational failure notice. Discord returned `40005` / `Request entity too large`. A read-only reconstruction produced 9 embeds and 4,796 total embed characters; no individual field exceeded 1,024 characters, but the JSON request encoded to 23,078 bytes. Discord documents 1,024 characters per field value and 6,000 characters total per embed: https://discord.com/safety/using-webhooks-and-embeds.
- `dags/source_extension/market_signals.py` now packs daily embeds into requests under the existing 10,000-byte guard and assigns a stable dedupe key per part. Successfully sent parts are skipped on retry; `notified_count=1` is recorded only after all parts are sent. This code was not manually triggered on 10/2; the failed run exhausted retries before the change was active.
- Root cause for mapped ingestion index 9 is the TPEx daily quote response stream, not the signal schema: the same roughly 4.67 MB body raised `ChunkedEncodingError` twice, at different offsets (about 0.70 MB and 4.19 MB read). Added three bounded stream-read attempts for `tpex_equity_quotes_daily` only, with 1s/2s backoff; schema, HTTP status, size-limit and JSON failures remain terminal. The next weekday natural after-close run is 10/5; only it can confirm the collector retry, TPEx context-source registration, and—if it produces a non-empty daily digest—the Discord batching change. No DAG or notifier was manually run.
- Confirmed a lineage collision: `source_extension_collect_intraday` and `source_extension_collect_after_close` both used logical `run_id=scheduled__2026-10-01T12:00:00+00:00`. `source_runs` had no `dag_id`, and collection-state lookup filtered only by `dag_run_id`, mixing rows across the two DAGs. Added nullable `source_runs.dag_id` and `market_signal_runs.source_dag_id` via additive schema migration, pass the source DAG ID in trigger conf, scope the source-status query by DAG ID plus run ID, and include the source DAG in future child run IDs. Existing rows remain unchanged; the 10/2 20:30 natural intraday collection applied migration 8 and confirmed DAG-scoped source status (see the verification entry below). No tests were run.
- After 20:30 Taipei, recheck the official BLS release page and schedule to resolve the broker catalyst date conflict; until then, retain the conflict as unconfirmed.
- 10/2 20:15 Taipei 自然 intraday run `scheduled__2026-10-02T12:00:00+00:00` finished successfully. The scan was `ready`, `source_status=fresh`, `as_of_date=2026-10-02`, `candidate_count=0`, `notified_count=0`; the notification ledger had no rows for that run. It was a successful empty scan, not a market direction call. This run began before the DAG-ID migration code was active, so it does not verify the new lineage fields.
- 10/2 20:30 Taipei natural intraday run (`source_extension_collect_intraday`, logical date `2026-10-02T12:15:00+00:00`) applied schema migration 8 and completed. Five mapped sources all recorded with `dag_id=source_extension_collect_intraday` and succeeded. Its child signal row persisted `source_dag_id` and exact source run ID; result was `ready/fresh`, 10/2 as-of, 0 candidates, `notified_count=0`, and no notification rows. This confirms the additive lineage migration and DAG-scoped status path operate on the natural run; empty scan remains non-directional.
- At 20:30 Taipei, BLS official schedule lists September Employment Situation for Oct 2 at 08:30 ET. The current BLS release page still showed August results and a Sep 4 last-modified date, so the scheduled catalyst time is confirmed but September results were not yet visible on the release page. Recheck for delayed publication before recording actual payroll figures.
- 10/2 20:31 Taipei direct read-only calls to both official endpoints confirmed they remained stale: TPEx day-trade aggregate returned one row dated `1151001` (10/1; 621,642,000 shares; 22.83% of market), and TAIFEX large-trader OAS returned 1,386 rows all dated `20261001`. Treat both as prior-session context, not 10/2 catalysts or directional evidence. Recheck only on a later natural update.
- BLS 20:35 Taipei recheck: the Employment Situation schedule and newsroom still listed Sept 2026 for Oct 2 at 08:30 ET, but the latest-release page remained August 2026 (last modified Sep 4) and newsroom's latest national releases ended Sep 30. No September payroll figures were visible in the official pages returned at that time. Keep the catalyst as scheduled, with results pending; this does not establish cancellation or publication delay. AP's "next week" wording remains a source conflict until an actual BLS release appears.
- Direct no-cache requests from the Airflow container to BLS returned HTTP 403, so runtime verification used the official BLS pages/search results; do not interpret the container block as a BLS outage.

- BLS 20:41 Taipei live page recheck: BLS current Employment Situation summary still identifies August 2026 (USDL-26-1435, dated Sep 4); no September figures were visible. The official schedule/newsroom continues to list the Sep report for Oct 2 at 08:30 ET. Record only the observed page state; this does not establish cancellation or an official delay. Sources: https://www.bls.gov/news.release/empsit.nr0.htm and https://www.bls.gov/schedule/news_release/empsit.htm

### 2026-10-02 20:50 凱基晨訊與南電題材交叉核對

- 凱基投顧官方 10/2 晨訊以 48,601 點前高、約 8,600 億元的 20 日均量及 47,578 支撐描述短線情境；有具體條件，較一般「偏多」標題可檢驗，但「短線」沒有固定交易日數，也沒有進場價、失效條件或明確用收盤/盤中判定。這是公開盤勢晨訊，不是有分析師模型、EPS 推估與估值的個股研究。[凱基 10/2 晨訊](https://moneyschool.kgi.com.tw/articles/stockcheck-stockbrief-advisory-20261002-4e5e0dbba7094323972b5ac938421c3c-1263)
- TWSE 官方 10/2 指數頁列加權收 48,475.74、上漲 122.25 點（+0.25%），低於 48,601 前高，故當日收盤沒有突破。成交額的兩個官方市場摘要快照不一致：一般股票 863,798,395,102 元（8,637.98 億）與英文頁 Stock 868,804,940,498 元（8,688.05 億）；尚未查明口徑/快照差異。兩者都僅比報告約 8,600 億門檻高約 0.44%–1.02%，只能說邊際超過近似門檻，不能把它當成明確放量或突破確認；若評估後續多日需先固定 TWSE 口徑及觀察窗。[TWSE 中文 10/2 摘要](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)、[TWSE 英文市場摘要](https://www.twse.com.tw/en/exchangeReport/MI_INDEX?response=html)
- 同篇將 8046 南電列焦點股，提出 ASIC 出貨可能超越 GPU、ABF 需求倍數提升、供給吃緊至 2030、下半年漲價 15%–20%、1.6T 交換器載板 Q3 放量。南電 2026 Q2 合併財報（8/6）支持近期基本面改善：Q2 營收 NT$13.575B、年增 41.7%，營業利益 NT$2.899B、EPS 3.50；H1 營收 NT$24.753B、年增 37.2%，EPS 5.53。H1 營業現金流 NT$2.997B，相對淨利 NT$3.570B 約 84%；應收與存貨增加合計占用約 NT$3.310B，顯示擴張同時有營運資金需求。[南電 2026 Q2 合併財報](https://www.nanyapcb.com.tw/nypcb/images/InvestorRelations/FinancialReport/115Q2.pdf)
- 南電官網列 7、8 月營收 NT$5.440B、NT$5.353B；兩月平均比 Q2 月均約高 19.3%，支持營收動能延續到 8 月，但 9 月數字當時尚未公布。公司 6/12 簡報確認資料中心/AI/HPC 與伺服器、交換器帶動高階 IC 載板，且開發客製 AI 伺服器晶片及新世代高速交換器/路由器載板；簡報未證實 KGI 所列 1.6T Q3 量產、15%–20% 漲價或供給緊俏至 2030 的量化假設。[南電月營收/法說資料](https://www.nanyapcb.com.tw/nypcb/chinese/InvestorRelations/FinancialReports)、[南電 6/12 營運簡報](https://www.nanyapcb.com.tw/nypcb/images/InvestorRelations/FinancialReport/NYPCBCH20260612.pdf)
- 品質分類：凱基指數情境＝「中等品質、可驗條件但期限/基準未定」；南電個股段落＝「有公司實績支持的產業方向，但低可執行訊號」；目前不進 market_signals 分數或自動通知。等 10/5 之後收盤、10/8 九月營收及後續 Q3 財報，再按預先固定的 1/5/10 交易日相對報酬、指數/量能口徑、營收/毛利/CFO 與報告假設逐項回填；不得把單日或事後已知財報倒填成報告當日可知訊息。
### 2026-10-02 20:55 TWSE 成交額快照覆核

- 以搜尋結果再次對照 TWSE 官方頁：中文一般摘要 MI_INDEX?amp=&response=html 與英文預設摘要均回傳一般股票成交額 NT$868,804,940,498、成交股數 4,785,531,792、3,688,063 筆；帶日期/篩選參數的中文、英文摘要則均回傳 NT$863,798,395,102、4,772,771,103 股、3,688,015 筆。兩組同標 10/2，數值差 NT$5,006,545,396（約 50.07 億）、12,760,689 股及 48 筆交易。這表示差異隨頁面/查詢樣式出現；未取得正式修訂時間與機器介面口徑前，不推定哪組是最終批次，也不消掉較早快照。
- 兩組成交額均高於凱基約 8,600 億元的近似 20 日均量門檻；較低組高 0.44%，較高組高 1.02%。最新共同摘要可作當下參考值，但只屬邊際越過近似門檻，不能代替日後實際突破或確認趨勢。券商評估表須保存 endpoint/query 樣式、擷取時間與數據版本，不只保存「成交額」一欄。[TWSE 中文預設摘要](https://www.twse.com.tw/exchangeReport/MI_INDEX?amp=&response=html)、[TWSE 英文預設摘要](https://www.twse.com.tw/en/exchangeReport/MI_INDEX?response=html)、[TWSE 日期篩選摘要](https://www.twse.com.tw/exchangeReport/MI_INDEX?date=20261002&response=html&type=ALLBUT0999)、[TWSE 英文篩選摘要](https://www.twse.com.tw/en/exchangeReport/MI_INDEX?_=2891&date=&response=html&selectType=)

## 券商報告共用品質判讀欄位

- 每份報告分開記錄：原始全文/摘要/標題線索、機構與分析師、發布及資料截點、初次覆蓋或更新/修正、引用來源及口徑、營收/毛利/EPS 假設橋接、估值方法/目標期限、風險與失效條件、後續修正、實績誤差及相對基準報酬。拿不到原文就不推定模型細節；搜尋摘要與媒體轉述只能支持可核實的數字，不能替代原報告。
- 分為三種用途標籤而不給未校準的單一分數：A「高品質研究」需能追到原始版本與時點、主要數據來源、可重算的公司獲利/估值假設、風險條件及修正歷程；若要稱為可執行訊號，另須有期限、觸發、失效/退出條件及獨立後續驗證。B「條件式市場/產業背景」是來源可靠且有可核數字或條件，但模型橋接、期限、觸發或風險不完整。C「標題線索/未核假設」適用於會員牆只露出評等/目標價、媒體二手摘要或缺來源與期限的推測；只追蹤，不進方向分數。
- 初次覆蓋不是低品質的充分證據；但缺少先前版本時，無法評估預估是否上修/下修，也沒有可核對的歷史準確度。必須標成「無修正歷史」，等到後續預估與實績才評估，不讓初次報告用一個目標價直接變成強訊號。
- 目前凱基 10/2 指數晨訊依此屬 B；南電段落的公司需求方向有實績支持，但 1.6T Q3 放量、15%–20% 漲價與供給至 2030 年缺量化公司依據，故仍是 B 背景/低可執行訊號。這些研究品質標籤不等於投資報酬機率，也不取代訊號結果回測。
### 2026-10-02 21:03 BLS 9 月就業數據 vs FactSet 事前預期

- BLS 官方就業報告頁已更新為 USDL-26-1549，標示 2026-10-02 08:30 ET 的發布時點。非農就業增加 29,000；失業率 4.2%。這更新並取代 20:41 當時仍看到 8 月資料的頁面狀態；先前頁面快取落後不等於官方延遲或取消發布。
- FactSet 10/1 報告的 9 月非農中位預估為 +90,000（27 個估計，範圍 60,000–130,000），失業率中位數 4.1%（28 個估計）。非農驚喜 = 29,000 − 90,000 = -61,000（相對預期低 67.8%）；失業率比預期高 0.1 個百分點。這是明確的宏觀負向就業驚喜，但 FactSet 中位數是單一調查基準，不能當成市場一致的唯一預期。[FactSet 9 月預估](https://insight.factset.com/total-nonfarm-payrolls-for-september-2026-are-projected-to-rise-by-90000)
- BLS 同時報告平均時薪月增 0.1%、年增 3.0%；本次核對的 FactSet 預覽沒有工資預估，故只記實際值，不標記工資驚喜。7 月由 +21,000 修正為 -10,000，8 月由 +162,000 修正為 +133,000，兩月合計下修 60,000。資料以 BLS 原始報告為高品質實績來源；FactSet 對樣本數、範圍及中位數定義清楚，適合作為透明的 B 級預期比較基準。[BLS 9 月就業報告](https://www.bls.gov/news.release/empsit.nr0.htm)
- 訊號處理：列為高品質「宏觀事件驚喜」候選，先供新聞/市場背景判讀，不併入個股方向家族或直接推買賣。就業放緩可能降低利率壓力，也可能提高需求轉弱疑慮，方向須由債券、美元/利率預期、美股及台股後續價格反應確認。10/2 台北晚間尚未回填市場反應；下一台股交易日 10/5 再核對，分開記錄實際反應與事後修訂。

### 2026-10-02 21:15 環宇-KY（4991）：多家族動能與 TPEx 官方注意交易風險

- 自然 after-close 掃描的 50 檔候選中，4991 列於四個同向資料家族的偏多候選：價格、量價、法人流及月營收；本次排序分數是同向家族數，不是報酬或上漲機率。該輪收集仍是 `partial`，不得當成完整市場掃描。候選價位 NT$534、近 5 交易日 +19.33%，高於前 20 日高點 NT$530；相對 TPEx 同期 +16.15 個百分點。短線漲幅已延伸，需將追價回撤風險和官方監理背景一起呈現。
- TPEx 10/2 官方「公布注意股票資訊」列出 4991，收盤 NT$534、本益比 166.36、股價淨值比 14.54（為所屬產業 2.59 倍）、週轉率 7.08%；元大當日買進成交額 NT$1.615B（占個股成交額 36.27%），單一投資人買進 NT$640M（占 14.39%，公告第六款）。官方處置清單共 29 列，當日沒有 4991。這是高權威的交易異常/集中度及執行風險資訊，不是方向訊號；公告中的券商與「單一投資人」欄位不能直接等同系統法人買賣家族的分類或獨立因果證據。[TPEx 上櫃注意資訊](https://www.tpex.org.tw/openapi/v1/tpex_trading_warning_information)、[TPEx 上櫃處置資訊](https://www.tpex.org.tw/openapi/v1/tpex_disposal_information)、[資料集 11395](https://data.gov.tw/dataset/11395)、[資料集 11396](https://data.gov.tw/dataset/11396)
- TPEx 開放資料平台確認 11395/11396 每日更新、免費、政府資料開放授權第 1 版，且對應欄位分別為公告日/代號/注意內容/收盤價/本益比，以及公告日/代號/處置期間/原因/措施。已在程式定義兩個 TPEx 官方來源，按同一候選市場關聯，只作風險背景；處置資料按公告日保存並分開比對生效期間，使未來生效但已公告的處置可以提前提示。候選卡的官方連結改用原始 API，且只對可信官方 HTTPS host 顯示。這些程式變更尚未經下一輪自然收盤排程驗證；10/2 當輪不手動重跑。
- TPEx 官方月營收 API 的 2026/08 觀測：單月 NT$262.258M、年增 45.62%，1–8 月累計 NT$1,894.097M、年增 45.57%。CMoney 9/8 的公開摘要稱富邦證券看多、目標價 NT$700，預估 2026 年營收 NT$3.57B、EPS NT$5.58。若此營收預估指全年，9–12 月需再達 NT$1.676B，約 NT$419M/月，較 8 月單月高約 59.8%、較 1–8 月月均高約 77.0%；NT$700 對 NT$534 收盤價隱含 +31.1%，若以同一預估 EPS 換算，現價約 95.7 倍、目標價約 125.4 倍；這些只是算術一致性檢查，不是原報告的估值方法。成長假設門檻高，但不足以證明預估錯誤。由於摘要沒有原始報告、分析師、發布版本歷程、EPS 橋接、估值方法、期限或風險條件，暫列 C「標題/摘要線索」，不將 NT$700 當作可執行訊號。[TPEx 月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)、[CMoney 9/8 公開摘要](https://www.cmoney.tw/notes/note-detail.aspx?nid=1264244)
- 公司 Q2 2026 法說資料支持高成長敘事，但同時顯示季減：Q2 營收 NT$770.103M、年增 57.2%，毛利率 43.3%（Q1 54.5%），營業利益 NT$98.046M（季減 46.2%），EPS 0.92（Q1 1.67）；上半年營收年增 54.2%、營業利益年增 81.8%、EPS 2.58。官方實績能驗證營收成長，卻不能獨自支持目標價或全年獲利預估；需追蹤毛利率恢復、產能/基板瓶頸與後續月營收，並在 10/5 交易日後回填前瞻報酬，不以當日已知資料倒填券商命中。[GCS 2026 Q2 投資人簡報](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)、[公司法說索引](https://www.gcsincorp.com/investors/Investor_Conference.php)

### 2026-10-02 21:34 BLS 就業數據的即時跨資產反應與來源註冊

- BLS 原始報告：9 月非農 +29,000，失業率 4.2%；主要產業大多變化不大，醫療保健 +17,000（低於過去 12 個月月均 +33,000），平均時薪月增 0.1%、年增 3.0%，7、8 月合計下修 60,000。[BLS 9 月 Employment Situation](https://www.bls.gov/news.release/empsit.nr0.htm)
- Reuters 10/2 08:46 EDT（台北 20:46）報導：標普期貨 +0.9%、Nasdaq 期貨 +1%；2 年債殖利率 -7bp 至 4.716%、10 年 -6bp 至 5.176%、30 年 -4bp 至 5.569%；美元指數 -0.2% 至 101.79，10 月升息機率最低降至 12%。這是發布後約 16 分鐘的即時行情快照，不是收盤表現，也不是獨立預測；市場當下按利率路徑偏鴿解讀，並不能消除招聘疲弱對需求的風險。[Reuters 即時市場反應（MarketScreener 轉載）](https://www.marketscreener.com/news/soft-september-jobs-report-sends-markets-higher-ce785ddad18efe2c)
- AP 指出失業率上升部分來自約 485,000 人進入勞動力，BLS 的勞動參與率則為 61.8%、當月變化不大；這有助避免把失業率上升直接等同裁員潮。Reuters 引用季節調整因素可能解釋偏弱數字及前月修正，屬受訪經濟學家假說，並非 BLS 對數據的官方結論。[AP 9 月就業報導](https://apnews.com/article/jobs-unemployment-economy-ai-hiring-44b99f9ccbff863888af99402ddf535e)
- 訊號處理：列為「宏觀事件驚喜＋即時市場反應」觀察，仍不併入個股多空家族。10/5 再核台股指數/廣度、美元兌台幣、台指期及主要出口產業反應，並分開記錄開盤與收盤；在台股實際反應及後續數日確認前，不稱作台股風險偏好已轉多。
- 10/2 21:30 台北時間自然 `source_extension_collect_intraday` 成功，signal run 為 `ready/fresh`、候選 0、通知 0；同一自然 run 的 catalog bootstrap 將 registry 從 31 增至 33 個 approved/enabled，兩個 TPEx 注意/處置來源已註冊。但兩來源 `last_success_at=NULL`、`source_runs` 無列，因此只證明註冊，不證明 ingestion。10/2 20:00 的日線 run 仍為 `ready/partial`、50 候選、`notified_count=0`；對應 Discord ledger 有失敗與操作失敗通知的 sent 記錄。下一個 `daily_after_close` 自然驗證日為 10/5。
- 10/2 21:41 台北時間再查 AP 的 breaking update，報導標普 +0.9%、道瓊 +357 點、Nasdaq +1.2%、10 年債殖利率約 5.20%；但頁面沒有提供這次更新的明確時間，因此只能作方向性補充，不能與 Reuters 08:45–08:49 EDT 的期貨快照當成同一觀測時點。Finance 查價工具在 21:41 台北仍回傳 13:27 UTC（美東 09:27、現貨開盤前）的 ETF 報價，故不把它當成開盤後確認。美股反應仍待有時間戳的盤中/收盤行情覆核。[AP 市場更新](https://apnews.com/article/stocks-markets-bonds-us-oil-a2b99562febc21d84955e243c87f5d31)

### 2026-10-02 21:48 美股開盤早段報價與台股自然掃描

- Finance 報價工具於台北 21:47 左右可取得的最新交易時間分散在 13:30:11–13:32:55 UTC（美東 09:30:11–09:32:55），不是 09:47 即時行情：SPY +0.80%（13:32:53 UTC）、QQQ +1.00%（13:31:55）、DIA +0.65%（13:32:55）、TLT +0.24%（13:31:52）、UUP -0.17%（13:30:11）。早盤方向與利率壓力緩和一致，但只作開盤數分鐘的反應紀錄；報價 feed 延遲、ETF 不是殖利率/美元現貨，也不能代表全日走勢。[Reuters 就業發布後即時市場摘要](https://www.marketscreener.com/news/soft-september-jobs-report-sends-markets-higher-ce785ddad18efe2c)
- 10/2 21:45 台北自然 `source_extension_collect_intraday`（logical run `scheduled__2026-10-02T13:30:00+00:00`）成功；signal run `d5222a9acb6f84ae24a94e61dd270d6f4f7231e3995ed8a49159c94121fee8f7` 為 `ready/fresh`、as-of 10/2、候選 0、通知 0。這是來源新鮮的空掃描，不代表偏多或偏空。兩個 TPEx 官方注意/處置來源仍 `last_success_at=NULL` 且 `source_runs` 無資料；本輪證明掃描正常，沒有證明這兩個來源已被收集。10/2 日線仍為 `ready/partial`、50 候選且未成功送出 daily digest；下一個自然收盤收集驗證點為 10/5。
- 台股跨市場檢驗仍待 10/5：對照指數及市場廣度、USD/TWD、TAIFEX 期貨/未平倉與出口族群；保留開盤與收盤截面，不能把美股單一早盤快照直接轉成個股方向分數。

### 2026-10-02 22:03 日線訊號觸發碰撞與首批結果基線

- 根因確認：`source_extension_collect_intraday` 與 `source_extension_collect_after_close` 的 Airflow 排程 logical run id 會重複使用 `scheduled__<日期時間>`；舊版 `trigger_market_signal_notify` 也只以這個 run id 建子 run。收盤 DAG 的 9/29、9/30 任務 log 明確記錄 `Skipping due to skip_when_already_exists is set to True and DagRunAlreadyExists`。同一 ID 已有盤中子 run；資料庫中相同 source DAG run id 對應的子 run 是 `scan_mode=intraday`、`ready/fresh`、0 候選。故那些收盤輪次沒有 daily scan，並非 0 檔日線候選。
- 直接影響：market_signal_runs 目前僅見 9/24 `daily ready/fresh` 81 候選、9/25 `daily failed`，接著 10/2 才有 `daily ready/partial` 50 候選；9/26–10/1 無日線訊號 run。10/2 20:00 收盤 DAG 的觸發任務已成功，target run id 帶 `daily`，證明 scan-mode 區隔開始生效；其 `source_dag_id` 仍為 NULL，因來源 DAG lineage migration 8 是 20:30 自然盤中輪才套用。21:45 盤中 run 已記錄 `source_dag_id=source_extension_collect_intraday`。目前 `dag.py` 的未來 target run id 同時包含 scan mode、source DAG ID、source run id；10/5 收盤輪須驗證完整的新 ID/lineage。
- 收盤輪仍有獨立來源故障：10/2 `tpex_equity_quotes_daily` 兩次下載分別在約 0.70 MB、4.19 MB 發生 `ChunkedEncodingError`，導致來源驗證失敗並讓 after-close DAG failed；收盤 scan 雖有觸發，source status 為 partial。已對這個 TPEx 行情 adapter 加三次有界讀取重試，但自然執行尚未驗證。10/2 的 Discord 日報也沒有成功 receipt；目前 10KB payload 分批修正尚未經非空日報自然投遞確認。
- TPEx 注意/處置來源目前 13 個 `daily_after_close` 核准來源中已包含 `tpex_trading_attention_daily`、`tpex_trading_disposition_daily`，兩者 `last_success_at=NULL` 且 `source_runs` 無列。它們在 10/2 20:00 after-close 開始後才由 21:30 intraday catalog bootstrap 註冊；intraday discovery 只收 `schedule_hint='intraday'`，因此未執行是排程順序預期，不是契約不符。10/5 才是第一個驗證窗口。
- 唯一已結算訊號日為 9/24：81 檔、僅 1 日 horizon、`next_close`（收盤後通知的保守進場代理）；其餘日期尚無成熟日線 outcome。上市偏多 n=43：平均個股報酬 +3.44%、TAIEX +0.65%、超額 +2.79 個百分點、相對命中 65.1%；上櫃偏多 n=30：平均個股 -0.26%、TPEx +1.17%、超額 -1.43 個百分點、相對命中 23.3%。這顯示當天跨市場差異大；單一日期樣本不能推廣成市場分層規則。
- 同一日期按同向家族數看不出穩定遞增：上市 3 家族偏多 n=37，超額 +2.83pp/命中 64.9%；4 家族 n=6，+2.56pp/66.7%。上櫃 3 家族 n=27，-1.32pp/22.2%；4 家族 n=3，-2.41pp/33.3%。樣本太小，不調整三家族門檻。 outcome 複核應同時報絕對報酬、對應指數超額與樣本數，避免只報相對命中率。
- 10/5 待驗：after-close 是否按新的來源 DAG ID建立 daily 子 run；13 個收盤來源是否全部完成、兩 TPEx 官方背景是否入庫；TPEx 日行情重試是否恢復；`ready/partial` 是否回到完整日線掃描；非空 Discord 報告是否成功 receipt；並開始累積第二個可比訊號日 outcome。

### 2026-10-02 6461 益得：月營收訊號需要基期與公司說明

- 10/2 `ready/partial` 日線候選：6461 偏多，價格、量價、法人流、月營收 4 個家族同向；近 5 個交易日 +10.21%，相對 TPEx +7.03 個百分點，成交量為近 20 日均量約 5 倍，收盤高於前 20 日收盤高。這是局部來源資料下的候選，不是機率或完整市場結論。
- TPEx 官方 2026/08 月營收 API：當月 NT$5.654M、年增 1,102.98%；1–8 月累計 NT$42.312M，去年同期 NT$96.602M、累計年減 56.20%。公司申報說明：單月增加主要因經銷商備貨增加；累計減少主要因去年認列海思科完成合約的技術服務收入 NT$68.04M。[TPEx 官方月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 判讀：單月同比基數僅 NT$0.470M，且公司指出經銷商備貨，故月增訊號尚未證明終端需求或重複性收入改善；累計同比也受到去年一次性技術服務收入影響，不能直接解讀成核心本業同幅度衰退。後續需看數月營收、產品/客戶出貨與季報毛利和現金流。
- 通知缺口：官方欄位已映射 `revenue_current`、`revenue_ytd_pct`，但舊版日線只用 `source_records.metric_value` 月同比與前月變化形成營收家族，Discord 卡沒有金額、累計趨勢或公司備註。已改為讀取當月金額、累計年增率及新映射的官方「備註」，放入獨立「營收脈絡」欄；維持既有月營收家族和方向門檻，不將這些背景欄位當額外家族。新備註要等來源下一次重收後才會進入 `source_record_fields`；自然 Discord 卡尚未驗證。
- 券商報告評估：本輪未找到可核實的獨立券商個股研究、分析師 EPS/營收預估或目標價。找到的是 2026-07-23 MOPS 存檔的益得公司法人說明會簡報，雖在元大證券舉行，簡報作者是公司，不是元大研究報告。簡報可作公司產品管線、商業化路線與監管風險的一手背景；有揭露長開發時程、高成本及法規不確定性，但沒有估值、財務預測橋接、目標價期限、機率或可量化失效條件，對交易方向的證據力低。[公司法說原始簡報（MOPS）](https://mopsov.twse.com.tw/nas/STR/646120260723M001.pdf)
- 下一步：在新的官方營收資料同步後，確認 `revenue_note` 是否入庫並由候選卡顯示；追蹤之後月營收、產品商業化公告及季報獲利/現金轉換。收到可追溯的券商研究時，保存原版、預估假設、估值方法、風險條件與修正歷程，再以同期間實績和市場超額報酬檢視，不用媒體標題替代原文。

## 2026-10-02 6174 安碁、8289 泰藝：動能、新聞適配與官方事件脈絡

- 10/2 日線快照中，6174 與 8289 都有價格、量能、法人流、月營收四個既有偏多家族；近 5 日分別上漲 23.46% 與 26.21%，相對 TPEx 指數超額 +20.27 與 +23.03 個百分點，成交量約為 20 日均量 2.43 倍與 3.89 倍。外資淨買超分別約 67.6 萬股與 54.6 萬股。此為 10/2 `ready/partial` 快照，且近 5 日漲幅已超過既有「不追價」提示門檻；家族票不等於報酬機率，也不能忽略價格與量能的相關性。
- 官方 TPEx 2026/08 月營收：6174 當月 NT$66.923M、年增 32.84%、1–8 月累計年增 26.13%；8289 當月 NT$214.038M、年增 67.15%、1–8 月累計年增 18.17%，公司說明為客戶需求增加。官方 Q2 2026 營業利益率分別為 6174 12.98%、8289 5.34%；單季數值只能作獲利背景，須看連續季度、毛利/營益率與現金轉換。[TPEx 月營收](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)、[TPEx 季獲利](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- ETtoday 10/2 文章的內文前段確實列出 6174 安碁及 8289 泰藝，現有 match_basis=content_lead 並非錯配；但文章主要題材是 MLCC 供需。兩家公司官方產品分類指向石英元件/頻率控制產品，因此 MLCC 題材不能直接證明兩家公司新增訂單或營收受惠。該則新聞只保留為二手產業/動能背景，不提升方向家族或分數。[ETtoday 原文](https://finance.ettoday.net/news/3247556)、[安碁產品](https://www.aker.com.tw/product)、[泰藝產品](https://www.taitien.com/products)
- 6174 的 TPEx 官方重大訊息記錄 9/30 已收足國內第三次無擔保轉換公司債款 NT$306M；另有 10/7 14:30 兆豐證券法人說明會通知。通知台帳未查到兩則事件的成功推播紀錄，符合現行只推送 `materiality=key` 的規則：法說邀請分類為 `routine/例行公告`；轉債收款公告經現行分類器判為 `review/公告待判讀`。兩者都不是方向訊號，也不符合 key-only 推播門檻。日線候選卡目前未合併這類官方公司事件，故評估新增中性事件脈絡欄時，應保留日期、公告原文、籌資/可能稀釋條件與法說日，不增加多空家族；需先查完整發行條件與轉換價，不能只憑收款金額判定稀釋幅度。[TPEx 官方重大訊息](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 網路找到的 CMoney「法人目標價 31 元」文章是市場內容作者的二手轉述；文內未辨明券商、分析師、原始報告、估值期間、EPS/營收假設或估值倍數，不能當作可驗證券商目標價，亦未納入訊號。[CMoney 文章](https://cmnews.com.tw/article/marketwatcher-09027030-3ebe-11f1-90e9-674ba718a0f9)
- 已在訊號卡的新聞脈絡展示匹配內文摘句與「正文前 320 字」範圍，讓讀者看見命中證據；只改呈現透明度，未改配分/家族/門檻。Airflow 容器中的 `py_compile` 通過；尚未等到新自然收盤卡驗證實際 Discord 呈現。下個驗證點為 10/5 自然日線通知及月營收 `revenue_note` 同步；10/7 後核對法說簡報/問答與後續公告。券商研究只在找到可追溯原件及假設後再評估，不以新聞改寫替代。

## 2026-10-02 23:01 獲利率期間口徑修正與環宇-KY 券商報告覆核

- 直接讀取 TWSE/TPEx 官方 `t187ap17` 端點並查 `crawler.source_records`：10/2 出表資料中，台積電 Q2 營收 NT$2,404,483.69M、營業利益率 59.29%；環宇-KY Q2 營收 NT$1,484.78M、營業利益率 18.87%。環宇官方 Q2 投資人資料列單季營收 NT$770.103M、單季營業利益 NT$98.046M、單季毛利率 43.3%；官方 H1 累計營收 NT$1,484.783M、營業利益 NT$280.185M，18.87% 正好是上半年累計營業利益率。這證實 `t187ap17` 當前 Q2 的收入及率是年初至該期的累計值，不是 Q2 單季。MOPSFIN 官方說明也把季報損益表列為各季累計，同時區分單季與累計獲利能力資料。[MOPSFIN 使用說明](https://mopsfin.twse.com.tw/terms)、[TWSE 官方 API](https://openapi.twse.com.tw/v1/opendata/t187ap17_L)、[TPEx 官方 API](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)、[環宇 Q2 官方簡報](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)
- DB 目前有 1,934 筆有效季獲利列：TWSE 1,049、TPEx 885，全部都是 2026Q2；0 檔股票有兩個不同季度。故這是已證實的欄位語義/潛在訊號缺陷，並非已經確認發生的錯誤通知：現在沒有兩季率變化可供原程式比較。若等到 Q3 後直接相減，訊號數字代表兩個累計率差，卻會被卡片說成「較前期」。
- 已修正 `market_signals.py`：從相鄰年度內的累計營收與累計營益率差推回單季營益率；Q1 直接視為單季，Q2–Q4 必須有前一季累計列才推算，且只比較連續季度。缺少必要季度時不建立 profitability 家族。保留既有 ±3 個百分點/跨零門檻與總家族規則；卡片改成「單季／較前季」並標明資料期。來源目錄欄名改清楚註明「財報所屬期間累計」。來源利潤率僅精確到小數點後兩位，回推單季率有小幅四捨五入誤差；通知須待完整相鄰季度資料，尚未經自然 DAG 驗證。當前只有 Q2，未來 Q3 可算 Q3 單季率，但 Q2 單季率需 Q1 累計數才能回推，故不會為了湊訊號而跨期比較。
- 環宇-KY 券商資料：找到 2026-03-05 元大「更新報告」的 Scribd 重上傳件，標示維持買進、12 個月目標價 NT$425（前次 285），以 2H26–1H27 稀釋後 EPS 8.5 × 50 倍本益比，並列 2026/27 EPS 5.22/10.20、營收 NT$3.095B/3.9B。報告包含季度財測、營益率、產品組合與同業倍數，具備可檢查假設的研究內容；但下載件不是券商官方原件，來源真偽/版本未驗證，且 50 倍估值沒有敏感度或下行情境。至 10/2 收盤 NT$534，已高於報告目標價約 25.6%，報告當下股價 NT$333.5 以來上漲約 60%，不能再把 NT$425 當作現行上檔訊號。[Scribd 重上傳的元大報告](https://www.scribd.com/document/1007990004/%E5%85%83%E5%A4%A7-4991-%E7%92%B0%E5%AE%87-KY-20260305)
- 實績驗算：重上傳文件列示 Q1/Q2 營業利益預估 NT$100M/178M、EPS 0.13/0.72。官方 Q1 營業利益 NT$182.139M，高於預估 82%；Q2 營收 NT$770.103M，高於預估 NT$680M 13.3%，但營業利益僅 NT$98.046M、低於預估 44.9%，單季營益率 12.7% 對預估 26.1%（低 13.4 個百分點）。Q2 EPS 0.92 高於 0.72，主要與實際業外項目為正 NT$35.533M、而報告估為負 NT$77M 有關，不能抵銷本業利潤率預測落差。H1 營業利益合計 NT$280.185M，接近兩季預估合計 NT$278M，是 Q1 大幅超標抵銷 Q2 大幅落後；H1 EPS 2.58 亦受 Q1 NT$163.760M 處分權益法投資利益影響。評價：研究內容可檢查且並非初報告，但季度營運假設誤差相反抵銷、估值倍數積極、原件未驗證，適合列為可追蹤研究樣本，不能直接當現行交易訊號。後續追 Q3 營收、單季營益率、經常性 EPS 與現金轉換。[環宇 Q2 官方簡報](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)、[環宇 Q1 官方財報](https://gcsincorp.com/tc/pdf/investors/2026_Q1_Financial_Report_CN.pdf)
- 當日運作狀態：最新自然 intraday 子掃描是 22:45 台北、`ready/fresh`、0 候選/0 通知；10/2 日線仍為 `ready/partial`、50 候選、`notified_count=0`。保留 10/5 自然日線 run 作新版通知拆批、官方事件來源與連續季度資料的下一個驗證點，不手動觸發。

## 2026-10-02 23:23 MOPS FIN 單季營益率歷史訊號查核

- 查到 MOPS FIN 的官方「營業利益率」趨勢頁可回傳單季值及多年季度序列；這是證交所官方財務比較系統，使用 MOPS 申報財報資料，系統每日更新、相對 MOPS 約落後一日。官方說明列明上市櫃從 2013 年起可查，Q1–Q3 單季金額採公司申報值，Q4 單季由全年累計減前三季累計計算。[MOPS FIN 使用說明](https://mopsfin.twse.com.tw/terms)、[MOPS FIN 官方網站](https://mopsfin.twse.com.tw/)
- 唯讀查詢公司 4991「單季營業利益率」得到 2026Q1 25.49%、2026Q2 12.73%，單季減少 12.76 個百分點。Q2 數值和公司官方資料的 98.046M / 770.103M = 12.73% 相符；這個本業下滑訊號被營收高於券商估計 13.3%、EPS 高於估計等表面正向結果掩蓋，主要因業外獲利為正。故單季本業率比只看營收或 EPS 更能揭露這份報告的風險，仍須把它稱為基本面證據，不宣稱已證明股價預測效力。[環宇 Q2 官方簡報](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)
- MOPS FIN 前端目前透過同源 `POST /compare/data` 請求此圖表；該歷史查詢不在公開 OpenAPI 的 `t187ap17` 參數契約中。`t187ap17` 的公開端點可查本期累計值，但沒有季度/公司查詢參數。MOPS FIN 趨勢回應可包含多年 `xaxisList` 與單季率，且 `showNameList` 帶代碼，但 `graphData` 標籤本身僅有名稱。
- 批次完整性尚未達可投入生產的標準：單次 2 檔測試回傳 2 個序列；9 檔和 10 檔請求分別只回傳 8 和 9 個序列，首個請求碼 2330 不見。必須依回傳的股票代碼清單逐檔比對，不能依序對位或把部分回傳當完整。來源也只給歷史結果，沒有每筆財報實際首次申報日；回補後的數列可能包含後續更正，因此不宜直接拿來做歷史回測或以回補時間冒充發佈時間。
- 結論：MOPS FIN 是高權威的單季指標研究入口，可精確補足現有 `t187ap17` 只有累計率、且資料庫只有單一 Q2 期間的缺口；當前標為「已驗證來源/人工研究」，還不是正式自動來源。將來納入前需找到穩定且允許介接的正式介面，或確認批次請求逐碼完整、加上回補及發布時間語義、限制節流並有缺漏失敗檢查；正式卡片仍需以公告可得時點和當季單季數值計算。

## 2026-10-02 23:35 4991 初評報導、基本面反證與訊號卡覆蓋缺口

- 新增一筆券商研究線索：2026-09-11《鏡週刊》轉載三立新聞網報導，稱「本土投顧」首次評等買進、目標價 NT$700；預估 2026/27 EPS 為 NT$5.69/19.08、2027 營收 NT$8.192B、毛利率 51%、AOC 占比 88%，以 2027 EPS × 37 倍估值。報導沒有券商、分析師、原始報告、報告日期、目標期間、EPS 橋接、敏感度或下行情境。依現有 A/B/C 規則暫列 C「來源未核實的研究線索」，理由是原件與作者不可追溯；「初評」本身不等於研究品質差。拿到原件後再升級，需保留報告版本、假設與風險，不用新聞轉述取代原始研究。[報導轉載](https://www.mirrormedia.mg/external/setn_1904855)
- 估值核算：NT$700 / 37 = EPS NT$18.92，與報導 19.08 有小幅四捨五入差；目標價隱含約 36.69 倍。報導同列的 2027 市場共識 EPS NT$13.89 若乘 37 倍，估值約 NT$513.93；以 10/2 收盤 NT$534 計，現價約等於 37 倍 EPS NT$14.43，較該共識 EPS 高約 3.9%。目標價相對 10/2 收盤上行約 31.1%，但報導沒有目標期間，這不是預期報酬或報酬機率。
- 用公司官方 Q2 2026 簡報交叉核對：Q1→Q2 營收 NT$714.68M→770.10M、毛利率 54.5%→43.3%、營益率 25.5%→12.7%、EPS 1.67→0.92；Q2 營業利益 NT$98.05M、業外項目 +NT$35.53M。H1 EPS NT$2.58 含 Q1 投資處分利益 NT$163.76M。故不能用 EPS 或營收超估單獨確認本業轉強；報導中的 2027 EPS 較其 2026 EPS 預測增加約 235%，但沒有產品出貨、單價、良率、毛利/費用與股數的逐項財測橋接，這是需要等實績驗證的高斜率假設。[公司 Q2 簡報](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)
- 營收方面，公司官方頁面截至本次查核僅公布 2026/08：7 月 USD4.52M、8 月 USD8.19M，1–8 月 USD59.68M，9 月欄仍空白；TPEx 官方 4991 8 月營收 NT$262.258M、年增 45.62%。報導另轉述公司法說營收 USD26–28M、毛利率 40–48%，但未提供第一手逐字稿，預測期間亦未獨立確認。若該區間確指 Q3，前兩個月合計 USD12.71M，9 月需 USD13.29–15.29M 才達標；僅為條件試算，不能在 9 月官方資料公布前判定達標或落空。[公司月營收](https://www.gcsincorp.com/investors/Monthly_Sales.php)、[TPEx 月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 單季本業反證已由官方 MOPS FIN 趨勢人工查得：4991 Q1 營益率 25.49%、Q2 12.73%，季減 12.76 個百分點。該序列還沒有按股票代碼逐筆完整性、發布時點與回補修訂契約驗證，仍屬人工研究，不是 production feed。現有 10/2 候選的四個偏多家族是價格、量能、法人、月營收；因 DB 季度獲利歷史目前只有 2026Q2 累計資料，無法形成連季比較，故四家族沒有納入上述可見的單季利潤率惡化。這是「基本面反證可見、正式卡片資料覆蓋不足」，不是已確認錯誤推播，也不能把四家族解讀為四份獨立證據。[MOPS FIN 說明](https://mopsfin.twse.com.tw/terms)、[MOPS FIN](https://mopsfin.twse.com.tw/)
- 後續採集：查找券商官網或具身份驗證的原始報告；公司 9 月營收發布後核對月營收及 Q3 公司展望；在季度營益率資料回補具逐碼完整與申報時點語義前，把 MOPS FIN 保持為人工核對來源。對報告逐季比較預估與實績，分開追蹤營收、單季毛利/營益率、經常性 EPS、現金轉換及假設修正。尚無證據證明這種家族同向或券商初評對未來超額報酬有預測力。
## 2026-10-03 00:03 台北時間訊號、來源新鮮度與新聞時間校正

### 10/2 自然收盤資料狀態

- Airflow 自然 `source_extension_collect_after_close` run `scheduled__2026-10-01T12:00:00+00:00` 未完整：TPEx 個股日線報價 task 兩次因 `ChunkedEncodingError` 失敗（0/0 列）；驗證 task failed。訊號子 run 仍為 `ready/partial`、資料日 10/2、母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、財務 1,902、候選 50、通知 0。TWSE 報價 task 雖成功，1,051 列最新有效日僅 10/1；TPEx 報價仍停在 10/1，不能宣稱上市櫃日線齊全。最新自然盤中子掃描為 10/2 22:45 `ready/fresh`、0 候選/0 通知；這是盤中空掃描，非收盤完整性證明。10/3 為週六，下一個收盤自然驗證窗口為 10/5，不手動觸發。
- After-close source run 摘要（成功列為 raw/accepted/rejected；最大有效日來自目前 `source_records`，不是 run 級欄位）：

| 市場/來源 | task 結果與列數 | 目前最大有效日 | 覆蓋解讀 |
|---|---:|---|---|
| 上櫃公司事件 | 83/83/0；首試 deadlock、retry 成功 | 2026-10-01 | 事件列數不等於上櫃公司全覆蓋 |
| 上市公司事件 | 157/157/0 | 2026-10-01 | 事件列數不等於上市公司全覆蓋 |
| 上櫃月營收 | 892/892/0 | 2026-08-01 | 最近公布期為 8 月 |
| 上市月營收 | 1,086/1,086/0 | 2026-08-01 | 最近公布期為 8 月 |
| 上市個股日線報價 | 1,051/1,051/0 | 2026-10-01 | 成功但落後訊號日一天 |
| 上櫃個股日線報價 | 兩次失敗，各 0/0/0 | 2026-10-01（舊資料） | 10/2 無新列，不得用舊資料冒充當日覆蓋 |
| 上市融資融券 | 1,039/994/45 | 2026-10-02 | 45 列隔離，不計入有效列 |
| 上櫃融資融券 | 801/801/0 | 2026-10-01 | 較訊號日落後一天 |
| 上櫃市場指數 | 2/2/0 | 2026-10-02 | 市場彙總，非個股訊號 |
| 上市市場指數 | 1/1/0 | 2026-10-01 | 基準指數落後一天 |
| TWSE/TPEx 借券可用量 | 1,717/1,717/0 | 2026-10-02 | 合併來源，不能由此推得兩市個股覆蓋齊全 |
| 上櫃借券 | 814/814/0 | 2026-10-01 | 落後一天 |
| 上市財測達成 | 8/0/8 | 2026-06-30 | 任務綠燈但全數隔離、無可用列 |
| 上市注意交易 | 0/0/0 | 無列 | 空回應不等同確認當日沒有注意股票 |
| 上市處置資訊 | 8/8/0 | 2026-10-02 | 8 列公告，不代表上市母體全部有處置 |

- `source_runs` 保留 raw/accepted/rejected 和錯誤；該輪發生在 migration 將 `dag_id` 寫入 run 前，`dag_id` 為 NULL。`source_records` 以 source key upsert、沒有 run id，最大有效日只代表目前持久化值，無法逐列證明屬於哪次 run。上市/上櫃用來源鍵分開，但列數不是證券母體分母或完整覆蓋率。
- `market_signal_outcomes` 目前 81 個候選（上市 47、上櫃 34），皆來自單一訊號日 2026-09-24，僅 1-session、fresh 樣本成熟。按方向拆開，偏多 73 檔：次日收盤基準上市 43 檔、超額正向 28 檔、平均超額 +2.789pp；上櫃 30 檔、7 檔正向、平均 -1.431pp。訊號日收盤基準是事後描述，上市 27/43、上櫃 20/30 超額正向。偏空樣本僅上市 4 檔、上櫃 3 檔；分歧 1 檔。報酬未扣成本，且不是偏多訊號勝率；不同市場表現分化、樣本窄，暫不調門檻。5/10-session 尚無成熟列。

### 當沖來源界線

- 10/2 自然 `stock_day_trade_collect` 任務 success（Airflow 08:57:51–11:44:29 UTC，即台北 16:57–19:44）。既有表有 1,228 個 TWSE TWTB4U 回應代碼，972 列可連上市主檔、256 列未分類；未分類列不得補零。當前舊 collector 仍讀 TWTB4U，卻選取 TWTB5U 統計商品欄位；兩商品契約/授權需按官方商品頁分開處理。上櫃個股 endpoint `https://www.tpex.org.tw/www/zh-tw/intraday/stat` 回空陣列；授權未確認，保留為舊收集觀察，不接入訊號。[TWSE TWTB4U](https://eshop.twse.com.tw/zh/product/detail/793e7c3c28e04a7c9c9675583c624f03)、[TWSE TWTB5U](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)
- TPEx dataset 11627 的 10/2 數字為全市場當沖 605,346,000 股、占市場成交量 22.15%；無個股代碼，只能作市場背景，需按日 freshness gate，不能冒充個股訊號。[TPEx dataset 11627](https://data.gov.tw/dataset/11627)

### 新聞來源時刻與政策範圍

- 最新 `news_ch_collect_process` 自然 run `scheduled__2026-10-02T10:30:00+00:00` 成功，台北時間約 23:30–23:41 收集；本次 4991 候選脈絡中，鉅亨網資料庫將 `publishAt` 記為 05:20:12，實際應為台北 13:20:12；ETtoday 實際 12:43。這使 DB 排序顛倒 37 分鐘，並讓 48 小時窗口的邊界提前。原因是容器 UTC 下使用本地 `fromtimestamp`，SETN 同樣把 JSON-LD UTC 時刻直接去時區。
- 已修正乾淨檔案 `dags/news_ch/news_crawler/anue.py`、`dags/news_ch/news_crawler/setn.py`：將來源 epoch/ISO UTC 明確轉為台北 +08 後再保存為現有 DB 慣例的 naive datetime。僅做 `py_compile` 靜態編譯成功；未跑測試、未觸發 DAG。已存資料需等自然新聞批次重收並核對是否更新，下一個工作日再確認排序和窗口。
- 4991 題材一手政策證據為 2026-09-25 美國參議員公告：跨黨派法案擬限制聯邦政府國家安全系統採購特定中國光收發器，涵蓋 InnoLight、Eoptolink 等指定廠商/關係企業，設五年轉換期及豁免；這是提案，不是已生效的全面進口禁令。廣泛 FCC 禁令說法仍未找到正式 FCC 規則/公告；亦無已核證的一手證據把該法案直接連到 4991 的現有產品營收，僅列作中性新聞背景，不推估短期獲利。[參議員原始公告](https://www.mccormick.senate.gov/news/press-releases/senators-mccormick-gallego-cornyn-fetterman-introduce-bill-to-keep-chinese-transceivers-out-of-u-s-national-security-systems/)
## 2026-10-03 00:11 券商原始研究可追溯性與量化預測校驗

### 4991 富邦個股報告：來源已確認，研究內文仍未能核原件

- 富邦投顧官方研究索引曾列出 2026/09/08 的 4991 個股報告，標題為「光收發模組帶動的成長爆發」，索引價格 NT$494；完整內文須會員登入。MT Newswires 同日 05:24 EDT（台北 17:24）轉述富邦「初始覆蓋、買進、目標 NT$700」。因此先前媒體寫「本土投顧初評」如今有強烈線索可歸到富邦，但官方公開頁只確認報告存在、標題、日期和索引價格；買進/700 目前仍來自二手 wire，不能說已核原文。[富邦官方研究索引](https://fubonresearch.fubon.com/Research/Home/Index)、[MT Newswires 轉載](https://www.marketscreener.com/news/fubon-starts-gcs-holdings-at-buy-with-nt-700-price-target-ce785bd8da8df021)
- 價格記錄有一元差異：富邦索引顯示 NT$494；MT Newswires 頁面同日市場報價欄顯示 NT$493，兩者的取價時間/口徑未能從公開內容釐清。10/2 官方日線收盤為 NT$534；700 相對 494 的算術幅度為 +41.7%，相對 534 為 +31.1%，不代表目標期報酬或機率。原報告作者、預估資料截止日、估值方法、EPS/營收橋接、上修/下修紀錄、反方風險、失效條件及目標期間皆未能核實。分類改為「一手報告存在、全文待核」，不再以來源身分不明為理由，也不因「初評」評低品質；在全文可審前不把 700 當成高品質方向訊號。
- 4991 公司官方月營收頁在 10/3 仍只列至 8 月：1–8 月 USD59.68M、年增 43.7%，9 月欄尚空白。這支持營收高成長背景，但不能證明富邦的 2026/27 EPS、營益率或估值假設；應等正式 9 月營收、季報和可核原文再評估。[GCS 官方月營收](https://www.gcsincorp.com/investors/Monthly_Sales.php)

### 凱基光通訊研究：可量化的產業預估修正，不能直接折算為個股獲利

- 凱基 2025/01/06 原始網通產業報告列 4991 當時價格 NT$121，明確標示「未評等」；報告預期全球 800GbE 模組 2025 年出貨 1,600–2,000 萬顆，並註明依據中際旭創指引。它提供產業升速脈絡與一般風險（AI 需求不如預期、地緣政治、景氣下行），不是 4991 個股 EPS/目標價報告。[凱基 2025 光通訊原始報告](https://www.kgi.com.hk/zh-hk/-/media/files/kgishk/research-reports/tw-reports/2024/01/networking-sector_06012025.pdf)
- 凱基 2026/02/02 官方週報將 800G 2026 年估計由 4,000 萬上修到 4,500 萬顆（+12.5%），1.6T 由 2,000 萬上修至 3,200 萬（+60%）；同一圖表將 2025 年列為 2,200 萬顆。來源標示 Bloomberg/KGI 投資顧問估計，週報以 AI 算力擴張為依據，未公開模型細節或敏感度，且未把這些出貨量逐項轉成 4991 預估。因此列為 B 級「可追蹤的量化產業修正」，只作產業背景；不是獨立個股方向家族。[凱基 2026/02/02 原始週報](https://www.kgi.com.hk/en/-/media/files/kgishk/research-reports/investment-perspectives/2026/kgis-global-markets-weekly-kickstart_20260202.pdf)

### 凱基 4979 公司預估：假設可檢驗，估值基準與成長斜率需追查

- 同一份凱基官方週報收錄華星光（4979）：2026 營收 NT$7.33B（+60.9%）、EPS NT$9.94（+84.2%）；明列擴產、4 吋線預計 2026Q4 完成、CW laser 月產能 2027 年逾 600 萬、800G 預計 2026Q1 出貨等可檢驗條件。資料截點顯示 1/29 股價 NT$334.50，市值/財務數據來源為公司資料與凱基估計；頁面沒有個別分析師姓名、季度 EPS/毛利橋接或悲觀情境。報告封面日期 2/2/2026，行情指標截至 1/29，市場資料標至 1/30。[凱基 2026/02/02 原始週報](https://www.kgi.com.hk/zh-hk/-/media/files/kgishk/research-reports/investment-perspectives/2026/kgis-global-markets-weekly-kickstart_20260202.pdf)
- 可重算的落地門檻：TPEx 官方 8/13 Q2 財報公告列 H1 營收 NT$2.483B、EPS 2.91；7、8 月官方月營收分別 NT$508.777M、545.866M，故 Jan–Aug 合計 NT$3.537B。要達 7.33B，Sep–Dec 尚需 3.793B，月均 948M，較 8 月高 73.7%；要達 EPS 9.94，H2 尚需 EPS 7.03，平均每季 3.515，約為 Q2 EPS 1.29 的 2.72 倍。這是高斜率但非不可能的基準情境；公開週報沒有給出足以重建該加速的季度出貨/單價/毛利橋接。[TPEx 官方公司公告 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)、[TPEx 官方月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 估值表需先釐清：週報列 2025F/2026F EPS 5.40/9.94、P/E 37.2/20.2；但同頁行情截點 NT$334.50 除以 2026F EPS 9.94 約為 33.65 倍，與 20.2 倍不符。20.2 倍反推股價約 NT$200.79；37.2 倍乘 5.40 也約 NT$200.88。現有公開頁未說明採用較舊股價或其他估值基準，故不能沿用 20.2 倍作目前合理價結論。品質評估：原始發布與明確可測假設為優點；估值基準不一致、盈利加速橋接不足，列為研究追蹤樣本而非高確信交易訊號。
- 10/2 daily candidate 查詢只有 4991 命中，4979 沒有當日候選；此 4979 報告是用來建立券商預測核驗基準，不是回填成當日訊號。後續等 4979 Q3/Q4 季報、月營收及 CW laser/800G 量產公告逐項比對；同時取得富邦原文後再評 4991。

## 2026-10-03 00:33 運行狀態與研究覆蓋再核對

- 唯讀 Airflow/DB 查核確認 10/2 台北時間收盤父流程仍為 failed：15 個不同來源鍵中，14 個至少有一次成功來源嘗試；TPEx 個股日線報價來源兩次失敗，接受/隔離均為 0，收集驗證器因此失敗。TWSE 報價來源接受 1,051/1,051/0，但持久資料最大有效日只有 10/1；指數來源為 TWSE 1/1/0、最大有效日 10/1，TPEx 2/2/0、最大有效日 10/2。月營收來源成功（TWSE 1,086/1,086/0；TPEx 892/892/0），但兩市資料有效期都只到 2026/08；8 列上市財測達成資料全被隔離（8/0/8），上市注意交易來源回空（0/0/0）。這些是任務列數，不等於公司母體覆蓋率。
- 訊號子 run 的 capture 成功，但狀態為 ready/partial、as_of_date=2026-10-02；母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、財務 1,902、候選 50。通知 task 重試 3 次後失敗，notified_count=0。通知台帳有一筆失敗的日線摘要，以及一筆自然排程送出的短版故障通知；本次稽核沒有手動發送訊息。
- 對台帳保存的摘要做唯讀重建、沒有發網路請求：9 個 embeds、可見文字共 4,796 字元，依 requests JSON 序列化估算為 23,078 bytes；Discord 回應 40005 Request entity too large。現行程式將同一內容切成 3 批，估算大小為 8,361、7,456、7,593 bytes，且每批使用穩定去重鍵。Airflow 容器內 market_signals.py 與 adapters.py 的 py_compile 通過，僅屬靜態證據；未觸發 DAG。須等 10/5 自然收盤 run 驗證訊息拆批與 TPEx 限次串流重試。
- 直接讀 TPEx 官方月營收 API 共 892 列；6461、6174、8289 最新都仍是 2026/08 期、2026/09/17 出表。TWSE API 共 1,086 列，這三檔上櫃股票不在該端點。10/3 查詢時官方端點尚無 9 月營收；先列為未公布/尚未入庫，不當成收集失敗或新訊號。[TPEx 官方月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 以股票代碼搜尋 6174、8289、6461 的近期券商原始報告，公開索引未找到可核實的券商原件。第三方 8289 頁面在其顯示日期沒有分析師共識/目標價資料，只能作為研究覆蓋不足的線索；不能據此證明沒有券商原文，也不是方向訊號。[第三方 8289 覆蓋頁](https://toalpha.tw/stock/8289)
- outcome 台帳再次查核仍只有 2026-09-24 訊號日，各有 81 筆一日 outcome（signal_close 與 next_close 入場口徑）；尚無 5/10 日 outcome 結算。維持市場分層，不依此單日樣本調門檻。
- 00:34 台北時間再次查看 BLS 官方頁面：10 月排程列 9 月 Employment Situation 於 10/2 08:30 ET，原始報告也寫明同時解除發布禁令，實際數字為非農 +29,000、失業率 4.2%。先前 AP 所寫「下週」與官方日期衝突已由實際報告釐清。僅把宏觀驚喜作市場背景，不加進個股方向家族；台股反應留待下一個交易日檢視。[BLS 10 月排程](https://www.bls.gov/schedule/2026/)、[BLS 9 月就業報告](https://www.bls.gov/news.release/archives/empsit_10022026.htm)
## 2026-10-03 00:44 石英高頻產業文章與泰藝產品證據更新

- 永豐金證券豐雲學堂「石英元件高頻元年全拆解：石英元件還沒開始鎖產能，但條件正在成形」頁面署名「研究小編」，更新日 2026/09/01，屬券商產業／股市話題，不是可重建的個股賣方模型。可讀摘要主張光模組差動振盪器規格從 156.25MHz 升至 312.5MHz，供應商門檻提高；並轉述 NDK 到 2029 年相關成長約七倍、大真空 312.5MHz 量產實績不足及台廠高頻時程。資料截止日、NDK 七倍原始估算、分析師姓名、個股 EPS/營收假設、估值、目標價/期間、修正紀錄、反方風險及失效條件均未能由頁面核實；本文完整內容的文字擷取亦不完整，故列為產業假說，不能當成個股方向訊號。[永豐原文](https://www.sinotrade.com.tw/richclub/hotstock/%E7%9F%B3%E8%8B%B1%E5%85%83%E4%BB%B6%E9%AB%98%E9%A0%BB%E5%85%83%E5%B9%B4%E5%85%A8%E6%8B%86%E8%A7%A3-%E7%9F%B3%E8%8B%B1%E5%85%83%E4%BB%B6%E9%82%84%E6%B2%92%E9%96%8B%E5%A7%8B%E9%8E%96%E7%94%A2%E8%83%BD-%E4%BD%86%E6%A2%9D%E4%BB%B6%E6%AD%A3%E5%9C%A8%E6%88%90%E5%BD%A2-%E8%82%A1%E5%B8%82%E8%A9%B1%E9%A1%8C-6a87ed5cb9c5f20b1e691d6c)
- 一手時程核對使「供應受限／未量產」需更精確表述：NDK 官方 2026/02/26 新聞稿稱 312.5MHz 型號已量產、625MHz 型號自 2026/03 起供樣；大真空官方 2026/07/15 宣布 Arkh 系列 7 月起全面量產，差動振盪器支援最高 625MHz，並規劃 FY2028 Arkh.3G 月產能 2,500 萬顆。這不證明客戶拉貨或獲利，但表示永豐 9/1 頁面對大真空量產經歷的說法若作為當前狀態，須交代較早訪談的資料截止日；NDK「七倍」亦尚未找到可對照的公司原始預估。[NDK 官方公告](https://www.ndk.com/jp/news/article/high-precision-spxo-625mhz.html)、[大真空官方公告](https://www.kds.info/en/news/product/605774/)
- 泰藝（8289）官方產品頁列出 OB-U 312.5MHz 差動振盪器，頁面日期 2026/09/24，標示典型 30fs RMS phase jitter、用於 800G/1.6T/3.2T 光模組。這是可核實的產品／技術就緒證據，可補充 10/2 動能候選的產業適配脈絡；頁面沒有客戶認證、量產數量、訂單、營收或毛利貢獻，故不另加方向家族，不視為獲利催化已實現。[泰藝官方產品頁](https://www.taitien.com/ti-products/ob-u-type-312-5mhz-ultra-low-phase-noise-crystal-oscillator)
- 後續可檢驗項目：泰藝後續月營收與季報毛利／營益率是否持續改善；公司是否披露客戶認證、量產、出貨或訂單；再核對同族其他廠商的正式產品與量產公告。NDK 七倍估算與日廠高頻時程若找不到原始發布內容，持續標為未核，不以二手重述補成事實。此次僅更新追蹤文件與記憶，未改程式、未重跑 DAG、未送 Discord、未交易。


## 2026-10-03 01:08 新聞時間戳根因、收盤資料與訊號成熟度覆核

### 10/2 自然 after-close 與訊號狀態

- 唯讀回看 10/2 台北收盤父流程 `source_extension_collect_after_close`（logical run `scheduled__2026-10-01T12:00:00+00:00`）：驗證 task failed；15 個來源鍵有 17 筆 run 記錄，14 個來源鍵至少一次成功。TPEx 個股日線報價兩次失敗、0/0/0，故不能把綠燈子任務或舊資料說成兩市收盤資料齊全。來源列數、有效日與覆蓋解讀見本文件前段表格；列數不是證券母體覆蓋率。
- daily 子 run 的快照擷取成功、通知失敗：`as_of_date=2026-10-02`、`ready/partial`，universe 1,946、price 1,938、flow 1,891、revenue 1,945、financial 1,902、候選 50、通知 0。10/2 22:45 台北的最新盤中子掃描為 `ready/fresh`、0 候選/0 通知；它不代表收盤完成。
- 成熟 `market_signal_outcomes` 仍只有 2026-09-24 的 fresh、settled 一日樣本；無 5/10 日成熟樣本。偏多 next-close 超額均值：上市 43 檔 +2.7895pp、28 檔正向；上櫃 30 檔 -1.4312pp、7 檔正向。signal-close 口徑分別 27/43 與 20/30 正向。偏空僅上市 4、上櫃 3；樣本單日、未扣成本，不用來調整門檻。
- 來源狀態摘要：TWSE/TPEx 公司事件分別 157/157/0 與 83/83/0，最新有效日 10/1；月營收 1,086/1,086/0 與 892/892/0，最新期均為 8 月；TWSE 個股日線 1,051/1,051/0、最大日 10/1；TPEx 個股日線當輪失敗，舊最大日 10/1。上市財測達成 8/0/8；上市注意交易 0/0/0；上市融資融券 1,039/994/45、上櫃 801/801/0；市場指數、借券及處置列仍按前段表格分開解讀。不得將未匹配證券或空回應當零量/零事件。
- 當沖未有新證據：上市 TWTB4U/TWTB5U 契約欄位差異仍未釐清；上櫃個股 endpoint 空資料且授權未確認；TPEx 11627 只含市場彙總，需 freshness gate，不是個股訊號。未手動觸發 DAG、未重跑、未發 Discord、未交易。

### 新聞收集時刻核對與先前紀錄更正

- `news_ch_collect_process` 10/2 自然 run `scheduled__2026-10-02T10:30:00+00:00` 在台北 23:30:00–23:41:17 成功；但全綠不代表發稿時間正確或來源涵蓋完整。資料庫沒有 `created_at` 索引，避免對整張新聞表排序；只把最新插入 2,000 列當抽樣，不能推成全量來源統計：聯合報 179（最大 23:29）、ETtoday 560（23:26，553 列日期為 10/2）、Newtalk 40（23:24）、自由時報 242（23:23:33）、中時 359（23:16）、中央社 250（23:15）、鉅亨 210（15:00:08；121 列日期為 10/2）、The News Lens 94（10/2 00:00；6 列日期為 10/2）、iThome 66（10/2 00:00；19 列日期為 10/2）。這是最近插入樣本，不是各來源接受/隔離總數。
- 鉅亨原頁文章 6620290 的 `datePublished=2026-10-02T05:20:12Z`，正確台北時間為 13:20:12。10/2 暫存 `article_url.created_at` 與最終 `news.created_at` 都是錯的 05:20:12；`news.create_time=23:30:36` 證明該筆在當晚被重新寫入。舊版 `datetime.fromtimestamp(t)` 在 UTC 容器會得到 05:20:12；容器純時計算亦重現舊值與 +08 正值。
- 根因時序：工作樹目前的 `dags/news_ch/news_crawler/anue.py` 已明確以 UTC epoch 轉台北 +08；host 檔案與 worker 掛載檔雜湊一致。但該檔修改時間為 2026-10-02 15:59 UTC，晚於新聞 task 15:30:02 開始、15:41:17 結束。因此 10/2 綠燈 run 執行的是修正前版本，問題在收集階段，`clean_time_format` 只保留字串、不會補時區。當前修改尚無自然 run 驗證；等 10/5 下一個交易日的排程收集後，核對暫存/最終時間、來源新鮮度及排序。SETN 的既有 UTC 處理修改也需自然收集結果驗證。本輪不改程式、不跑測試、不重跑 DAG。
- 先前段落的 ETtoday 時刻更正：原頁 `https://finance.ettoday.net/news/3247556` 的 `datePublished=2026-10-02T10:34:00+08:00`，DB 亦為 10:34；前段曾記為 12:43，應以原頁為準。故鉅亨正確時刻比 ETtoday 晚 2 小時 46 分，DB 卻把鉅亨列成早 5 小時 14 分，實際排序被倒置；先前「相差 37 分」的敘述不成立。
- 追蹤品質規則不變：來源事實、分析、未知分開記錄；原始券商全文不可得時不推測 EPS/估值或動機；只用可量化、可追溯且具時點條件的新增證據。現有 source-extension/訊號通知工作樹仍有其他既存 dirty changes，本輪只更新本追蹤文件與授權的 ad-hoc memory，不改動那些檔案。



## 2026-10-03 01:25 候選新聞核名、益得反向基本面與快照時間界線

- 再讀目前 DB 的 10/2 daily 候選，最高同向數為 4 家族。新聞關聯抽查結果：ETtoday 3247556 正文前段明列安碁 6174、泰藝 8289 為被動元件漲停股；鉅亨 6620290 正文亦明列環宇-KY 4991。這三個 content_lead 命中有正文核名，並非只靠題材關鍵字誤配；兩篇仍是媒體二手背景，不計方向家族，鉅亨對美國政策範圍的描述須以官方公告校驗。[ETtoday 3247556](https://finance.ettoday.net/news/3247556)、[鉅亨 6620290](https://news.cnyes.com/news/id/6620290?exp=a)
- 益得 6461 同向 4 家族（價格、量能、法人、單月營收），資料日 10/2：近 5 日 +10.21%、收盤 26.45、越過先前 20 日高 25.75；成交量為前 20 日均量約 5 倍；法人淨買超 100,663 股。這是規則式動能訊號，不等同基本面改善或預測勝率。
- 官方 TPEx 2026/08 月營收（9/17 出表）為 NT$5,654 千元，去年同期 NT$470 千元，年增 1,102.98%；但 1–8 月累計 NT$42,312 千元、年減 56.20%。原始官方備註稱單月增加主因經銷商備貨增加，累計減少主因去年認列與海思科完成合約的技術服務收入 6,804 萬元。TPEx 8/10 Q2/H1 重大訊息列 H1 營收 NT$36.447M、毛損 NT$89.730M、營業損失 NT$204.658M、EPS -1.22。故 6461 的營收方向只由單月反彈支撐，低基期與備貨效應明確，累計與獲利仍弱；列為「動能候選、有官方反向基本面」，不升格高品質基本面訊號、不單憑此例改門檻。[TPEx 月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)、[TPEx 重大訊息 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)、[益得產品與研發](https://www.intechbiopharm.com/products.php)
- 官方 8/4 減資換股計畫列減資 38.1422%，舊股最後交易日 9/1、新股 9/9 上櫃；20 日技術/量能窗跨舊、新股制。9/9 新股首日收盤 25.75，10/2 收 26.45；以後 15 個新股交易日平均量約 93.7 張/單位，10/2 為 465，約 4.96 倍，仍接近資料卡的 5 倍量能。價格高點與量能比較需標示有公司行動，避免將跨制窗口視為完全同質歷史基準。
- 兩篇新聞在 10/2 20:02 的掃描視窗中都仍在 48 小時內，因此這次發稿時間校正能更正排序，但沒有證據顯示它改變本次新聞入選與否；接近窗口邊界的未來 run 仍可能受影響。
- 修正 dags/source_extension/market_signals.py 的 as-of 時間比較：source registry 的 created_at/updated_at 由 UTC 容器的 naive 時鐘寫入，signal started_at 是台北 naive。新增轉 UTC-naive 截止時間，並在 daily 的營收、指數、融資融券、監理、財務、借券、董監、股利來源查詢，以及 intraday 新列/修訂與前期營收查詢中限制 updated_at 不晚於掃描開始。實例核驗：6461 月營收列 updated_at=2026-10-02 14:45:07 UTC，晚於 20:02:35 台北的掃描；舊混合時鐘比較會納入，轉換後截止為 12:02:35 UTC，會排除。這避免把掃描後才收集/修訂的來源資料倒灌進當時快照。
- worker 掛載版 Python 語法編譯通過；靜態檢查涵蓋 daily 10 組、intraday 3 組 SQL 佔位符/參數數量，均相符。未跑測試、未重跑 DAG、未發 Discord、未交易。新增營收脈絡的 formatter（單月金額、累計年增、公司申報備註與官方連結）已存在於目前工作樹，但 10/2 快照早於該版本；待 10/5 自然流程驗證 payload 及訊息呈現。當前沒有新增可核原文券商報告，未對券商方向作新增評等。


## 2026-10-03 01:38 3081 新候選反向籌碼、賣方原報告與門檻文案校正

### 自然資料與候選覆核

- 週六凌晨再查資料庫，最新日線訊號仍是 10/2 20:02:35–20:02:42 台北時間的 daily run：ready/partial，母體 1,946、價格 1,938、法人 1,891、營收 1,945、財務 1,902、候選 50、通知 0。最新盤中 run 10/2 22:45:13–22:45:14：ready/fresh、0 候選/0 通知。沒有 10/3 新交易日 run。
- 10/2 after-close 子 run 仍與前節一致：TPEx 個股日線兩次 0/0/0 失敗、TWSE 1,051/1,051/0 但最大有效日 10/1；市場事件 TWSE 157/157/0、TPEx 83/83/0，最大有效日 10/1；月營收 TWSE 1,086/1,086/0、TPEx 892/892/0，最新期 8 月；融資 TWSE 1,039/994/45（45 隔離）、TPEx 801/801/0、最大日分別 10/2 和 10/1；指數 TWSE 1/1/0（10/1）、TPEx 2/2/0（10/2）；合併借券可用量 1,717/1,717/0（10/2，不能推成兩市逐股覆蓋）、TPEx 個股借券 814/814/0（10/1）；TWSE 財測達成 8/0/8、注意交易空回應 0/0/0、處置 8/8/0（10/2）。有效日期和列數不等於證券母體覆蓋率；未匹配與空回應不作零值。完整來源鍵表沿用 01:08 節，本次沒有更新 run。
- 成熟 outcome 仍只含 2026-09-24 的 81 個候選、1-session 樣本；signal_close / next_close 各 81 筆，尚無 5/10-session。資料未扣成本且只有單一訊號日，不改門檻。當沖來源仍按前節限制：上市 TWTB4U/TWTB5U 契約差異未解、TPEx 個股 endpoint 空資料且授權未確認、TPEx 11627 只含市場彙總並需 freshness gate。
- 最新新聞來源 run 仍為 10/2 晚間；發稿時間修正尚未經修正後的自然收集驗證，待 10/5 自然 run 核對。10/2 兩篇光模組新聞會配到 3081/4991，但兩篇皆是同一政策題材的二手報導，不計兩份獨立方向證據。

### 3081 聯亞：實績強、同日資金面反向，獲利家族有資料缺口

- 10/2 daily candidate 實際方向為偏多但只有 2/3 家族支持：價格 +9.14%、8 月月營收年增 +180.90%；法人家族近 5 日淨賣超 922,988 股。候選分數 2、不是勝率。新聞欄配到 ETtoday 與鉅亨兩篇報導；官方提案新聞稿可核政策範圍，但沒有公司營收受惠證據。不能把政策標題當獲利確認。
- TPEx 官方月營收 OpenAPI：2026/08 單月 NT$520.469M，去年同期 NT$185.285M，年增 180.90%；1–8 月累計 NT$3,147.247M，去年同期 NT$1,374.127M，年增 129.04%。官方備註是「數據中心產品出貨量較同期增加」。這是可量化公司營運證據，仍需後續季度毛利/營益、現金流及產品組合驗證。[TPEx 月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- TPEx 官方季度獲利率 OpenAPI 最新回應（資料日期 2026/10/02）列 2026Q2 累計營收 NT$2,125.57M、毛利率 56.31%、營益率 44.50%、稅前率 46.22%、稅後率 36.98%。資料庫目前對 3081 只有一筆 2026Q2 累計列，effective_date=6/30、published_date=10/1；Q1 / 前年同期獲利率歷史缺列，無法可靠推回單季變化，所以 10/2 候選未納入 profitability 家族。官方 API 已更新至 10/2，而 registry 最後成功 run 是 10/1 20:00、885/885/0，為一日來源新鮮度差；數值相同，尚未證明會改變方向。[TPEx 季度獲利率 API](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 追蹤高品質訊號的延伸方向：回補上市櫃官方單季獲利率歷史時，需保留實際首次公告/更新時點，逐碼檢查期別完整性，並將單季營益率變化與營收年增拆成不同證據家族；MOPS FIN 的歷史圖表不應在發布時間與批次完整性契約未證明前直接充當生產 feed。

### 券商研究原文品質

- 永豐證券投資顧問《永豐晨訊》封面標示 2026/03/06。3081 段引用 2 月營收及 1–2 月累計，稱 1Q26 營收達成率 73.6%；提出 2026 年 1.6T 光模組出貨 32mn、2027 年 90mn（+181%），並預估 2027 EPS NT$30.65，但明寫目標價待後續調整。頁面未署個別分析師，未列營收/EPS 橋接、毛利與費用假設、估值倍數、下行情境或風險，也沒交代 1.6T 出貨資料來源及前次 EPS 修正值。原始券商 PDF 可追溯，個股模型可重建性低；它不是已核實的「初評」，也沒有證據可推測出刊動機。[永豐晨訊原始 PDF](https://scm.sinotrade.com.tw/Article/GetFile/5558020e-5960-4068-98bc-907fd5c73153)
- 富邦投顧官網索引列出 2026/09/08 環宇-KY 報告，標題「光收發模組帶動的成長爆發」旁括號顯示 NT$494；公開頁沒有說明該數字是參考股價還是目標價，全文要求登入。作者、資料截止、EPS/營收假設、估值、上修/下修、目標期間和反方風險均未核，維持「原件索引可驗、報告內容未核」，不可據標題評為高品質方向訊號。[富邦投顧報告索引](https://fubonresearch.fubon.com/Research/Home/Index)
- 同一光模組政策題材的首手資料目前是參議員原始新聞稿：2026/09/25 宣布提出法案，內容針對聯邦政府國家安全系統採購涵蓋的光收發器，列 InnoLight、Eoptolink 及其關係企業/相關軟韌體，提供五年轉換期與豁免。這是提案新聞稿所述範圍，不是已生效的全面進口禁令；尚無可核原文把它連到 3081/4991 的訂單、營收或獲利。[參議員原始公告](https://www.mccormick.senate.gov/news/press-releases/senators-mccormick-gallego-cornyn-fetterman-introduce-bill-to-keep-chinese-transceivers-out-of-u-s-national-security-systems/)

### 已做的窄幅通知修正

- 10/2 的 3081 候選是 2 個偏多家族、1 個偏空家族；現行程式要求至少 3 個非中性的有方向家族，再按多空家族票數差決定方向，平手列「訊號分歧」。個股卡片/摘要原本誤寫成「至少 3 個同向家族才入列」，與程式及 2/3 家族卡片不一致。
- 只修正 market_signals.py 三處說明文案，明示「至少 3 個有方向的家族、依票數差判斷、平手列分歧」；不改候選門檻或方向演算法。AST 語法解析通過。未跑測試、未觸發 DAG、未發 Discord、未交易；其他既存 dirty changes 保留。

## 2026-10-03 01:58 公司財測差異訊號與通知去重覆核

### 自然收集與資料時效

- 截至台北 10/3 01:58，沒有新的交易日或 10/3 自然排程可用。最後日線訊號仍為 10/2 20:02 的 `ready/partial` 快照（候選 50、通知 0）；最後盤中掃描 10/2 22:45 為 `ready/fresh`、候選 0、通知 0。成熟 outcome 仍只有 9/24 的 81 檔單日樣本，無 5/10 日成熟樣本；不據此調門檻。
- 新聞來源最新自然 run 仍是 10/2 23:30–23:41 台北時間的 `news_ch_collect_process`。發稿時區修正晚於該次執行，尚未經修正後的自然 run 驗證；最近插入樣本不能當作全來源接受/隔離統計。下一個交易日收集後再驗原頁發稿時間、DB 時間排序及來源新鮮度。
- 更正來源歸屬：10/2 `source_extension_collect_after_close` 自然 run（`scheduled__2026-10-01T12:00:00+00:00`，台北 20:00–20:04）有 11 個 mapped source keys，10 個成功；`verify_collection_and_triggers` 最後失敗，因 map index 9 的 `tpex_equity_quotes_daily` 兩次都遇到 HTTP `IncompleteRead`，raw/accepted/quarantined = 0/0/0。TWSE/TPEx 報價與其餘成功來源的日期、接受/隔離及覆蓋解讀仍按 01:38 節的逐源盤點，綠燈不代表雙市場日線完整。22:45 的 `twse_quarterly_forecast_variance` 7/7/0 是另一個 `source_extension_collect_intraday` run（`scheduled__2026-10-02T14:30:00+00:00`），不是 after-close 子 run。該盤中來源列有 5 家上市公司、有效期別 2026Q2（effective_date 6/30）、出表日 10/1；2515 在官方資料中有序號 0/1/2 三列且數值相同，故七列不是七個獨立事件。此 TWSE 清單按門檻篩選，不等於上市全體或上市櫃覆蓋率。TWSE OpenAPI 說明列示「當季差異達 10% 以上或截至當季累計差異達 20% 以上」；MOPS 將其列在「當季/累計綜合損益預測數與實際數差異較大公司(簡式)」。[TWSE OpenAPI 原始資料](https://openapi.twse.com.tw/v1/opendata/t187ap16_L)、[TWSE OpenAPI 規格](https://openapi.twse.com.tw/)、[MOPS 專區](https://mops.twse.com.tw/mops/web/t120sb02_q10)

### 官方財測差異：可核但方向未定

- 以官方原始值和預測區間上緣比較（括號內依序為當季實績/預測上緣、累計實績/預測上緣）：2412 中華電 10,561,556/9,249,329（+14.2%）、20,593,145/18,260,262（+12.8%）；2515 中工 227,020/-32,649（實績轉正，負分母下百分比不具解讀性）、618,350/128,467（+381.3%）；3036 文曄 11,401,032/9,677,746（+17.8%）、23,040,758/21,317,472（+8.1%）；3702 大聯大 9,113,403/5,377,000（+69.5%）、17,106,806/13,370,403（+27.9%）；4904 遠傳 4,000,371/3,572,546（+12.0%）、7,698,761/7,055,241（+9.1%）。上述比率是對 TWSE 回傳預測區間上緣或單值的計算，原始欄位為資料源數值；OpenAPI 沒提供金額單位，不做幣別或倍率換算。
- 這是公司先前提出的財務預測相對會計師查核／核閱實績，不是券商共識預估，也不是 10/1 新發生的營運期。官方原始資料可作「公司財測偏差／達成」證據，但應回看公司預測原件、預測修正版本、範圍及差異原因後再判斷偏多偏空；目前維持通知卡「方向未定、暫不計入多空分數」。其可延伸方向是加入原始財測公告版本、原定發布時間與實績披露時間，量化誤差率及修正方向，而非直接加方向票。[MOPS 專區](https://mops.twse.com.tw/mops/web/t120sb02_q10)

### 10/2 20:00 與 20:15 重複通知：已確認的去重契約變化

- 唯讀通知 ledger 顯示，20:00（run key 前綴 `75171fb614ec`）和 20:15（`a449b5a7a50d`）各送出一批五檔財測差異卡，共 10 筆 `sent` ledger rows。兩批五卡 payload 的 SHA-256 完全相同；2412、2515、3036、3702、4904 的候選雜湊及通知 dedupe key 卻全部不同，因此唯一鍵未阻止同內容重送。
- 同一五檔的舊 source rows 為 id 1539993–1539999、原 `record_key` 無版本尾碼；20:15 新 rows 為 id 1543308–1543314，`record_key` 加上版本 fingerprint，財測值、期別與出表日期仍相同。新版本候選去重鍵按來源、代碼、有效期別、涵蓋期間、實績及公司預測語意計算，忽略 source row id、record hash 與 2515 的重複財測序號，因此現在的 2515 三列會收斂為一個語意候選。
- 證據確認了「source row 版本／候選雜湊改變，導致 ledger 去重鍵改變」這條重送路徑；兩次執行間究竟是哪次部署或程式改動使雜湊契約轉換，僅靠現存 ledger 未能還原，不推定操作者或根因細節。當前程式的語意 fingerprint 已避免此次來源版本差異；這組 fingerprint 欄位與 ledger dedupe key 應視為相容性契約，日後若修改必須先核對舊 sent ledger 或提供遷移相容層。20:30 之後盤中掃描未再有候選，故不能視為通知修正已經過未來自然 run 驗證。
- 本輪只讀 DB 與官方公開資料並更新此追蹤文件；沒有改程式、手動觸發/重跑 DAG、實發 Discord 或交易。其他既有 dirty changes 均保留。


## 2026-10-03 02:02 大聯大財測誤差與Q3可驗證預測

### 3702：第一手財測差異可信，交易時效性弱

- 大聯大投資人專區索引列出「2026年第2季財務預測」日期 2026/05/12、「2026年第2季合併財報」索引日期 2026/08/14；本輪已讀公司原始 Q2 財測 PDF（FRP00000167）及 Q2 合併財報 PDF（FRP00000168）。財測為公司董事會核准預測，不是券商研究；原文列明 Q2 美元匯率假設 31.6、收入/成本費用/獲利/EPS 預測區間及其他綜合損益匯率與金融資產價格風險。正式財報的會計師核閱報告簽署日及公司公告日均為 2026/08/11；8/14 是入口索引日期。TWSE 財測差異清單 10/1 出表，比 8/11 結果公告晚 51 日（若與入口 8/14 索引日期相比則晚 48 日）。[大聯大財報／財測索引](https://www.wpgholdings.com/investors/financial_highlights/zhtw/audited-financial-reports)、[Q2 財測原件](https://www.wpgholdings.com/investors/financial_highlights/zhtw/audited-financial-reports/download/FRP00000167)、[Q2 合併財報原件](https://www.wpgholdings.com/investors/financial_highlights/zhtw/audited-financial-reports/download/FRP00000168)
- 公司 8/11 公告確認 Q2 營收、營業淨利、淨利及 EPS 超越財測。合併財報單位為新台幣仟元；Q2 實績：營收 456,210,621、毛利 20,196,814、營業利益 11,953,893、稅前淨利 11,295,755、合併總淨利 8,775,847、歸屬母公司業主淨利 8,620,054、基本 EPS 5.12。財測表 Q1「稅後淨利」實績 5,548,632 與 Q1 母公司業主淨利一致，因此可比 Q2 預測上緣 6,824,000 的是母公司業主淨利 8,620,054（+26.3%），不是合併總淨利；營收、毛利、營業利益、稅前淨利、母公司業主淨利及 EPS 相對各自區間上緣約高 25.0%、24.3%、24.0%、31.1%、26.3% 及 26.1%。官方報告稱營業利益率 2.62%；以預測營業利益上緣/營收上緣估算為 2.64%，所以絕對營業利益超標主要伴隨營收規模超標，不能宣稱營業利潤率明顯超預測。[公司 8/11 公告](https://www.wpgholdings.com/investors_msg/detail/zhtw/other/3081)
- TWSE 財測差異原始值 3702 當季綜合損益實績 9,113,403 對預測上緣 5,377,000（+69.5%）、累計實績 17,106,806 對預測上緣 13,370,403（+27.9%），現在可與財報逐欄對上：Q2 9,113,403 是「歸屬母公司業主之綜合損益總額」，不是營業淨利、稅前淨利或單純淨利；H1 17,106,806 同樣是母公司業主綜合損益。Q2 財測附註特別說明，母公司業主綜合損益受匯率及金融資產市價變化影響，波動可能重大且不影響 EPS。因此 +69.5% 必須標作含 OCI 的綜合損益差異；EPS/淨利/營業利益超財測則另列為營運獲利證據。TWSE API 未列單位，數值維持原樣；不把官方差異率等同市場共識 surprise。方向家族仍不加票，差異報告屬已延遲披露的歷史校準資料。

### 可前瞻檢驗的 Q3 指引

- 8/11 公司公告以 NT$32.2/USD 為匯率假設，Q3 營收預估 NT$4,600–5,000 億、毛利率 4.35–4.55%、營益率 2.78–2.91%、稅後淨利 NT$95.02–107.18 億、EPS 5.45–6.15。這是第一手、可量化、具到期結果的發行人指引；並非券商目標價，也未提供可重建的敏感度／下行情境。其 EPS 區間較 Q2 實際 EPS 5.12 高約 6.4–20.1%，應列為待驗預測，不當成獲利已實現。
- 公司 7/9 公布的 6 月營收為 NT$1,929.2 億；8/10 7 月營收 NT$1,461 億、9/10 8 月營收 NT$1,422.4 億，兩個月合計 2,883.4 億。要達 Q3 營收財測低標／中點／高標，9 月需分別約 1,716.6／1,916.6／2,116.6 億；相對 6 月，低標月量約低 11.0%、中點低 0.7%、高標高 9.7%。這使 9 月公司營收正式公告成為下一個明確可核的檢查點；7、8 月公司月營收附註均註明未經會計師查核／核閱，不把兩月 run-rate 當成季財報結果。[6月/第二季營收](https://www.wpgholdings.com/news/detail/zhtw/revenue/26040)、[7月營收](https://www.wpgholdings.com/news/detail/zhtw/revenue/26044)、[8月營收](https://www.wpgholdings.com/news/detail/zhtw/revenue/26055)
- 版本對帳仍未完：7/9 自結營收公告稱 Q2 NT$4,582.8 億、H1 NT$7,747.8 億；公司目前月營收表以百萬元列 4–6 月 127,338/138,024/192,916，合計 NT$458,278 百萬元，H1 月值合計 NT$774,778 百萬元，均註明 2026 數據為內部結算、未經會計師查核。8/11 核閱合併財報列 Q2 NT$456,210.621 百萬元、H1 NT$772,711.078 百萬元；以月表合計計，Q2/H1 分別高 NT$2,067.379/2,066.922 百萬元（約 20.674/20.669 億）。Q1 月表合計 316,500 百萬元，與正式報表推算 Q1 316,500.457 百萬元近似，差額集中於 Q2。尚未找到公司對這項月報自結與季報核閱差異的版本說明或修正軌跡；不能推定為重編、更正或錯誤。做財測誤差回測時，季度獲利與現金流以核閱季報為準，月營收保留原申報值與未核閱標籤。[公司 7/9 公告](https://www.wpgholdings.com/investors_msg/detail/zhtw/other/3064)、[公司月營收表](https://www.wpgholdings.com/investors/financial_highlights/zhtw)

### 券商原文查找結果

- 大聯大 IR「產業研究券商」頁只列 Credit Agricole、Credit Suisse、Deutsche、Morgan Stanley、Yuanta，沒有報告日期、作者、全文或模型。[公司券商索引](https://www.wpgholdings.com/investors/investor_services/zhtw/analyst-coverage)
- 元大證券網站可找到 5/20 時報記者文章，轉述未具名美系外資對大聯大 2026–2028 EPS 上修及目標價由 121 元升至 160 元、使用剩餘收益模型；它不是該外資券商原始研究，未核作者、資料截止、模型輸入、風險與 EPS 上修橋接，不納入方向訊號，也不推測報告動機。[元大網站轉載的時報文章](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202605200093850)
- 後續追蹤：等 9 月正式營收與 Q3 合併財報，對照 8/11 公司指引；回查 2026Q2 財報全文及 Q2 月營收修訂史；只有取得券商原文才加入券商預測誤差、估值與反方風險比較。未因單一公司調整方向門檻或發送規則。


## 2026-10-03 02:30 大聯大 Q2 原件覆核：獲利超財測但現金轉換弱

### 一手原件與報告品質

- 已取得並讀取公司 IR 的 Q2 簡式合併財測（FRP00000167）與 2026Q2 合併財報（FRP00000168），含 PDF 原頁視覺覆核。財測 5/11 編製、5/12 董事會通過，Q2 美元/台幣假設 31.6，警示預測與實際可能有重大差異；預測表同時列營收、毛利、費用、營業利益、稅前/稅後淨利、EPS 及母公司業主綜合損益。這是公司原始財測，不是券商報告；沒有作者個人署名、可重建產品組合/匯率敏感度或完整下行情境，不替它打券商研究品質分數。[Q2 財測原件](https://www.wpgholdings.com/investors/financial_highlights/zhtw/audited-financial-reports/download/FRP00000167)
- 財報是 PwC 核閱、不是年度查核；核閱報告日期 8/11 並出具「保留結論」。基礎是部分非重要子公司及權益法投資使用同期間自行編製、未經會計師核閱的報表；其 2026/6/30 資產 202,012,876 仟元占合併資產 33%、負債占 28%，Q2/H1 綜合損益分別占 28%/24%。這不是整份報表無效或已證實錯報，但對重大子公司/投資資訊的核閱保證有限，應與超財測敘述同卡呈現。[Q2 財報原件](https://www.wpgholdings.com/investors/financial_highlights/zhtw/audited-financial-reports/download/FRP00000168)

### 獲利差異的拆項

- Q2 合併收入 456,210,621 仟元、毛利 20,196,814、營業利益 11,953,893、稅前淨利 11,295,755、合併總淨利 8,775,847；歸屬母公司業主淨利 8,620,054、基本 EPS 5.12。與 5/12 財測區間上緣比較：收入 +25.0%、毛利 +24.3%、營業利益 +24.0%、稅前 +31.1%、母公司業主淨利 +26.3%、EPS +26.1%。收入規模強且淨利/EPS 同步超標，支持歷史「營運獲利超公司財測」標記；營業利益率 2.62%，接近財測表高端營益額/高端收入所隱含的 2.64%，因此不能把獲利超額簡化成利潤率大幅擴張。預測表「稅後淨利」採可比母公司口徑（Q1 財測表 5,548,632 仟元等於 Q1 母公司業主淨利），不可拿合併總淨利替代。
- TWSE 10/1 差異表的 Q2 9,113,403 與 H1 17,106,806 可由 Q2 報表逐欄確認為歸屬母公司業主「綜合損益總額」。Q2 合併 OCI 493,922 仟元，母公司淨利 8,620,054 仟元；財測原文另註 OCI 易受匯率及金融資產市價影響且不影響 EPS。故 +69.5% 綜合損益差異是含 OCI 的獨立項，核心盈利 beat 要用淨利/EPS/營業利益等口徑另呈現。

### 現金流與營運資金反向品質訊號

- H1 合併營業現金流為淨流出 NT$98,898,501 仟元，去年同期淨流入 NT$18,397,568 仟元；同期 H1 營收年增 54.8%、淨利 14,526,635 仟元。現金流調節表列應收帳款增加耗現金 129,953,175 仟元、存貨增加耗現金 72,334,783 仟元；應付帳款增加回補 92,395,952 仟元、合約負債增加回補 9,840,646 仟元。6/30 應收帳款 292,054,831 仟元、存貨 215,019,090 仟元，分別較 2025/6/30 高約 87.3%/62.3%，高於 H1 營收增幅。這是顯著營運資金吸收現金、值得後續追收款/庫存周轉與 Q3 CFO 的反向品質訊號；配銷業成長可能使應收與備貨同步擴張，單看一個 H1 不足以判定壞帳、需求反轉或調低規則門檻。
- 追蹤卡片/評估資料應將「收入與 EPS 超財測」和「現金轉換差、應收及存貨快於營收增長」並列，保留營運資金季節性/業務規模變動為待查因素；本次只形成單一公司案例，不改方向家族數、候選門檻或通知邏輯。

### 月營收與季報對帳狀態

- 目前 IR 月營收頁列 4/5/6 月 127,338/138,024/192,916 百萬元，Q2 合計 458,278 百萬元；核閱季報為 456,210.621 百萬元，差 2,067.379 百萬元。H1 月值合計 774,778 百萬元，核閱季報 772,711.078 百萬元，差 2,066.922 百萬元；Q1 三月月值合計與季報推算只差 0.457 百萬元。自結月營收頁仍標示「內部結算、未經會計師查核」；目前未找到公司對差異的修訂公告或調節說明。保留每版發布日與數值，不推測差異由收購重組、匯兌、會計調整或更正造成。
- 下一個檢查點是公司正式發布 9 月月營收後，檢驗 8/11 的 Q3 營收指引；待第三季合併財報核閱後再驗毛利率、營益率、EPS、CFO 及營運資金是否改善。10/1 TWSE 差異表對應的是 8/11 已公開實績，歷史校準價值高、即時交易增量弱；未找到可核原文券商報告，本次沒有券商方向評分，也不根據轉載文章推測報告動機。

- 本輪只更新追蹤文件與授權 memory ad-hoc note；未改程式、未跑測試或 DAG、未發 Discord、未交易；所有其他既存 dirty changes 保留。PDF/頁面影像留在 `tmp/pdfs/stock-signal-q2`；自動審核政策拒絕本輪對該目錄的刪除操作，因此尚未清理，內容僅為本輪下載的兩份公司原件與其頁面檢視圖。

## 2026-10-03 02:51 聯亞 EPS 股本基礎與擴產資本支出追蹤

### 最新自然資料狀態

- 唯讀查核截至台北 10/3 02:51：最後日線訊號仍是 10/2 20:02 的 ready/partial 快照（候選 50、通知 0）；最後盤中掃描為 10/2 22:45 ready/fresh（候選 0、通知 0）。成熟 outcomes 仍只有 9/24 訊號日的 81 檔單日樣本，signal_close 與 next_close 各 81 筆；尚無 5/10 日成熟樣本，不調門檻。
- 10/2 20:00 after-close 收集器驗證仍因 TPEx 個股日線報價兩次失敗而不完整；失敗來源 raw/接受/隔離為 0/0/0，其他來源的有效日、接受/隔離及市場範圍以 00:33 與 01:58 區段逐源表為準。任務 success 不等於日線完整。
- 公司公告來源最後一次自然 intraday map 為 10/2 22:45，TPEx MOPS 公告 API 42/42/0；這是 TPEx 公司公告 feed，不代表上市櫃全部證券或新聞來源都完整。資料庫內 3081 最新公告有效日仍是 9/15。新聞自然批次仍是 10/2 約 23:30–23:41；發稿時間修正尚未有下一次自然批次驗證。

### 永豐公開摘要：有預測，券商原模型未核

- 4/23 公開摘要署名「研究小編」，稱 1Q26 獲利與毛利率改善高於預期，並援引 Sumitomo 5 年 InP 基板供貨合約、後續擴產，列 2026/2027 EPS 20.34/40.95 元及維持看好。頁面沒有個別分析師、明確資料截止日、EPS/營收橋接、估值倍數或目標價；只說市場可能給較高 P/E，沒有可重建估值或下行情境。[永豐 4/23 公開摘要](https://www.sinotrade.com.tw/richclub/StockNavigator/%E5%85%89%E9%80%9A%E8%82%A1%E7%8E%8B---%E8%81%AF%E4%BA%9E-3081--69e96b4886807e0a5321b9b7)
- 8/20 公開摘要同樣署名「研究小編」，列 Q3 EPS 預估 6.08 元，稱 7 月 EPS 2.07 元達成 34%，並預期 8、9 月獲利墊高、Q3 有機會優於預期；全文原模型、資料截止日、估值方法、修正紀錄及反方風險未公開核實。算術上 6.08−2.07=4.01 元尚需由 8、9 月合計達成；8 月營收上升不能替代 EPS 檢驗。[永豐 8/20 公開摘要](https://www.sinotrade.com.tw/richclub/StockNavigator/3081-%E8%81%AF%E4%BA%9E-7%E6%9C%88EPS-2-07%E5%85%83%E7%AC%A6%E5%90%88%E9%A0%90%E6%9C%9F-%E5%8A%A0%E9%80%9F%E6%93%B4%E7%94%A2%E7%87%9F%E9%81%8B%E5%8B%81%E6%8F%9A-6a86550ca9d77c0b180d468c)
- 兩頁都是券商公開摘要，不等於已核完整賣方原文；作者欄不是具名分析師。可評估其可檢驗預測，不能據此判斷出刊動機，也不把摘要直接計成獨立多空家族。
- 另以 3081/聯亞、2026 年 5 月等關鍵字查過國票 ibt.com.tw/ibfs.com.tw 官方站，未找到可核原件；這是公開索引未找到，不證明報告不存在，第三方轉載不納入評分。

### 官方 EPS 同股本口徑校正

- 公司 Q1 核閱財報 4/22 通過發布：Q1 基本 EPS 3.44 元，當時財報普通股股本 925,173 仟元。[聯亞 Q1 原始核閱財報](https://www.lmoc.com.tw/index.php?i=1&id=366&lang=cht&option=module&task=dfile)
- TPEx/MOPS 8/19 公告明列 2026Q2 與 7 月 EPS 都是「除權後追溯調整」口徑：Q2 單季 4.62 元（公司稱已核閱）、7 月 2.07 元（公司明示未經查核/核閱）。8/12 公司 Q2 公告列 H1 累計基本 EPS 7.75 元；以後續除權調整口徑扣回 Q2，推得 Q1 同口徑約 7.75−4.62=3.13 元。這是推算值，不是公司另發的 Q1 修正版；不能把原報 3.44 與調整後 4.62 混加成 8.06 元。[TPEx 官方 MOPS 公告資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 4/23 年度 EPS 預估 20.34/40.95 元的股本基礎未能從摘要確認；不得自動用 1.1 去調整或和追溯 EPS 拼接。比較券商預測時保存原文每股基礎、預測版本與除權事件日期，基礎不明就標未核。
- 公司 IR 月營收索引目前列到 8 月：8 月 NT$520.469M，較 7 月 NT$501.210M 月增 3.8%，較 2025/8 NT$185.285M 年增 180.9%；1–8 月累計 NT$3,147.246M，去年同期 NT$1,374.127M，年增 129.0%。9 月欄仍空白；營收趨勢支持成長背景，但不等於已驗證永豐 Q3 EPS 預估。[聯亞 IR 財務資訊](https://www.lmoc.com.tw/index.php?lang=cht&temp=investment)

### 新的中性擴產/融資觀察，不作方向票

- TPEx/MOPS 公司公告：8/13 無塵室擴建工程 NT$1.2264B；9/15 生產用機器設備約 NT$0.535B，兩項合計 NT$1.7614B。以 8/12 公告 H1 營收 NT$2.125567B 計為 82.9%，以 Q2 資產 NT$6.217544B 計為 28.3%。
- 公司 8/11、8/12 分別公告第一次有擔保 CB 收足 NT$1.486598B、第二次無擔保 CB 收足 NT$1.872B，合計 NT$3.358598B，相當於 H1 營收 158.0%、Q2 資產 54.0%。這些發債與擴建公告時間相近，但現有一手文件尚未證明募集款逐項指定用於上述兩筆採購；CB 轉換後稀釋股數的條件也待核。金額足以作為擴產/資本配置觀察，不直接等同訂單或 EPS 上修。
- 下一步核驗：9 月月營收發布後比對年增與月增；Q3 財報檢查 EPS、毛利/營益率、應收存貨與營業現金流；追查擴建驗收/投產節點與 CB 轉換價格、可轉股數及資金用途。若資本支出要進通知，先用多家公司/多期樣本建立相對資產或營收的規模門檻，事件保持中性且跨公告去重。

### 通知摘要窄幅修正

- 現行事件摘要只抓 JPY/USD/NTD/TWD 文字碼，對「新台幣1,226,400,000元」和「折合新台幣約5.35億元」格式會漏金額；用途欄也未涵蓋「取得或處分之具體目的或用途」。已在 dags/source_extension/market_signals.py 加入有交易/募資標籤的中文新台幣金額換算與該用途欄位；不改事件分類、方向票、候選門檻、去重或通知發送。
- 靜態 py_compile 通過；沒有跑測試、觸發/重跑 DAG、發 Discord 或交易。公司債在現有 routine 關鍵字中，機器設備事件目前仍可能落在 review；本次沒有把它們改成推播，避免以一家公司資料定全市場門檻。

## 2026-10-03 03:20 自然收盤覆蓋、聯電原始券商財測與設備公告摘要

### 自然資料與完整度

- 唯讀核對：最新 daily `market_signal_runs` 是 `as_of_date=2026-10-02`、`ready/partial`、50 個候選、0 通知；最新 intraday 是 10/2 22:45 台北、`ready/fresh`、0 候選、0 通知。`market_signal_outcomes` 仍只有 9/24 的 81 筆 `signal_close` 和 81 筆 `next_close` 單日結算，沒有成熟 5/10 日樣本，不調方向門檻。
- 10/2 20:00 台北自然 `source_extension_collect_after_close` 的 Airflow run 仍 failed：11 個 mapped source 有 10 個成功、TPEx 個股收盤價來源失敗；該 mapped source attempt 1、2 都是 `ChunkedEncodingError / IncompleteRead`，每次 raw/接受/隔離皆 0/0/0。`trigger_market_signal_notify` 成功不代表來源完整，父層 `verify_collection_and_triggers` 仍 failed；未手動觸發或重跑。

| 自然來源 | 市場範圍 | raw/接受/隔離 | 可用資料日期或狀態 |
|---|---|---:|---|
| `twse_equity_quotes_daily` | TWSE 上市 | 1051/1051/0 | 全部資料有效日仍到 10/1，並非 10/2 |
| `tpex_equity_quotes_daily` | TPEx 上櫃 | 0/0/0，失敗兩次 | 無可用有效日；不可視為零成交 |
| `twse_market_index_daily` | 加權指數 | 1/1/0 | 10/1；收盤 benchmark 落後一交易日 |
| `tpex_market_index_daily` | 櫃買指數 | 2/2/0 | 含 10/1、10/2；不可替代 TPEx 個股行情 |
| `tpex_securities_borrowing_daily` | TPEx | 814/814/0 | 資料最新日 10/1 |
| `twse_tpex_sbl_available_daily` | TWSE+TPEx | 1717/1717/0 | 資料最新日 10/2；屬可借券餘額/供給背景 |
| `twse_margin_balance_daily` | TWSE 上市 | 1039/994/45 | 資料最新日 10/2；保留 45 隔離列，不把 mapped task success 當全收 |
| `tpex_margin_balance_daily` | TPEx 上櫃 | 801/801/0 | 資料最新日 10/1 |
| `twse_trading_disposition_daily` | TWSE 上市 | 8/8/0 | 資料最新日 10/2 |
| `twse_trading_attention_daily` | TWSE 上市 | 0/0/0 | 本輪無資料列，不能由此推論其他來源也無注意股 |
| `twse_quarterly_forecast_attainment` | TWSE 上市 | 8/0/8 | 此輪 8 列全隔離；已存有效日僅至 6/30，沒有本輪可用列 |

- 同時查到的 10/2 22:45 自然公告 feed：TWSE 78/78/0、TPEx 42/42/0，庫內最新公告有效日 10/1；這只是兩個交易所公告 feed，不涵蓋上市櫃全部個股、全部新聞或所有公司報告。月營收來源 TWSE 1086 與 TPEx 892 列，最新有效月份仍是 2026/8，並非 9 月營收。
- 新聞自然 run `news_ch_collect_process` 在 10/2 23:30–23:41 台北完成，`collect_article_url`、`collect_news`、`delete_and_insert` 都 success。新聞表分開保存來源文章時間 `created_at` 與採集/寫入時間 `create_time`；表內沒有獨立、可通用信任的發布時間欄。最新 5000 個 id 樣本含九個媒體來源，但不能視為完整來源全集。最高 id 樣本中，The News Lens 一篇文章的寫入時間為 10/2 23:30、文章時間為 1/20；回查發稿頁確認是 2026/1/20 的 Brand Studio/美通社內容。此例證明採集成功不等於新發新聞，新聞仍只作背景，時間比對應使用可回查的原發稿頁。[The News Lens 原始頁](https://www.thenewslens.com/article/263815)

### 永豐投顧原始研究：聯電 2303

- 找到並閱讀永豐證券投顧原始 PDF《永豐晨訊》，封面日期 2026/5/4；聯電頁標「維持中立」。PDF 把作者歸於「研究部」，我未找到個人分析師姓名或獨立於出刊日的資料截止日；聯電分頁未列目標價或估值方法。它是券商原始研究文件，不能和網站「研究小編」摘要混為一談，也不從估值缺項推測出刊動機。[永豐投顧 5/4 原始 PDF](https://scm.sinotrade.com.tw/Article/GetFile/b6690bb5-b7fa-4fa0-9513-f235bba56fc4)
- 報告估 2026 營收 NT$2770 億（+16.6%）、毛利率 31.9%、稅後淨利 553 億、EPS 4.40 元（由 3.89 上修）；2027 營收 3116 億（+12.5%）、毛利率 34.2%、EPS 5.05 元。2Q26 預估營收 679 億、毛利率 30.5%、稅後淨利 109 億、EPS 0.87 元，並說對業外獲利採較保守估計。方向論證是 2H26 漲價與稼動率回升；反方包含本業改善慢、4Q25–1Q26 業外占比逾 30%、Fab 12i P3 折舊及能源/原料成本增加、預期已被市場反映。報告記載公司當時維持 US$1.5B 年資本支出預算；公司 7/29 為新加坡 P4 無塵室及台南新廠分階段擴產，把預算調至 US$2B。這些假設可用 2H 營收、毛利與本業利潤率檢驗；預算增加本身不是 EPS 上修，報告也沒有量化下行情境。
- 公司 7/29 原始 Q2 結果：營收 NT$68.733B（對券商估值高 1.2%）、毛利率 32.5%（高 2.0 個百分點）、歸屬母公司淨利 NT$42.260B、EPS 3.39 元。Q2 營業利益為 NT$14.950B，另有 NT$30.236B 淨業外收入，其中淨投資利益 NT$30.045B；所以淨利/EPS 超估近 3.9 倍主要由業外投資收益拉動，不可當成本業常態成長。報告明列「保守估業外」，不因結果差距臆測其研究動機。[聯電官方 2Q26 報告](https://www.umc.com/upload/media/08_Investors/Financials/Quarterly_Results/Quarterly_2020-2029_English_pdf/2026/Q2_2026/UMC26Q2_report.pdf)
- Q1 EPS 1.29 加 Q2 3.39 為 H1 4.68 元，已高於該報 FY26 EPS 4.40 元；但 Q2 非本業投資收益很大，不能把這個算術超越直接外推成全年 EPS。公司當時給 Q3 可驗預測為出貨量高個位數成長、ASP 穩、毛利率中 30% 區間、稼動率逾 90%，並把 2026 資本支出預算提高到 US$2B；等自然公告/財報逐項結算，不以 Q2 非經常性收益代替本業驗證。[聯電 7/29 官方新聞稿](https://www.umc.com/zh-TW/News/press_release/Content/investor/20260729)

### 10/1 官方設備與廠務事件樣本

- 標題關鍵字在 TWSE/TPEx 官方公司公告 feed 命中 8 列：7 個可能工程/設備事件與 1 個只因公司舊名含「設備」而命中的 2072 更名公告。這是小型試抽，不是母體門檻校準。
- 2303 聯電公告機器設備 NT$6.984B，交易期間 2025/12/23–2026/10/1，非關係人，供生產及研發；相當 Q2 資產約 1.05%、1–8 月營收約 3.91%。
- 2458 義隆公告把 2023 年既有 AI 園區廠辦預算由 NT$6.5B 調高至 7.5B，新增 NT$1B；公司說明原因是建築方案調整及原物料/人力成本上漲。增額約為 Q2 資產 5.94%，但不是新增產能或訂單證據；8 月營收年減 36.7%，需把成本壓力和收入變弱作反向品質資訊。
- 3711 日月光投控代矽品公告 14 批設備 NT$1.130B、廠務工程 NT$2.365B（2026/8 至 2029/9）、另一廠務工程 NT$1.055B（2026/9 至 2027/12）；公告合計約 NT$4.549B。相對母公司 Q2 資產約 0.43%、1–8 月營收約 0.87%；子公司單體比例、逐季認列及實際投產尚未核，不能把多年契約合計視作當季支出。
- 6257 矽格公告約 NT$549M、17 批設備，事實/交易期間 2025/9/30–2026/10/1，作營業用途；需再以公司資產/營收和設備到貨或稼動資料核相對重要性。
- 6274 台燿子公司工程約 THB 1.029B，公司公告約折合 NT$982.375M，契約期依建造進度規劃；10/1 是更正既有公告的關係人交易勾選，不可再次加總為新工程金額。TPEx/MOPS 原始公告 feed：[上市公司 TWSE feed](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)、[上櫃公司 TPEx feed](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)。
- 跨公司樣本支持摘要需保留「幣別及公司聲明折算值、公告/契約期間、前次公告日期、更正原因、工程款是總額還是增額、子公司/母公司基準、費用上升或營收走弱」。固定絕對金額會忽略公司規模、期間與更正重複；暫不改通知分類或任何方向門檻。

### 本次窄幅摘要改進與待辦

- `_summarize_company_event` 現能摘要「事實發生日/交易期間」、「契約起迄日期」、明列的「折合新台幣」及泰銖原幣金額，並提示前次公告日期與「誤勾關係人」更正，避免把總契約、增額與重發公告混成新訊號。只改摘要，不改 `_classify_company_event`、方向票、materiality、候選門檻、去重或 Discord 發送。
- 靜態 `python -m py_compile dags/source_extension/market_signals.py` 通過；未跑測試、未觸發/重跑 DAG、未發 Discord、未交易。下一輪等 9 月營收、Q3 財報與自然收盤來源修復後，再驗 UMC 指引與資本支出樣本；持續收集多家公司、多期 CapEx/預算更正，補齊 6257 基準及子公司單體資料，不因本樣本調閾值。


## 2026-10-03 03:31 財測達成全隔離根因與 catalog 窄修

### 自然行情與來源覆蓋

- 唯讀即時覆核截至台北 10/3 03:31：最新日線仍是 10/2 `ready/partial`、50 候選、0 通知；最後盤中掃描 10/2 22:45 `ready/fresh`、0 候選、0 通知。成熟 outcome 仍只有 9/24 `signal_close` 和 `next_close` 各 81 筆，沒有 5/10 日成熟樣本。10/2 自然收盤 TPEx 個股日線來源失敗、raw/接受/隔離 0/0/0；不把失敗或缺列視為零量，也不調方向門檻。
- 官方 `t187ap15_L` 即時回應 HTTP 200、8 列、6 個不同上市代碼（2412、2454、2515、3036、3702、4904），資料期 2026Q2、出表 10/1、財報期末 6/30。10/2 自然 after-close 收集紀錄為 raw/接受/隔離 8/0/8；現存 8 筆也全部是 `quarantine`，並非八筆有效歷史樣本。[TWSE 財測達成原始 API](https://openapi.twse.com.tw/v1/opendata/t187ap15_L)
- 原因已定位：適配器依目前契約保存 `__reported_actual`、`__reported_forecast` 原字串及 `coverage_period`，預測區間字串非空即保留，不計算達成率；但 DB `source_fields` 還有已從現行來源定義移除的 `__cumulative_actual`、`__cumulative_forecast` 兩個舊必填欄。`_required_fields_missing` 因此把每列都隔離。這不是 TWSE 區間值解析失敗；例如 2454 預測原值 `-20510430~32087313` 仍會原樣保存。這個端點「截至該季實際」與公司預測「涵蓋期間」亦可能不是同一期間（多列涵蓋期間為「二」），所以先保留觀察資料，待逐公司原始財測/財報配對後，才考慮是否可回測；目前不算達成率、不通知、不加多空分數。
- 已在 `registry.py` 的 catalog bootstrap 加入窄幅舊欄位停用：對每個程式定義的來源，停用仍 enabled 但已不在當前 schema 契約內的舊 source fields，再照現行欄位定義 upsert。沒有直接改 DB；須由下次自然 source-extension DAG 執行 bootstrap 後，才有機會看到這 8 列進入接受區。`python -m py_compile dags/source_extension/registry.py` 通過；未跑測試或 DAG。

### 差異事件與通知 ledger 對帳

- 官方 `t187ap16_L` 即時回應仍為 7 列、5 家上市公司；2515 中工序號 0/1/2 三列財測值一致，語意候選收斂為單筆。10/2 20:00 與 20:15 台北的兩批舊候選仍各留下 5 筆 `sent` ledger，合計 10 筆；來源實績/預測語意相同但舊新 source row / candidate hash 不同，語意指紋的相容性修正不會回收已送出的歷史卡。22:45 後未查到更多財測卡；不手動補發。[TWSE 財測差異原始 API](https://openapi.twse.com.tw/v1/opendata/t187ap16_L)、[MOPS 財測差異專區](https://mops.twse.com.tw/mops/web/t120sb02_q10)
- 已存的 20:00 卡片 payload footer 未帶「金額單位未提供」，雖目前工作樹渲染器已加上單位未知提示；最後一次自然盤中掃描沒有候選，所以新渲染器的實際 Discord payload 尚未經自然資料驗證。只保留此待核，不發測試訊息；券商或 TWSE 財測資料都不能單獨作為交易方向。
- 本輪只做唯讀 DB/官方 API 核對、`registry.py` catalog 修正、追蹤文件和授權 memory note；未手動改資料庫、未觸發/重跑 DAG、未發 Discord、未交易，所有其他既有 dirty changes 保留。

## 2026-10-03 04:00 益得單月營收低基期與月/累計訊號衝突

### 最新快照與來源完整度

- 台北時間 10/3 04:00 唯讀核對：最新日線 `as_of_date=2026-10-02`，20:02 `ready/partial`、50 候選、0 通知；最新盤中 10/2 22:45 `ready/fresh`、0 候選、0 通知。成熟 outcome 仍只有 9/24 `signal_close`、`next_close` 各 81 筆單日結算，沒有成熟 5/10 日樣本，不調候選或通知門檻。
- 自然 after-close Airflow run `scheduled__2026-10-01T12:00:00+00:00` 於 10/2 20:00–20:04 台北時間執行，父層仍 failed；11 個 mapped source 共 12 次 attempt。逐源 raw/接受/隔離與資料日如下：

| 自然來源 | 市場覆蓋 | raw/接受/隔離 | 有效日期/狀態 |
|---|---|---:|---|
| `tpex_equity_quotes_daily` | TPEx 個股 | 0/0/0，重試後失敗 | 無可用日期，不代表零成交 |
| `twse_equity_quotes_daily` | TWSE 個股 | 1051/1051/0 | 最新仍為 10/1 |
| `tpex_market_index_daily` | TPEx 市場指數 | 2/2/0 | 含 10/1、10/2，不能替代個股報價 |
| `twse_market_index_daily` | TWSE 市場指數 | 1/1/0 | 10/1 |
| `tpex_margin_balance_daily` | TPEx 融資融券 | 801/801/0 | 10/1 |
| `twse_margin_balance_daily` | TWSE 融資融券 | 1039/994/45 | 10/2，45 列隔離 |
| `twse_tpex_sbl_available_daily` | TWSE+TPEx 可借券餘額 | 1717/1717/0 | 10/2；是供給背景，不是個股成交訊號 |
| `tpex_securities_borrowing_daily` | TPEx 借券 | 814/814/0 | 10/1 |
| `twse_quarterly_forecast_attainment` | 本批有財測達成列的 TWSE 上市公司 | 8/0/8 | 本批無接受列；隔離列所屬財報期末 6/30 |
| `twse_trading_disposition_daily` | TWSE 處置證券 | 8/8/0 | 10/2 |
| `twse_trading_attention_daily` | TWSE 注意證券 | 0/0/0 | 本批無列，不能推論其他來源無注意股 |

- 最新新聞自然 DAG `news_ch_collect_process` 仍是 10/2 23:30–23:41 台北時間 success；週六尚無新日線收盤或新聞批次。成功只代表該次工作流完成，不等於全部媒體完整或每篇都是當日新稿。資料庫保留原文 `created_at` 與寫入 `create_time`，The News Lens 1/20 Brand Studio 舊稿在晚間才寫入的案例仍是 freshness 提醒；新聞只列背景，不計方向票。[原發稿頁](https://www.thenewslens.com/article/263815)

### 6461 益得：單月 +1103% 是低比較基期，需連同累計及公司說明

- 台北時間 10/3 約 03:55 直接讀取 TPEx 官方月營收 OpenAPI，6461 的 `資料年月=11508`、`出表日期=1150917`：8 月營收 NT$5.654M，去年同期 NT$0.470M，年增 1102.98%；1–8 月累計 NT$42.312M，去年同期 NT$96.602M，累計年減 56.20%。公司原註明單月增加主要由經銷商備貨增加；累計減少主要因去年認列海思科完成合約技術服務收入 6,804 萬元。[TPEx 官方月營收原始 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 事實與判讀分開：巨大單月年增的分母僅 NT$0.470M，不能單獨代表可延續成長；公司說明同時指出通路備貨。另一方面，累計下降含去年一次性技術服務收入比較基期，亦不能直接解讀為本年核心營運同比萎縮。需要下一期營收是否重複、季報毛利/營益/現金流與經銷商庫存訊息作驗證，不能由營收推成 EPS 或獲利上修。
- 10/2 候選將 6461 列偏多、4 個有方向家族；候選 `details_text` 留有 2026/08 與單月年增，卻沒有 `revenue_context`、單月金額、累計年增、公司備註或來源 URL。當日候選 run 20:02 台北時間；目前 TPEx 8 月來源列最後更新時間為 10/2 14:45 UTC（22:45 台北），晚於掃描。候選快照保存的年增值與目前 TPEx 原始值吻合，但目前 upsert 後的來源列不能還原掃描當時 payload/欄位版本；需等下一個自然日線 run 核對 context、來源及資料截止，而非回填或手動重跑。
- 以 10/2 的 50 個候選對照官方 8 月月營收：9 檔單月年增與 1–8 月累計年增方向相反（5 檔單月正、累計負；4 檔單月負、累計正）。9/24 的 61 檔候選有 10 檔方向相反，但仍是同一個 8 月財報月份，不能當作多期獨立驗證。這證明值得保留兩個期間並顯示矛盾，不足以單日調方向門檻或直接決定計分規則。

### 決策與下一步

- 本輪沒有改候選/通知演算法或程式，沒有觸發/重跑 DAG、Discord 通知或交易；只讀資料庫、官方 TPEx API，並更新追蹤文件與已授權 memory note。
- 下一輪自然掃描先確認營收卡是否真的呈現當月金額、累計 YoY、公司備註與官方 URL；後續跨多個公告月份，把單月/累計矛盾與季度獲利、現金流逐期對照，再考慮是否將矛盾的營收家族列中性。待 9 月營收公布後另行建立新發布期樣本；不把同一 8 月公告的重複日快照視為新證據。原始券商研究清單本輪無新增可核全文，不據二手摘要推論券商假設或動機。

## 2026-10-03 04:33 滾動營收背景、新聞樣本回收與 8084 官方風險漏判

### 自然資料完整度（台北時間 10/3 04:33）

- 週六凌晨沒有新的市場自然批次；最新日線仍為 10/2 20:02 `ready/partial`、50 候選、0 通知；盤中最近一筆是 10/2 22:45 `ready/fresh`、0 候選；`market_signal_outcomes` 仍只含 9/24 的 `signal_close` 與 `next_close` 各 81 筆，沒有成熟 5/10 日樣本。
- 最新 after-close Airflow 父 run 仍 failed（10/2 20:00–20:04 台北），11 個 mapped source / 12 次 attempt；失敗來源 `tpex_equity_quotes_daily` raw/接受/隔離 0/0/0，不代表上櫃零量。TWSE 個股日線 1051/1051/0 只到 10/1；上市融資融券 1039/994/45，櫃買融資融券 801/801/0、櫃買借券 814/814/0 到 10/1；跨兩市可借券餘額 1717/1717/0 到 10/2；上市處置 8/8/0、上市注意股本批 0/0/0、TWSE 財測達成 8/0/8。精確逐來源覆蓋承接 04:00 表格，父層綠燈/部分 trigger 不能替代逐來源完整性。
- 月營收自然 run 10/2 22:45：TWSE 1086/1086/0、TPEx 892/892/0，最新有效月仍為 2026/8。盤中 TWTB4U/TWTB5U 契約差異仍待確認；TPEx 個股當沖 endpoint 空資料及個股網站授權仍未確認。TPEx 官方 OAS 11627／當沖統計只屬市場彙總，需 freshness gate，不當個股訊號。新聞最近自然 run 10/2 23:30–23:41 success；沒有新券商原始研究全文可評，不從二手摘要推報告假設或動機。

### 月營收方向衝突與滾動三月背景

- 對 2026/6–8 官方月營收 accepted records 逐市場檢查：TWSE 各月 1082、1085、1086 列；TPEx 各月 891、890、892 列；各市場月/代碼 grain 無重複。單月 YoY 與累計 YoY 方向衝突，上市各月 225/1082（20.8%）、232/1085（21.4%）、225/1086（20.7%）；上櫃 199/887（22.4%）、177/888（19.9%）、197/887（22.2%）。
- 連續三個月完整資料共 1,963/1,976 個上市櫃代碼，缺至少一期 13 檔。2026/6–8 的單月 YoY 與 3M 合計 YoY 相反：TWSE 145/1076（13.5%），TPEx 126/887（14.2%）；YTD YoY 與 3M 合計相反：TWSE 165/1076（15.3%），TPEx 137/887（15.4%）。10/2 的 50 候選中 44 檔有完整 3M 值，其中單月/3M 矛盾 4 檔、YTD/3M 矛盾 3 檔。這是同一組月份的橫截面/期間比較，不是多個獨立發布期的預測驗證，未調任何方向票或門檻。
- 窄幅程式改進：`registry.py` 取得官方去年同期單月營收欄位；`market_signals.py` 對三個連續、已發布且有效的月份計算滾動 3M YoY，只放入營收背景並標明與單月/YTD方向衝突，不改方向家族、分數、候選門檻、雜湊或通知路由。下一自然 run 才能驗證新欄位入庫及候選卡呈現。
- 可重跑分析草稿：[docs/stock_signal_monthly_revenue_quality.ipynb](docs/stock_signal_monthly_revenue_quality.ipynb)。已修正 notebook source 行界線；JSON 解析與唯一非空 Python cell AST 解析通過，未執行 notebook。

### 新聞發布時間品質與候選新聞回收

- 近 5000 個新聞 ID 檢視：TNL 102 篇中 89 篇的資料表文章時間超過 48 小時（含 2026/1/20 Brand Studio 舊稿於 10/2 才寫入）；iThome 102 篇中 57 篇超過 48 小時。寫入成功不等於新稿；保留文章原始時間與採集寫入時間，回查原發稿頁。[TNL 原發稿頁](https://www.thenewslens.com/article/263815)
- 舊 2000-ID 取樣在 10/2 20:02 回放時，113/2000 列已超出 48 小時，佔 5.65% 樣本名額。把時間過濾放到 ID 窗內、最後候選樣本前，並設最新 10,000 個 ID 的掃描上限及合格文章最多 4,000 篇；同一觀察時間以目前保留表回放，合格樣本達 4,000 cap，找回原樣本漏掉的 3564 其陽、8042 金山電、8084 巨虹文章。8084 回放命中 ETtoday 全額交割/私募失敗報導及中時報導。這證明有增量召回，不證明最近 10,000 ID 外沒有文章；因 4,000 cap 已滿，超出上限的完整合格文章數未知。新聞保持非方向背景。

### 8084 巨虹 TPEx 官方重大訊息：原通知分類漏掉具體條件風險

- `tpex_company_events` 官方來源的 accepted source record 1399539/1399540，皆為 `quality_status=ok`。9/30 記者會公告本文稱私募案未收足、失效；子公司收購條件未成就，賣方「得」沒收美金 3,000 萬訂金；公司表示若條件成立、存在現時義務且損失可可靠衡量，Q3「可能」認列損失；淨值可能為負、可能終止櫃買，金額與認列時點待結算及會計師核閱。公司另回答子公司第三方股東持有 99.99%、巨虹持有 0.1%，自身實際承擔比例及認列尚未確定。另一筆公告交易方法自 10/2 起變更為先收足款券。[TPEx 上櫃重大訊息 feed](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 時間記錄：DB `event_time` 分別為 9/30 22:54:35、23:04:12（資料列未帶 timezone）；第一筆本文另稱記者會/發布資訊時刻為 9/30 19:30。這兩個欄位語義不同，不能拿 `event_time` 直接冒充本文記載的發布時刻。
- 原分類器把 1399539 歸為 `routine`（標題含「召開說明會」的例行詞優先，正文的有條件淨值/櫃買風險未被捕捉）；1399540 歸 `review`。兩筆按當時邏輯均未進 intraday key-candidate ledger；依 8084/巨虹文字回查 10/1–10/2 通知 ledger 亦無紀錄。10/2 daily 8084 候選仍偏多（2 個偏多家族、1 個偏空家族，5 日報酬 -40.68%），`news_context=no_match_in_sample` 且無 regulatory context，顯示跨源風險提示缺口；不把公司條件式聲明寫成已確定損失或退市。
- 窄幅修正 `_classify_company_event`：只有正文同時出現「可能導致期末淨值為負數」、「終止櫃檯買賣」及「認列相關損失/全額損失」才將其升為 `key` 條件式偏空風險事件；說明金額、時點、公司負擔比例待確認。不改日線計分或門檻。歷史 source row 不回填、不重播；本次沒有補發 Discord，修正只對之後自然掃描適用。

### 變更與核對界線

- `market_signals.py` 新增 10,000-ID/4,000 合格新聞上限、顯示實際合格樣本數及窄幅淨值風險分類；`registry.py` 加去年同期單月營收正規化欄位。兩檔 `py_compile` 通過；notebook JSON/AST 通過。沒有執行測試或 notebook、手動觸發/重跑 DAG、寫資料庫、發 Discord、交易。工作樹其他變更保留。
- 下一自然掃描核對新聞資料數及 8084 以外的官方事件；9 月營收公告後再看跨發布期樣本。報告品質仍只讀券商原始報告並記作者、日期/資料截止、估值、EPS/營收假設與修訂、反方風險及可檢驗預測；無原文時標未核，不猜動機。

## 2026-10-03 04:54 FCC 光模組消息校驗與 4991 高分候選反方覆核

### 最新來源執行狀態與時間語義

- 唯讀重查 Airflow：`source_extension_collect_intraday` 最近 run 為 `scheduled__2026-10-02T14:30:00+00:00`，台北 10/2 22:45 成功；`news_ch_collect_process` 最近 run 預定 10/2 18:30、實際 23:30–23:41 成功。`source_extension_collect_after_close` 最新列的 run_id/execution_date 是 `scheduled__2026-10-01T12:00:00+00:00`（10/1 20:00 台北），實際到 10/2 20:00–20:04 才執行並失敗；11 個 mapped source 中 10 個成功、index 9 個股上櫃日線收集失敗，`verify_collection_and_triggers` 失敗。run_id 排程日期與實際開始日相差一天，記錄兩者，不臆測延遲原因。沿用上節核對：失敗來源 raw/接受/隔離 0/0/0，不可當作上櫃股票零量；月營收最近有效月份仍 2026/8。截至台北 10/3 04:54 尚無新交易日 run：最近日線為 10/2 20:02 `ready/partial`、母體 1,946、候選 50、通知 0；盤中為 10/2 22:45 `ready/fresh`、候選/通知 0/0。`market_signal_outcomes` 仍只有 9/24 的 `signal_close` 與 `next_close` 各 81 筆，沒有成熟 5/10-session 樣本。
- 以新聞主鍵查得 `id=18918813` ETtoday 文章 URL `3247759` 是光通訊新聞，DB `created_at=12:43`、寫入 `create_time=23:30`；原頁 `datePublished=2026-10-02T12:43:00+08:00`，與 parser 直接解析頁面台北時間相符，早於 20:02 訊號快照，沒有快照後倒灌。不要與 ETtoday `3247556` 混為一篇：其 10:34+08 原頁是被動元件新聞，正文提 6174/8289，不是 4991。鉅亨 Cnyes 6620290 的原始 `05:20:12Z` 為台北 13:20:12，但舊 DB `created_at` 存 05:20；因此在本次 20:02 掃描仍是已發稿內容，只會造成顯示排序或接近 48 小時邊界時的過早淘汰風險。Cnyes epoch 修正仍等下一自然收集驗證；不對所有來源做統一時區平移。

### 4991 環宇-KY：動能候選，基本面與政策論點都需反向核驗

- 10/2 20:02 日線候選為規則式 4 家族同向：5 日漲幅 +19.33%、收盤 534 高於先前 20 日高 530、量約 20 日均量 2.05 倍，近 5 日法人淨買 1,873,682 股。價格、量能、法人流均使用重疊的 5 日窗，不能視為四個獨立因果證據；家族數 4 不是勝率，只有 9/24 一批 81 個 1-session outcome，沒有成熟 5/10-session 樣本，尚不足以調門檻或宣稱可預測。
- TPEx 已接受官方月營收列（2026/6–8）：3 個月累計年增 +34.23%；6 月 +55.06%、7 月 -6.93%、8 月 +45.62%，可支持營收擴張但也顯示月度波動。[TPEx 官方月營收來源](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 公司 8/5 Q2 官方法說簡報提供反向基本面：Q2 營收 NT$770.103M、年增 57.2%，但毛利率 43.3%（Q1 54.5%、去年同期 48.8%）；營業利益 NT$98.046M，季減 46.2%、年減 16.3%。H1 EPS 2.58 元包含 Q1 處分投資收益 NT$163.760M，不能把 EPS 轉正全當作經常性本業增長。追蹤 Q3 營業利益率/毛利率、現金流和營收延續性，暫列「動能候選 + 反向獲利率」，不升格為獲利品質已確認。[公司法說日期與簡報入口](https://gcsincorp.com/investors/Investor_Conference.php)、[Q2 2026 官方簡報](https://gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)

### 政策新聞與券商研究原文品質

- FCC 一手文件已找到：9/11 Federal Register 發布 FCC 26-50 最終規則。正文處理的是 Covered List 元件的認證漏洞，範圍限定為 FCC Covered List 上特定實體製造的 logic-bearing hardware components、且若由該實體製造整機本來就會被禁止認證；規則面向新設備認證，既有授權不受影響。文件並明載不把僅因外國製造地而被列入 Covered List 的生產地分類自動套用到本限制。這是一般元件認證規則，不是已生效的「全面禁中國光模組」或「10 月起禁 3.2T」命令；Federal Register 寫公布 30 日後生效，10/3 當下尚未到生效日。[Federal Register 原文摘要與正文](https://www.federalregister.gov/d/2026-18535)
- 另一路一手資料是 9/25 美國參議員新聞稿：兩黨議員提出法案，針對聯邦政府最敏感國安系統採購涵蓋的光收發器，列 InnoLight、Eoptolink 與關係企業/涵蓋元件；含 5 年轉換期及豁免。這是已提出的法案而非已生效一般進口禁令。[參議員原始新聞稿](https://www.mccormick.senate.gov/news/press-releases/senators-mccormick-gallego-cornyn-fetterman-introduce-bill-to-keep-chinese-transceivers-out-of-u-s-national-security-systems/)
- Taiwan News 10/2 15:39、TechNews 10/2 14:01 台北時間，分別把 3.2T、65% 美國 BOM、2028/2029 放量時程歸於 Morgan Stanley 報告；CMoney 論壇轉貼聲稱標題為 “Potential FCC Rules on Optical Transceivers More Likely to Come in at 3.2T”、日期 10/1 04:09 GMT（台北 12:09）、作者 Meta A. Marshall。這些都是二手歸因；未取得 Morgan Stanley 原始報告或原始發布頁，因此不能確認其作者、實際發布時刻、資料截止，也不能判定原報告品質或推斷動機。原始 EPS/營收模型、估值/目標價、預估修訂、反方風險與可檢驗條件均未核。政策只作待證產業背景，不歸因為 4991 營收/EPS 增量。[Taiwan News 二手報導](https://www.taiwannews.com.tw/news/6450840)、[TechNews 二手報導](https://technews.tw/2026/10/02/ms-see-fcc-rules-on-optical-transceivers/)、[CMoney 論壇轉貼](https://www.cmoney.tw/forum/article/185060813)

- 本輪沒有程式改動或門檻調整；未觸發 DAG、未寫 DB、未發 Discord、未交易。後續自然批次重點：確認 Cnyes 修正後發稿時間、覆核失敗的 TPEx 個股日線是否恢復、公布後的 9 月營收和 4991 Q3 獲利率；另外只在取得 FCC 一手 docket/原始券商全文後，才重評 3.2T 政策論點。

## 2026-10-03 05:16 台北：季財報來源血緣與 8289 獲利率脈絡補強

### 最新自然收集狀態

- 唯讀核對工作樹；既有 `M`、`D` 與 `??` 項目全部保留。最新日線仍是 10/2 20:02 `ready/partial`、1,946 檔 universe、50 候選、0 通知；盤中仍是 10/2 22:45 `ready/fresh`、0 候選。`market_signal_outcomes` 尚只有 9/24 的 `signal_close`/`next_close` 各 81 筆，沒有成熟 5/10-session 樣本，不作績效或門檻推論。
- `source_extension_collect_process` 最近 run 的 ID 為 `scheduled__2026-10-01T20:00:00+00:00`；實際於 10/2 20:00 UTC（台北 10/3 04:00）啟動，04:04 台北前後結束。這是 process DAG，不是 after-close DAG；不要以排程 execution date 冒充實際啟動時間。

| 成功的 mapped source | 市場／範圍 | raw/接受/隔離 | 本輪有效日或資料期間 |
|---|---|---:|---|
| `tpex_balance_sheet_bd` | TPEx，類別 `bd` | 7/7/0 | 2026-06-30 |
| `tpex_balance_sheet_ci` | TPEx，類別 `ci` | 884/884/0 | 2026-06-30 |
| `tpex_quarterly_profitability` | TPEx | 884/884/0 | 2026-06-30；884 個代碼 |
| `twse_balance_sheet_basi` | TWSE，類別 `basi` | 10/10/0 | 2026-06-30 |
| `twse_balance_sheet_bd` | TWSE，類別 `bd` | 3/3/0 | 2026-06-30 |
| `twse_balance_sheet_ci` | TWSE，類別 `ci` | 1049/1049/0 | 2026-06-30 |
| `twse_balance_sheet_fh` | TWSE，類別 `fh` | 13/13/0 | 2026-06-30 |
| `twse_balance_sheet_ins` | TWSE，類別 `ins` | 5/5/0 | 2026-06-30 |
| `twse_balance_sheet_mim` | TWSE，類別 `mim` | 4/4/0 | 2026-06-30 |
| `twse_quarterly_profitability` | TWSE | 1054/1049/5 | 2026-06-30；5 列隔離，不能按全收解讀 |
| `tpex_dividends` | TPEx，歷史股利事件 | 2483/2483/0 | 本輪列的日期僅 2019-01-18 至 2021-08-03，793 代碼；任務成功但無近期事件覆蓋 |
| `twse_dividends` | TWSE，歷史股利事件 | 1228/1228/0 | 日期 2026-01-30 至 2026-09-21，1112 代碼；多筆事件列，不等於 1228 檔 |

- 上表只列這個 process run 的 mapped source 成功列，不能代替其他日線/盤中/新聞來源的有效日盤點。`ingest_approved_source` 的 CBC FX index 12 最終失敗：第一次 deadlock 後重試遇到 JSONDecodeError、raw/接受/隔離 0/0/0；驗證任務 failed，portfolio trigger 為 upstream_failed。其他三個來源首試 deadlock 後重試成功，採最終成功 attempt 的接受數。沒有手動觸發或重跑。
- `source_extension_collect_after_close` 的 TPEx 個股日線缺口、TWSE 日線落後一交易日及上一節列出的各來源 accepted/quarantine 數仍適用；`tpex_equity_quotes_daily` 的失敗 0/0/0 不能當零量。TWTB4U/TWTB5U 契約差異、TPEx 個股當沖 endpoint 空資料/授權未確認、TPEx OAS 11627 為市場彙總且需 freshness gate，均未改變。

### 8289 泰藝：官方營收強，獲利率目前只能作累計脈絡

- TPEx/MOPS 月營收列：2026/8 營收 NT$214.038M、年增 67.15%、月減 1.48%；2026/1–8 累計 NT$1,385.028M、年增 18.17%。公司申報備註把當月較去年同期增加超過 50% 歸因於客戶需求增加。單月強度高於累計，屬已公布事實；單月變化不能單獨證明後續獲利/EPS 會延續。[TPEx 官方月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- TPEx/MOPS 公司重大訊息列記錄泰藝董事會於 2026-08-06 15:21:46 通過第二季合併財報；本文期間為 4/1–6/30，但財務數字明寫「1月1日累計至本期止」。截至 6/30 H1 營收 NT$954.096M、毛利 NT$228.444M（23.94%）、營業利益 NT$50.927M（5.34%）、稅前淨利 NT$56.458M（5.92%）、淨利 NT$43.335M（4.54%）、基本 EPS NT$0.63。[TPEx 官方重大訊息 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 另一個 TPEx 季獲利率 API 在 process run 的成功 mapped task 收到 884/884/0，資料有效日也全是 6/30。資料庫對 8289 只找到該 Q2/H1 點，沒有上一季累計基準；API 的 `Date=1151002` 是列內日期欄，不把它當原始財報發布時刻。故 5.34% 目前只能標為 H1 累計營益率，不能推算單季 Q2 營益率，也不能加入季比方向；原始公告時間採上一點 company-events row 的 8/6 15:21:46，時間欄位語義分開記。[TPEx 官方季獲利率 API](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 10/2 20:02 日線候選掃描早於 10/3 04:00 財報資料收集，不會回填改寫既有候選。既有 8289 候選仍是規則式價格、量能、法人、營收家族觀察，四家族不是勝率；被動元件新聞僅作背景，不能算獨立的營收/獲利驗證。等下一個自然交易日 scan 再看已入庫 H1 資料是否呈現在卡片；不先動方向門檻。

### 窄幅通知脈絡改善

- `market_signals.py` 讓現有日線候選卡顯示最新官方季度營益率脈絡；如果有相鄰累計期間可計算單季值，就標「單季」；缺前季基準時則清楚標「上半年/前三季/全年累計」，註明不換算單季趨勢，並保留交易所官方來源連結。此欄不另計訊號家族，不改方向分數、門檻、雜湊或通知路由，避免在 8289 上把 H1 5.34% 誤寫成 Q2 單季或 QoQ 訊號。
- 本輪對 `market_signals.py` 做 Python AST 靜態解析，通過；未新增或執行測試。未觸發 DAG、未寫資料庫、未發 Discord、未交易。下次自然 scan 核對欄位顯示及財務來源 freshness；持續追蹤官方 Q3 季報、單季/累計財務歷史和券商原始報告，無原文的標題仍標未核，不推測報告動機。

## 2026-10-03 05:43 台北：TPEx 股利來源停更核實與新聞發稿精度抽樣

### 最新日線、盤中與 outcome

- 唯讀資料庫確認最新日線 run 是 10/2 20:02–20:02 台北、`as_of_date=2026-10-02`、`ready/partial`：universe 1,946、當日有價 1,938、法人流 1,891、營收 1,945、季財務 1,902、借券 1,795、董監 1,944、fresh dividend codes 1,053（均為上市近期事件；TPEx 舊 feed 沒有近期列）、候選 50、通知 0。舊行情表 10/2 有上市 1,050/1,054、上櫃 888/892；缺列是上市 1589/2323/2601/3311、上櫃 3085/4527/4804/5301。資料庫沒有把缺列當零成交。相對強弱/公司事件來源不完整時，run 的 partial 不升格為完整。
- 最新盤中 child 是 10/2 22:45 `ready/fresh`、候選/通知 0/0；這只代表該盤中 mapped-source 工作流完成，盤中 run 的價格/法人/營收/財務計數為 0，不代表收盤行情完整。
- 成熟 `market_signal_outcomes` 仍只有 9/24 的 1-session `signal_close` 與 `next_close` 各 81 筆，兩組各只有 80 筆有方向命中值；尚無成熟 5/10-session 樣本，不以一個訊號日調門檻或宣稱預測效力。

### 最新自然 after-close（逐來源，不用父層狀態替代）

Airflow 最新 `source_extension_collect_after_close` 是 `scheduled__2026-10-01T12:00:00+00:00`，實際 10/2 20:00:00–20:04:37 台北時間，父層 failed；當時只映射 11 個來源。2 個 TPEx 注意/處置來源於 10/2 21:30 台北時間才註冊，尚無 `last_success_at` 或 `source_runs`，週末沒有自然盤後批次，預計下個交易日 10/5 才首次驗證。

| 成功映射來源 | raw/接受/隔離 | 有效日期 | 範圍與覆蓋解讀 |
|---|---:|---|---|
| `tpex_securities_borrowing_daily` | 814/814/0 | 10/1 | 上櫃借券；供給背景，不等於成交方向 |
| `twse_quarterly_forecast_attainment` | 8/0/8 | 6/30 財報期 | 本批有財測達成列的上市公司；未代表全體上市櫃 |
| `twse_tpex_sbl_available_daily` | 1717/1717/0 | 10/2 | 上市+上櫃可借券餘額；供給背景 |
| `tpex_market_index_daily` | 2/2/0 | 10/1、10/2 | 櫃買市場指數，不是個股報價 |
| `twse_market_index_daily` | 1/1/0 | 10/1 | 上市市場指數仍落後最新行情一日 |
| `twse_trading_attention_daily` | 0/0/0 | 無列 | 本批沒有記錄，不代表其他 feed 未出注意股 |
| `twse_trading_disposition_daily` | 8/8/0 | 10/2 | 上市處置證券 |
| `tpex_margin_balance_daily` | 801/801/0 | 10/1 | 上櫃融資融券 |
| `twse_margin_balance_daily` | 1039/994/45 | 10/2 | 上市融資融券；45 列隔離，不能按全收解讀 |
| `tpex_equity_quotes_daily` | 0/0/0，重試兩次失敗 | 無可用日期 | 上櫃個股行情；串流未收完（第二次收到 4,193,897 bytes，尚缺 472,744），不可解讀成零量 |
| `twse_equity_quotes_daily` | 1051/1051/0 | 10/1 | 上市個股日線；本批上市來源完整回列，但日期晚於 10/2 舊行情表一日 |

上述 11 個映射以 Airflow `discover_approved_sources` 的 XCom 清單和每個 mapped task 的 XCom 回傳逐一對齊；不能把同一時間窗內其他 DAG 的 `source_runs` 混入本 run。TPEx OAS 11627/當沖統計仍只是市場彙總，須做 freshness gate，不能補成個股當沖訊號；TWTB4U/TWTB5U 契約差異、上櫃 individual endpoint 空資料及未確認授權狀態都未變。

### 每日 fundamentals、月營收與舊股利 feed

- `source_extension_collect_process` 最近 run 在 10/3 04:00–04:04 台北時間，父層 failed：12 個 mapped sources 最終成功、`cbc_fx_daily` 第 2 次仍失敗 0/0/0。成功 financial sources 的有效財報期為 2026-06-30：TPEx 季獲利率 884/884/0；TWSE 季獲利率 1054/1049/5；TWSE/TPEx 月營收分別 1086/1086/0、892/892/0，最新月份 2026/08（官方出表日 9/17）；TWSE 股利 1228/1228/0，最新有效日期 9/21。來源 task success 不等於所含期間新，也不代表全市場無隔離列。
- TPEx 股利來源 task `tpex_dividends` 成功 2483/2483/0，表面全接受，但直讀[官方 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap39_O)目前 2483 列僅含民國 107–110 年，最新 `出表日期=1100804`；資料庫正規化後最大股利生效日 2021-08-03、事件/出表日 2021-08-04、793 個代碼。此 feed 至少五年未覆蓋新公告，不能把成功/全接受當近期股利覆蓋；現有日線邏輯以 45 天發稿窗排除這些舊事件，因此本次不會產生過期股利訊號。
- 已檢視 TPEx [官方 115/08《櫃檯買賣概況表》原始 XLSX](https://www.tpex.org.tw/storage/co_overview/2026/09/1788231560_CH_%E6%AB%83%E8%B2%B7%E6%A6%82%E6%B3%81%E8%A1%A8_202608v.xlsx)。它含 8289 泰藝的每股股利摘要（2024 年度/2025 配發現金股利 0.30 元；2025 年度/2026 配發盈餘現金股利 0、資本公積現金 0.15 元）及 2026/05/29 股東會日期，證明月報可補近期股利背景；但月報沒有董事會決議/原始公告發布時刻，不能替代事件 feed 或精確事件研究。先保留為官方候選補充來源，不自動接入、不改方向訊號。

### 新聞時間品質

- `news_ch_collect_process` 最新自然 run `scheduled__2026-10-02T15:30:00+00:00` 已於台北 10/3 05:30:00–05:38:18 成功。按 `news.id` 最新寫入順序取 5,000 列估算重送/回補時效：The News Lens 96/103、iThome 80/100 的 `created_at` 比 `create_time` 早超過 48 小時。這 103 篇 TNL 與 100 篇 iThome 樣本的 `created_at` 全部落在 00:00:00，只有日期粒度；48 小時邊界的小時級新鮮度不確定。ETtoday、鉅亨網、中央社、自由時報及聯合報樣本有近期發稿時刻。採集 run 綠燈不代表所有來源只含新稿；新聞保留為來源事實/待核背景，不作方向票，重大內容仍回查原發稿。
- 本輪未取得新的券商原始研究全文；公開標題/二手歸因仍標未核原文，不推測作者假設或報告動機。

### 決策

- 本輪只更新追蹤與記憶；沒有改候選/通知門檻、打分、DAG 或資料庫，沒有手動觸發/重跑、Discord 通知或交易。下一個交易日自然批次先核 TPEx 個股日線和兩個 TPEx 注意/處置來源的 effective date、raw/接受/隔離與上市櫃覆蓋；再確認新聞是否持續回補日期型舊稿。評估是否以 TPEx 月報補股利背景時，需保留 MOPS 原公告連結與事件時間缺口，不用月報彙總冒充公告時點。

## 2026-10-03 05:52 台北：正式券商研究入口定位與可評估性界線

### 4991 環宇-KY：富邦投顧個股報告只取得公開索引資訊

- 富邦投顧官網的研究首頁列出一筆 `4991 TT` 個股報告：2026/09/08，標題「光收發模組帶動的成長爆發」，標籤為光通訊、資料中心、RF；同一標題欄括號顯示 NT$494.00。頁面明確要求登入才能閱讀全文。[富邦投顧正式研究頁](https://fubonresearch.fubon.com/Research/Home/Index)
- NT$494.00 的欄位用途未在公開索引說明，不把它標成目標價。作者、資料截止日、估值方法、EPS/營收假設、預估上修/下修、反方風險、可檢驗預測及原始引用均無法從公開頁面核實。因此來源身分/發布日期可確認，研究內容品質與方向性皆標「未核全文」，不根據標題推測研究動機，也不把它當成 10/2 動能候選的獨立確認。
- 富邦首頁另顯示 10/2 盤後快訊對三五族半導體、光通訊等族群的盤面描述；那是市場評論，不等同個股預估模型，不加入 4991 訊號家族。[富邦投顧研究首頁](https://fubonresearch.fubon.com/Research/Home/Index)

### 永豐投顧：CPO/NPO 文章亦屬會員內容

- 永豐投顧官方入口可定位「CPO和NPO是什麼？NVIDIA光通訊降規傳言拆解，聯亞、光聖、訊芯-KY誰才是真正受惠者？」文章，但頁面顯示會員登入/訂閱與報告下載入口；可見資訊不足以核驗文中引用、資料截止或估值假設。[永豐投顧官方文章入口](https://scm.sinotrade.com.tw/Article/Inner/cf244a69-a655-4aba-bce9-ec45fc5eee6a)
- 將其保留為光通訊產業研究線索，不能替代 4991 的個股研究，也不算可量化交易訊號。搜尋中出現的第三方鏡像不作一手證據、不用來補全文。

### 報告日期與官方月營收資料期對照

- TPEx 官方月營收 API 現列 4991 的 2026/07 列 `出表日期=1150817`，2026/08 列 `出表日期=1150917`。8 月營收 NT$262.258M、單月年增 45.62%；2026/1–8 累計 NT$1,894.097M、年增 45.57%。Fubon 公開索引日期是 9/8，早於該 API 的 8 月列出表日 9/17；因此這筆 8 月數據是報告索引日期之後才出現在該 feed 的資料。`出表日期` 是 API 欄位，不等同公司原始公告的精確發布時刻；不能據此斷言 Fubon 分析師的真實資料截止日。[TPEx 官方月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 8 月及累計營收都支持營收同比成長，但兩者高度重疊，不能當成兩個獨立訊號；亦不能單憑營收推成 EPS 上修。既有官方 Q2 財報脈絡仍有毛利率下降與 H1 EPS 含處分投資收益，故「成長爆發」目前最多是營收方向合理、獲利與估值假設未能驗證。報告全文不可見，品質結論維持「無法評估」，不評分其模型準確度。

### 研究品質狀態

- 本次新增的正式券商來源具可驗證發布者與日期，但研究假設、估值與風險內容均不可見；品質結論是「無法評估」，不是「高品質」或「為了出報告」。在取得合法可讀的原文前，維持現有 4991「價格/量/法人動能候選 + 官方獲利率反向脈絡」，不新增報告方向票或提高通知權重。
- 本輪只追加可追溯研究登錄；未登入會員、未使用第三方鏡像、未改訊號程式或門檻、未觸發 DAG、未發 Discord、未交易。

## 2026-10-03 06:04 TNL/iThome 新聞時間精度標示

- 回查媒體原始頁：TNL 10/2 新聞卡與文章頁只列 `2026/10/02`；iThome 10/2 原文顯示「2026-10-02 發表」，未列時分。[TNL 原文頁](https://www.thenewslens.com/article/270324)、[iThome 原文頁](https://www.ithome.com.tw/news/179322)
- 本地爬蟲也把 TNL 列表頁的日期字串、iThome `.submitted .created` 的 `%Y-%m-%d` 轉成 `00:00:00`。前一輪最近寫入樣本中，TNL 96/103、iThome 80/100 的 `created_at` 比 `create_time` 舊超過 48 小時，但 `00:00` 表示來源只有日期，無法精確判斷小時級新鮮度或 48 小時邊界。
- `market_signals.py` 的新聞窗仍以 DB `created_at` 篩近 48 小時，這輪只改善卡片解讀：已匹配的 TNL/iThome 文章標為「僅日期」；無匹配時提醒日期精度會讓窗口邊界漏配，不能把無匹配當成沒有相關報導。新聞仍是背景，不計方向票；沒有改時間窗、候選分數或通知門檻。
- 對 `dags/source_extension/market_signals.py` 做 Python AST 靜態解析通過；沒有跑測試或 DAG。工作區中此整個 `dags/source_extension/` 資料夾原本為未追蹤狀態，本輪僅改上述卡片呈現。後續須等 10/5 自然批次，再核對原頁日期、資料庫時間窗邊界與實際候選卡呈現。

### 06:10 台北唯讀 runtime 再核對

- Airflow 最新 after-close 仍是 `scheduled__2026-10-01T12:00:00+00:00`，執行於台北 10/2 20:00:00–20:04:37、`failed`；最新 fundamentals/process 是 `scheduled__2026-10-01T20:00:00+00:00`，執行於台北 10/3 04:00:00–04:04:21、`failed`。未手動觸發或重跑。
- DB 按該 process run 的每個 source 最新 attempt 去重後，共 13 個來源：12 success、`cbc_fx_daily` failed `0/0/0`（`JSONDecodeError`）。TWSE 季獲利率 `1054/1049/5`，TPEx 季獲利率 `884/884/0`，兩股利來源各 `1228/1228/0`、`2483/2483/0`；成功率不改變前節有效期間解讀：季財務仍為 2026-06-30，TPEx 股利仍只到 2021。對 TWSE 季獲利率的 5 列隔離保留，不視為全收。
- 最新 daily DB run 仍是 10/2 20:02，`ready/partial`、母體 1,946，價格 1,938、法人 1,891、月營收 1,945、季財務 1,902、借券 1,795、董監 1,944、股利 1,053、候選 50、通知 0。最新 intraday 是 10/2 22:45、`ready/fresh`，但 universe/價格/法人/營收/財務與候選/通知全為 0；週六未出現更新的市場快照。
- 成熟 outcome 聚合仍只見 9/24 `signal_close`、`next_close` 各 81 筆，各 80 筆有方向命中值；沒有成熟 5/10-session 結果。不能據此宣稱已校準預測力或調整門檻。
- `news_ch_collect_process` 最新自然執行仍為台北 10/3 05:30:00–05:38:18 success。最新 5,000 筆寫入樣本中 TNL 為 103/103、iThome 為 100/100，發稿時間都在 00:00；這與原始頁只列日期一致，不用寫入成功或午夜時間推定精確發稿時刻。

## 2026-10-03 06:19 台北：4991 候選快照與季獲利脈絡的先後確認

- 唯讀重新查 Airflow 與訊號 DB：最新 after-close 仍是 `scheduled__2026-10-01T12:00:00+00:00`，台北 10/2 20:00–20:04 failed；fundamentals/process 仍是 `scheduled__2026-10-01T20:00:00+00:00`，台北 10/3 04:00–04:04 failed；新聞最新批次仍是 10/3 05:30–05:38 success。最新日線仍為 10/2 20:02 `ready/partial`、1,946 檔 universe、50 候選、0 通知；最新盤中 10/2 22:45 `ready/fresh`，行情/候選/通知計數為 0。outcome 仍只有 9/24 的 `signal_close` 與 `next_close` 各 81 列，無成熟 5/10-session 樣本。週末沒有新的日線或 after-close 自然 run；未手動觸發或重跑。
- 直接讀取 10/2 日線候選快照，4991 仍是四個規則式偏多家族（價格、量、法人、營收），但該列 `profitability_context` 為空。這不表示新財務來源漏收：候選是在台北 10/2 20:02 產生；TPEx 獲利率來源在 10/3 04:00 才執行、884/884/0，資料有效期仍為 2026-06-30。4991 唯一獲利率 row 是 18.87%，代表 Q2/H1 累計，不能從此單點推 Q2 單季趨勢。舊候選是 scan-time snapshot，不會因後來收集而回填；待 10/5 自然日線 scan 驗證新卡片是否顯示「上半年累計」與交易所來源連結。
- 公司 Q2 官方簡報的 Q1→Q2 單季毛利率 54.5%→43.3%、營益率 25.5%→12.7%仍是重要反向脈絡；TPEx API 的 H1 累計 18.87%不能取代該單季比較，也不能與其混算。四個動能家族有價格/量/法人重疊，不是四個獨立基本面證據，且無矛盾只表示本候選方向家族未登錄反向票。維持「動能候選 + 獲利率反向脈絡」觀察，不調分數、方向或通知門檻。
- 本次券商官方網域補查未得到可公開核驗的新 4991/6174/8289 原始研究全文；Fubon 4991 9/8 索引仍要求登入，作者、資料截止、估值、EPS/營收假設、修訂及反方風險都未核，不據公開標題猜報告動機或評定品質。
- 本次只確認時間先後與既有來源資料，不改程式/資料庫、不跑測試或 DAG、不發 Discord、不交易。10/5 自然批次再核財務 context、TPEx 個股行情及注意/處置來源的有效日期、接受/隔離列數和上市櫃覆蓋；新聞仍保留來源發布時間精度標籤。

## 2026-10-03 06:23 台北：TWSE 財測達成來源欄位修正唯讀預檢

- 核對 10/3 04:00 台北 `source_extension_collect_process` 的 Airflow task state：`ensure_source_extension_schema` 與 `discover_approved_sources` 均 success；整個 DAG 後來因 `cbc_fx_daily` 第 2 次 `JSONDecodeError` 失敗。DB 目前 `twse_quarterly_forecast_attainment` catalog 已將舊契約欄位 `__cumulative_actual`、`__cumulative_actual_value`、`__cumulative_forecast` 設為 disabled；現行必填欄位只有代碼、財報年月季、期末、出表日期與實際/預測原字串。這確認 catalog cleanup 已隨自然 schema/bootstrap task 套用，未直接修改資料庫。
- 只讀呼叫 TWSE 官方 `t187ap15_L` 並以目前已核准 contract 正規化：HTTP 200、8 列、6 個不同上市代碼（2412、2454、2515、3036、3702、4904）、有效期別均為 2026Q2（期末 6/30）、出表日 10/1；8/8 列所需欄位皆在、缺必填欄位 0。官方端點描述為「截至各季綜合損益財測達成情形(簡式)」；本批回傳資料只涉及這 6 個上市代碼，不代表上市全體或上市櫃完整覆蓋。[TWSE 原始 API](https://openapi.twse.com.tw/v1/opendata/t187ap15_L)、[官方 API 規格頁](https://openapi.twse.com.tw/)
- 這次是未寫 DB 的 API/contract 預檢，不是 collector 收集成功或 8 列已接受的證據。前一筆 after-close run 在 catalog cleanup 前的歷史結果仍為 raw/接受/隔離 8/0/8；舊列保留隔離。下次自然 after-close 預計 10/5，需以新 run 實際驗證列數與 effective date，再比較 source run 的 accepted/quarantined，不回填舊候選。
- 8 列含 2515 三筆序號 0/1/2，這三列目前實際數、預測數與涵蓋期相同，不能算三個獨立達成訊號。其他列 `涵蓋期間` 有「二」及「一、二、三、四」；目前仍未逐公司對回原始財測及財報的期間口徑。保留預測/實際原始字串作官方觀察，不算達成率、不設方向、不進分數或通知，直到期間與單位語意可比。
- 本輪僅做 Airflow/DB 唯讀核對與官方 API 取樣；沒有測試、手動觸發/重跑 DAG、改 DB、Discord 通知或交易。下一個自然 run 若確實接受新列，再逐公司核對涵蓋期間、原公告和實績可比性，決定能否提供候選卡上的中性預測差異脈絡。

## 2026-10-03 06:31 台北：財測欄位跨端點對照與出表日 freshness

- 再次唯讀抓取 TWSE 官方 `t187ap15_L` 與 `t187ap16_L`：兩端點目前均 HTTP 200，前者 8 列/6 個上市代碼、後者 7 列/5 個上市代碼；資料期都是 2026Q2、期末 6/30，`出表日期` 均已由前次存庫的 1151001 更新為 1151002（2026-10-02）。API 只有日期粒度，不能由欄位推定發布時分。資料庫仍保存前次 `t187ap15_L` run 8/0/8、出表日 10/1；`t187ap16_L` 最近自然 run 是 10/2 22:45 7/7/0、出表日 10/1。故眼前是官方來源比本地收集新一日，不是新交易日行情或 10/2 個股訊號；待 10/5 after-close 自然 run 再核實際 accepted/quarantined 與有效日，不補跑。
- 以公司代碼、年度、季別、財測序號逐列連接兩端點，現有 7 個交集列、5 家公司（2412、2515 的 3 個序號、3036、3702、4904）：`t187ap15_L` 的「截至該季實績」7/7 等於 `t187ap16_L`「截至當季累計實績」；其「截至該季預測數」7/7 等於 `t187ap16_L`「當季預測數」，而 0/7 等於 p16 的「累計預測數」。例如 2412 實績 20,593,145 對 cumulative actual 20,593,145；p15 forecast 9,232,695–9,249,329 對 p16 quarter forecast 同值，而 p16 cumulative forecast 為 18,224,469–18,260,262。這批交集顯示 p15 欄位原配對會把截至季累計實績與單季預測放在一起，不能算財測達成率；它只核對同一期有差異事件的交集列，不推廣到所有公司/季別。2454 是 p15-only 列，期間仍未獨立核實。兩端點原始欄位與門檻定義見[TWSE 財測達成 API](https://openapi.twse.com.tw/v1/opendata/t187ap15_L)、[TWSE 財測差異 API](https://openapi.twse.com.tw/v1/opendata/t187ap16_L)及[官方規格](https://openapi.twse.com.tw/)。
- 目前保存的 10/2 財測差異通知 ledger 內容仍是舊格式原始數字，未顯示千分位或單位未知提示；現在的本地 renderer 已將數字分組、區間「~」呈為「至」，並明示 API 未提供金額單位。這些歷史 payload 不會自動回寫；新的 renderer 尚待有新候選時作自然驗證，本輪不補發或製作實際 Discord 通知。
- 調查只做官方 API 讀取、既有 DB/runtime/card payload 對照；沒有更改方向、分數或門檻，沒有寫 DB、觸發 DAG、跑測試、發 Discord 或交易。下一自然批次同時核 p15 新列是否被接受，以及 p16 10/2 新出表日是否入庫；解讀財測前仍需逐公司對回原始財測與季度實績口徑。

## 2026-10-03 06:36 台北：公司財測卡唯讀 renderer 預覽

- 取既有 2412 公司財測差異候選快照，唯讀呼叫目前 `_format_intraday_candidate` 產生本地預覽。輸出保留 2026 Q2、涵蓋期、序號、當季及累計實績/預測、官方來源；數值千分位、區間 `~` 改為「至」，並標明 OpenAPI 未提供單位及勿單憑此卡判定多空。這確認本地 renderer 輸出符合目前欄位契約；快照 footer 仍是舊出表日 10/1，不能冒充剛更新為 10/2 的官方 API 資料。
- 這只是從舊候選資料呼叫 renderer 的預覽，未送至 Discord，也沒有證明外部投遞或新資料入庫；已存舊通知內容未回寫。待新自然候選出現時再核實實際候選與 notifier 路徑，仍不手動補發。


## 2026-10-03 07:04 台北：歷史資料品質、outcome 樣本與券商報告分級

### 最新自然資料狀態

- 週六沒有新的交易日收盤或 after-close 執行。最新日線仍為 10/2 20:02 `ready/partial`：母體 1,946、行情 1,938、法人流 1,891、月營收 1,945、季財務 1,902、50 個候選、0 個通知；最新盤中 scan 為 10/2 22:45 `ready/fresh`，但行情/候選/通知計數皆為 0，不能視為新的個股快照。
- 最新 after-close 自然批次對應台北 10/2 20:00，父層仍 `failed`。映射來源有部分成功，不等於整體完整：TWSE 個股日線 1,051/1,051/0、有效日 10/1；TPEx 個股日線串流失敗 0/0/0；TWSE 指數 1/1/0、有效日 10/1；TPEx 指數 2/2/0、有效日 10/1–10/2；TWSE 注意交易 0/0/0、無列；處置證券 8/8/0、有效日 10/2。逐來源完整清單沿用上方最新 after-close 表。後續應查 TPEx 個股報價的 endpoint/授權，不以空資料當零量。
- 最新 fundamentals/process 自然 run 為台北 10/3 04:00，父層因 `cbc_fx_daily` 第 2 次 `JSONDecodeError` 失敗；其他 12 個來源最終成功。TWSE 季獲利率 1,054 raw/1,049 接受/5 隔離；TPEx 884/884/0；月營收 TWSE 1,086/1,086/0、TPEx 892/892/0，最新有效月份均為 2026/08（官方出表日 9/17），季財務期為 2026Q2，董監持股有效月份只有 2026/08。不能由來源 run 綠燈推定期間新或全市場無缺漏。
- 新聞最新自然 run 台北 10/3 05:30–05:38 success。TNL/iThome 原始文章頁只提供日期、DB 對應時間落在 00:00；視為日期粒度，不宣稱精確發布時分，也不把成功收集當成全是新稿。新聞仍保留來源事實/背景，不加方向票。
- 當沖仍未形成可用個股訊號：TWSE TWTB4U 資格主檔與 TWTB5U 個股成交統計契約不同；TPEx OAS 11627 僅市場彙總，需 freshness gate；TPEx individual endpoint 曾空資料且授權未確認。不能把未匹配證券列當零量，也不把未列在官方開放授權的個股查詢路徑接入評分。

### 歷史資料與 outcome 品質

- 唯讀歷史盤點：`stock.個股成交資訊` 6,050,865 列、4,172 個資料日、1,946 個代碼，最新 2026-10-02；`stock.三大法人買賣` 33,672,125 列、2,829 日，期間 2014-12-01 至 2026-10-02。法人表 distinct code 含 unmatched 值，不當成證券母體大小。
- 行情表有 63 列疑似日期品質異常：`0114` 年 39 列、`0115` 年 24 列；逐一加 1911 年後，同代碼/日期鍵匹配數皆為 0。尚未追明上游，不自動轉換、也不計入長期回測。全期鍵值檢查及近 18 日重複鍵檢查皆無重複。
- 10/2 行情上市 1,050/1,054（空收盤 0）、上櫃 888/892（空收盤 20，可用收盤 868）；法人流匹配上市 1,043、上櫃 788，另有 22,966 列 unmatched。缺列/空值保留為缺漏，不補零。
- `market_signal_outcomes` 成熟樣本仍只有 9/24 一個訊號日：`signal_close` 和 `next_close` 各 81 列，各 80 列有方向結果。`next_close` 命中 46.25%、平均超額 +0.98679 個百分點；`signal_close` 命中 62.5%、平均超額 +1.77597 個百分點。單日橫斷面候選互相關聯、未扣交易成本，收盤後 `signal_close` 亦非實際入場價；10/2 的 50 個候選尚未成熟。不可解讀成勝率、不能據此調權重/門檻。
- 可複核分析記在 [歷史資料品質 notebook](stock_signal_family_history_quality.ipynb) 與 [唯讀查詢 helper](stock_signal_family_history_queries.py)。Notebook 11 個程式 cell 均以順序執行器跑完並嵌入輸出，`nbformat.validate` 通過；`nbclient` 核心啟動曾因 ZMQ `Bad file descriptor` 失敗。沒有改寫資料庫或觸發 DAG。

### 券商研究：品質線索和未知事項

#### 富邦投顧｜8046 南電｜2026/10/02 索引標題

- 富邦官方研究索引搜尋結果列有「4Q26營收將優於預期；將開始供應AMD Venice載板」，旁列 NT$1,460；該數字欄位用途不明，不能當目標價。全文要求會員登入。作者、研究資料截止日、估值方法、EPS/營收假設、上修/下修幅度、反方風險及可檢驗預測均未核實。[富邦官方研究入口](https://fubonresearch.fubon.com/Research/Home/)
- 官方頁直接讀取時只顯示到 10/1；搜尋索引顯示 10/2 條目，記錄為索引/頁面快取或呈現日期差異，不能說成原文已核。此為後續取得合法全文時優先檢視的線索，現在不計方向票、不評定報告品質，也不推測發布動機。
- AMD 官方材料確認 Venice 開始在台灣台積電 2nm 製程量產爬坡，另有 AMD 台灣先進封裝生態系合作資訊提及南電；但沒有證實南電供應特定 Venice 載板、4Q26 開始出貨的數量、價格或營收。[AMD Venice 官方公告](https://www.amd.com/zh-tw/newsroom/press-releases/2026-5-20-amd-announces-production-ramp-of-next-generation-a.html)、[AMD 台灣生態系公告](https://www.amd.com/zh-tw/newsroom/press-releases/2026-5-20-amd-announces-more-than-10-billion-in-taiwan-ecos.html)
- 南電官網投資人頁列 2026/08 營收約 NT$5,353 百萬元、月減約 1.6%；TWSE OpenAPI 出表日為 2026/09/17，年增約 40.59%、1–8 月累計年增約 39.56%。公司頁當時最新顯示 8 月；這些歷史營收支持需求/基期脈絡，不驗證 4Q 預測或特定新平台供貨。[南電投資人財務資訊](https://www.nanyapcb.com.tw/nypcb/chinese/InvestorRelations/FinancialReports)、[TWSE 月營收 API](https://openapi.twse.com.tw/v1/opendata/t187ap05_L)

#### 永豐投顧｜2303 聯電｜2026/05/04 方法參考，非新訊號

- 《永豐晨訊 2026/5/4》公開原始 PDF 為 36 頁；作者署研究部，未見個別作者。內容以 1Q26 實際值對預估，提出 2Q/全年營收與 EPS 假設、上修理由與風險；個股建議維持中立，未見個股估值方法/目標價。報告日期明確，資料基礎為 1Q26 實績，至本次已約五個月，不能當新交易訊號。[永豐原始報告 PDF](https://scm.sinotrade.com.tw/Article/GetFile/b6690bb5-b7fa-4fa0-9513-f235bba56fc4)
- 報告列 1Q26 營收 NT$61.038B（預估 61.752B）、毛利率 29.2%、EPS 1.29（預估 0.85），並說明業外匯兌/金融資產評價提高帳面獲利；2Q 預估營收 67.9B、毛利率 30.5%、EPS 0.87；2026 全年營收 277B、EPS 4.40（EPS 由 3.89 上修），2027 營收 311.6B、EPS 5.05。上修依據包含價格與稼動率回升；中立論點是復甦已反映於股價。反方風險包括 8 吋產品組合壓 ASP、匯兌/資產評價噪音、折舊/能源/物流成本、22/28nm 競爭及復甦時程。2Q 數據與下半年稼動/價格可作後驗檢查；模型沒有目標價/估值方法，故可作研究結構參考但非完整估值評分。

### 本輪決策與下次核對

只更新追蹤和 notebook；沒有調整候選門檻、方向分數、通知權重或 DAG，也沒有 DB 寫入、手動觸發/重跑、Discord 通知或交易。下個交易日 10/5 自然批次先核 TWSE/TPEx 個股行情及注意/處置來源的有效日期、原始/接受/隔離列數與上市/上櫃覆蓋，再檢查指數基準是否同日、日線訊號是否重跑、TPEx 授權/空資料和日期異常來源。outcome 要多個訊號日成熟後再評估。


## 2026-10-03 07:18 台北：自然 run 複核與券商預測後驗

### 週末 latest-state 唯讀重查

- Airflow metadata 當下最新日線父 run 仍是台北 10/2 20:00 對應的 after-close `scheduled__2026-10-01T12:00:00+00:00`，於 20:00–20:04:37 執行後 failed；legacy `stock_collect_everyday_process` 同日 16:10–20:15 執行後 success；fundamentals/process 於台北 10/3 04:00–04:04 failed。新聞最新為 `news_ch_collect_process` logical 10/2 15:30Z、台北 10/3 05:30–05:38 success。週六無新的市場日線。
- 直接唯讀 DB 重查 `market_signal_runs`：最新 daily 仍是 10/2 20:02 `ready/partial`，1,946 檔母體、1,938 價格、1,891 法人、1,945 月營收、1,902 季財務、50 候選、0 通知；intraday 最新 10/2 22:45 `ready/fresh` 但市場列/候選/通知均為 0。`market_signal_outcomes` 仍只有 9/24 一個成熟訊號日，`next_close`/`signal_close` 各 81 列、各 80 列有方向結果。
- 最新收盤來源仍不完整：TWSE 個股行情 1,051/1,051/0、有效日 10/1；TPEx 個股行情失敗 0/0/0；TWSE 指數 1/1/0、有效日 10/1；TPEx 指數 2/2/0、有效日 10/1–10/2；TWSE 注意交易 0/0/0、處置 8/8/0、有效日 10/2。fundamentals/process 的 `cbc_fx_daily` 第 2 次仍失敗 0/0/0；其餘來源成功但期間仍只到：月營收 2026/08（TWSE 1,086/1,086/0、TPEx 892/892/0，出表日 9/17）、季獲利率 2026Q2（TWSE 1,054/1,049/5，TPEx 884/884/0）、董監持股 2026/08。成功 task 不是當日完整的證據。

### 永豐投顧 2303 聯電：2026/05/04 預測已可後驗

- 已回到[永豐原始 36 頁報告 PDF](https://scm.sinotrade.com.tw/Article/GetFile/b6690bb5-b7fa-4fa0-9513-f235bba56fc4)第 17–18 頁核對：日期 5/4；作者只標「研究部」，無個別作者。1Q26 實績與 2Q/全年預估有逐項數字、理由、反方因素；聯電評等為中立，但未提供可核的目標價或該股估值方法。
- 報告的 2Q26 預估為營收 NT$67.9B、毛利率 30.5%、稅後淨利 NT$10.9B、EPS NT$0.87；並明言 EPS 季減主要因研究部對業外較保守，且提醒 1Q 的業外貢獻大於獲利 30%。
- 聯電 7/29 官方 2Q26 結果為營收 NT$68.733B（季增 12.6%，比報告估值高約 1.2%）、毛利率 32.5%（高 2.0 個百分點）、營業利益 NT$14.950B、EPS NT$3.39；公司同時列非營業淨收益 NT$30.236B。故營收/毛利的本業預估相當貼近，EPS 預估大幅低於實績，但該季非營業淨收益占營收 44%，且研究原文已揭露它採保守假設，不能把 headline EPS 誤差直接當成本業判斷失準。[UMC 2Q26 官方結果](https://www.umc.com/zh-TW/News/press_release/Content/investor/20260729)、[UMC 2Q26 財務報表](https://www.umc.com/upload/media/08_Investors/Financials/Quarterly_Results/Quarterly_2020-2029_English_pdf/2026/Q2_2026/UMC26Q2_financial_statements-E.pdf)
- 後驗評級（分析判斷）：報告可評估性高；本業方向/營收和毛利預估接近實績、EPS 保守且明示了業外假設，研究內容品質中高；但因無目標價/估值法、也未以原始發布時可成交價格檢驗中立建議，不能評定其選股報酬好壞。此例支持把 broker forecast scorecard 拆為「營運預估」和「業外/EPS」兩欄，不以單一 EPS surprise 代表報告品質。
- 下一個可檢驗事件：UMC 官方行事曆列 3Q26 財報電話會議預定 10/28（標示 subject to change）；9 月營收預告於 10/6。追蹤公司先前「Q3 出貨量高個位數成長、產能利用率超過 90%」的明確預測及獲利品質。[UMC 投資人行事曆](https://www.umc.com/en/IR_Event/ir_events)、[UMC 官方 Q2 展望與結果](https://www.umc.com/zh-TW/News/press_release/Content/investor/20260729)

### 富邦 8046 南電標題：一般需求已有營運佐證，特定 Venice 供應仍未證實

- 本次重新讀富邦官方研究首頁的搜尋索引，仍列 10/2 南電標題「4Q26營收將優於預期；將開始供應AMD Venice載板」，全文需登入；搜尋索引旁的 NT$1,460 欄位用途不明。官網直接頁面顯示內容更新到 10/1，故保留「索引顯示 10/2、頁面呈現到 10/1」的日期/快取差異，不把標題視為原文已核。[富邦官方研究頁](https://fubonresearch.fubon.com/Research/Home/)
- 新增核對南電 2026/08/27 Q2 營運簡報原始 14 頁：H1 合併營收 NT$24.75B、年增 37.2%；Q2 營收季增 21.5%，公司稱 1.6T 交換器應用載板開始量產；Q2 營業利益較 Q1 增 NT$1.547B。公司把成長歸於伺服器/交換器/記憶體高階產品需求；反向條件是車用需求偏弱，PC/消費電子受零件短缺限制。簡報免責條款提醒部分歷史數可能未經會計師審閱。[南電 Q2 2026 官方營運簡報](https://www.nanyapcb.com.tw/nypcb/images/InvestorRelations/FinancialReport/NYPCBCH20260506.pdf)
- 以上是高品質的「公司層級成長與產品量產」證據，可支撐一般 ABF/伺服器交換器需求方向；沒有提 AMD、Venice 或特定客戶，不能驗證券商標題中的指定平台、開始出貨日期、訂單量或 EPS 貢獻。故富邦報告仍是全文未核的研究線索，不推測報告動機，也不作方向票。
- 最新 10/2 的 50 個日線候選內未見 8046 或 2303。這兩項券商研究評估是研究品質與待驗證營運脈絡，不是當前程式的候選/通知結果。

### 決策

將券商預測後驗拆分為營運線與 EPS/業外線，留待累積更多報告樣本；把南電 1.6T 量產與季度營收/營業利益列為官方可量化產業背景，等待 9 月月營收正式公告及券商原文，不新增通知權重。沒有改程式、門檻、DB、DAG、Discord 或交易；10/5 自然收盤繼續核實來源 effective date、raw/接受/隔離及上市櫃覆蓋。

## 2026-10-03 07:32 台北：FCC 光收發模組傳聞核對與 4991 公司曝險

### FCC 一手法規與媒體說法分層

- 2026/08/04 Reuters 報導引述四名未具名知情人士，稱美國政府/FCC 正研擬限制中國製新型資料中心光收發器；這是匿名來源的媒體線索，未附提案原文或官方公告，不當作已發布政策，也不推測消息方動機。[Reuters 報導轉載頁](https://www.investing.com/news/stock-market-news/exclusivetrump-administration-drafting-ban-on-chinese-data-center-devices-sources-say-4833406)
- FCC FCC 26-50 最終規則於 2026/07/22 採納、07/23 發布，Federal Register 文號 2026-18535，2026/09/11 刊登、2026/10/13 生效。規則是前瞻性設備授權限制：只有在 Covered List 指名的 producer/provider 生產 logic-bearing hardware component，且若由該 Covered List 實體製造整機本來就會被禁止授權時，才限制包含該元件的新設備授權。正式摘要明確排除僅因產地而列入的 UAS/路由器項目（除非生產者另被列名），並表示目前不擴至所有 foreign-adversary-controlled entities 或所有零件；先前已授權設備不受此新元件規則影響。這不是「中國製光模組一律禁止進口」的定案。[Federal Register 最終規則](https://www.federalregister.gov/documents/2026/09/11/2026-18535/protecting-against-national-security-threats-to-the-communications-supply-chain-through-the)
- 另有 FCC FCC 26-50 的 Third Further Notice of Proposed Rulemaking，Federal Register 文號 2026-16197，2026/08/07 刊登，意見期限 09/08、reply comments 期限 09/21。它徵詢擴大 Covered List 生產者零件管制、BOM 揭露與 Covered List sectors 認證等可能方案；列出的 sector 範例為 UAS/UAS 零組件/路由器，並未點名光收發模組。這是徵詢意見，不是最終規則；其範圍也不能直接證成 Reuters 報導所稱的按中國產地限制新型光模組。可能存在另一個未公開草案，不能單靠公開 docket 排除。[FCC 26-50 官方 Proposed Rules PDF](https://www.govinfo.gov/content/pkg/FR-2026-08-07/pdf/2026-16197.pdf)
- FCC 2026/06/12 Covered List 更新列出的是指定 producer/provider 實體與特定產地類別；本次公開資料未找到光收發模組專項列項或把 4991/GCS 點名的官方文件。這表示政策範圍仍有不確定性，不代表未公開草案不存在。[FCC Covered List 更新](https://docs.fcc.gov/public/attachments/DA-26-585A1.pdf)

### 4991 環宇-KY：有直接產業曝險，但無可量化的政策方向

- 環宇 2025 年報刊印日 2026/04/05。合併營收 NT$2,195.155M、年增 25.43%；毛利 NT$1,034.555M、年增 56%；歸屬母公司淨利 NT$17.469M、EPS NT$0.15（前一年淨損 NT$236.986M、EPS -NT$2.13）。AOC 自有品牌光電元件營收 NT$1,508.495M、占 68.72%，高於 2024 年 NT$1,076.886M、占 61.54%。這證實公司營運與光電元件/光收發模組鏈有實質關聯，惟 2025 淨利率仍約 0.8%。[環宇 2025 年報（中文）](https://www.gcsincorp.com/tc/pdf/2025_Annual_Report_CN.pdf)
- 年報內部有兩個應保留的數字品質問題：股東報告稱 AOC 年增約 44%，但產品表兩年金額計算為約 40.08%；集中度風險敘述稱第一大客戶占 2025/2024 營收 66.19%/57.88%，同一份詳細客戶表卻列 35.70%/57.88%，表內銷貨合計 NT$1,350.604M/1,750.044M 也對應 2023/2024 的公司營收總額，而不是表頭 2024/2025 的當期合併營收。故保留「公司敘述 44%、66.19%」與「表格可重算/標示不一致」兩者，不把 44% 當精確 AOC 成長率，也不把 66.19% 當已由表格核實的 2025 集中度；待 MOPS 更正或下一份公司揭露釐清。
- 管理階層在年報列出 2026 可檢驗目標：發射端元件通過客戶驗證並取得量產訂單；截至本輪未用年報目標推定已通過或已接單。環宇另持有上海宙鎵光電 48%，累計現金投資約 US$5.016M，投資目的包括拓展光電元件生產/銷售；這是中國營運/投資的反向供應鏈脈絡，但沒有揭露其光模組對美出口或受 FCC 規則涵蓋的法律實體，不能直接推成利空。
- 對 10/02 日線的 4991 動能候選，FCC 傳聞僅列事件風險背景，不新增正負方向票、不增加獨立訊號家族、不調通知門檻。可受益的非中國替代需求與中國投資/客戶集中風險彼此相抵，現有一手資料沒有訂單、客戶或地區營收足以計算淨影響。後續可驗證項目：FCC 是否發布光收發器專項原文/列入規則；環宇 2026 發射端元件認證、量產訂單與營收；年報 AOC 成長率及客戶集中度數字是否更正。

本次僅追加官方文件與公司年報核查結果；未改程式、資料庫、分數/門檻，未手動觸發或重跑 DAG、未發 Discord、未交易。

### 客戶表期間第二來源核對

- 再讀環宇 2024 年報（刊印 2025/04/07）：明列 FY2023 合併營收 NT$1,350.604M、FY2024 NT$1,750.044M。這與 2025 年報詳細客戶表在標頭標作 FY2024/FY2025 的兩個總額完全相同，確認其詳細客戶表的金額/比例落後一個年度；FY2025 第一大客戶 66.19% 只有風險敘述，未在該表中得到數字交叉支持。暫不把 66.19% 當精確已核數字，待公司更正或後續披露。[環宇 2024 年報](https://www.gcsincorp.com/pdf/2024_Annual_Report.pdf)

## 2026-10-03 07:49 台北：週末訊號、來源覆蓋與通知重核

### 最新快照與 outcome

- 週六沒有新交易日收盤或盤後自然批次。唯讀 DB 重查的最新 daily snapshot 仍為 10/2 20:02 `ready/partial`：母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50。10/2 日線價量列上市 1,050/1,054、上櫃 888/892（上櫃空收盤 20）；法人流匹配上市 1,043、上櫃 788，另 22,966 列 unmatched，缺列與 unmatched 都保留為缺漏、不補零。
- 最新盤中 Airflow 子 run 是 10/2 22:45，自然收集及 `market_signal_notify` DAG state 均 success；DB snapshot 是 `ready/fresh`，但母體/價格/法人/月營收/季財務/候選/通知計數全為 0。這是空盤中掃描，不是個股行情或收盤完整性的證據。
- `market_signal_outcomes` 仍只有 9/24 一個成熟訊號日：`signal_close`、`next_close` 各 81 列，其中各 80 列有方向結果；沒有成熟的 5/10-session 樣本。單日候選彼此相關，且尚未扣交易成本，不用來調整方向、權重或門檻。

### Airflow 子 run 與通知結果

- 最新自然 `source_extension_collect_after_close` logical run `scheduled__2026-10-01T12:00:00+00:00` 實際於 10/2 20:00–20:04:37 執行後 failed。11 個 mapped source 有 10 個成功；map index 9 的 TPEx 個股日線收集失敗，raw/接受/隔離皆 0，`verify_collection_and_triggers` failed。`trigger_market_signal_notify` task 本身 success，不能據此認定來源完整。
- daily 子 run capture 成功並寫入 10/2 `ready/partial` 的 50 個候選；`notify_market_signal_snapshot` 重試三次後 failed。三份 Airflow task log 中兩份回應文字包含 `Request Entity Too Large`；通知 ledger 有一筆日線摘要 failed，另有一筆流程故障通知為 sent（10/2 20:02:46），`notified_count=0`。這是自然排程既有結果，不是本次手動發送；ledger 的 sent 表示 notifier 回成功，沒有外部訊息回執。
- 本地現行 renderer 已將 daily digest 拆成不超過 10,000 bytes 的批次；此前靜態檢查記錄為三批估算 8,361/7,456/7,593 bytes。10/2 失敗發生在此拆批版經自然流程驗證前；等 10/5 自然 after-close run 才能確認，不補跑、不重送。
- 最新 `source_extension_collect_process` 是 10/3 04:00–04:04 台北時間，父 run failed：12 個來源成功，`cbc_fx_daily` 第 2 次 JSON decode 失敗。最新新聞自然 run `news_ch_collect_process` 為 10/3 05:30–05:38 success；TNL/iThome 原文頁僅列日期、DB 對應發稿時間為 00:00，按日期粒度處理，不能推定實際發布時分，也不因 batch success 把舊稿算成新訊號。

### 10/2 22:45 來源 effective date 與上市/上櫃覆蓋

- TWSE 月營收 1,086/1,086/0、TPEx 892/892/0；兩者最新有效月份均為 2026/08、官方出表日 9/17。以目前證券主檔交叉比對，TWSE 1,054/1,054 個現行上市代碼有匹配，另有 32 個收集代碼不在現行上市/上櫃主檔；TPEx 892 個收集代碼中 891 個匹配現行上櫃主檔，另 1 個未配對，同時有 1 個現行上櫃主檔代碼未出現在此期收集列。未先判定未配對原因，均不視為零營收。
- TWSE 公司事件 78/78/0，TPEx 公司事件 42/42/0；兩邊最新 `effective_date=2026-10-01`，分別 52 與 34 個發行人。這是事件列，不是全市場母體覆蓋率。
- TWSE 財測差異名單最新自然 run 7/7/0，資料期 2026Q2（effective date 6/30）、5 家發行人；兩個連續輪詢後 source store 有 14 列但只形成 7 個相異語意鍵。`market_signals.py` 目前按財報季、涵蓋期間與實績/預測值指紋去重，因此不把重複列算成多個獨立訊號；本輪不改程式或回補資料。
- 本輪唯讀 runtime 核對沒有新增交易日訊號樣本，也沒有足以支持調整候選條件或通知權重的新量化依據。FCC/4991 研究仍按前節標成正式規則、提案、匿名媒體線索與公司年報事實，不加方向票。本輪只追加狀態與覆蓋核對；沒有寫 DB、改程式/門檻、跑測試、手動觸發 DAG、發 Discord 或交易。 
## 2026-10-03 08:04 台北：TPEx 月營收代碼時點差與 6461 訊號拆解

### 月營收覆蓋差異：已找到時點原因

- 更正 07:49 所記「TPEx 有 1 個收集代碼未配對、現行主檔有 1 碼未收集」：兩筆差異都有上市狀態時點解釋，暫不視為 2026/08 月營收漏收。7856 漢測於 2026/09/22 才轉上櫃，晚於 8 月營收期間；8183 精星於 2026/09/23 停止交易、10/01 終止上櫃，故 8 月營收列仍存在、10/03 現行主檔已無該碼。[TPEx 漢測掛牌公告索引](https://mis.tpex.org.tw/ib150news.aspx)、[TPEx 精星終止上櫃公告](https://www.tpex.org.tw/storage/eb_data/11509/11500054131.html)
- 因此本次 TPEx 月營收覆蓋重新判讀為 2026/08：收集 892 碼、892 接受、0 隔離；相對 10/03 現行 892 碼主檔的差異可由期間後新掛牌/下櫃解釋。TWSE 仍為 1,086/1,086/0；其 32 個額外收集碼尚未在本輪逐碼解釋，保留為待核、不當成零營收。這是時點核對，不代表其他月份或其他來源完整。

### 6461 益得｜10/02 候選的營收反向脈絡

- 10/02 日線資料庫快照列 6461 為偏多、四個家族對齊（價、量、法人、月營收）：5 日報酬 +10.21%，相對 TPEx +7.03 個百分點；收盤 NT$26.45，突破前 20 日高 NT$25.75；量能擴張 5.0 倍；5 日法人合計淨買超 100,663 股、4 個觀察日。新聞 48 小時樣本為 no_match_in_sample，只表示該樣本沒匹配，不表示沒有相關新聞。此為歷史候選描述，不是交易建議。
- TPEx 官方 2026/08 月營收列的單月年增 +1,102.98%，但金額僅 NT$5.654M；公司原因說明為經銷商備貨增加。本年累計營收年減 56.20%，公司說明包含去年認列海思科完成合約的技術服務收入 NT$68.04M。7 月營收僅 NT$0.211M、單月年減 70.24%，累計年減 61.87%；6 月營收 NT$6.162M、單月年減 91.26%，累計年減 61.80%。因此 8 月高年增率受低基期/出貨節奏影響，不足以當作持續需求或獲利改善的證據；公司自己的說明與年初至今數字應同時出現在訊號卡上。[TPEx 月營收開放資料來源](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 這筆 8 月官方來源列是本專案先前成功收集並保存的資料；本輪直接重抓官方 API 遇 Cloudflare 1016，故不是本輪新鮮抓取。資料庫僅保留 6–8 月當期營收列，沒有對應的前一年單月金額，不能自行計算或宣稱已核對近三月營收合計。
- 不將營收子指標、年增百分比與累計百分比拆成多張獨立方向票：此候選雖有四個訊號家族，營收家族本身須附上公司備貨說明和 YTD 反向資料；整體仍屬動能觀察，沒有足夠新證據調權重或改通知門檻。

### 券商原始研究搜尋結果與後續驗證

- 針對 6461 搜尋富邦、永豐/元大公開官方頁面與可取得原始文件，本輪未找到可核實的 2026 年個股原始研究全文。搜尋結果中的一般股票介紹、承銷文件、同業比較均不當作 6461 當期評等；故不替不存在於可見證據中的作者、估值法、EPS/營收假設、風險、預測或發布動機打分。結論是「本輪未找到可公開核實原文」，不是斷言券商沒有報告。
- TPEx 官方即時 API 路徑本輪受 1016 阻擋；下次自然來源 run 檢查 8 月公告列是否仍可讀及 effective date，再驗證新 renderer 是否在 10/5 自然日線通知呈現累計年增、公司說明及來源連結。未手動重跑 DAG、未發 Discord、未交易，也未變更程式或訊號門檻。

### 08:08 台北 Airflow 最新 run 唯讀刷新

- Airflow CLI 最新 run 清單與 07:49 盤點一致：10/2 20:00–20:04:37 的 after-close 收集父 run failed；由該流程啟動的 10/2 日線 market_signal_notify run 於 20:02:33–20:08:54 failed；10/2 22:45 最新盤中 run success；10/3 05:30–05:38 新聞收集 run success。沒有新交易日或較新的自然日線/outcome run。DB 快照列數與 outcome 成熟樣本仍以 07:49 唯讀查詢所記為最後核實值。

## 2026-10-03 08:17 台北：月營收欄位解包缺陷修正

### 根因、影響範圍與修正

- 根因：dags/source_extension/market_signals.py 的 _load_daily_candidates 在新增月營收內容列後，revenue_by_code 保存 9 欄 tuple；原發布時效檢查卻把該列解包給 8 個變數，遇到任一新鮮月營收代碼就會拋 ValueError: too many values to unpack (expected 8)，阻斷候選產生。capture_market_signal_snapshot 會在 1972 行呼叫此函式，因此下一次自然日線執行可能失敗。
- 修正：時效檢查只讀該列第 2 欄發布時間，不再重複解包整列；後續建立訊號卡的解包仍完整接收 9 欄。價量/法人/營收方向條件未改，沒有根據單一 6461 案例調權重或門檻。
- 時序核對：舊的 10/2 20:02 快照在此段程式更新以前。修正前檔案修改時間是 10/3 06:03 台北，晚於 10/2 自然收集，因此舊 run 的候選成功不能驗證新增欄位路徑。容器與工作目錄指向同一份程式。
- 影響範圍掃描：全 repo 搜尋 revenue_by_code/revenue_rows 後，只找到這個查詢/組裝/消費路徑；九欄 append 與最終九欄解包一致。三月摘要只取必要的 period/amount/prior-year 欄位，沒有同形解包風險。

### 驗證與尚待核實

- 在 Airflow scheduler 容器的 Python 對目前掛載來源做 AST parse/compile；靜態斷言確認 revenue_by_code append tuple 寬度 9、revenue_rows[-1] 的剩餘 tuple 解包寬度 9、發布時間索引為 1。使用含益得欄位的 9 欄資料列重現抽取流程，發布日、YTD -56.20% 與公司備貨說明均保留。
- 沒有執行測試套件、DAG、資料庫寫入或通知。此檢查不等於 end-to-end 驗證；等 10/5 自然日線 run 核對候選數、source_status、營收有效月份/收集接受隔離列數、通知 ledger 與卡片中的 YTD/公司說明/官方來源連結。

### 2026-10-03 券商原始研究檢索：6174 安碁、8289 泰藝

- 以兩檔代碼/名稱搜尋富邦與永豐官方研究頁，本輪未找到可核實的 2026 個股原始研究。可見的永豐結果是 2026 SinoPac+ ESG 評鑑方法與評鑑結果頁，不是個股評等或財務估值；其中數據一致性、可比較性、量化目標與時程可作為公司揭露品質檢查項，但不計方向票，也不據此推斷券商研究動機。[永豐 2026 方法學](https://scm.sinotrade.com.tw/Article/Inner/7e71d519-bdfb-4de4-8f8b-64b0a87bf65b)、[永豐 2026/06 評鑑說明](https://scm.sinotrade.com.tw/Article/Inner/7059699c-70c0-459e-8032-bb9b1a60b06a)
- 此結果只表示本輪公開搜尋未找到原文，不表示券商沒有報告；若後續取得個股原始研究，仍需補作者、日期/資料截止日、估值方法、EPS/營收假設、預測修正、反方風險與可檢驗預測。



## 2026-10-03 08:48 台北：創新板覆蓋、週末事件品質與新聞發布時間

### 最新訊號、收集與成熟樣本

- 最新日線快照仍是 10/02 20:02 ready/partial：母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、notified_count=0。舊價格表分市場為上市 1,050/1,054、上櫃 888/892；缺列及 unmatched 不補零。最新一般盤中 run 為 10/02 22:45 ready/fresh，資料家族與候選全為 0；fresh 只是盤中檢查狀態，不是有報價。
- 10/02 20:00 自然 after-close 父 run 仍 failed：11 個 mapped source 有 10 個成功，TPEx 個股日線 map index 9 失敗，raw/接受/隔離為 0/0/0；日線子 run 雖產生 50 候選但通知失敗，notified_count=0。尚未有下一交易日收盤樣本。
- 10/03 08:00 自然週末收集父 run failed，5 個 source 中 3 成功、2 個 TPEx source 失敗（HTTP 530，重試後仍為 0/0/0）：TWSE 公司事件 70/70/0、TWSE 月營收 1,086/1,086/0、TWSE 公司財測差異 7/7/0（effective date 2026Q2）；TPEx 公司事件與月營收失敗。這些事件列不是全市場分母。最近可用的 TPEx 月營收成功輪次仍是 10/02 22:45 的 892/892/0、effective date 2026/08；TWSE 10/03 08:00 的月營收為 1,086/1,086/0、effective date 2026/08。成功狀態不能替代 effective date 與市場覆蓋檢查。
- 10/03 08:02 週末子 run ready/partial，只處理公司事件，10 張卡、notified_count=10；母體、價格、法人、營收與財務均為 0。通知 ledger 記為 sent 表示 notifier 回成功，沒有外部平台回執；本輪未手動觸發或發送通知。
- market_signal_outcomes 仍只有 2026/09/24 一個成熟訊號日：signal_close 與 next_close 各 81 列、各 80 列有方向結果，命中分別 50、37；沒有成熟 5/10-session 樣本，未扣交易成本。單一日期仍不足以調權重或門檻。

### TWSE 8 月月營收額外 32 碼：分類為宇宙差異，不是漏收

- TWSE 官方月營收 effective date 2026/08：1,086 列、1,086 接受、0 隔離；其中 1,054 碼符合目前上市普通股訊號主檔，另 30 碼是名稱帶 -創 的創新板普通股，2 碼為 DR：9105 泰金寶-DR、912000 晨訊科-DR。30 碼為 2237、2254、2258、2432、3150、4195、4582、4590、6534、6645、6771、6854、6908、6921、6924、6949、6951、6955、6969、6988、7610、7631、7730、7740、7803、7823、7827、7835、8162、8487。
- TWSE 官方 10/02 全市場 MI_INDEX 有上述 32 碼的成交行情；本地舊日線表對這 32 碼當日均無列。因此不可把它們說成當日零量或退市，也不可直接以 32 個營收列擴大訊號母體。2 檔 DR 仍排除於普通股訊號；30 檔創新板是可研究的系統性覆蓋缺口，需先有官方 point-in-time 證券主檔、歷史行情/營收/法人完整性和成熟 outcome 再評估納入。

### 7610 聯友金屬-創：基本面觀察，不是正向通知

- 官方月營收資料：2026/06 NT$752.329M、年增 555.43%；07 月 NT$970.895M、年增 721.99%；08 月 NT$1,134.242M、年增 852.63%，年初至今年增 563.52%。公司說明成長來自鎢製品出貨量與出貨價格較去年同期增加。[TWSE 月營收 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap05_L)
- TWSE 公司資料頁列掛牌日 2025/09/09，主要業務為鎢酸鈉與硫酸鈷；頁面 P/E 49.66 截至 2026/09/29。[TWSE 7610 公司資料](https://www.twse.com.tw/pdf/ch/7610_ch.pdf)
- 10/02 官方收盤 NT$1,725、成交量 663,600；可比的最近四個交易日（9/29–10/02）收盤由 1,730 至 1,725，期間約 -0.29%；同期三大法人淨賣合計 288,101 股。以上與高營收增幅並列，不能只讀單月年增率作多。本地訊號主檔尚未包含該創新板代碼。本輪公開搜尋未找到可核實的 2026 個股券商原始研究；公司於券商活動簡報是 IR，不當作券商報告，也不推測研究動機。

### 8084 巨虹：官方重大風險有明確限定，但盤後送達延遲

- 已存官方 TPEx 個股重大訊息列（source key tpex_company_events、effective date 9/30）記載私募案未收足款而失效；子公司宏康與 Midas 交易條件未成就，已付 US$30M 訂金可能被沒收。公司稱若符合認列條件，2026Q3 可能需認列損失；確切金額、時間及巨虹實際承擔比例仍待月底結算、會計師核閱與董事會確認。公司並披露宏康持股為第三方股東 99.99%、巨虹 0.1%；不能把 US$30M 全數算作巨虹母公司損失，亦不自行修正這組持股數字。[TPEx 公司事件 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- TPEx feed event_time=2026/09/30 22:54:35；公告內另寫發布財務業務資訊/記者會時間為當日 19:30。兩者欄位語義不同，保留原值而不稱為互相矛盾。來源列首次入庫 10/01 14:15 台北時間；8084 已出現在 10/02 日線候選（該輪摘要通知失敗），並在 10/03 08:02 週末 run 出卡，距首次入庫約 41 小時 47 分。週末卡將損失、認列與終止上櫃明確標成條件式，方向屬公司事件風險脈絡，不等於價量交易訊號。10/03 TPEx 公司事件抓取 HTTP 530，0/0/0；這張卡依賴既有已接受官方列，不表示當日 TPEx 收集成功。
- 同一週末通知把 1103 嘉泥同一重要子公司在約 4 分鐘內發布的三份不同公告拆成三張卡。這是可檢查的通知整併機會；保留不同公告事實與來源時點，避免把同一發行人事件重複算成三個獨立多空家族。單次批次不足以證明需調分數，暫只記錄。

### 新聞來源時間與下一步

- 10/03 08:30 自然新聞 run 三個 task 全成功，08:37:32 完成：收集後資料庫總筆數 9,202,013，本批刪除/重寫 650 筆、插入 838 筆。按本批 838 筆的來源日期欄位檢視：The News Lens 90 筆，2026/01/20–10/03，全部記為 00:00；iThome 60 筆，2026/09/30–10/02，全部記為 00:00。兩者只能當日期粒度，不能推定發布時分，且 The News Lens 批次混有歷史頁；其餘六來源本批發布欄有時分秒。一次 run success 不代表每篇皆新稿或日期粒度等於實際發稿時間，也未將新聞當成方向票。
- 當沖資料界線維持：TWTB4U/TWTB5U 契約差異未解前不混用；TPEx 個股當沖 endpoint 空資料/授權未確認；官方 OAS 11627 只含市場彙總且須 freshness gate，不冒充個股訊號。unmatched 永不補成零量。
- 本輪沒有改程式、訊號權重/門檻、DB、DAG 或通知設定；沒有跑測試、手動觸發 DAG、發 Discord 或交易。下一步等 10/05 自然 after-close：驗證月營收 tuple 修正、逐來源 effective date/raw/接受/隔離與上市櫃覆蓋、TPEx 行情缺口及通知批次結果；另研究 30 檔創新板能否以官方主檔和足夠歷史納入，不擴入 DR。

## 2026-10-03 09:05 台北：6174／8289 新聞命中與催化品質核對

### 10/02 日線候選與來源狀態

- 10/02 日線四個訊號家族對齊名單中，6174 安碁：5 日 +23.46%、相對 TPEx +20.27 個百分點、量能 2.43 倍、法人淨買 690,983 股（4 個觀察日）；8289 泰藝：5 日 +26.21%、相對 TPEx +23.03 個百分點、量能 3.89 倍、法人淨買 596,406 股（4 個觀察日）。這些是本地 10/02 快照資料，描述動能與資金流，不表示後續報酬。
- 兩檔所配對的 TPEx 官方 2026/08 月營收列來自 10/02 22:45 自然收集輪：上櫃月營收有效期 2026/08、892 列接受、0 列隔離。6174 單月年增 +32.84%、月減 11.31%、累計年增 +26.13%；8289 單月年增 +67.15%、月減 1.48%、累計年增 +18.17%。單月年增支持營收成長，但不直接代表獲利；月減與累計增速需同卡呈現。[TPEx 官方月營收 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 時效交叉核對：本地 stock.個股成交資訊確有 10/02 的 6174 收盤 55.00／量 1,576、8289 收盤 56.10／量 1,925；兩筆候選 as_of_date=10/02、observed_at=20:02:35，5 日價量引用到 10/02。這驗證候選的資料日；同一輪 TPEx mapped 個股日線來源仍失敗、raw/接受/隔離 0/0/0，市場收集狀態仍 partial，不把 mapped source 失敗改寫成「來源完整」。\n
- 最新日線、盤中及收集狀態沿用 08:48 盤點：10/02 20:00 after-close 父 run 失敗於 TPEx 個股日線來源，raw/接受/隔離 0/0/0；10/02 22:45 盤中 run 成功但行情/法人/候選皆為 0；10/03 08:00 TPEx 月營收自然收集亦失敗 HTTP 530，故目前可用的 8 月櫃買營收列仍是前一輪 892/892/0。成功 run 或 fresh 狀態均不代表有新資料。outcomes 仍只有 9/24 成熟訊號日，此兩檔 10/02 候選尚無成熟績效樣本，不調整門檻或權重。

### 新聞是盤勢回顧，題材與公司產品只有寬鬆連結

- ETtoday 原頁 `2026-10-02 10:34:00+08:00` 發稿，10:34:43 更新；內文確實將 6174 安碁和 8289 泰藝列在華新科領漲後的漲停股清單，因此公司名稱命中有效。文章是在描述當時已發生的類股行情，屬同日盤勢回顧，不作為獨立預測訊號。[ETtoday 原文](https://finance.ettoday.net/news/3247556)
- 文章引述 TrendForce 的解釋是日、韓廠減少消費級 MLCC 供應、轉向 AI 相關應用；可核對的是 TrendForce 2026/09/24 的官方 Press Center 公開摘要，早於 10/02 當日媒體回顧，可作為先行的產業背景；本輪未檢視其完整研究或底層數據，也沒有公司別營收／股價因果證據。[TrendForce 原始新聞](https://www.trendforce.com/presscenter/news/20260924-13252.html)
- 兩家公司官方產品資料顯示其主業為石英晶體、石英振盪器／頻率控制元件，而非 MLCC：安碁官方投資人資料、泰藝官方公司簡介及產品頁均如此描述。[安碁官方資料](https://www.aker.com.tw/investors)、[泰藝官方公司簡介](https://www.taitien.com/tw/%E5%85%AC%E5%8F%B8%E7%B0%A1%E4%BB%8B/)。因此「被動元件族群被提及」成立，但文章沒有證明 MLCC 供需變化直接傳導到這兩家石英元件公司的訂單、售價或 EPS；把它標為同族群／產品外溢待驗證，不標成公司別催化。
- 另有媒體資料頁[轉載此稿](https://www.wellnewss.com/post/1056092.html)並標示 16:10:54；那是轉載頁時刻，不能覆蓋 ETtoday 原頁的 10:34 發稿欄位。原稿清單也重複列出 4760 勤凱，與「另外 7 檔」的數量敘述不一致。此為一項可觀察的內容品質瑕疵，進一步支持把媒體清單當背景、不用來當官方個股事實。
- 現行卡片程式將新聞明確放在「新聞背景（不計入多空訊號）」，標示日期、標題／正文首段命中基礎和連結；本案例未顯示需要改變計分。後續可以累積同類樣本，再評估是否新增「個股直接催化／產品關聯／盤勢回顧」解讀標籤；不依單一文章修改 matcher 或門檻。[訊號卡程式](../dags/source_extension/market_signals.py)

### 券商原始研究與後續驗證

- 沿用本 watchlist 08:17 的公開檢索狀態：未找到可公開核實的 2026 年 6174／8289 個股原始券商研究全文，故作者、估值方法、EPS／營收假設、預測修正、反方風險及可檢驗預測均未核。這不表示券商沒有報告，也不推測報告動機。
- 下一自然交易日收盤後核對這兩檔最新價量／法人、月營收與財報進展、新聞原始發布時間、來源有效期及市場覆蓋；等待足夠成熟 outcome 後再檢查訊號表現。未手動觸發 DAG、未發 Discord、未交易、未改程式或計分。

## 2026-10-03 09:38 台北：3162 精確三家族候選、獲利反證與研究來源品質

### 執行狀態與來源覆蓋

- 唯讀確認工作樹；沿用所有既有 M/D/?? 變更。本節沒有執行程式測試、寫 DB、手動觸發/重跑 DAG、發 Discord 或交易。
- 最新日線仍是 10/02 20:02 `ready/partial`：universe 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、notified_count 0。最新盤中 10/02 22:45 `ready/fresh`，各資料家族與候選為 0。週末收集 10/03 08:00 父 run failed：3/5 成功；TWSE 公司事件 70/70/0、月營收 1,086/1,086/0（effective 2026/08）、財測差異 7/7/0（effective 2026Q2）；TPEx 公司事件與月營收因 HTTP 530 各為 0/0/0。TPEx 最近一次月營收成功輪仍是 10/02 22:45 的 892/892/0、effective 2026/08；已依上節的掛牌/下櫃時點差調整覆蓋判讀。
- 10/03 04:00 台北的 fundamentals/source_extension 自然父 run failed（CBC FX 來源失敗），但季營益率子來源各自成功：TWSE effective 2026Q2、1,054 raw/1,049 接受/5 隔離；TPEx effective 2026Q2、884/884/0。按 10/03 現行普通股主檔做 join，TWSE 有 1,018/1,054 個接受代碼可配對、31 個接受代碼不在現行主檔、36 個現行代碼未配對；TPEx 為 884/892 可配對、8 個現行代碼未配對。這是現行主檔比對，尚非 6/30 point-in-time 母體；因此雖然 task success，仍不宣稱市場完整。不得將缺碼補零。
- `market_signal_outcomes` 仍只含 2026/09/24 一個成熟訊號日：`signal_close`、`next_close` 各 81 列，各 80 列有方向結果；沒有成熟 5/10-session 樣本，也未扣成本。10/02 候選沒有績效成熟資料，不據此改權重或門檻。
- 新聞最新 DAG run 的 Airflow logical date 是 10/03 05:30 台北，但實際開始 08:30:00、完成 08:37:32，三個 task 均 success。先前 watchlist 的 08:30 是實際執行時間；05:30 是 logical date。上一個不同 run 才是在 05:30 實際執行。不能把 logical date 當實際收集時間。

### 3162 精確｜10/02 日線候選拆解

- DB 候選有 3 個偏多方向家族：價格、法人、月營收；score 3 是同向資料家族排序，不是上漲機率。價格 5 日報酬 +19.95%、相對 TPEx +16.76 個百分點，收盤 NT$89.60，高於前 20 日收盤高 NT$87.50。5 日法人合計淨買 4,459,110 股（外資 +4,383,736、自營商 +75,374、投信 0），4 個有效觀察日中 4 日淨買。
- 成交資料有 21 個有效交易日，不是缺量：10/02 成交量欄 4,335，前 20 日均量 2,173.4，倍數 1.995；當日漲幅 +2.4%。程式要同時滿足量比至少 2 倍及單日絕對漲跌至少 3% 才建立 volume 家族，因此本次不計 volume 是既有條件結果，不是零量或行情缺列。
- TPEx 官方 8 月營收原始列：NT$1,137.089M，年增 +101.19%、月增 +5.66%、累計年增 +33.42%；去年同月 NT$565.183M，去年累計 NT$4,904.402M，官方備註「訂單量增加」。近三個月金額逐月增加（6 月 897.696M、7 月 1,076.152M、8 月 1,137.089M），但尚無證據把營收加速等同高毛利或 EPS 加速。[TPEx 月營收 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 10/02 20:02 的候選資料沒有 `profitability_context`；季營益率資料直到 10/03 04:00 自然收集才被接納入庫，故不能回填到 10/02 當時可得的候選。官方 Q2 財報在 8/14 已上傳，顯示訊號系統對這份財報的收集/可用性落後公開約 50 日。下個自然日線應確認財務背景是否顯示；歷史點仍保持原樣，不做事後資訊回填。[TWSE 電子財報查詢](https://doc.twse.com.tw/server-java/t57sb01?co_id=3162&colorchg=1&mtype=A&step=1&year=115)
- 交易所季營益率 API 的 3162 原始 `Date=1151002`，資料期 `2026Q2/06-30`；這和正式財報 8/14 上傳日不同。`Date` 先視為官方資料列/快照日期，不能標成報告原始發布時刻。DB 目前只有 Q2 營益率列，沒有可相鄰換算的 Q1 累計基準；故只能顯示 H1 累計營益率，不把它當成單季獲利轉折。

### 官方財報反向證據與催化驗證

- 3162 官方合併財報（115/08/14 上傳，報導期間 Q2/H1）：Q2 營收 2,386.673M、年增 31.9%，但毛利率 11.45% 對去年同期 14.54%，營益率 1.84% 對 4.63%，歸屬母公司 EPS 0.03 對 0.52。H1 營收年增 19.0%，毛利率 10.99% 對 14.00%，營業利益 28.197M 對 162.162M、營益率 0.65% 對 4.44%，歸屬母公司 EPS 0.05 對 1.02。強營收增長和獲利率/每股獲利下滑必須同卡呈現；不推測 FX、移轉或客戶組合等未核原因。
- 可驗證的新業務事件：盟立 9/15 公司公告稱已與精確合資成立奧威科技，目標含機器人平台、關節模組、自主載具與智慧物業維運。[盟立官方公告](https://www.mirle.com.tw/news-detail/671/) 這將 4 月「提議合資」更新為公司宣稱已成立的事件；公告沒有揭露精確持股、投資金額、在手訂單或營收/EPS貢獻，因此是已發生的策略/公司事件背景，尚不是量化獲利訊號。公司官網當前列出的 AI 水冷、半導體和機器人產品同樣不能替代客戶訂單或財務貢獻證據。[精確解決方案頁](https://www.al-alloy.com.tw/solutions)
- 多個金融網站有轉載一份標示為 MOPS 的 5/20「澄清媒體報導」，內文稱公司未發布財務預測；本輪未取得原始 MOPS/TPEx 公告連結，因此只列二手公告轉錄線索，不當成已核一手事實。[MoneyDJ 轉載](https://www.moneydj.com/kmdj/news/newsviewer.aspx?a=04e7c62e-c302-4c09-b7df-5545948d0df0)。這是追查 5/06 券商營收/EPS 預估是否被媒體誤讀為公司指引的待辦，不推測任何報告動機。

### 券商研究品質與後續可檢驗項目

- CMoney 5/06 券商評等彙整稱群益看多、目標價 NT$95、2026 年營收預估 NT$10.105B、EPS NT$3.05；它是券商摘要的二手轉述，不是群益原始研究。[CMoney 摘要](https://cmnews.com.tw/article/cmoneyaicurator-59a78eec-4939-11f1-add8-d89a5303ffd7) 群益公開研究搜尋本輪未找到 3162 原文，故作者/分析師、資料截止日、估值法與倍數、分季營收和毛利/EPS 假設、上修或下修、反方風險、預測期與可檢驗目標均未核；摘要本身不能評作高品質研究。
- 以 8 月實績累計營收 NT$6.544B 計，該摘要的全年營收估值要求 9–12 月合計約 NT$3.561B、平均每月約 NT$0.890B；近期月營收實績超過此月均值，營收目標尚可觀察。相反地，H1 EPS 僅 NT$0.05，達全年 EPS NT$3.05 仍需 H2 約 NT$3.00；要由正式財報驗證獲利率是否明顯回升。此差距提出檢驗問題，並非證明預估錯誤或推論報告動機。
- 分類：3162 是值得保留的「三家族研究候選／獲利反證待整合」，可量化且一手營收和財報的證據品質高；它的預測效力還沒有成熟 outcome 驗證，且 10/02 日線來源 partial、短線已有延伸、淨利與營益率大降。因此不升格為高品質交易訊號、不調分數或通知門檻。
- 下個自然交易日收盤後：查 3162 是否仍符合價/量/法人條件；確認候選卡顯示 Q2 H1 營益率 0.65% 的累計 basis、官方連結及「缺 Q1 前季資料，無法計算單季變化」；驗證 9 月營收實績和奧威科技有無已披露可量化訂單/營收；確認 after-close 上櫃行情接受列數及 TWSE/TPEx 覆蓋；持續等多個交易日 outcome，再評估通知品質。沒有成熟樣本前維持現行門檻。
## 2026-10-03 09:54 台北：4991 環宇-KY 四家族候選、政策傳聞與財報口徑

### 最新 run、來源有效期與成熟 outcome

- 唯讀檢查後保留工作樹既有變更。最新日線仍是 10/02 20:02:35 `ready/partial`：universe 1,946、價格 1,938、法人 1,891、月營收 1,945、季財務 1,902、候選 50、notified_count=0。10/03 08:02 週末事件子 run 為 event-only `ready/partial`，10 張卡；其他個股行情/法人/營收/財務家族均為 0。通知 ledger 的 sent 僅表示 notifier 呼叫回成功，無外部平台回執；本輪沒有發 Discord。
- 10/03 08:00 自然來源輪：TWSE 公司事件 70/70 接受/0 隔離；TWSE 月營收 1,086/1,086/0，effective 2026/08，其中 1,054 碼符合現行上市普通股母體，另有 30 檔創新板普通股和 2 檔 DR；TPEx 公司事件與月營收均 HTTP 530、0/0/0。TPEx 最近成功月營收輪為 10/02 22:45，892/892/0、effective 2026/08；該筆 4991 營收列為 NT$262,258 千元。來源成功與市場完整度分開記錄，未把 unmatched 補零。
- TPEx 季營益率 10/02 20:00 自然輪成功，effective 2026Q2，884 raw/884 accepted/0 rejected；截至目前的普通股主檔配對數與 point-in-time 母體仍分開核。4991 原始列 `effective_date=2026-06-30`、營益率 18.87%、H1 累計營收 1,484.78 百萬元。此列 `created_at=9/24 20:00`、10/02 20:00 才 `observed_at`，候選 run 20:02:35 開始；欄位時間支持它在候選前可見。
- `market_signal_outcomes` 共 162 列，仍只成熟 9/24 一個訊號日、1 個交易日持有窗：`signal_close` 81 列、80 列有方向結果（50 正、30 負）；`next_close` 81 列、80 列有方向結果（37 正、43 負）。沒有成熟 5/10 日樣本，未扣成本；單一日期不足以重設門檻。

### 4991 環宇-KY：價格、量、法人與營收四家族同向

- 10/02 日線快照 score=4，家族是價格、量能、法人、月營收；score 代表家族數，不是上漲機率。5 日報酬 +19.33%、相對 TPEx +16.15 個百分點，收盤 NT$534，高於先前 20 日收盤高 NT$530；量比 2.05 倍；近 5 日有效 4 天法人淨買 4/4，累計淨買 1,873,682 股。該輪整體 `source_status=partial`，此候選尚無成熟 outcome。
- TPEx 2026/08 月營收 262.258 百萬元、年增 +45.62%、月增 +80.31%、年初至今累計年增 +45.57%；7 月為 145.446 百萬元、年減 6.93%。因此 8 月營收回升值得追蹤，但月增幅含低基期反彈，應與累計值並列。原始列 payload 的「出表日期」為 2026/09/17，而 `source_records.created_at` 是 2026/09/11；來源欄位時間不一致，未用 created_at 推定實際公告／首次可得時刻。[TPEx 月營收 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 10/02 候選細節配到 ETtoday 和鉅亨網兩篇同日「美國擬限中國光模組」新聞，但只存日期，沒有精確發稿時分；兩篇是市場新聞，不能代替 FCC 公告或公司訂單。10/02 候選保存的細節沒有 `revenue_context`／`profitability_context`，只保留月營收 YoY 百分比，缺金額、累計趨勢、資料 basis 及官方連結。特別是 4991 的 H1 季營益率原始列在日線開始前已成功入庫，故此處要驗證的是現行候選組裝/卡片呈現，而非聲稱該公司資料未收集。

### 公司一手資料、政策催化和反向證據

- 環宇官方 2026/08/05 Q2 法說簡報是公司資料，不是券商研究。Q2 營收 NT$770.103M、年增 57.2%、季增 7.8%；但營業利益 NT$98.046M、季減 46.2%、年減 16.3%，營益率 12.7%，低於 Q1 25.5% 與去年同期 23.9%；EPS NT$0.92，低於 Q1 NT$1.67，高於去年同期 NT$0.13。H1 營益率 18.9% 對去年同期 16.0%，H1 EPS 2.58 對 -0.57；H1 非營業項目淨額 NT$99.217M，包含 Q1 處分投資利益 NT$163.760M，不能把 H1 EPS 改善直接外推為 Q2 營運加速。[環宇-KY 官方 Q2 法說簡報](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)
- TPEx 季營益率 feed 的 4991 18.87% 是截至 6/30 的 H1 累計值，不是單季 Q2；目前 source_records 對該檔只找到 6/30 一列，不能單靠此列計算季對季。公司簡報另列出 Q2 單季 12.7% 與 Q1 25.5%，兩種口徑需在卡片上分開標示。[環宇法說會日期與簡報頁](https://www.gcsincorp.com/tc/investors/Investor_Conference.php)
- 公司產品官網列 GaAs/InGaAs PIN 光探測器與晶片，產品列表可見最高 56G 型號；該頁未獨立證實 10/02 媒體所稱 200G PD 量產，也不能據此否定公司另有新產品。政策新聞描述的是中國光模組／特定高速模組，不等於已證實環宇晶片取得新訂單或 EPS 貢獻。[環宇官方 PIN 光探測器產品頁](https://www.gcsincorp.com/tc/optical_chips/GaAs_InGaAs_PIN_Photodetectors.php)
- 本輪公開檢索未找到支持「3.2T 中國光模組全面禁令」的 FCC 原始規則/公告。可查到的 FCC 26-50（2026/07/22 通過、07/23 發布、09/11 刊於 Federal Register）限制的是：若邏輯硬體由 FCC Covered List 指定實體製造，且整機由該實體製造會被禁認證，則該新設備不得取得 FCC equipment authorization；這不是依 3.2T 速率或中國製造地全面禁進口光模組。故將 10/02 文章作「二手政策傳聞／未核原文」，不當成公司別利多或方向票。[Federal Register：FCC 26-50 final rule](https://www.federalregister.gov/d/2026-18535)
- 本輪以元大、永豐、capital.com.tw 與 CTBC 公開入口搜尋，未找到可核實的 4991 原始券商研究。缺作者、報告日/資料截止日、估值法、EPS/營收假設、修正方向、反方風險和可檢驗預測，因此不引用二手新聞中的「法人預期」當券商模型，也不推論研究動機。

### 判讀與下個自然日檢查

- 分類為「高品質資料支撐的四家族研究候選」，不是已驗證的可交易訊號：價量/法人/營收方向一致且月營收可量化，但全市場日線為 partial、短線已延伸；Q2 營益率與 EPS 較前季收縮，新聞催化沒有政策原文，且沒有成熟績效樣本。本輪不升級通知等級、不改權重/門檻。
- 10/05 自然日線後檢查現行程式是否在 4991 候選卡顯示 8 月營收金額、YoY/MoM/YTD、官方連結，並將 Q2 財報 18.87% 清楚標成 H1 累計；另人工對照官方 Q2 單季營益率 12.7% 對 Q1 25.5%，不可混為同一欄。一起核對 TPEx 成功/失敗來源有效期、raw/accepted/rejected、TWSE/TPEx 覆蓋、after-close run 與 outcome 成熟狀態。未手動觸發/重跑 DAG、未送 Discord、未交易、未執行測試。

## 2026-10-03 10:07 台北：4991 年報更新、產品成熟度與來源再分級

### 唯讀狀態刷新

- 重查工作樹時保留全部既有 M/D/?? 項目。最新日線仍是 10/02 20:02 `ready/partial`：母體 1,946、價格 1,938、法人 1,891、營收 1,945、財務 1,902、候選 50、通知 0。10/03 08:02 最新盤中子 run 是 `ready/partial`、event-only 10 卡/10 筆 notifier `sent`，個股母體/價量/法人/營收/財務全為 0；`sent` 沒有外部平台回執。最新自然 after-close 仍是 10/02 20:00 的 11 個 mapped sources 有 10 個成功、TPEx 個股日線失敗而父 DAG failed；未手動觸發或重跑。
- 10/03 08:00 source-extension 成功來源重新核有效期、列數與母體：TWSE 月營收 1,086/1,086 接受/0 隔離、effective 2026/08；對照目前上市普通股主檔為 1,054 碼，其餘 30 碼創新板普通股、2 碼 DR，不能混作普通股收集漏列。TWSE 公司事件 70/70/0、effective 10/02，是事件列而非市場覆蓋分母；TWSE 財測差異 7/7/0、effective 2026Q2，是 5 家發行人的事件樣本。TPEx 公司事件及月營收該輪均失敗 0/0/0；月營收最近成功輪仍為 10/02 的 892/892/0、effective 2026/08。失敗及不在普通股母體的列不補零。
- 成熟 `market_signal_outcomes` 仍只有 9/24 訊號日的 `signal_close`、`next_close` 各 81 列、各 80 列有方向結果；沒有成熟 5/10-session 樣本、未扣成本。沒有新的績效證據支援調分或調門檻。

### 公司一手資料更新：2025 年報與 2026 Q2 法說簡報

- 環宇官方 2025 年報刊印日為 2026/04/05。2025 合併營收 NT$2,195.155M（+25.43%）、毛利 NT$1,034.555M（+56.04%）、營業利益 NT$403.852M（+109.50%），但稅後淨利僅 NT$17.469M、EPS NT$0.15；報表列營業外支出 NT$390.475M。2025 營業活動現金流 NT$396.121M，較 2024 年減 10.78%，公司說明包含暫繳稅款因素。[環宇 2025 年報](https://www.gcsincorp.com/tc/pdf/2025_Annual_Report_CN.pdf)
- 年報產品別表列自有光電元件 AOC 2025 營收 NT$1,508.495M、占 68.72%，2024 年為 NT$1,076.886M、占 61.54%；按表中金額計算年增約 40.08%。同份股東報告書敘述 AOC 年增約 44%，與表格算術不一致，先保留兩個公司披露值並列、待查明口徑，不拿 44% 當確認實績。年報風險段直接列最大客戶占總銷貨 66.19%（2024 年 57.88%）；銷售地區表列中國 2025 年 NT$1,566.842M、占 71.38%。地區、最大客戶、AOC 產品三者沒有足夠細分交叉表，不推定最大客戶在哪個地區或該地區收入全屬光通訊/FCC 影響產品。
- 產品成熟度按年報自身用語分級：公司稱 2025 年 PD 產品市佔表現高，且自有品牌產品已開發/量產之應用範圍包含 200Gbps 至 1,600Gbps；但 200G PD 的單獨營收、出貨量/客戶訂單未揭露。年報稱 CW Laser 仍投入研發、發射端元件希望於 2026 年通過客戶驗證並取得量產訂單；因此年報本身未把 CW Laser 或發射端元件寫成已有量產訂單。這是待後續公司公告與月/季實績檢驗的里程碑，不加為已實現營收家族。
- 公司 8/05 Q2 官方法說簡報將 H1 和 Q2 單季分開：H1 營收 NT$1,484.783M、營業利益 NT$280.185M、營益率 18.9%、EPS NT$2.58；H1 處分投資利益 NT$163.760M。Q2 單季營益率 12.7%、EPS NT$0.92，均低於 Q1 的 25.5%、NT$1.67；因此 H1 改善與 Q2 季減要同時呈現。[環宇 2026 Q2 官方簡報](https://gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)

### 券商入口、媒體線索與候選判讀

- 找到元大網站轉載的《時報》記者張漢綺 8/05 17:08 法說會報導，提到 200G PD/CW Laser 已可量產及年底訂單能見度；作者標為時報記者，頁面沒有券商分析師、目標價、估值方法或 EPS/營收模型，故分類為證券網站所載媒體稿，不是元大原始研究。公司 IR 頁確實連到該次官方錄影，但本輪網頁無可檢索字幕/逐字資料，未把這些媒體引述升格為公司原始逐字證據。[元大網站上的時報報導](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202608050170831)、[公司官方法說會頁](https://www.gcsincorp.com/tc/investors/Investor_Conference.php)
- 本次公開搜尋仍未找到 4991 券商原始研究全文；作者、研究資料截止日、估值法、EPS/營收假設、上修/下修、反方風險及可檢驗預測仍未核。不要把「掛在證券商網站」等同「券商分析師研究」，也不猜報告動機。
- 綜合分類維持「高品質官方資料支撐的四家族研究候選」，不升級為高品質可交易訊號：多家族價格/量/法人/營收對齊及 AOC 年度成長有公司資料支持；整體行情來源仍 partial、短線已延伸、Q2 單季獲利率回落、H1 EPS 含重大處分利益、最大客戶集中度升至 66.19%，且 AOC 年增披露有 40.08% 對 44% 的待釐清差異。下一自然交易日驗證候選卡中的營收/獲利 basis 和 source coverage，並追蹤 9 月營收、發射端驗證/訂單及成熟 outcome；本輪不改計分、通知門檻或程式，不跑測試、不送 Discord、不交易。
- 沿用已核實的個股當沖資料界線：TWSE TWTB4U 資格資料與 TWTB5U 個股成交統計契約不同；TPEx 個股 endpoint 曾回空資料且授權未確認；官方 OAS 11627/當沖統計只有市場彙總，須 freshness gate。這些不是本輪重新測端點的結果，不接入個股訊號，也不將 unmatched 當零量。

## 2026-10-03 10:16 台北：把月增營收納入背景欄位

- 靜態檢查現行日線候選組裝及卡片 renderer：已呈現最近月營收金額、單月年增、年初至今累計年增、近三月合計年增、公司申報說明及官方來源連結，但沒有較上月的變化。4991 的 2026/07 為 NT$145.446M、08 月 NT$262.258M，TPEx 原始月營收列換算月增 +80.31%；只顯示 YoY/YTD 會漏掉這個可量化但可能受低基期/出貨時點影響的背景。
- 已在 `market_signals.py` 增加 MoM 背景計算與 renderer 欄位：只用最近有效月份和緊鄰的前一曆月實績；若少月、金額缺漏或前月金額小於等於零就省略，不補零。卡片標為「較上月」且明示「背景，不另計方向票」。此欄不寫進 `signal_values`、`family_directions`，不改營收方向條件、候選門檻、權重或排序。
- 手動靜態檢閱 helper、組裝及顯示三處呼叫，MoM 欄位只接到背景 renderer。未跑測試或其他驗證命令；未重跑/觸發 DAG、未送 Discord、未交易，故尚無自然通知成品確認。下一個自然交易日的日線卡核對 4991 顯示約 +80.3%，並確認沒有增加方向家族或重複計分；同時核對該自然 run 的有效月份、TWSE/TPEx 接受/隔離列數及市場覆蓋。
- 本輪唯讀資料狀態未變：10/02 日線 `ready/partial`、50 候選/0 通知；10/03 08:02 event-only 盤中子 run 10 卡；10/03 08:00 TPEx 公司事件與月營收失敗 0/0/0；outcome 仍只有 9/24 一個訊號日，無成熟 5/10-session 樣本。沒有新績效證據支援調分或門檻。

## 2026-10-03 10:33 台北：益得（6461）低基期判讀與營收基數呈現

- 唯讀資料庫回查 10/02 最新日線候選：益得 score=4，為價格、量能、法人、月營收四個同向家族；近 5 日報酬 +10.21%、量比 5.00 倍，法人流資料只有 4 個有效日、淨買 100,663 股，略高於現行 100,000 股門檻；8 月單月年增 +1,102.98%。分數只是家族數；全市場日線仍 `ready/partial`，不代表機率或可直接交易。
- TPEx 官方月營收列顯示 2026/08 營收 5,654 千元、去年同期 470 千元、前月 211 千元，月增 +2,579.62%；1–8 月累計 42,312 千元、去年同期 96,602 千元，年減 56.20%。公司申報說明將 8 月增加主要歸因經銷商備貨；累計減少則主要因 2025 年認列海思科完成合約技術服務收入 6,804 萬元。[TPEx 月營收官方資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 只作比較基數敏感度試算：若從去年同期累計 96,602 千元扣除公司提及的 68,040 千元技術服務收入，餘額 28,562 千元；今年 42,312 千元相對該餘額約高 48.1%。這不是公司揭露的調整後營收，也未驗證此收入能否視為非經常；8 月 5,654 千元亦與 4–6 月 5,556/7,112/6,162 千元相近，而 7 月僅 211 千元，因此單月年增、月增都受比較基數影響，不能單獨解讀為持續加速。
- TPEx 公司事件列及季營益率列可交叉核對 H1 2026 營收 36,447 千元、毛損 89,730 千元、營業損失 204,658 千元、EPS -1.22；營益率 -561.52% 是上半年累計口徑，缺 Q1 累計基準時不換算成 Q2 單季趨勢。[TPEx 公司事件](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)、[TPEx 季營益率資料](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 公司官網 2024 年財報列 2023/2024 營收 25,847/39,779 千元、EPS -2.81/-2.68。[益得 2024 年財報](https://www.intechbiopharm.com/CKEdit/upload/files/202512310000.pdf) CMoney 小編 2023/12/12 的券商評等摘要稱台新投顧看多、目標價 40 元，並摘要預估 2023/2024 營收 29/46 百萬元、EPS -2.38/-2.74；與實績的簡單差異為營收約低 10.9%/13.5%、EPS 分別比預估多虧 0.43 元/少虧 0.06 元。[CMoney 二手評等摘要](https://cmnews.com.tw/article/cmoneyaicurator-3518aa20-98dc-11ee-97bc-2988ced2af20) 該頁沒有券商原文、分析師、模型/估值法、資料截止日、目標價期限、修正歷程或反方風險，故只列 C 級二手線索，無法判斷目標價合理性或報告動機；不將舊 NT$40 當現行參考。
- 發現單顯示 MoM 百分比卻不顯示比較分母，會讓 6461 的 +2,579.6% 難以辨認前月僅 211 千元。已在 `market_signals.py` 的背景 context 增加前月金額，卡片預期呈現「較上月 +2,579.6%（前月 211 千元；背景，不另計方向票）」；不寫進 `signal_values`、不新增方向家族或改門檻。手動靜態檢閱 helper 與 renderer；尚未經自然卡片驗證，未跑測試、未觸發 DAG、未送通知或交易。
- TPEx 公司事件 normalized records 的最新有效日仍是 10/01；10/03 週末兩次自然收集都 HTTP 530、0/0/0。故當前不可宣稱公司事件來源已涵蓋 10/02 之後消息。下一個交易日自然 run 核對有效日期、接受/隔離、TWSE/TPEx 覆蓋，並確認 4991/6461 通知卡是否分別顯示 MoM 分母、YoY/YTD、公司原文說明與上半年累計營益率。

## 2026-10-03 10:41 台北：6174 安碁獲利改善、現金轉換與可轉債反證

### 官方財報新增交叉驗證

- 讀取安碁官網的 2026 Q2 合併財報（報導期間截至 6/30；董事會 8/11 通過）。Q2 單季營收 NT$197.877M，年增 28.7%；毛利率約 38.1% 對去年同期 36.8%；營業利益 NT$30.448M、年增 102.6%，營益率約 15.4% 對 9.8%；稅後淨利 NT$22.058M、EPS NT$0.44，去年同期為淨損 NT$5.401M、EPS -0.11。H1 營收年增 22.4%，營業利益 47.282M 對 21.769M（約 +117.2%），營益率 13.0% 對 7.3%，淨利 38.540M 對 8.154M，EPS 0.77 對 0.16。Q2 單季與 H1 累計分開標示，避免把累計值當單季。[安碁 2026 Q2 官方財報](https://www.aker.com.tw/upload-files/company/financial-report/2026/2026Q2.pdf)
- 現金轉換未與獲利同步：H1 營業現金流入 NT$20.131M，約為淨利的 52.2%，去年同期則流出 NT$15.240M。現金流量表列應收帳款占用 NT$37.381M、存貨占用 NT$61.800M；期末應收帳款 NT$199.264M，年增約 47.9%，存貨 NT$364.659M，年增約 19.6%。期末短期借款 NT$185M，去年同期 NT$121M，增加約 52.9%。這些是流動資金/營運資金風險檢查項，不足以單獨判定收入品質或信用惡化。[同一份官方財報](https://www.aker.com.tw/upload-files/company/financial-report/2026/2026Q2.pdf)

### 融資事件、新聞與券商研究分級

- 官方重大訊息顯示公司董事會 8/11 通過國內第三次無擔保可轉債：面額 NT$300M、以面額 102% 發行、3 年期、票面利率 0%，用途為充實營運資金及償還銀行借款；當時轉換價格、轉換/賣回/買回條件稱待主管機關核准後另訂/公告。9/30 公司公告已收足 NT$306M 應募款。面額相當於 6/30 權益 NT$668.365M 的約 44.9%；這是規模比率，不是換股稀釋率。轉換價未核前不估稀釋股數。[TPEx 官方公司事件資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 價量候選截至 10/02 近 5 日 +23.46%，公司債款收足公告在 9/30；事件時間與股價走勢相鄰，但不能由先後關係宣稱公告造成漲幅。10/02 元大網站轉載的盤勢文章把安碁與 MLCC 題材並列；它是市場稿而非安碁公司訂單或券商估值研究。安碁官網列示的主要產品是石英晶體/振盪器等，故 MLCC 敘事只列市場背景，不加方向家族。[元大轉載盤勢稿](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202610020103820)、[安碁官方產品/公司資訊](https://www.aker.com.tw/zh-CN/investors)
- 本次以富邦、永豐、元大、CTBC 等券商公開入口補查，未找到可公開核驗的安碁個股原始研究。作者、報告日/資料截止日、估值方法、EPS/營收假設、上修/下修、反方風險、預測期與可檢驗預測均未核；券商站上的新聞或市場稿不替代原始研究，也不據此推測研究動機。

### 候選判讀與後續核驗

- 分類為「官方基本面有量化支撐的四家族研究候選」，不是已驗證的高品質交易訊號：H1/Q2 單季營收與營益率改善是正面證據；營業現金流僅淨利約一半、應收/存貨及借款增加、3 億元可轉債條件未完全落地、10/02 五日漲幅已延伸、全市場日線仍 partial，都是需同卡呈現的反證/風險。score=4 仍只代表既有價格、量能、法人、月營收四個家族；尚無此候選成熟 outcomes，不改分數、權重或通知門檻。
- 10/05 自然日線後核對候選卡是否保留 2026/08 營收金額、YoY/MoM/YTD 與官方來源，並把營益率明確標為 H1 累計（若資料足以還原 Q2 單季則另欄顯示）；盤點卡片是否呈現 9/30 可轉債官方事件及其日期/金額/未確定轉換價。若未呈現，先評估以中性「公司事件背景」併卡，避免增加方向家族或重複通知；本輪不改程式。同步檢核來源有效日、接受/隔離列數、上市/上櫃覆蓋、after-close 自然 run 與成熟 outcomes。無新自然資料前不手動觸發 DAG、不發通知、不交易。


## 2026-10-03 11:03 台北：6907 雅特力營收延續、Q2獲利品質與財測來源分歧

### 先看官方事實

- 10/02 日線候選資料庫列 6907 score=3，三個方向家族為價格、法人流、月營收；近 5 日 +8.67%、相對 TPEx +5.49 個百分點，法人 4 個有效觀測日淨買 532,719 股。沒有量能家族，未匹配公司新聞；分數是家族數，不是成功機率。當日全市場日線仍 `ready/partial`，且此訊號尚無成熟 outcome。
- TPEx 官方月營收：2026/06、07、08 分別 NT$369.094M、402.798M、366.266M；單月 YoY 為 +113.56%、+198.45%、+198.72%，08 月 MoM -9.07%。2026/01–08 累計 NT$2,385.294M，年增 +115.32%；公司說明為「終端市場需求成長帶動營收大幅增加」，這是公司歸因，不是獨立驗證的終端需求或訂單。08 月資料列出表日 9/17；API 日戳與原始月份/實際申報時刻分開記錄。[TPEx 官方月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 證交所財報申報清單顯示 2026Q2 合併財報 2026/07/31 08:48:40 上傳、無更補正；報導期間截至 6/30。Q2 單季營收 NT$963.095M、年增約 102%；毛利 384.995M（40%）、營業利益 185.664M（營益率 19%）、稅後淨利 174.611M、基本 EPS 2.79（稀釋 EPS 2.76）。H1 累計營收 1,616.230M、毛利率 38.01%、營益率 15.78%、淨利 242.879M、基本 EPS 4.00；單季和累計口徑分開使用。[TWSE 電子財報查詢](https://doc.twse.com.tw/server-java/t57sb01?co_id=6907&colorchg=1&mtype=A&step=1&year=115)
- 獲利現金轉換有支持也有反證：H1 營業現金流入 NT$457.400M，約為淨利 1.88 倍；但現金流包含 NT$86.836M 員工股份基礎給付加回，以及關係人應付帳款增加 308.581M、合約負債增加 50.914M。存貨由 3/31 的 464M 升至 6/30 的 709.574M（季增約 52.9%，占總資產 28%），H1 現金流量表列存貨占用 323.078M。現金轉換比率不宜單獨當作持續性保證；需觀察後續庫存轉銷、應付帳款與現金流。
- TPEx 季營益率 API 記錄有效期 6/30、原始 `Date=1151002`；這個 API 出表/快照日不是財報首次公開日。證交所清單核對的實際上傳時間是 7/31。追蹤財報可得性時應保留「財報期間」「官方申報時間」「API 出表日」「本地收集時間」四種時間，不把 10/02 當成公司於當日首次揭露。[TPEx 季獲利資料 API](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)

### 公司訊息與研究來源分級

- 雅特力官網 9/10 稱加入 Arm Total Design for Physical AI；內容是生態系合作/技術整合方向，未披露新客戶、訂單或量化收入，僅作背景，不加多空家族。[公司官方消息](https://arterychip.com/en/news/260910_eng.jsp)
- 找不到可公開核驗的 6907 券商原始個股研究全文。元大 7/29 頁面報導的是智原 3035 外資觀點，日/美系研究員未具名；內文轉述 6907 MCU 銷售與母公司法說資訊，沒有 6907 專屬估值、營收/EPS 模型、分析師、資料截止日、修正歷程或反方敏感度，故不是 6907 原始研究。[元大 7/29 轉載](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202607290091542)
- 富聯網 4/24「今日焦點」文章（署名呂泰德）稱雅特力法說提出全年營收年增約 60%、Q2 為全年高點，並稱漲價前提前拉貨、全年營業費用可能年增 40–50%；這是二手摘要，未找到對應公司原始投影片/逐字稿，作者非已確認券商分析師，沒有個股估值、EPS 假設或上/下修紀錄。7/29 元大轉述智原外資報告時又寫「管理階層仍預估」6907 全年營收約年增 100%。兩個日期的數字不一致；可能是預期更新或轉述口徑不同，缺少一手原文，暫不能判定，亦不推測報告動機。[4/24 二手摘要](https://ww2.money-link.com.tw/realtimenews/NewsContent.aspx?PU=1011&SN=72507900)、[7/29 元大轉載](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202607290091542)
- 只作可檢驗試算：公司 2025 全年營收官網約 NT$1.67B；若 +60% 指引成立，全年約 2.672B，8 月累計 2.385B 後尚差約 287M；若 +100% 轉述成立，全年約 3.34B，尚差約 955M。這些基於四捨五入基期與二手指引，不能當成已核實財測。Q3 的 7–8 月營收合計約 769M；9 月若達約 194M，Q3 即超過 Q2 的 963M 季營收。後續用官方月營收檢驗，不替兩則摘要背書。[公司 2025 營收說明](https://www.arterytek.com/tw/news/260129_cht.jsp)

### 判讀與下一步

- 暫列「官方營收/獲利有實績支撐的三家族研究候選」，尚非已驗證的高品質可交易訊號。支持面是 6–8 月 YoY 連續成長、H1/Q2 獲利明顯改善及法人流；反證是 8 月 MoM 回落、存貨快速增加、日線來源 partial、上漲已延伸，以及 60%/100% 財測說法尚未找到一手原文。原始券商作者、報告日與截止日、估值方法、EPS/營收模型、修正、風險及預測期限均維持「未核」。
- 下個自然日線確認獲利率脈絡只標示 H1 累計或可正確還原 Q2 單季、保留原始官方連結；待 9 月月營收自然發布後記錄實際出表日並測試上述分歧說法。成熟 outcome、交易成本及公司行動校正仍不足，本輪不調分、不改門檻、不接未授權來源、不手動觸發 DAG、不送 Discord、不交易。

## 2026-10-03 11:22 台北：盤中公司事件卡補上非方向新聞背景

- 新增的唯讀盤點未改變日線與績效結論：10/02 日線仍 `ready/partial`、50 候選/0 通知；10/03 08:02 盤中 run 為 `ready/partial`、10 張公司事件卡，但價格/法人/營收/財務個股計數均為 0。Airflow 週末父 run 最終 failed；通知 ledger 的 `sent` 是內部狀態，沒有外部 Discord 回執。成熟 outcomes 仍僅有 9/24 訊號日 81 筆 signal-close 與 81 筆 next-close，一個交易日結果樣本，沒有 5/10-session 成熟樣本。
- 來源逐項對帳：10/03 TWSE 公司事件 70/70 接受/0 隔離、有效日 10/02（事件列不是母體覆蓋）；TWSE 月營收 1,086/1,086/0、有效月 2026/08，1,086 列包含 1,054 上市普通股、30 創新板普通股與 2 DR；TWSE 財測差異 7/7/0、有效期 2026Q2，是個案樣本。TPEx 公司事件及月營收本輪均 HTTP 530、0/0/0，最近有效日分別 10/01、2026/08（營收最近成功 10/02 22:45 的 892/892/0）。季獲利來源 TWSE 1,049 接受/5 隔離，TPEx 884/884；個股 TPEx 日行情仍因回應截斷失敗。成功 task 不等於上市櫃完整。
- 新聞庫在最新 10,000 個 ID 範圍、48 小時視窗中有 2,448 筆合格時間列；最近出版時間戳至 10/03 10:29。查到 8084 的中央社 10/02 11:27 與聯合報 11:52 同標題、段落近乎相同的盤勢稿，屬同一新聞敘事而非兩份獨立證據。報導把可能於 Q3 認列損失寫得較確定；TPEx 官方公司重大訊息仍是「符合會計認列條件時可能需認列」，確切損失與公司承擔額待結算/核閱，不採用媒體把條件句升格為事實的說法。[中央社稿](https://www.cna.com.tw/news/afe/202610020073.aspx)、[聯合報稿](https://udn.com/news/story/7251/9790322)、[TPEx 公司事件 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)。
- 程式缺口確認：每日掃描在寫入候選前呼叫 `_attach_recent_news_context`；盤中/週末掃描先寫入並早退，完全沒有新聞背景。已在盤中候選寫入前呼叫同一 helper，並把 `news_context_status` 放入 Airflow run payload。卡片使用既有「新聞背景（不計入多空訊號）」欄位；不改方向家族、計分、排序、候選門檻或通知頻率。短於三字的公司名稱仍需要代號鄰近匹配，巨虹（兩字）文章是否能自動關聯仍未解，避免為一家公司放寬全市場匹配造成誤命中。
- 對目標檔做 Python AST parse，成功；未跑測試、未手動觸發 DAG、未送 Discord、未交易。下一個自然盤中/週末事件候選檢查 `news_context_status` 與卡片中的來源、日期/時間精度、摘錄是否仍維持非方向背景；來源刷新及成熟 outcome 再變化前不調門檻或權重。

## 2026-10-03 11:50 台北：週末來源回看、8289 研究分級與 Airflow 血緣修補

### 最新快照與自然流程

- 10/02 20:02:35 日線仍是最新收盤掃描：`ready/partial`；目前普通股主檔 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、通知計數 0。這是資料列數，不是品質完整度；TPEx 官方 mapped 個股日行情失敗，不能把既有價格表資料說成兩市場官方來源都成功。
- 10/03 08:02:15 最新盤中／週末子 run `ready/partial`、10 張事件卡；價格、法人、營收、財務個股列均為 0。內部 `notified_count=10` 不代表 Discord 外部回執。其自然父 run 在 08:00–08:04:17 失敗，兩個 TPEx 官方來源（公司事件、月營收）HTTP 530、各 0/0/0；收集驗證 task 失敗。沒有手動重跑。
- 10/02 20:00 收盤父 run 在 20:00–20:04:37 失敗：11 個 mapped 來源中有 10 個成功、1 個 TPEx 個股行情來源失敗；市場訊號子 run 仍執行完成，但結果 `ready/partial`。TWSE mapped 行情 task 接受 1,051 列，最新有效日只有 10/01，對現行 1,054 檔上市普通股仍有 3 碼未覆蓋；TPEx mapped 行情為 0/0/0。10/02 舊價格表另有 1,938/1,946 列，兩種行情管線需分開報告。
- 週末 08:00 成功子來源逐筆核對：TWSE 公司事件 70/70/0、有效日 10/02（事件樣本，不是市場覆蓋率）；TWSE 月營收 1,086/1,086/0、有效月 2026/08，其中 1,054 上市普通股、30 創新板普通股、2 DR；TWSE 財測差異 7/7/0、有效期 2026Q2，是 5 家公司的個案樣本。TPEx 公司事件與營收本輪各 HTTP 530、0/0/0；前次成功資料分別為事件 42/42/0、有效日 10/01，以及月營收 892/892/0、有效月 2026/08。月營收 892 列對照現行上櫃普通股主檔 892 碼；事件列與財測個案不拿來當市場母體分母。
- 季財務自然來源 effective date 為 2026/06/30：TWSE 1,054 raw、1,049 接受、5 隔離；TPEx 884/884/0。相對現行上市普通股 1,054 碼，5 列仍隔離；相對上櫃普通股 892 碼，8 碼沒有接受列。缺列不補零。TWSE/TPEx 季營益率仍按累計口徑呈現，沒有相鄰累計基準時不換算單季。
- `market_signal_outcomes` 仍只有 2026/09/24 一個成熟訊號日：`signal_close`、`next_close` 各 81 列，各 80 列有方向標記；無成熟 5/10-session 樣本，亦未扣交易成本或完整調整公司行動。未用單日樣本改分數或門檻。

### 新聞時點及獨立性

- 10/03 中文新聞自然 run 實際在 08:30–08:39 台北時間完成；在最新 10,000 個新聞 ID、48 小時發布時間窗內，11:41 查到 4,190 筆，發布時間戳最新到 ETtoday 10:29、聯合報 10:28、中時 10:27、自由時報 10:21:06、中央社 10:07、鉅亨 10:00:04。The News Lens 8 筆及 iThome 20 筆的時間欄是日期粒度（00:00），不可當作精確發稿時刻。08:02 週末事件快照早於 08:30 新聞收集，不包含該批新資料；下個自然交易日 scan 再驗新聞欄位與來源時間精度。
- 仍將官方事實、媒體摘要、分析、未知分開；相同稿源轉載不當成獨立佐證。來源日期只有日、時間欄不明或發稿日在快照之後時，不倒灌到候選。

### 8289 泰藝：永豐公開內容不是個股原始研究

- 本輪核對永豐金證券「豐雲學堂」9/22 更新的石英產業百科文章，作者欄為「研究小編」。文章把 8289 列入 6 家台廠，聲稱 AI 光模組升級帶動產業，並概括訂單能見度提高、4 月營收普遍月增；它未列泰藝專屬分析師、資料截止日、估值法、EPS/營收模型、修正方向、反方風險、預測期間或可驗證的個股預測。平台免責亦表明不保證資料真實/完整。分類為券商平台產業教育內容，不能當成券商個股原始研究，也不足以評定個股研究品質；不推測作者/平台動機。[永豐文章](https://www.sinotrade.com.tw/richclub/industry/-6a1548f9b1fc50196daf60d5)
- 對照交易所原始資料：泰藝 2026/08 月營收 NT$214.038M、年增 67.15%、月減 1.48%、1–8 月累計年增 18.17%；公司申報說明稱客戶需求增加，但未給新增訂單或客戶拆分。TPEx 季營益率 5.34% 是截至 6/30 的上半年累計值；DB 沒有相鄰 Q1 累計基準，不能換算單季獲利率或 QoQ 趨勢。[TPEx 月營收 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)、[TPEx 季營益率 API](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 因此 8289 的官方營收有量化成長證據，但營收不等於獲利；文章所述產業訂單能見度沒有公司別量化驗證。沒有找到可公開核實的泰藝券商原始個股研究全文，作者、報告截止日、估值、EPS/營收假設、上/下修、風險及後續校準均維持未核。不升格為獨立高品質交易訊號。

### 資料時間及 lineage

- 時間戳查核排除了 8289 財務背景的 point-in-time bug：registry 以 UTC-naive 系統時間寫入（容器時區 UTC）；10/02 日線開始於 20:02:35 台北，即 12:02:35 UTC。泰藝 H1 季財務列的 `updated_at=2026-10-02 20:00:07 UTC`，對應 10/03 04:00 台北的自然來源 run，晚於 10/02 日線掃描；當時排除它是正確避免未來資料，歷史卡不回填。
- lineage 更正：10/02 20:00 after-close 舊 run 的 `source_runs.dag_id` 及 child `source_dag_id` 為 NULL，是 20:30 schema migration 前的歷史列。10/02 20:30 自然盤中輪已依先前修補填入 DAG ID 並驗證 DAG 範圍查詢；本次直接回查 10/03 08:00 週末 run，7 筆 source-run 列皆帶 `dag_id=source_extension_collect_weekend`，其訊號列也有 `source_dag_id` 與相同父 run ID。先前暫加的 context fallback 因沒有現行缺陷而撤回，程式維持既有修補；目標檔 AST 靜態解析通過。歷史 NULL 列不回填，下個自然 after-close 再驗證跨市場來源與候選狀態。
- 當沖資料仍不接入個股方向：TWSE TWTB4U/TWTB5U 契約不同；TPEx individual endpoint 空資料且授權未確認；OAS 11627 僅市場彙總並須 freshness gate；unmatched 絕不視為零量。

### 10/02 20:00 收盤來源逐項對帳補記

以下為該收盤 run 的 11 個 mapped 來源（同一失敗映射項在 `source_runs` 有兩筆 retry）；effective date 以接受列檢查。指數、事件、融券/借券可用額度及融資融券列是各自資料契約的樣本，不以列數冒充普通股行情覆蓋。

| 來源 | raw / 接受 / 隔離 | 最新有效日 | 母體與限制 |
|---|---:|---|---|
| TPEx 個股日行情 | 0 / 0 / 0，失敗 | 無 | 上櫃行情無本輪有效列；不能推成無成交 |
| TPEx 融資融券餘額 | 801 / 801 / 0 | 10/01 | 只代表回傳的可融資券證券，未證明 892 檔普通股全覆蓋 |
| TPEx 指數 | 2 / 2 / 0 | 10/02 | 兩條指數序列，不是上櫃股票覆蓋 |
| TPEx 借券賣出餘額 | 814 / 814 / 0 | 10/01 | 只代表來源回傳證券；缺列不是零量 |
| TWSE 個股日行情 | 1,051 / 1,051 / 0 | 10/01 | 現行上市普通股主檔 1,054 碼，尚差 3 碼且晚於資料日；不能稱 10/02 mapped 行情完整 |
| TWSE 融資融券餘額 | 1,039 / 994 / 45 | 10/02 | 45 列隔離；對 1,054 檔上市普通股名冊另有 15 碼未在 raw 回應，融資資格母體未核定，空列不補零 |
| TWSE 指數 | 1 / 1 / 0 | 10/01 | 指數序列，不是上市股票覆蓋 |
| TWSE 財測達成資料 | 8 / 0 / 8 | 無接受列 | 全部隔離，不輸入訊號 |
| TWSE/TPEx 可借券賣出額度 | 1,717 / 1,717 / 0 | 10/02 | 兩市場合併的額度列，非實際放空量，也不是完整普通股行情分母 |
| TWSE 注意交易 | 0 / 0 / 0 | 無 | 空集合不是股價覆蓋；不延伸成個股當沖量訊號 |
| TWSE 處置股票 | 8 / 8 / 0 | 10/02 | 監理事件清單，不是市場母體覆蓋 |

10/03 自然流程後，TPEx 注意交易與處置來源已列為 approved/enabled、排程 `daily_after_close`，但 `last_success_at` 仍為 NULL；它們未包含在 10/02 的 11 個 mapped task。下一自然收盤輪須分別核對有效日、接受/隔離列與上市/上櫃覆蓋，不以註冊成功代替資料收集成功。

## 2026-10-03 12:07 台北：3323 加百裕四家族候選、營收反證與券商內容分級

### 最新執行與新聞快照

- 12:07 唯讀回查，最新日線仍是 10/02 20:02:35：`ready/partial`，母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、通知 0。沒有較新日線收盤掃描。10/03 08:02:15 週末子 run 是 `ready/partial`、10 張事件卡，個股價格/法人/營收/財務列均為 0；不能拿它驗證日線資料或 Discord 回執。
- 10/02 20:00 after-close 父流程最近仍為失敗（來源清單詳見前節）；較新的 10/03 08:00–08:04:17 是週末來源父 run，也失敗，TPEx 公司事件與月營收各 0/0/0、HTTP 530。最近成功的中文新聞 Airflow run 為 10/03 08:30 logical run，實際 10:30–10:39 台北成功。按最新 10,000 個 ID 的 48 小時時間窗查得 4,056 筆時間合格列，最新發稿戳至 ETtoday 10:29、中時 10:27、聯合報 10:28、自由時報 10:21:06、中央社 10:07、鉅亨 10:00:04；TNL 8 列與 iThome 20 列僅有日期、資料欄落在 00:00，不能當精確發布時刻。這是滾動 ID 範圍的查詢樣本，不是全來源接受列數或固定分母；10:30 批次不會倒灌進 08:02 事件快照。
- 成熟 `market_signal_outcomes` 仍只有 2026/09/24 一個訊號日：`signal_close` 與 `next_close` 各 81 列、各 80 列有方向標記；尚無成熟 5/10 日樣本，也未扣交易成本。沒有足夠結果證據用來調整評分、權重或門檻。

### 3323 加百裕：價量與法人偏多，營收及獲利相反

- 10/02 `ready/partial` 候選保存 4 個方向家族、3 個偏多（價格、量能、法人）及 1 個偏空（月營收），排序 score=3。收盤 NT$30.35，近 5 日 +8.59%、相對 TPEx +5.40 個百分點，量能為 20 日均量 2.31 倍，並高於此前 20 日收盤高點 NT$29.70。近五日有資料的 4 日，法人合計淨買 557,956 股，其中外資 +560,719、投信 0、自營商 -2,763。家族數不是獨立因果或上漲機率；價格與成交量也彼此相關。
- TPEx 2026/08 月營收為 NT$281.339M，年減 46.98%、月減 27.62%、1–8 月累計年減 17.09%。6–8 月合計約 NT$987.396M，較去年同期約 NT$1,452.359M 減少 32.01%；單月與近季趨勢均未確認反轉。官方公司說明欄為「-」，目前沒有可量化訂單或客戶別催化。[TPEx 月營收原始資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- TPEx 公司重大訊息原文記載 2026/08/11 17:34:15 董事會通過 H1 合併財報：營收 NT$2,173.727M、毛利 NT$185.830M、營業損失 NT$118.000M、歸母淨損 NT$96.286M、EPS -NT$0.97。季獲利 API 的 2026Q2 欄是截至 6/30 累計營益率 -5.43%；目前只收到此期，不拿它當 Q2 單季 margin，也不算季比票。[TPEx 公司事件原始資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)、[TPEx 季營益率原始資料](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 官方 8/11 事件另披露對孫公司加百裕（南通）增加無擔保資金貸與 NT$96.93M、貸後餘額 NT$193.86M，借款方累積虧損 NT$192.787M，年利率 2.155%、一年期；同日保證資料記載對子公司保證餘額 NT$646.2M，占母公司淨值 27.02%。這是信用曝險背景，不單獨轉為方向票；數值的發生/公告日為 8/11，不能聲稱是目前餘額。[TPEx 公司事件原始資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- Point-in-time 核對：候選的每日快照在 10/02 20:02 台北觀察；加百裕 8 月營收來源列直到 10/02 22:45 台北才被本地來源觀測，H1 季營益率列則在 10/03 04:00 台北觀測。因此舊快照只存月營收 YoY -46.98%、沒有 `revenue_context` 或 `profitability_context` 是資料可見時點差，不回寫歷史卡；下個自然交易日檢查新候選是否加上金額、MoM/YTD 以及「H1 累計」-5.43% 的官方背景，而且不把累計率升格為季方向票。

### 券商研究與新聞分級

- 元大證券官網可查到 2026/03/24 10:15 的「盤中焦點股」頁面，引用時報盤中新聞稱加百裕「3月起營運可望回溫、今年目標轉虧為盈、外資連買」。這是盤勢/新聞摘要，沒有列個股研究分析師、報告資料截止日、估值方法、EPS/營收假設、修正方向、反方風險、預測期限或可量化失效條件；頁尾編輯署名不能當研究作者。這句話保留作當時市場說法，H1 實績仍虧損但全年尚未結束，不能只憑這些資料判定年度目標已落空。[元大盤中焦點原頁](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockTodayNews&id=202603240101558)
- 本輪搜尋元大、永豐、富邦與中國信託公開頁，未找到可公開核實的 3323 個股券商原始研究全文；因此作者、研究日/截止日、估值、EPS/營收模型、預估修訂、反方風險、預測期與事後命中率都維持未核。這不等於券商沒有內部或付費報告，也不推測發布動機。
- 快照當時候選的新聞脈絡為 `no_match_in_sample`。最新 10,000 ID／48 小時窗口中，沒有「加百裕」出現在標題或正文前 320 字的列；但中時 10/02 08:39 的 Q4 權證名單比較文在正文約第 1,012 字把「加百裕(3323)」列於減少名單。[中時新聞資料列](https://www.chinatimes.com/realtimenews/20261002001339-260410) 本輪未取得 115Q4 櫃買原始名單核對，故這仍是二手名單線索；可能涉及新發權證標的資格，不是公司營運催化，也未量化對 3323 的發行、避險或股價影響。新聞前 320 字匹配漏掉了正文表格，但目前不因此加方向票或改門檻；下一輪優先核對官方 115Q4 名單再決定是否作為制度性背景。

### 分級與下一個自然驗證

- 分類為「價量/法人動能偏多、營收及已公告 H1 獲利明確反向、來源屬 partial 的研究候選」，不是高品質可交易訊號。短線突破與法人買盤是可觀察支持項；8 月營收與 H1 本業虧損、子公司貸款/保證曝險是反證/風險；沒有公司量化催化、券商原始模型或成熟績效樣本。不升級通知、不改分數/權重/門檻、不做交易。
- 下一個自然交易日日線輪核對官方 TPEx 月營收來源是否刷新（effective month、raw/accepted/quarantined、對 892 檔上櫃普通股覆蓋）、H1 財務是否在候選可見時點被呈現為累計背景，並檢查新聞發稿時間、核對 TPEx 官方 115Q4 權證名單及 10/03 兩個 TPEx HTTP 530 來源是否自然恢復。仍遵守市場行情 TWSE/TPEx 覆蓋分開盤點，未匹配證券不補零。未手動觸發 DAG、未送 Discord、未交易、未跑測試。

## 2026-10-03 12:40 台北：自然流程重核與 3323 權證資格線索分級

### 最新快照、來源與績效樣本

- 唯讀查 market_signal_runs：最新日線仍是 10/02 20:02:35–20:02:42，ready/partial；母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、通知 0。最新盤中/週末事件子 run 是 10/03 08:02:15–08:02:16，ready/partial、10 張卡、通知 ledger 計數 10；價格、法人、營收、財務及母體列均為 0，ledger 不等於 Discord 外部回執。
- Airflow 唯讀列出 source_extension_collect_weekend 最近自然父 run：logical date 9/27 08:00 台北，實際 10/03 08:00:00–08:04:17，父層 failed。5 個 source key 中 TWSE 事件 70/70/0、有效日 10/02；TWSE 月營收 1,086/1,086/0、有效月 2026/08（1,054 上市普通股、30 創新板、2 DR）；TWSE 財測差異 7/7/0、有效期 2026/06/30、是個案樣本而非全市場名冊。TPEx 公司事件及月營收各兩次失敗，均 HTTP 530、0/0/0；上櫃月營收無本輪有效列，不能補零。有效日、接受/隔離數和證券母體分開呈現；事件/財測列不作上市櫃行情覆蓋率。
- source_extension_collect_after_close 最近自然 run 仍是 10/02 20:00–20:04:37 failed；上一節有該 run 11 個 mapped sources 的逐項對帳，舊歷史列的 DAG lineage 為 NULL，不拿相近時間的其他 DAG source_runs 冒充它。source_extension_collect_process 最近 run 是 10/03 04:00–04:04:21 failed；12 個不同 source key 成功、CBC 匯率兩次失敗（deadlock 後仍 JSONDecodeError）。TWSE/TPEx 季營益率資料期均為 2026/06/30：TWSE 1,054 raw、1,049 接受、5 隔離；TPEx 884/884/0，對 892 檔上櫃普通股名冊尚有 8 碼無接受列，缺列不補零。TWSE 股利 1,228/1,228/0 的最大有效日為 9/21；TPEx 股利 2,483/2,483/0 但最大有效日僅 2021/08/03，列數不代表當期覆蓋或新鮮度。
- market_signal_outcomes 仍只有 2026/09/24：signal_close、next_close 各 81 列，各 80 列有方向值，directional_hit=1 分別為 50 與 37；沒有成熟 5/10-session 樣本，尚未扣成本。10/03 08:30 logical 中文新聞 run 實際 10:30–10:39 成功；下一個 logical 10:30 台北 run 的三個 Airflow tasks 均成功（collect_article_url 12:30:02–12:30:32、collect_news 12:30:34–12:39:19、delete_and_insert 12:39:25–12:39:28）。process_execution_logs 記 success=1、12:39:24–12:39:28，處理前/後 9,202,262/9,202,562，新增 930、替換刪除 630（淨增 300），error_message 空白。唯讀 article_url 暫存有 9/12 個啟用來源回傳，共 997 個網址：ETtoday 200（最新 created_at 12:29）、聯合報 60（12:29）、鉅亨網 150（12:20:06）、中央社 100（12:13）、TheNewsLens 90（最新只到 10/03 日期、無時分），iThome 60、中時日報 199、公視新聞 43、自由時報 95（這四來源本批 created_at 皆空白）；Newtalk、三立新聞、TVBS 未出現在本批暫存。成功狀態不等於來源完整，來源未回傳的具體原因尚未核實；created_at 空白也不補造發稿時間。沿用 12:07 的 rolling 10,000 ID／48h 樣本數 4,056，不宣稱固定分母或全來源完整。

### 3323 加百裕：權證資格名單是制度背景，不是交易方向

- 櫃買 115Q2 官方 PDF（有效期間 5/18–6/30）列出 3323 在「A 級發行人可發行標的」附表；這只能證明該份 Q2 名單的資格類別。[TPEx 115Q2 官方名單 PDF](https://www.tpex.org.tw/storage/eb_data/11505/115030132811-1.pdf)
- 這裡的「A 級」是權證發行券商的評等，不是加百裕的公司評級；Q2/Q3/Q4 表二的意思是 A 級發行人可發行權證的額外標的名單。115/09/30 現行評等辦法以權證週轉率及造市品質指標評級，且容許 A 級發行人就另依上櫃市值新臺幣 30 億元等標準篩選的標的申請發行。故名單移除不能直接說成「3323 被評為 A 級/遭降級」；個股未列入的實際原因仍須官方公告或基準日數據證明。[TWSE/TPEx 權證發行人評等辦法](https://twse-regulation.twse.com.tw/m/LawContent.aspx?FID=FL074019)
- 115Q3 名單的二手來源是 MoneyDJ 對櫃買公告的轉載；115Q4 另找到 E.SUN 證券新聞頁，頁面標示 10/02 09:47:56，轉述櫃買於 10/01 公告、有效期 10/01–12/31，並在「115Q4 與 115Q3 修正版比較」的 A 級附表減少名單列出 3323。兩者都是二手轉載，尚無可核對的櫃買 Q3/Q4 原始公告；Q4 轉載沒有說明 3323 被移出的個別原因。未取得官方原件前，不升格為已核官方事實，也不推測是因財務、流動性或其他標準。[MoneyDJ Q3 轉載](https://www.moneydj.com/KMDJ/news/newsviewer.aspx?a=8faba704-dba3-4b0e-81e3-d9988a125486)、[E.SUN 證券 Q4 轉載](https://m.esunsec.com.tw/news/instant-detail.aspx?id=%7B70EF98AB-A15B-4B46-A4D2-918322C56F09%7D)
- 這不是券商個股研究：沒有作者/分析師、估值、EPS/營收模型、上/下修、個股風險評估或可驗證報酬預測。名單資格變化可作制度性觀察候選，但發行人可否發行權證不等於已有個股權證發行、避險流或股價方向證據；不計入訊號家族、不改通知門檻、不作買賣判斷。原因仍未知。
- 研究延伸只在取得官方 Q4/Q3 原始公告後進行：按公告時點凍結新增/移除狀態，再以符合條件且未異動的上櫃股票作比較組，觀察公告後 5/10 個交易日相對櫃買指數報酬與流動性變化，保留交易成本、公司行動和同日多事件控制。未形成多日期、多股票樣本前只記為可檢驗假說，不評價預測力。

本輪只更新追蹤文件與記憶；未改程式、未跑測試、未寫資料庫、未手動觸發/重跑 DAG、未發 Discord、未交易。

## 2026-10-03 13:19 台北：通知封包失敗重現、來源覆蓋回看與 3323 來源校正

### 訊號、流程與成熟樣本

- 唯讀快照仍為 10/02 收盤日線 `ready/partial`：universe 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、通知數 0。10/03 08:02 盤中/週末子 run 是 event-only、10 張卡；股票價格/法人/營收/財務列皆為 0，內部 ledger 10 筆不等於 Discord 回執。
- 10/02 after-close 父 run 20:00–20:04:37 台北失敗，11 個 mapped source tasks 有 10 個成功、TPEx 個股行情失敗。TWSE 行情本次 1,051 列接受、有效日 10/01；相對 1,054 檔上市普通股名冊尚有 3 碼無接受列。TPEx 行情 0/0/0。另有舊價格表 10/02 1,938/1,946 列，不能代替本次 TWSE/TPEx 官方行情來源覆蓋。
- 連動 daily child 的 `capture_market_signal_snapshot` 成功；`notify_market_signal_snapshot` 三次失敗，Discord 回 `40005 Request entity too large`。歷史單包 requests 相容 JSON 為 23,078 bytes；Airflow 記錄短的操作失敗通知已送達。對同一份 10/02 50 候選用目前程式做唯讀靜態重建（沒有 HTTP/Discord 請求），得到 8 embeds、3 batches，分別 9,276、8,818、6,413 bytes，均低於 10,000-byte 內部上限。這支持目前分批程式能縮小封包，但尚未證明外部自然投遞成功；待 10/05 自然收盤流程核對 Discord 回執與 ledger，不手動重跑。
- 最新 weekend 父 run 10/03 08:00–08:04:17 失敗。TWSE 公司事件 70/70 接受/0 隔離、有效日 10/02，是事件樣本；TWSE 月營收 1,086/1,086/0、有效月 2026/08，含 1,054 上市普通股、30 創新板與 2 DR；TWSE 財測差異 7/7/0、有效期 2026/06/30，是個案列。TPEx 公司事件、月營收各 HTTP 530、0/0/0；上櫃月營收前次成功 892/892/0、effective 2026/08，不能算成本輪新鮮覆蓋。以上成功數、事件個案與市場母體覆蓋分開。
- `market_signal_outcomes` 仍僅有 9/24 一個訊號日：signal-close/next-close 各 81 列、各 80 個方向值，hit 分別 50/37；沒有成熟 5/10-session 樣本，也未扣交易成本。樣本不支援調整方向門檻、權重或分數。

### 新聞與制度背景

- 中文新聞自然收集最近一次 10/03 12:39:24–12:39:28 台北成功，處理前/後 9,202,262/9,202,562，新增 930、替換刪除 630。article_url 暫存僅 9/12 個啟用來源有列，共 997 個網址；Newtalk、三立、TVBS 本批缺席，其他來源部分 `created_at` 空白或只有日期。最新 10,000 ID／48 小時樣本有 4,098 筆通過時間篩選；這是滾動樣本而非固定分母/來源完整度。新聞時間欄以資料庫觀測時間呈現時仍須標記原頁發稿時刻與時區未核，空白不補時間。
- 3323 加百裕：Q3 名單線索來自 MoneyDJ 轉載；Q4 另有 E.SUN 證券 10/02 09:47:56 頁面轉載，稱名單 10/01 生效並在與 Q3 比較表的 A 級額外標的減少名單列 3323。這是權證發行資格名單的二手來源，非個股評級、實際權證成交/造市流或股價方向；櫃買原始 Q4 公告仍未取得，移除原因未知，不納入訊號家族。

本次僅更新追蹤文件與記憶備忘；沒有程式變更、測試、資料庫寫入、手動 DAG、Discord 通知或交易。後續以 10/05 自然日線/收盤 run 驗證來源 effective date、接受/隔離與 TWSE/TPEx 覆蓋、通知自然投遞回執及新成熟樣本；若無新增可量化證據，不調訊號門檻。

## 2026-10-03 12:58 台北：4991 政策催化與券商模型後驗、新聞時點標示

### 目前訊號與資料完整度

- 沿用 12:40 唯讀核對的最新收盤資料：10/02 日線 `ready/partial`，universe 1,946、價格 1,938、法人 1,891、月營收 1,945、季財務 1,902、50 候選/0 通知；10/03 08:02 週末事件子 run 為 event-only、10 張卡，個股行情等家族均為 0。10/02 after-close 父 run 和 10/03 08:00 weekend 父 run 均 failed；後者 TPEx 公司事件/月營收為 HTTP 530、各 0/0/0。TWSE 月營收 1,086/1,086/0、effective 2026/08（1,054 上市普通股、30 創新板、2 DR）；TPEx 最近有效月同為 2026/08，前次成功 892/892/0，當輪失敗無接受列。這不是當前日線的全市場行情覆蓋證明。
- `market_signal_outcomes` 最近可核成熟資料仍只含 2026/09/24 一個訊號日：signal-close/next-close 各 81 列、各 80 個方向值；沒有成熟 5/10 session 樣本，也未扣交易成本。不同家族同向和單日 outcome 均不足以改分、權重或門檻。當沖仍維持既有界線：TWSE TWTB4U/TWTB5U 契約不等同；TPEx 個股 endpoint 空資料且授權未確認；OAS 11627 僅市場彙總並需 freshness gate；不得把 unmatched 證券當零量。

### 4991 環宇-KY：公司實績、政策原文及市場說法分開

- 10/02 候選是價格、量、法人、月營收四家族同向、score=4；近 5 日 +19.33%、相對 TPEx +16.15 個百分點、收 NT$534、量比 2.05 倍，近 5 日 4 個有效法人日累計淨買 1,873,682 股。score 是家族數，不是機率；全市場資料仍 partial，且該候選未有成熟 outcome。
- 公司一手實績：8 月美元營收 US$8.19M、YoY +37.2%；TPEx 月營收為 NT$262.258M、YoY +45.62%、MoM +80.31%、1–8 月累計 YoY +45.57%，金額和比較基期分開保留。[公司月營收](https://www.gcsincorp.com/investors/Monthly_Sales.php)、[TPEx 月營收原始資料](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)。公司 8/05 Q2 簡報列 Q2 單季營收 NT$770.103M、營業利益 NT$98.046M、營益率 12.7%、EPS NT$0.92；Q1 EPS NT$1.67、H1 EPS NT$2.58，H1 還含 Q1 處分投資利益 NT$163.760M。故營收強不代表單季獲利加速，H1 EPS 也不能全當經常性動能。[公司 Q2 法說頁與簡報](https://www.gcsincorp.com/investors/Investor_Conference.php)、[簡報 PDF](https://www.gcsincorp.com/pdf/Investor_PresentationQ2_2026.pdf)
- 政策原件：美國參議員 9/25 提出《Securing National Security Systems from Chinese Optical Transceivers Act》，範圍是聯邦政府國安系統採購的涵蓋光收發器，點名 InnoLight、Eoptolink 及關係企業/含其元件者；設五年轉換期與有限豁免。這是提案，不是已生效禁令、不是一般進口禁令，也沒有證明 4991 新增訂單/營收。[McCormick 參議員原始新聞稿](https://www.mccormick.senate.gov/news/press-releases/senators-mccormick-gallego-cornyn-fetterman-introduce-bill-to-keep-chinese-transceivers-out-of-u-s-national-security-systems/)
- 元大網站 10/02 11:43 的內容標為《時報》市場稿，稱矽光子族群受「美擬禁中國光模組」帶動，列 4991 等多檔漲停；它又引述外電對中國 3.2T 光模組的不同說法，沒有提供 FCC 原始規則，也不是元大投顧個股報告。故與 9/25 法案原文分列；3.2T 相關政策說法仍未核一手原文，不拿作公司別量化利多。[元大網站所載 10/02 市場稿](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockHotNews&id=202610020114352)

### 券商報告候選及可檢驗誤差

- 找到一份標示 2026/03/05 的元大 4991 更新報告，但可讀全文僅在 Scribd 第三方鏡像；未取得元大原始 PDF/報告頁元資料，作者、正式原始發布時間與風險頁完整性均未核。暫列「未驗證的二手研究線索」，不作一手券商證據，不推測報告發布動機。[Scribd 第三方鏡像](https://www.scribd.com/document/1007990004/%E5%85%83%E5%A4%A7-4991-%E7%92%B0%E5%AE%87-KY-20260305)
- 鏡像內容有可檢驗的模型細節：以 2026/03/04 收盤 NT$333.5 為基準，維持買進、12 個月目標價 NT$425；以預估 2H26–1H27 稀釋 EPS NT$8.5 × 50 倍本益比計價。鏡像列 2026/27 營收約 NT$3.095B/3.900B（較前版 2.910B/3.840B，上修約 6.4%/1.6%），EPS NT$5.22/10.20（較前版 5.09/10.02，上修約 2.5%/1.8%）。模型具明確假設和上修表，屬可做後驗的定量材料；但目標倍數偏高、模型依賴快速 H2 營益率爬升，鏡像未能確認作者/風險頁及敏感度分析。
- 對照 2026 Q2 公司簡報：Q2 實際營收 770M 高於鏡像預估 680M，但營業利益 98M 低於預估 178M，實際營益率 12.7% 對模型 26.1%。這是一季的收入高於預測、獲利率低於預測，不能外推全年，也不單憑結果推斷報告動機。H1 EPS 包含 Q1 投資處分利益，更需以本業後續季報檢驗預測。
- 來源搜尋仍未取得可核元大原始報告；在原件到手前不把鏡像評為正式高品質券商研究。後續量化檢驗需等 2026/27 實際營收、單季營益率及稀釋 EPS 逐季出表，保存預測版本/發布日和當時可得資料；若無報告風險頁，不補造反方論點。

### 通知時間欄窄幅改善及驗證

- `_attach_recent_news_context` 查詢的時間是 `news.created_at`。新增 context 會保存 `source_time_field`、欄位 basis 和精度；只保存日期或資料列時間為午夜時標成「僅日期」，有時分則顯示資料庫時間並明示時區與原文發稿時間未核。舊 payload 缺新欄位時仍標示發稿時分未保存。新聞只作非方向背景，不進家族計分。
- 對 `dags/source_extension/market_signals.py` 做 Python AST 靜態解析，成功；沒有跑測試或 DAG。此改動只改時間來源/精度呈現，不改訊號分數、方向條件、通知門檻或排序。下一自然日線驗證新 payload 與卡片的時間標籤，並再次檢查有效日、接受/隔離列、上市/上櫃覆蓋、after-close 子 run、新聞來源/實際發布頁及成熟 outcomes。未手動觸發/重跑 DAG、未寫資料庫、未發 Discord、未交易。

## 2026-10-03 13:28 台北：7810 捷創科技三家族候選、營運實績與券商摘要品質

### 訊號快照與新聞時序核對

- 最新可用 10/02 收盤 run 仍 `ready/partial`。7810 捷創科技（上櫃）列 3 家族，`family_directions={institutions:1,revenue:1,volume:1}`；price 未列入方向家族。候選底稿顯示近 5 日收盤報酬 +4.48%、相對 TPEx +1.29 個百分點、成交量為前期均量 2.59 倍、突破前 20 日收盤高點 NT$235、10/02 收盤 NT$245；8 月月營收 YoY +119.81%。法人表在 9/29、9/30、10/01、10/02 四個觀測日依序淨買 14,109、107,815、23,867、70,241 股，合計 216,032 股（不是 216,032 張）。
- 10/02 11:19 的《時報》二手盤勢稿報「本週迄今買超 146 張」；其頁面時間早於當日收盤。本地日資料 9/29–10/01 三筆合計 145,791 股，約 145.8 張，四捨五入恰為 146 張；再加 10/02 收盤後的 70,241 股，變成四日 216,032 股。這項時序核對顯示新聞中的籌碼數字與當時截止範圍相符，但不能把新聞看成獨立方向票。時報稿內的盤中高點 252.5 元也不同於本地當日收盤 245 元，卡片須區分盤中與收盤。
- 同一時報稿稱 10/02 盤中創近 4.5 個月高，並轉述管理層所說 Clamper 需求、自動化設備達年度目標約 80%、H2 / 2027 展望及服務/自有產品營收占比 65/35；這些是媒體轉述的公司說法，尚未在 8/20 公司法說簡報原文核到，暫列二手背景。公司簡報只可直接核到 Q2/H1 損益與資產負債表，另明載 5/31 與 FormFactor 簽約、在台提供設備安裝和硬體支援，未披露合約金額/量。時報稿內 H1 營益率先寫 26.42%、後寫 26.52%；公司官方表列 H1 營業利益 110,909 仟元/營收 418,152 仟元，計算約 26.52%，稿件前一數值有內部不一致。

### 一手實績與反證

- 捷創 8/20 官方 Q2 法說簡報與 8/05 董事會核准公告顯示，Q2 單季營收 NT$243.897M（去年同期 NT$124.465M）、毛利率 57.10%（47.55%）、營業利益 NT$74.764M（19.430M）、EPS NT$3.28（0.79）；H1 累計營收 NT$418.152M、毛利率 54.24%、營業利益 NT$110.909M、EPS NT$5.12，去年同期 EPS 2.19。單季與累計口徑分開保留。[公司 2026 Q2 法說簡報](https://www.jtron-tech.com/files/%E6%B3%95%E4%BA%BA%E8%AA%AA%E6%98%8E%E6%9C%83_20260820_TC.pdf)、[公司 Q2 財報公告](https://www.jtron-tech.com/zh-tw/news-53417/2026-08-05-%E5%85%AC%E5%91%8A%E6%9C%AC%E5%85%AC%E5%8F%B8%E8%91%A3%E4%BA%8B%E6%9C%83%E9%80%9A%E9%81%8E115%E5%B9%B4%E7%AC%AC%E4%BA%8C%E5%AD%A3%E8%B2%A1%E5%8B%99%E5%A0%B1%E5%91%8A.html)、[TWSE 電子財報上傳清單](https://doc.twse.com.tw/server-java/t57sb01?co_id=7810&colorchg=1&mtype=A&seamon=&step=1&year=115)。董事會通過 8/05，財報電子檔上傳 8/11 13:17:57，頁面標示無更補正。
- 公司 9/10 月營收公告：8 月 NT$77.273M，YoY +119.81%、MoM -7.86%；1–8 月累計 NT$579.288M、YoY +79.17%。8 個月累計已約為 2025 全年 NT$594.386M 的 97.5%。月增回落與低去年同期比較基期需保留，不把單月 YoY 當加速度。[公司 8 月營收公告](https://www.jtron-tech.com/zh-tw/news-53484/2026-09-10-%E6%8D%B7%E5%89%B5%E7%A7%91%E6%8A%80-7810-2026%E5%B9%B408%E6%9C%88%E5%8F%8A2026%E5%B9%B4%E7%B4%AF%E8%A8%88%E7%87%9F%E6%94%B6%E5%85%AC%E5%91%8A.html)
- 風險檢查：6/30 應收帳款（含關係人）NT$235.777M，YoY +154.5%；存貨 NT$64.527M，+138.9%，皆高於 H1 營收 +68.5%。同時現金及約當現金 +218.3%、總負債 +46.0%；僅憑期末存量不判定回款/庫存惡化，也不把現金增加直接歸因於營業現金流，後續需看現金流量表與應收/庫存轉換。公司簡報列前期金融資產減少 NT$249.9M，現金組成變化需與完整報表勾稽。

### 券商研究分級與可檢驗預測

- 只找到 CMoney 小編 2026/05/19 18:50 的二手彙整：稱當日永豐金給予看多、目標價 NT$320、預估 2026 年營收 NT$815M、EPS NT$9.36。未取得永豐原始報告；作者/分析師、報告資料截止日、目標價期限、估值方法、EPS與營收分部假設、上下修歷史、反方風險均未核，不推測報告動機。[CMoney 券商摘要](https://cmnews.com.tw/article/cmoneyaicurator-80e570a8-5370-11f1-81d0-f9e87fbabedb)
- 以摘要預測做後驗待驗，不作當前目標：8M 營收 579.288M 後，815M 預測尚需 235.712M，9–12 月平均約 58.928M；公司簡報列 2025 同期 9–12 月合計 271.075M，故預測只需低於去年同期約 13.1%，營收目標具可行性但尚未兌現。H1 EPS 5.12 後，全年 9.36 尚需 H2 4.24，即每季平均 2.12 元；Q2 單季實績 3.28 元可供比較，但不保證下半年能維持。按 10/02 收盤 245 元與舊摘要 320 元作算術差約 30.6%，因期限/估值法未核，不稱為現行合理上漲空間。
- 搜尋索引另出現 8/24 永豐目標價 290 元的 CMoney 社群頁，但頁面 403，未見券商原文，故不記成確認的降價/修正。下次取得原始研究時補作者、發布時刻、資料截點、估值/EPS橋接、風險及預測期限。時報 10/02 市場稿不是券商研究。

### 候選分級與下次驗證

- 暫列「三家族同向、公司 H1/Q2 獲利與 8M 營收有官方量化支撐的研究候選」，仍非已驗證高品質交易訊號：日線母體 `ready/partial`，10/02 TWSE mapped 行情有效日 10/01、TPEx mapped 行情 0/0/0；此候選無成熟 outcome，market_signal_outcomes 也只含 9/24 單日樣本。分數 3 只代表法人、營收、量能，不是機率；本輪不改權重/門檻。
- 10/05 自然收盤後核 7810 是否續留候選、9 月營收公告後檢驗上述摘要預測，並看 H2 季報的毛利/營益率、現金流、應收及庫存；確認 Discord 外部回執。未手動跑 DAG、未送通知、未交易。

## 2026-10-03 13:54 台北：3030 德律—H1 營運背景與點時資料核驗

### 最新訊號、來源完整度與成熟樣本

- 唯讀重查的最新日線仍是 10/02 20:02:35–20:02:42 台北，`ready/partial`：母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50、通知 0。10/03 08:02:15 週末事件快照 `ready/partial`、10 張卡，個股價格/法人/營收/財務與母體列皆為 0；ledger 計數不能當 Discord 外部回執。
- 10/02 after-close 自然父 run（logical 10/01 20:00 台北）實際 10/02 20:00–20:04:37 失敗；TWSE 行情 1,051/1,051/0、effective 10/01，對 1,054 檔上市普通股名冊有 3 碼沒有接受列；TPEx 行情 0/0/0。舊價格表 1,938 列不代表 mapped TWSE/TPEx 官方行情已覆蓋當日。10/03 weekend 自然父 run（logical 9/27 08:00 台北）實際 08:00–08:04:17 失敗：TWSE 事件 70/70/0、effective 10/02（個案樣本）；TWSE 月營收 1,086/1,086/0、effective 2026/08（1,054 上市普通股、30 創新板、2 DR）；TWSE 財測差異 7/7/0、effective 2026/06/30（個案樣本）；TPEx 公司事件和月營收均 HTTP 530、0/0/0。TPEx 月營收前次成功為 892/892/0、effective 2026/08，不能當成本輪新鮮列。
- `market_signal_outcomes` 仍只含 2026/09/24 一個訊號日：`signal_close` / `next_close` 各 81 列、各 80 列有方向值，hit 分別 50/37；無成熟 5/10 日窗，未扣交易成本或調整公司行動，樣本不足以改權重/門檻。
- 中文新聞最近自然 run `news_ch_collect_process` logical 10/03 10:30，實際 12:30:00–12:39:29 台北成功；處理前/後 9,202,262/9,202,562，新增 930、替換刪除 630。article_url 暫存 9/12 個啟用來源有回傳，共 997 個網址；Newtalk、三立、TVBS 缺席。其餘來源仍有 `created_at` 空白或只到日期；沿用 rolling 10,000 ID／48h 樣本 4,098，不宣稱固定分母或全來源完整。10/02 3030 候選 48h 抽樣 `no_match_in_sample`，不等於全網無新聞。

### 3030 德律：三家族候選與官方財務背景

- 10/02 20:02 候選為三個方向家族 `institutions/revenue/volume` 同向，排序 score=3；這不是機率。量能比 4.01，法人近 5 日有效 4 天合計淨買 1,427,941 股（外資 +1,092,712、自營商 +335,229、投信 0）；8 月營收年增 31.88%、月減 3.49%。卡片新聞樣本未匹配，該候選無成熟 outcome。
- TWSE OpenAPI `t187ap05_L` 的 2026/08 德律列：營收 NT$997.357M、YoY +31.88%、MoM -3.49%；1–8 月 NT$7,418.733M、YoY +27.42%，出表日期欄 2026/09/17。這是官方月營收，不將 8 月單月成長解讀成加速。公司 IR 的 2026 月營收頁目前標示 2026/05/01、只列至 4 月；該頁不是最新營收源，讀取時以 TWSE 官方表為準。
- TWSE OpenAPI `t187ap06_L_ci` 的 115Q2 列為截至 6/30 累計：營收 NT$5,387.967M、營業利益 NT$2,079.245M、歸母淨利 NT$1,738.510M、EPS NT$7.36；這是 H1 累計，不能標成 Q2 單季。`t187ap17_L` 對應 H1 累計營益率 38.59%，出表日期欄 2026/10/02。公司 9/07 15:28:21 台北的 MOPS 自結公告補充 8 月單月稅後淨利 NT$248.675M/EPS 1.05，1–8 月稅後淨利 NT$2,344.624M/EPS 9.93，並明示未經會計師查核或核閱。[TWSE 月營收 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap05_L)、[TWSE 綜合損益 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci)、[TWSE 營益率 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap17_L)、[TWSE 公司重大訊息 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)。
- 點時缺口追查：10/02 日線卡未帶 `profitability_context`，但這不是過濾程式缺陷。官方 H1 營益率 source record 的期間為 6/30、output date 欄為 10/02；registry row `updated_at=2026-10-02 20:00:13 UTC`（10/03 04:00:13 台北），而日線掃描是 10/02 20:02:35 台北（12:02:35 UTC），來源列實際晚約 7 小時 58 分才被收進資料庫。`_source_updated_at_cutoff_utc()` 依掃描時點排除未曾觀測資料，是正確的 point-in-time 行為；不回填舊卡、不改時間條件。下個自然日線核對 H1 累計營益率是否以非方向背景顯示；在缺相鄰單季資料時，不升格為獲利趨勢方向票。[訊號時間界線](../dags/source_extension/market_signals.py#L50)、[財務來源篩選](../dags/source_extension/market_signals.py#L924)

### 買方交易披露與券商報告分級

- 日月光投控代表矽品 9/07 17:31:32 台北披露，事實期間 2025/10/08–2026/09/07，向德律取得設備 72 批合計 NT$1,007.833M；另一則 9/03 16:45:58 台北披露 2025/09/17–2026/09/03 一批 NT$1,002.814M。公告稱雙方非關係人；設備用途是營業用途/生產營運。兩個事實期間重疊，不相加；披露的是歷史取得額，不是新簽前瞻訂單，沒有收入認列排程或產品別，不加方向家族。[TWSE 公司重大訊息 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)
- 元大、富邦、凱基/永豐及 Capital 的公開可索引頁面搜尋未取得 3030 券商個股研究原件。找到一份上傳在 Scribd 的「宏遠投顧」報告鏡像線索：檔名日期 2026/01/13，但作者、原始發布頁/時刻、資料截止日及全文完整性未核；鏡像文字同時出現目標價 215/220 元，表格列 2026E 營收 NT$10,545M、EPS 14.27、15 倍本益比，乘算約 NT$214，需原件確認。按 8M 官方營收，該鏡像預測若為原始模型，9–12 月還需 NT$3,126.267M（平均每月約 NT$781.6M）；8M 自結 EPS 9.93 到 14.27 還差 4.34 元。這些只是對未核鏡像的可檢驗算術門檻，不是已核券商觀點或估值結論；不推測報告動機。[Scribd 第三方鏡像線索](https://www.scribd.com/document/982397301/%E5%AE%8F%E9%81%A0%E6%8A%95%E9%A1%A7-2026%E5%B9%B41-2%E6%9C%88%E5%B0%81%E6%B8%AC-%E8%A8%AD%E5%82%99-%E4%B8%89%E4%BA%94%E6%97%8F-%E8%A8%98%E6%86%B6%E9%AB%94-%E7%9F%BD%E6%99%B6%E5%9C%93%E7%94%A2%E6%A5%AD%E8%A9%95%E8%AB%96%E8%88%87%E5%80%8B%E8%82%A1%E6%8E%A8%E8%96%A6-20260113)
- 買方設備公告可作未來研究的「外部客戶交易背景」候選來源；正式收成方向家族前，須先保留買方/賣方、事實期間、公告時點、累積/單筆性、產品別、期間重疊去重與後續營收認列，並用多公司、多期間對照 mature outcomes。現有兩筆重疊公告不足以作預測力或通知門檻證據。本輪不改分數、家族或門檻。

本輪未改程式、未跑測試、未寫資料庫、未手動觸發/重跑 DAG、未送 Discord、未交易。下一個平日自然收盤 run 再核通知外部回執、各市場行情有效日及覆蓋；後續等 9 月營收官方列與成熟多日 outcomes，再檢驗 3030 候選的延續性。
## 2026-10-03 14:09 台北：6461 長短期營收衝突與期交所大額部位時效核對

### 最新運作、覆蓋與結果樣本

- 唯讀重查的最新日線仍為 10/02 20:02:35–20:02:42 台北，`ready/partial`：母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、財務 1,902、候選 50、`notified_count=0`。10/03 08:02:15 事件快照 `ready/partial`、10 張卡，價格/法人/營收/財務/母體列皆 0；其通知 ledger 數不能當 Discord 外部回執。
- 自然 after-close 父 run 最新為 10/02 20:00–20:04:37 台北、failed：TWSE 行情 1,051 接受/0 隔離、effective 10/01，對 1,054 上市普通股名冊差 3 碼；TPEx 行情 0/0/0。週末父 run 最新為 10/03 08:00–08:04:17、failed：TWSE 公司事件 70/70/0、effective 10/02；TWSE 月營收 1,086/1,086/0、effective 2026/08（1,054 普通股+30 創新板+2 DR）；TPEx 公司事件/月營收 HTTP 530、0/0/0。另 `source_extension_collect_process` 的自然子 run 10/03 04:00 台北失敗，但 TPEx 季營益率 task 成功 884/884/0、effective 2026/06/30；相對 892 檔上櫃普通股票有 8 碼未匹配，非全市場覆蓋。
- 最新中文新聞 run logical 10/03 10:30、實際 12:30:00–12:39:29 台北成功；`news_ch.article_url` 暫存 9/12 個啟用來源、997 個網址。ETtoday 200、鉅亨 150、中央社 100、聯合 60 有時分；TheNewsLens 90 僅日期精度；iThome 60、中時 199、公視 43、自由 95 的網址列時間空白。Newtalk、三立、TVBS 本批缺席。收集任務成功不代表來源完整，也不把網址列時間當原頁發稿時刻。
- `market_signal_outcomes` 仍只有 9/24 一個訊號日：signal-close/next-close 各 81 列、各 80 列有方向結果；無成熟 5/10 日窗且未扣交易成本。未以單日績效調整其他家族門檻。

### 6461 益得：低基期單月暴增與基本面反證

- 10/02 保存候選為 4 個方向家族同向：法人、價格、營收、量能；近 5 日股價 +10.21%、成交量為前期均量 5 倍，4 個法人觀測日合計淨買 100,663 股。訊號卡的持久細節有 `revenue_context=null`、`profitability_context=null`；本候選近 48 小時新聞樣本未匹配，這不是全網無新聞證明。
- TPEx 官方月營收原始列（出表日期 2026/09/17）：8 月營收 NT$5.654M、YoY +1,102.98%；7 月 NT$0.211M、YoY -70.24%；6 月 NT$6.162M、YoY -91.26%。近三月合計 NT$12.027M，去年同期 NT$71.644M，約 -83.22%；1–8 月 NT$42.312M 對 NT$96.602M，-56.20%。因此 8 月巨大年增由 NT$0.470M 的低比較基數放大，不能單獨當作經營加速。[TPEx 月營收 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- 同一官方公告的發行人說明稱，8 月較去年同期增加主要因經銷商備貨增加；本年累計下降主要因去年認列海思科完成合約技術服務收入 NT$68.04M。這是公司申報說明，未證明終端需求或備貨是否延續，保留為來源事實而非推論。
- 益得 8/10 18:01:42 董事會通過 H1 財報的重大訊息列：1–6 月營收 NT$36.447M、毛損 NT$89.730M、營業損失 NT$204.658M、稅後損失 NT$214.559M、EPS -1.22。TPEx 季獲利率 API 列 H1 累計營益率 -561.52%；有效期間 6/30、出表日期欄 10/02，但 source row 在 10/03 04:00 台北才被觀測，晚於 10/02 20:02 日線，因此歷史候選不能回填這項背景。[TPEx 公司重大訊息 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)、[TPEx 季營益率 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 信號品質改善：已在 `market_signals.py` 加上語意一致性規則：只有當單月營收所形成的方向，同時與近三月及年初累計 YoY 方向相反時，暫停營收家族投票，並把衝突寫入風險/反證欄；缺少任一長週期比較值時維持原規則。它不改數值門檻、不改其他家族；按 6461 已收集數值，將不讓單月低基期反彈獨自提供營收方向票。10/02 舊卡不回算；下一個自然收盤 run 才能驗證新卡是否取得上述背景並呈現反證。本地容器對修改後檔案做 Python AST 靜態解析成功；未跑測試或重觸發 DAG。
- 用公開券商網頁搜尋 6461 時，找到的 Yuanta 頁是行情/新聞，KGI/Fubon 結果是報告索引或一般研究，沒有取得 6461 原始個股研究報告；因此不記分析師、目標價或假設，也不推測報告動機。

### TAIFEX 大額交易人資料：新鮮度改善、口徑仍不併入個股票

- 直接唯讀取得 TAIFEX OpenAPI 1,386 列，所有列資料日均為 2026/10/02；官方期貨大額交易人未平倉表也標示 10/02。TX `SettlementMonth=999912`、`TypeOfTraders=0` 列與表格 aggregate 完全對上：Top5/Top10 買方 73,291/80,841 口、賣方 57,034/78,636 口、全市場 OI 118,531。以 10/02 為最近交易日，此輪資料新鮮；它不是 1,386 檔股票的覆蓋率，上市/上櫃個股覆蓋不適用。[TAIFEX OpenAPI](https://openapi.taifex.com.tw/v1/OpenInterestOfLargeTradersFutures)、[官方表格](https://www.taifex.com.tw/cht/3/largeTraderFutQryTbl)、[欄位與計算說明](https://www.taifex.com.tw/cht/3/largeTraderFutQryDetail)
- 官方說明指出此表是各商品買方與賣方各自排名前 5/10 的持倉加總，括號僅計其中特定法人，且臺股期貨依小型/微型契約規模折算合併。買方 Top5 與賣方 Top5 是兩個分別排序的集合，不能相減宣稱市場淨多，也不能映射成單一股票方向。另 OpenAPI `ContractName` 寫 `TX+MTX/4`，官方表與說明寫 `TX+MTX/4+TMF/20`；數值和表格相符但契約標籤不完全一致，保留口徑疑點。暫只列為「有 freshness gate 的市場背景研究候選」，不接入方向家族或個股通知。

本輪修改僅為營收長短期衝突的方向票保護；未改其他門檻、資料庫、排程或通知投遞。未手動觸發/重跑 DAG、未送 Discord、未交易。下一自然日線核對 TPEx 報價/財務覆蓋與 6461 卡片營收脈絡、H1 虧損、衝突提示及 Discord 外部回執；若來源欄位缺失，先查 source_fields 的觀測時點，不回填歷史卡。

## 2026-10-03 14:27 台北：富邦 4991 報告線索與週末資料完整度複核

### 最新快照、結果樣本與自然收集

- 唯讀重查 `market_signal_runs`：最新日線仍是 10/02 20:02:35–20:02:42、`ready/partial`，universe 1,946、價格 1,938、法人 1,891、營收 1,945、季財務 1,902、候選 50、通知欄 0。最新盤中/週末 run 為 10/03 08:02:15–08:02:16、`ready/partial`、10 張事件卡；母體、價格、法人、營收、財務均 0。`notified_count=10` 是 ledger，不是外部 Discord 回執，也不是新個股行情快照。
- `market_signal_outcomes` 仍只含 9/24 一個訊號日：`signal_close`、`next_close` 各 81 列、各 80 列有方向值；命中計數分別 50、37，平均超額分別約 +1.7760、+0.9868 個百分點。沒有成熟 5/10 日窗，未扣成本、未調整公司行動，`signal_close` 亦非可成交入場價；不能拿這個單日樣本調權重或門檻。
- 10/02 20:00 台北 after-close 自然父 run 於 20:04:37 失敗：11 個 mapped ingestion 中 10 個成功、TPEx 個股行情 map index 9 失敗；TWSE 行情 1,051/1,051/0、有效日 10/01，對 1,054 檔上市普通股差 3 碼；TPEx 行情 0/0/0。trigger task success 不代表收集完整或外部通知送達。
- 10/03 08:00 台北 weekend run 於 08:04:17 失敗：TWSE 公司事件 70/70/0、effective 10/02（事件樣本，不是全股覆蓋）；TWSE 月營收 1,086/1,086/0、effective 2026/08（1,054 普通股、30 創新板、2 DR）；TWSE 財測差異 7/7/0、effective 2026/06/30。TPEx 公司事件/月營收 HTTP 530、0/0/0；其前次月營收成功 892/892/0、effective 2026/08，不算本輪新鮮資料。
- 10/03 04:00 台北 `source_extension_collect_process` 自然 run 於 04:04:21 失敗。TWSE 季營益率 1,054 raw、1,049 接受、5 隔離、effective 2026/06/30；TPEx 884/884/0、effective 同為 2026/06/30，對 892 檔上櫃普通股尚差 8 碼。TWSE 股利 1,228/1,228/0 的最大有效日為 9/21；TPEx 股利 2,483/2,483/0 但最大有效日僅 2021/08/03。列數或 task success 不代表資料新鮮或全市場覆蓋。Airflow task map 狀態可確認成功/失敗；`source_runs.dag_id` 為 NULL，部分 run id 也不一致，不以相近時間列冒充精確 DAG lineage。

### 新聞來源和原始發稿時刻

- 最新 `news_ch_collect_process` logical 10/03 10:30 台北，實際 12:30:02–12:39:28 成功。處理前/後 9,202,262/9,202,562，新增 930、刪除 630。`article_url` 暫存為 9/12 個啟用來源、997 條：ETtoday 200、鉅亨 150、中央社 100、聯合報 60 有時分；TheNewsLens 90 僅日期；iThome 60、中時 199、公視 43、自由 95 的網址列時間空白；Newtalk、三立、TVBS 本批缺席。網址收集時間/日期不是已核原頁發稿時刻，不宣稱來源完整。個別新聞的時刻需回到發稿機關/媒體原頁核對，本輪沒有用新聞摘要補強方向訊號。

### 富邦投顧 4991 環宇-KY 原始報告候選

- 富邦投顧官方索引可核一份 2026/09/08 台股報告存在，標題為「光收發模組帶動的成長爆發」，索引顯示參考價 NT$494.00，並註明全文須登入會員。[富邦投顧官方報告索引](https://fubonresearch.fubon.com/Research/Home/Index)
- 本輪只核到官方索引的日期、標題與索引參考價；作者、資料截止日、目標價、估值方法、EPS/營收假設、前次預測上修/下修、風險與可檢驗預測均未取得。第三方轉述數字不當作富邦原文，也不推測報告動機。現階段可評為「一手來源存在、正文未核」，不是可量化券商訊號；不以 4991 之後股價表現替代預測後驗。

本輪只讀重查、更新追蹤文件；未改程式/門檻、未寫資料庫、未手動觸發/重跑 DAG、未送 Discord、未交易、未跑測試。下一個平日 10/05 自然日線/收盤後再核新的有效日、raw/接受/隔離、TWSE/TPEx 覆蓋、TPEx 個股報價、通知外部回執、新聞來源與原頁時刻，以及是否新增成熟 outcomes；券商正文若仍無公開原件，維持未核分級。

## 2026-10-03 14:38 台北：8084 交易方式限制漏報與官方事件分類修補

### 官方事件與時間口徑

- 唯讀檢查 10/03 08:02 週末事件 run 保存的 10 張卡：8084 巨虹已有一張官方 TPEx 記者會公告風險卡。原文稱私募未收足而失效；子公司與 Midas 股權交易條件未成就，US$30M 訂金可能被沒收，若適用條件成立才可能認列損失，並可能導致季末淨值為負及終止上櫃。公司同時表示最終損失金額、認列時點及 8084 實際承擔比例尚未確定；公司在問答中列出對宏康持股 0.1%，故不能把 US$30M 直接記為 8084 損失，也不能把可能退市寫成確定事件。[TPEx 公司重大訊息 API](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 同一發行人另有一筆官方 TPEx 公告（本地保存 source record 1399540）：主旨稱櫃買通知變更交易方法，說明列自 2026/10/02 起券商須先收足款券。此為即時交易/交割限制，與尚待認列的財務損失是不同事實。TPEx 的變更交易資訊頁是核對實施狀態的官方查詢入口。[TPEx 變更交易資訊](https://www.tpex.org.tw/zh-tw/mainboard/trading/info/altered.html)
- 時間欄保留原樣，不選一個時間冒充發布時刻：上述兩筆 API row 的 `Date` 欄為 1151001（2026/10/01）；兩筆 `發言日期/時間` 分別是 1150930 22:54:35 和 1150930 23:04:12；記者會原文另載財務業務資訊說明會日期時間為 115/09/30 19:30；交易方式公告的實施日則是 115/10/02。這些欄位涵義不同，沒有獨立核得 MOPS 原頁實際發布時間。現在瀏覽器對 TPEx 明細頁回 403、worker 容器對官方 API DNS 查詢失敗，故本輪依本地先前成功收集的官方 API 原文與保存列判讀，不宣稱做過即時頁面核驗。

### 漏報原因與窄幅修補

- 已存在的 8084 記者會候選不會自動代表交易方式公告。以 source record 1399540 原文跑現行 `_classify_company_event`，回傳 `materiality=review`、`公告待判讀`；公司公告候選只保存/推播 `materiality=key`，因此明確的「變更交易方法＋先收足款券」限制沒有成為事件卡。
- 已在 `dags/source_extension/market_signals.py` 的公告分類加一條窄規則：公告內文同時明示變更交易方法/方式及先收足款券/全額交割時，列為 `交易方式限制風險`；公告若明示恢復普通/一般交易、交割或原方法則不觸發。卡片提示核對實施日與 TPEx 當前狀態，並說明這是交割/流動性限制，不等於已終止上櫃或股價方向預測。沒有增加方向家族、分數權重或價格門檻。
- Airflow worker 對檔案做 Python AST 解析成功；使用保存的 8084 原始公告做單次函式核對後，該公告分類為 `key`，附加「恢復普通交易」的情境仍留在 `review`。未跑測試套件、未寫 DB、未觸發/重跑 DAG、未送 Discord、未交易。10/05 自然事件/收盤掃描驗證是否出現獨立交易限制卡、是否保留時間欄不確定性，以及與既有同公司財務風險卡是否造成重複推播。

## 2026-10-03 14:52 台北：2301 併購公告誤判修補與收集完整度更新

### 最新資料狀態

- 唯讀重查後，最新收盤快照仍為 10/02 `ready/partial`：universe 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、50 候選、通知欄 0。10/03 08:02 週末事件快照是 `ready/partial`、10 張事件卡，個股行情/法人/營收/財務及母體列均為 0；通知 ledger 不代表 Discord 外部回執。
- 自然 after-close 父 run 10/02 20:00–20:04:37 台北失敗：TWSE mapped 行情 1,051 raw/1,051 接受/0 隔離、effective 10/01，對 1,054 檔上市普通股名冊差 3 碼；TPEx 行情 0/0/0。週末父 run 10/03 08:00–08:04:17 失敗：TWSE 公司事件 70/70/0、effective 10/02（事件列非全股母體）；TWSE 月營收 1,086/1,086/0、effective 2026/08（1,054 普通股、30 創新板、2 DR）；TWSE 財測差異 7/7/0、effective 2026/06/30。TPEx 公司事件/月營收 HTTP 530，皆 0/0/0；月營收前次成功 892/892/0、effective 2026/08，不能算作本輪新鮮資料。
- `source_extension_collect_process` 10/03 04:00 自然 run 04:04:21 失敗。TWSE 季營益率 1,054 raw/1,049 接受/5 隔離、effective 2026/06/30；TPEx 884/884/0、effective 同日，與 892 檔上櫃普通股名冊差 8 碼。TWSE 股利 1,228/1,228/0 的最大有效日為 9/21；TPEx 股利 2,483/2,483/0 但最大有效日為 2021/08/03，列數多不代表資料新鮮。
- `market_signal_outcomes` 仍只有 2026/09/24 一個訊號日：signal-close/next-close 各 81 列，各 80 列有方向值，directional hit 分別 50/37；沒有成熟 5/10-session 樣本，且未扣交易成本或調整公司行動。樣本不足以調分或改門檻。
- 最新中文新聞 `news_ch_collect_process` logical 10/03 12:30 台北，實際 14:30:00–14:39:27 成功；執行記錄 14:39:20–14:39:26，文章庫處理前/後 9,202,562/9,202,857，新增 1,000、替換刪除 705。`article_url` 暫存 10/12 個啟用來源、1,065 個網址：ETtoday 250、iThome 60、Newtalk 20、TheNewsLens 90、中央社 100、中時 192、公視 43、聯合 60、自由 100、鉅亨 150；三立與 TVBS 本批未出現。ETtoday、Newtalk、中央社、聯合、鉅亨列有時分；TheNewsLens 僅日期；iThome、中時、公視、自由時間欄空白。網址列時間不等同原頁發稿時刻，仍需逐篇回官方/媒體原頁核對。

### 2301 光寶科：官方併購公告的分類誤判與證據界線

- 從本地唯讀 `source_records` 讀取 TWSE 官方重大訊息列 1650668（source URL `https://openapi.twse.com.tw/v1/opendata/t187ap04_L`）：發言日期/時間 2026/10/02 16:05:48、effective date 10/02，本地 observed_at 10/03 08:00:08。這是已保存的官方來源列；本輪瀏覽器不能直接開啟 OpenAPI endpoint，故不宣稱剛完成外部即時重抓。[TWSE 公司重大訊息 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)
- 公告事實：光寶全資子公司 Leotek 與其全資子公司 Leotek Charging 以反三角合併取得 AmpUp 100% 股權，AmpUp 存續並成為 Leotek 子公司；交易對價由 Leotek 發行 275,631 股普通股，另保留最多 24,369 股供繼受之管理階層選擇權。換股約為每 1 股 Leotek 換 191.09 股 AmpUp；獨立專家列 163.14–203.63 股區間，AmpUp 用收益法、Leotek 用收益法及市場法估值。最終價金仍待交割前依協議調整，預估 2026 年底完成；取得之 Leotek 股份有 2 年轉售限制，另有不超過 US$6M 向部分 AmpUp 原股東買股的安排。
- 公司公告自述短期每股淨值/EPS 可能稀釋、長期應有正面效益；後者仍是管理層預期，不是實現值。獨立專家換股比率區間支持價格合理性的一個檢查，不等於商業綜效或母公司 EPS 預測。因發股者是子公司 Leotek，公告未提供足以量化的 Leotek 既有股本/稀釋率、AmpUp 可併入的營收獲利或母公司 EPS 橋接，不能把子公司新股數直接換成 2301 稀釋率。後續看交割完成、AmpUp 財務貢獻及併購後合併報表。
- 同日 source record 1650664 是另一筆光寶董事會核准的高雄擴產資本支出，公告金額 NT$8.85B，理由為因應營收成長並購置設備/廠房機電工程；交易對象及合約金額確定時才另行公告。現有原文沒有工程排程、資金來源、增加產能量或可驗證的增量營收/獲利，因此保留為一手、量化、方向中性的擴產候選，分類仍為 `review`；不單憑公告金額把它升成偏多交易訊號。

### 分類修補及窄幅驗證

- 原分類器把 `下市` 當一般風險子字串；M&A 公告的制式問項「下市(櫃)」因此觸發 `偏空風險`，即使同一內文明確說「本次交易並無解散、終止上市」及「對本公司既有營運及全體股東權益無重大不利影響」。
- 在 `dags/source_extension/market_signals.py` 加入窄條件：僅當目前命中風險詞只有 `下市`、內文同時含下市(櫃)問項、併購/收購、以新股作價、明確否認終止上市及「無重大不利影響」時，標為 `併購交易｜稀釋與效益待驗`、`中性事件`。卡片說明短期稀釋可能、長期效益待驗與母公司影響未知；若另有重大損失等風險詞，仍落回原偏空分類，不被此例外遮蔽。
- Airflow worker 以 source record 1650668 原文直接執行函式，結果為 `key/中性事件`；在同一公告加上「認列重大損失」作反例仍為 `偏空風險`；8084 source record 1399540 仍分類為 `交易方式限制風險`。本地 Python AST 解析成功。沒有跑測試套件、資料庫寫入、手動 DAG、Discord 通知或交易。

下一個平日 10/05 依自然收盤/事件 run 核對此中性併購卡是否出現、與 2301 其他事件是否有重複，以及新日線 effective date、接受/隔離、上市/上櫃覆蓋、新聞來源和原始發稿時間、通知外部回執及成熟 outcomes。當沖仍不把 TWSE TWTB4U/TWTB5U 視為同一契約；TPEx 個股 endpoint 空列/授權未確認、OAS 11627 僅市場彙總且需 freshness gate；unmatched 證券列不得視為零量。沒有新多日可檢驗證據前不調交易門檻。

## 2026-10-03 15:01 台北：2301 擴產決議與既有資本支出指引對照

### 重新核對的 runtime

- 截至 15:01 台北時間仍無新日線：最新 `daily` run 為 10/02 20:02:35–20:02:42、`ready/partial`（universe 1,946、價格 1,938、法人流 1,891、營收 1,945、季財務 1,902、50 候選/0 通知）。10/03 08:02 事件快照 `ready/partial`、10 張卡，個股行情/法人/營收/財務列均 0。outcomes 仍僅 9/24 的 signal-close/next-close 各 81 列、各 80 有方向值（hit 50/37），無成熟 5/10 日樣本，未扣成本。
- 自然 10/02 after-close run 父 DAG 仍 failed，實際 20:00–20:04:37；其 market-signal child trigger 是 Airflow success，但 TWSE mapped 行情 1,051 raw/1,051 接受/0 隔離、effective 10/01，對 1,054 上市普通股名冊差 3 碼；TPEx mapped 行情 0/0/0 且該 map 失敗。child run success 和通知 ledger 不等於外部送達回執。
- 10/03 08:00 weekend 父 DAG 仍 failed：TWSE 公司事件 70/70/0、effective 10/02（非股票全體覆蓋）；月營收 1,086/1,086/0、effective 2026/08；財測差異 7/7/0、effective 2026/06/30。TPEx 公司事件和月營收 map 均 HTTP 530、0/0/0；通知 child trigger success 不能抹去兩個來源缺口。
- 10/03 04:00 `source_extension_collect_process` 父 DAG failed，mapped index 12 `cbc_fx_daily` 因 JSONDecodeError 0 列失敗，其他 12 個 map 最終 success（部分曾出現 DB deadlock 後完成 retry）。同批 TWSE 季營益率 1,054 raw/1,049 接受/5 隔離、effective 2026/06/30；TPEx 884/884/0、effective 同日，相對 892 檔上櫃普通股差 8 碼。TWSE 股利列 1,228、最大有效日 9/21；TPEx 2,483 列但最大有效日 2021/08/03。任務綠燈與列數不能替代有效日和市場覆蓋。
- 最新 `news_ch_collect_process` 仍是 10/03 logical 12:30、實際 14:30:00–14:39:27 台北成功；logs 記新增 1,000、刪除 705、庫存 9,202,562→9,202,857。`article_url` 為 10/12 啟用來源、1,065 URL；ETtoday 250、iThome 60、Newtalk 20、TheNewsLens 90、中央社 100、中時 192、公視 43、聯合 60、自由 100、鉅亨 150，三立及 TVBS 缺列。四來源時間欄空白、TheNewsLens 僅日期；其他列有時分仍不代表逐篇原頁發稿時刻已核。

### 2301 高雄擴產是否構成新的投資訊號

- 10/02 官方董事會重大訊息列 1650664 載高雄分公司擴產資本支出 NT$8.85B，設備及廠房機電工程對象/合約金額確定後再公告。這是董事會通過的具體投資決議，不是已簽供應商合約，也未給出排程、資金來源、新增產能、投產時點或增量毛利/EPS。
- 後續對照 LITEON 官方 2026/07/31 Q2 營運公告：公司當時已把 2026 年全年資本支出上修至 NT$18B；Q2 營收 NT$52.7B、營益率 15.6%、EPS NT$3.14，H1 營收 NT$96.1B、營益率 12.8%、EPS NT$4.80。9/09 公布的 8 月營收為 NT$19.592B，1–8 月 NT$134.710B、YoY +27%。[LITEON 2026 Q2 官方公告](https://www.liteon.com/zh-cn/news/press-center/content/quarterly-second-eps-2026)、[LITEON 2026 年 8 月營收](https://www.liteon.com/zh-tw/news/press-center/content/liteon-monthly-sales-aug-2026)
- NT$8.85B 約為 NT$18B 年度指引的 49.2%，亦約為 1–8 月營收的 6.6%；這只是規模對照，資本支出不可直接當成當期損益。10/02 公告未說明 8.85B 是否已包含於 7 月的 18B 指引，因此不能把它再加總成新增 capex，也不能稱為指引上修。最合理分級仍是「一手、量化、方向中性的投資執行事件」；對產能/獲利的可檢驗預測尚缺，維持 `review`，不增加多空家族或通知門檻。
- 本輪以券商官方可索引頁面查詢 2301 原始研究，沒有取得具名分析師、報告日期/資料截止日、估值法、EPS/營收假設及風險頁完整的個股報告。元大網站可索引的部分內容為《時報》記者稿/新聞，不能當元大投顧原始研究；其他公開搜尋未能核到 2301 券商原件。故不推測報告動機，也不以新聞稿中管理層的公司展望冒充券商預測。

本次僅補做唯讀 runtime/一手資料對照並更新追蹤文件；沒有新增程式變更、調分、資料庫寫入、手動 DAG、Discord 通知或交易。10/05 自然 run 後再驗證 2301 中性併購事件分類、通知外部回執及新增行情/結果樣本。

## 2026-10-03 15:09 台北：舊 2301 偏空卡的去重鍵修正

- 再查 10/03 08:02 event snapshot，source record 1650668 的 candidate id 1400、舊 hash `560419e2…` 已被保存為 `company_announcement / 偏空風險 / 偏空`。`market_signal_notifications` 序列化 payload 也含標題「2301 光寶科｜偏空風險」，對應列為 `sent`、時間 08:02:19；同一批通知 payload 會按多個候選去重鍵各存一列。這只能證明 notifier ledger 記為 sent/payload 已記錄，不是外部 Discord 回執證明；此舊分類發生於本輪規則修正前。
- 發現只修分類仍不夠：既有 intraday fingerprint 只由公告來源列與事實欄位組成；去重在重新分類前就會看到舊 `sent` key 並跳過，因此 10/05 自然掃描仍可能沿用舊分類且不送更正卡。
- 已在 `_load_intraday_candidates` 將公司事件分類移到去重檢查前；對唯一的修正類別 `併購交易｜稀釋與效益待驗`，把穩定分類結果（materiality/label/direction）加入 fingerprint。故 2301 的新中性卡產生另一個 dedupe key，而其他既有公司事件維持舊 fingerprint，避免全面重播已送公告。舊錯誤卡不會因此撤回；新更正卡需等待自然 scan/notifier，不手動投遞。
- 驗證：本地 AST 解析成功。Airflow worker 唯讀呼叫 `_load_intraday_candidates`，source record 1650668 得 `key/中性事件`，新 dedupe key 目前沒有 ledger 列，舊 key 仍為 `sent`；8084 record 1399540 仍為 `交易方式限制風險`，candidate hash 與舊語意輸入相同。未寫資料庫、未觸發/重跑 DAG、未呼叫 notifier/Discord、未交易。

後續 10/05 自然 scan 核對是否出現修正後的 2301 中性卡、是否只有一次新 dedupe 投遞、以及實際平台 receipt 是否可取得；不能把 Airflow success 或 `sent` ledger 視為 Discord 已讀/送達證明。

## 2026-10-03 15:31 台北：4991 四家族候選、TPEx 監理來源待首收集與日線通知失敗追查

### 最新掃描、來源完整度與通知

- 唯讀查 `market_signal_runs`：最新日線為 10/02 20:02:35–20:02:42 台北，`ready/partial`，universe 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、候選 50、`notified_count=0`。最新盤中為 10/03 08:02:15–08:02:16，`ready/partial`、10 張公司事件卡；母體、價格、法人、營收、財務都是 0，不能當作盤中行情掃描。
- 10/02 after-close 的 mapped TWSE 個股報價 1,051 raw/1,051 接受/0 隔離、effective 10/01，對 1,054 上市普通股少 3 碼；TPEx 報價 map 失敗，0/0/0。父 run verifier 失敗，trigger task success 不證明來源完整或通知送達。10/03 weekend 的 TWSE 公司事件 70/70/0 effective 10/02、月營收 1,086/1,086/0 effective 2026/08、財測差異 7/7/0 effective 6/30；TPEx 事件與月營收 map 都失敗，0/0/0。來源成功狀態不能替代有效日和市場覆蓋。
- outcomes 仍只有 9/24 一個訊號日：signal-close/next-close 各 81 列、80 列有方向值，hit 分別 50/37；無成熟 5/10 日結果，未扣交易成本或調整公司行動。4991 的 10/02 新候選尚無任何 outcome；不以單一截面調分或門檻。
- 追查同一 10/02 日線 child：`capture_market_signal_snapshot` 成功，`notify_market_signal_snapshot` 三次失敗，Discord 回 `40005 / Request entity too large`；候選摘要未完成投遞、`notified_count=0`。失敗批次內確實含 4991；另有短流程錯誤通知 ledger 記 `sent`，僅代表內部 notifier 回報成功，沒有獨立平台 receipt。本地現行 renderer 對保存的失敗 payload 做唯讀重建，將 9 embeds/23,078-byte 單包分成 3 批，估算 8,361、7,456、7,593 bytes，每批 3 embeds；未送出任何重試。這沿用追蹤清單較早的修補，待 10/05 自然收盤流程才可確認實際投遞。
- 最新新聞 run 仍為 10/03 logical 12:30、實際約 14:30–14:39 台北 success；暫存 10/12 個啟用來源有 10 個回傳、1,065 個網址，三立/TVBS缺席；多個來源時間欄空白或僅日期精度，網址時間仍不等於原頁發稿時刻。10/03 15:31 前沒有更新的新聞批次。

### 4991 環宇-KY：有支持，但只列觀察候選

- 10/02 日線候選為 score 4、4 個方向家族（法人、價格、營收、量能），無反向家族：5 日報酬 +19.33%、TPEx 指數同期 +3.18%、相對 +16.15 個百分點，收盤 534 元並高於前 20 日收盤高點 530 元，量能擴張比 2.05。法人 5 日淨買超約 187 萬股；這些都是同一段行情的共振，不是上漲機率。短期已急漲，且日線資料狀態 partial，先列「強動能、待後續驗證」，不作交易建議。
- 一手官方基本面支持：TPEx 月營收 OpenAPI 的 2026/08 列 YoY +45.62%，7 月 -6.93%、6 月 +55.06%，月增速不是連續加速；8 月列在 10/02 14:45 才首次被本地觀測。TPEx 8/31 公司公告列 Q2 合併營收 7.70 億元、YoY +57.14%，EPS 0.92 元（前期 0.13 元）；公告註明 Q2 數字經會計師查核，7 月自結數則未經查核/核閱。TPEx `t187ap17` 6/30 營益率 18.87% 是累計期間口徑，於 10/02 20:00 才觀測到；它沒有出現在本候選的 directional family 列表，沒有把累計率冒充單季訊號。[TPEx 月營收 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)、[TPEx 公司重大訊息 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)、[TPEx 季營益率 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 候選 `news_context` 只收 ETtoday 與鉅亨 10/02 兩個「美傳擬限制中國光模組」媒體標題，發稿欄僅有日期；原頁時分和政策一手文件未核。因此將其保留為未核的媒體/產業背景，不列官方催化事實。富邦投顧官方 10/02 盤後評論把光通訊列為當日強勢族群，是市場廣度背景，不能證明 4991 個股展望或政策傳聞屬實。[富邦投顧首頁/盤後評論](https://fubonresearch.fubon.com/Research/Home/)
- 富邦投顧官方報告索引曾列 9/08 4991 報告標題「光收發模組帶動的成長爆發」及參考價 NT$494，全文需會員登入。作者、資料截止日、估值方法、EPS/營收預測、修正歷史、風險與可檢驗預測均無法從索引核實；參考價不是目標價，報告品質暫不可評。[富邦投顧報告索引](https://fubonresearch.fubon.com/Research/Home/Index)
- TPEx 官方歷史處置查詢顯示 4991 今年曾有 7/30–8/14、8/14–8/20、9/04–9/10 的處置期間，最後一段已於 9/10 結束。這是歷史交易風險，不等於 10/02 當日仍在處置。[TPEx 官方處置資訊](https://www.tpex.org.tw/www/zh-tw/bulletin/disposal?code=4991&endDate=2026%2F10%2F29&id=&order=date&response=html&startDate=2026%2F07%2F01&type=code)

### TPEx 官方監理 OpenAPI 首次自然收集待驗

- `source_registry` 有 `tpex_trading_attention_daily`、`tpex_trading_disposition_daily` 兩個官方 OpenAPI 來源，均 `approved/enabled`、priority 97.75、`daily_after_close`，但 `last_success_at=NULL`、沒有 `source_runs`。新來源在 10/02 after-close run 後才註冊；目前 `discover_approved_source_keys('daily_after_close')` 已回 13 個來源。10/03 weekend 使用 intraday hint，故不包含這兩個 daily-after-close 來源。對同一官方 API 做一次無認證唯讀 GET 的 Airflow worker 連線測試回 `ConnectionError`，不是一次 source run，不能宣稱 endpoint 可用或資料為空。
- 現有日線 renderer 已讀取 `trading_attention_notice` / `trading_disposition` 並作風險背景；下個平日自然 after-close 要核 API 是否成功、有效期/公告期、raw/接受/隔離與實際 4991 列。未首收集前，空的 `regulatory_context` 不能解讀為沒有公告。歷史個股網站查詢只作研究核對，不回填訊號，也不取代官方授權 OpenAPI。

本輪新增的是候選品質與通知/來源狀態稽核，未改程式、分數或門檻；只追加追蹤文件及授權的 memory ad-hoc note。未重跑 DAG、未送通知、未交易、未跑測試。下一檢查點為 10/05 自然 after-close：確認新 TPEx 監理/報價來源的 effective_date、列數及市場覆蓋、4991 候選是否持續、daily digest 真實投遞，以及後續成熟 outcome。

## 2026-10-03 16:00 台北：6174 可轉債規模脈絡補入多家族候選卡

### 最新自然資料與來源完整度

- 重新讀取 `market_signal_runs`：最新日線仍是 10/02 20:02:35–20:02:42 台北，`ready/partial`；母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、50 候選、通知欄 0。最新 10/03 08:02 盤中快照是 `ready/partial`、10 張公司事件卡，母體/行情/法人/營收/財務列均 0，不是盤中行情掃描。
- 由 Airflow 只讀列出自然 after-close DAG：最新 run `scheduled__2026-10-01T12:00:00+00:00`（台北 10/02 20:00）仍 `failed`，20:04:37 結束；mapped 0–8、10 success，map 9 failed，`verify_collection_and_triggers` failed；`trigger_market_signal_notify` success 只代表觸發子 run。TWSE 日線行情 map 1,051 raw/1,051 接受/0 隔離、effective 10/01，對 1,054 檔上市普通股少 3；TPEx 行情 map 0/0/0、失敗，對 892 檔上櫃普通股沒有有效收集。父 DAG /子 trigger 綠燈不能代替資料完整與外部通知回執。
- 10/03 weekend source run：TWSE 公司事件 70/70/0、effective 10/02（事件列不等於股票母體）；TWSE 月營收 1,086/1,086/0、effective 2026/08（含 1,054 上市普通股、30 創新板、2 DR）；TWSE 財測差異 7/7/0、effective 6/30。TPEx 公司事件及月營收本輪皆 0/0/0、HTTP 530。10/03 04:00 process run 父 DAG failed：CBC FX JSON decode 失敗；TWSE 季營益率 1,054 raw/1,049 接受/5 隔離，TPEx 884/884/0；兩者 effective 6/30，TPEx 對 892 檔普通股少 8。`market_signal_outcomes` 仍只有 9/24 一個訊號日，signal-close/next-close 各 81 列、80 列有方向（命中 50/37）；沒有成熟 5/10-session 窗，未扣成本或調整公司行動，不能用這一日調門檻。
- 最新 `news_ch_collect_process` logical 10/03 12:30、實際 14:30:00–14:39:27 台北成功，仍是此刻最新；10/12 啟用來源有 10 個回傳、1,065 個網址，三立及 TVBS 缺列。多來源原文發稿時分缺值或僅日期精度，資料庫網址時間不是原頁發稿時間；沒有以本批新聞補強方向分數。

### 6174 安碁：四家族動能卡漏掉一筆大額官方融資脈絡

- 10/02 日線候選 6174 score 4、法人/價格/營收/量能四家族同向；5 日報酬 +23.46%、相對 TPEx +20.27 個百分點、8 月營收年增 +32.84%、量能擴張比 2.43。新聞只配到 ETtoday 被動元件族群標題，並非安碁個別催化；短線漲幅已大且行情母體 partial，維持觀察候選，不解讀成上漲機率。
- 本地唯讀 `source_records` 的 TPEx 官方重大訊息列 1399462：公告欄日期 9/30、`event_time` 11:54:44（來源的發言時間欄，非本輪獨立核實的網頁上線時刻）、首次本地觀測 10/02 14:00，載明安碁第三次無擔保可轉債應募款已收足 NT$306M。較早 8/11 官方董事會列載債券面額 NT$300M、以面額 102% 發行、3 年、0% 票息、用途為營運資金及償還銀行借款；當時轉換價格/方法待後續核准後議定。另份同日 H1 官方財報公告列報導期間截至 2026/06/30，期末歸屬母公司權益 NT$668.365M。收款金額/該期末帳面權益 = 45.8%；這是融資規模對帳面權益的比率，不是可轉股比例、稀釋率、營收或獲利。收款公告本身沒有轉換價/可換股數；本輪未核得後續發行辦法原件，不估算稀釋。瀏覽器直接開官方 OpenAPI 本輪回 403；此處依賴先前自然收集的官方來源原列，不稱為本輪即時重抓。[TPEx 公司重大訊息 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O)
- 現行 `_classify_company_event` 將 9/30 收款列分成 `review/公告待判讀`；`_load_intraday_candidates` 僅推 `key`，所以這項官方財務背景不會成為獨立事件卡。10/02 日線候選 `details_text` 也沒有官方融資欄位。富邦/元大等券商官網搜尋沒有找到可核 6174 的完整原始個股研究；永豐豐雲學堂有「研究小編」產業編輯文，並非具名分析師原始研究。作者/截止日/估值法/EPS與營收假設/修正/反方風險均無原件可核，因此不作券商品質或動機判斷。

### 脈絡卡修補與驗證

- 在 `dags/source_extension/market_signals.py` 加上只讀 point-in-time 脈絡查詢：限 TWSE/TPEx 官方重大訊息；只看近 45 日「可轉債應募款全數收足」公告；配對公告當時之前最新一份官方財報列報導期間、並以來源更新截止時刻避免前視。只有收款金額至少達先前已披露母公司權益 20% 才附到已通過原有三家族方向門檻的日線候選。卡片另列「官方融資脈絡」，明示 NT$306M、6/30權益 NT$668.365M、45.8%，且明示不計入多空家族與「20%是脈絡篩選門檻、非方向/預測門檻」。不改分數、方向門檻或 candidate hash；若公告未提供轉換價/換股數，不推算稀釋率。
- Airflow worker 對整檔 Python AST 解析成功；以已保存的 6174 官方來源列唯讀呼叫新 helper，得到 306M、6/30 母公司權益 668.365M、45.8%；將 context 注入已保存候選後呼叫純 formatter，卡片欄位長 307 字，四家族方向仍完全相同；以 9/15 公告日前視點呼叫時回空 context，確認不把 9/30 收款洩漏到較早日期。未跑測試套件、未寫 DB、未手動觸發/重跑 DAG、未送 Discord、未交易。

下一個平日 10/05 自然日線掃描再看：6174 若仍符合原方向家族，卡片是否呈現融資脈絡且不改分數；確認收盤 effective date、接受/隔離、上市/上櫃覆蓋、after-close mapped source 數、TPEx 監理來源首次自然收集、日線通知外部回執、新聞逐篇原頁發布時刻及是否出現成熟多日 outcomes。研究原件未公開時仍標未核；當沖個股資料未確認官方授權前不納入訊號。

## 2026-10-03 16:19 台北：安碁三轉換條款待官方核實、日線通知拆批待自然驗證

### 例行快照、收集與通知狀態

- 只讀重查 `market_signal_runs`：最新日線仍是 10/02 20:02:35–20:02:42 台北、`ready/partial`，universe 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、50 候選、通知欄 0。最新盤中列仍是 10/03 08:02:15–08:02:16、`ready/partial`；母體、價格、法人、營收、財務皆 0，只有 10 張公司事件卡，不能視為個股行情掃描。內部通知計數不等於獨立 Discord 收件回執。
- 最新 after-close 父 run 仍是 logical `scheduled__2026-10-01T12:00:00+00:00`（台北 10/02 20:00），20:04:37 失敗；TWSE 日線行情 1,051 raw/1,051 接受/0 隔離、effective 10/01，對 1,054 檔上市普通股差 3 碼；TPEx 行情 0/0/0。對應 `market_signal_notify` daily child 的 `capture_market_signal_snapshot` 成功，`notify_market_signal_snapshot` 失敗，三次重試均回 Discord `40005`（`Request entity too large`，其中一次回應說明欄位診斷失敗）。當日 digest `notified_count=0`；通知 ledger 有一筆摘要 `failed`，另有一筆短錯誤通知內部記 `sent`，未取得平台端獨立 receipt。
- 以目前本地純 formatter 對同一 10/02 保存資料重建，取 10 名候選共 8 embeds，按現行 `_split_discord_embed_batches` 為 3 批：9,276、8,818、6,413 request bytes；三批均低於程式內 10,000-byte 預算。這只驗證本機序列化/分批結果，沒有 HTTP 呼叫，不能算外部成功。10/02 的失敗發生在拆批版投入自然流程前；待 10/05 自然收盤 run 驗證，不能手動重跑或重送。較早文件中「9 embeds、8,361/7,456/7,593 bytes」是較舊 renderer 重建，後續追蹤以當前 8 embeds/三批數字為準。
- `market_signal_outcomes` 仍只有 9/24 訊號日，signal-close 與 next-close 各 81 列、各 80 列有方向值；命中 50/37。無成熟 5/10-session 窗，未扣交易成本/公司行動，不足以調門檻。
- 最新中文新聞 DAG logical 10/03 12:30、實際 14:30:00–14:39:27 台北成功；啟用來源 12 個中 10 個回傳、1,065 個網址，三立/TVBS未見。部分時間欄僅日期或空白；不能把資料庫網址時間當作原頁發稿時分。10/03 weekend source latest TWSE 公司事件為 70/70/0、effective 10/02（事件樣本，非全股覆蓋）；TPEx 公司事件失敗 0/0/0，不把它解讀成無公告。

### 6174 安碁三：發行條款外部索引有線索、轉換價未核成一手事實

- 本地自然收集的 TPEx 官方重大訊息列 1399462 載 9/30 已收足安碁三應募款 NT$306M；官方 8/11 董事會列載面額 NT$300M、按 102% 發行、3 年、0% 票息及資金用途為充實營運資金/償還銀行借款。收款額相對 6/30 母公司權益 NT$668.365M 為 45.8%，仍只作融資規模脈絡，不等於換股股數或稀釋率，也不計入方向家族。
- TPEx「最近上櫃轉(交)換公司債」官方頁提供發行機構篩選及 CSV 下載，但此頁以動態資料表載入；本輪沒有取得 61743/安碁三的官方列。官方頁/腳本透過瀏覽工具回 403，桌面瀏覽器無法解析網域；因此這是資料路徑失敗，不是官方查詢回傳 0 列。後續要從官方掛牌公告、公開說明書或同一官方 CSV 核對價格與掛牌日。[TPEx 最近上櫃轉換公司債查詢](https://www.tpex.org.tw/zh-tw/bond/issue/cbond/listed.html)
- Vocus 一篇 9/29 發文列安碁三 10/02 預定發行、面額 3 億、102 元發行、3 年、0% 票息，並稱初始轉換價 NT$46.4；作者同段註明其截至 9/28 尚未查到櫃買中心正式掛牌公告，並要求再核官方公告。面額、發行價、年期、票息與先前官方公告一致，但不能因此推定 NT$46.4 已有一手佐證；本輪仍標「未核原件」，不進訊號/稀釋估算。[Vocus 二手整理（未核轉換價）](https://vocus.cc/article/6ab9e6e8fd897800018a121c)
- 另一本地 TPEx 公司事件列 623671 的公告內文稱安碁受邀參加兆豐證券 10/07 14:30 法說會。這是未來資訊揭露日程，不是當前營運催化；待公司簡報/法說原始資料公開後再記錄作者、資料截止日、估值法、EPS/營收假設、修正及反方風險。另搜尋仍未找到 6174 具名分析師的券商原始個股報告；第三方 EPS/本益比模型網站自述無法準確估算，且非券商原件，不把其模型當成市場共識或方向信號。[安碁投資人關係](https://www.aker.com.tw/investors)

本輪只讀檢查資料庫、Airflow 狀態與日誌，並以純 formatter 重建 payload；沒有程式碼修改、測試套件、資料庫寫入、手動 DAG、Discord 通知或交易。下一檢查點為 10/05 自然批次：按來源記錄 effective date、raw/接受/隔離、上市/上櫃覆蓋；確認日線通知拆批是否自然送達及平台回執；追蹤 TPEx 報價/事件失敗、10/07 法說材料及 61743 官方發行原件。無多日成熟樣本前不調訊號門檻。

## 2026-10-03 16:31 台北：3030 9 月營收二手報導與官方收集期別不一致

### 例行狀態重看

- 週六 16:31 台北，尚無 10/05 收盤自然 run。最新日線仍為 10/02 `ready/partial`：universe 1,946、價格 1,938、法人流 1,891、月營收 1,945、財務 1,902、候選 50、通知計數 0；最新盤中 10/03 08:02 為事件卡掃描，候選 10、行情/法人/營收/財務列皆 0。`notified_count`/內部 ledger 不是獨立 Discord 收件回執。
- outcomes 仍只有 9/24 一個訊號日：`signal_close`/`next_close` 各 81 列、各 80 列有方向值，命中數 50/37；尚無成熟 5/10 日窗，未扣成本或調整公司行動，不因單日樣本調門檻。
- after-close 最新仍是 logical `scheduled__2026-10-01T12:00:00+00:00`、實際台北 10/02 20:00–20:04:37，父 run failed；TWSE 報價 map 1,051/1,051/0、effective 10/01，對 1,054 上市普通股差 3 碼；TPEx 報價 0/0/0。週末最新月營收收集：TWSE success 1,086 raw/1,086 accepted/0 rejected，資料期別最新 2026/08；TPEx failed 0/0/0（HTTP 530）。綠燈不代表截至該批的期別已是最新。
- 最近完成新聞 run 為 logical 10/03 12:30、實際台北 14:30–14:39 success；截至 16:31，logical 10/03 14:30 run 已於台北 16:30 開始、仍 running，完成後才可更新來源數、文章數與時間欄。前批 10/12 啟用來源有 10 個回傳、1,065 URL，SETN/TVBS 缺列；有些發布時間僅日期或空白，URL/資料庫時間不代替逐篇原頁時分。

### 3030 德律：8 月官方列與 9 月媒體摘要須分開

- 10/03 台北 16:25 左右直接讀取 TWSE `t187ap05_L` 官方 OpenAPI，德律最新列仍是 2026/08：營收 NT$997.357M、YoY +31.88%、MoM -3.49%、1–8 月 NT$7,418.733M、YoY +27.42%，出表日期 9/17。10/03 08:00 台北本地 `twse_monthly_revenue` 收集雖成功，1,086 raw/1,086 accepted/0 rejected，並沒有把德律期別推進 9 月。[TWSE 月營收 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap05_L)
- MoneyDJ 10/01 16:34 報導 9 月營收 NT$868.016M、YoY +54.02%、1–9 月 YoY +29.76%，並轉述伺服器/網通/半導體需求解釋；這是二手新聞，未在本輪取得對應 MOPS 原始申報頁。當前 MOPS 頁面可進入，但該個別查詢請求遭網站安全頁阻擋；TWSE OpenAPI 仍列 8 月。故把數字及來源落差記為「待一手核實」，不回填 10/02 候選、不把新聞摘要當已核官方數字。[MoneyDJ 9 月營收報導](https://www.moneydj.com/kmdj/news/newsviewer.aspx?a=b44a3d78-94fd-44a2-a36b-284a4302456f)、[MOPS 官方查詢入口](https://emops.twse.com.tw/server-java/t58query)
- 德律官網 2026 月營收頁標示 5/01，表格明列 1–4 月且後續月份未填，較 TWSE OpenAPI 更落後，不可當最新資料源。[德律 IR 月營收頁](https://www.tri.com.tw/en/investor/investor_detail-86-2-3--2031-1.html)
- 10/02 候選仍依可用官方 8 月列：score 3、法人/營收/量能三個方向家族；1 個月成長是正值，但 MoM 為負、相鄰月 YoY 從 7 月 +38.19% 降至 8 月 +31.88%，不是連續加速。先前候選及關聯公告已在 13:54 章節列過；本輪不重寫方向，也不假設 9 月媒體數字已被程式讀取。
- 券商官網公開可索引搜尋仍沒有取得含具名分析師、資料截止日、估值法、EPS/營收假設、修正歷史及反方風險的德律原始個股研究。元大頁可見法說內容/市場新聞整理，並非可評分的完整模型報告；搜尋不到公開原件不代表券商內部或會員區不存在，也不推測報告動機。

結論與待驗證：這是一個來源期別/發布時間的完整性警訊，而非已確認的 9 月基本面訊號。10/05 自然收集時核對 TWSE 月營收 effective period、raw/接受/隔離、上市/上櫃覆蓋及德律列；如 OpenAPI 仍是 8 月，再由可核的一手 MOPS/公司原件確認 9 月申報時刻和數字後，評估是否需要新增官方月營收來源。沒有來源核實與跨批穩定證據前不改程式、訊號門檻或通知。

本輪唯讀查詢官方 OpenAPI、本地來源列/收集紀錄、Airflow run list，並查閱券商及公司公開頁；沒有寫 DB、手動觸發/重跑 DAG、送 Discord、交易或跑測試。

## 2026-10-03 16:54 台北：2340 營收動能須與 H1 虧損及合作對手新產能併讀

### 自然掃描、資料覆蓋與新聞批次

- 週六仍沒有新的收盤日線掃描。最新 daily 為 10/02 20:02:35 台北，ready/partial，母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、財務 1,902、候選 50、內部通知計數 0。最新 intraday 為 10/03 08:02:15，只有 10 張公司事件卡；母體、價格、法人流、營收、財務列均為 0，不是盤中行情掃描。成熟 outcomes 仍只有 9/24：signal_close/next_close 各 81 列、各 80 列有方向值，命中 50/37，無成熟 5/10 日窗且未扣成本或公司行動。
- after-close 最新仍是 logical scheduled__2026-10-01T12:00:00+00:00，10/02 台北 20:00–20:04:37 父 run failed。TWSE 報價 source run success 1,051 raw/1,051 accepted/0 rejected、effective 10/01；對 1,054 檔上市普通股少 3 碼。TPEx 個股報價 failed 0/0/0；這是上櫃覆蓋失敗，不能當作 0 成交或無訊號。週末最新月營收：TWSE 10/03 08:00 success 1,086/1,086/0，期別最新 2026/08、僅上市來源；TPEx 10/03 08:02 failed 0/0/0（HTTP 530），沒有當批上櫃覆蓋。source task 綠燈不代表期別最新或上市櫃完整。
- 最新中文新聞自然 run logical 10/03 14:30、實際 16:30:01–16:40:49 台北，三個 Airflow task success。實際資料表 article_url 有 1,130 個網址、10 個來源；news_temp 有 1,071 篇、9 個來源，最後新增 1,071 列並重寫舊文 761 列。公視有 43 個候選網址但沒有解析成一篇可寫入文章；自由時報 98 個網址中收進 83 篇、鉅亨網 150 中收進 149 篇；TVBS 清單 URL 回 404，SETN 無候選網址。故 12 個啟用來源裡只有 9 個形成可寫入文章的來源。news_temp 有 151/1,071 列時間落在 00:00:00，包含 iThome/The News Lens 等日期精度欄位，不能據此排序盤中先後；The News Lens 最早列到 2026/01/20。網址數、有效文章數、日期精度及原頁逐篇發布時間分開判斷。

### 2340 台亞：三家族觀察候選，反向基本面 context 不可漏

- 保存候選 id 1349（as-of 10/02 20:02:35）score 3，方向偏多、三個方向家族為法人/營收/量能；這是同一截面的共振，非上漲機率。法人近五日只有 4 個觀察日，4/4 淨買，合計 +24,061,033 股（外資 +22,568,041、券商自營 +1,492,992）；量能擴張比 5.87。8 月營收 NT$474.269M，YoY +21.87%、MoM +20.28%，月 YoY 對 7 月 +2.18% 加速 +19.70 個百分點；但 1–8 月累計 YoY 僅 +4.82%，6–8 月合計 YoY 約 +13.56%，所以不是單月百分比就代表全年轉強。[TWSE 月營收 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap05_L)
- 官方 8/07 第二季財務公告列 2026 H1 營收 NT$2,163.511M、毛損 NT$40.972M、營業損失 NT$479.107M、稅前損失 NT$423.267M、淨損 NT$462.667M、母公司業主淨損 NT$408.499M、EPS -0.93；TWSE 累計營益率列為 -22.14%。營收變好與 H1 大幅虧損必須同卡並陳，不應把累計營益率誤作單季值，也不把它硬加成一個新的方向家族。[TWSE 公司重大訊息 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap04_L)、[TWSE 季營益率 OpenAPI](https://openapi.twse.com.tw/v1/opendata/t187ap17_L)
- t187ap17 這筆 H1 row 本地首次觀測為 10/02 20:00:13 UTC（台北 10/03 04:00），晚於 10/02 20:02 台北候選掃描；不得回填改寫舊候選。8/07 的官方財務公告已包含同一 H1 虧損資料，下一次自然候選檢查應確認卡片是否能以財務脈絡呈現，而不是只顯示營收加速。
- 另一個反向 context：10/02 融資餘額由 9,336 降至 9,061 張，融券餘額由 314 升至 449 張，可借券餘額較前日增加 26.57%；目前不把這些部位快照當獨立方向家族。候選 news_context.status=no_match_in_sample 只表示限定 48 小時樣本沒有匹配列，不證明沒有新聞。
- 官方 9/23 公司公告稱董事會通過認購 JC Nexus Fund L.P. 私募股權基金「不超過」US$6M、非關係人、長期投資用途；不是已付出全額的證明。公告第 21 項原欄位文字為「會計師出具非合理性意見：是」；本輪沒有取得會計師完整意見書，因此保留原詞，不延伸解讀成保留意見或交易異常。台亞另公告 10/14 14:20 舉辦財務業務座談會，待公開簡報/原始材料再核實。

### 台亞集團 PASC 與 RIR：已證實合作，財務歸屬及新產能影響仍未知

- PASC 官網稱公司於 2022 Q1 由台亞集團投資成立；RIR 2025/06/05 官方新聞稿稱其 1200V SiC diode 在台灣由 PASC contract fab 生產並出貨，系列為 2–60A，技術 IP 來自 Sicamore，6 吋擴產有 PASC/Vortex 技術支援。RIR 也稱已取得 Richardson Electronics 與 Ankit Plastics 訂單，但那是 RIR 所述客戶訂單，不能當作台亞/PASC 新增訂單或可量化營收/權利金。[PASC 官網](https://www.pascsemi.com/)、[RIR 2025 官方合作稿](https://www.rirpowersemi.com/news/rir-power-electronics-expands-manufacturing-of-1200v-silicon-carbide-sic-diodes-in-collaboration-with-pro-asia-semiconductor-corporation-taiwan)
- RIR 9/16 向 NSE 提交的公司新聞稿稱 Odisha SiC EPI reactor 已安裝、電力/公用設施已就位，但 commercial production 尚待剩餘法定/監管程序；該項屬 RIR 的新進展，不是 PASC 訂單。未來垂直整合可能改變其委外需求，但 reactor 是 EPI 製程，對 PASC 後續產量的正負影響未披露，暫作產業關係觀察而不作 2340 方向票。[NSE 原始公司公告 PDF](https://nsearchives.nseindia.com/corporate/RIRPOWER_16092026180249_Covering_Letter_-_Press_Release.pdf)
- 本輪搜尋富邦、元大、永豐、KGI 的公開索引沒有取得 2340 可評估的券商原始研究；可見內容是新聞稿轉載、權證日報等，沒有作者、資料截止日、估值法、EPS/營收假設、修正歷史、風險及可檢驗預測，故沒有報告品質結論，也不推測報告動機。

### 當沖欄位與市場彙總界線

- 最新 TWSE 官方商品頁已明確區分兩個不同商品：TWTB4U 是次日當沖資格主檔，含證券代碼與先買後賣/先賣後買註記，交易日 22:30 產檔；TWTB5U 才是每日當沖標的/統計，含市場彙總與個股成交股數/金額，約 20:00 產檔，T 至 T+2 可修正且 T+2 才是最終數。兩項產品均列 NT$1,500/月內部使用，授權/合約不能互換。現行舊 collector 仍是讀 TWTB4U 卻取 TWTB5U 統計商品欄位，契約及程式欄位差異未修正前不把它當個股當沖訊號。[TWSE TWTB4U 商品頁](https://eshop.twse.com.tw/zh/product/detail/793e7c3c28e04a7c9c9675583c624f03)、[TWSE TWTB5U 商品頁](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)
- 官方 TPEx 11627 OpenAPI 本輪回 10/01 與 10/02 兩列；10/02 上櫃當沖量 605,346,000 股、占市場 22.15%，買進 NT$125.449B、賣出 NT$126.033B。資料集 schema 不含證券代碼，這是有日期的新鮮市場參與度背景，不能衍生成個股方向。TPEx individual endpoint 空列/授權未確認，仍不接入；不把 unmatched 證券列補零。[TPEx 11627 開放資料集](https://data.gov.tw/dataset/11627)、[TPEx 統計 OpenAPI](https://www.tpex.org.tw/openapi/v1/tpex_intraday_trading_statistics)

本輪有新增可行動研究脈絡，但沒有多日成熟預測證據：不改程式、分數或門檻；只追加此追蹤段落與授權的 memory ad-hoc note。沒有寫資料庫、手動觸發/重跑 DAG、發 Discord、交易或跑測試。10/05 自然收盤後檢查 2340 卡片是否呈現 H1 虧損 context、日線/營收各來源的有效日與上市櫃覆蓋、TPEx 報價/營收/監理來源、日線通知外部回執及新 outcomes；10/14 再讀台亞原始座談材料。

### 2026-10-03 17:02 台北補記：10/02 after-close mapped collection 按實際執行窗核對

- Airflow natural parent logical run 是 scheduled__2026-10-01T12:00:00+00:00，實際 10/02 20:00–20:04:37 台北；11 個 mapped task indices 中 10 success、1 failed。trigger_market_signal_notify 為 success，但 verify_collection_and_triggers failed。以下以 source_runs 實際 start time 10/02 12:00–12:05 UTC 對照，不單靠可能跨 DAG 重複的 run_id。
- TWSE/TPEx quote：TWSE 1,051/1,051/0，effective 10/01，只有上市映射且較 1,054 檔上市普通股少 3；TPEx 首試與 retry 都遇到 ChunkedEncodingError incomplete read，最後 0/0/0 failed，沒有上櫃行情覆蓋。
- 月營收：本 after-close run 中 TWSE 1,086/1,086/0、TPEx 892/892/0；兩市場來源各自最新期別都只有 2026/08。之後週末 TWSE 月營收仍 success 1,086/1,086/0、effective 2026/08；TPEx 最新週末 run HTTP 530、0/0/0，所以當下上櫃完整性仍缺。
- 市場指數：TPEx 2/2/0，日期 10/01–10/02；TWSE 1/1/0，日期僅 10/01。單列不構成六個連續 session benchmark，日線 run 維持 partial。
- 其他 mapped feeds：TWSE/TPEx 公司事件分別 78/78/0（最新披露日 10/02）與 42/42/0（最新披露日 10/01）；這是公告列樣本，不是全市場個股行情覆蓋。當沖/借券相關：TWSE margin 1,039 raw、994 accepted、45 quarantine，effective 10/02；TPEx margin 801/801/0、有效 10/01；TPEx securities borrowing 814/814/0、有效 10/01；TWSE+TPEx SBL available 1,717/1,717/0、有效 10/02。TWSE trading attention 0/0/0（空公告列，非行情覆蓋），disposition 8/8/0、日期範圍 9/22–10/02。
- TWSE forecast attainment 雖 HTTP 200，8 raw 全數 quarantine、0 accepted、effective 6/30；同一 after-close 的 forecast variance 是 7/7/0、effective 6/30。不要把 API success 當成欄位可用或行情完整。

### 2026-10-03 17:11 台北：3402 漢科新候選的基本面交叉檢查

- 10/02 20:02:35 台北保存候選 id 1353（3402 漢科、上櫃），score 3，法人/價格/營收三家族同向；5 日價格報酬 +8.14%、相對 TPEx +4.96 個百分點，收盤 139.5 高於此前 20 日高點 137。近五日法人淨買 +702,560 股，但只有四個有效觀察日，外資 +687,542、券商自營 +15,018；此單一截面是待追蹤候選，不是勝率或可直接執行的交易判斷。候選 `news_context=no_match_in_sample` 僅指限定樣本沒有匹配新聞，不代表沒有新聞。
- 直接讀取 TPEx 官方月營收 OpenAPI：2026/08 營收 NT$601.466M、YoY +32.84%、MoM +1.59%，1–8 月累計 NT$4,137.776M、YoY +21.37%。7 月營收 NT$592.044M、MoM +6.91%、YoY +36.77%；6 月 NT$553.782M、MoM +7.65%、YoY +29.90%。收入與年增仍強，但 8 月月增與 YoY 斜率都低於 7 月；保留為營收正向家族的反方背景，不因單月成長率加速通知。[TPEx 月營收 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O)
- TPEx 官方獲利率 API 本次回傳漢科 `Date=1151003 / Year=115 / 季別=2`，H1 累計營收 NT$2,944.27M、毛利率 21.05%、營業利益率 11.72%、稅前純益率 11.77%、稅後純益率 9.03%。這些比率是半年累計，不是 Q2 單季；API 列的日期為 10/03，無法從本次回傳證明該列在 10/02 候選前已進入收集流程，所以不回填 id 1353。下一次自然日線掃描只檢查是否以正確期別及可得時間帶入背景。[TPEx 獲利率 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- 公司 IR 的財務業務頁目前 2026 年 Q1/Q2 連結欄為空，並將歷史財務報告查詢導向 MOPS；這只代表 IR 頁面未提供可點閱的 2026 Q2 原件，不代表 MOPS 未公告。TPEx 獲利率 API可核 H1 累計比率，但本次沒有從 MOPS 取回原始財報全文；MoneyDJ 8/14 的季度損益表為二手呈現，不能替代原始報告。[漢科 IR 財務業務頁](https://www.wholetech.com.tw/wholetech/views/i_s/financial_info.php)、[MOPS](https://mops.twse.com.tw/)
- 元大公開頁找到 7/13 一篇署名「時報記者」的新聞，轉述 6 月/Q2/H1 營收、在手訂單及海外建廠/雙位數成長目標；它不是具名券商分析師的原始模型。富邦、永豐、KGI 公開網頁檢索未取回 3402 原始個股研究；沒有作者、資料截止日、估值方法、EPS/營收假設、預測修正、反方風險或可檢驗預測可供評分。搜尋不到公開原件不代表券商內部或會員區不存在，也不推測報告動機。[元大頁上的時報報導](https://www.yuanta.com.tw/eYuanta/Securities/News/GetAPItem?Type=StockHotNews&id=202607130113823)
- 本次重看時仍無 10/03 收盤日線/outcomes 新樣本；最新盤中仍是 10/03 08:02 的事件卡批次，不能代表盤中行情。成熟 `market_signal_outcomes` 仍只含 9/24 一個訊號日，無成熟 5/10-session 窗，未扣成本或公司行動。10/02 after-close mapped run、新聞批次與各來源 effective date/接受隔離/上市櫃覆蓋詳見本節以上紀錄；下一次 10/03 18:30 台北新聞排程尚未到時，不能先報完成。既有 TPEx quote/revenue 缺列與 TWSE quote 少 3 碼問題仍按來源狀態處理，不把缺列當零量。

結論：3402 可列為「基本面有官方支持、但訊號與資料時間仍待下一自然批次確認」的觀察候選。營收增幅、H1 累計獲利率及價格/法人同向可互相佐證部分背景；近月增速放慢、法人有效日偏少、新聞未匹配、H1 API 列的取得時間及缺少券商原始模型，限制目前結論。這次不改程式、分數或門檻，不手動觸發/重跑 DAG、不發 Discord、不交易、不寫資料庫、不跑測試。10/05 收盤自然流程檢查 id 1353 後續、財務背景可得時間、日線/新聞來源列數及上市櫃覆蓋、通知平台回執與 outcomes；券商只公開標題/摘要時繼續標「未核原文」。

### 2026-10-03 17:25 台北：把同季獲利率年對年衝突改成通用通知脈絡

- 使用者澄清目標是可自動擷取、排程、整合的訊號流程，不是逐檔人工調查彙整。依此檢查現有 `source_extension.market_signals`：它已有 TWSE/TPEx 官方季度營益率、月營收、法人、價格、新聞背景來源，但通知的單季營益率方向主要以季對季變化判斷，營收家族與同季營益率年對年變化沒有直接的自動衝突提示。
- 窄幅通用改善：若相鄰官方累計季資料足夠推導出本季單季營益率，且上一年同季也能推導，候選 `profitability_context` 現在會記錄同季值與年對年百分點變化；月營收家族方向與營益率同季年對年方向相反時，自動加到「相反訊號與風險」欄。此脈絡不另計訊號家族、不改 score、家族票數或方向門檻。資料缺一期/基準時不比較；仍依掃描時間截斷，不回填舊候選。
- 此通用改善的核對例來自 MOPS 官方 Q2 合併損益表查詢：3402 的 2026Q2 單季營業利益率 12.05%，2025Q2 為 17.24%，差 -5.19 個百分點；H1 累計則為 11.72% 對 14.79%，不能把 H1 累計拿來冒充 Q2 單季。此例只是驗證比較口徑，程式會對所有有足夠官方季度歷史的股票自動計算，而不是為 3402 寫單檔規則。[MOPS 合併損益表查詢](https://mopsov.twse.com.tw/mops/web/t164sb04)、[TPEx 季獲利率 OpenAPI](https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O)
- MOPS 原始查詢頁成功回傳115年第2季損益表；完整財報 PDF/附註仍未在公司 IR 財務頁取得。先前「未取得原始財報全文」仍指完整 PDF/附註，不代表本次沒有取得官方損益表資料。官方欄位也支持 Q2 稅後淨利 NT$151.605M 對去年同期 NT$162.868M、EPS 2.08 對 2.23；這些只作核對，不新增盈利 surprise 家族。
- 靜態 `ast.parse` 通過 `dags/source_extension/market_signals.py`；未跑測試、未手動觸發/重跑 DAG、未寫資料庫、未送 Discord 或交易。10/05 下一次自然掃描再核接受列是否含同季兩年基準、候選卡是否顯示年對年差與反向提示，並同時檢查來源 effective date、接受/隔離數與 TWSE/TPEx 覆蓋。

### 2026-10-03 21:18 台北：修復 TDCC 全市場批次入口並把持股分布接到候選卡背景

- 使用者再確認本目標是自動化批次收集訊號，不是逐家公司線上調查或人工評比。這次只查現有全市場來源與排程：`stock_collect_weekday_process` 的 10/03 09:00 台北自然週 run 為 success，但 `stock_major_holder_collect` 寫入 `insert=0, delete=0`；`stock.大戶比例` 仍只有 565 個有效日、922,555 列，最新 `資料日期=20250425`、最後寫入 2025/04/26 01:00。Airflow 綠燈在這裡沒有代表資料有更新。
- 找到原因路徑：舊 collector 呼叫 `https://opendata.tdcc.com.tw/getOD.ashx?id=1-5`，本輪唯讀檢查得到 HTTP 302 至 `smart.tdcc.com.tw`；collector 沒檢查 HTTP/轉址，也允許空轉換跳過 save。官方替代批次 `https://openapi.tdcc.com.tw/v1/opendata/1-5` 唯讀 GET 回 HTTP 200、9,770,015 bytes、69,156 列、單一有效日 `20260924`、4,068 個證券代碼。按目前 `全證券代碼相關資料表` 對照，1,054 檔上市及 892 檔上櫃代碼全數有列；同日最新行情有效日是 10/02，因此 TDCC 快照相差 8 天。這只是端點與快照現況核對，不是已寫入資料庫。
- 官方來源有更新頻率說明差異：政府資料開放平台 dataset 11452 的 metadata 標「每1月」，TDCC 個股查詢頁則說資料按每週最後營業日編製。追蹤時以實際資料日期/新鮮度 gate 判斷，不因週排程成功就假設有新週資料。[政府資料開放平台 11452](https://data.gov.tw/dataset/11452)、[TDCC OpenAPI](https://openapi.tdcc.com.tw/)、[TDCC 股權分散表說明](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock)
- 程式窄幅修正：legacy collector 改讀 TDCC 官方 OpenAPI，去除日期欄位 BOM；要求一個有效日期、數值/級距完整、總級距 17、資料日期不晚於收集日、相對最新日線不超過 14 天，以及上市/上櫃原始與轉換覆蓋各至少 95%。任何空批次、轉址、過期或不完整轉換會明確失敗；日誌帶有效日、落後天數、raw/目標/彙總列數與雙市場覆蓋。
- 同一個既有 `大戶比例` 表已接入 `market_signals` 自動 join，沒有再加第二條抓取。只對最新兩個有效日都存在、每市場雙期有效覆蓋均至少 95%、每檔兩期不衝突、於掃描起始前載入且快照間隔及對行情有效日間隔都不超過 14 天的股票比較。單一公司缺列不當零；任一 gate 不過就不顯示。符合既有大戶比例 2 個百分點變化條件時，只加在已成立候選卡的 TDCC 背景行，不加方向家族、分數或候選門檻；方向預測力尚未驗證。
- 目前 legacy 表沒有 2026 年兩期快照，官方 API 本輪只確認最新一期是 2026/09/24；所以本次尚無可展示的 TDCC 變化，也未回填過去候選。需等自然週批次收進連續兩期後，才核 effective date、insert/轉換列數、兩市場覆蓋與候選卡。未手動觸發/重跑 DAG，未送 Discord、未交易、未寫資料庫、未執行測試。
- 21:18 台北 runtime 唯讀複核：日線仍是 10/02 `ready/partial`，母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、股利 1,053、候選 50，通知 receipt 為 0。10/03 08:02 的最新盤中列 10 張候選，但價格/法人/營收/財務/借券母體皆為 0，屬事件卡掃描，不是盤中行情訊號。
- `market_signal_outcomes` 仍只有 2026/09/24 一個訊號日；`signal_close` 與 `next_close` 各 81 列、各 80 列有方向值，只有 1-session 已結算結果；沒有成熟 5/10-session、交易成本或公司行動調整樣本，不調方向門檻。10/02 after-close Airflow 子 run 仍 failed：mapped ingestion 有 1 個失敗、終端 verifier failed；10/03 週末沒有新 after-close 收盤掃描。
- 10/03 最新新聞批次是 logical 16:30、實際 18:30–18:38 台北；三個 task 均 success，寫入新增 1,146 篇、資料表總量 9,203,464，但 `collect_article_url` 日誌記 TVBS 列表請求 `ChunkedEncodingError`，所以不能把整批 success 解讀為所有來源完整。此批次沒有可見的 `[source_coverage]` 行；原頁發稿時刻也未在本輪逐篇核對，新聞仍只作來源與時間精度受限的背景，不作方向票。
- 同一早間來源 run：TWSE 重大訊息 70/70 接受、月營收 1,086/1,086、財測差異 7/7；TPEx 重大訊息與月營收各失敗 0/0。來源 failure 是明確缺口，不當成零列或當日完整覆蓋。

### 2026-10-03 21:28 台北：使用者再次校正為自動化批次訊號主線

- 使用者指出逐家公司上網找券商報告再評比彙總，偏離「可自動化、可抓取」目標。承認前序工作比重偏向單檔研究；後續不再把公司個案網搜當成待辦或訊號來源。
- 主線限定為全市場或明確可批次擷取的來源：盤點 registry/排程/原始列/轉換及通知；查 freshness、effective date、accepted/quarantined、TWSE/TPEx 覆蓋、新聞來源與發布時間品質；自動 point-in-time join 可量化資訊；用成熟 outcomes 和真實通知 receipt 評估。券商原始研究只有在有授權且存在可排程批次取得介面時納入；不得逐檔手動抓取補洞。
- 前序個股案例與公開網頁線索保留為歷史紀錄，不代表建立個股特例、評分器或下一輪工作清單。下一個可行動工作由全市場來源缺口及自然排程證據決定；無新批次資料時不重複人工研究。
- 本次只核對追蹤紀錄、工作樹及 Python AST；目前週末無新平日收盤自然 run 可驗。保留 TDCC 官方 OpenAPI 批次接入與 freshness/coverage gates 的待自然收集驗證；不手動跑 DAG、不發通知、不交易、不跑測試。

## 2026-10-03 21:47 台北：自然批次、訊號成熟度與新聞時間品質複核

### 台股來源與訊號狀態

- 唯讀核對 Airflow 與 `crawler`：最新 daily market snapshot 仍是 as-of 2026/10/02，`ready/partial`，universe 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、借券 1,795、董監 1,944、股利 1,053、候選 50、內部通知計數 0。最新 intraday 是 10/03 08:02，`ready/partial`，10 個候選但 universe/價格/法人/營收/財務/借券均為 0，是事件卡掃描而非盤中行情訊號。
- `market_signal_outcomes` 仍只覆蓋 2026/09/24 一個訊號日：horizon 1 的 `signal_close` 81 列，50 hit/30 miss/1 unresolved；`next_close` 81 列，37 hit/43 miss/1 unresolved。無成熟 5/10-session、成本或公司行動調整樣本；不調方向門檻。
- 10/02 20:00 台北 after-close Airflow parent 失敗，11 個 mapped source 中 10 success、1 failed；TPEx 個股行情 0/0/0，兩次 incomplete read，第二次收到 4,193,897 bytes、尚差 472,744 bytes；TWSE 行情 1,051/1,051/0，相較 1,054 檔上市普通股少 3。其他有效日、接受/隔離和覆蓋沿用本文件 10/03 17:02、18:50 的來源分項，不把成功 task 當全日完整。
- 10/03 08:00 台北 weekend extension 5 個 mapped task indices（兩個 TPEx feed 各重試一次，共 7 次 source_run attempts）：TWSE 公司事件 70/70/0、effective 10/02、53 個證券代碼（公告事件列，不是股票覆蓋）；TWSE 月營收 1,086/1,086/0、effective 2026/08、包含 1,054 上市普通股、30 創新板及 2 DR；TWSE 財測差異 7/7/0、effective 6/30、5 個發行人。TPEx 公司事件與月營收各兩次 HTTP 530、0/0/0；parent verifier failed。各成功來源的有效日和樣本語義分開記錄，不把事件/財測列當個股全市場覆蓋。
- TAIFEX 期貨及選擇權法人來源已有本地批次 adapter/registry 定義，但本輪唯讀查 DB `source_registry` 與 `source_runs` 仍無 TAIFEX 列；程式定義尚未經自然 DAG bootstrap/ingestion。10/04 自然 daily process 後再確認 catalog，下一個平日 after-close 核對兩 feed 的 raw/accepted/quarantined、effective date、完整度；該市場彙總不計入個股方向票。

### 新聞批次與發稿時間精度

- 最新新聞 DAG logical 10/03 16:30 台北、實際 18:30–18:38，`collect_article_url`、`collect_news`、`delete_and_insert` 都 success；收網址階段記錄 TVBS 列表請求 `ChunkedEncodingError` 並略過該來源，因此綠燈不是 12 個來源全覆蓋。此 run DB 最近主鍵樣本 1,146 篇；依 parser 保存的 `created_at` 統計，缺值 0、晚於 ingest `create_time` 五分鐘以上 0、午夜時間 151（iThome 60、thenewslens 90、ETtoday 1）。這些是資料庫時間欄統計，不是原頁發稿時刻或時區已核證。
- thenewslens 90 篇時間落在 2026/01/20–10/03，iThome 60 篇落在 9/30–10/03；既有 `_attach_recent_news_context` 只查近 48 小時並將這兩源標作 `date_only`，過舊列不應變成即時新聞訊號，但日期精度會造成窗口邊界漏配。新聞仍只作候選背景，不計方向家族。
- Airflow 這一批沒有 `[source_coverage]` 行；本機 `all_news.py` 的 per-source coverage instrumentation 最後修改時間為 18:56，晚於 18:30 run，故這次舊執行不能驗證新記錄。待下一次自然新聞排程檢查各來源 urls/parsed/valid_time/accepted，不回頭重跑。

本輪新增 runtime 與批次新聞時間品質證據，無程式碼改動；四個現行模組在本機及 worker 均通過 Python AST 解析。未跑測試、未手動觸發/重跑 DAG、未寫資料庫、未人工發 Discord、未交易。工作樹既有無關變更均保留。

### 2026-10-03 21:54 台北：補上 TPEx 行情續傳的批次完整度診斷

- 根因證據仍是官方 TPEx 日行情 HTTP body 截斷：4,666,641 bytes 的回應兩次分別只收到 704,105 與 4,193,897 bytes，拋 `ChunkedEncodingError`，使上櫃行情 0/0/0。adapter 已使用 identity encoding 與驗證過的 Range/Content-Range 續傳；自然排程尚未驗收非零 offset 的完整下載。
- 窄幅增強 `dags/source_extension/adapters.py::_fetch_tpex_equity_quotes_payload`：只有完整 byte sequence 通過總長檢查且 JSON parse 成功後，才輸出 request 數、bytes_received、expected_bytes、final_http_status；若續傳耗盡，例外訊息包含相同傳輸計數。HTTP 206 只有在整體資料完整後才會作為成功回傳狀態。`docs/source_extension.md` 已說明這個契約。
- 本機與 Airflow worker Python AST 解析均通過；沒有測試或手動 DAG。需等 10/05 自然 after-close 檢查 `[tpex_quote_transfer]`、HTTP status、raw/accepted/quarantined、effective date 及上市/上櫃覆蓋，不能把靜態驗證當成 runtime 修復成功。

### 2026-10-03 22:01 台北：修正新聞日期精度列的自動配對邊界

- 以 10/03 最近寫入的 1,146 篇文章資料為證，151 列的 `news.created_at` 恰為午夜（iThome 60、thenewslens 90、ETtoday 1）；程式將午夜值標示為 `date_only`，但候選新聞 SQL 原本仍只用精確 `observed_at - 48h` 比較，可能漏掉 cutoff 當天實際在 48 小時內發布的日期精度文章。
- 通用修正 `_attach_recent_news_context`：一般時間戳仍限精確近 48 小時；`created_at=00:00:00` 的日期精度列，若資料日期與 48 小時 cutoff 落在同一日或更新，也按日期重疊納入。這可能包含實際超出 48 小時的午夜列，因此 matched 卡片明示原頁發稿時分/時區未核，並保留「只作媒體背景、不參與多空判斷」；不影響家族、分數或候選門檻。無匹配訊息同步說明日期精度列的擴展。
- `docs/source_extension.md` 已記錄這個新聞時間契約。本機與 Airflow worker 的 `market_signals.py` AST 解析通過；未跑測試、未寫資料庫、未手動觸發 DAG 或送通知。待下個自然 daily scan 核對 SQL runtime 與卡片時間標籤。

## 2026-10-03 22:10 台北：自動化優先與盤中資料授權界線

- 使用者再次校正主線：要能批次擷取、自動評估與持續校準的訊號；逐家公司上網搜券商報告再評分不符合目標。以來源 registry / 排程 / raw-to-accepted / point-in-time join / 成熟 outcomes / 通知回執推進；券商研究只在有授權、可排程批次來源時納入。
- 即時唯讀查詢：最新日線仍為 10/02 ready/partial，母體 1,946、價格 1,938、法人流 1,891、月營收 1,945、季財務 1,902、候選 50。10/03 08:02 盤中 run 為 ready/partial，價格/個股母體/法人/營收/財務/借券皆 0；10 個候選全是 company_announcement。formatter 的批次標題是「台股公告與營收快訊」，不是盤中行情掃描。本 run 的通知 ledger 有 11 筆 sent 記錄；按程式語義，候選是以 dedupe key 記錄、另有來源不完整警示，不能解讀為 11 檔方向訊號或 11 次獨立通知。
- 10/03 08:00 週末批次：TWSE 事件 70/70 接受、0 隔離、effective 10/02、53 個公司代碼；TWSE 月營收 1,086/1,086、effective 2026/08；TWSE 財測差異 7/7、effective 6/30、5 個發行人。TPEx 事件與月營收當批各重試一次後仍 HTTP 530、0/0/0；資料表較早保留的有效列分別是 42 個事件列（effective 10/01）及 892 檔月營收（effective 2026/08），不得把本次失敗當作零列。
- 最新平日 after-close parent 仍是 10/02 20:00 台北失敗；TWSE 行情有效日 10/01、1,051/1,051 接受（上市普通股母體 1,054，缺 3），TPEx 行情 0/0/0。resume adapter 尚待 10/05 自然批次驗證。market_signal_outcomes 仍只有 9/24 一個訊號日；1-session signal-close / next-close 各 81 列、80 列有方向，尚無成熟 5/10 日或成本調整樣本。
- 新聞 DAG 最新成功 scheduled run 為 logical 10/03 16:30 台北、實際 18:30–18:38；Airflow next-create-after 是 10/03 23:30 台北，尚未到期，沒有手動補跑。先前該批 TVBS 列表抓取 ChunkedEncodingError 並跳過來源，因此綠燈仍不代表全來源覆蓋。
- 官方即時個股行情是授權門檻：TWSE 即時資訊頁要求申請/簽約，非券商申請使用者列有每月傳輸授權費與資訊費；TWSE 盤中零股成交商品雖有全市場逐日檔，產製時間是 14:00 且需付費，不等於 15 分鐘盤中行情 API。維持官方批次開放來源；未核授權的網站查詢或即時行情路徑不接入。
- 本輪不改訊號程式或門檻；已有快訊文案清楚標明公告/營收用途，主要未解缺口是個股行情批次完整度與官方即時資料授權。未手動觸發 DAG、未發通知、未交易、未跑測試。

### 2026-10-03 22:20 台北：當沖個股統計的官方契約、授權與現有覆蓋

- 只讀請求 TWSE 官方 OpenAPI /v1/exchangeReport/TWTB4U：HTTP 200、1,228 列，欄位只有 Date/Code/Name/Suspension，有效日為 1151005（2026/10/05）。這是次日資格/暫停註記，不含成交股數與買賣金額；有效日也晚於本次檢查日 10/03，不回填成 10/03 訊號。
- TWSE Data E-Shop 分別定義 TWTB4U 資格主檔與 TWTB5U 當日沖銷標的及統計。TWTB5U 約 20:00 產出個股沖銷股數與買賣金額，成交日資料可於 T+2 更正；產品列內用 NT$1,500/月、外用 NT$3,000/月。[TWSE TWTB4U](https://eshop.twse.com.tw/zh/product/detail/793e7c3c28e04a7c9c9675583c624f03)、[TWSE TWTB5U](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)
- 現有舊 StockDayTradeCollectProcess 仍由 www.twse.com.tw/exchangeReport/TWTB4U 取成交統計欄位，另 POST TPEx 個股網站查詢路徑。10/02 自然 Airflow 任務先反覆 10 次因 TWSE 回應缺成交股數/買賣金額欄位而記 KeyError，之後因上櫃請求回應而標 success；不能用 parent/task 綠燈證明上市和上櫃完整。
- 唯讀查 stock.當沖資訊 最新有效日 10/02：1,228 列/代碼，證券主檔可匹配 972 列上市、256 列未匹配、0 列上櫃；現有列的三個數值欄非 NULL。未匹配不能當零量，且未匹配代碼的證券類別未分類。本表目前不被 source_extension/market_signals.py 讀取，不計入候選分數或訊號通知。
- 不把未列入官方開放授權的個股網站路徑接入訊號。若要自動化 TWSE 個股當沖量值，需先取得 TWTB5U 訂閱/內用授權；TPEx 個股端點授權仍未核。資格主檔可作執行限制背景，不能當方向訊號。未取得授權前沿用已核准官方批次訊號，不手動補抓。

### 2026-10-03 22:29 台北：日線多批通知超限診斷與分段標示

- 既有通知紀錄顯示 10/02 daily run 的 2 個 digest 批次為 1 sent、1 failed；Airflow 前一個失敗 child 的第三次嘗試記錄 Discord `Request entity too large`（HTTP 40005）。這是通知傳輸/批次問題，不是訊號分數或行情證據，未據此調整門檻。
- `dags/source_extension/market_signals.py` 對分批 byte budget 預留 `第 10/10 批` 標籤空間，並在實際每批訊息加上 `第 i/n 批`，方便識別同一份 digest 的續批。這是狹義通知可靠性修正；候選、排序、家族分數、觸發門檻均未變。
- 對已保存的 10/02 候選做 formatter/splitter 唯讀 dry-run：50 candidates、8 embeds，切為 2/3/3 embeds 的 3 批；含標籤的 JSON payload 為 7,859、9,165、9,316 bytes（保守 budget 數值 7,861、9,167、9,318），均低於 10,000 bytes。未送 Discord。
- 本機及 Airflow scheduler container 的 Python AST 解析通過。這只驗證語法與既有資料 dry-run；尚未驗證下一個自然排程的實際 HTTP 回執。待 10/05 自然 daily run 核對每批 ledger sent/failed、digest part 標籤與市場快照完整度。未跑測試、未手動觸發/重跑 DAG、未寫 DB、未交易。保留既有工作樹變更。

### 2026-10-03 22:44 台北：週末自然批次診斷與 TAIFEX 批次來源預檢

- 只讀查 Airflow，自然週末 run 為 source_extension_collect_weekend 的 scheduled__2026-09-27T00:00:00+00:00（邏輯區間起點），實際 10/03 08:00 台北啟動；共映射 5 個來源，3 個成功、2 個失敗。這是週末排程的區間標記，不是把 9/27 資料誤認成今天。
- 來源 task 回執及資料列複核：TWSE 重大訊息 HTTP 200，70/70 接受、0 隔離，有效日 10/02，53 個發行人（事件樣本，不是全上市母體覆蓋）；TWSE 月營收 HTTP 200，1,086/1,086、0 隔離，有效月 2026/08（1,054 上市普通股、30 創新板、2 DR；此 TWSE feed 的上櫃覆蓋不適用）；TWSE 財測差異 HTTP 200，7/7、0 隔離，有效期末日 2026/06/30、5 個發行人。TPEx 重大訊息與月營收各兩次嘗試均 HTTP 530，0/0/0；沿用先前資料不等於本批成功。
- 下游 intraday scan 仍按設計執行並如實標 partial：市場母體/價格/法人/營收/財務/借券計數皆 0，10 個候選；不是行情掃描。通知 task 回執 sent=3、failed=0，ledger 11 筆 sent（10 個候選去重記錄及 1 個來源不完整警示）。程式卡片明示「本輪仍已分析可用資料；訊號名單請視為暫定」。這是自然 Airflow 任務回執；沒有手動發通知。
- 日線 outcome 唯讀複核：10/02 daily run ready/partial，1,946 母體、1,938 價格、1,891 法人流、1,945 營收、1,902 財務、1,795 借券、50 候選；通知欄 0。成熟表目前仍只有 9/24 的 signal_close/next_close 各 81 筆 horizon=1；行情資料內 9/24 至 10/02 僅有 9/24、9/29、9/30、10/01、10/02 五個交易資料日，故 9/24 往後第 5 個交易日尚未收進本輪資料。維持原門檻，等 10/05 自然收盤批次檢查 5-session maturity。
- 為免下一個自然收盤批次才發現欄位不合，使用現有 TAIFEX allowlisted adapter 對兩條官方 OpenAPI 做唯讀、不落庫預檢：期貨與選擇權皆 HTTP 200，最新有效日 10/02，各 6 筆 normalized、6 筆 quality=ok；期貨覆蓋「臺股期貨/櫃買指數期貨 × 3 法人」，選擇權覆蓋「臺指選擇權 × CALL/PUT × 3 法人」。這是批次市場彙總上下文，不是個股覆蓋或方向票。本次 DB source_registry 尚無 TAIFEX 列，因最近部署定義後尚未有自然 ensure-schema run；等 10/04 04:00 daily catalog bootstrap 核對 registry，再等 10/05 after-close 自然 task 驗證 raw/accepted/quarantined、effective date 與通知上下文，沒有手動寫 DB。
- 本輪未改程式、分數或門檻；未手動觸發/重跑 DAG、未寫 DB、未手動發 Discord、未交易、未跑測試。現有 TPEx HTTP 530 與 TAIFEX scheduled persistence 仍待自然排程驗收。


### 2026-10-03 22:49 台北：保存來源 HTTP 失敗碼供自動監控

- 以 10/03 自然週末批次的來源執行紀錄確認：TPEx 重大訊息與月營收各重試兩次，錯誤文字明確是 HTTP 530，但 source_runs.http_status 四筆都為 NULL；排查與自動化監控無法直接用數值欄位分辨 HTTP 狀態。
- 窄幅修正 dags/source_extension/registry.py 的 ingest_source 失敗處理：若例外有 response.status_code，就將該 HTTP code 寫入 source_runs.http_status；DNS、連線中斷等沒有 HTTP response 的錯誤仍留 NULL。保留錯誤文字，不更動重試、來源狀態、資料列、訊號分數、候選或通知行為。
- Airflow scheduler 與 worker 容器對修改後 registry.py 均通過 Python AST 解析。未跑測試、未回寫歷史 DB。等 10/04 自然來源批次如再遇 HTTP failure，核新 source_runs 是否保存數值狀態；不能把靜態驗證當 runtime 驗證。


### 2026-10-03 22:54 台北：讓 partial 來源警示列出失敗 feed

- 10/03 自然 intraday run 已確認 source_status=partial，當時外部警示只列出上游 run ID，沒有 feed 名稱或 HTTP 狀態，現場排查需要再去查 Airflow。
- 在 dags/source_extension/market_signals.py 新增唯讀 _source_collection_issues：按 source DAG run 取每個來源映射的最後一次回執，列出 failed feed 名稱與 HTTP 狀態；另把已核准且啟用但缺少回執的來源列為缺漏。partial warning 現在帶「未完成來源」欄位，仍保留名單暫定文案。只顯示來源名稱/HTTP code，不把失敗列數寫成零量或零資料。
- 用已保存的 10/03 source_runs 做不送出、不寫 DB 的通知內容預覽，正確列出 TPEx 重大訊息及 TPEx 月營收兩個失敗來源；舊紀錄 HTTP code 原本為 NULL，所以預覽顯示「HTTP 狀態未保存」。前一個 registry.py 修正會在未來 HTTPError 有 response 時保存 status_code，待自然執行確認為 530。
- scheduler 與 worker 容器對 market_signals.py 通過 Python AST 解析。未跑測試、未手動發 Discord、未手動觸發/重跑 DAG、未寫 DB。待下一個自然 partial run 驗收警示回執及 HTTP 530 顯示。


### 2026-10-03 22:55 台北：讓 partial 來源警示列出失敗來源與狀態

- 最新可用自然 intraday run 是 10/03，scan status 為 ready/partial，來源不完整通知先前只列上游 run ID；看通知本身無法知道是哪個 feed 失敗。
- 在 dags/source_extension/market_signals.py 加入唯讀來源回執摘要：按 source DAG run 的來源映射挑最後嘗試，顯示失敗來源名稱/HTTP 狀態；另標出已核准啟用但缺少回執的來源。警示仍提醒名單暫定，不將失敗 row_count=0 解讀為零資料或零量。
- 以既有 10/03 source_runs 做 no-send/no-write 預覽，正確列出 TPEx 公司重大訊息和月營收兩項失敗；歷史 HTTP status 欄仍為 NULL，預覽因此標「HTTP 狀態未保存」。registry.py 已另修正新 HTTPError 回執保存 status_code，尚待自然批次驗收。
- Airflow scheduler 與 worker 的 market_signals.py AST 解析通過。沒有測試、DB 寫入、DAG 觸發、Discord 發送或交易。待 10/04 後續自然批次檢查警示回執與新 HTTP 狀態欄。


### 2026-10-03 23:01 台北：收盤摘要也列出來源缺口，修正欄位覆寫

- 將 10/03 partial 警示的來源回執摘要延伸到收盤日線 embed；只在 partial 且有上游 DAG lineage 時插入「未完成來源／覆蓋檢查」欄，保留分批 byte budget。市場廣度欄改為依欄名更新，避免新欄加入後固定索引覆寫來源診斷。
- 對最新可用的 10/02 daily 50 候選做 no-send/no-write formatter dry-run：8 embeds、3 batches，JSON payload 分別 4,791／5,543／5,538 bytes；新欄位被保留，市場廣度欄也仍存在。舊 daily run 的 source_runs 缺 DAG ID lineage，所以內容如實顯示「來源回執缺少 DAG ID 或 DAG 不符，無法逐來源核對」，沒有把 13 個來源都誤報為漏跑。
- scheduler 與 worker 的 market_signals.py AST 解析通過。沒有測試、DB 寫入、DAG 觸發或 Discord 發送。待下一個自然 daily run 以新的來源 lineage 驗證具體失敗 feed 顯示與外部通知回執。


### 2026-10-03 23:03 台北：修正收盤來源欄位覆寫與舊 lineage 誤報

- no-send daily formatter dry-run 先發現新加入的來源回執欄被固定索引的市場廣度寫入覆蓋；將廣度欄改為按欄名更新，保持來源欄位與廣度內容各自完整。
- 同時修正來源問題摘要對舊資料的判斷：若該 run 沒有任何逐來源紀錄可用指定 DAG ID 關聯，先查是否存在未帶 DAG ID/錯 DAG 的 legacy rows，改報「無法逐來源核對」，不再把所有啟用來源誤列為漏跑。新 ingest_source task 已於開始時擷取 Airflow lineage；待下個自然收集驗證。
- 重做 10/02 daily 50 候選的 no-send/no-write dry-run：來源欄明確標示舊回執缺 lineage，市場廣度欄仍存在；8 embeds 拆為 3 批，JSON payload 4,791／5,543／5,538 bytes，均低於 10,000。通知未送出。
- scheduler 與 worker 容器 AST 解析通過。沒有跑測試、手動觸發 DAG、寫 DB、發 Discord 或交易。等待 10/04 自然批次與 10/05 收盤 run 驗收新的逐來源 lineage 與警示回執。


### 2026-10-04 01:47 台北：重驗自動通知封包並更新自然排程前基線

- 本輪嚴格沿用全市場批次主線：只讀自然排程、來源列數/有效日、outcomes 與通知回執；沒有逐檔搜尋或人工評比券商報告。
- 唯讀刷新 market_signal_runs：最新 daily 為 10/02 20:02:35–20:02:42 台北，ready/partial；母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、除息 1,053、候選 50，notified_count=0。最新 intraday 仍為 10/03 08:02:15–08:02:16、ready/partial；母體/價格/法人/營收/財務/借券/董監/除息計數皆 0，只有 10 個公司事件候選，內部通知列數不可當外部平台收件證明。
- market_signal_outcomes 最新仍只含 9/24 訊號日：signal-close horizon 1 有 81 列（50 hit、30 miss、1 unresolved），next-close 有 81 列（37 hit、43 miss、1 unresolved）；沒有成熟 5/10-session、交易成本或公司行動調整樣本，不據此調方向門檻。
- after-close 最新 parent 仍是 scheduled__2026-10-01T12:00:00+00:00 且 failed。其 daily child capture_market_signal_snapshot success，notify_market_signal_snapshot 第三次嘗試仍 failed；通知 ledger 為 digest failed 1 筆、短操作錯誤通知 sent 1 筆，後者不等於候選摘要送達。從已保存的 10/02 50 候選做唯讀 no-send/no-write 重建：8 embeds 切成 3 批，實際 JSON request 為 7,859、9,182、9,367 bytes；預留最長批次標籤後為 7,861、9,184、9,369 bytes，均低於 10,000-byte 上限。這只驗證目前 formatter/splitter 對歷史資料可切包，不代表 Discord 已收到；待 10/05 自然收盤 child 看每批實際回執。
- 10/03 08:00 台北 weekend source parent 使用舊排程邏輯區間 scheduled__2026-09-27T00:00:00+00:00：5 個 mapped task 中 3 success、2 failed。成功 feed：TWSE 公司事件 70/70 接受/0 隔離、最新 effective 10/02、53 個發行人（公告列樣本，不是全上市母體）；TWSE 月營收 1,086/1,086/0、effective 2026/08；TWSE 財測差異 7/7/0、5 個發行人、期末 2026/06/30。TPEx 公司事件與月營收各兩次失敗，最後皆 0/0/0，因此該批沒有上櫃事件/營收覆蓋。舊 source_runs.http_status 仍為 NULL；它們發生在 numeric HTTP-code 保存修正之前，不回寫歷史列。
- 週末 DAG 的 CronTriggerTimetable 已載入，下一自然排程是 10/04 08:00 台北；截至本檢查 01:47 尚未到點、Airflow 尚無此 run。10/03 01:34 新增的 TWSE/TPEx 內部人預定轉讓/未完成轉讓四個批次 feed 也尚無自然 source_runs 回執；待 08:00 run 核有效日期、raw/accepted/quarantined 與上市/上櫃分布，不以先前 HTTP probe 冒充落庫。
- 最新新聞自然 run logical 時點為 10/03 18:30 台北、實際執行 23:30–23:39（scheduler 延遲約 5 小時）。12 個來源中 ETtoday 480/480/480/480、iThome 60/60/60/60、The News Lens 90/90/90/90、中央社 100/100/100/100、中時 198/198/198/198、聯合 60/60/60/60 為 complete；自由 100 URLs/100 parsed/100 valid-time/84 accepted 與鉅亨 150/150/150/148 為 partial；TVBS、新頭殼、三立均 no URLs；公視 43 URLs/43 parsed/0 valid-time/0 accepted（empty_after_parse）。合計 1,281 parsed、1,238 valid-time、1,220 accepted。此 run 早於 10/04 的 parser 修正，並非新 parser 驗收；日期精度午夜欄也不代表原頁發布時分/時區已核。下一次新聞自然排程 10/04 05:30 台北再查逐來源時間與覆蓋。
- 本輪僅資料庫 SELECT、Airflow metadata/log 讀取及 formatter 靜態重建；沒有測試、DAG 觸發/重跑、DB 寫入、Discord 發送或交易，未更動訊號程式或門檻。

### 2026-10-04 02:01 台北：台股通知 ledger 增加平台訊息回執

- 前一輪日線摘要曾因 Discord 40005 Request entity too large 投遞失敗；現有分批器 no-send 重建可將歷史 payload 切在 10 KB 內，但 ledger 原本只存內部 sent/failed，不能把各批記錄連到 Discord 建立的 message。
- Discord 官方 Execute Webhook 規格說明，wait=true 會等待 server confirmation 並回傳建立的 message body。新增專用 DiscordNotify.notify_with_receipt；既有 notify() 呼叫與其他 DAG 行為不變。台股訊號日線摘要、盤中批次和來源/失敗提示現在要求回傳數字 message ID 才記 sent，並將 platform_receipt_id 與 platform_http_status 寫入 market_signal_notifications。
- 新回執狀態語義：2xx 且有 message ID 才是 sent；2xx 無 ID 記 accepted_unverified；請求逾時/連線錯誤或 HTTP 5xx 記 delivery_unknown；明確非 2xx rejection 記 failed。accepted_unverified 與 delivery_unknown 不自動重送，避免在 Discord 可能已建立訊息但回執不完整時重複推播；failed 可依既有 Airflow retry 重試。歷史 ledger 不回填猜測的 message ID。
- schema 新增 migration v9，資料表欄位是 additive nullable fields。此輪沒有呼叫 create_schema 或改 DB；當前資料庫仍是 v8，待自然 market_signal_notify 執行套用並核 migration/runtime。既有來源自然排程照常，不手動觸發、發 Discord 或交易。
- host、Airflow scheduler、worker 的 Python AST/靜態 import 均通過；另靜態確認 v9 欄位只加入 market_signal_notifications，未改 portfolio notification schema。未跑測試；沒有 DB 寫入、Webhook 請求或程式以外的副作用。待 10/04 首個自然 child 核 schema migration、每批 HTTP status/message ID、intraday 每候選 ledger 及 daily notified_count；10/05 after-close 再驗日線分批 receipt。
- 規格來源：[Discord Webhook Resource — Execute Webhook](https://docs.discord.com/developers/resources/webhook#execute-webhook)。

### 2026-10-04 02:08 台北：避免未知回執令盤中來源游標卡住

- 審查 Discord 新回執狀態與既有 intraday source cursor 的交互後，確認一個控制流程風險：若 delivery_unknown / accepted_unverified 一直不重送，舊 candidate hash 會反覆留在 cursor overlap 視窗，可能阻塞游標或使同一警示週期性重播。
- 調整 intraday cursor gate：明確 HTTP rejection 或尚在送出的 pending row 仍停住 cursor，保留 Airflow retry；本次新出現的 ambiguous receipt 會留在 ledger、當次 child 保持失敗並發操作錯誤提示，但 cursor 可越過該批，避免重複投遞或無限重掃。後續自然 run 遇到舊 ambiguous key 只略過且不計入 notified_count；該 run 的 ledger run_key 與 status 保留供稽核。只有帶 message ID 的候選才增加 notified_count。
- 靜態 AST 與 scheduler/worker import/schema 檢查通過。未跑測試、未呼叫 schema migration、未送 webhook、未觸發 DAG 或交易。第一次自然 intraday run 仍須驗證 cursor、ledger status、平台 ID 及 Airflow 子 run 狀態是否符合此分流。

### 2026-10-04 02:12 台北：明確校正為自動化全市場訊號

- 使用者再次明確指出目標是可自動抓取、排程和批次處理的全市場訊號，不是逐家公司上網找 IR、新聞或券商頁面，再人工評比報告。前面的單一公司研究偏離範圍；既有個案紀錄只作歷史核對，不作為後續常規工作模式。
- 後續以來源登錄與授權、自然批次回執、發布/有效時間與 PIT 對齊、raw/accepted/quarantined、上市/上櫃覆蓋、跨來源脈絡、成熟 outcomes 和實際通知回執為主。券商研究只盤點可合法授權、可排程、可批次取得的 feed/API；沒有核實的來源記為缺口，不再逐檔搜尋或人工評分。
- 這是工作範圍校正，未宣稱新一輪資料收集或訊號結果；本次只更新追蹤記錄與授權的 ad-hoc memory note，沒有改程式、測試、資料庫寫入、DAG 觸發、Discord 發送或交易。
### 2026-10-04 02:38 台北：董監設質風險脈絡與來源重抓造成的 PIT 回放缺口

- 持續按全市場批次目標工作。最新可查 daily 仍是 10/02 `ready/partial`，保存 50 候選、通知計數 0；最新 intraday 是 10/03 `ready/partial`，只有 10 個公告候選、行情/方向家族計數為 0。Airflow 最近週末 parent 仍為 10/03 08:00 台北 failed；10/04 08:00 自然排程尚未到。最新 after-close parent 是 10/02 20:00 台北 failed。Outcome 只來自 9/24 一個訊號日：signal-close/next-close 各 81 列、各 80 列有方向結果；方向命中 50/80 與 37/80，平均超額報酬 +1.776/+0.987 個百分點。只有一個日期、1-session，沒有成熟 5/10-session 或成本/公司行動調整樣本，不調整門檻。
- 唯讀核對董監月資料最新自然 source_runs 為 9/25 04:00 台北：TWSE HTTP 200、27,475 raw、26,804 accepted、0 quarantined、671 duplicates、effective 2026/08；TPEx HTTP 200、17,568 raw、17,139 accepted、0 quarantined、429 duplicates、effective 2026/08。兩市 accepted entity codes 與目前證券主檔交集為上市 1,054/1,054、上櫃 890/892；另 30 個 TWSE 與 1 個 TPEx accepted codes 未匹配目前主檔，保留為未匹配，不當成零持股。來源列上的 date-only 出表日為 2026/09/18。
- 批次列品質檢查：accepted rows 的目前持股、設質股數與比例欄均非空；比例均在 0–100，設質股數沒有高於目前持股。依姓名去角色列重複後，5 個 TWSE 人員群組的目前持股值衝突、設質數無衝突；公司月份若任何人群組欄值衝突便不產生該月份脈絡。另 29 個 TWSE、15 個 TPEx accepted rows 的來源比例欄與股數重算比例相差超過 0.25 個百分點，因此新脈絡從股數重算，不採報告比例欄。單一 2026/08 期資料不能推出月趨勢。
- 在 `dags/source_extension/market_signals.py` 加入整批可重用的 `insider_pledge_context`：按公司/月份與唯一申報人加總直接持股及設質股數，需 PIT 可得、出表與月份皆不超過 90 日、分母正值且人員列無衝突；排除關係人重複欄，不含人名。僅在相反訊號/風險欄顯示比例、期別、申報人數及官方來源連結，不新增 directional family、不改 score、候選門檻或排序。對已保存的 10/02 50 候選做唯讀、no-send formatter 預覽：14 檔有正設質比例，8 張可見個股卡中 1 張顯示；拆 3 批，實際 request-size 預估 7,859／9,182／9,424 bytes，均低於 10,000。未送通知。
- PIT 回放診斷發現：用現行程式與 10/02 掃描截止時間對來源表做唯讀重算，只得 9 個候選，revenue_count 與 financial_count 為 0；已保存的 10/02 run 是 50 檔、1,945 revenue_count、1,902 financial_count。截止時間換算為 2026-10-02 12:02 UTC。部分月營收/季財務列的 `created_at` 早於截止，但 `updated_at` 因 10/02 20:00–10/03 00:00 UTC 的後續批次重抓而晚於截止；`registry.py` 原 upsert 不論內容是否變化都更新 `updated_at`，而同 record key 的 payload 會覆寫。這能解釋回放資料量下降，但不能證明後續內容與當時完全相同，也不能從現存表復原已覆寫的舊版 payload。
- 針對未來批次窄幅修正 `dags/source_extension/registry.py`：source_records 與 source_record_fields 的 `updated_at` 現在只在 normalized content/typed field 值變更時前移；相同內容重抓仍更新 `observed_at`。這不回填既有時間、不重建被覆寫歷史、不寫 DB；下一個自然來源批次需驗證相同列時間穩定、修訂列仍更新，後續 daily scan 再核收入/財務覆蓋恢復。
- `docs/source_extension.md` 已記錄設質脈絡的公式、限制、批次覆蓋與 timestamp 語義；host、scheduler、worker 對 `registry.py`、`market_signals.py` 通過 AST 解析與靜態 import。未跑測試、未手動觸發/重跑 DAG、未套 schema、未寫 DB、未發 Discord、未交易。Git 工作樹含大量既有無關變更，均保留。

### 2026-10-04 03:40 台北：來源通知區分 HTTP 200 與完整收集

- 針對 TPEx 日線報價失敗回執可能保留 HTTP 200、但 byte-range 收集仍未取得完整本文的情形，調整 `dags/source_extension/market_signals.py` partial-run 來源摘要：`IncompleteTPExQuoteResponseError` 顯示「回應本文不完整（HTTP 200）」；`ChunkedEncodingError` 顯示本文中斷；其他 2xx 失敗顯示後處理例外類別。一般例外只取型別，不把錯誤全文、網址或敏感內容帶進通知。
- 查詢仍唯讀；不改來源狀態、接受/隔離列、重試策略、訊號分數、候選、通知發送或交易。Host AST、scheduler/worker AST、worker 模組 import 均通過；沒有跑測試、手動觸發 DAG、寫 DB 或送 Discord。等待自然 partial run 核通知呈現與 `source_runs.http_status/error_message` 對應。
### 2026-10-04 03:54 台北：partial 通知補上成功來源的隔離列與有效日

- 重新唯讀核對 Airflow 與 crawler DB。最新 after-close 父 run 仍是 `scheduled__2026-10-01T12:00:00+00:00`、failed；mapped task 11 個來源中 TPEx 個股日線兩次 `ChunkedEncodingError`、0/0/0，TWSE 個股日線 1,051/1,051（effective 10/01），TWSE 融資融券 1,039/994/45，TWSE 財測達成 8/0/8（有效期末 6/30）。歷史 after-close source_runs 的 `dag_id` 仍為 NULL，不能用新 helper 假裝逐來源 lineage 已驗收；該批也早於 forecast 舊必填欄停用修正。
- 最新自然週末 source run `scheduled__2026-09-27T00:00:00+00:00`：TWSE 公司事件 70/70/0、有效日 10/02、53 個發行人；TWSE 月營收 1,086/1,086/0、有效月 2026/08；TWSE 財測差異 7/7/0、期末 2026/06/30、5 個發行人。TPEx 公司事件、月營收各兩次失敗且 0/0/0；舊回執 HTTP code 為 NULL。週末 trigger 建立的盤中快照為 10/03 `ready/partial`，10 張事件卡，但行情/方向資料家族為 0。
- 最新 daily snapshot 仍是 10/02 `ready/partial`：母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、除息 1,053、候選 50、內部通知數 0。候選上市 6、上櫃 44；這是候選分布，不是市場來源覆蓋率。成熟 outcomes 仍只有 9/24 的 signal-close/next-close 1-session，各 81 列，無成熟 5/10-session 樣本，不調門檻。
- 將 `market_signals.py::_source_collection_issues` 擴為 partial 通知也列成功 feed 中的隔離異常：顯示正規化/接受/隔離/重複列數，並按該來源執行時間窗查可用列的有效日期範圍與接受實體數；failed feed 也標明批次列數。只描述本次收集和正規化列，不把缺列/0 列解讀成零成交量。用 10/03 已保存的週末回執做唯讀 no-send 預覽，兩個 TPEx 失敗來源均列 0/0/0/0；成功且有隔離列的分支待新 lineage 自然 run 驗證。
- 通知 receipt migration 仍未自然套用：DB 最新 source schema migration 為 v8，`market_signal_notifications` 尚無平台 HTTP/message 欄；ledger 歷史 125 sent、32 failed 只是內部狀態，不能證明 Discord 收件。Outcome 樣本同樣仍只有 9/24。
- Host、scheduler、worker 對修改後 `market_signals.py` AST 解析及 scheduler/worker import 通過。未跑測試、未寫 DB、未手動觸發/重跑 DAG、未發 Discord、未交易；保留其他既有工作樹變更。成功隔離列分支與新通知 receipt 仍待自然批次驗收。
### 2026-10-04 04:13 台北：把工作範圍鎖定在可排程的全市場訊號

- 使用者再次校正：目標是持續批次抓取、串接、驗證並改善全市場訊號；不要把逐家公司找新聞/券商報告和人工評分當成日常工作。後續只把已確認可批次抓取且授權清楚的券商 feed/API 納入；目前 `source_registry` 有 47 個 approved/enabled 來源，查無券商批次來源列。這是已知來源缺口，不能以人工逐檔研究代替。
- 唯讀檢查自然 `source_extension_collect_process`：run `scheduled__2026-10-02T20:00:00+00:00`，台北 10/04 04:00 啟動；21/21 mapped sources 最後一次嘗試成功、HTTP 200。總計 normalized 13,259、accepted 13,254、quarantined 5、duplicates 0；5 筆隔離全在 `twse_quarterly_profitability`（1,054/1,049/5/0）。`tpex_quarterly_profitability` 和 `twse_dividends` 各在第二次嘗試成功。資料庫 schema migration v9 已於 04:00:01 台北自然套用。

| 自然批次 source_key | normalized / accepted / quarantine / duplicate | effective_date | 目前主檔對照 |
|---|---:|---|---|
| `cbc_fx_daily` | 3,660 / 3,660 / 0 / 0 | 2025-09-30..2026-09-30 | 15 個匯率實體；股票母體不適用 |
| `tpex_balance_sheet_bd` | 7 / 7 / 0 / 0 | 2026-06-30 | 7 個上櫃銀行類別代碼；類別 feed，不是全市場百分比 |
| `tpex_balance_sheet_ci` | 884 / 884 / 0 / 0 | 2026-06-30 | 884/892 上櫃普通股代碼（99.10%） |
| `tpex_dividends` | 2,483 / 2,483 / 0 / 0 | 2019-01-18..2021-08-03 | 793 個歷史事件實體：748 上櫃、9 上市、36 未匹配；不能當現行全市場覆蓋，資料已過舊 |
| `tpex_income_statement_bd` | 7 / 7 / 0 / 0 | 2026-06-30 | 7 個上櫃銀行類別代碼；非全市場百分比 |
| `tpex_income_statement_ci` | 884 / 884 / 0 / 0 | 2026-06-30 | 884/892 上櫃普通股代碼（99.10%） |
| `tpex_quarterly_profitability` | 884 / 884 / 0 / 0 | 2026-06-30 | 884/892 上櫃普通股代碼（99.10%）；第二次嘗試成功 |
| `twse_balance_sheet_basi` | 10 / 10 / 0 / 0 | 2026-06-30 | 10/10 主檔代碼；產業類別 feed |
| `twse_balance_sheet_bd` | 3 / 3 / 0 / 0 | 2026-06-30 | 3/3 主檔代碼；產業類別 feed |
| `twse_balance_sheet_ci` | 1,049 / 1,049 / 0 / 0 | 2026-06-30 | 1,018/1,054 上市普通股主檔代碼（96.58%）；31 個已接受創新板代碼不在目前普通股主檔，另有 5 個主檔代碼未出現在本批 |
| `twse_balance_sheet_fh` | 13 / 13 / 0 / 0 | 2026-06-30 | 13/13 主檔代碼；產業類別 feed |
| `twse_balance_sheet_ins` | 5 / 5 / 0 / 0 | 2026-06-30 | 5/5 主檔代碼；產業類別 feed |
| `twse_balance_sheet_mim` | 4 / 4 / 0 / 0 | 2026-06-30 | 4/4 主檔代碼；產業類別 feed |
| `twse_dividends` | 1,228 / 1,228 / 0 / 0 | 2026-01-30..2026-09-21 | 1,112 個歷史事件實體：1,053 上市、59 未匹配；事件列數不代表全市場公司覆蓋；第二次嘗試成功 |
| `twse_income_statement_basi` | 10 / 10 / 0 / 0 | 2026-06-30 | 10/10 主檔代碼；產業類別 feed |
| `twse_income_statement_bd` | 3 / 3 / 0 / 0 | 2026-06-30 | 3/3 主檔代碼；產業類別 feed |
| `twse_income_statement_ci` | 1,049 / 1,049 / 0 / 0 | 2026-06-30 | 1,018/1,054 上市普通股主檔代碼（96.58%）；31 個已接受創新板代碼不在目前普通股主檔，另有 5 個主檔代碼未出現在本批 |
| `twse_income_statement_fh` | 13 / 13 / 0 / 0 | 2026-06-30 | 13/13 主檔代碼；產業類別 feed |
| `twse_income_statement_ins` | 5 / 5 / 0 / 0 | 2026-06-30 | 5/5 主檔代碼；產業類別 feed |
| `twse_income_statement_mim` | 4 / 4 / 0 / 0 | 2026-06-30 | 4/4 主檔代碼；產業類別 feed |
| `twse_quarterly_profitability` | 1,054 / 1,049 / 5 / 0 | 2026-06-30 | 1,018/1,054 上市普通股主檔代碼（96.58%）；31 個已接受創新板代碼不在目前普通股主檔，另有 5 個主檔代碼未出現在本批 |

- 三組普通股財務 feed 的上市/上櫃分母來自目前證券主檔 CFICode `ES%`（上市 1,054、上櫃 892）。創新板 accepted rows 是批次官方資料，不因未匹配而刪除或視為零；尚未納入候選母體。只有補齊 PIT 證券主檔、可用歷史價格/財報/法人流和成熟 outcomes 後，才評估整批擴充，不逐家補洞。18 個季財務來源雖成功且有效日都是 2026-06-30，這表示資料新鮮度取決於季報週期，不是盤中訊號。
- 自然 run 後未觸發市場訊號掃描。唯讀 `market_signal_runs` 最新 daily 仍為 10/02 `ready/partial`（母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、除息 1,053、候選 50、內部通知 0）；最新 intraday 為 10/03 `ready/partial`（10 個公告候選，行情/方向來源計數為 0）。本次新季財務資料尚未進下一次掃描。
- `market_signal_outcomes` 仍只有 9/24 訊號日 horizon=1：signal-close 81 列（50 hit、30 miss、1 unresolved；平均超額報酬 +1.776 個百分點），next-close 81 列（37 hit、43 miss、1 unresolved；+0.987 個百分點）。單一訊號日、無成熟 5/10-session、成本及公司行動調整樣本，不用來調門檻或宣稱策略有效。
- After-close 最新自然 parent `scheduled__2026-10-01T12:00:00+00:00` 仍 failed；daily capture 子任務成功但通知子任務失敗。v9 平台回執欄位已自然上線，但既有 ledger 125 sent、32 failed 的 receipt ID/HTTP 欄仍全為空；10/04 04:00 source parent 沒有啟動 market-signal notifier，因此尚無新平台收件驗證。下一個 after-close 自然排程預計 10/05 台北 20:00。
- 新聞最新自然成功批次仍是 logical 10/03 18:30 台北、實際 23:30–23:39；12 個來源共 1,281 parsed、1,238 valid-time、1,220 accepted。自由時報 84/100、鉅亨 148/150 部分接受；公視 43 parsed、0 valid-time/accepted；TVBS、新頭殼、三立無 URLs；其餘列為完整的來源仍須以各自抓取回執判斷。這批早於新版時間 parser，午夜日期值不等於已核原頁發布時分或時區；下一自然新聞批次再驗時間與來源覆蓋。
- 可自動新增的研究缺口依序是：可合法授權的券商批次 feed/API（目前 registry 無來源列）；TWSE TWTB5U 當沖個股統計的授權/批次接入；TPEx 個股當沖 endpoint 的授權與資料可用性。TPEx OAS 11627 與官方當沖市場彙總只作帶 freshness gate 的市場背景，不填成個股訊號。禁止把個股網站查詢路徑或未匹配代碼接入訊號，也不把缺列當 0 量。
- 本輪僅做自然 Airflow/資料庫唯讀審核並更新本追蹤文件與 ad-hoc memory。未改程式、未跑測試、未手動觸發/重跑 DAG、未寫入 DB、未送 Discord、未交易；保留所有無關工作樹變更。

### 2026-10-04 04:34 台北：加入上市櫃官方每日估值批次脈絡

- 為增加可自動排程的全市場資料，直接唯讀預檢交易所 OpenAPI 的整批當日本益比/殖利率/股價淨值比，不以券商逐檔研究取代。TWSE `BWIBBU_ALL` HTTP 200 回 1,082 列、1,082 代碼，資料日期全為 1151002（2026-10-02）；對目前上市普通股主檔 1,054 代碼匹配 1,051，另 31 個代碼在普通股主檔外、3 個主檔代碼未在本批。P/E 有值 870、殖利率 847、P/B 1,082，空值維持缺值。來源登記於政府開放資料集 11547、授權政府資料開放授權條款第1版。
- TPEx `tpex_mainboard_peratio_analysis` HTTP 200 回 887 列、887 個不重複代碼，資料日期全為 1151002；匹配上櫃普通股主檔 887/892，主檔內另 5 個代碼未出現在本批。P/E 有值 672、殖利率 887、P/B 887；殖利率 276 列為來源明列 0，沒有把空值轉成 0。資料集 11373 的說明稱每日收盤更新、欄位含每股股利與殖利率，但詮釋頁更新頻率欄標示每月；記錄這項 metadata 差異，執行時以 API `Date` 核 freshness。TPEx 每股股利欄保留來源欄位，不拿它推算上市櫃共用殖利率。
- 已在 `adapters.py` 加入兩個官方全市場 endpoint 契約與 `valuation_metrics_daily` 正規化，保留完整原始列、effective date 與空值；在 `registry.py` 加入每日收盤批次來源 `twse_valuation_metrics_daily`/`tpex_valuation_metrics_daily`，明示政府開放資料授權；在 `market_signals.py` 以代碼/市場/有效日做 PIT join，只有 valuation effective date 等於候選的價格資料日且記錄當時已可得才附在卡片。估值只列為官方背景，不計方向家族、不改候選門檻、分數或排序；本益比極端值或來源缺值不會變成方向票。兩個來源自動啟用分數皆 96.25；目前 `daily_after_close` 定義共 17 個，低於 32 個映射來源上限。
- 兩來源目前僅在程式定義，尚未寫入 DB `source_registry` 或落庫。預計 10/05 04:00 的自然 schema bootstrap 登錄，10/05 20:00 after-close 才是首批收集驗收：逐來源核 HTTP、有效日期、raw/accepted/quarantined/duplicate、上市/上櫃覆蓋、候選卡同日脈絡。若資料日落後於價格日，該候選不顯示估值欄，不能沿用舊日估值冒充當日。
- 這兩個每日估值 feed 是不同資料粒度，不能補回 `tpex_dividends` 2019–2021 的歷史股利分派情形；舊 feed 過時問題仍列來源缺口。新增估值歷史可用於未來累積 PIT 相對估值分位，但需要跨期資料及成熟 outcomes，現在不設便宜/昂貴門檻。
- `docs/source_extension.md` 已補來源目的、授權與同日 PIT/非計分語義。Airflow scheduler 對三個修改模組通過 AST parsing/import；兩個 source definitions 的 allowlisted endpoint contract 與 daily_after_close schedule 驗證通過。沒有跑測試、觸發 DAG、寫 DB、發 Discord 或交易；保留既有無關工作樹變更。

### 2026-10-04 04:44 台北：再次確認自動化訊號範圍與自然排程基線

- 使用者再次明確校正：要的是可合法授權、可排程、可批次抓取並可持續驗收的全市場訊號，不是逐家公司線上找券商報告、人工評比或彙總。後續工作只沿來源 registry、自然 run、有效日、上市/上櫃覆蓋、隔離率、PIT join、成熟 outcomes 與通知平台回執推進；券商研究只有在確認授權且存在批次 API/feed 時納入。當前 registry 查無券商批次來源。
- 唯讀 Airflow/DB 複核：最新 daily snapshot 仍是 10/02 `ready/partial`，母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、除息 1,053、50 候選、內部通知數 0。最新 intraday 為 10/03 `ready/partial`，10 個候選；行情、法人、營收、財務、借券、董監與除息來源計數皆為 0，是事件型資料而非盤中行情訊號。
- `market_signal_outcomes` 仍僅 9/24 一個成熟訊號日：horizon=1 的 `signal_close` 81 列（50 hit、30 miss、1 未分類；平均超額報酬 +1.776pp），`next_close` 81 列（37 hit、43 miss、1 未分類；+0.987pp）；沒有成熟 5/10-session 組，不以單日樣本改分數或宣稱預測力。
- 最新 after-close parent `scheduled__2026-10-01T12:00:00+00:00` 仍為 failed；11 個來源 map 中 10 個成功、1 個（map index 9）第二次嘗試 failed；`verify_collection_and_triggers` 第二次嘗試亦 failed，`trigger_market_signal_notify` 任務為 success 不等於摘要已被 Discord 收件。此 metadata 是 10/02 20:00 台北實際 run；檢查時尚無更新的 after-close parent。
- 新聞最新 Airflow logical run 為 10/03 18:30 台北，實際 23:30–23:39 成功；沿用其來源回執：12 feed、1,281 parsed、1,238 valid-time、1,220 accepted；公視仍無有效時間列，TVBS/新頭殼/三立當批無 URLs，自由與鉅亨未全數接受。最新 10,000 個新聞 ID 的唯讀精度摘要中，iThome 133/133、The News Lens 110/110 的 `created_at` 為午夜日期精度；公視最近保存列日期僅至 10/01。新聞維持匹配背景，不因標題/首段或多站同稿提升方向分數。
- 10/04 04:34 加入的 TWSE/TPEx 官方每日估值批次來源在目前資料庫 `source_registry` 尚無列、尚無任何 source_runs；最新每日來源批次實際於 10/04 04:00 台北啟動，早於此程式定義變更。待下個自然 after-close 週一 10/05 批次再驗來源登錄、HTTP、有效日、raw/accepted/quarantined/duplicate、兩市場主檔覆蓋及同日 PIT 卡片脈絡；估值只作背景，不計方向家族。
- 本次只有唯讀 Airflow/資料庫檢視與新聞精度摘要；未改程式/分數/門檻、未跑測試、未手動觸發或重跑 DAG、未寫 DB、未送 Discord、未交易；保留全部既有無關變更。

### 2026-10-04 04:56 台北：官方全市場 XBRL 季報批次檔初步連線檢查（後續全檔結果見 05:03）

- 為尋找可自動化且可量化的現金流量品質資料，直接唯讀抓 TWSE 與 TPEx 官方 Swagger JSON：HTTP 200，分別 143 與 225 個 paths，cash-flow/現金流量關鍵字均無命中。這只表示 JSON API 目錄沒有該個股現金流量路由，不能推廣成官方沒有批次財報檔。
- 改查 MOPS 官方 XBRL「案例文件整批下載」頁：`t203sb02` 以季為單位，TWSE 2024 使用手冊說選擇季別後可下載該季所有公司資料；手冊稱平台涵蓋上市、上櫃、興櫃及金控子公司，資料自 2009 Q4。對官方 2026 Q2 ZIP 下載端點僅做串流前 128 bytes 的唯讀預檢，回 HTTP 200、`application/x-zip-compressed` 及 ZIP signature；未下載完整檔、未保存或解析。
- 這是有潛力提供營業現金流、資本支出及自由現金流的批次來源候選，符合跨市場整批資料方向，且不是逐家公司查詢。但資料集未在 data.gov.tw 查到對應開放授權，公開網站按季下載不等於已證明可排程重複擷取或再散布。TWSE 另列有 IFRS XBRL 主動派送套裝 S21，NT$40,000/月；這是獨立的付費推送服務，不據此推定公開下載頁也收同一費用。
- 未把候選接進 registry。正式登錄前需核官方使用權/自動擷取限制、ZIP 大小與串流完整性、上市櫃當季公司覆蓋、XBRL taxonomy/合併與個別報表單位、申報/更正時間及版本沿革，並先做全市場解析與 PIT 檢查。不能從財報期末日推成當時已知，也不能只解析少數公司代替覆蓋驗收。
- 已更新 `docs/source_extension.md`，將原本「API catalog 未找到」明確限縮在 JSON 路由，並記錄此批次檔候選與授權驗收門檻。未下載整檔、未寫 DB、未新增來源定義、未手動觸發 DAG、未發通知或交易。

### 2026-10-04 05:03 台北：完成官方 XBRL 全市場季度批次覆蓋預檢

- 將前一筆「只看 ZIP 前 128 bytes」的候選預檢更新為全檔唯讀掃描。官方 2026 Q2 壓縮檔 111,404,708 bytes，含 2,703 個 HTML 文件，全部含 `xbrli:context`；2,620 個檔名符合季報尾碼格式，代表 2,609 個四位代碼。暫存檔解析後已刪除，未入庫。
- 目前普通股主檔是上市 1,054、上櫃 892。抽到營業現金流概念的文件代碼覆蓋 1,053 上市、891 上櫃；投資現金流 1,053/890；籌資現金流 1,051/888。少數文件沒有主要現金流概念；archive 另有超出普通股名冊的代碼，未匹配列不當成零值。每碼多份文件，合併/個別與最終報告選擇規則待定。
- 掃描指定申報時間欄位名稱沒有命中；抽取的 XBRL context/date facts 範圍 2024-04-01 至 2027-08-06，不能當申報發布時間。故批次覆蓋很有潛力，但沒有已證的可得時間、修正版本沿革、XBRL taxonomy/單位/期間規則，也沒有確認公開頁面允許定期自動擷取/再利用；現在不得做 PIT 訊號或接入排序。
- 官方 E-Shop 另列 XBRL 推送套裝 NT$40,000/月，與公開季度下載頁是不同服務；沒有購買或推定公共下載路徑有同等授權。官方 OpenAPI catalog 僅能說沒有列出現金流 JSON 路由，不能因此忽略此季度 archive。
- 既有操作基線距此輪僅約 20 分鐘：daily 10/02、intraday 10/03 仍 `ready/partial`；outcomes 僅 9/24 horizon-1 成熟結果；after-close 最新 10/02 20:00 parent failed；新聞最新完整批次 logical 10/03 18:30 台北（1,281 parsed/1,238 valid-time/1,220 accepted），尚待下一自然批次。故沒有新候選訊號進卡片，沒有理由調門檻。這些執行資料均不是本輪新驗證。
- 沒有程式/registry 或資料庫變更；未觸發 DAG、未發 Discord、未交易；僅更新追蹤文件與 ad-hoc memory note，保留所有既有無關工作樹變更。

## 2026-10-04 05:10 台北：TEJ 全市場 PIT/當沖與券商共識批次候選

- 按全市場自動化來源查 TEJ 一手資料。現行 TQuant 公開目錄列 `TWN/APISHRACT`：台灣上市、上櫃及下市股票每日資料，涵蓋三大法人、融資券、借券、當沖與每週集保；當沖欄 `vol_dt` 是當沖成交股數（千股），`vol_dtp` 是當沖量占成交量比例，標示資料源 TWSE/TPEx、當沖/借券更新約 21:30。目錄總體聲稱提供 PIT 資料與歷史下市樣本，但尚未證實此表的版本/申報時間欄、回修保存、逐代碼覆蓋或專案授權。可作完整歷史樣本/當沖參與度候選，當沖比例先作投機/注意度背景，不能直接當多空方向票。[TEJ TQuant 資料目錄](https://tquant.tejwin.com/%E8%B3%87%E6%96%99%E9%9B%86/)
- TEJ REST 文件支持用 API key 取 datatable JSON/XML；E-Shop 說資料庫分免費/付費，試用 key 需向 TEJ 申請。未確認 `TWN/APISHRACT` 是否含在可用 API 方案、此專案 entitlement/儲存與展示權，也未呼叫需 key 的 API。registry 無 TEJ feed，不作接入或費用承諾。[REST 文件](https://api.tej.com.tw/document_rest.html)、[API 資料服務](https://eshop.tej.com.tw/E-Shop/Edata_api)
- 券商共識只列為未來「機器可讀、可授權的全市場批次 feed」候選：需具逐分析師/券商/期間欄位、首發與修訂時間戳、歷史修訂留存、全市場覆蓋及自動化儲存/展示權；未確認前不接入。券商文章、摘要和逐檔報告不算本工作可抓取訊號，也不做人工逐份評比。
- `TWN/APISHRACT` 若取得有效授權，優先評估與現有已收集法人/融資券/借券欄位的差異及退市歷史完整性，避免重複存儲；只有原始欄位與時間語義清楚、自然批次 receipts/市場覆蓋經驗收後才寫入 registry。沒有 TEJ API 呼叫、程式/DB 變更、DAG 觸發、Discord 發送或交易。

## 2026-10-04 05:12 台北：自然批次、訊號與通知回執複核

- 唯讀刷新本地 crawler/stock DB 與 Airflow metadata。最新 `source_extension_collect_process` 父 run 是 `scheduled__2026-10-02T20:00:00+00:00`，Airflow success，實際執行 10/04 04:00–04:02 台北；21 個不同 source keys 最終均成功（共 23 筆 task attempts；`twse_dividends` 與 `tpex_quarterly_profitability` 各有一次首試失敗後重試；兩個最終嘗試都是 HTTP 200）。
- 每個成功來源均對照本批正規化列數/接受/拒絕/重複數 與已存 `source_records` 的最新 effective date/普通股主檔匹配。18 個上市/上櫃產業季財報 feeds 的有效日期均 2026-06-30；一般產業資料如 TWSE income/BS CI 1,049 rows 接受、匹配上市主檔 1,018/1,054，TPEx CI 884 rows 接受、匹配上櫃 884/892。TWSE quarterly profitability 1,054/1,049/5 reject，主檔上市匹配 1,018；TPEx 884/884/0，匹配 884 上櫃。sector-specific 小 feed 不用其列數冒充普通股全覆蓋。TWSE 股利 feed row 1,228/1,228，最新 event effective date 2026-09-21；TPEx 股利 feed 2,483/2,483，但當前保存資料最新 effective date 2021-08-03，仍是明顯資料缺口。CBC 匯率 3,660/3,660/0，最新有效日 2026-09-30（宏觀序列，不是個股覆蓋）。


| 最終 source key | HTTP | normalized rows/accepted/rejected/duplicate | 本批有效日範圍 | 本批 entity 對目前普通股主檔匹配（上市/上櫃） |
|---|---:|---:|---|---:|
| `cbc_fx_daily` | 200 | 3,660/3,660/0/0 | 2025-09-30–2026-09-30 | 0/0（15 個總經 entity） |
| `tpex_balance_sheet_bd` | 200 | 7/7/0/0 | 2026-06-30 | 0/7 |
| `tpex_balance_sheet_ci` | 200 | 884/884/0/0 | 2026-06-30 | 0/884 |
| `tpex_dividends` | 200 | 2,483/2,483/0/0 | 2019-01-18–2021-08-03 | 9/748（歷史股利事件，不是母體覆蓋） |
| `tpex_income_statement_bd` | 200 | 7/7/0/0 | 2026-06-30 | 0/7 |
| `tpex_income_statement_ci` | 200 | 884/884/0/0 | 2026-06-30 | 0/884 |
| `tpex_quarterly_profitability` | 200 | 884/884/0/0 | 2026-06-30 | 0/884 |
| `twse_balance_sheet_basi` | 200 | 10/10/0/0 | 2026-06-30 | 10/0 |
| `twse_balance_sheet_bd` | 200 | 3/3/0/0 | 2026-06-30 | 3/0 |
| `twse_balance_sheet_ci` | 200 | 1,049/1,049/0/0 | 2026-06-30 | 1,018/0 |
| `twse_balance_sheet_fh` | 200 | 13/13/0/0 | 2026-06-30 | 13/0 |
| `twse_balance_sheet_ins` | 200 | 5/5/0/0 | 2026-06-30 | 5/0 |
| `twse_balance_sheet_mim` | 200 | 4/4/0/0 | 2026-06-30 | 4/0 |
| `twse_dividends` | 200 | 1,228/1,228/0/0 | 2026-01-30–2026-09-21 | 1,053/0（事件列，不是母體覆蓋） |
| `twse_income_statement_basi` | 200 | 10/10/0/0 | 2026-06-30 | 10/0 |
| `twse_income_statement_bd` | 200 | 3/3/0/0 | 2026-06-30 | 3/0 |
| `twse_income_statement_ci` | 200 | 1,049/1,049/0/0 | 2026-06-30 | 1,018/0 |
| `twse_income_statement_fh` | 200 | 13/13/0/0 | 2026-06-30 | 13/0 |
| `twse_income_statement_ins` | 200 | 5/5/0/0 | 2026-06-30 | 5/0 |
| `twse_income_statement_mim` | 200 | 4/4/0/0 | 2026-06-30 | 4/0 |
| `twse_quarterly_profitability` | 200 | 1,054/1,049/5/0 | 2026-06-30 | 1,018/0 |

上表 `row_count` 是正規化後、去重前的列數；accepted/rejected 以去重列計算，duplicate 是去重剔除數。「主檔匹配」是有效列中代碼可匹配目前普通股名冊的數量，不把缺列或未匹配列當零。財報 CI 主檔分母為上市 1,054、上櫃 892：TWSE CI feeds 接受列中 1,018 匹配、31 個 accepted code 在目前主檔外、5 個主檔代碼未匹配；TPEx CI feeds 884/892。按產業篩選的 `basi/bd/fh/ins/mim` 列數是該業資料，不能充當全市場母體覆蓋。已在 DB 登記的價值估值來源仍無列，因本次自然 daily run 發生在新定義加入之前；待後續自然 bootstrap 驗收，不手動觸發。
- 最新 `market_signal_runs`：daily 10/02 `ready/partial`，母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、除息 1,053、50 候選、notified_count 0；intraday 10/03 `ready/partial`，10 個事件候選、方向家族資料計數均 0。成熟 outcomes 仍僅 9/24、horizon=1：signal-close 81 rows/50 hit/30 miss/1 unresolved，平均超額 +1.776pp；next-close 81/37/43/1、+0.987pp。無成熟 5/10-session evidence，不調門檻。
- Airflow after-close 最新仍 `scheduled__2026-10-01T12:00:00+00:00` failed（10/02 約 20:00 台北）；沒有更新的 daily signal child。`market_signal_notifications` ledger 目前仍 125 sent/32 failed，但所有 row 的 `platform_receipt_id` 和 HTTP status 都是 NULL；v9 receipt code 部署後尚無自然 notifier run 證明平台收件，內部 `sent` 不當成 receipt。
- 新聞 `news_ch_collect_process` 截至 05:12 尚無 10/04 run。最新成功仍為 10/03 logical 18:30 台北，實際 23:30–23:39，12 feed、1,281 parsed/1,238 valid-time/1,220 accepted；各來源發稿精度與覆蓋沿用前次記錄，日期欄午夜精度不推成時分。Airflow 設定 timezone 為 UTC；cron `30 0,2,4,6,8,10,15,21` 的 21:30Z 相當於台北 05:30，尚未看到該次自然 run，不人工觸發。
- 本次只做 Airflow/DB SELECT、run list 與來源覆蓋 join，沒有 DB 寫入、DAG 觸發、通知、交易或測試。維持 TEJ `TWN/APISHRACT` 和 MOPS XBRL 為待授權/時間語義驗證候選；未把來源標題或成功狀態冒充新訊號。


## 2026-10-04 05:45 台北：05:30 新聞自然批次回執與覆蓋

- 使用者更正後，本工作聚焦可排程、機器可讀、可覆蓋全市場的批次訊號；不再把券商逐份報告／公開摘要的人工瀏覽評比當訊號收集。券商資料只有在有授權的全市場 feed、PIT 首發/修訂時間和可檢驗歷史時才列候選。
- `news_ch_collect_process` 自然 run `scheduled__2026-10-03T15:30:00+00:00` 在 05:30:00–05:41:04 台北成功。`collect_article_url` 05:30:01–05:30:32、`collect_news` 05:30:34–05:40:52、`delete_and_insert` 05:40:54–05:41:03 均 success，沒有手動觸發。
- run 後唯讀 `news_ch`：`article_url` 有 1,364 列、12 個來源；`news_temp` 有 1,346 列、12 個來源。18 列 URL 沒有成為接受文章；中間 parsed／valid-time／隔離原因未持久化為可查來源表，本輪不臆分 18 列的失敗階段。9/12 feeds URL 數與接受數一致；三個 partial feed 是三立 30/26、自由時報 100/88、鉅亨網 150/148。

| 新聞來源 | URL/接受列 | 48 小時內有時分 | 日期晚於 cutoff 的日期級列 | cutoff 日期時間不明 | 較舊列 |
|---|---:|---:|---:|---:|---:|
| ETtoday | 490/490 | 489 | 1 | 0 | 0 |
| iThome | 60/60 | 0 | 2 | 20 | 38 |
| newtalk | 25/25 | 25 | 0 | 0 | 0 |
| thenewslens | 90/90 | 0 | 2 | 6 | 82 |
| TVBS | 16/16 | 16 | 0 | 0 | 0 |
| 三立新聞 | 30/26 | 26 | 0 | 0 | 0 |
| 中央社 | 100/100 | 100 | 0 | 0 | 0 |
| 中時日報 | 200/200 | 200 | 0 | 0 | 0 |
| 公視新聞 | 43/43 | 43 | 0 | 0 | 0 |
| 聯合報 | 60/60 | 59 | 1 | 0 | 0 |
| 自由時報 | 100/88 | 88 | 0 | 0 | 0 |
| 鉅亨網 | 150/148 | 148 | 0 | 0 | 0 |
| **合計** | **1,364/1,346** | **1,194** | **6** | **26** | **120** |

- freshness 以 05:45 台北資料庫時間計，48 小時 cutoff 為 10/02 05:45。按來源欄位的原始時間精度，1,194 列有可判讀時分且落在窗內，另 6 列只有日期且晚於 cutoff 日期；26 列日期戳落在 cutoff 當天、無法判斷時分；120 列日期級時間較舊。合計 1,200/1,346 可按時間或日期判為新近，26 列保留未知，120 列不新近。iThome/thenewslens 的日期級內容仍以 source coverage/freshness 背景處理，不拿午夜日期冒充發稿時刻。
- 新聞批次成功不等於形成股票訊號。現有 `market_signals._attach_recent_news_context` 會在訊號掃描時，以公司名/代碼比對近 48 小時標題與內文前 320 字，最多附 2 篇作非方向背景；新聞 DAG 本身不觸發訊號掃描，所以本次新文章尚未出現在訊號卡。最新日線仍 10/02 `ready/partial`（50 候選），最新盤中快照 10/03 是 10 張事件卡、價格/方向家族資料數 0；成熟 outcomes 仍只有 9/24 horizon=1、signal-close/next-close 各 81 列，無成熟 5/10-session 樣本。`source_extension_collect_after_close` 最近 run 仍是 10/02 約 20:00 台北失敗；週末不手動補跑，等下一個自然平日 run。
- Runtime 部分只讀 Airflow metadata 與 crawler/news DB；沒有 DB 寫入、手動 DAG 觸發、Discord 發送或交易。另因 per-source parsed/valid-time/quarantine 只印 log、沒有查詢回執，已在 `all_news.collect_news` 加入 content-free XCom 摘要（總量、逐來源接受/隔離與 freshness），並在 PythonOperator 明確啟用 `do_xcom_push`；只做靜態語法驗證，尚待下一個自然新聞 run 確認 XCom 回執，不把此次既有成功 run 當新程式驗收。

## 2026-10-04 06:00 台北：自然 run 驗收門檻刷新

- 重新對照 Airflow 與 crawler `source_runs`：after-close 最新仍是 `scheduled__2026-10-01T12:00:00+00:00`，10/02 20:00 台北執行；TPEx 行情 map index 9 兩次 `ChunkedEncodingError`，分別只讀到 704,105 與 4,193,897 bytes（宣告總長 4,666,641），兩次都 0/0/0；mapped source 失敗後 `verify_collection_and_triggers` 失敗。`source_runs` 舊列 `dag_id/http_status` 為 NULL，保留歷史原貌。
- 根因句：TPEx 全市場行情端點在回應本文尚未收完時中斷，舊版單次 body 讀取無法續傳，導致上櫃行情 source run 失敗並讓 verifier fail。現行 `adapters._fetch_tpex_equity_quotes_payload` 已加入最多 6 次 identity byte-range 續傳及 `Content-Range`/總長驗證，`IncompleteTPExQuoteResponseError` 也會把最後 HTTP 狀態傳到 `source_runs`；然而 10/02 失敗發生在此修正前。10/04 的單次完整 HTTP 200 預檢不是 Airflow 落庫回執，未覆蓋 Range 續傳分支；修補仍等 10/05 平日 20:00 after-close 自然 run 驗證。
- 重新查新聞：最新成功 run 仍是 `scheduled__2026-10-03T15:30:00+00:00`，早於 XCom 修改；其 `collect_news` `return_value` 為空，不能據此判新程式失敗。Airflow 目前可載入 `news_ch_collect_process`，`collect_news.do_xcom_push=True` 且該 DAG 無 import error；下一個自然新聞時段再核逐來源摘要是否進 XCom，不手動觸發。
- 市場訊號仍是 10/02 daily `ready/partial`、50 候選與 10/03 weekend intraday event-only 10 張卡；`market_signal_outcomes` 仍只有 9/24、horizon 1 的 162 列，沒有成熟 5/10-session 樣本。無新成交證據，不調訊號門檻或把新聞單獨轉成方向票。
- 本輪只讀 Airflow/DB 狀態與 DagBag parse；未觸發 DAG、未改 DB、未發 Discord、未交易、未跑測試。保留其他工作樹變更。
## 2026-10-04 06:08 台北：使用者再次校正工作重心為自動化訊號管線

- 使用者指出近期工作偏向線上逐家查報告／彙總，偏離需求。目標是持續改善 DataFlow 的全市場自動訊號收集、整合與通知；日常優先做自然排程巡檢、批次來源故障修復、有效日與上市/上櫃覆蓋驗收、PIT 關聯、成熟 outcomes 與通知回執。券商報告只有存在明確授權、可機器批次取用的全市場 feed/API 才列入接入評估；逐家公司人工評比不是工作流。
- 本次唯讀盤點：source_registry 有 47 個 approved/enabled 來源；source_extension_collect_process 最近自然批次於 10/04 04:00–04:02 台北成功。任務 success 仍逐來源核對有效日、accepted/rejected/duplicate 與兩市場覆蓋，不能當作全市場完整。
- 最新 daily 訊號快照仍為 10/02 ready/partial、50 候選；intraday 為 10/03 ready/partial，10 張事件卡但無盤中價格/方向族資料。market_signal_outcomes 仍只有 9/24 的 horizon-1 結果，沒有成熟 5/10-session 樣本。after-close 最近自然 parent 仍為 10/02 台北失敗；新版 TPEx 續傳與通知回執需等下一個平日自然排程驗收。這些才是現有自動化訊號品質的直接缺口。
- 後續按自動化可行性排序：先處理已接來源的自然 run 缺口和驗收，再評估能補充既有訊號且授權/批次契約清楚的全市場 feed；保留新聞來源事實、分析和未知事項分層。未有新跨期證據前不因單一日期調分數或門檻。
- 本輪唯讀檢查並更新追蹤文件與 ad-hoc memory；未改程式/DB、未手動觸發或重跑 DAG、未發通知、未交易；保留所有無關既有工作樹變更。

## 2026-10-04 06:17 台北：outcome 成熟日按交易所休市日重算

- 更正本輪先前口頭把 2026-09-25、2026-09-28 稱作缺漏交易日的判斷：TWSE 115 年市場開休市表列 9/25 中秋節、9/28 教師節休市；TPEx 115 年 9 月 17 日公告同樣明載上櫃市場這兩日休市。專案 holiday.taiwan 同兩日期也標記「是」，資料列於 10/01 建立。官方證據：[TWSE 115 年市場開休市日期](https://www.twse.com.tw/holidaySchedule/holidaySchedule?response=html)、[TPEx 休市公告](https://www.tpex.org.tw/storage/eb_data/11509/11503027221.html)。因此 9/25、9/28 無價量列屬正常休市，不能記成收集缺口。
- stock.個股成交資訊及官方指數 source_records 在 9/24 後依序有 9/29、9/30、10/01、10/02 四個市場交易日。程式 OUTCOME_HORIZONS_SESSIONS=(1,5,10) 以實際收盤日序列計算；9/24 訊號的 signal_close 五日退出日為 10/05，next_close（9/29 後收盤進場）五日退出日為 10/06。下一個自然日線掃描及資料完整性決定何時可結算，先不調結算門檻。
- 另發現 10/02 收盤來源執行呈現跨市場指數 freshness 不一致：TWSE 指數 source_run HTTP 200、1 row，effective date 仍為 10/01；TPEx 指數 source_run HTTP 200、2 rows，最新 effective date 為 10/02。成功狀態不表示兩市場指數同日完整；結算候選需等 TWSE 基準指數到齊。列為下一個自然 after-close 的驗收項。
- 下一步以 10/05 收盤自然 run 核：兩市場行情與指數的有效日、raw/accepted/rejected、上市/上櫃覆蓋；9/24 訊號能否生成 signal_close 五日及 next_close 五日 outcomes；daily digest 的實際平台回執。未手動補跑、未寫 DB、未送 Discord 或交易。

## 2026-10-04 06:19 台北：交易所指數端點晚間更新差異唯讀複核

- 10/02 after-close 已落庫的 TWSE 指數 source_run 是 HTTP 200、1 列、effective date 10/01；同批 TPEx 指數為 HTTP 200、2 列、最新 effective date 10/02。此為當次來源執行的延遲/覆蓋差異，不應只看 HTTP success。
- 06:19 台北直接唯讀查詢官方 allowlisted endpoint（未落庫、不是 source_run）：TWSE MI_5MINS_HIST 回 HTTP 200、2 列、有效日期 1151001/1151002；TPEx tpex_index 回 HTTP 200、2 列、有效日期 20261001/20261002。代表 TWSE 的 10/02 指數資料已在之後發布；前次落庫較舊目前看來是來源發佈時序差，不足以支持改結算公式或排程。
- 下一個 10/05 after-close 自然收集再驗 TWSE/TPEx 原始列、接受列、各自有效日和上市/上櫃行情覆蓋；只有實際 source_run/source_records 同日資料可作收集成功證據。9/24 訊號 signal-close 五日結果預期在 10/05 收盤後成熟；next-close 五日結果預期在 10/06 收盤後成熟，需基準指數同日列齊全。

## 2026-10-04 06:22 台北：TPEx 週末 530 服務端失敗加有界重試

- 最新已完成的 source_extension_collect_weekend 為 Oct 3 08:00 台北自然 run，parent failed。兩個 TPEx OpenAPI source（公司事件、月營收）各在兩次 Airflow 嘗試均回 HTTP 530；TWSE 同批公司事件 70/70 接受、月營收 1,086/1,086 接受。兩個 TPEx 來源均 0/0/0，兩次錯誤時間約相隔 1 秒，HTTP status 欄位舊列為 NULL，但錯誤訊息保留 530。
- 10/04 06:21 台北直接唯讀請求同兩個 TPEx 官方 OpenAPI 現回 HTTP 200：公司事件 36 rows、月營收 892 rows。此為端點預檢，沒有落庫，不當成自然收集驗收；支持這次是暫時服務端失敗而非固定 schema mismatch。
- 在 adapters 共用 JSON 抓取器加入只針對 HTTP 500–599 的最多 3 次請求（1 秒、2 秒退避）。最終錯誤明確帶 http_status，供既有 source_runs catch 優先記錄；4xx、payload 大小、JSON 解析、正規化、接受/隔離與去重規則不變。HTTP 530 被重試，但最終耗盡仍 fail closed。
- 靜態驗證：host AST parse、Airflow scheduler import、worker import 均通過。沒有跑測試、手動觸發 DAG、寫 DB 或發 Discord。下一個週末自然來源 run 預計 10/04 08:00 台北，可驗收兩 TPEx feeds 是否恢復及有效日/接受/隔離/上市櫃匹配；新聞下一自然時段約 08:30；valuation sources 尚未進 DB registry，待 10/05 04:00 daily bootstrap；after-close 下一自然時段 10/05 20:00。

## 2026-10-04 09:06 台北：自動化批次採用、新聞脈絡與自然回執

- 使用者再次明確校正：工作目標是可排程、可批次擷取的全市場訊號；逐家公司手動調查券商報告或人工評分不符合需求。券商訊號只在有授權且機器可批次取得的全市場 feed/API 存在時才列入來源候選。目前 registry 沒有券商批次來源；券商摘要與單篇報告不拿來填自動訊號缺口。
- `source_extension_collect_process` 10/04 04:00–04:02 自然 run 成功，21 個來源映射皆為最終 success。季度財報最新期間 2026-06-30；TWSE CI 損益及資產負債列接受 1,049/1,049、目前上市主檔匹配 1,018，31 個 accepted code 在目前普通股主檔外；TPEx CI 財報 884/884、上櫃主檔匹配 884。這些是 CI 子群來源，不當成市場整體母體覆蓋。TWSE 季度獲利率另有 5/1,054 隔離列。CBC 匯率 3,660 列、15 個貨幣/總經 entity，最新有效日 9/30；它是總經背景，沒有股票主檔配對。
- 10/04 04:00 daily run 的 TPEx 股利仍是舊批次資料，2,483 列接受但最新輸出日僅 2021-08-04。08:00 自然 schema bootstrap 後，DB registry 已指向官方 dataset 58285 的替代 CSV；本次 04:00 run 早於該更新，不能當替代 feed 的驗收。兩個估值來源也已在 08:00 registry 啟用，但目前沒有 `source_runs` 列，待 10/05 收盤自然批次驗有效日與上市/上櫃匹配。
- 10/04 08:00 `source_extension_collect_weekend` 自然 run parent failed。該次 bootstrap 早於 08:40 左右的 TPEx 事件/營收 JSON 程式契約更新，DB registry 到 09:06 仍是舊 MOPS CSV URL；公司事件與月營收各經兩次 60 秒讀取逾時，最終 0 raw/accepted/quarantined。當批 TWSE 公司事件 5/5 接受，其中 3 碼匹配目前上市普通股主檔；另 2 碼不在主檔，保留為未匹配，不能改成零或視作覆蓋。TWSE 月營收 1,086/1,086 接受、1,054 碼匹配上市主檔，最新報告月 2026-08；forecast variance 7 列/5 個上市代碼接受。兩市場已轉讓董監資料各 1/1 接受且各匹配本市場主檔；未轉讓端點各回日期 sentinel 1 列、0 接受/1 隔離，不解讀成無未轉讓資料。
- 本次週末盤中子 run `market_signal__intraday__source_extension_collect_weekend__scheduled__2026-10-04T00:00:00+00:00` 成功，market snapshot 為 10/04 `ready/partial`、5 候選、沒有個股行情/方向家族資料。5 張候選都有新聞欄：2 張找到 3 篇背景、來自 2 個新聞來源；另 3 張在抽樣內無匹配。這些報導時間來自 `news.created_at`，不是文章頁時間/時區複核，仍是非方向性背景。該自然 notifier ledger 有 6 列 sent，6 列均有平台 receipt id，平台 HTTP 狀態均為 200；這是 Airflow 自然排程產生的回執，不是本輪手動發送。
- 08:30 台北附近的最新自然 `news_ch_collect_process` run 成功，XCom 回執為 12 sources、967 URL、966 parsed/valid-time、945 accepted、1 fetch/parse reject、0 invalid-time quarantine、21 content/duplicate rejects。48 小時新鮮度按來源原始時間精度分為 794 筆有時分且落窗、5 筆日期級晚於 cutoff、26 筆 cutoff 日期但無時分而未知、120 筆較舊。自由時報為 80/99、鉅亨網為 147/150 partial；iThome 與 thenewslens 有日期級時間混合舊列/邊界未知，午夜日期不當發稿時刻。
- 日線仍停在 10/02 `ready/partial`：母體 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、50 候選；10/02 after-close parent failed，TPEx 行情在兩次嘗試只收到 704,105 與 4,193,897 / 4,666,641 bytes，尚無新自然 run 驗證有界續傳程式。其 market-signal child 的 capture 成功但 `notify_market_signal_snapshot` 失敗。Outcome 仍只有 9/24 horizon 1：signal-close 81 列（50 hit/30 miss/1 unresolved，平均超額 +1.776pp）；next-close 81 列（37/43/1，+0.987pp）；沒有成熟 5/10-session 或成本調整證據，不調門檻。
- 當沖自動來源缺口未變：TWSE TWTB4U/TWTB5U 的契約差異與授權仍待核、TPEx 個股 endpoint 空資料且未確認授權、官方 TPEx OAS 11627/當沖統計僅市場彙總且必須 freshness gate；不接未列入官方開放授權的個股網站查詢路徑，不把未匹配列當零量。這輪只讀 Airflow/資料庫與本地文件，未寫 DB、手動觸發/重跑 DAG、發 Discord、交易或跑測試；保留其他既有工作樹變更。

## 2026-10-04 09:27 台北：after-close digest 通知失敗尺寸線索

- 唯讀檢查 10/02 `market_signal_notify` daily child 的三次通知嘗試 log：snapshot capture 成功，`notify_market_signal_snapshot` 三次皆只留下泛化的 `Failed to send the message`；舊 log 沒有 HTTP status 或 response body。通知帳本有兩個 digest 分段，一筆 sent 與一筆 failed，但兩筆都沒有平台 receipt id/status，故舊的 `sent` 不能證明送達。
- 失敗分段的資料庫序列化 JSON 為 13,624 bytes、9 embeds、4,796 個 embed 字元；這高於目前程式的 10,000-byte Discord request budget，與 payload 過大相符，但舊回應細節缺失，原因仍未確證。現行 `_split_discord_embed_batches` 以 Discord ASCII JSON 序列化大小和 embed 數量切分；10/02 是更早的自然 run，不能證明當時已使用現行切分器。
- 10/04 週末盤中 child 已自然驗證新的回執管線：6 筆 sent 均有 receipt id、HTTP 200；它只證明該次較小的盤中訊息，不能替代 50 候選 daily digest 的分段驗收。等 10/05 20:00 自然 after-close child 核每段 HTTP 狀態、receipt id、候選分段與總體失敗數，不手動重跑或發送。

## 2026-10-04 10:04 台北：舊當沖表五日市場覆蓋複核

- 以 Airflow worker 連線執行唯讀交易，只查 `stock.當沖資訊` 與 `全證券代碼相關資料表`，資料庫 transaction 設為 read-only。最新五個有資料日期為 10/02、10/01、09/30、09/29、09/24；每日 1,228–1,232 個唯一代碼，三個當沖數值欄在已存列均非 NULL。
- 用全市場代碼主檔（不限普通股）逐代碼分上市/上櫃/跨市場/未匹配：五日上市匹配各 972/971/973/974/975，上櫃匹配均為 0，跨市場歧義均為 0，未匹配各 256/257/257/257/257。普通股主檔匹配同樣是上市列、上櫃 0；故這不是僅被 CFI 普通股篩選排除 OTC 的結果。數值非 NULL 只能說明有值，不能證明來源契約或列內容正確；未匹配列不得改成零量。
- 舊 `StockDayTradeCollectProcess` 對 TWSE 呼叫 TWTB4U、對 TPEx 呼叫個股網站 POST 查詢，最後按日期/代碼合併；資料表沒有市場欄位。`source_extension/market_signals.py`、registry 與 adapters 均沒有讀取或註冊此表/當沖欄位。因無可確認的上櫃批次覆蓋、來源授權及表內市場 provenance，維持整張舊表不進訊號評分或通知。
- 後續只考慮能合法排程的全市場批次來源：先核 TWSE TWTB5U 產品的正式批次/API 交付與本專案授權；TPEx 個股資料需官方、授權和自然批次覆蓋證據。TEJ `TWN/APISHRACT` 仍是需核 entitlement/PIT 欄位的候選。獲准來源先以中性參與度/注意度背景評估，不直接加多空票；需逐次驗收 effective date、raw/accepted/quarantined、上市/上櫃覆蓋與回修語義，再對照成熟 outcomes。
- 本輪只做唯讀資料庫查詢、程式引用搜尋和文件記錄；沒有更動 collector、registry、訊號權重或門檻，沒有手動跑 DAG、發 Discord、交易或跑測試。工作樹其他既有變更保留。

## 2026-10-04 10:21 台北：官方 TPEx 當沖彙總加入收盤背景

- 官方政府資料集 [11627](https://data.gov.tw/dataset/11627) 標示每日更新、免費及政府資料開放授權第 1 版，來源為 TPEx 上櫃市場當沖統計彙總；欄位不含證券代碼。官方統計說明指出成交日金額/股數可以修正到 T+2，以 T+2 更新值為最終值。[TPEx 統計說明](https://www.tpex.org.tw/storage/zh-tw/web/stock/trading/intraday_stat/intraday_trading_statD.htm)
- Airflow worker 對官方 OpenAPI `tpex_intraday_trading_statistics` 做唯讀 fetch-and-normalize 預檢：HTTP 200、2 筆、欄位契約完整且數值範圍通過；有效日 2026-10-01/02。10/02 上櫃當沖量 605,346,000 股、占市場量 22.15%，買進 NT$125,448,623,510（44.71%）、賣出 NT$126,032,912,480（44.92%）。stock DB 最新收盤日亦為 10/02。這是程式/端點預檢，尚非 source_runs 自然收集證明。
- 新增 `tpex_daytrade_market_summary_daily` 的官方 allowlisted contract 與 `daily_after_close` source definition；只存 macro aggregate 與原始量/金額/占比欄位。空回應 fail closed；無效日期保留隔離，未來日期、數值缺漏/負數、占比超 100% 及同一 effective date 多列都隔離。下一自然 schema bootstrap 前 DB registry 仍是 17 個 after-close source；本地定義為 18 個，新來源目前 0 registry/0 source_runs。
- 收盤通知新增「上櫃市場現股當沖彙總」中性背景欄。只有 latest accepted effective date 與 price snapshot 同日且 source record 在 scan start 前已可用才顯示數值；缺列、隔離、過期皆說明原因，不沿用舊數、不改成 0。5-session 比率變化只在 8 個官方 TPEx 指數交易日與對應完整 aggregate 記錄齊備後顯示，區間終點取 T-2；同日當沖值明示仍可能 T+2 修訂，且舊版本未保存。新欄不加入個股方向家族、score 或門檻。
- 靜態驗證：worker AST parse 與 module import 通過；使用同一 runtime 對官方 endpoint 正規化出 2 筆 `quality=ok` macro records，並預覽日期吻合及過期兩種格式，過期預覽不輸出交易數值。唯讀查 DB 確認股票最新收盤 10/02、after-close registry 17→本地定義 18、新來源尚未進 DB。等 10/05 04:00 自然 bootstrap 與 20:00 自然 after-close 才驗收 source_runs 的 HTTP/effective date/列數/隔離數；未手動觸發 DAG、寫 DB、發 Discord、交易或跑測試。

## 2026-10-04 10:35 台北：週末自然批次回執與使用者再次確認自動化範圍

- 工作範圍再次明確固定為可排程、全市場、機器批次擷取及可回測的訊號管線；不以逐家公司瀏覽券商報告、新聞頁面或人工評比填補訊號缺口。券商來源只在全市場 feed/API 的授權、schema、PIT 修訂鏈與儲存/回測權確認後才接入。
- 週末自然 run `source_extension_collect_weekend / scheduled__2026-10-04T00:00:00+00:00`（台北 08:00）核了 9 個來源、0 個漏登、整體 `partial`。TWSE 公司事件 5/5 accepted（有效日 10/03，上市主檔匹配 3、主檔外 2）；TWSE 月營收 1,086/1,086（有效月 2026-08，上市匹配 1,054、主檔外 32）；TWSE 季財測差異 7/7、5 家上市公司；上市預定轉讓 1/1、上櫃預定轉讓 1/1，皆有效日 10/02 且各匹配本市場 1 檔。上市/上櫃未轉讓來源各 1 raw、0 accepted、1 quarantine，無有效日，不能解讀成零事件。兩個 TPEx 全市場來源（重大訊息、月營收）各兩次 read timeout（60 秒），均 0/0/0，故不構成上櫃覆蓋；父 DAG failed。失敗 run 的 DB registry 仍是 `tpex_csv`/舊 MOPS URL，而目前本地定義 `tpex_json`/官方 TPEx OpenAPI 檔案修改時間晚於 08:00 run；須等下一自然 bootstrap/收集驗證是否已同步，不把這次舊契約 run 當作新程式驗收。
- 該 source run 自然觸發的盤中 child 成功：`ready/partial`、as-of 10/04，5 張候選，新聞脈絡狀態 `matched`，但價格、法人流、營收、財務、借券、董監與除息家族計數皆 0；所以這是事件卡，不是有盤中價格確認的方向訊號。此 run 的通知 ledger 有 6 筆 sent，6 筆均有平台 receipt、HTTP 200；這是排程自然發送的回執，沒有手動發送。
- 最新 daily signal 仍為 10/02 `ready/partial`：主檔 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、除息 1,053、50 候選、通知計數 0。成熟 outcomes 仍只有 9/24 horizon-1：signal-close 81 列（50 hit/30 miss/1 unresolved，平均超額 +1.776pp），next-close 81 列（37/43/1，+0.987pp）；尚無成熟 5/10-session 結果，不調權重或門檻。
- 最新新聞自然 run `news_ch_collect_process / scheduled__2026-10-03T21:30:00+00:00` 於台北 08:30:01–08:38:15 完成。12 feeds 合計 967 URLs、966 parsed/有效時間、945 accepted、1 fetch/parse rejected、0 invalid-time quarantine、21 content/duplicate rejects。來源接受列：ETtoday 100、TVBS 16、iThome 60、newtalk 21、thenewslens 90、三立 30、中央社 100、中時 198、公視 43、聯合報 60、自由時報 80/99、鉅亨網 147/150；自由時報與鉅亨網為 partial。08:38 台北的 48 小時窗內，794 列有明確發稿時分、5 列只有較新的日期戳、26 列日期落在 cutoff 當天但時間未知、120 列依來源時間精度較舊；不把日期級時間升格成精確發布時刻。新聞只保留為可 PIT 對齊的背景，不單獨轉成方向票。
- 唯讀檢查 Airflow runs/logs 與 crawler 結果，無 DB 寫入、DAG 手動觸發/重跑、手動 Discord 或交易，未跑測試。下一個平日自然節點是 10/05 04:00 bootstrap、20:00 after-close；重點核 TPEx 新 JSON 合約、當沖彙總 registry/source_runs、有效日及上市櫃覆蓋、daily 通知收件回執和成熟 outcome 更新。

## 2026-10-04 10:41 台北：TPEx 官方批次分段擷取縮小區塊

- 對目前本地 allowlisted TPEx JSON 合約做 Airflow-worker 端點/正規化預檢（不寫 DB）。256 KiB 分段的 `tpex_monthly_revenue` 在 5 次請求後仍只有 262,144/496,547 bytes，HTTP 206 串流以 `ChunkedEncodingError` 中斷；`tpex_company_events` 同一版先前可完整讀取 67,201 bytes/42 rows。這說明失敗具來源/回應大小差異，不能由一個端點成功推成所有端點完整。
- 將 allowlisted TPEx JSON byte-range chunk 從 256 KiB 縮至 64 KiB。修改後同一 runtime 完整讀完：重大訊息 67,201 bytes/42 rows/34 個代碼，42 筆 `quality=ok`，34 個上櫃普通股主檔匹配、無未匹配，effective date 10/01；月營收 496,547 bytes/892 rows，892 筆 `quality=ok`、effective month 2026-08，891 個上櫃普通股匹配、1 個代碼未匹配（保留未匹配，不視為零或完整覆蓋）。兩端點分別使用 2 與 8 次經驗證的 byte-range 請求，均 HTTP 206 且完整回應驗證通過。
- 同次唯讀預檢確認 TPEx 當沖市場彙總回 HTTP 200、2 筆 `quality=ok` macro rows，effective date 10/01–10/02；這些列無個股代碼，只供市場背景。
- 靜態驗證 worker `ast.parse` 與 module import 通過，讀回 chunk 設定為 65,536 bytes；未跑測試、未寫資料庫、未手動觸發/重跑 DAG、未手動發 Discord 或交易。以上仍是直接 endpoint 預檢，不是 Airflow `source_runs` 收集證明。等 10/05 自然 bootstrap/after-close 驗收 registry 合約、HTTP、完整列數、隔離與上市櫃匹配。

## 2026-10-04 10:44 台北：registry 載入前拒絕 adapter/endpoint 漂移

- 10/04 08:00 自然週末 run 的只讀 audit 證實 TPEx 兩來源 registry 還保留舊 `tpex_csv`/舊 URL，任務因此對過時路徑請求並失敗。檢查載入流程發現 `_runtime_definition_matches` 當時只比對目標、實體、config 與排程，漏比對 `adapter_key`、`endpoint_url`；audit 會標 drift，但 collector allowlist 未 fail closed。
- 已在 `registry._runtime_definition_matches` 加入 adapter 與 exact endpoint URL 比對。下一自然 bootstrap 仍可依程式定義同步 registry；若其後仍漂移，discover/load 會在抓取前拒絕舊契約，不再請求未 allowlist 的舊路徑。
- 靜態驗證：Airflow worker `ast.parse` 及 registry import 通過，50 個來源定義均有 adapter/endpoint；未手動跑 DAG、寫 DB、發通知或交易，未跑測試。待 10/05 自然 bootstrap/after-close 驗證更新後 registry、來源回執及覆蓋。

## 2026-10-04 10:58 台北：自動標記成功但陳舊的批次來源

- 再次確認本工作的交付物是可排程、全市場、批次抓取與 PIT/outcome 驗證；不做逐家公司券商報告搜尋和人工評比。券商預估只在授權的全市場 batch/API、上市櫃覆蓋、歷史 vintage/修訂時間和儲存回測權可核時納入。
- 唯讀檢查最新 `source_extension_collect_process / scheduled__2026-10-02T20:00:00+00:00`（台北 10/04 04:00）：21 個預期 source 全回報，無漏登或 runtime contract drift，overall `all_expected_tasks_succeeded_data_review_required`。其中 `tpex_dividends` HTTP/task success、2,483/2,483 accepted、793 個不同代碼（748 上櫃、9 上市主檔匹配，36 主檔外），但 effective date 只到 2021-08-03、發布/事件日只到 2021-08-04；相對 run start 已舊 1,886 天。這是每日任務成功但內容多年未前進的批次 feed，不代表當日沒有上櫃股利。
- 已將 source-semantic、report-only freshness check 加到 `audit_runs.py`：對此官方年度股利 feed 檢查 accepted 列的最大 `event_time` 是否超過寬鬆 365 天；過期或缺時間會加入 quality flag，並把最新事件時間、年齡與檢查界限放進 source-run audit/XCom issue 摘要。Airflow worker 靜態解析 `audit_runs.py`/`dag.py` 並實際唯讀 audit 最新自然 run，確認回報 `latest_event_time_older_than_expected`、1,886 天。這不丟棄歷史列、不改訊號分數/交易門檻、不作為交易決策；現有個股股利觀察只讀 45 天內發布列，故這批 2021 年資料不會冒充近期個股訊號。
- 官方 TPEx E-Shop 有上櫃股利統計的全市場批次檔，但官方產品頁標示外部使用月費 NT$10,000；本專案 entitlement/合約未知，所以僅列候選、未接入。[上櫃股利統計產品](https://eshop.tpex.org.tw/zh/product/detail/703BD71401F177340A45B08188F82BAD) 與[TPEx 資料產品/訂閱說明](https://www.tpex.org.tw/web/service/info_service/off_market_info.php?l=en-us)。不改接個股查詢頁，也不把官方市場彙總誤作個股訊號。
- 最新新聞自然 run（台北 10/04 10:30–10:38）12 feeds 共 971 URLs、971 parsed/有效時間、940 accepted、0 fetch/parse rejects、0 invalid-time quarantines、31 內容/重複篩除；ETtoday 100、TVBS 16、iThome 60、newtalk 23、thenewslens 90、三立 30、中央社 100、中時 200、公視 43、聯合 60、自由 70/99、鉅亨 148/150。自由與鉅亨 partial。48 小時窗內 790 列有精確發稿時分、4 列只有較新日期戳、26 列 cutoff 當日時間未知、120 列已舊；日期級時間不升格成精確時間。
- 訊號/驗證底盤未變：最新 daily 仍 10/02 `ready/partial`（1,946 universe、1,938 價格、1,891 法人流、50 候選）；最新 intraday 是零行情家族的事件觀察卡。成熟 outcomes 仍僅 9/24 horizon-1 signal-close/next-close 各 81 列，沒有成熟 5/10-session 或成本後結果，故不調方向分數/門檻。最新 after-close parent 仍 failed（map index 9 與 verifier）；daily child snapshot capture 成功，但通知三次 failed，舊 ledger 缺平台 receipt。
- 本輪只唯讀 Airflow/DB 查詢及 worker AST/import/audit 驗證；未跑測試、未寫 DB、未手動觸發或重跑 DAG、未送 Discord、未交易。工作樹既有無關變更保留。

## 2026-10-04 11:07 台北：全市場券商預估 feed 資格與 outcome 成熟日

- 只核全市場資料產品，不查單家公司。CMoney 官方 M037 說明頁標示更新時段每日 09:50、19:54；資料以近半年同一機構對同一股票的最新有效報告彙整，列出機構預估年營收/稅後盈餘/EPS、預估上修幅度、高低 EPS、預測機構數與最新報告更新日。[M037 欄位與更新規則](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M037)。M188 是外資 subset，不能與 M037 重複算成兩個獨立訊號家族。[M188](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M188)
- CMoney Owl API 官方頁宣稱台股歷史結構化 REST API、可多股批次查詢、自動更新；公開授權文字限個人/企業內部研究，未經書面同意不得重製、轉售或散布。[API 產品頁](https://owl.cmoney.tw/)。獵報商品另宣稱有每日預估修訂與歷史軌跡。[獵報](https://www.cmoney.com.tw/product/hunter-report)
- 目前仍未證明 M037/M188 是否在 Owl API 的實際訂閱商品、批次 rate/歷史 vintage、代碼與普通股覆蓋、估值年度/修訂首次可得時刻、以往快照可否持續保留回測、或 private Discord 訊號顯示是否符合授權；故不登入抓取、不爬 M037 個股頁、不接入。M037 彙總也不等於可檢視原始報告的假設與反方風險；若獲授權，可以用全市場每日 PIT 快照研究上修/下修 breadth、EPS 變動、分歧度和預測機構數，配合 MOPS 實績及市場相對 outcomes 做校準；若只有 M037 聚合欄位，將明確標註無法作報告動機/原文品質判讀。
- 唯讀核對 `market_signal_outcomes`：9/24 的 signal-close horizon-1 81 列（80 列有 hit，50 hit），next-close horizon-1 81 列（80 列有 hit，37 hit）；目前仍無 horizon-5/10 settled rows。`stock` DB 可見交易日只有 9/24、9/29、9/30、10/01、10/02，故訊號日後僅 4 個交易日，horizon-5 未成熟不是可據以調參的負樣本。按現行 entry/exit 定義，signal-close h5 最早需 10/05 收盤；next-close entry 是 9/29，h5 最早需 10/06 收盤；成熟後仍需上市/上櫃 benchmark 對應日期齊全。只等自然收盤 run，不手動補算/重跑。
- 本輪讀 Airflow metadata 唯讀確認：10/04 08:00 source weekend parent failed；自然觸發的盤中 child success。`source_extension_collect_process` 最新 10/04 04:00 本地時間 run success、21/21 source 回報但 audit 是 data-review-required；10/02 after-close latest parent failed（ingest map index 9、verifier），trigger 子 run 成功。新聞 10/04 10:30 run success、12 feeds 中自由與鉅亨為 partial。沒有手動觸發 DAG、寫資料庫、送通知、交易或跑測試。

## 2026-10-04 11:12 台北：舊 after-close source_runs 缺 DAG 歸屬

- 直接查 Airflow metadata：latest `source_extension_collect_after_close / scheduled__2026-10-01T12:00:00+00:00` 仍 failed，`ingest_approved_source` map index 9 與 verifier failed，market-signal trigger 成功。指定查 `market_signal_notify / market_signal__daily__scheduled__2026-10-01T12:00:00+00:00` 亦為 failed（Oct2 20:02–20:08 台北）；前述自然子 run 的 snapshot 成功但通知多次失敗，不能將 trigger task success 當成通知完成。
- Crawler `source_runs` 對相同 logical `dag_run_id` 有 17 筆歷史列、15 個不同 source key，`dag_id` 全部 NULL。由於 Airflow 的 scheduled run ID 可在不同 DAG 重複，這些列不能安全歸因到 after-close，也不能只按 run ID 混合出有效日/覆蓋。Airflow task map index 9 失敗是 metadata 證據；來源列的上市櫃接受與 effective date 需等正確 lineage 的自然 run。
- 已讓 `audit_source_run` 遇到指定 DAG/run ID 無精確 source_rows、但同 run ID 有 `dag_id` NULL 舊列時，回 `status=unknown`、未歸屬列數與 source 數，明確寫不能安全歸因；source-extension verifier 的 XCom 摘要也保留 reason/count。沒有做猜測式 fallback 或回填舊資料。
- Airflow worker `ast.parse`、module import 通過；唯讀 audit 精確查舊 run 回 17 orphan rows/15 source keys，latest `source_extension_collect_process` 仍核到 21/21 reported、no contract drift、`all_expected_tasks_succeeded_data_review_required`。等 10/05 自然 after-close 核新 `source_runs.dag_id` / `dag_run_id` / `task_id` / `map_index`、source freshness/上市櫃覆蓋、daily child capture 與平台收件回執。未跑測試、未觸發/重跑 DAG、未寫 DB、未送 Discord 或交易。

## 2026-10-04 13:46 台北：自然排程驗收基線（維持全市場批次主線）

- 本輪只盤點全市場可排程訊號來源、自然 collection receipts、下游快照/outcomes、通知收件及新聞 freshness；沒有逐家公司找券商報告或人工評比。當下沒有新的自然來源回執足以判定新增來源可用，沒有因此調整評分或門檻。
- Airflow scheduler/worker 等服務均在執行，`airflow dags list-import-errors` 無匯入錯誤。最新 process run ID 的 logical date 是 10/02 20:00 UTC；Airflow metadata 的實際 start/end 是 10/03 20:00–20:02:32 UTC，即台北 10/04 04:00–04:02:32。該 run Airflow success 但來源 audit partial：舊 registry/run 有 21 個來源，runtime 現為 23 個，TWSE/TPEx 當沖資格 feed 均尚未在該 run 留下自然回執。
- 最新 intraday run ID 的 logical date 是 10/02 14:30 UTC（台北 22:30）；實際 start/end 是 14:45:00–14:45:14 UTC（台北 22:45:00–22:45:14）。早於本週日新增/更新的來源定義：該次 audit 為 partial，舊 catalog 9 個、現 runtime 10 個；新 TPEx 財測差異來源沒有自然 run receipt，四個內部人轉讓來源也未在該 run 出現，TPEx 公司事件/月營收的舊 catalog adapter 與目前 runtime contract 不同。不能把舊 HTTP NULL/零列當成來源空資料或新版本驗收。
- 官方上櫃財測差異 feed 的 10/04 唯讀端點回應仍只有一列快照日期 metadata，沒有公司/季度事件列，因此尚無個股事件覆蓋可計算；必須等自然 collection 逐源核有效日期、accepted/quarantined/duplicate、普通股主檔匹配率及 source contract。正式來源為[政府資料集 20761](https://data.gov.tw/dataset/20761)，日誌/通知不得把快照日期冒充事件發稿時間。
- 最新 after-close logical run 為 10/01 12:00Z（台北 10/02 20:00），父 run failed；舊 source_runs 有 17 列缺 dag_id，故 audit lineage unknown；runtime 18 個、DB catalog 17 個，TPEx 當沖市場彙總 feed 未在 catalog。此處不猜配舊血緣。最新自然驗收點是 10/05 20:00 台北。
- 最新週末盤中子訊號為 10/04 08:04 台北，ready/partial，5 張候選均為事件觀察；價格、法人流、營收、財務、借券、董監及股利等方向資料家族計數為 0。通知帳本有 6/6 receipt、HTTP 200，但只證明該次事件觀察通知收件，不代表行情資料完整。TPEx OAS 11627/當沖統計仍是市場彙總，需 freshness gate；不得當成個股方向訊號。
- 成熟 outcomes 最新仍只有 9/24 一個訊號日的 horizon-1，signal-close 81 列（80 列有方向結果、50 hit、平均超額 +1.7760pp）及 next-close 81 列（80 列有方向結果、37 hit、平均超額 +0.9868pp）；均未扣成本，無成熟 h5/h10。單日、單 horizon 不足以改參數。
- 最新新聞自然 run 為 10/04 12:30–12:39 台北、success：12 個 feeds、1,047 URLs、1,042 parsed/valid-time、1,019 accepted、5 fetch/parse rejects、0 invalid-time quarantine、23 content/duplicate rejects；三立、自由、鉅亨 partial。來源時間精度仍須保留；date-only 的 TNL/iThome 時間不確定性不能轉成盤中精確事件時間。
- 下一批自然檢查點：10/05 04:00 process/bootstrap（TWSE/TPEx 當沖資格來源登錄及逐源列數/覆蓋）、05:30 新聞（時間欄/來源 coverage）、07:00 起 intraday（四個內部人來源及 TPEx 財測差異來源的自然 receipts）、20:00 after-close（TPEx 行情 chunk resume、source lineage、上市/上櫃日線覆蓋、通知分批 receipt）。任務 success 仍逐源驗 effective_date、raw/accepted/quarantined/duplicate、coverage 與 receipt；未手動觸發 DAG、未寫 DB、未發 Discord、未交易。

## 2026-10-04 13:55 台北：將新聞背景關聯限制在掃描開始前已收錄資料

- 靜態追查訊號卡的新聞背景流程時，確認 news_ch 的 news 表同時有 source publication 欄 created_at 與批次寫入欄 create_time。news crawler 的 delete-and-insert 會刪除並重寫重遇文章，所以舊稿的 create_time 會更新；`_attach_recent_news_context` 原本只按 created_at 選最近 48 小時、取最新 10,000 個 id 後再篩，未以 signal scan `started_at` 限制收錄時間。這個背景不參與方向分數，但並行/延遲查詢時可能把掃描開始後才寫入的新聞帶入當次卡片，也削弱歷史 PIT 可重現性。
- 唯讀歷史核對 2026-10-04 08:04:24 盤中 scan 時點：目前資料庫最近 10,000 個 id 中，811 列符合原本 publication-time/date-only 條件，但其目前 create_time 晚於該 scan 時點。這只能證明後續批次重寫會令今日資料庫無法還原當時新聞表狀態，不證明 08:04 live card 當時真的讀到未來列；已避免過度宣稱。
- 已窄幅修改 `_attach_recent_news_context`：候選與 eligible-count 兩條讀取都在 source-time 篩選前套用 `create_time <= observed_at`，並先過濾再取最近 10,000 個可用列；日期精度不足、原始發稿時間/時區未核的項目仍只呈現為背景，不增加方向票。
- Airflow worker 靜態驗證：修改檔 AST parse 與 module import 成功；兩條 SQL 的 `%s` placeholder/parameter 數為 7/7、6/6。未跑測試、未寫 DB、未觸發 DAG、未發 Discord、未交易。工作樹其他既有變更保留。

## 2026-10-04 13:59 台北：新聞背景通知明確標示 PIT 截止與取樣分母

- 同步調整每日候選卡的新聞背景說明：命中與未命中兩種分支都標明只比對訊號掃描開始前已收錄資料；樣本達上限時改按實際流程敘述——先以掃描時點、發布時間與內容條件篩列，再依最新 ID 取最多 10,000 篇供最多 6,000 篇比對。未匹配仍明確表示不代表無相關報導，日期級來源仍保留時間/時區未核警語。
- Airflow worker 靜態 AST parse、module import 成功；兩條新聞查詢 placeholder/parameter count 再核為 7/7、6/6。未跑測試，待自然訊號掃描確認卡片渲染；未觸發 DAG、寫 DB、發 Discord 或交易。


## 2026-10-04 14:16 台北：官方 TPEx 除權息批次來源資格預檢

- 按全市場自動收集範圍檢查政府資料開放平臺資料集 [11634](https://data.gov.tw/dataset/11634)「上櫃股票除權除息預告表」：目錄標示每日更新、免費、政府資料開放授權第 1 版；官方下載端點為 TPEx CSV。Airflow worker 唯讀請求 HTTP 200、`application/csv`，解析到 80 列、12 欄、78 個代碼、80 個唯一代碼/除權息日鍵、0 重複鍵；日期範圍 2026-09-23 至 2026-11-05；80 列欄位皆非空，類型為 79 列除息、1 列除權。
- 傳輸層已完整接收：TPEx 回應使用 `Transfer-Encoding: chunked`，worker 的 `iter_content` 正常讀完 10,000 bytes 並收到乾淨的結尾換行，故本次證據不支持「HTTP 傳輸中斷」。仍無法證明端點層的全量性：沒有官方總列數/頁碼或 continuation contract，Range 被忽略且不同範圍回相同內容，無從排除來源自身回傳限制；市場覆蓋仍只確認 16/78 代碼命中普通股主檔。
- 此表是未來除權息/現金增資行動排程，不等同舊 `tpex_dividends` 年度配息實績 feed；舊 feed 的最新事件仍停在 2021-08-04。可作為未來 corporate-action 背景或 outcomes 調整的批次來源候選，但在完整性及證券範圍可核之前不新增到 registry、不替換舊 feed、不產生個股通知/方向訊號。下一步僅在端點能提供可核總長/頁數，或由另一個官方授權批次資源交叉證明完整性後，再評估整合。
- 本輪唯讀核對近期 Airflow run 清單：最新 process 實際台北 10/04 04:00、最新週末 collection 10/04 08:00（failed）、最新 intraday source collection 台北 10/02 22:45、最新新聞批次台北 10/04 12:30–12:39；after-close 最新實際台北 10/02 20:00（failed）。沒有更新的市場日線/盤中/成熟 outcomes 可改變前述基線；不手動觸發/重跑 DAG，不寫 DB、不送 Discord、不交易。保留其他既有工作樹變更。
## 2026-10-04 14:29 台北：區分 TPEx 除權息回應傳輸與全量性

- 重新按資料品質檢核拆分傳輸完整性與端點全量性。Airflow worker 收到 HTTP 200，Transfer-Encoding: chunked；用 streaming iter_content 讀至正常結束，共 10,000 bytes、乾淨換行，無 parser/truncated-transfer error。這證明此次 HTTP body 有完整 chunk framing，不代表資料提供端已證明全市場枚舉完整。
- 同一端點仍無 total row count/page cursor；兩個 Range 請求都回 HTTP 200、相同 10,000-byte body，無 Content-Range。目前只能確認 80 rows、78 code、80 個 unique code/date keys、0 duplicates、日期 2026-09-23 至 2026-11-05、16/78 命中 892 檔上櫃普通股主檔。未匹配代碼仍是 unknown。
- 因此修正前次對「可能傳輸截斷」的表述；目前疑點是無法獨立驗證 endpoint 的全量契約及證券範圍，不是已觀測到 HTTP 中斷。仍先不註冊、不替換陳舊實績 feed，也不把它當個股方向訊號；若 TPEx 提供總列數/分頁合約或授權的全市場參照，再補 source-level completeness check。
## 2026-10-04 14:43 台北：自然新聞批次完成與通知回執查核

- 最新新聞自然 run scheduled__2026-10-04T04:30:00+00:00（logical date 台北 12:30）於台北 14:30:00–14:39:31 完成，Airflow/XCom success。12 feeds 共 1,092 URLs、1,090 parsed/valid-time、1,069 accepted、2 fetch/parse rejects、0 invalid-time quarantine、21 content/duplicate rejects。三立 28/30、自由時報 79/100 為 partial；其餘來源接受列為 ETtoday 220、TVBS 16、iThome 60、newtalk 24、TNL 90、中央社 100、中時 199、公視 43、聯合報 60、鉅亨網 150。
- freshness cutoff 為 10/02 14:39:24 台北：1,069 accepted 中 911 列精確時間落於 48 小時窗、4 列僅日期級且落在較新日期、26 列落在 cutoff 當日但時分未知、128 列依來源時間精度已舊；0 筆 future recorded time。TNL 為 82 舊、2 日期級較新、6 cutoff-date 時分未知；iThome 為 38 舊、2 日期級較新、20 cutoff-date 時分未知。後兩源不能當盤中精確發稿訊號。
- 唯讀重查 market-signal DB：最新 daily snapshot 仍為 10/02 ready/partial，1,946 universe、1,938 price、1,891 flow、50 candidates、0 notified；最新盤中 snapshot 為 10/04 08:04 ready/partial，只有 5 張事件觀察卡，行情/法人流/營收/財務等方向家族均為 0。該次 notification ledger 6/6 rows 有 platform receipt、HTTP 200。成熟 market_signal_outcomes 仍只有 9/24 horizon-1：signal-close 81 rows/80 scored/50 hits/平均超額 +1.7760pp，next-close 81/80/37/+0.9868pp；未扣成本，沒有成熟 h5/h10。
- 另查到 10/03 08:02 盤中通知 ledger 有 11 rows 標成 sent，但 0 platform receipts、HTTP status 全空；不能把它當成平台已收件。現行 _notification_delivery_status 只有結果含 success=true 且有 receipt_id 才回 sent；10/04 08:04 的 6/6 HTTP 200 receipt 是新版本的自然 runtime 證據，舊列維持未證實，未重送。
- 來源排程基線：10/04 04:00 process Airflow success、audit partial，21/21 舊登錄回報但 runtime 23，缺 TWSE/TPEx 當沖資格 feeds；現有 tpex_dividends 仍 2,483/2,483 接受但有效日僅至 2021-08-03、36 個代碼不匹配上櫃普通股主檔。10/04 08:00 weekend collection failed，TPEx 重大訊息/月營收舊契約 0/0/0 不代表空資料，且 runtime 新增的 TPEx 財測差異來源未留回執。After-close 最新父 run 10/02 20:00 失敗，TPEx 行情 map index 9 失敗；其 daily child snapshot 成功、notify task failed。下個平日自然檢查點 10/05 04:00 process/bootstrap、07:00 intraday、20:00 after-close，逐源核 effective date/接受與隔離/重複/上市櫃覆蓋及 receipts。
- 本輪使用 Airflow metadata、XCom 與 market-signal DB 唯讀查詢；沒有寫 DB、手動觸發/重跑 DAG、發 Discord、交易或調整門檻。

## 2026-10-04 15:01 台北：成功回執也納入批次品質狀態

- 發現盤中 _source_collection_state 原本只看 mapped task success 與預期來源鍵，未使用回執中的 row_count/accepted_count/rejected_count/duplicate_count。因此任務成功但有隔離列、空回應或列數無法核帳時，盤中 XCom 仍可能標 complete，通知便不會附品質警告；這與來源 audit 的 data_review_required 定義不一致。
- 已改為讀每個 mapped task 的最終嘗試及四項批次計數：缺來源/任務失敗仍為 partial；成功但零列、存在隔離列、核帳不符或有輸入列卻無接受列，回 data_review_required；僅重複列按帳，不單獨當品質失敗。通知將這兩種狀態分開說明，並列出具體來源與計數/有效日。
- 盤中品質提醒改用持久狀態指紋，只在問題類別首次出現、改變或恢復後再發時通知，避免固定 15 分鐘排程重覆提醒相同問題；送達回執模糊時沿用 at-most-once，不因指紋狀態改變而重送。沒有呼叫通知器或改寫資料庫。
- Airflow worker 專案 Python 檔 AST 靜態解析通過。未跑測試、未手動觸發/重跑 DAG、未寫 DB、未送 Discord、未交易。等待下一個自然盤中 run 核 source_collection_status、來源計數、問題指紋是否正確抑制重覆提醒及平台收件回執；其餘工作樹既有變更保留。
## 2026-10-04 15:20 台北：日線有效收盤覆蓋不再計入缺值列

- 工作範圍按使用者再次校正：核心是全市場、可排程批次擷取、可驗收和可回測的訊號；不以逐家公司手動瀏覽券商報告或人工評比分類作主線。新增來源先判斷是否有可用批次端點、授權、有效時間、PIT/歷史版本及上市櫃覆蓋。
- 既有唯讀資料顯示 2026-10-02 普通股分母為上市 1,054、上櫃 892。最新日行情列上市 1,050、上櫃 888；但有效正收盤只有上市 1,050、上櫃 868。因此舊 `price_count=1,938` 把上櫃 20 檔缺值/非正收盤仍算成有效，正收盤基線應為 1,918。法人近 7 日有效列為上市 1,049、上櫃 842。這是當時 DB 快照觀察，尚待下一次自然 daily run 驗證新程式落庫。
- 修改 `market_signals.py`：行情列、唯一正收盤與無效收盤代碼分開計數；同日收盤缺值、非正或同代碼多個不同收盤皆排除於有效覆蓋、個股價格家族及相對產業報酬。每個市場的通知列出日行情列數、有效收盤覆蓋及無效收盤數；細項保留缺值為 unknown，不補零。只調整資料品質狀態，不變更 70% 每市場覆蓋門檻或訊號分數。
- `market_signal_run_market_coverage` 增加每日按上市/上櫃持久保存的行情列代碼數與無效收盤代碼數；schema migration 10 以 additive ALTER 支援舊表。AST parse 通過。未跑測試，未手動觸發/重跑 DAG，未寫 DB、未送 Discord、未交易；等下一個自然收盤 run 核 effective date、有效/無效列數、上市櫃覆蓋與通知回執。
- 最近已知運行底盤（10/04 14:43 台北 audit）：daily as-of 10/02；盤中最新 10/04 08:04 為事件觀察卡；after-close 父 run 最新 10/02 20:00 failed；新聞最新自然批次 10/04 14:30–14:39；成熟 outcomes 仍只有 9/24 h1。未手動補跑。
- 相容性補充：舊覆蓋快照未保存「當日行情代碼數/無效收盤數」的欄位以 NULL 表示未知；通知會明示歷史快照未留存分項，不回填成 0。

## 2026-10-04 15:32 台北：個股當沖數值來源契約與自然批次重查

- 官方 TWSE OpenAPI Swagger 的 `/v1/exchangeReport/TWTB4U` schema 只有 `Date`、`Code`、`Name`、`Suspension`；它是上市當沖資格/暫停註記，不含個股當沖量或買賣金額。官網舊 `exchangeReport/TWTB4U?...selectType=All` 頁面雖展示過這些數值，仍不能當成同一個 OpenAPI contract 或已確認開放授權批次 feed。
- TWSE 官方網路資訊商店將含個股當沖成交股數、買進/賣出金額及市場彙總的日檔列為 TWTB5U：每交易日約 20:00 產製、XLS、資料起始 2020-06-01。官方標示內用 NT$1,500/月；傳給資訊用戶 NT$3,000/月。商品頁列網路下載/電子郵件交付，但未公開機器 API、自動排程交付時程或 SLA。TWSE 說 T/T+1 可調整，T+2 才是最終正確數額。尚未購買、未接收檔案、未接入訊號；先要求授權與自動交付契約明確，並確認私人 Discord 展示權利。
- 如果獲授權且機器交付可確認，先保存 T、T+1、T+2 vintages，分開即時預估與 T+2 最終數；僅在交易股數分母定義核對後計算個股當沖占比/趨勢，先作交易強度與投機注意度背景，對成熟且成本/公司行動處理後 outcomes 驗證，再決定是否加方向。TPEx 個股網站查詢仍不接；保留官方 OAS 11627 市場彙總、freshness-gated、非方向背景。
- 已在 `docs/source_extension.md` 增補來源授權、交付契約與後續驗證規格；沒有改 legacy collector 或訊號程式以連接 TWTB5U。
- 唯讀 Airflow 最新狀態：process 最新仍 `scheduled__2026-10-02T20:00:00+00:00`，success，實際台北 10/04 04:00；intraday source 最新仍 10/02 22:45 台北；after-close 最新仍 10/02 20:00 台北 failed；news 最新 `scheduled__2026-10-04T04:30:00+00:00`，台北 14:30–14:39 success。均無 10/04 14:30 之後的新自然批次。
- 唯讀 DB 重查：daily snapshot 仍 10/02 `ready/partial`，1,946 universe、1,938 舊價格計數、1,891 flow、50 candidates、0 notified。最新 intraday child 10/04 08:04 有 5 candidates/5 notified；對應通知帳本 6 rows sent、6/6 HTTP 200 有 receipt。成熟 outcomes 仍僅 9/24 horizon-1（signal-close 50/80 hit、平均超額 +1.7760pp；next-close 37/80、+0.9868pp），無 5/10-session。新 per-market coverage table 在 DB 尚不存在，因此 10/02 舊 row 不能驗證 15:20 後程式變更；等下一次自然 daily run 套 schema migration 並核數值/通知，不手動建表或重跑。
- 本輪只執行 Airflow metadata 與 SQL SELECT、查公開官方規格/商品條款；沒有 DB 寫入、DAG 觸發、Discord 發送、交易或測試。其他工作樹變更保留。

## 2026-10-04 15:35 台北：自然排程前 runtime source registry 差異

- 從 Airflow worker 只讀匯入當前 `SOURCE_DEFINITIONS` 並對照 crawler DB：程式有 53 個 source definitions（daily 23、daily_after_close 18、intraday 10、monthly 2），DB `source_registry` 有 49 列。DB 缺少 `twse_daytrade_eligibility_daily`、`tpex_daytrade_eligibility_daily`、`tpex_daytrade_market_summary_daily`、`tpex_quarterly_forecast_variance_events`；缺 catalog/receipt 不代表這些來源回傳零列或無事件。
- 同一對照另確認 `tpex_company_events` 及 `tpex_monthly_revenue` 的 runtime contract 已是 TPEx JSON OpenAPI，而 DB registry 仍記舊 MOPS CSV adapter/URL。這是來源契約 drift，舊回執不作為新 JSON endpoint 收集證據；endpoint HTTP/raw/accepted/quarantined/有效日/覆蓋需等後續自然 run 逐源確認。
- 最新自然 source registry 更新時間仍是 10/04 約 08:04 台北，早於目前 runtime definitions；process 最新 logical run 10/02 20:00Z（實際台北 10/04 04:00）success，但不能驗證之後新增/改版項目。下個平日自然 process/bootstrap 核實 catalog 是否新增 4 個來源、更新 2 個 TPEx JSON contract，以及各自 receipt/effective date/接受隔離/上市櫃 join；不手動改 DB 或觸發 DAG。
- 本機日線有效收盤修正的 `market_signal_run_market_coverage` 表目前尚未建立，舊 10/02 daily `price_count=1,938`/50 候選/0 notified 仍為舊程式快照；成熟 outcomes 仍只有 9/24 h1。新表與新版計數都待下一次自然日線 run 驗證。
- 本次只讀 Python registry definitions、DB `source_registry` 及既有 Airflow metadata；未寫 DB、觸發/重跑 DAG、送 Discord、交易或跑測試。
- 自動更新路徑已由 DAG 原始碼確認：`ensure_source_extension_schema`（呼叫 `ensure_schema_and_catalog`/`_bootstrap_catalog`）先於 `discover_approved_sources`。4 個缺漏鍵的程式 priority 分數為 89、89、88.5、93.25，均高於 75 自動啟用線；下個自然 run 預期會先 upsert catalog 再發現 mappings，但仍以實際 DB catalog/run receipts 驗收為準。
- 兩個 TPEx contract drift 是既有 key 的 adapter/url 改版，`_bootstrap_catalog` 有相應 `ON DUPLICATE KEY UPDATE`；必須核自然 bootstrap 是否更新成功及新 JSON 回應，不把 catalogue update 視為 endpoint 完整性證明。

## 2026-10-04 17:26 台北：自然來源、新聞與訊號快照只讀複核

- 週末 `source_extension_collect_weekend` 自然 run（logical `2026-10-04T00:00:00Z`，台北 08:00）Airflow failed。唯讀 audit 顯示 9 個已登錄來源全回報，但目前 runtime 預期 10 個；缺少 `tpex_quarterly_forecast_variance_events`，`tpex_company_events`、`tpex_monthly_revenue` registry adapter/endpoint 與 runtime 契約漂移。兩個 TPEx 來源各有 2 次 `ReadTimeout`（HTTP status 未取得）；TPEx 個股覆蓋因此缺資料，不能解讀為沒有事件/營收。當沖轉讓來源有失敗 retry，但最終來源回執為 success：上市、上櫃各 raw 1 列，未轉讓列各 1 筆 quarantine，仍不是零活動。
- 該 run 的盤中子 run 成功，最新 snapshot as-of 10/04、source status `partial`，5 個事件觀察候選；價格、法人流、營收、財務、借券、董監、股利等方向家族皆為 0。通知帳本 6 列均有 receipt、HTTP 200，只能確認事件觀察訊息送達。舊 10/03 ledger 11 列仍為 0 receipt，不把 `sent` 當送達。
- 最新日線仍 10/02 `ready/partial`：universe 1,946、price 1,938（舊計數）、flow 1,891、revenue 1,945、financial 1,902、borrow 1,795、insider 1,944、dividend 1,053、50 candidates、0 notified。新 `market_signal_run_market_coverage` 表目前不存在，因此新的有效收盤覆蓋修正尚未有自然落庫證據。
- 成熟 outcomes 仍只有 9/24 horizon-1：signal-close 81 列／50 hit／30 miss／1 unresolved，平均 excess +1.77597pp；next-close 81／37／43／1，+0.98679pp。無 h5/h10，也未扣成本；維持不調分數/門檻。
- 最新新聞自然 XCom 為 logical 10/04 14:30（UTC run id `scheduled__2026-10-04T06:30:00+00:00`，台北實際 16:30–16:38）：12 feeds、1,153 URLs、1,153 parsed、1,152 valid-time、1,135 accepted、1 invalid-time quarantine、17 content/duplicate rejects、0 fetch/parse reject。此回執早於空白內容/reject reconciliation 修改，沒有新分類欄位；17:26 時下次 18:30 自然批次尚未執行，待其驗分類 delta=0 與 failure source count=0。
- 下次自然驗收依序是 10/05 04:00 process/bootstrap（新來源 catalog 與兩個 TPEx 合約）、09:00 TDCC 單來源更新、07:00 起 intraday、20:00 after-close（新版行行情覆蓋表、TPEx chunk resume、source lineage、雙市場 effective date/coverage、通知 receipt）。本輪只讀 Airflow metadata/XCom 和 crawler DB SELECT；未手動觸發/重跑 DAG、寫資料庫、送 Discord、跑測試或交易，保留其他工作樹變更。

## 2026-10-04 17:32 台北：全市場券商預估 API 批次資格再核

- CMoney 官方目前有可用的 Owl 歷史資料 API 平台：公開產品說明列出台股全市場歷史資料、自動更新、HTTPS/JSON、多股查詢模式；但公開授權限個人/企業內部研究，非經書面同意不得重製、轉售或散布。[CMoney API 產品](https://www.cmoney.com.tw/product/api)、[Owl 平台與授權](https://owl.cmoney.tw/home)。
- 官方 M037 頁列每日 09:50、19:54 更新；同一機構/證券取近半年最新有效預估，顯示年營收/稅後純益/EPS、盈餘預估月增減、高低 EPS、預測機構數及報告更新日。[M037 規格](https://www.cmoney.com.tw/M_Table.aspx?CMenuID=M037)。這可支援批次研究「預估修正廣度、共識分歧、預測機構數變動」候選因子；因資料是彙總估計，不等於原始報告、作者假設或逐次首次發布紀錄。
- Owl 一般平台的全市場/歷史/REST 宣稱，不證明 M037、M188 或 Hunter Report 已包含在此專案訂閱、具指定 API 表格、保存可重現的每日 PIT revisions，或授權將摘要呈現在 Discord；Hunter Report 頁雖宣稱有每日預估變化與歷史軌跡，也沒有公開機器 API 欄位契約。[獵報產品](https://www.cmoney.com.tw/product/hunter-report)。
- 故將 CMoney M037/Hunter 列為高價值「可批次來源資格候選」，尚未接端點或進訊號。正式接入前需確認訂閱品項、全上市櫃覆蓋、每列可得時間/版本、歷史 vintage 是否被覆寫、速率/SLA、分析師身分/預測期間及儲存、回測、Discord 使用權。若只有 M037 彙總，先把共識上修/下修廣度、分歧度及機構數變化作獨立 PIT 特徵研究；不得宣稱能由彙總表評估個別報告動機或分析師品質，M188 外資 subset 也不重複計為另一家族。
- 本次官方網頁查核沒有讀個股報告、登入、呼叫 API、聯絡供應商或抓取資料。未變更 source registry/訊號分數/門檻；後續只有在 entitlement、版本與授權可核後才接入，並以多個成熟 outcomes 驗證。

## 2026-10-04 19:30 台北：全市場 TDCC 大戶比例歷史面板回測

- 本輪依使用者校正，聚焦可自動批次取得的全市場來源；未逐家公司瀏覽或評比券商報告。新增可在 Airflow worker 以既有連線執行的唯讀研究腳本 [`stock_major_holder_panel_research.py`](stock_major_holder_panel_research.py)，查詢舊 `stock.大戶比例` 對日行情，沒有連接正式打分或通知流程。
- 時間可得性 gate 要求 TDCC row 的 `create_time` 在有效日後 0–7 天，且快照至少 95% 列及時；進場取資料可得日之後第一個有效收盤。2023-01-01 至 2025-04-25 有 108 個合格快照日（合格列平均及時比例 100%），其中 102 個日期有後續價格結果；按年每個變化 bucket 含 2023 41 日、2024 48 日、2025 13 日（2025 僅到 4/25）。變化門檻仍依現行背景欄位的嚴格 ±2pp 分桶，回看 1/5/10 個後續交易日。
- 相對命中率（同有效日、同 horizon 的可用 TDCC 面板平均報酬為基準，單位 %；只作描述）：

| 指標與變化 | 2023 H1/H5/H10 | 2024 H1/H5/H10 | 2025 H1/H5/H10 |
|---|---:|---:|---:|
| >400 張增加 >2pp | 44.4 / 41.6 / 41.4 | 45.0 / 43.5 / 42.1 | 44.7 / 39.3 / 40.4 |
| >400 張減少 <-2pp | 57.1 / 55.8 / 57.8 | 53.8 / 56.1 / 60.3 | 49.7 / 52.5 / 53.4 |
| >1000 張增加 >2pp | 44.9 / 40.9 / 40.4 | 46.3 / 44.3 / 40.9 | 46.3 / 40.0 / 37.1 |
| >1000 張減少 <-2pp | 54.0 / 55.3 / 56.8 | 54.3 / 55.8 / 59.0 | 54.0 / 61.4 / 57.2 |

- 增持 bucket 的方向命中率未見 >50% 的一致性；減持 bucket 雖多數年份/期間高於 50%，按預期方向乘號後的等日期超額報酬仍跨年反轉（例：>400 減持 H10 在 2023/24/25 為 -0.303/+0.305/-0.679pp，>1000 減持 H5 為 -0.088/-0.493/+0.491pp）。結果不支持把增加變化升格為看多，也不足以把減少改成反向或另加方向票。大戶持股維持已建置的候選卡非方向背景；不因單一日期或單一分桶調門檻。
- 解讀限制：舊表截至 2025-04-25、只用目前仍存在的代碼，缺逐日歷史上市櫃普通股分母；TDCC 週資料與 5/10 日 horizon 重疊；面板平均不是正式上市櫃指數 benchmark；收盤未做公司行動調整，也未扣成本。因此不是可執行交易訊號或正式策略回測。曾誤帶出一個「每日期名稱數」彙總欄，已從腳本移除；上表只保留有效日數與以有效樣本計算的命中率/超額資料。
- 執行後最新狀態唯讀複核：market-signal 最新日線仍是 10/02 `ready/partial`（universe 1,946、舊價格列計數 1,938、flow 1,891、50 candidates），個股行情有效日 10/02；最新盤中快照為 10/04 08:04 `ready/partial`、5 個事件觀察候選，行情/法人流等方向資料族為 0。成熟 outcomes 仍僅 9/24 horizon-1：signal-close 81/80 scored、50 hit、平均超額 +1.7760pp；next-close 81/80、37 hit、+0.9868pp；未扣成本，無成熟 H5/H10。新市場覆蓋表仍未建立，15:20 程式修正尚待自然日線驗收。
- 10/04 08:00 自然 weekend collection 仍 failed；盤中 child 成功但事件觀察與市場方向資料分開。10/04 18:30 最新新聞自然批次 success：12 feeds、1,220 URLs、1,219 parsed/valid-time、1,197 accepted、1 fetch/parse rejected、0 invalid-time quarantine、22 content/duplicate rejects；三立、自由時報、鉅亨 partial（分別 1 fetch/parse、21 空白內容、1 空白內容拒收）。TNL/iThome 時間精度仍有限。新聞只作掃描時點後續新聞背景，publication-time 與 scan cutoff 必須做 PIT join，不能補回舊候選方向家族。
- 使用唯讀 `audit_source_run` 詳查 process：狀態 partial，21/21 登錄 feed 有回執，但 runtime 預期 23，缺 `twse_daytrade_eligibility_daily` 和 `tpex_daytrade_eligibility_daily`；TWSE/TPEx 普通股主檔各 1,054/892。多數季報的有效日是 2026-06-30；TPEx `ci` 資產負債/損益/獲利三 feed 各 884 接受、全對上櫃主檔；TWSE 季獲利 1,054 raw、1,049 accepted、5 quarantine、31 個接受代碼未匹配主檔。TWSE 季資產負債與損益各 1,049 accepted、1,018 listed 命中、31 未匹配。TWSE 股利 1,228 accepted、有效日到 2026-09-21、59 未匹配；TPEx 股利雖 2,483 accepted，最新有效日仍 2021-08-03，748 個上櫃及 9 個上市匹配、36 未匹配。不可將未匹配代碼當零或將舊股利成功回執視為新鮮訊號。
- Airflow 唯讀 run-list 重查：process 最新仍為 logical 10/02 20:00Z，10/04 04:00 台北實際執行 success，但來源品質仍為上述 partial；after-close parent logical 10/01 12:00Z（10/02 20:00 台北）failed，其對應 `market_signal__daily__scheduled__2026-10-01T12:00:00+00:00` child 亦 failed。今日 10/04 尚無 process/after-close 自然 run；下一個排程才是 10/05。
- 下個自然驗收點為 10/05 04:00 source bootstrap、07:00 起盤中來源收集及市場掃描、09:00 TDCC 單來源刷新、20:00 after-close；逐源核 effective date/raw/accepted/quarantined/duplicates、上市/上櫃覆蓋、TPEx endpoint contract、source lineage 及通知平台 receipt。此輪只做讀取/SELECT 與研究查詢、腳本 AST parse；未手動觸發/重跑 DAG、未寫 DB、未發 Discord、未交易、未跑測試。

## 2026-10-04 19:45 台北：收盤通知揭露市場基準日期

- 使用 `source_records` 唯讀盤點官方指數 history：TWSE TAIEX 21 accepted rows / 21 distinct effective dates，2026-09-01 至 10-01；TPEx TPEX 22/22，2026-09-01 至 10-02。這足以支援近端基準 context，但不足以替 2023–2025 TDCC 面板回測提供官方歷史 benchmark。最新個股收盤日為 10/02，當日上市指數尚未對齊；不能拿 10/01 TAIEX 填成 10/02，或把缺值當零。
- 現有 `_load_daily_candidates` 僅當各市場 index 的最近六個有效數值點完整且最後有效日等於 scan `as_of_date` 才加入該市場相對報酬；此 gate 維持不變。但日線摘要只顯示籠統 `partial`，沒有揭露各市場基準日期，造成行情相對基準缺席原因不易稽核。
- 窄幅改善 `market_signals.py`：掃描 payload 附上市/上櫃 index code、最新資料列日、最新有效數值日、與 scan 日是否對齊，以及最近六點有效數量；日線 Discord 摘要顯示實際有效日期與未對齊/窗口缺值情況。歷史快照沒有該欄位時明示未保存，未知不補零；不改候選門檻、方向票或資料覆蓋 gate。
- 本輪市場基準 profile、官方來源資料日、scan 邏輯與格式化程式均已唯讀檢視；修改後 `market_signals.py` AST parse 通過。沒有跑測試或手動 DAG，等待下個自然收盤通知驗證 effective dates 與訊息呈現；最新 daily 仍 10/02，after-close 下一自然時點為 10/05 20:00 台北。

## 2026-10-04 19:54 台北：庫藏股公告股數進度比由批次欄位自動計算

- 依使用者再次明確校正，工作主線是全市場可自動收集及排程的訊號，不做一家一家人工瀏覽／評比券商報告。沿用既有 TWSE/TPEx 官方重大訊息批次 feed；不新增逐公司查詢、不接未確認授權的 MOPS 網頁路徑，也不要求人工作業。
- 官方公告若同時提供「本次已買回股份數量」及「預定買回之數量」，通知卡新增中性衍生欄位「公告股數進度比 = 本次已買回股數／預定股數」；只有兩值存在、預定股數大於零且實際股數非負時才顯示。保留公告原值與來源，不將缺值補零；不改公告過濾、方向分數、每日候選或交易規則。
- `dags/source_extension/market_signals.py`、`docs/source_extension.md` 已同步修改。這是沿現有批次欄位的小幅卡片改善，不構成報酬預測或新方向訊號。待下一自然事件卡驗有效公告欄位、計算與收件回執；沒有手動觸發 DAG、送 Discord、寫 DB、跑測試或交易。其他工作樹變更保留。

## 2026-10-04 20:08 台北：來源完整性稽核缺口改為 fail closed

- 從目前健康的 Airflow 容器、bind-mounted `D:\DataFlow` runtime 及 crawler DB 唯讀重核：worker 中 `dag.py`、`registry.py`、`market_signals.py` 與工作樹內容雜湊一致，容器執行的是目前批次程式。
- 最新自然 process logical run `scheduled__2026-10-02T20:00:00+00:00`（10/04 04:00 台北）Airflow success，但 source audit 是 partial：21/21 當時登錄 feed 有回執，runtime 需要 23，缺 `twse_daytrade_eligibility_daily`、`tpex_daytrade_eligibility_daily`。季度資料期多為 2026-06-30；TPEx 普通公司季財務三 feed 各 884/884 對上櫃主檔；TWSE 季獲利 1,054 raw、1,049 accepted、5 quarantine、31 個 accepted code 未匹配。TWSE 股利 1,228/1,228，最新 effective date 9/21；TPEx 股利 2,483/2,483，但最新 effective date 僅 2021-08-03，不能因 accepted 多就當新鮮訊號。
- 10/04 08:00 weekend collection failed：catalog/runtime 為 9/10，缺 `tpex_quarterly_forecast_variance_events`；`tpex_company_events`、`tpex_monthly_revenue` catalog 仍是舊 `tpex_csv`，與目前 `tpex_json` contract 漂移，兩 feed 各兩次 ReadTimeout、0 raw/accepted。TWSE 月營收 1,086/1,086、effective month 2026-08；官方重大訊息 5/5 accepted，但 2 個代碼不在普通股主檔，仍是 unmatched/unknown。當沖轉讓未執行列 1 raw/0 accepted/1 quarantine，不解讀成零活動。
- After-close 最新 parent logical `scheduled__2026-10-01T12:00:00+00:00`（10/02 20:00 台北）failed，其 market-signal daily child 亦 failed。唯讀 audit 對舊 `source_runs` 回 `unknown`：17 rows 缺 `dag_id`，因此不可安全歸屬到 DAG；目前 runtime 需要 18、catalog 17，缺 `tpex_daytrade_market_summary_daily`。不把無法歸屬的列當已驗收。
- 最新 market-signal DB snapshot 是 10/04 08:04 intraday `ready/partial`：5 張事件觀察卡，價格/法人流等市場方向家族為 0；通知帳本 6 列均有 receipt、HTTP 200。日線仍 10/02 `ready/partial`（1,946 universe、舊 price count 1,938、flow 1,891、50 candidates）。成熟 outcomes 仍只有 9/24 horizon-1，沒有成熟 h5/h10，不改分數/門檻。
- 新聞最新自然批次 10/04 18:30 台北 success：12 feeds、1,220 URLs、1,219 parsed/valid-time、1,197 accepted、1 fetch/parse rejected、0 invalid-time quarantine、22 content rejects（全為空白內文）、分類 delta 0、reconciliation failures 0。三立、自由、鉅亨為 partial；該批晚於 08:04 scan，只能由後續 scan 按 publication/cutoff PIT 加入，不回填舊候選。
- 根據「任務 success 不代表來源完整」的實際錯位，`dag.py` 的終端 verifier 現會對 source audit `partial`/`unknown`/`unavailable` fail closed，並在 audit 成功時以 XCom 明示 `data_review_required` 或 `complete_completeness_unproven`。它仍不會把 row-quality flag 變成方向票；`docs/source_extension.md` 已同步。AST 靜態解析通過，host/worker `dag.py` hash 相符，Airflow scheduler DAG list 可見四個來源收集 DAG；這些都不是自然執行證據。未跑測試或 DagRun。等待 10/05 自然 process bootstrap 核實上述新 source catalog/TPEx contract 更新，再核 07:00 後自然盤中掃描與 20:00 after-close 的 effective date、計數、上市櫃匹配及平台 receipt；不手動觸發 DAG、不寫 DB、不送 Discord、不交易，保留其他工作樹變更。

## 2026-10-04 20:22 台北：批次行情 audit 自動輸出雙市場主檔匹配率

- 這次只補強自動化來源稽核，沒有做券商網站或個股報告人工調查。`audit_runs.py` 對成功且在 registry 明確設為 `common_equity_only` 的普通股全市場股票來源，計算本次 accepted securities 與目前上市／上櫃普通股主檔的匹配數及覆蓋率；`dag.py` 把該 diagnostic 放進 audit log、issue 明細及 compact XCom，供既有排程每次自動保存/查看。
- 覆蓋分母是 audit 當下主檔，不是有效日 PIT 母體；因此只反映目前代碼 join coverage，不證明該日每一證券都有列、事件完整或訊號有效。非普通股全市場行情、事件和彙總來源不計這個百分比；缺列/未匹配一律不補成零，亦未新增門檻、方向票或交易規則。
- `docs/source_extension.md` 已說明欄位範圍。以靜態 AST 解析驗證變更，未跑測試、未觸發 DAG、未查寫業務 DB、未發 Discord、未交易。此診斷尚無自然 run 驗證；待 10/05 第一個工作日行情收集後核對上市／上櫃分子分母、effective date 和實際 XCom。

## 2026-10-04 20:45 台北：TPEx 官方全市場日行情分段 reader 預檢完整

- 最新自然 process 仍是 10/04 04:00 台北，Airflow success 但 source audit `partial`：21/21 登錄來源有回執、runtime 預期 23，缺 `twse_daytrade_eligibility_daily` 和 `tpex_daytrade_eligibility_daily`；上市／上櫃普通股主檔 1,054/892。10/04 08:00 weekend run failed：registry 9、runtime 10，缺 `tpex_quarterly_forecast_variance_events`；兩個 TPEx 公司事件／月營收契約在該次仍是 catalog `tpex_csv`、runtime `tpex_json`，各 ReadTimeout、0 raw/accepted。這些都是批次來源狀態，不是零交易。
- After-close 最新仍為 10/02 20:00 台北失敗。Airflow task instance 顯示 `ingest_approved_source` map index 9 failed；同 logical run 的 17 筆 `source_runs` 沒有 `dag_id`，不能僅依 run ID 安全歸屬。該 source row 的歷史錯誤是整段串流 `IncompleteRead(4,193,897 bytes read, 472,744 more expected)`，0 raw/accepted；TWSE 1051 accepted rows 的最新有效日仍是 10/01。daily market signal 10/02 `ready/partial`（universe 1946、price 1938、flow 1891、revenue 1945、financial 1902、50 candidates），尚無 daily digest receipt。
- 對政府開放資料授權的 TPEx 個股日行情官方批次端點，用目前 worker 上的分段 reader 做唯讀預檢：72 個 206 Range segments 完整取回 4,666,641 bytes / 11,928 原始列，全部 11,928 列日期均為 2026-10-02；來源全市場資料原始品質分布為 6,442 ok / 5,486 quarantine（包含非普通股範圍）。按目前 CFI ES* 普通股主檔篩選後 888 rows，去重未移除列數；其中 867 ok、21 quarantine，符合接受品質的代碼為 867/892 上櫃主檔（97.20%），另外 4 檔沒有本批列、21 檔只有 quarantine 列。這是唯讀預檢的正規化狀態，非已落庫的 source-run 計數；缺列或隔離列都不推成零交易。[官方 dataset 11370](https://data.gov.tw/dataset/11370) 列明上櫃有價證券每日收盤成交資料及政府資料開放授權條款第 1 版；[授權條款](https://data.gov.tw/license) 要求顯名標示。這只確認 quote dataset，不延伸到另行查詢的個股當沖資料端點。未寫 source DB、未執行 DAG。這驗證新的 reader 可完整解析舊版整段串流會中斷的回應，但不等同自然 ingestion、PIT 母體完整；仍待下一次自然 after-close 確認 source_run、接受／隔離／重複及 XCom。
- 預檢也發現 coverage 輸出不能把非涵蓋市場列為 0%。`audit_runs.py` 現依 feed 設定標出 `in_scope`；非該 feed 市場的 counts/rates 為 null。只有 TWSE/TPEx 各自實際涵蓋的市場計匹配率，避免「該 feed 不負責上市／上櫃」被看成 0% 覆蓋。`docs/source_extension.md` 已同步說明。
- 最新 intraday 快照仍是 10/04 08:04 `ready/partial`，5 張事件卡、價格/法人流等方向家族為 0。該次 notification ledger 6/6 有平台 receipt、HTTP 200；10/03 的 11 列與 10/02 daily digest 歷史列沒有 receipt，不視為已送達。最新新聞自然 run 10/04 18:30 台北 success：12 feeds、1,220 URLs、1,219 valid-time parses、1,197 accepted、1 fetch/parse reject、0 invalid-time quarantine、22 content rejects；晚於 08:04 快照，只供後續 scan 按時間做 PIT 關聯，不回填舊卡。
- 成熟 outcomes 仍僅 2026-09-24 horizon 1：signal-close 81 rows/80 scored、hit rate 62.50%、平均超額 +1.7760pp；next-close 81/80、46.25%、+0.9868pp；未扣交易成本、無成熟 h5/h10。維持描述性監看，不調門檻或分數。變更後 `audit_runs.py`、`dag.py`、`adapters.py` AST 靜態解析通過，`git diff --check` 通過；未跑測試、手動 DAG、寫業務 DB、發 Discord 或交易。其他既有工作樹變更保留。

## 2026-10-04 20:54 台北：來源稽核自動標記隔離列與重複列

- 延續全市場批次自動化目標，未做逐公司人工研究。發現 `audit_runs.py` 原本會列出 quarantine/duplicate 計數，但僅因這兩種 row-quality 問題，不會加品質旗標；即使 mapped ingestion success，摘要仍可能看不出需要複核。
- 已在每筆來源 audit 自動加入 `quarantined_records_present`、`duplicate_records_present` 旗標；現有彙總邏輯據此給 `data_review_required` 並放入 compact issue list，不改訊號分數、門檻或交易規則，不把缺列／隔離列轉成 0。`docs/source_extension.md` 同步說明此狀態語義。
- 唯讀用現有 source-run 記錄呼叫修改後 audit 驗證：process 的 TWSE 季獲利 1,054 raw / 1,049 accepted / 5 quarantine 現帶 quarantine flag；weekend 上市、上櫃未轉讓來源各 1 raw / 0 accepted / 1 quarantine，均帶 flag 與 `no_accepted_records_do_not_infer_zero_activity`。這只驗證稽核輸出會暴露舊回執品質，不代表新自然收集成功。
- TPEx 政府開放資料個股收盤行情唯讀預檢仍為 2026-10-02：普通股範圍 888 列、867 ok、21 quarantine、0 duplicate；上櫃主檔覆蓋 867/892 = 97.20%，4 檔沒有列、21 檔只有隔離列。逐看隔離值：20 列 `Change`/收盤欄為 `---`，另 1 列 `Change=除息`、收盤 24.50、成交股數 342,895。解析器需要數值 `Change` 才接受日漲跌記錄；非數值公司行動標記保留隔離，不推成零漲跌或零交易。上述只是 endpoint 預檢，非自然 source-run 落庫計數。
- 最新自然 after-close 仍是 10/02 20:00 台北失敗；舊 17 筆來源列缺 `dag_id`，不能安全歸屬，當前 registry/runtime 缺 TPEx 市場當沖彙總 feed。週末 10/04 08:00 source collection failed，process 最新仍 10/04 04:00 success/partial；盤中快照仍 10/04 08:04 ready/partial、5 張事件卡、方向家族為 0，該批新聞 10/04 18:30 成功但只能供未來 PIT scan 關聯。成熟 outcomes 仍只到 9/24 horizon-1，不足以調參。
- 修改後 `audit_runs.py` AST parse 通過，host/worker SHA-256 一致；以既有 source-run 唯讀執行 audit 見上述新 flags，`git diff --check` 通過。未跑測試、手動觸發/重跑 DAG、寫業務 DB、發 Discord 或交易；保留其他既有工作樹變更。等待 10/05 工作日自然收集核 effective date、接受／隔離／重複列、上市櫃覆蓋及 after-close 子 run。

## 2026-10-04 20:58 台北：延遲通知的市場背景讀取加 PIT cutoff

- 重新唯讀檢查目前訊號程式：個股候選快照已保存 scan start，但通知格式化時才載入同日漲跌廣度及 TAIFEX 指數期貨／選擇權部位。這三個背景查詢原先只限制有效日，未限制 `source_records.updated_at`；延遲通知重試可能讀到 scan start 後才到達的更正版本。相鄰的 TPEx 當沖背景已用同一 run start cutoff，故此缺口可沿用現有點時做法修復。
- 已將三個背景讀取函式改為同時要求有效日及 `updated_at <= scan_started_at` 對應的 UTC cutoff；缺少 scan start 時回空背景，不填成零。通知中的廣度／期交所部位仍只是非方向背景；候選分數及門檻不變。`docs/source_extension.md` 已記錄這個時間可得性契約。
- 唯讀核對目前資料庫中最近兩筆 daily scan：10/02 scan 的 TWSE/TPEx 同日行情與歷史 TAIFEX 部位沒有任何可見 post-cutoff row；因此本次是修正查詢可納入未來修訂的程式路徑，不聲稱已觀察到歷史通知曾受污染。該檢查不驗證未來自然 run。
- 同時重查狀態：10/04 intraday 快照仍 5 張事件卡、0 個價格／法人流／營收／財務方向家族；最新成熟 outcome 仍 9/24 horizon-1。10/04 週末 TPEx company events 與 revenue 仍因舊 registry `tpex_csv`／新 runtime `tpex_json` contract drift 各失敗；after-close 最新仍 10/02 20:00 台北 failed，17 筆舊 source rows 缺 `dag_id`、不能安全歸屬。需等下一個工作日自然 bootstrap/run 驗證 contract 與通知。
- 此次改動待 AST、呼叫點與 diff 靜態檢查；不跑測試、手動觸發／重跑 DAG、寫 DB、發 Discord 或交易。其他工作樹變更保留。

## 2026-10-04 21:09 台北：PIT 背景修正靜態驗收與自然狀態更新

- PIT 改動已完成靜態驗收：`market_signals.py` AST parse 通過，三個函式各只有一個生產呼叫點且已傳入 run `started_at`，本機與 scheduler 掛載檔 SHA-256 相同（`dd9fea31a1855dea3ee3059be16877b2df2024abeeafbffbb697e2419703c217`）。`git diff --check` exit 0；輸出只有其他既有 dirty 檔案的 LF/CRLF 警告。未跑測試。
- 最新自然 process 仍 `scheduled__2026-10-02T20:00:00+00:00` success/partial：21/21 已登錄來源有回執、runtime 預期 23，缺 TWSE/TPEx 當沖資格來源；普通股主檔上市／上櫃 1,054/892。TWSE 季獲利 1,054 raw / 1,049 accepted / 5 quarantine，資料期 2026-06-30，已被 audit 標 row-quality flag。
- 最新週末 process `scheduled__2026-10-04T00:00:00+00:00` failed：registry/runtime 9/10，缺 TPEx 季財測差異事件；TPEx company-events、monthly-revenue 舊 registry 為 `tpex_csv`，runtime 為 `tpex_json`，兩 feed 各 ReadTimeout、0 raw/accepted。TWSE 月營收 1,086/1,086 accepted，資料期 2026-08；不得將 TPEx 失敗解讀成無公告/無營收。
- 最新 after-close 仍只有 `scheduled__2026-10-01T12:00:00+00:00` failed；audit 為 unknown，因 17 筆舊來源列缺 `dag_id` 無法歸屬，runtime 還缺 `tpex_daytrade_market_summary_daily`。10/04 週末觸發的 intraday 子 run 自身 success，保存 5 個事件候選、價格／法人流／營收／財務方向家族為 0，該 run notification ledger 6/6 有平台 receipt、HTTP 200。最新 news 自然 run 為 10/04 18:30 台北 success（12 feeds、1,197 accepted；三 feed partial，細項見 18:39 記錄）。
- 成熟 outcomes 查詢仍只有 2026-09-24 horizon-1：81 rows、80 directional scores；signal-close 命中率 62.50% / 平均超額 +1.7760pp，next-close 46.25% / +0.9868pp。無成熟 h5/h10，維持描述性監看，不調門檻。
- 下一驗收點為 10/05 工作日自然 process bootstrap 與 20:00 after-close：核 catalog 是否更新兩個 TPEx JSON contract，逐 feed 驗有效日、接受／隔離／重複、上市櫃覆蓋、daily child PIT 背景、outcome 與平台 receipt。未手動觸發 DAG、寫 DB、送 Discord 或交易；其他工作樹變更保留。

## 2026-10-04 21:13 台北：訊號 SQL 點時條件靜態盤點

- 以 SQL 字串為單位盤點 `market_signals.py` 全部 `source_records` 查詢：daily/intraday 候選特徵、公告背景、valuation/index、廣度、TPEx 當沖彙總、TAIFEX 期貨與選擇權 context 都含 `updated_at` 可得時間限制；本輪新增三個遺漏 context 也已涵蓋。
- 沒有 cutoff 的其餘查詢是來源收集問題診斷（按 `observed_at` 範圍取列數）、outcome settlement 讀取已實現收盤／指數資料，及公告候選的證券名稱展示資料；它們不參與方向特徵建構。這是靜態 SQL 檢查，不能替代自然 run/通知回執驗證。
- 另靜態確認目前 `_airflow_lineage()` 會取 `dag_id`、`run_id`、task/map/try 欄位；`_write_run()` 對 success 與 exception/failure 都寫入 `source_runs`。所以 10/02 的 17 筆缺 lineage 列屬舊回執，下一個自然 after-close 可用新列驗證歸屬；仍以實際 DB 回執為準。
- 週日尚未有新的 after-close 自然 run；下一步仍等工作日的 bootstrap 與收盤 child，以實際 row receipt 和 notification payload 驗證契約、PIT context 和送達。

## 2026-10-04 21:17 台北：候選卡揭露底層資料群，避免同源行情被讀成獨立證據

- 以已收集的日線候選快照唯讀盤點：2026-10-02 有 50 檔候選，2026-09-24 有 81 檔。10/02 的 19 檔 `price+revenue+volume` 與 3 檔 `institutions+price+volume`，其三個方向家族各只覆蓋兩個廣義資料群，因價格與量能同用個股日行情；另外 28 檔覆蓋三群。9/24 同樣有 19 檔 `price+revenue+volume`、7 檔 `institutions+price+volume` 落在兩群，其餘 55 檔落在三群。兩日候選均沒有 profitability 或 insiders 方向票。這是輸入來源重疊盤點，不是績效證據；樣本不足以校準門檻。
- `market_signals.py` 現在把每個新日線候選的方向家族歸至行情（價格/量能）、法人流、月營收、季營業利益率、內部人持股等底層群組，將群數/群名/群內方向衝突保存進自動快照；收盤通知卡同步顯示。歷史快照缺此欄時只從已存家族欄位還原，並明示歷史還原。未改三家族入選門檻、方向票、排序、分數或交易規則；資料群僅用於來源診斷，不代表統計獨立。
- `source_extension.md` 已說明這項解讀限制。待 10/05 下一工作日自然 after-close run 驗證快照 JSON 與 Discord payload 的分組欄位，再核通知 receipt；本輪只允許靜態驗證，沒有手動觸發/重跑 DAG、寫 DB、發 Discord、交易或跑測試。其他既有工作樹變更保留。

## 2026-10-04 21:47 台北：自然批次複核與通知失敗例外類別遙測

- Live Airflow 唯讀 run list：最新 process `scheduled__2026-10-02T20:00:00+00:00`（10/04 04:00 台北）success，但來源 audit partial，21/21 已登錄來源有列、runtime 預期 23，缺 TWSE/TPEx 當沖資格 feed。最新 weekend `scheduled__2026-10-04T00:00:00+00:00`（08:00 台北）failed，正確 `intraday` audit 為 9/9 登錄來源、runtime 10，缺 TPEx 財測差異來源；TPEx 重大訊息/月營收契約仍與 runtime 不符，兩者各 ReadTimeout、HTTP 未取得、0 raw/accepted。TWSE 月營收 1,086/1,086 accepted、effective month 2026-08，1,054 個上市普通股代碼匹配，32 個非普通股/未匹配列保留為 unmatched。上櫃未轉讓轉讓列 1 raw、0 accepted、1 quarantine，不解讀為零事件。
- process 季度來源：TWSE 季營益 1,054 raw / 1,049 accepted / 5 quarantine、資料期 2026-06-30；accepted code 1,018 匹配上市普通股、31 個 accepted code 在目前普通股主檔之外。TPEx 季營益 884/884 accepted、884 個上櫃代碼匹配 892 檔主檔。TWSE 股利 1,228/1,228 accepted、最新有效輸出日 2026-09-21；TPEx 股利 2,483/2,483 accepted 但最新有效輸出日僅 2021-08-03，仍帶 stale freshness flag，不能因 accepted 多就當成新鮮訊號。
- 日線 DB 快照仍是 10/02 `ready/partial`：普通股 universe 1,946、舊 price row-code count 1,938、flow 1,891、50 candidates、`notified_count=0`。股票歷史表同日最新行情列：上市 1,050/1,054 檔有列且收盤有效；上櫃 888/892 有列，其中 20 列收盤缺值，另 4 檔無列。官方 reader 的來源品質預檢另為 867/892 accepted、21 quarantine、4 absent；不能把 legacy row count 當有效收盤數。DB schema migration 仍為 v9，新增的 per-market coverage table 尚不存在。
- After-close parent 仍為 `scheduled__2026-10-01T12:00:00+00:00`（10/02 20:00 台北）failed；daily child 的 `capture_market_signal_snapshot` success、`notify_market_signal_snapshot` 三次嘗試後 failed。17 筆舊 `source_runs` 缺 `dag_id`，audit 拒絕歸屬；runtime 18、catalog 17，缺 `tpex_daytrade_market_summary_daily`。daily notification ledger 對該 run 有 `sent` 但無 receipt 與 `failed` 各 1 列，兩列均無 HTTP status，均不算已驗證送達。當前週末 intraday 子 run 10/04 08:04 `ready/partial`，5 個事件候選、0 個市場方向家族；該次通知帳本 6/6 有 receipt、HTTP 200。
- 最新新聞自然 run `scheduled__2026-10-04T08:30:00+00:00`，10/04 18:30–18:38:56 台北 success；XCom 為 12 feeds / 1,220 URLs / 1,219 parsed-valid-time / 1,197 accepted / 1 fetch-parse reject / 0 invalid-time quarantine / 22 空白內容 reject。三立 29/30、自由 77/98、鉅亨 149/150 為 partial。48 小時 cutoff 為 10/02 18:38:47；iThome 與 TheNewsLens 的日期級時間混合邊界未知及舊列，未將午夜日期補成時分。此批晚於既有日線掃描，不回填舊候選。
- 成熟 outcome 仍只有 2026-09-24 horizon-1：signal-close 81 列/80 scored/50 hits/平均超額 +1.776pp；next-close 81/80/37/+0.987pp。未扣成本、無成熟 h5/h10，不據單一 cohort 調門檻。最新日線尚未帶入上一筆 source-group 診斷，需等下一次自然日線。
- 追查收盤通知失敗 log：三次 Airflow attempt 都只以通用 `daily digest notification was not delivered` 結束，通知帳本沒有 HTTP status、receipt 或錯誤分類，無法分辨網路例外與其他通知問題。已為 notification ledger 加 `notification_error_type` additive migration v11；Discord wrapper 只回傳安全的例外類別，通知帳本不存例外全文、Webhook 或憑證。下一次自然 schema bootstrap 後，先驗 v10 覆蓋表及 v11 欄位，再由自然通知確認 error class/HTTP/receipt 各欄位；未手動套 schema、重跑 DAG、發 Discord、交易或跑測試。


## 2026-10-04 22:03 台北：全市場月營收與季獲利加入自動覆蓋率稽核

- 重新讀取目前 Airflow scheduler/worker 與 crawler DB（SELECT-only）。最新 process 仍為 `scheduled__2026-10-02T20:00:00+00:00`、Airflow success 但 audit partial：21 個已登錄來源、runtime 23，缺 TWSE/TPEx 當沖資格來源。最新 weekend 為 `scheduled__2026-10-04T00:00:00+00:00` failed：catalog/runtime 9/10、缺 TPEx 季財測差異事件；TPEx 重大訊息及月營收當次 catalog 為舊 `tpex_csv`，runtime 已為 `tpex_json`，兩者 ReadTimeout 且 0 raw/accepted。程式每次自然收集前會跑 `ensure_schema_and_catalog`，待 10/05 自然排程驗證更新後的新契約。
- latest 日線快照仍為 10/02 `ready/partial`、1,946 universe、1,938 舊 price row-code count、1,891 flow、50 candidates、notification ledger 未列已通知；10/04 最新 intraday 為 5 張事件觀察候選、price/flow/revenue/financial 均 0、通知計數 5。10/02 收盤後子 run 的最新 after-close audit 仍因 17 筆缺 `dag_id` 的 legacy source rows 回 unknown，不把它們錯歸；runtime 18、catalog 17，缺 `tpex_daytrade_market_summary_daily`。
- 成熟 outcomes 仍只有 9/24 horizon-1：兩個 entry basis 各 81 rows、80 directional scores；signal-close 50/80 hits，next-close 37/80 hits；無成熟 h5/h10 或成本調整資料，不據此調門檻。最新 news XCom 是 10/04 18:30–18:38:56 台北自然 run success：12 sources、1,220 URLs、1,219 valid-time、1,197 accepted、1 fetch/parse reject、22 空白內容拒收、0 invalid-time quarantine，拒收核帳 reconciled。新聞 run 晚於 10/04 08:04 intraday scan，不回填該快照。
- 發現當前自動 run audit 對完整市場的 TWSE/TPEx 季獲利與月營收 feed 沒有 common-equity denominator，雖能列 accepted/matched 數，`market_coverage` 仍為 null。已在四個 registry contract 加 `common_equity_only` coverage flag；未啟用 `filter_to_stock_master`，故不刪非普通股接受列，也不影響方向特徵。文件說明兩個 flag 的差別。
- scheduler 上 AST parse 成功並讀到四個來源的新設定；local 與 worker 的 `registry.py` SHA-256 均為 `d3f0d85f3bb4922c7531e19543d5f081b124a9b25120d595ac3f5bbd7f5ea16e`。此為靜態檢查，無自然執行證明。等 10/05 自然 process/intraday bootstrap 驗 catalog、每來源 raw/accepted/quarantined/duplicate/effective date 及上市櫃 coverage。未跑測試、手動 DAG、DB 寫入、Discord 或交易；其他既有 dirty work 保留。

## 2026-10-04 22:07 台北：覆蓋率實算、來源目錄待自然同步與收盤送達回查

- 以目前 runtime 定義重跑唯讀 audit，對既有 source rows 回算目前普通股主檔覆蓋：TWSE 季獲利 1,018/1,054 = 96.58%（1,054 raw、1,049 accepted、5 quarantine、31 accepted codes unmatched）；TPEx 季獲利 884/892 = 99.10%；TWSE 月營收 1,054/1,054 = 100%（1,086 accepted、32 unmatched）；TPEx 月營收此次 failed、0 raw/accepted，沒有覆蓋分母可算。覆蓋率以目前 master 為分母，不是 PIT universe；非普通股列保留，不當作零。
- 上述 registry 設定尚未進 DB：當前 audit 顯示舊 process catalog 對兩個季度獲利、weekend 對 TWSE 月營收及兩個 TPEx JSON feed 有 contract drift。下次自然收集先跑 ensure_schema_and_catalog，再驗 catalog/runtime 一致與 source run 正常收集；沒有對 DB 手動修正。
- Airflow metadata 唯讀核對 after-close parent 仍 failed，trigger 的 daily child 已建立：capture success、notify 三次 retry 後 failed。該 daily run ledger 一筆 sent 與一筆 failed，兩筆皆無平台 receipt 或 HTTP status。schema migrations 在 crawler DB 仍只到 v9；程式的 v10/v11 只能等自然 bootstrap 驗證。
- 本次沒有測試、手動觸發 DAG、DB 寫入、Discord 發送或交易。

## 2026-10-04 22:39 台北：自動標記大盤基準與訊號掃描日不一致

- 唯讀核對目前 after-close/日線資料：訊號掃描使用的 `stock.個股成交資訊` 最新日期（截至 source run 日 2026-10-02）是 10/02；官方指數來源目前最新有效日為 TAIEX 10/01、TPEX 10/02。10/02 after-close 的 TWSE 指數 mapped task 雖 success 且 1/1 accepted，卻落後掃描日；TPEx 指數 2/2 accepted 並對齊。上櫃個股行情批次失敗，不能用缺列補零。市場訊號程式已因 TAIEX 最新日未對齊 as-of date 而不提供上市相對報酬視窗；收盤摘要覆蓋欄也會顯示基準日期未對齊。
- 在 `audit_runs.py` 增加 after-close 的唯讀跨資料檢查：用 source run 日期界定 `stock.個股成交資訊` 的 `MAX(資料日期)`，再比對 TWSE/TPEx 指數來源的 accepted `effective_date_max`；日期不同時將 `market_index_not_aligned_to_stock_price_snapshot` 加到指數來源品質旗標，並把比較基準、兩日期及查詢狀態送入既有 compact XCom issue。`dag.py` 一併保留該診斷欄。這只讓來源成功但日期落後可自動複核，不影響候選分數、門檻或交易規則；不把缺資料當零。
- Worker 上 `audit_runs.py`、`dag.py` AST 靜態解析通過、無尾端空白，且本機與 worker SHA-256 相同。這不是新自然執行驗證；舊 10/02 source run 缺 `dag_id`，audit 仍不可安全回溯歸屬。待 10/05 自然 after-close 驗 `effective_date_alignment` 與 issue flag。沒有跑測試、手動觸發/重跑 DAG、寫業務 DB、發 Discord 或交易。

## 2026-10-04 22:43 台北：再次確認交付是全市場自動訊號

- 使用者再次指出工作不可偏成逐家公司搜尋、人工評比或整理券商報告。後續每次工作以可排程、可批次取得並可重跑的全市場來源為核心；成功收集仍須核對有效日、接受/隔離/拒收/重複列、上市櫃覆蓋、點時關聯、成熟 outcomes 與實際通知回執。
- 券商研究只有在明確授權、可機器化取得全市場資料且具歷史版本/發布時間時才列為 feed 候選。此輪只記錄範圍校正，沒有新的自然 run 可驗（週日；下一驗收點為 10/05 工作日排程），不把舊證據重述成新進度。

## 2026-10-04 23:01 台北：TDCC 大戶變化的自動歷史面板結果

- 在 Airflow worker 執行既有唯讀 stock_major_holder_panel_research.py；SQL 範圍為 2023-01-01 至 TDCC effective date 2025-04-25 的全市場快照及 2025-12-31 前日線。108 個快照符合至少 95% 已存列在有效日後 0–7 天入庫；平均即時入庫比例 100%。最新舊表日期 2025-04-25，有 1,851 個代碼；按目前普通股主檔匹配為上市 1,015/1,054（96.30%）、上櫃 836/892（93.72%）。這是用目前主檔回配的歷史覆蓋，不是歷史上市櫃 universe，也不是當前快照品質。
- 待拆分布驗證的欄位為 >400_lot 持股占比週變動低於 -2pp；入場採入庫日後首個有效收盤，超額相對每個有效日期同面板平均報酬。h5 等日期平均/中位 excess：2023（41 dates）+0.1793/+0.0349pp、2024（48）+0.0862/+0.4650pp、2025 至 4/25（13）+0.4270/+0.2324pp。研究腳本的 directional_hit_rate 對此負向 bucket 計算個股報酬低於同日面板平均的比例，三年為 57.09%、56.06%、52.46%；它與正的日期平均超額是不同統計層級且結論互相牴觸，顯示尾端分布效應，不能稱為正向或穩定方向訊號。只保留已合格候選卡片上的持股變化事實，不作方向解讀或加分；如要升格，先查每檔/每日的 excess 分位數與公司行動調整結果，再用新資料作 out-of-sample。
- >1000_lot 持股增加超過 2pp 的 h5 命中分別 40.94%、44.26%、40.03%，平均 excess 為 -0.3132、+0.0339、-0.8880pp；方向也未形成穩定可加權依據。整體仍是描述性候選：週資料與後續報酬窗重疊、只用當前證券主檔、未做公司行動/交易成本調整、2025 僅局部年份，故不調分數或門檻。既有通知可把持股變化保留為合格候選的背景，不單獨新增候選。
- 完整 54 組年度/持股門檻/方向/期間彙總保存在 docs/stock_major_holder_panel_results_20261004.json。同一 stock.大戶比例表的 live 最新有效日仍是 2025-04-25。官方 TDCC OpenAPI 修復後的星期一 09:00 自然 refresh DAG 尚無 run；待 10/05 回執核 effective date、raw/accepted/quarantine/rejected、上市櫃 coverage 和入庫時差後，再延伸近期樣本。沒有手動觸發 DAG、DB 寫入、Discord 或交易。

## 2026-10-04 23:10 台北：TDCC 官方全市場端點自然排程前預檢

- 在 Airflow worker 直接呼叫既有 StockMajorHolderCollectProcess.collect() 做唯讀端點/正規化預檢，沒有呼叫 process()/save()；沒有 source_runs 回執或 DB 寫入。官方 OpenAPI 目前回傳有效日 2026-10-02，與個股日行情最新日同日，market_date_lag_days=0。
- 整份回應 69,326 rows；篩入目前上市櫃代碼 33,048 rows，轉換成 1,944 個代碼摘要（剛好每個摘要代碼有 17 個級距）。上市 1,054/1,054，TPEx 890/892，兩市場均通過程式的 95% 覆蓋閘門；兩個缺列代碼維持 missing，不補零。這些是 in-memory batch/summary counts，不等同 accepted/quarantined/rejected 持久化列數。
- Airflow 中 Monday refresh DAG 的 run list 目前仍空白；本地 stock.大戶比例表 latest effective date 仍是 2025-04-25。10/05 09:00 自然 run 後再核正式 source_runs、落庫 effective date、接受/隔離/拒收數、market coverage 及新聞/候選 PIT cutoff。預檢只證明端點此刻可用，不替代收集回執；未手動觸發 DAG、寫 DB、發 Discord 或交易。

## 2026-10-04 23:35 台北：即時營收卡改用精確前月與三月合計背景

- 檢查 `_load_intraday_candidates` 發現前月年增比較原本取「最近三個月內最新一筆較早營收」；若前一曆月缺列，兩個月前的年增可能被當成前月，並顯示成月增/減百分點。改為只和精確前一曆月比較；缺資料時不以較早月份代替。
- 即時月營收卡補入同一官方上市或上櫃批次來源的近三個連續月份合計年增及官方累計年增背景。計算只讀取 scan start 前已發生且 source/field `updated_at` 不晚於 PIT cutoff 的記錄；三個月份或去年同期金額缺漏/基期無效時顯示無法計算及原因，不補零。
- 這是解讀背景與前月資料正確性修正，不改當月 +30%/-20%、前月變化 ±15pp 的既有觸發門檻，不改方向判讀、候選排序或交易規則。`docs/source_extension.md` 已記錄新卡片語義。
- Host/worker `market_signals.py` AST 靜態解析通過、SHA-256 相同、無尾端空白。沒有跑測試、手動觸發/重跑 DAG、寫業務資料庫、發 Discord 或交易；待 10/05 07:00 後第一個自然盤中收集/掃描驗收雙市場月營收、effective month、PIT baseline 狀態及卡片摘要。

## 2026-10-04 23:39 台北：TPEx 全市場財務風險提示批次端點資格缺口

- TPEx 官方 FAQ 將「財務及交易資訊重點專區」說明為對 MOPS 已揭露資訊的彙整/提醒，並指向 MOPS 專區查詢；本輪直接抓取官方 TPEx OpenAPI Swagger，225 條 operations 的路徑/摘要關鍵字掃描未找到明確的財務重點清單或營業現金流批次 endpoint。已登錄的一般季度財報 endpoint 是原始財報資料，不能代替專區完成的市場風險旗標。[TPEx FAQ](https://www.tpex.org.tw/zh-tw/about/company/faq.html)｜[TPEx OpenAPI](https://www.tpex.org.tw/openapi/swagger.json)
- 專區可能涵蓋高價值、可全市場計算的長期現金流/財務警示；但本輪沒有核實現行各項門檻、批次傳輸契約、機器存取授權、上市櫃覆蓋、發布/生效時間或歷史版本，故只列為 feed qualification candidate。不得接 MOPS 個股頁面路徑，也不當成方向訊號。
- 後續資格條件：找到官方授權的市場批次檔/API、逐列有效/發布時間及版本、可核上市櫃分母和來源定義；缺任一條件就保留為來源缺口。沒有新增個股或券商報告調查。

## 2026-10-05 01:07 台北：再次校正交付為自動化訊號

- 使用者指出先前把券商來源資格研究做成逐家線上調查，仍偏離需求。後續工作以程式化、排程化、全市場批次訊號的實際改善為主：來源契約與授權、自然收集、有效日、接受／隔離／拒收／重複、上市櫃覆蓋、PIT 關聯、成熟 outcomes 和通知 receipt。不可把人工查網站或整理報告當成進度。
- CMoney M037/Hunter 的批次資格與缺少訂閱/API/歷史版本/PIT/使用權證據已列在既有追蹤；本次不再重複瀏覽產品頁。只有出現可程式化取得的全市場授權資料，才重新開啟此候選；否則視為來源缺口，先延伸已接入的官方批次 feed。
- 最新證據仍是 01:03 台北盤點：下一自然 checkpoint 為 04:00 process、05:30 news、09:00 TDCC、20:00 after-close。新 run 尚未到時，不手動觸發或重跑；等自然回執後更新驗收。這次只記錄工作範圍更正，沒有改程式、跑測試、寫業務 DB、送 Discord 或交易。

## 2026-10-05 01:20 台北：自動稽核有成交但法人列缺漏

- 唯讀查核 2026-10-02 日行情、近 7 日 `stock.三大法人買賣` 與目前上市櫃主檔：上市 1,054 檔中 1,050 有效收盤、1,043 檔有近 7 日法人列，另 3 檔有效收盤且成交量大於零但無法人列；上櫃 892 檔中 868 有效收盤、788 檔有近 7 日法人列，另 40 檔有成交但無法人列。近 7 日法人覆蓋為上市 98.96%、上櫃 88.34%。無資料列一律是未知，不補零。
- 為區分端點未回傳與本地落庫遺漏，使用現有 TPEx 官方全市場 `dailyTrade` 批次端點唯讀查 2026-10-02 (`type=Daily`, `sect=AL`)：HTTP 200，首表 6,190 列/代碼，其中 788 代碼匹配目前上櫃普通股主檔。當日有成交但本地沒有同日法人列的 83 檔全都不在官方回應；因此目前證據支持「端點當日無該列」，不支持「本地抓取漏列」，也不能推成無法人交易。40 檔則在掃描採用的近 7 日視窗內都沒有來源列。
- `market_signals.py` 新增上市/上櫃「有效收盤且成交量 > 0、近 7 日無法人來源列」數量，保存於市場覆蓋表並放進日線摘要/來源問題；歷史未有該欄的列保留 NULL。`schema.py` 加 additive migration v12。僅改善自動品質揭露，不改候選門檻、投票、方向、排序或交易規則。
- 唯讀歷史樣本仍只有 2026-09-24 一個成熟 h1 訊號日：signal-close 與 next-close 各 81 列、80 個可計方向；family 分組也都只有一個訊號日，不能拿來調門檻。10/02 50 檔候選尚未有成熟未來報酬。
- Airflow scheduler container 對 `market_signals.py`、`schema.py` AST 解析通過，`airflow dags list-import-errors --output json` 為 `[]`。這不是新自然批次或通知回執；v12 尚待自然 schema bootstrap 驗收。沒有跑測試、手動觸發 DAG、寫業務 DB、送 Discord 或交易；其他既有工作樹變更保留。

## 2026-10-05 01:30 台北：上櫃股利批次端點已有新資料，待自然收集落庫

- 最新已持久化 `source_runs` 仍是 `source_extension_collect_process / scheduled__2026-10-02T20:00:00+00:00` 的舊回執：HTTP 200、2,483 raw/accepted/0 quarantine/0 duplicate；目前 `tpex_dividends` source_records 的 effective date 仍截至 2021-08-03。不能因舊任務綠燈就判斷當前端點資料新鮮。
- 對現行 allowlisted 官方 MOPS TPEx CSV 端點做未落庫唯讀預檢：HTTP 200、963 raw rows，現有 `fetch_and_normalize` 正規化 963 accepted/0 quarantine；生效日範圍 2026-01-29–2026-10-02，發布/出表日為 2026-10-04，892 個代碼中 890 個匹配目前上櫃主檔（99.78%）。這是 endpoint/normalizer 預檢，不是 `source_runs` 回執，也不能替代自然收集的上市櫃品質核帳。
- 現行 CSV 欄位包含每股現金/股票股利、擬議分派與股東會日期，沒有可直接當成個股除權息交易日的欄位；只可供公告/股利脈絡及後續公司行動資料接合，不以股東會日代替除權息日，也不因此改方向或評分。TWSE 日行情開放資料列的是原始日 OHLCV，不是個股還原報酬。[政府資料集 11549](https://data.gov.tw/dataset/11549)
- 下一驗收點為 10/05 04:00 台北自然 process：核 `tpex_dividends` catalog contract、自然 source_run row/accepted/quarantine/duplicate/effective/event date、890/892 目前預檢覆蓋能否由持久化回執重現，以及後續候選卡片 PIT 是否使用 scan cutoff。沒有手動觸發 DAG、寫業務 DB、送 Discord 或交易；未跑測試。

## 2026-10-05 01:45 台北：兩交易所除權息公告接入自然批次與候選脈絡

- 使用者再次校正範圍：交付是可自動化、可批次抓取的全市場訊號，不是逐家公司線上調查/評比報告。此次新增兩個官方 OpenAPI 批次定義，排入 `daily_after_close`：TWSE `TWT48U_ALL`（資料集 89748，目錄標示不定期更新）及 TPEx `tpex_exright_prepost`（資料集 11634，目錄標示每日更新）。兩資料集免費、政府資料開放授權第 1 版；TWSE 即使排程輪詢成功，也仍需 freshness gate，不能當作發布端每日更新證據。
- Airflow worker 唯讀端點預檢：TWSE HTTP 200、58 raw rows/58 個代碼/0 重複代碼日期鍵、58 列具日期/代碼/行動欄，日期 2026-10-05–10-28；10 個代碼匹配目前 1,054 檔上市普通股主檔，另 48 不匹配。TPEx HTTP 200、80 raw rows/78 個代碼/0 重複鍵、80 列具日期/代碼/行動欄，日期 2026-09-23–11-05；16 個代碼匹配目前 892 檔上櫃普通股主檔，另 62 不匹配。這些是有事件列的代碼匹配數，不是來源完整性或母體覆蓋率；不是 source run，也沒有資料庫落庫的 accepted/quarantine/rejected 數。
- `adapters.py` 加固定 endpoint allowlist 與公司行動正規化；缺少的配息/配股欄仍為 NULL，公告 feed 沒有可信發布時刻所以 `event_time` 留空。`market_signals.py` 只會把掃描 cutoff 前已入庫、30 日內將發生的匹配事件放進候選背景；通知卡明示只作事件脈絡、不計分。沒有新事件列不表示沒有公司行動，也不調方向家族、分數、候選門檻或現有 raw-close outcomes。
- Airflow worker 對三個修改模組 AST 解析成功；57 個 runtime source definitions 中 `daily_after_close` 為 22 個，低於每批 32 個上限；兩個新來源契約均命中 allowlist。Scheduler `airflow dags list-import-errors --output json` 回傳 `[]`。未跑測試，未寫 DB/手動觸發 DAG/送 Discord/交易；保留其他既有變更。待 10/05 自然 after-close source run 驗 catalog、HTTP、有效日、accepted/quarantine/rejected/duplicate、匹配代碼和 candidate PIT 卡片，再決定 outcome 是否具備公司行動校正條件。

## 2026-10-05 01:50 台北：自然執行基線複查與 TPEx 行情分段收取預檢

- 目前沒有 10/05 新自然批次。最新 daily process 仍是 10/04 04:00 台北：21/21 catalog sources 有回執，source audit `partial`，runtime 預期 23；缺 TWSE/TPEx 當沖資格來源，TWSE/TPEx 季獲利來源 contract drift。逐來源 effective-date/列數仍須按 audit 記錄解讀：TPEx 股利 2,483/2,483 接受但最新有效日 2021-08-03；TWSE 股利 1,228/1,228、有效日截至 2026-09-21；TWSE 季獲利 1,054 raw、1,049 accepted、5 隔離、1,018/1,054 上市普通股匹配（96.58%）；TPEx 季獲利 884/884、884/892 上櫃普通股匹配（99.10%）。新除權息來源及 v11 通知錯誤類型欄尚未進 DB catalog/schema，等 04:00 自然 bootstrap 驗證。
- latest after-close parent 仍為 10/02 20:00 台北 failed，mapped index 9 是 `tpex_equity_quotes_daily`；舊版兩次 HTTP status 為 NULL，分別因 chunked response 在約 0.7 MB/4.2 MB 截斷而失敗。現行 worker 分段 adapter 只讀預檢已成功：HTTP 206、72 requests、4,666,641 bytes 完整、11,928 raw rows；篩到 892 檔現行 OTC 普通股後 888 rows/888 codes，其中 867 可接受、21 隔離、4 個主檔代碼無列，effective date 2026-10-02。這是 adapter 預檢而非 `source_runs`；等 10/05 20:00 自然 after-close 驗 source receipt。舊 after-close 的 daily child snapshot 是 10/02 `ready/partial`、50 候選；通知 task failed，ledger 有 sent/failed 兩列但均無 platform receipt 或 HTTP status，且現行 DB 尚無 `notification_error_type` 欄位。
- latest daily snapshot 仍是 10/02 `ready/partial`：1,946 universe、1,938 price、1,891 flow、1,945 revenue、1,902 financial、1,795 borrow、1,944 insider、1,053 dividend、50 candidates。latest intraday 為 10/04 08:04 台北 `ready/partial`：5 張事件觀察候選，方向家族行情/法人/營收/財務等全為 0；自然通知 6/6 有 receipt、HTTP 200。不能把事件卡視為價格確認方向訊號。
- mature outcomes 仍只有 2026-09-24 一個 h1 signal date：signal-close 81 rows/80 directional、平均超額 +1.7760pp；next-close 81/80、+0.9868pp；無成熟 h5/h10，不調門檻。News latest natural run 10/04 23:40 台北成功：12 feeds、1,298 URLs、1,297 valid-time、1,270 accepted、1 fetch/parse reject、0 invalid-time quarantine、27 content/duplicate rejects。自由時報與三立為 partial；iThome 有 20 篇 cutoff 日期但時分未知、38 篇按日期已舊；TheNewsLens 為 6 篇時分未知、82 篇已舊。此新聞批次晚於 10/02 daily scan，不回填舊快照。
- 本次只讀查 Airflow metadata/XCom、crawler/stock DB 與 TPEx 官方 API；未手動觸發 DAG、寫 DB、發 Discord、交易或跑測試。其他既有工作樹變更保留。

## 2026-10-05 02:47 台北：重新校正為可自動抓取的全市場訊號

- 使用者再次指出工作目標是能排程、程式化批次抓取、覆蓋全市場並累積 outcomes 的訊號；逐家公司人工瀏覽/評比券商報告不符合交付。券商研究只在找到有授權、可批次抓取的來源後才納入；否則記為來源缺口，不以人工逐家調查替代自動化。
- 依此範圍，以既有上市/上櫃日線和法人資料做可重跑的唯讀全市場研究，程式為 `docs/institutional_flow_intensity_research.py`，輸出為 `docs/institutional_flow_intensity_research_20261005.json`。涵蓋訊號日 2023-01-01 至 2026-10-02，使用目前 1,946 檔普通股主檔；初版摘要曾誤記「1,706 個市場/日期通過、80 個排除」；該數字已作廢，正確 PIT 覆蓋數見後續 03:26 修正版（88 通過、1,698 排除）及 11:12 worker 重算。
- 現有法人偏空規則（近 5 日至少 4 日同向、5 日淨流出至少 10 萬股）在 5-session 結果上，上市及上櫃 2023、2024、2025、2026 YTD 的 signed equal-date excess 均為正。上市按年度落在 +0.004 至 +0.1678 個百分點， pooled signed hit 54.82%–57.03%；上櫃 +0.0831 至 +0.2132 個百分點，hit 57.13%–58.42%。這是值得繼續自動追蹤的既有候選，不代表扣成本後可交易；10-session 2025 兩市場 signed excess 轉負，故不因這個子樣本調方向、分數或門檻。
- 新測的「5 日淨流量 / 前 20 日平均成交量」市場內十分位沒有跨市場、跨年度穩定；例如偏多十分位上市 2026 H5 signed excess 為 -0.2638pp，上櫃偏空十分位 2026 為 -0.0148pp，暫不升格成方向票。回測仍有目前主檔存活者偏差、歷史成員未重建、未做除權息/拆併股調整、未扣交易成本與滑價、`create_time` 只是 DB 入庫時間且舊資料修訂未版本化等限制；後續以批次方式加入成本/公司行動/PIT 母體驗證與 walk-forward，再決定是否改通知。
- 自然狀態唯讀複查（台北 02:47）：process 最近仍是 10/04 04:00 success，但 source audit `partial`（21 個已登錄來源/23 個 runtime 預期）；weekend 10/04 08:00 failed。盤中最新 10/04 08:04 `ready/partial` 是 5 張事件觀察卡，行情/法人/營收/財務母體列為 0；該批通知 receipt 6/6 且 HTTP 200。日線仍是 10/02 `ready/partial`、1,946 universe、1,938 price、1,891 flow、50 candidates、0 notified。成熟 outcomes 仍只有 9/24 的 H1：`signal_close` 81 列/80 scored/命中 62.50%/均值超額 +1.7760pp，`next_close` 81/80/46.25%/+0.9868pp；無成熟 H5/H10。After-close 10/02 20:00 parent 及 `market_signal_notify` child failed；capture snapshot task 成功但通知 task 重試三次仍失敗，該批 ledger 沒有 receipt。
- 新聞最新自然 run 10/04 23:40 success：12 feeds、1,298 URLs、1,297 parsed/valid-time、1,270 accepted、1 fetch/parse reject、0 invalid-time quarantine、27 content/duplicate rejects。iThome 有 20 篇落在 cutoff 日期但時分未知，TheNewsLens 有 6 篇時分未知；這些日期粒度資料不能當成精確發稿時間或小時級新鮮度。批次晚於 10/02 日線掃描，只能在後續 scan 依發稿時間精度與 scan cutoff 做 PIT 關聯，不回填舊卡。當時尚未到 10/05 04:00 process、07:00 盤中、09:00 TDCC 或 20:00 after-close 自然時點。
- 本次僅唯讀查詢與產生研究 JSON；沒有改生產分數/門檻、寫業務 DB、手動觸發/重跑 DAG、送 Discord、交易或跑測試。未觸碰其他既有工作樹變更。

## 2026-10-05 03:12 台北：加入行情點時閘門並撤回未驗證動能分組

- 延伸既有批次研究，加入「法人偏空 + 前 5/20 日個股相對大盤動能同向/背離」分組；第一版只用法人 `create_time` 閘門，結果看似 H5 跨年一致。之後發現舊行情資料也有 `create_time`，且歷史樣本不能假設訊號日收盤當時已入庫。因此本節以下為修正版，先前 02:47 的 H5 流向績效只保留為未具價格點時證明的探索結果，撤回其「跨年候選」解讀；不據舊值通知或調門檻。
- 已調整 `docs/institutional_flow_intensity_research.py`：母體限定目前上市/上櫃 `ES*` 普通股，依證券主檔上市日剔除尚未上市期間；行情訊號日和回看價格、法人流資料都要求其 UTC-naive `create_time` 嚴格早於次一交易日台北 09:00。資料庫 session/system 時區為 `SYSTEM` 且 `NOW()=UTC_TIMESTAMP()`；1,946 檔現行主檔上市日皆可解析。下市日及歷史轉市場資料仍不存在，存續者偏差未解。
- 修正版回測涵蓋 2023-01-01 至 2026-10-02 共 1,786 個上市/上櫃市場日期窗；只 88 窗通過價格與法人各自 70% 覆蓋閘門，1,698 窗排除。法人點時列比例各市場/年度仍為 99.40%–100%，但訊號價格的「次日開盤前已入庫」覆蓋中位數各市場/年度皆為 0.00%；說明 legacy 行情表中的歷史列不能普遍證明其當時可得。存活/點時修正版每年樣本約只有十餘日期；流向背離組雖部分格子的平均超額仍正，但逐日方向率與中位數有反轉、極端值影響明顯，不足以升格新通知訊號。
- `stock.個股成交資訊.create_time` 跨歷史年份顯示明顯晚於行情日期的列：2023 資料日期最晚入庫延遲 1,050 日、2024 為 815 日、2025 為 420 日、2026 為 164 日。這是舊資料庫時戳，不是可用的歷史官方發稿時間；除非能由來源 run/修訂版證明，不能用來回填 PIT。
- 官方批次行情資格複核：[TWSE 個股日成交資訊 dataset 11549](https://data.gov.tw/dataset/11549) 與 [TPEx 上櫃股票行情 dataset 11370](https://data.gov.tw/dataset/11370) 均列每日更新與政府資料開放授權第 1 版。官方 [TWSE OpenAPI](https://openapi.twse.com.tw/v1/swagger.json) 的 `/exchangeReport/STOCK_DAY_ALL`、[TPEx OpenAPI](https://www.tpex.org.tw/openapi/swagger.json) 的 `/tpex_mainboard_daily_close_quotes` 與 `/tpex_mainboard_quotes` 都沒有文件化的日期參數；它們可支持自然全市場日批次，但不證明可用同一契約重建歷史 PIT。TPEx 官網按證券代碼/月查歷史價的互動查詢不作批次回測替代。下一步需沿自然每日行情回執累積版本化 PIT 樣本，或只評估有明確授權與點時/退市歷史的全市場批次供應；不逐股查詢。
- 重新計算後 JSON 已更新為 480 個訊號/市場/年度/期間彙總列，包含逐日組合均值為正的日期率。此輸出仍是研究檔而非 `market_signal_outcomes` 落庫結果。未改生產訊號、分數、門檻或 Discord 卡片；沒有寫 DB、觸發 DAG、送 Discord、交易或跑測試。10/05 03:12 尚早於自然 04:00 process、07:00 盤中和 20:00 after-close。

## 2026-10-05 03:18 台北：自動訊號來源修訂版本留存缺口

- 依使用者再次校正，本目標只做可排程、可批次抓取的全市場訊號工程；不把逐家公司券商報告網頁調查或人工評比當成工作流。券商研究只在可驗證授權、批次 API/檔案契約、全市場覆蓋及點時版本後才列為機器來源候選；否則記來源缺口。
- 程式碼檢查發現 `adapters.py::_record_hash` 只以 `source_key|record_key` 計算唯一雜湊；例如日行情業務鍵含市場、代碼及有效日。`registry.py` 對相同 `record_hash` 以 upsert 原地更新 `payload` 與欄位，並將 `updated_at` 改成最後內容變更時間。故相同業務鍵若被來源更正，現有表保留最新內容但無法還原舊版；`updated_at` 只能作變更時間，不能重播更正前 PIT 值。少數 adapter（如部分季財測版本）會把內容 fingerprint 納入 `record_key`，但不是通用修訂歷史機制。
- 這會限制舊行情／事件來源的 PIT 回測；不可將現存 `source_records` 的歷史 payload 當成當時真實版本。後續改善候選是增 append-only 的版本/自然 source-run observation，再以觀測時間和來源 run 對 scan cutoff 做 as-of 選版，同時保留 current view；此牽涉 schema 與讀取路徑，先盤點下游使用及 run lineage，未直接改正式 schema、評分或通知。
- 本輪僅檢查 repo 程式及追蹤文件，尚未到 10/05 04:00 自然 process；沒有手動觸發 DAG、寫業務 DB、送 Discord、交易或跑測試。下個自然驗收仍需分開記錄 process/after-close 回執、來源有效日、接受/隔離/重複數及上市櫃覆蓋。

## 2026-10-05 03:35 台北：量化修訂風險並加入自然收集版本留存

- Airflow worker 對 crawler DB 的唯讀品質查詢：`source_records` 目前 107,667 列，其中 67,396 列（62.60%）`updated_at > created_at`。這是更新時間 proxy，提示需要核對版本；單靠這欄不能證明 payload 或訊號數值改變。訊號相關列中，上市/上櫃月營收的 proxy 比率各為 100%（3,253/3,253、2,673/2,673；最大時間差 29/30 日）；上市/上櫃季度獲利亦各為 100%（1,054/1,054、885/885；8/9 日）；重大訊息為約 12.9%/12.3%；現有上市日行情 1,051 列為 0%。以上是更新時間分布，不能直接解讀為數值修訂率或訊號變化率。
- `source_schema_migrations` 目前只記到 v9；工作樹 schema 已有 v10–v12，故 10/05 自然 schema bootstrap 還須確認 v10–v13 是否實際套用。Airflow 唯讀 run list 顯示最新 process 是 10/04 04:00 台北 success；latest 10/04 08:00 weekend 的 TPEx 重大訊息/月營收兩 source_runs 為 failed、0 raw/accepted。當前台北約 03:35，10/05 04:00 process 尚未開始。
- 已在 `schema.py` 加 additive `source_record_versions` 表與 migration v13；`registry.py` 在每個來源 run 內，比對 incoming normalized row 與目前同 `record_hash` 的列，只保存首次自然觀察/內容改變的完整 normalized snapshot、canonical version hash、微秒 captured time 及 Airflow lineage。首次觀察若 incoming 已不同於舊 current row，先記本次 ingestion 前讀到的舊列，再記來源回傳的新列；兩筆與 current upsert 同一 DB transaction。版本表不做大型歷史回填，避免在 schema bootstrap 同步複製目前約 66.2 MB payload；migration 不會假裝舊 `created_at` 是可證明的來源發布時間。
- 這只開始保存後續可重播版本，不可復原 deployment 前已被覆寫的 vintage；目前 live `market_signals.py` 仍讀 current `source_records`，還未切換為 as-of history query。沒有改任何方向票、分數、候選排序或通知閾值。待自然 10/05 process 驗 v13 catalog/table、來源 run 狀態、版本列與 current upsert 同步、有效日、接受/隔離/重複數和上市/上櫃覆蓋；未手動觸發 DAG、寫 DB、送 Discord、交易或跑測試。
- 靜態驗證：Airflow scheduler 對修改的 `schema.py`/`registry.py` AST parse 通過，`airflow dags list-import-errors --output json` 為 `[]`。這不代表新 migration 已在 DB 執行，也不代表自然來源收集已驗收。
- 03:42 台北再次唯讀查 Airflow metadata 與 crawler schema：process 最新仍是 `scheduled__2026-10-02T20:00:00+00:00`（10/04 04:00 台北）success；尚無 10/05 process run。DB `source_schema_migrations` 仍 9/9，`source_record_versions` 尚不存在；因此 v10–v13 都等待下一個自然 `ensure_source_extension_schema`，沒有用手動 schema bootstrap 製造驗收證據。


## 2026-10-05 04:00 台北：自然批次驗收與修訂版本開始留存

本節只記錄可排程批次來源、自然執行結果和自動訊號品質；沒有逐家公司搜尋券商報告或人工評分。

- Airflow 自然 run `source_extension_collect_process`：run id `scheduled__2026-10-03T20:00:00+00:00`，04:00:00 開始、04:02:19 結束，父 DAG success。來源登錄、排程契約及 runtime 預期皆為 23 keys，23/23 有最終成功列，缺 source keys 0、contract drift 0。TPEx 季營益與 TWSE BASI 資產負債來源各有一次初始失敗，既有 retry 後成功。只看父 DAG 綠燈仍不足；唯讀來源稽核狀態是 `all_expected_tasks_succeeded_data_review_required`。
- 普通股主檔分母：上市 1,054、上櫃 892。表內 matched 是目前普通股主檔代碼匹配數；未匹配代碼保持 unknown，不代表零量或沒有公司行動。raw 是來源列數，與匹配代碼數不是同一粒度。
- 自然 schema bootstrap 已將遷移套用至 13/13；`source_record_versions` 與 `market_signal_run_market_coverage` 均存在。該批新寫入 20,864 個版本觀測：`pre_ingest_current` 7,056 列、`source_feed` 13,808 列，captured_at 為台北 04:00:07–04:02:14，具 Airflow lineage。這只建立部署後的版本軌跡，無法還原更早被覆寫的歷史版本；訊號掃描器尚未切換至 as-of 版本讀取，故目前不能把它宣稱為完整 PIT 回測。
- 23 個最終成功來源的有效日、原始／接受／隔離／重複列及普通股主檔覆蓋如下：

| source_key | raw | accepted | quarantined | duplicate | effective date | listed / OTC matched; unmatched |
|---|---:|---:|---:|---:|---|---:|
| cbc_fx_daily | 3,660 | 3,660 | 0 | 0 | 2025-09-30–2026-09-30 | n/a |
| tpex_balance_sheet_bd | 7 | 7 | 0 | 0 | 2026-06-30 | 0 / 7; 0 |
| tpex_balance_sheet_ci | 884 | 884 | 0 | 0 | 2026-06-30 | 0 / 884; 0 |
| tpex_daytrade_eligibility_daily | 841 | 841 | 0 | 0 | 2026-10-05 | 0 / 725; 116 |
| tpex_dividends | 963 | 963 | 0 | 0 | 2026-01-29–2026-10-02 | 0 / 890; 2 |
| tpex_income_statement_bd | 7 | 7 | 0 | 0 | 2026-06-30 | 0 / 7; 0 |
| tpex_income_statement_ci | 884 | 884 | 0 | 0 | 2026-06-30 | 0 / 884; 0 |
| tpex_quarterly_profitability | 884 | 884 | 0 | 0 | 2026-06-30 | 0 / 884; 0 |
| twse_balance_sheet_basi | 10 | 10 | 0 | 0 | 2026-06-30 | 10 / 0; 0 |
| twse_balance_sheet_bd | 3 | 3 | 0 | 0 | 2026-06-30 | 3 / 0; 0 |
| twse_balance_sheet_ci | 1,049 | 1,049 | 0 | 0 | 2026-06-30 | 1,018 / 0; 31 |
| twse_balance_sheet_fh | 13 | 13 | 0 | 0 | 2026-06-30 | 13 / 0; 0 |
| twse_balance_sheet_ins | 5 | 5 | 0 | 0 | 2026-06-30 | 5 / 0; 0 |
| twse_balance_sheet_mim | 4 | 4 | 0 | 0 | 2026-06-30 | 4 / 0; 0 |
| twse_daytrade_eligibility_daily | 1,228 | 1,228 | 0 | 0 | 2026-10-05 | 972 / 0; 256 |
| twse_dividends | 1,228 | 1,228 | 0 | 0 | 2026-01-30–2026-09-21 | 1,053 / 0; 59 |
| twse_income_statement_basi | 10 | 10 | 0 | 0 | 2026-06-30 | 10 / 0; 0 |
| twse_income_statement_bd | 3 | 3 | 0 | 0 | 2026-06-30 | 3 / 0; 0 |
| twse_income_statement_ci | 1,049 | 1,049 | 0 | 0 | 2026-06-30 | 1,018 / 0; 31 |
| twse_income_statement_fh | 13 | 13 | 0 | 0 | 2026-06-30 | 13 / 0; 0 |
| twse_income_statement_ins | 5 | 5 | 0 | 0 | 2026-06-30 | 5 / 0; 0 |
| twse_income_statement_mim | 4 | 4 | 0 | 0 | 2026-06-30 | 4 / 0; 0 |
| twse_quarterly_profitability | 1,054 | 1,049 | 5 | 0 | 2026-06-30 | 1,018 / 0; 31 |

- 這次可直接用於自動訊號系統的新增品質證據，是上市／上櫃當沖資格 feed 都有 2026-10-05 有效日，且自然批次可取回；它們是可交易資格／篩選背景，不是方向預測。TWSE 256、TPEx 116 個接受代碼未匹配普通股主檔，先保留 unknown，不併入個股方向票。財報有效日仍為 2026-06-30；TWSE 財報有 5 列隔離、31 個接受代碼未配對。TPEx 股利來源最新事件日 2026-10-02；TWSE 股利來源最新日仍為 2026-09-21。
- 最新日線訊號仍是 10/02 `ready/partial`：universe 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、股利 1,053、候選 50、notified 0。最新盤中快照是 10/04 08:04 `ready/partial`，5 個事件觀察候選，行情／法人／營收／財務家族皆為 0；屬事件觀察，不是方向訊號。成熟 outcomes 仍只有 2026-09-24 H1：signal-close 81 rows（80 scored、50 hits、平均超額 +1.776pp），next-close 81（80 scored、37 hits、+0.987pp）；沒有成熟 H5/H10，不調權重或門檻。
- 最新自然新聞 run 仍是 10/04 23:30–23:40：12 feeds、1,298 URLs、1,297 有效時間列、1,270 accepted、1 fetch/parse reject、0 時間隔離、27 內容／重複拒收。iThome/TNL 的日期精度發稿時間不能作小時級 PIT；不回填舊掃描。
- 最新 after-close source run 仍是 10/02 20:00 failed；同日 daily child 通知也 failed（Discord 回 40005，摘要過大）。10/05 20:00 自然 after-close 尚未到期；程式分批修正須等該自然通知回執驗收。本輪只讀 Airflow／SQL 稽核自然結果，沒有手動觸發或重跑 DAG、寫業務表、實際發 Discord 或交易。



## 2026-10-05 04:18 台北：核心月營收讀取改為 as-of 版本

- 前一輪以 `updated_at > created_at` 標記的上市／上櫃月營收 current rows 比率皆為 100%；該時間戳是更新風險 proxy，不能單獨證明營收值曾改變。既有 `source_record_fields` 只保留最後值，而 `source_record_versions` 已由 10/05 自然 process 開始留存後續原始 payload 版本。版本 as-of 查詢可避免新捕捉的版本覆蓋較早 cutoff 可用內容，但不能回復版本留存前的歷史值。
- 已修改 `market_signals.py` 的盤中三月營收背景與日線營收家族讀取：依 scan start 的 UTC cutoff 從版本表挑選每個 business key 當時最後捕捉的版本；該 cutoff 尚無版本時，只回退到 `updated_at<=cutoff` 的 current row。營收金額、去年同月基準、累計年增與備註直接從該版本 payload 解析，避免與 current-only field table 混用。分數、方向票、門檻和通知內容均未改。
- 這是核心月營收資料的窄幅 PIT 改善，不等於全訊號 PIT 已完成：法人、股利、估值等讀取仍有 current-table 路徑，且版本表只有部署後的觀測，不能回補舊 vintage。待自然 intraday 月營收來源寫入版本後，再以後續自然掃描確認 coverage 和 context 的 as-of 行為，不手動補跑。
- 靜態驗證：scheduler 容器 AST parse 顯示 `AST_OK`，`airflow dags list-import-errors --output json` 為 `[]`。未新增或執行測試；未觸發 DAG、寫業務 DB、發 Discord 或交易。



## 2026-10-05 04:22 台北：季度財務高修訂家族切換至 as-of 版本

- 延續 03:35 的更新時間 proxy 盤點：上市／上櫃季營益 current rows 的 `updated_at > created_at` 比率皆為 100%，這本身不證明財務數值改變。10/05 04:00 自然版本表已記錄上市／上櫃營益率、損益表及資產負債表版本；例如 TWSE 季營益 1,049 個 accepted keys 加 5 個隔離 keys、TPEx 季營益 884 keys。此證明自然版本資料可供掃描器讀取，但不是舊年份的歷史回補。
- 已把日線掃描的季度營益率、季損益表及季資產負債表三條查詢，改成在掃描 cutoff 選每個 record_hash 當時最後捕捉的版本；尚無截止前版本時，僅允許使用 `updated_at<=cutoff` 的 current row。保留 `quality_status='ok'`、財報有效日、event_time 和原有指標運算規則，隔離列不進訊號。
- 掃描器現在對核心月營收和季度財務使用 as-of 版本讀取；法人、股利、估值、內部人、監管事件等其餘家族尚待個別遷移。現有舊 outcomes 因過往覆寫無法全面還原，不能把這次修正解讀為完整歷史 PIT 或預測力提升。
- 版本表唯讀盤點確認季度來源已有觀測、月營收版本尚未開始（等待今天自然 intraday 收集）。scheduler AST parse 為 `AST_OK`、DAG import errors 為 `[]`；未新增或執行測試、未觸發 DAG、未寫業務 DB、未送通知。待後續自然掃描驗 coverage 與版本 cutoff 行為。

## 2026-10-05 04:31 台北：修訂 proxy 語義與股利 PIT 讀取

- 修正文內對 `updated_at > created_at` 的描述：它只表示更新時間晚於建立時間，是需查原始版本的 proxy，不足以證明 payload、數值或訊號改變。10/05 04:00 首次保存的成對版本顯示，TWSE 股利 1,168 組、TWSE/TPEx 季獲利與財報主要批次組別的 payload 和 event_time 有差異，但已比較的 `metric_value` 未變。這支持保留 event-time/payload 的 PIT 版本，不支持宣稱財務數值普遍修訂或預測改善。
- `market_signals.py` 的日線近期股利公告查詢已切換至 as-of 版本：按掃描 cutoff 選每個業務鍵當時最後捕捉的版本；該 cutoff 前沒有版本時，僅回退至 `updated_at<=cutoff` 的 current row。仍保留官方事件時間、45 日視窗、`quality_status='ok'` 與每股現金股利背景語義；股利不作方向票或加分。
- 核心月營收、季度財務、董監持股與股利目前採用版本 as-of 讀取。其餘訊號家族仍有 current-table 路徑，版本留存只從自然部署後開始，舊 outcomes 不能全面還原。10/05 07:00 盤中自然收集/掃描尚未執行，故本次改動還沒有自然掃描行為證據；後續核有效日、行數、上市櫃覆蓋及 PIT cutoff。
- 本次只做靜態程式驗證；未手動觸發/重跑 DAG、寫業務資料庫、發 Discord、交易或跑測試。

## 2026-10-05 04:34 台北：擴展市場脈絡資料的 as-of 讀取

- `market_signals.py` 新增共用 `MARKET_CONTEXT_ASOF_CTE`，按掃描/結算截止時間選取每個 `record_hash` 當時最後捕捉的版本；只有 cutoff 前沒有任何版本且 current row 的 `updated_at` 也不晚於 cutoff 時，才回退 current row。
- 將已接入的公司行動預告、估值、指數行情、融資融券、注意/處置公告、可借券額度/借券活動、上市櫃漲跌家數、TPEx 當沖市場彙總、總報酬指數與 TAIFEX 期貨/選擇權市場部位查詢切換至此 as-of 快照。成熟 outcomes 的市場指數比較亦按結算時點讀取。原有效日視窗、事件時間、來源品質和市場/來源鍵過濾保留；未改多空分數、候選門檻或交易規則。法人買賣超目前仍讀 legacy `stock.三大法人買賣`，不在這次版本表改造範圍。
- Airflow worker 對版本表作唯讀事件型態盤點，目前共有六種已捕捉類型：`quarterly_balance_sheet` 3,950 列、`quarterly_income_statement` 3,950、`quarterly_profitability` 3,876、`fx_daily` 3,660、`dividend` 3,359、`daytrade_eligibility_daily` 2,069。上述合計 20,864 列；本次切換的其他市場脈絡類型目前尚無版本列，因此在各來源下一次自然刷新前仍由 cutoff-safe current-row fallback 供應，不能宣稱已完成新版本行為驗收或回溯 PIT。
- 10/05 04:34 尚未到 07:00 盤中收集/掃描或 20:00 after-close 自然 run。待自然更新後核各來源 effective date、接受/隔離/重複、上市櫃覆蓋、版本 capture 與掃描 cutoff；不得手動觸發 DAG。
- 靜態驗證：scheduler AST parse 為 `AST_OK`，Airflow DAG import errors 為 `[]`。未新增或執行測試、未寫業務 DB、未發 Discord、未交易；其他既有 dirty-worktree 變更保留。

## 2026-10-05 04:43 台北：核心行情與法人流增加入庫截止閘

- 唯讀查詢確認 legacy `stock.個股成交資訊` 與 `stock.三大法人買賣` 都有非空 `create_time`。最新有效日 2026-10-02：價格 1,938 列，入庫時間 16:57–17:51 台北；法人流 24,797 列，入庫時間約 16:27 台北。兩表所有當日列的 `create_time` 均非空。
- 舊 collector 對相同證券與日期採刪除既有列再插入；因此更正後舊值不保留，`create_time` 代表現存列的入庫時點而非完整歷史版本。日線候選的 45 日價格/法人視窗現在只讀 `create_time<=scan-start UTC cutoff` 的列；成熟 outcomes 的價格交易日與收盤標籤只讀 `create_time<=settlement UTC cutoff`。這可以排除 cutoff 後才補入的現存列，但若更正後列較晚入庫，舊值已消失，該鍵會暫時缺席而不會以新值回填。覆蓋不足應維持 partial/pending，不補零。
- 此修改不增加法人交易量以外的新方向訊號，也不改既有方向、分數、候選門檻、報酬定義或交易規則；核心 legacy 行情/法人來源仍沒有 append-only 修訂歷史。需等下一次自然 daily scan/outcome settlement 核對上市與上櫃覆蓋、有效日及待成熟樣本，不能只憑靜態驗證宣稱回測已改善。
- 最後靜態驗證：scheduler 對 `market_signals.py` AST parse 為 `AST_OK`，`airflow dags list-import-errors --output json` 為 `[]`。沒有執行測試、手動觸發 DAG、寫業務 DB、發 Discord 或交易。


## 2026-10-05 04:56 台北：重申只做可批次自動化訊號

- 使用者再次明確校正：交付是可排程、全市場、可程式化批次抓取的訊號；逐家公司上網找券商報告、人工評分及彙總不符合目標。券商資料只在有明確授權的批次/API/檔案契約、全市場覆蓋、發布/有效時間及可保存歷史版本時才列入候選，否則登記為來源缺口，不以人工調查替代。
- 10/05 04:00 台北自然 process 已有 23/23 runtime source keys 最終成功，schema v13 自然套用並開始寫版本列；各源 raw/accepted/quarantine/duplicate、effective date 與上市/上櫃匹配詳見上方 04:00 表。這表示來源批次完成，不代表所有訊號已有可回溯 PIT；每日掃描覆蓋表尚待新日線掃描自然寫入，成熟 outcomes 仍不足以調門檻。
- latest daily scan 仍為 10/02 `ready/partial`、50 候選；latest intraday 仍為 10/04 事件觀察卡，6/6 有平台 receipt、HTTP 200。`market_signal_outcomes` 仍只有 9/24 H1 成熟樣本，沒有 H5/H10；不據單日結果改方向、權重或門檻。
- after-close latest natural parent 仍為 10/02 20:00 failed，原因是舊 TPEx 個股日行情批次傳輸短讀。現行 adapter 已採固定 byte-range、驗證 Content-Range/ETag 或 Last-Modified、每段最多三次重試；官方端點的唯讀預檢已 HTTP 206 完整取回 72 段、4,666,641 bytes、11,928 raw rows，有效日 2026-10-02。預檢不是 `source_runs` 或自然 DAG 回執；10/05 20:00 自然 after-close 才能驗 catalog、有效日、accepted/quarantine/duplicate、上櫃主檔覆蓋及通知回執。
- 本次沒有逐檔瀏覽券商報告，也未新增程式、跑測試、手動觸發/重跑 DAG、寫業務 DB、送 Discord 或交易。下一個自然 checkpoint：05:30 新聞、07:00 盤中、09:00 TDCC、20:00 after-close。

## 2026-10-05 05:05 台北：修正新聞批次的 PIT 入庫時間戳

- 範圍維持可排程、全市場批次訊號；本次檢查新聞與候選自動關聯，不做逐家公司券商報告調查。最新自然 source process 仍是 10/05 04:00 台北 23/23 成功；after-close 最近仍為 10/02 failed，下一次自然 after-close 在今天 20:00。最新新聞批次仍為 10/04 23:30–23:40，今天 05:30 新聞排程尚未執行。
- 找到新聞 PIT 欄位風險：`NewsLogic` crawler 物件在模組載入時建立，`create_time` 因而可能早於文章實際寫入；`market_signals._attach_recent_news_context` 以 `news.create_time <= scan-start` 篩選，過早時間戳可能把掃描後才寫入的舊發稿文章納入背景。
- 在 `collect_news` 遇到空 URL 集合時，先清除 `news_diff`/`news_temp` staging，避免把前次殘留暫存當成本次資料。`delete_and_insert` 現於寫入 `news` 表前以台北當下時間覆蓋 `create_time`，記錄無文章內容的 ingestion receipt；新聞 DAG 明確啟用 XCom 回傳該回執，供後續自然批次稽核。
- 靜態驗證：兩個修改模組 AST parse 為 `AST_OK`；Airflow `list-import-errors` 為 `[]`；`git diff --check` 無空白錯誤。未跑測試、未手動觸發 DAG、未寫業務 DB、未送 Discord、未交易。等 05:30 自然新聞 run 驗 accepted/source counts、寫入列數、`create_time_tpe` XCom 回執，並確認後續訊號卡只納入 scan cutoff 前已寫入新聞。
- 目前 `news` 表仍以刪除再插入方式更新相同文章，沒有 append-only 新聞版本表；此修正避免使用過早入庫時間，不會復原已被覆蓋的舊文章版本。沒有新自然 outcome，維持訊號分數、門檻及方向規則。

## 2026-10-05 05:21 台北：註冊 TAIFEX 全量大額交易人批次背景 feed

- 依使用者再次校正，訊號工作只走可排程、批次抓取及可驗收的自動化路徑；不逐家公司線上搜尋或人工評分券商報告。新增候選來源必須先有明確批次端點/授權，再以有效日、列品質、自然 run 回執及下游 PIT gate 驗收。
- TAIFEX 官方 OpenAPI `OpenInterestOfLargeTradersFutures` 對應政府資料集 [11337](https://data.gov.tw/dataset/11337)。官方目錄列出日期、契約、到期月份、交易人類別、前五/前十大買方與賣方口數、全市場未沖銷口數；授權為 Government Open Data License v1、免費，但目錄更新頻率欄標「每1月」，與本次端點回傳的日資料不同，故有效日 freshness gate 優先於任務綠燈。
- 唯讀官方端點預檢 HTTP 200，1,386/1,386 列有可解析日期、契約/到期/類別代碼及五個整數指標候選，0 列隔離、0 重複自然鍵；唯一有效日為 2026-10-02，含 346 個不同契約代碼。這是 macro 期貨資料，上市/上櫃個股覆蓋為不適用，不可把它宣稱為個股全市場覆蓋或個股方向訊號。TAIFEX 官方頁 10/02「所有契約」資料可交叉核對：前五買/前十買 73,291/80,841 口、前五賣/前十賣 57,034/78,636 口、市場未平倉 118,531 口；買方和賣方是分別排名的集合，不相減成淨部位。
- 已在官方 allowlist/registry 加入 `taifex_futures_large_trader_positions_daily`，priority score 92（自動收集門檻 75），掛既有 after-close 自然批次；保留日期、契約、到期別、類別碼與各買賣排名數值，壞值進隔離。日線卡僅在來源有效日等於掃描日、cutoff 前可得且所有欄完整時顯示 TX 所有契約 (`SettlementMonth=999912`, 原始 `TypeOfTraders=0`) 市場背景；買/賣欄分開顯示，不加個股分數、不改候選門檻。目錄更新頻率與觀察到日資料不一致，若來源日期過舊就不沿用。
- 這是端點預檢而非自然 `source_runs` 回執。10/05 04:00 自然 process 已先完成 23/23 最終來源鍵；新來源尚未登錄/自然收集。after-close 最新仍為 10/02 20:00 failed，今天 10/05 20:00 才能驗 registry、原始/接受/隔離/重複、有效日與 context PIT；在取得自然回執前不可稱已上線成功。daily 最新仍為 10/02 `ready/partial`，intraday 最新 10/04，成熟 outcomes 仍只有 9/24 H1，無 H5/H10；不據單一日期結果調門檻。
- 靜態驗證：三個修改 Python 模組 AST parse `AST_OK`；Airflow `list-import-errors` 為 `[]`；`git diff --check` exit 0（僅既有 Windows 換行提示）。未手動觸發/重跑 DAG、未寫業務 DB、未發 Discord、未交易、未跑測試；保留其他既有 dirty-worktree 內容。

## 2026-10-05 05:38 台北：接入 TPEx 外資持股日批次背景來源

- 依使用者再次校正，本目標只做全市場可排程、可批次抓取與可自動驗收的訊號；逐家公司搜尋券商報告、人工打分或彙總不屬於工作流程。這次接入官方批次來源與 PIT 查詢，不做人工作業替代批次 feed。
- 新增 `tpex_foreign_investor_holdings_daily`：政府資料開放平台資料集 [20214](https://data.gov.tw/dataset/20214) 標示每日更新、Government Open Data License v1、免費，欄位含有效日、代碼、持股股數與比率；registry 排程為 `daily_after_close`，priority score 92.75，高於 75 門檻。實際使用資料集列出的 TPEx CSV 資源端點，沒有查詢未授權的個股網站路徑。
- 唯讀端點預檢 HTTP 200：893 raw/normalized，893 接受、0 隔離、0 重複自然鍵；唯一有效日 2026-10-02。對目前 common-equity 主檔配對為上市 0（本 feed 不涵蓋上市）、上櫃 892/892、未配對 1；該列保留為 unknown。所有 893 列的 event time 都為 NULL，因來源沒有發布時刻欄，不合成午夜時間。預檢不是 `source_runs` 自然回執，也沒有歷史舊快照可回填。
- 已更新 `adapters.py` 的官方 CSV allowlist、欄位契約與持股比例/股數一致性檢查；`registry.py` 註冊開放授權、市場範圍、欄位及有效日 gate；`market_signals.py` 使用 as-of 版本並只在來源有效日等於掃描日、capture cutoff 前可見、且代碼配對上櫃普通股時，附上持股比例背景。缺值/未配對不轉成零；不作淨買賣流、不加方向家族、不改候選門檻或分數。
- 最近自然 process 是 `scheduled__2026-10-03T20:00:00+00:00`（10/05 04:00 台北），Airflow success，source audit 為 `all_expected_tasks_succeeded_data_review_required`，23/23 預期/回報/runtime source keys；該次在新來源程式加入前執行，所以尚未登錄本來源。當沖資格兩市場有效日已到 10/05：TWSE 1,228 接受、256 未配對；TPEx 841 接受、116 未配對，未配對仍 unknown。下一次 10/05 20:00 after-close 自然 run 才能驗本來源 registry、列數、隔離/重複、有效日與 892/892 上櫃覆蓋。
- after-close 最新自然 Airflow run 仍為 10/02 20:00 台北 `failed`；舊 `source_runs` 缺 `dag_id`，稽核拒絕錯誤歸屬，狀態為 `unknown`，有 17 筆／15 個來源鍵無法歸到該 DAG。沒有手動補跑。日線最新 10/02 `ready/partial`、universe 1,946、價格 1,938、法人流 1,891、營收 1,945、財務 1,902、借券 1,795、董監 1,944、股利 1,053、候選 50；通知表沒有可驗證平台 receipt。盤中最新 10/04 08:04 `ready/partial`，5 個事件觀察候選、行情/法人/營收/財務家族均 0；6/6 平台通知 receipt 為 HTTP 200，這不是新日線行情快照。
- 最新成熟 `market_signal_outcomes` 仍只到 2026-09-24 H1：signal-close 81 列、80 scored、50 directional hits、平均超額 +1.776pp；next-close 81 列、80 scored、37 hits、+0.987pp。沒有成熟 H5/H10，不用單日結果調分數或門檻。
- 最新自然新聞 run `news_ch_collect_process` 於 10/05 05:30–05:38 台北成功，12 feeds、1,307 URLs/parsed、1,306 有效發稿時間、1,287 接受、0 fetch/parse reject、1 時間隔離、19 內容拒收、0 article-id 重複；9 feeds complete、3 partial，持久化回執為 1,287 列、`create_time_tpe=05:38:11`。iThome 60 列及 TNL 90 列雖全部接受，但只有日期精度；在 05:38 cutoff 中各有 2 列落在 cutoff 日期、無法判斷小時先後，分別另有 58/88 列依日期被視為較舊，不把它們當成小時級 PIT 證據。
- 驗證：三個修改模組 AST `AST_OK`，Airflow `list-import-errors` 為 `[]`；直接以新 normalizer 對官方最新 CSV 做唯讀剖析，上述列數與覆蓋成立。沒有跑測試、手動觸發/重跑 DAG、寫業務 DB、發 Discord 或交易；其他既有 dirty-worktree 變更保留。

## 2026-10-05 05:55 台北：上市外資持股批次 feed 的授權缺口

- 以官方目錄/API 搜尋上市市場的證券代碼級外資持股日資料。TWSE OpenAPI 目錄列出「集中市場外資及陸資投資類股持股比率表」與「持股前 20 名彙總表」，是產業彙總/Top 20，不足以替代全上市普通股個股快照；政府開放資料 11655 也是產業持股比率資料。現有 `source_extension` 沒有上市個股外資持股來源鍵。
- TWSE 官方「外資及陸資投資持股統計」頁提供日期/分類查詢及 CSV 下載，並稱自 2004-02-11 提供；但 TWSE 使用條款規定，未經 TWSE 同意不得以 script、爬蟲或其他自動化方式下載網站資料，僅已授權政府開放資料平台使用的資料除外。因此不把該公開查詢頁的下載路徑接入排程。
- 可核對的授權批次候選為 TWSE Data E-Shop「外資投資持股統計」：每日資料、資料起始日 2004-02-19，交易日 17:00 與 21:30 產製，欄位含代碼/名稱、發行股數、外資持有與可投資股數/比率、法令上限、前日異動原因及最近申報異動日；文字或 Excel，線上下載或 email 遞送。頁面列內部使用 NT$1,500/月、外部使用 NT$3,000/月。這是一個有價且有用途限制的候選，不等於目前已有 entitlement，也不自動代表允許排程留存/外發；未購買、未接入，待使用者確認已有授權或明確同意支出並核對交付及使用權後再做 adapter。
- 官方 TWSE 說明持股數亦會受借券、海外存託憑證、海外可轉債轉換、增減資/股東會、除權、ETF 申購贖回、投資人國籍變更等非集中市場交易因素影響。因此若取得批次 feed，須保留 17:00/21:30 快照時點、有效日、前日變動原因、申報日並作點時比較；持股變化不能直接命名成外資買賣超，應與三大法人淨買賣及公司行動分開顯示。
- 資料來源依據：[TWSE 官方查詢頁](https://wwwc.twse.com.tw/zh/trading/foreign/mi-qfiis.html)、[TWSE 使用條款](https://wwwc.twse.com.tw/zh/terms/use.html)、[TWSE E-Shop 商品與價格](https://eshop.twse.com.tw/zh/product/detail/fc2ca33908244644b066e0f12cb8efe5)、[TWSE OpenAPI](https://openapi.twse.com.tw/)、[政府開放資料 11655](https://data.gov.tw/dataset/11655)。

## 2026-10-13 20:45 台北：TAIFEX 大額交易人表徵切換的根因與修正

- 目標仍是排程批次訊號。最新唯讀快照為 [2026-10-13 20:45 source/run snapshot](stock_signal_inventory_snapshot_20261013_2045.json)：after-close 32/32 keys 回報、31 success/1 failed、Airflow failed，無 missing key 或 contract drift。TAIFEX 大額交易人兩次 HTTP 200 皆收滿 81,511 bytes，`application/octet-stream`，解碼在 JSON 第一個位元組失敗；失敗列的 0 raw/accepted/quarantine 是 fetch 在正規化前中止，不代表部位為零；不是空 body 或短讀，重試也未改變內容。
- 歷史 `source_runs` 有 10/05、10/06、10/12 成功、各 1,386/1,386 accepted；10/07–10/09 與 10/13 失敗，故非單次偶發網路錯誤。官方 OAS 明列該 endpoint 可回 JSON 與 CSV；此次 response 附件檔名為 `OpenInterestOfLargeTradersFutures.csv`，帶 UTF-8 BOM。舊 adapter 固定按 JSON 解碼是程式格式假設缺陷；歷史成功 body 未留 MIME，故無法從資料庫判定哪一天開始切換。
- 已更新 allowlisted adapter 依 MIME 解析兩種官方格式：JSON 限 `application/json` 且核對必要 keys；CSV 核欄位，對 `application/octet-stream` 再要求附件檔名完全相符；兩路都保留界線內的 HTTP/MIME/長度/收取 bytes 診斷，不保留 body。10/13 改後唯讀 worker 預檢 HTTP 200、1,386 rows 全部 quality=ok、資料有效日 10/12；這不是自然 run 或落庫證明。
- 定向離線驗證 `tests/test_source_extension.py` + `tests/test_source_http_quality.py`：16 passed；`py_compile` 成功；Airflow `list-import-errors` 為 `[]`。下一個 10/14 after-close 自然 run 才能驗新 adapter 的 accepted/quarantined/duplicate 與有效日 freshness。TAIFEX 數據是市場彙總背景，上市/上櫃個股覆蓋不適用，不投個股方向票；目前有效日落後 10/13 行情日一個交易日，仍受 freshness gate 限制。
- 同一 after-close 的 TWSE margin 為 1,040 raw、0 accepted、1,040 quarantined、無 effective date，繼續視為未知。10/13 daily scan 為 `ready/partial`、universe 1,947、price 1,918、flow 0、candidates 0；0 flow 是沒有合格方向法人流列，不是零淨流，0 候選也不代表沒有訊號。成熟 outcomes 只新增 10/12 signal-close H1 16 rows/5 hits、平均超額 +1.324pp，不因單一日期調分數或門檻。
- 本輪未手動觸發/重跑 DAG、未寫來源業務列、未手動發 Discord、未交易；下一步只等自然 after-close 驗收。
## 2026-10-14 20:35 台北：TAIFEX 雙格式已通過解析，欄位別名缺陷導致全批隔離

- 使用唯讀快照 `docs/stock_signal_inventory_snapshot_20261014_2035.json`；after-close `scheduled__2026-10-13T12:00:00+00:00` 自然 run 於 10/14 20:00 TPE 執行，32/32 keys 回報且 Airflow success。不要把此狀態等同當日完整。
- TAIFEX large-trader 自然 `source_runs` id 7905：HTTP 200、1,386 rows、accepted 0、quarantined/rejected 1,386、duplicate 0、effective date 10/13。持久化 payload 的日期、契約鍵和五個指標值完整；中文 CSV 欄名可成功正規化。對照 10/08 英文 JSON：1,386 accepted、10 欄/筆共 13,860 個 `source_record_fields`；10/13 因英文字段 validator 找不到 CSV 中文欄名而 1,386 列全隔離，欄位值表為 0。這是 deterministic 後處理契約缺陷，不是來源空資料、暫時傳輸或格式解碼失敗。
- 已在 `registry.py` 的官方來源設定登記各 canonical 英文字段對應的中文 CSV 別名，必填欄檢查與 source-field 儲存使用相同 allowlisted resolver；CSV header 還是嚴格核對官方完整欄位。以資料庫中實際隔離的 payload 唯讀回放，1,386/1,386 列新規則 quality=ok、必填缺欄 0，13,860/13,860 canonical source-field 值可映射；沒有寫回。新增測試涵蓋中文別名的 required-field 驗證與欄位取值。定向測試 17 passed；`py_compile` 成功；worker Airflow DAG import errors `[]`。這些是本地程式及已持久化 payload 的回放驗證，沒有證明自然批次恢復。
- 不手動觸發或重跑 DAG。下一個 after-close 自然批次要核 1,386 accepted、0 quarantine/duplicate、規範欄位持久化、有效日及 freshness。TAIFEX 仍僅屬期貨市場彙總背景，個股上市櫃覆蓋不適用、不加方向票；effective date 10/13 對 10/14 仍落後一個市場日。
- 快照中 10/14 日線 `ready/partial`、universe 1,947、price 1,917、flow 0、candidate 0；資料部分缺失時 flow=0/candidate=0 都不代表市場流量或訊號為零。10/14 outcomes 新增的單日 H1 尚不足以調門檻；來源成功數、候選卡、通知送達回執和人已讀分開。
