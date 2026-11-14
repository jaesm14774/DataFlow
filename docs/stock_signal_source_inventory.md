# 台股訊號自動收集盤點

## 2026-11-03 20:32 台北：TAIFEX 自然恢復與當日日線候選/通知查核

快照：[20:32 inventory](stock_signal_inventory_snapshot_20261103_2033.json)；完整判讀見[自然驗收報告](stock_signal_natural_validation_20261103.md)。Registry 67 keys。最新 daily / after-close / intraday / monthly audits 分別 23/23、32/32、10/10、2/2 source keys；各 Airflow roots success，runtime/registry contract drift 均 0。After-close raw/accepted/quarantine/duplicate 為 83,758/82,707/1,051/0。全部 32 source tasks 成功，但 audit 狀態仍 `data_review_required`，不是全來源完整。

| 當次來源 | raw / accepted / quarantine / duplicate | 日期 / 覆蓋 | 解讀 |
|---|---:|---|---|
| `taifex_index_options_positions_daily` | 6 / 6 / 0 / 0 | 11/02；市場彙總、不適用個股主檔 | 自然 try 1 HTTP 200、registry/runtime `taifex_csv` 相同、無 drift；6/6 PIT version、6/6 有 persisted source fields（8 canonical fields、48 field rows）。前次 CSV/JSON parser contract fix 已自然驗收；仍需 freshness gate，不投個股方向票。 |
| `taifex_index_futures_positions_daily` | 6 / 6 / 0 / 0 | 11/02；市場彙總 | 有效日較 11/03 行情日早一日，僅背景。 |
| `taifex_futures_large_trader_positions_daily` | 1,410 / 1,410 / 0 / 0 | 11/02；市場彙總 | 大額交易人解析成功，仍不得推成個股流向。 |
| `twse_equity_quotes_daily` | 1,057 / 1,057 / 0 / 0 | 11/03；1,057/1,058 listed master | 6 列收盤值無效，訊號有效價 1,051。 |
| `tpex_equity_quotes_daily` | 893 / 884 / 9 / 0 | 11/03；884/897 OTC master | 9 quarantine 分開；信號有效價 873，11 明確 no-trade、13 缺行情列。 |
| `tpex_institutional_flow_daily` | 917 / 917 / 0 / 0 | 11/03；795/897 OTC master，122 unmatched | 接受列不等於主檔配對；unmatched 不補零。 |
| `twse_margin_balance_daily` | 1,042 / 0 / 1,042 / 0 | 無 effective date | 保留全隔離；不是融資融券零活動。 |
| `tpex_daytrade_market_summary_daily`（OAS 11627） | 1 / 1 / 0 / 0 | 11/02；市場彙總 | 需 freshness gate，不是個股當沖成交；TWTB4U 資格名單與 TWTB5U 成交資料仍分開。 |

TWSE `trading_attention` 這批 0 raw/accepted，來源任務雖成功仍需按其契約判讀；不當成「無注意股」。兩市場 director-shareholding-shortfall feeds 最新 effective date 均為 2026-08-19；TWSE/TPEx ex-right preannouncement 分別有 46/49、55/63 列無法配普通股主檔，不能把公告列數當個股覆蓋率。

自然 daily signal as-of 11/03：universe 1,955（listed 1,058、OTC 897），partial；price/flow/revenue/financial/borrow/insider/dividend counts 為 1,924/1,841/1,948/1,912/1,796/1,943/1,952。Listed 有效價 1,051、flow 1,046（510 達方向條件）、上市分點 750 accepted panels、307 direction-usable、53 qualified；listed flow 的 natural PIT same-day 是 0，這些是 collection-time usable rows，不能冒充完整歷史 vintage。OTC 有效價 873（11 no-trade、13 缺列）、flow 795（211 qualified、122 unmatched）、78 有成交缺 flow；OTC 分點來源不可用且只略過該家族。

候選合併 gate 評估 1,941 檔、1,929 檔低於 3 個有方向家族、12 檔達條件；family triggers 為 institutions 721、listed broker 53、price 487，同一股可重複。方向分布偏空 11、偏多 1、分歧 0，不代表漲跌機率。摘要候選卡 3 張均有 HTTP 2xx receipt，摘要另列 9 檔未逐張顯示；5 ledger rows 全有 HTTP 2xx 與 receipt。這只確認 transport receipt，不表示人已讀。Intraday 166 event rows 中 154 routine/review 公告按 policy 排除；12 fingerprints 均為既有已送 receipt，0 新卡為 dedupe 結果。

新聞自然 run 15 feeds、1,120 URLs、1,115 accepted、5 fetch/parse rejects、0 invalid-time/duplicate rejects。時間統計是來源記錄時間：48h 內精確時間 930、日期精度且在 cutoff 日後 48、cutoff 日時刻未核 1、較舊 136、未來 0；10 recent、4 mixed、1 no accepted articles。CNA 98/100、鉅亨 145/148 partial，報導者本批無 URL。不得等同逐篇驗原頁時間。

成熟 outcomes：1w due 1,528，1,403 settled/125 pending/81 source-complete；1m due 131，6 settled/125 pending/81 source-complete；都 `outcome_data_incomplete`、無 confirmed delivery receipt rows、政策未自動調整。Monthly latest run 的 43,971 accepted rows PIT version 100%、1,103 duplicates；當次 run-attribution 無 unattributed legacy rows，但有效月份仍 2026-09，不能聲稱整段歷史 lineage 全已核。Airflow monthly `next_run_create_after` 為台北 11/24 20:00。

## 2026-11-02 20:40 台北：TAIFEX 選擇權契約錯配修正後唯讀盤點

快照：[20:40 inventory](stock_signal_inventory_snapshot_20261102_2040.json)；根因及針對性驗證見[自然驗證紀錄](stock_signal_natural_validation_20261102.md)。Registry 為 67 keys；daily／after-close／intraday／monthly 最新自然 audits 分別為 23/23、32/32、10/10、2/2 reported。Raw／accepted／quarantine／duplicate 為 8,201/8,201/0/0、83,477/82,415/1,062/0、1,990/1,990/0/0、45,074/43,971/0/1,103。After-close logical run `scheduled__2026-10-30T12:00:00+00:00` 實際在 11/02 20:00 台北啟動、root failed；32 keys 全有回報，TAIFEX 選擇權來源是唯一失敗，非 32/32 成功。Current-code snapshot 對該舊 registry row 顯示 adapter/config drift 1；自然 `ensure_source_extension_schema` bootstrap 待下次排程執行，本輪沒有手動改 DB。

| 自然來源 | raw / accepted / quarantine / duplicate | effective date / coverage | 解讀 |
|---|---:|---|---|
| `taifex_index_options_positions_daily` | 0 / 0 / 0 / 0（fetch 解析失敗） | unknown；市場彙總，上市/上櫃主檔分母不適用 | HTTP 200、octet-stream、JSON char-0。不能把 0 列解讀成無持倉。現行官方 endpoint 唯讀回應已確認為 16 欄 CSV；修正後 parser read-only probe 接受 6 列、effective 10/30，尚待自然持久化驗收。 |
| `taifex_index_futures_positions_daily` | 6 / 6 / 0 / 0 | 10/30；市場彙總 | 仍是背景，不是個股方向票。 |
| `taifex_futures_large_trader_positions_daily` | 1,386 / 1,386 / 0 / 0 | 10/30；市場彙總 | 成功不代表與 11/02 同日；期貨資料保持 freshness gate。 |
| `twse_equity_quotes_daily` | 1,057 / 1,057 / 0 / 0 | 11/02；1,057/1,058 listed common master（99.91%） | 行情日已和官方行情日期對齊。 |
| `tpex_equity_quotes_daily` | 893 / 874 / 19 / 0 | 11/02；874/897 OTC master（97.44%） | quarantine 分開保留，不補零。 |
| `tpex_institutional_flow_daily` | 884 / 884 / 0 / 0 | 11/02；763/897 OTC master（85.06%），121 unmatched | 不把 unmatched 當零；依 10/16 規則讀取既有 flow 並另列可投多空票，TPEx 分點仍是缺項。 |
| `tpex_daytrade_market_summary_daily`（政府資料集 11627） | 19 / 19 / 0 / 0 | 10/01–10/30；市場彙總、個股分母不適用 | 有 freshness gate；不是個股當沖成交或方向票。TWSE TWTB4U 仍是資格／限制名單，不能與 TWTB5U 個股成交量混為一談。 |
| `twse_margin_balance_daily` | 1,042 / 0 / 1,042 / 0 | 無 effective date；不能計覆蓋 | HTTP 成功不代表欄位可判讀；來源日期缺失的列隔離，不補本機日期。 |

本輪自然 daily signal as-of 11/02：universe 1,955、price 1,915、flow availability 1,808、candidate 0；1,931 檔進入 family gate 且全部低於三個方向家族。上市法人 available/usable 1,045/1,058、448 達多空條件；上櫃 763/897、187 達條件。上市法人 natural PIT same-day version 仍 0，保留歷史版本限制，但按 10/16 政策不因此略過當前可判讀資料。上市分點 745 same-day rows、737 accepted panel、242 direction-usable、29 達條件；TPEx 分點為 `source_unavailable_skipped`。這些是資料可判讀列與方向條件數，candidate 0 的直接原因仍是未達三家族，不是來源全缺或市場零訊號。Intraday 9 個已選事件列中 routine/review 公司公告與內部人事件已按策略排除或被 sent ledger 去重；3 個舊 fingerprints 均有 HTTP 2xx receipt，沒有新 candidate。新聞最新自然批次 15 feeds、1,115 URL、1,113 accepted、2 fetch/parse rejects、0 invalid-time quarantine；兩個 partial feeds 為中央社 99/100 accepted（1 parse/fetch reject，記錄時間近期）與鉅亨網 149/150（1 reject，135 記錄時間近期、14 較舊）；總 freshness 分類為 9 recent、4 mixed、1 無 accepted、1 stale。Observed time 為 18:41:32 台北、48 小時 cutoff 為 10/31 18:41:32；這是 feed 記錄時間，不替代原頁發稿時間或 revision vintage。

10/31 自然 daily run 的 8 個 `income_statement_*` EPS feeds 全部以 HTTP 200 success：3 feeds 有 84 accepted，5 feeds 為 0 raw；84 個 accepted rows 的 `basic_eps_ntd_per_share` 均有 numeric value，0 缺值。資料所屬期間為 2026-09-30；目前普通股主檔對碼為 listed 46/1,058、OTC 38/897，僅代表當下已收報表列，不是全市場財報覆蓋。無可比舊期時不算同比，負 EPS 僅保留反向脈絡、不另增方向家族；0-row feeds 不代表零 EPS 或沒有公司財報。最新 H1/H5/H10 的日期、來源及 basis 結果仍依 outcome groups 分開；1w 有 967 settled/125 pending，1m 有 6 settled/125 pending，review 均 `outcome_data_incomplete`，不作 policy change。

## 2026-10-30 20:30 台北：source_records 共用 pool 首輪自然批次

詳見[自然驗證紀錄](stock_signal_natural_validation_20261030.md)與[20:30 唯讀快照](stock_signal_inventory_snapshot_20261030_2030.json)。修正後 daily source process、after-close mapped source tasks、intraday source process 分別 23/23、32/32、10/10 成功，row counts 為 13,857/13,852/5/0、85,487/84,434/1,053/0、2,249/2,246/3/0（raw/accepted/quarantine/duplicate）；task instances 全在 2-slot `source_records_write`、每批 try 1，contract drift 0。After-close parent 尚在等待既有 broker collection 子工作；source map 成功不等於 parent 完成。三批寫入時間沒有彼此重疊，故只確認修正已載入並首批自然執行無 deadlock，不宣稱競爭壓力已驗證。

10/30 daily/intraday signal snapshot：daily 最新仍是 10/29、1,935 檔均低於三家族、0 candidates；10/30 intraday 30 fingerprints 全部已 sent 去重、0 新候選，25 有 HTTP 2xx+receipt、5 receipt 未確認。10/28 settled H1 是 subthreshold stock evidence；成熟 horizon review 仍 incomplete，不調門檻。

10/30 after-close 日期與覆蓋：TWSE quote 1,057 accepted，TPEx quote 882/893 accepted、11 quarantine；TPEx institutional flow 917 accepted、796 對碼、121 unmatched；TAIFEX large-trader 1,386 accepted、effective 10/29；TPEx OAS 18 rows、只市場彙總、最晚 10/29；TWSE margin 1,042 全 quarantine 且無 effective date。新聞自然 run 15 feeds／1,127 URLs／1,122 accepted／5 fetch-parse rejects／0 invalid-time quarantine。詳見逐來源 freshness、market coverage、PIT 與 pending child 狀態於驗證紀錄。

## 2026-10-29 20:35 台北：來源批次 deadlock 根因與併發修正

詳見[根因與自然驗證報告](stock_signal_natural_validation_20261029.md)與[20:35 快照](stock_signal_inventory_snapshot_20261029_2035.json)。Daily source process 23/23 keys 有回報但 root run failed；`twse_balance_sheet_ins` 在 `source_records` bulk upsert 兩次均出 MySQL 1213 deadlock、無 HTTP status、0 raw rows。10/25–27 daily 與 10/23、10/26 after-close logs 亦見同類首輪 deadlock。五個 collection DAG 已共用 2-slot `source_records_write` pool，啟動設定會建立／更新 pool，且 Scheduler 已核實載入；每 DAG `max_active_tasks` 亦為 2。保留既有 retry；同一 10/29 日線 cutoff 的來源與候選結果未改寫。BDD unittest、Compose config、DagBag import 與 compile 通過；等待下一次自然 run 驗證死鎖率與耗時。

最新 after-close `scheduled__2026-10-28T12:00:00+00:00` 32/32 keys，raw／accepted/quarantine/duplicate 為 85,440/84,385/1,055/0、contract drift 0。TAIFEX 大額交易人 1,386/1,386、期貨／選擇權法人 6/6、TPEx OAS 17/17 accepted，effective date 10/28；OAS 僅市場彙總。TWSE margin 1,042 全隔離且無有效日。

10/29 日線 candidate 0：1,935 檔全 `below_three_directional_families`；法人與上市分點有 576 與 64 個方向觸發，上櫃分點 unavailable。上市法人 natural PIT 0、上櫃 760；上市 quote 1,051/1,057 有效、上櫃 868/896 有效。缺行情列、invalid close、no-trade 分開保留。摘要通知有 2/2 HTTP 200 與 receipt、無個股卡片。詳見完整新聞 freshness、intraday 去重、outcome cohort 與上市櫃覆蓋細節於驗證報告。

## 2026-10-28 20:33 台北：TAIFEX／OAS 自然資料前進與 outcome cohort 標示

詳見[自然驗證報告](stock_signal_natural_validation_20261028.md)與[20:33 唯讀快照](stock_signal_inventory_snapshot_20261028_2033.json)。最新 daily／after-close／intraday／monthly source runs 各回報 23/23、32/32、10/10、2/2 keys，contract drift 0；raw／accepted／quarantine／duplicate 為 13,846/13,841/5/0、85,499/84,443/1,056/0、2,219/2,190/29/0、45,074/43,971/0/1,103。這些仍是 `data_review_required`，不能把 task success 當完整收集。

TAIFEX 大額交易人／期貨法人／選擇權法人與 TPEx OAS 各 1,386/1,386、6/6、6/6、16/16 accepted，0 quarantine／duplicate，最新 effective date 10/27、age 1。TAIFEX JSON 解碼錯誤未在本次自然 run 重現；OAS 無個股代碼，只是市場彙總。資料日與 10/28 掃描日不同，reader 不取用作同日值。

10/28 日線 run `bce74231fc671e931278443b56a9b0426627442668628933b2f6a6a00058c734`：上市／上櫃主檔 1,057／896；有效價 1,054／872；同日可判讀法人 1,048／782，上市自然 PIT 0、上櫃 785；達法人／上市分點方向條件分別 482／73。1,937 檔均低於三個方向家族，candidate 0。上櫃分點 unavailable；缺 quote rows 16、上櫃 no-trade 9、上市 invalid close 2 分開記錄。摘要 ledger 2 列均有 HTTP 200 與 receipt，無候選卡。

本次也修正唯讀 inventory 的 outcomes 分組：加入 `entity_type` 與 `evaluation_cohort`，避免將 `stock_evidence` 次門檻樣本誤讀成正式候選績效。10/27 上櫃 signal-close H1 分組維持 857 settled／59 directional hits／-2.596541 pp，現標為 `subthreshold_evidence`。這不表示候選規則或預測品質變化；單一 signal date 不調權重。

## 2026-10-27 20:58 台北：日頻 freshness audit 與自然掃描判讀

最新唯讀快照：[20:58 snapshot](stock_signal_inventory_snapshot_20261027_2057.json)。registry 67 keys。日頻／after-close／intraday／monthly source audit 分別為 23/23、32/32、10/10、2/2 keys，contract drift 均 0；raw/accepted/quarantine/duplicate 為 13,845/13,840/5/0、85,480/84,427/1,053/0、2,009/2,006/3/0、45,074/43,971/0/1,103。After-close logical run `scheduled__2026-10-26T12:00:00+00:00` 實於 10/27 20:00 台北啟動；TWSE 第二次分點收集 20:53:54 成功，wait sensor 20:55:26、signal trigger 20:55:28、驗證步驟 20:55:35 自然成功，parent 已 success。沒有手動干預。

### TAIFEX freshness 判讀假說與修正

TAIFEX 期貨法人、選擇權法人與大額交易人 effective date 均為 10/22，分別 6/6、6/6、1,386/1,386 accepted；TPEx OAS 11627 市場當沖摘要 15/15 accepted，日期範圍 10/01–10/22，最新日也是 10/22。TWSE/TPEx quotes 已有 10/27 rows。舊 audit 未替這四個 daily market-context sources 設有效日上限。20:48 暫定 3 日 budget 先把 age=5 全部標 stale。官方 TAIFEX 公告確認 10/23 期貨市場因薔琵颱風休市；TWSE 日曆列 10/26 補假。TPEx OAS 官方資料更新頻率為每日，其當沖統計說明 T、T+1、T+2 都可能更新、T+2 才是最終數值。這些只能說明日曆日差不等於交易日數，不能推定不同市場的休市日或確認 TPEx 某日未更新的原因；各來源仍依自身有效日與自然 run 核對。[TAIFEX 公告](https://www.taifex.com.tw/cht/11/announcement)、[TWSE 115 年市場開休市日期](https://accessibility.twse.com.tw/zh/trading/holiday.html)、[TPEx OAS 11627](https://data.gov.tw/dataset/11627)、[TPEx T+2 更新說明](https://www.tpex.org.tw/storage/zh-tw/web/stock/trading/intraday_stat/intraday_trading_statD.htm)。

因此把四個 daily market-context source 的 audit-only effective-date budget 設為 5 日曆日。固定同一 after-close source run 的唯讀比較：原 audit 無 age limit；暫定 3 日快照 age 5 標 stale；修正後快照 2056 的 age 5 正好在 5 日界線、不報 stale，四個 source 的 accepted/quarantine 列數、品質狀態、資料日期與信號讀取均不變。`market_signals.py` 對 TAIFEX 三個 context 與 TPEx OAS 都要求 effective date 等於 scan date 才引用數值；不同日仍顯示日期未對齊且不沿用。TPEx OAS 仍是上櫃市場彙總、無個股代碼與個股覆蓋。若後續自然發布後資料仍停在 10/22、age 超過 5，audit 才標 stale；不把 10/22 缺口稱為零值或 parser 失敗。

回歸驗收情境置於 `tests/test_source_audit_freshness.py`：6 日資料超過 5 日 budget 應標 stale、剛好在界線不標 stale、未來有效日另標 flag、未設定 age limit 時保持 unknown。先前紅燈因分類函式尚不存在；實作後 4 passed，`py_compile`、Airflow import-error `[]` 與 source-file whitespace check 通過。repo-wide `git diff --check` 另報使用者既有修改的 `dags/stock/temp/img_temp/twse_temp.jpg` 有尾端空白，本輪保留未動。Freshness age 是 collection-run date 與 effective date 的日曆差；不是交易日計數。

上市／上櫃普通股主檔分母 1,057／896。當日 TWSE quotes 1,056 accepted（1,056 對碼，對上市主檔 99.91%）；TPEx quotes 895 raw、884 accepted、11 quarantine（accepted 對碼 884/896，98.66%）。TPEx institutional flow 893 accepted、772 對碼、121 未匹配；未匹配不是零買賣超。TWSE margin 1,042 列全隔離且沒有 effective date，仍為未知。TDCC 週分布有效日 10/22，69,700 accepted、上市 1,055/1,057、上櫃 895/896 對碼、PIT 100%；當前來源品質旗標指出普通股覆蓋不完整，不能推導週級距變化。

### 10/27 自然日線判讀與零候選原因

After-close 自然 trigger 建立日線 run `d339acd4b0bf588c36b68351330b8b50d15535d06cbdb2701ca8ab26a4d6b614`，20:55:30 開始、as-of 10/27。source status 是 `partial` / `data_review_required`，但已實際評估 1,940 檔；master universe 為上市 1,057、上櫃 896，共 1,953。市場覆蓋：上市有效價 1,055、quote code rows 1,056、法人 flow 1,049；上櫃有效價 872、quote code rows 884（其中 no-trade 12）、法人 flow 771。另有 13 檔普通股沒有同日 quote code row（上市 1、上櫃 12），缺列不補零。

方向 family trigger counts：`institutions` 434（TWSE 326、TPEx 108）、`broker_branches` 25（僅上市 branch）。TWSE broker subset 517 same-day rows、513 accepted panels、194 direction-usable、25 qualified；上櫃 broker readiness 是 `source_unavailable_skipped`，不是零筆或中性。候選條件要求同一股票至少三個 directional families；1,940/1,940 evaluated stocks 均記為 `below_three_directional_families`，所以 candidate_count 0。這是當日家族觸發與第三家族未達／不可用的合併結果；沒有證據支持降低門檻。TWSE flow 的 1,049 same-day rows 以 collection time 儲存、natural PIT same-day count 0；TPEx flow 772 有 natural PIT same-day。兩市場規則仍標為 predictive performance unvalidated。

掃描 counters：universe 1,953、price 1,927、flow 1,820、revenue 1,948、financial 1,908、borrow 1,721、insider 1,943、dividend 1,949。來源狀態 partial 與各計數及候選過濾原因分開。0 candidate 日線仍送出一則 `新聞即時通` 摘要通知；ledger 2 rows，HTTP 200 與 receipt 各 2，屬摘要送達，沒有個股卡片。日線結果、來源部分狀態、摘要回執分開記錄。

10/27 intraday 實際選入 33 event rows；materiality routine 4／review 14，另排除 18 非 key events、7 舊財測版本與 2 重複財測語意。6 個候選 fingerprint 全已存在 sent ledger，只有 1 個有 HTTP 2xx/receipt，5 個 receipt 未確認；新增候選 0、未自動重送。News natural collection 15 feeds、1,118 URLs、1,112 accepted、6 fetch/parse rejects、0 invalid-time quarantine；freshness 按來源記錄精度分成 915 exact timestamp within 48h、35 date-only 僅證明晚於 cutoff 日期、162 較舊。中央社 1、聯合報 1、鉅亨網 4 fetch/parse rejects；報導者無 URLs。這不是逐篇原頁時間驗證。

成熟 H1 `signal_close` 已有 TWSE 5 個獨立 signal dates／56 rows、TPEx 4 dates／15 rows；`next_close` 為 TWSE 4 dates／46 rows、TPEx 4 dates／54 rows。H5/H10 各 basis 與市場只有 1–2 個日期，且 outcome 分組不等同已確認通知 receipt、未扣交易成本；不調權重或宣稱上漲機率。最新 monthly run `scheduled__2026-09-24T20:00:00+00:00` 實於 10/25 04:00 台北執行，2/2 source keys 成功，raw/accepted/quarantine/duplicate 為 45,074/43,971/0/1,103，兩市場 source rows 均有本次 DAG/run attribution、effective date 2026-09-01、PIT 100%。這確認最新 scheduled run 的 lineage，不外推到所有歷史月頻列。

## 2026-10-26 20:38 台北：自然批次恢復、market-closed scan 與來源日期

詳見[20:38 唯讀快照](stock_signal_inventory_snapshot_20261026_2038.json)與[自然驗證報告](stock_signal_natural_validation_20261026.md)。修正 `source_extension/__init__.py` 的字面 `\n` 後 DagBag import errors 清空，scheduler 自然執行 after-close 32/32 與 intraday 10/10 source rows；daily／after-close／intraday／monthly 的 contract drift 均 0。初次 MySQL deadlock 的 5 個 source map 經既有 retry 全部成功，after-close 最終 85,469 raw／84,411 accepted／1,058 quarantine，intraday 1,995／1,992／3，duplicate 0。

after-close quotes、TPEx flow、TAIFEX effective date 均為 10/22。TWSE quotes 1,053 accepted；TPEx quotes 876/892 accepted、16 quarantine、8 no-trade；TPEx flow 909 accepted、788 對碼、121 unmatched。TWSE margin 1,042 全隔離且無日期。月營收為 2026-09，TWSE 1,088 accepted／1,055 對碼／33 主檔外，TPEx 893 全接受。Monthly holdings 兩市場各自 effective 2026-09-01、有來源 run lineage 與 PIT 100%。

10/26 daily child 被 market-closed gate 跳過，沒有日線候選或通知。Intraday 有 11 source rows、7 個歷史 fingerprints，全已 sent 去重；2 個已確認 receipt、5 個 receipt unknown，無新 candidate。最新 outcome 還是單一 10/21 signal-close H1 日期，沒有新成熟 horizon review。News 1,120 accepted，4 fetch/parse rejects、1 invalid-time quarantine，date-only 留原精度。

## 2026-10-23 20:30 台北：自然批次、日期與通知/回測契約更新

詳見[自然驗證報告](stock_signal_natural_validation_20261023.md)與[20:30 唯讀快照](stock_signal_inventory_snapshot_20261023_2030.json)。最新 daily／after-close／intraday source rows 23／32／10，raw/accepted/quarantine/duplicate 為 13,835/13,830/5/0、85,266/84,208/1,058/0、2,177/2,174/3/0，contract drift 0。日線有效日期是 10/22；22 candidates 實際未完成 digest 通知，因單則摘要超出 byte budget。formatter 修正後同 cutoff 離線重播最大 request 9,244 bytes、8 張個股卡皆保留；不視為自然 delivery。

資料端：TAIFEX 大額交易人 1,386/1,386 accepted、effective 10/22；TPEx 法人流 909 accepted、788 對碼／121 unmatched；TWSE margin 1,042 全隔離，無 effective date；TWSE/TPEx 季獲利率都只有 2026Q2。盤中選入事件 2,293 列，27 個舊送出指紋中 22 有回執、5 無確認回執，新候選 0 是 dedupe/policy 的結果，未自動重送。follow-up 欄位契約修正與 static tests 已通過，仍待自然 run 寫入 horizon/follow-up rows。monthly latest lineage 仍 unknown，不按 run ID 推斷。

成熟 outcome：最新 10/21 signal-close H1 分組為 TPEx 5/5 hits／+1.4585pp、TWSE 10/6／+1.9097pp；單一獨立日期、未扣成本，不調門檻或方向權重。News 1,125 accepted 的原記錄精度分為 925 精確 timestamp 在 48h、62 date-only 晚於 cutoff 日、3 date-only 無時刻、135 較舊；date-only 不算精確 48h。

## 2026-10-22 20:33 台北：自然批次、分母與盤中 XCom 更新

詳見[自然驗證報告](stock_signal_natural_validation_20261022.md)與[20:33 唯讀快照](stock_signal_inventory_snapshot_20261022_2033.json)。registry 67 keys；daily、after-close、intraday 分別 23/23、32/32、10/10 回報，contract drift 0。raw/accepted/quarantine/duplicate 分別 13,834/13,829/5/0、85,543/84,485/1,058/0、2,107/2,102/5/0。Monthly audit 仍 unknown，來源列缺 dag_id，不得按 run ID 猜測。

日線主檔 1,950（上市 1,056、上櫃 894），有當日接受行情列 1,929、有效收盤 1,915；差額 14 為上市 6 個無效收盤和上櫃 8 個 no-trade。候選評估數等於當日代碼列數，故不是分母錯誤。另 21 檔未進接受代碼覆蓋；隔離與實際未收列仍須分辨。日線 22 候選、1,907 因不足三個方向家族未達條件，摘要 8 卡＋footer 14，10/10 通知 ledger rows 有 HTTP 2xx 和 receipt。

盤中自然 XCom 已寫出診斷：選入 2,255 rows；月營收 1,981 缺預期基準、241 非 key 公告、7 筆舊財測版本與 2 筆重複語意排除；餘下 24 fingerprints 全命中 sent ledger，19 有 receipt、5 筆財測差異傳輸未確認，沒有新待送候選。沒有自動重送。

台股季損益／EPS 八個批次來源本次合計 1,978 accepted，effective date 2026-06-30；最新存量 1,979 列 EPS 均非空，但多出的 TPEx CI 一筆沒有本次 source-run lineage。上市／上櫃 CI 對目前普通股主檔配對分別 1,020/1,056、885/894。唯一最新成熟 H1 為 10/21 signal-close、TWSE 10/TPEx 5 rows；只有一個獨立日期，不調參。

## 2026-10-21 20:50 台北：自然盤點更新

最新唯讀快照：[20:49 inventory](stock_signal_inventory_snapshot_20261021_2049.json)；完整數據和相同 cutoff 盤中重算見[自然驗證報告](stock_signal_natural_validation_20261021.md)。registry 67 keys。daily／after-close／intraday 預期來源數 23／32／10，全數回報、contract drift 0；raw／accepted／quarantine／duplicate 分別為 13,828／13,823／5／0、85,550／84,496／1,054／0、2,144／2,136／8／0。

日線 15 候選（10 偏多／4 偏空／1 分歧），1,917/1,932 因未達 3 個方向家族而排除；摘要 8 張卡＋footer 7 張與候選數相符，9/9 HTTP 2xx／receipt。盤中零新增不是沒有收集：同一 cutoff 選入 121 source rows，分類及去重後 14 個 fingerprints 都已在 `sent` ledger；9 有傳輸回執，5 個財測差異列的 receipt 未確認。已加計數診斷，不重送。

本次來源待確認集中在 TWSE margin 1,042 全隔離且來源日期缺失、TPEx 報價 12 隔離、TAIFEX 1,386 列有效日落後行情一個交易日、董監持股不足額來源逾期，以及 TWSE 注意資訊空回應。monthly 兩個來源仍無可歸屬 source_run；9 列舊 lineage 不按 run ID 猜測。新聞每源 rejects 與 timestamp precision 已納入快照；date-only 不折算精確 48h。

## 2026-10-20 20:45 台北：來源日、主檔覆蓋、新聞 freshness 與訊號計數

同時點證據：[唯讀快照](stock_signal_inventory_snapshot_20261020_2045.json)、[自然批次驗證](stock_signal_natural_validation_20261020.md)。registry 67 keys。最新 daily source run 23/23，13,825 raw／13,820 accepted／5 quarantine／0 duplicate；after-close 32/32，85,514／84,459／1,055／0；intraday 10/10，2,106／2,103／3／0；三者 contract drift 均 0。任務成功不等於當日來源完整。after-close 的 1,055 隔離由 TPEx 報價 13 列及 TWSE 融資融券 1,042 列組成；TWSE 融資資料沒有有效日期。intraday 3 列隔離為 TPEx 預定轉讓 2、TWSE 未轉讓公告 1。TWSE trading-attention 回 HTTP 200 但 0 rows，來源語意未確認，不當成無注意股。

上市／上櫃普通股主檔分母 1,056／893。10/20 同日行情：TWSE 1,053 accepted，1,050 有效收盤、3 無效收盤；TPEx 891 raw／878 accepted／13 隔離，878 個代碼有列、其中 9 已知零成交，869 個有效收盤。當日沒有行情列為上市 3／上櫃 15 檔；上櫃其餘 9 個 no-trade 與未配對／隔離列分開保留。法人：TWSE stored 1,045 可判讀、503 達方向條件、自然 PIT 0（有 collection time 1,045）；TPEx 882 accepted、761 matched、121 unmatched，760 可判讀、204 達條件、自然 PIT 761；另上市 5／上櫃 109 檔有成交但無同日法人列。上市券商分點 420 同日資料、411 accepted panels、139 可判讀、35 達條件、15 duplicate/invalid/mixed、4 unbalanced；TPEx 分點 source unavailable。未配對與缺列均未知，不補成零。

TPEx margin 801 accepted、effective 10/19；TAIFEX 大額交易人 source_run 10843 1,386 accepted、effective 10/19，與 10/20 股價晚一個交易日，僅市場背景。TAIFEX 成功回應 MIME 未存，不宣稱觀察到 octet-stream。TPEx OAS 日當沖市場彙總 12 rows、effective date 範圍 10/01–10/19，無個股代碼，只能作有 freshness gate 的市場脈絡。上市／上櫃當沖資格 feeds 分別 1,235／842 accepted、effective 10/19／10/20，現行主檔對碼 979／725；256／117 未匹配不當作不具資格。

八個季損益／EPS批次來源本次共 1,977 accepted，effective date 全為 2026-06-30；各 feed 依報表子集處理，不能相加當全市場分母。基本 EPS 持久化 1,978 rows 均非空，其中 TPEx CI 比本批 accepted 多 1 row，無 source-run row lineage；不把它歸給本次。來源只有 Q2，沒有可比期時不計同比。季獲利率 point-in-time set 也只見 Q2，故沒有同年 Q1 可推單季或去年 Q2 可同比。

新聞自然批次 `news_ch_collect_process / scheduled__2026-10-20T08:30:00+00:00` 於 18:30–18:42 台北執行，15 feeds、1,130 URLs、1,125 parsed、1,124 有效時間且 accepted、5 fetch/parse reject、1 invalid-time quarantine、0 content/duplicate reject；`delete_and_insert` 回報 1,124 rows。freshness cutoff 10/18 18:42:16：925 個有精確時間且在 48h、54 個 date-only 僅能證明晚於 cutoff 日期、2 個 cutoff 日期時刻未知、143 個按來源記錄精度較舊、future 0。reject：中央社 2、鉅亨網 3；自由時報 1 個 invalid timestamp。這是自然 feed metadata freshness，不代表逐篇核原始頁或獨立新聞佐證。日線 news context 為 matched，盤後 20:30 intraday context 為 no_candidates。

每日訊號 17 candidates（12 偏多、5 偏空）；1931 個有行情列的代碼進入評估，1,914 因少於 3 個方向家族未過條件，18 無行情列。通知摘要只展示 8 candidate cards，footer 明確列 9 個未逐張展示；共 9 ledger rows、9 HTTP 2xx、9 receipts、0 errors。詳見追蹤報告，無人工已讀證據。

monthly audit 仍 unknown：最近成功 run logical date 8/24，實際 9/25 04:00 台北完成；source rows 缺 DAG lineage 的 legacy 來源 9 個，不能按相似 logical ID 歸因。下一次 scheduler earliest creation 為 10/25 04:00 台北，不手動觸發。

## 2026-10-19 20:40 台北：TAIFEX post-fix 自然解析成功；成功 MIME 未留存

最新 [唯讀 inventory snapshot](stock_signal_inventory_snapshot_20261019_2043.json) 含 per-run candidate／notification delivery 對帳。10/19 daily sources 23/23（13,824／13,819／5／0）；after-close 32/32（85,516／84,463／1,053／0），contract drift 0。後者的 TAIFEX 1,386/1,386 接受且 13,860 欄位列已落庫，effective date 10/16，與兩市場 10/19 行情不同日；post-fix adapter 自然解析成功，但成功回應 MIME 未留存，不能證明 octet-stream 分支已自然觸發；freshness 仍不足。TWSE margin 1,042/1,042 隔離、無有效日，不合成日期。

10/16 日線自然讀取上市／上櫃既有法人資料：1,036／768 可判讀，551／237 達既有方向條件；flow_count 1,804。上市自然 PIT 為 0、上櫃 768；不把 stored capture-time 當 PIT。10/19 after-close parent 於 20:36:57 完成，daily 子流程 20:36:52–20:43:01 成功，as-of 10/19、candidate 7、flow 1,810。通知 ledger 7 rows sent／7 HTTP 2xx／7 receipt；回執不代表人已讀。詳見 [自然驗證與逐來源狀態](stock_signal_natural_validation_20261019.md)。

## 2026-10-16 07:31 台北：依使用者校正，既有法人與行情恢復實際訊號判讀

目前政策取代下方「legacy 法人不讀／方向須先等成效驗證」的舊限制。已修正資料被排除、方向固定關閉、觸發數被誤當覆蓋率，以及既有行情歷史未讀取四項根因。固定 10/15 20:59:28 cutoff 的唯讀比較：法人可判讀 0 → 1,820，834 檔達法人條件；接回行情後量能 77 檔，合併候選 0 → 20（5 多／13 空／2 分歧），門檻未放寬。上市／上櫃法人已收 1,044／777，可判讀 1,043／777，達條件 578／256。TPEx 券商分點未收集只影響該項；TWSE 分點來源存在，最新保存 10/14、796 檔，10/15 缺同日列要精確標日期，不能概稱未接通。

59 targeted tests passed；新回歸已實跑 red/green；AST 與 worker DagBag 通過，20 張卡與 8 批預覽均符合限制。新規則與舊自然結果分開，未改舊 run、未觸發 DAG、未發 Discord。既有平日 20:30 heartbeat 已原位更新，每次須檢驗判讀合理性並完成有證據的窄幅改善；無證據不硬調門檻。詳見 [本次修正與驗證](stock_signal_order_flow_repair_20261016.md) 及 [完整唯讀重算](order_flow_recompute_20261016_verified.json)。

## 2026-10-16 20:46 台北：after-close 仍等待上市分點收集；TAIFEX MIME 缺陷已修

最新自然 source snapshot：[20:33:37 唯讀快照](stock_signal_inventory_snapshot_20261016_2034.json)。Daily 23/23（13,817／13,812／5／0），intraday 10/10（2,116／2,110／6／0），monthly lineage unknown，9 筆 source_runs 無 dag_id；after-close `scheduled__2026-10-15T12:00:00+00:00` 在 20:33 仍 running/partial，32/32 回報、31 success／1 failed、83,840／82,787／1,053／0，無 registry/runtime contract drift。上述四數依序為 raw／accepted／quarantine／duplicate。

| Feed | 本次自然結果 | 可判讀限制 |
|---|---|---|
| TWSE／TPEx equity quotes | 1,051 accepted；TPEx 886 raw、874 accepted、12 quarantined；兩者 effective 10/16 | listed master 1,055、OTC 893；不把未配對或隔離列當零成交 |
| TPEx institutional flow | 890 accepted；768 matched、122 unmatched；effective 10/16 | unmatched 不是零買賣；TWSE legacy flow 由既有 collection 另行提供 |
| TAIFEX large trader | source_run 9361、HTTP 200、`application/octet-stream`、unknown content length、0 bytes read；0 raw/accepted/quarantine/duplicate、無 effective date | 舊程式需 exact filename 才放行 octet-stream；因自然失敗沒有 body，不能斷定該 body 是有效 CSV。adapter 已改成只對該 source 開放 MIME，再嚴格驗 JSON keys／CSV headers；7 tests、py_compile、worker DagBag 通過。下一自然批次前不宣稱恢復 |
| TWSE margin | 1,041 raw、0 accepted、1,041 quarantined、effective date unknown | success task 不能當可用資料；不補採集日 |

10/16 20:30 intraday event scan 為 universe 0／candidate 0；自然批次有 TWSE events 99 accepted（62 matched／2 unmatched）、TPEx events 31 accepted（28 matched）、月營收 1,079／893 accepted、財測差異 7／1 accepted；董監轉讓 5 raw 全隔離。這表示本次沒有持久化的合格 event candidate，不表示無事件或整體資料不足；目前沒有逐來源未入選 rule-rejection 計數，故保留為通知可解釋性缺口。14:15 earlier event scan 的 3 candidates 有 3 個 sent rows／HTTP 200／平台 receipt；不等於方向卡或人已讀。每日方向掃描尚未驗收新規則：舊 10/15 natural row `flow_count=0,candidate=0` 是修正前輸出；同 cutoff 重算 0→20 已在 [order-flow repair report](stock_signal_order_flow_repair_20261016.md)，不能冒充 10/16 natural scan。

After-close 在 20:44 因 `wait_existing_equity_order_flow_collection` 為 up_for_reschedule 而未觸發 daily child。法人收集於 16:26–16:27 TPE 結束；上市券商分點第二次收集自 19:21 還在跑。程式有 21:30 TPE 放行界線，現無證據支持改短等待；自然 run 目前仍 pending。latest settled outcome 是 10/12 H1（signal-close 16/5 hits/+1.324pp；next-close 16/6/+0.371pp），同一日期兩個 basis 不獨立，不調參。

新聞 `scheduled__2026-10-16T08:30:00+00:00` 於 18:30–18:40 自然完成，15 feeds、1,133 URLs、1,129 accepted、4 fetch/parse rejects、0 invalid-time/content/duplicate/future。Freshness cutoff 10/14 18:40:41：933 exact timestamps、40 date-only after cutoff date、23 cutoff-date time unknown、133 older by recorded precision。18:40:46 XCom receipt 1,129 rows，PK-desc 5,000-row window 逐來源 DB 核對 15/15 exact counts 和 distinct article_id。iThome 60 rows date-only；TheNewsLens 80/90、報導者 7/10、泛科學 8/10、環境資訊中心 38/48 依其記錄精度偏舊。完整原因與受限處見 [20:46 follow-up](stock_signal_order_flow_repair_20261016.md#2026-10-16-2046-台北追補taifex-媒體類型閘門與自然批次狀態)。

盤點時間：2026-10-16 06:53 Asia/Taipei。這份清單描述排程登記的來源與目前資料證據；「approved/enabled」只表示 registry 準備排程，不代表來源在最新自然批次完整、及時或可投方向票。

## 2026-10-16 06:53 台北：新聞自然恢復、月頻實際時程與唯讀快照改善

證據：[完整來源/coverage/PIT/outcomes/receipt 快照](stock_signal_inventory_snapshot_20261016_065331.json)、[自然新聞 XCom 與 Airflow 時程](stock_signal_natural_followup_20261016.json)、[新聞實際落庫及鎖狀態](stock_signal_news_storage_20261016.json)。既有 20:30 平日 heartbeat 保持 ACTIVE；本次是晨間追加查核，沒有新增或變更排程。

| 來源自然流程 | 實際執行時間（台北） | keys；raw / accepted / quarantine / duplicate | 說明 |
|---|---|---|---|
| Daily source process | 10/16 04:00–04:02 | 23/23；13,817 / 13,812 / 5 / 0 | logical ID 10/14 20:00 UTC；runtime contract 無 drift。8 個 EPS feeds 共 1,977 accepted，effective 2026Q2、版本 100%，未造同比。CBC 3,660 accepted 但日期仍止 9/30，無 signal reader，不能稱目前匯率。 |
| After-close | 10/15 20:00–20:59 | 32/32；85,240 / 84,191 / 1,049 / 0 | parent 與自然 daily child 均完成，已超過前次快照的等待 sensor 階段。TAIFEX 沿用已驗收 source_run 8638、有效日 10/14，沒有新的 run 可宣稱再恢復。TWSE margin 1,040 全隔離、日期 unknown。 |
| Intraday source process | 10/15 22:45–22:47 | 10/10；2,127 / 2,080 / 47 / 0 | logical ID 10/15 14:30 UTC；最新事件掃描 0 candidates/partial，不能代替全市場方向掃描。月營收最新可得期 2026/09；僅收集不代表 EPS 成長或新方向票。 |
| Monthly | 9/25 04:00–04:00:53 | 0/2 可歸因；unknown | logical ID 是 8/24 20:00 UTC。DAG active/unpaused；下一次最早可建立 run 為 10/25 04:00 TPE。仍有 9 筆 source_runs 缺 dag_id，不能以 Airflow 時程補造 source-row attribution。 |

來源 audit 以預期自然 run 為範圍；新增 `latest_source_attempts` 則保留各來源最後一筆嘗試（可能是 manual/其他 cadence），兩者不能混用。`airflow_schedule_state` 分開 logical date、實際開始/結束及 `next_run_create_after`；後者是 scheduler 最早可建時間，不是保證執行時間。此窄幅改善只修改 `docs/stock_signal_inventory_snapshot.py`，worker smoke run 成功，AST 檢查通過；沒有修改業務流程，因此未重跑 pipeline tests 或 DAG。所有無關既有變更保留。

日線普通股主檔上市 1,054、上櫃 894。上市 accepted 行情 1,050、其中 2 筆 close 無效，實用價格 1,048、4 檔缺 accepted 列；上櫃 887 raw/878 accepted/9 quarantine，6 檔 no-trade、實用價格 872、16 檔缺 accepted 列。缺列不補零。最新 daily `ready/partial`：1,948 universe、1,920 price、flow 0、candidate 0；TPEx flow 898 accepted、777/894 matched、121 off-master、版本 100%。保存的 cutoff 為 10/15 20:59:28 台北，同日自然 PIT 777、五日完整 685、三日完整 716、有成交卻缺同日 flow 95。上市 `source_not_authorized`；上櫃 `context_only_validation_pending`，多日期成效未驗證，均不開方向票。兩個 daily notification ledger rows 有 HTTP 200 及 receipt，payload 為「資料不足」和「法人與分點來源覆蓋」；0 方向候選卡，沒有已讀證據。

成熟 outcome groups 與前次快照一致：H1 signal dates 9/24、10/02、10/12；H5 僅 10/02 單一信號日期，且上市 signal-close 6 列與上櫃 next-close 43 列分屬不同價格來源/entry basis；H10 尚無 settled。同候選的 basis/horizon 不算獨立樣本；未證明扣成本、公司行動調整或歷史主檔倖存者偏差已解決，不調門檻/權重、不宣稱上漲機率。

### 新聞修正後自然驗收

兩次自然 run 都是 scheduled、external_trigger=false，三個 tasks 均一次成功：logical `scheduled__2026-10-15T10:30:00+00:00` 於 10/15 23:30–23:40 執行（1,139/1,140 accepted，鉅亨 1 reject）；logical `scheduled__2026-10-15T15:30:00+00:00` 於 10/16 05:30–05:37 執行（1,036/1,040 accepted）。最新 `create_time_tpe=05:37:45`；以主鍵倒序 3,000 筆、有較舊批次邊界的有界查詢，逐來源實際 rows 與 XCom accepted 全對平、source/article_id 無重複。第一次直接依 create_time 的查詢觸及 10 秒 statement 上限而自行終止，未放寬上限或留下長查詢；後續使用主鍵視窗成功。06:52 `article_url` 相關 active process/metadata lock 均無列，舊長讀者身份仍未確認。這是自然恢復驗收，未手動觸發/重跑、發通知或改業務資料。

最新觀察時間 10/16 05:37:41；cutoff 為 10/14 05:37:41 台北。15 feeds 為 13 complete/2 partial，拒收對平；0 invalid-time、0 content/duplicate rejects。下表只核自然 parser 保存的原發稿欄位與時間精度，本輪沒有逐篇重讀原頁，也不把收集時間當發稿時間。

| source | URLs / accepted / fetch-parse reject | 精確 48h | 僅日期晚於 cutoff 日 | cutoff 日時刻未知 | 依記錄精度較舊 |
|---|---:|---:|---:|---:|---:|
| ETtoday | 100 / 100 / 0 | 100 | 0 | 0 | 0 |
| TVBS | 16 / 16 / 0 | 16 | 0 | 0 | 0 |
| iThome | 60 / 60 / 0 | 0 | 21 | 33 | 6 |
| 新頭殼 | 23 / 23 / 0 | 23 | 0 | 0 | 0 |
| TheNewsLens | 90 / 90 / 0 | 8 | 0 | 0 | 82 |
| 三立 | 30 / 29 / 1 | 28 | 1 | 0 | 0 |
| 中央社 | 100 / 100 / 0 | 100 | 0 | 0 | 0 |
| 中時 | 200 / 200 / 0 | 200 | 0 | 0 | 0 |
| 公視 | 43 / 43 / 0 | 43 | 0 | 0 | 0 |
| 報導者 | 10 / 10 / 0 | 0 | 1 | 1 | 8 |
| 泛科學 | 10 / 10 / 0 | 1 | 0 | 0 | 9 |
| 環境資訊中心 | 48 / 48 / 0 | 12 | 0 | 0 | 36 |
| 聯合 | 60 / 60 / 0 | 59 | 1 | 0 | 0 |
| 自由 | 100 / 100 / 0 | 100 | 0 | 0 | 0 |
| 鉅亨 | 150 / 147 / 3 | 147 | 0 | 0 | 0 |
| 合計 | 1,040 / 1,036 / 4 | 837 | 24 | 34 | 141 |

future=0。date-only 不列入精確 48h；標題/正文公司 mention 不作事件或獲利驗證，轉載不計獨立佐證；重抓文章仍可覆寫原列，沒有完整不可變歷史 PIT。下一自然排程持續觀察拒收/freshness/落庫一致與鎖競爭，兩次成功只驗收本次修正。

### 上市法人可排程來源契約補充

官方 [TWSE 三大法人買賣超檔](https://eshop.twse.com.tw/zh/product/detail/c4c87ac184e44896a05fcab5a9d544ec) 說明 18:00 `TWT86UC` 不含鉅額、20:00 `TWTAIUC` 含鉅額；有 TEXT/Excel 與線上/電郵交付。未來需保存 product code、產製版本與實際接收時間，不能混合兩個 release 或事後當作早盤可得資料。官方商品區分內外部使用，[條款](https://eshop.twse.com.tw/zh/shopping/finishOrder?show=true&showTIP=) 另有使用限制；本專案 entitlement、機器交付、歷史修訂 vintage 及儲存/回測/外顯權利仍未核，不訂購、不聯絡供應商、不啟用方向來源。TPEx [11856](https://data.gov.tw/dataset/11856) 仍明列免費、每日、政府開放授權；這不能推出其歷史回補介面或早期 PIT 已完備。沒有新券商原文或方法缺口需追加樣本，本輪不擴大逐公司手工研究。

## 2026-10-15 20:46 台北：TAIFEX 欄位修正自然恢復；新聞網址批次遭 metadata lock 阻塞

- 唯讀快照：[2026-10-15 20:32 source/run snapshot](stock_signal_inventory_snapshot_20261015_2031.json)。After-close logical run `scheduled__2026-10-14T12:00:00+00:00` 於 10/15 20:00 台北開始；32/32 source keys 均回報 success，raw 85,240、accepted 84,191、quarantined/rejected 1,049、duplicate 0。Airflow parent 仍 running，`wait_existing_equity_order_flow_collection` 為 `up_for_reschedule`，等 `stock_collect_everyday_process / scheduled__2026-10-14T08:10:00+00:00`；after-close scan/通知 child 尚未觸發。來源任務成功不是整體資料完整證明。
- TAIFEX 大額交易人 source_run 8638：HTTP 200、1,386 raw/accepted、0 quarantined/rejected/duplicate、effective date 10/14；自然批次已在 source_records 保存 1,386 筆 quality=ok，source_record_fields 共有 13,860 個規範欄位值。正常 `ensure_source_extension_schema` 先同步 10 個中文 CSV header aliases，再由同一自然 run discover/ingest。這是 10/14 parser/schema 修正後第一個自然落庫驗收，確認欄位別名修正有效；先前 10/13 的 1,386 筆隔離歷史列保留，未回寫。TAIFEX 仍是期貨市場彙總背景，不代表上市櫃個股訊號。行情來源有效日 10/15：TWSE quotes 1,050/1,054 listed master、TPEx quotes 878/894 OTC master（另 9 quarantine）；TPEx institutional flow 898 accepted、777/894 OTC master matched、121 accepted codes off-master，方向家族仍關閉。TDCC weekly 69,496 accepted、listed 1,054/1,054 與 OTC 894/894 配對，但有效日僅 10/08。
- 同一 after-close 的 TWSE margin 1,040 raw、0 accepted、1,040 quarantined、無 effective date，仍是未知，不推成零融資融券。10/14 日線最新 `ready/partial`：universe 1,947、price 1,917、flow 0、candidates 0；flow 0 是沒有合格法人方向輸入，部分來源下 0 candidates 也不表示無訊號。最新成熟 outcomes 仍以 10/12 H1 為主（16 rows、5 hits、signal-close 平均超額 +1.324pp），不因單一日期調整門檻。
- 新聞自然收集 `news_ch_collect_process` 今日三個連續排程 `scheduled__2026-10-15T04:30/06:30/08:30:00+00:00` 均 failed；各在 `collect_article_url` 第 2 次嘗試到達 `TRUNCATE article_url` 後以 AirflowTaskTimeout 結束。日誌顯示來源列表抓取已進入後段，非單一 publisher parser 根因。20:35 台北唯讀 MySQL processlist 顯示另一筆執行數小時的 `article_url`/`news` SELECT 持有共享 metadata lock，6 個重試中的 TRUNCATE 等待 exclusive metadata lock；因此判定為 DDL metadata-lock convoy，非單次網路或新聞格式偶發。該 reader 的發起程式尚未能由 repository 對應。確認 Airflow 對應 runs 均 terminal failed 後，只取消這 6 個仍在等待、尚未執行的 TRUNCATE statements；之後 pending TRUNCATE 為 0，`article_url` 僅剩該 reader 的 1 個 granted shared-read lock，未改新聞業務列。
- 最近成功新聞收集為 `scheduled__2026-10-15T02:30:00+00:00`（實際 12:30–12:40 台北）：15 sources、1,130 URLs、1,126 parsed/valid-time/accepted、4 fetch/parse rejects、0 invalid-time/內容/重複拒收。觀察截止 12:40:51 台北；iThome 60 筆只有日期（43 筆在 cutoff 日期之後、17 筆 cutoff 日時刻未知），TheNewsLens 9/90 在 48h、81 筆較舊，報導者 0 筆有精確時間、1 筆 cutoff 日時刻未知且 9 筆較舊，環境資訊中心 8/48 在 48h、40 筆較舊。Feeds 的 complete/status 不等於每篇新聞新鮮或可作個股事件證據。
- 已做窄幅程式修正：網址 staging refresh 改用 `DELETE FROM article_url`，避免普通讀取者造成 TRUNCATE 的 exclusive metadata lock 排隊；同一 DAG 設 `max_active_runs=1`，避免共享 staging table 被並行自然 runs 覆寫。定向回歸 2 passed，`py_compile` 成功；尚待下一個自然 run 驗證，沒有手動觸發 DAG 或寫入新聞業務資料。

## 2026-10-14 20:35 台北：TAIFEX CSV 已解析但全部被隔離的第二層根因

最新唯讀快照：[20:35 source/run snapshot](stock_signal_inventory_snapshot_20261014_2035.json)，擷取於 20:35:16 Asia/Taipei。after-close logical run `scheduled__2026-10-13T12:00:00+00:00`（10/14 20:00 TPE）Airflow success、32/32 source keys 回報；此綠燈不代表資料完整。

- `taifex_futures_large_trader_positions_daily` 自然來源列 id 7905：HTTP 200、1,386 rows、0 accepted、1,386 rejected/quarantined、0 duplicate，effective date 2026-10-13。資料庫逐列都有日期、契約、entity key 與全部五個指標值；payload 中 1,386/1,386 都是官方中文 CSV 欄名。這證明 10/13 JSON/CSV 雙格式解析已通過，但 source 尚未恢復可用。
- 根因是 deterministic 的欄位契約不一致，不是瞬時網路/資料異常：adapter 可以依中文 CSV 欄名正規化；其後 `registry._required_fields_missing()` 與欄位值儲存只查 source registry 的英文欄名（`Date`、`Contract`、`Top5Buy` 等），所以把每一列都標成 quarantine，且 `source_record_fields` 完全沒有該日欄位列。作為對照，10/08 英文 JSON 成功批次為 1,386 accepted，並有 13,860 個規範欄位值（10 欄/列）。
- 已在 source config 為 10 個 canonical 欄位登記官方中文別名，並讓必填檢查及 `source_record_fields` 寫入共用 allowlisted alias resolver；原始 payload 保持來源欄名，CSV header 仍由 adapter 精確 allowlist 驗證。以資料庫中實際隔離的 10/13 payload 做唯讀回放，1,386/1,386 列重新正規化為 `quality=ok`、必填欄缺失 0，canonical 欄位可映射 13,860/13,860；沒有寫回業務表。定向測試 `tests/test_source_extension.py` + `tests/test_source_http_quality.py`：17 passed；`py_compile` 成功；worker `airflow dags list-import-errors --output json` 為 `[]`。
- 這是本地程式修正，尚未由自然批次驗證。不得手動觸發/重跑 DAG；下一個自然 after-close 應驗收 1,386 accepted、0 quarantined/duplicate，且 `source_record_fields` 有 canonical 欄位值，再核 effective date 和 freshness。10/14 snapshot 中 TAIFEX effective date 仍為 10/13；該來源是期貨市場彙總，不適用上市/上櫃普通股覆蓋、只作市場背景，不增加個股方向票。
- 本次也確認 10/14 日線掃描 `ready/partial`、universe 1,947、price 1,917、flow 0、candidate 0；來源/候選不完整時不能解讀為無交易訊號。新聞、其他 source、成熟 outcomes、通知 receipt 以同一快照為當期證據，不以此單一日期調門檻。

## 2026-10-13 20:45 台北：TAIFEX 大額交易人 JSON/CSV 表徵切換調查

最新唯讀快照：[20:45 source/run snapshot](stock_signal_inventory_snapshot_20261013_2045.json)，擷取於 20:45:41 Asia/Taipei。registry 67 keys。Daily source process `scheduled__2026-10-11T20:00:00+00:00` 為 23/23；after-close logical run `scheduled__2026-10-12T12:00:00+00:00` 實際於 10/13 20:00 台北執行，32/32 keys 有回報、31 success/1 failed，Airflow run failed，無 missing key 或 contract drift；intraday `scheduled__2026-10-13T12:30:00+00:00` 為 10/10，monthly 最新仍是 8/24、0/2 可歸屬，狀態 unknown。

### TAIFEX 失敗根因及修正

- 唯讀查 `source_runs`：同一大額交易人來源於 10/05、10/06、10/12 有 1,386 rows、1,386 accepted 的成功批次；10/07、10/08、10/09、10/13 失敗。10/07–10/09 多次重試均為 HTTP 200 + `JSONDecodeError`，當時未留 response MIME/長度；10/13 try 1、try 2（20:00:35、20:02:38 台北）都收到完整 81,511 bytes，`Content-Type: application/octet-stream`，仍由 JSON decoder 在 byte 0 失敗。失敗列顯示 row/accepted/quarantine 均為 0 是 fetch 在正規化前失敗，不代表部位資料為零。相同內容重試沒有幫助，故這不是單次傳輸短讀或空 body。
- 對官方端點的唯讀 GET 確認回應 `Content-Disposition: attachment; filename=OpenInterestOfLargeTradersFutures.csv`、UTF-8 BOM 與 CSV header，1,386 rows，唯一資料日 2026-10-12。TAIFEX OpenAPI 對同 endpoint 同時列出 `application/json` 與 `text/csv`；政府資料集 11337 列同一組欄位及開放授權。結論是來源表徵會變動而舊 adapter 假設永遠是 JSON，屬 parser/契約缺陷；歷史成功/失敗的交替不支持只靠網路 retry 解決。歷史成功回應 MIME 未保存，不能斷言每次切換的精確時點。
- `adapters.py` 現在同時容許官方 JSON 與 CSV：JSON 必須為 `application/json`、array-of-object 且有官方必要欄位；CSV 必須符合欄位集合，`application/octet-stream` 僅在附件檔名完全符合官方 CSV 檔名時接受。保留 UTF-8 BOM 解析及錯誤時 HTTP 狀態/MIME/長度/已收 bytes 診斷，不儲存 response body。request 同時宣告兩種 Accept。
- 當時的驗證結果：離線 adapter/HTTP 品質測試 16 passed、py_compile 成功、Airflow `list-import-errors` 為 `[]`；worker 唯讀預檢 HTTP 200、1,386/1,386 quality=ok、effective date 10/12。其後 10/14 自然 run 已證明格式解析可用，但因英文/中文 source-field 欄名不一致而 1,386 列全隔離；後續修正與待驗收條件見本檔最新 10/14 節。
- TAIFEX 是期貨市場彙總背景，不適用上市/上櫃普通股覆蓋、不增加個股方向票。當前官方回應有效日仍落後 10/13 收盤日一天，須維持 freshness gate。此次 after-close 另有 TWSE margin 1,040 raw、0 accepted、1,040 quarantined、無有效日；不代表融資融券為零，與 TAIFEX 解析問題分開。

### 當日掃描及自然收集狀態

- 10/13 daily market scan `ready/partial`：universe 1,947、price 1,918、flow 0、candidate 0。0 flow 是沒有合格方向法人流列，不是市場法人淨流為零；0 candidates 遇 partial sources 也不等於無訊號。10/13 intraday 是增量事件掃描，universe/candidates 皆 0，不能替代日線掃描。
- 新聞自然 run `news_ch_collect_process / scheduled__2026-10-13T08:30:00+00:00` 18:30–18:42:32 TPE：12 feeds、1,346 URLs、1,342 parsed/valid-time、1,321 accepted 並持久化；4 fetch/parse rejects、21 content rejects、0 invalid-time quarantine、重複 ID 0。3 feeds partial（ETtoday、自由時報、鉅亨）。cutoff 18:42:24.723：1,171 筆為精確時間且在 48h 窗內；69 筆只有日期精度且在 cutoff 日期之後，不視為精確 48h；81 筆依精度較舊，future 0。
- 10/13 結果日新增 10/12 signal-close H1 組 16 rows/5 directional hits、平均超額 +1.324pp；尚不足單一日期調門檻，H5/H10 仍待成熟。最新通知 rows 有 HTTP 200 與 platform receipt；receipt 不代表人已讀。

## 2026-10-12 20:38 台北：自然來源、新聞、方向投票政策與回執

完整唯讀快照：[20:38 source/run snapshot](stock_signal_inventory_snapshot_20261012_2038.json)，擷取於 20:38:03 Asia/Taipei。registry 67 keys；目前普通股主檔上市 1,054、上櫃 893。各 source audit 均須與有效日、raw/accepted/quarantined/duplicate、主檔對碼、PIT 和來源語義一起判讀。

| 自然流程 | 最新 run / 狀態 | 主要資料證據 | 判讀 |
|---|---|---|---|
| Daily source process | `scheduled__2026-10-10T20:00:00+00:00`；23/23 success；missing 0、contract drift 0 | 8 個季損益/EPS feeds 合計 1,976 raw/accepted、0 quarantine/duplicate、effective 2026-06-30、版本 lineage 100%；基本 EPS 缺值 0 | 只有 2026Q2，不能自造同比。TWSE/TPEx 財報來源的接受列及逐 key 對碼見 snapshot；上市櫃主檔外代碼不算普通股覆蓋。 |
| After-close source process | `scheduled__2026-10-09T12:00:00+00:00`；Airflow success；實際 10/12 20:00–20:32 TPE；32/32 回報、missing 0、contract drift 0 | TWSE/TPEx quotes effective 10/12：TWSE 1,052 accepted、1,052/1,054 配對；TPEx 887 raw、877 accepted、10 quarantined、877/893 配對，掃描識別 9 筆 no-trade、16 檔沒有 accepted 行情列。TPEx institutional flow 908/908 accepted，787/893 普通股配對、121 accepted codes 在主檔外，PIT versions 100% | run logical date 比實際執行早三天；訊號 child 按實際市場日判為 10/12 開市。TPEx flow 可用於來源/PIT/覆蓋診斷，不因 batch 成功就解鎖方向家族。 |
| TAIFEX / TWSE margin | 同一 after-close run | TAIFEX large-trader 1,386/1,386 accepted、HTTP 200、0 quarantine/duplicate、effective 10/08；期貨及選擇權 index positions 各 6 accepted、effective 10/08。TWSE margin 1,040 raw、0 accepted、1,040 quarantined、無 effective date | TAIFEX JSON 解碼在此自然 run 成功，但距 10/12 行情日仍落後；只能說排程時段非 JSON 是偶發可恢復的上游回應，不能宣稱當日資料已恢復。margin 缺來源日期後全數隔離，沒有再使用本機日期補標。 |
| Intraday source process | `scheduled__2026-10-12T12:15:00+00:00`；10/10 tasks success；data review required | 當前訊號 run `ready/partial`，universe 0、candidate 0 | 增量掃描沒有市場母體，0 候選不是「當日無訊號」。 |
| Monthly source process | Audit `unknown`；最近可讀 scheduled id 仍是 8/24 | 9 筆舊 `source_runs` rows 缺 `dag_id`；兩個月頻來源沒有可歸屬的最新自然 run | 不用重疊 run id 推測來源歸屬；當月 lineage 仍待修復。 |
| News collection | `news_ch_collect_process / scheduled__2026-10-12T08:30:00+00:00`；Airflow success；18:30–18:41 TPE | 12 feeds、1,289 URLs、1,285 parsed/valid-time、1,265 accepted 並持久化；4 fetch/parse rejects、0 invalid-time quarantine、20 blank-content rejects、duplicate 0；post-time reconciliation pass | 8 complete、4 partial（三立、中央社、自由、鉅亨）。觀察時間 18:41:05.756、48h cutoff 10/10 18:41:05.756：1,112 精確時間列在窗內；34 date-only 日期晚於 cutoff 日期、2 cutoff-date 時刻未知、117 依記錄精度較舊、future 0。date-only 不算精確 48h。 |

### 日線法人方向政策偏差與修正

- 10/12 日線掃描 run `d53770968158cfc40f32987d5cc66061c9c04a059bd793cc3e593d7676dbc5da` 在本次程式修正前完成，as-of 10/12、`ready/partial`、universe 1,947、price 1,917、flow 1,828、16 candidates。上市市場 coverage 表的 flow 1,041/1,054、8 檔有成交但缺 flow；上櫃 787/893、81 檔有成交但缺 flow。
- readiness JSON 顯示上市來源 `twse_t86_stored`、same-day 1,043、natural PIT 0，仍標 `direction_enabled` 並計 465 個合格投票；上櫃 TPEx flow same-day/natural PIT 787、五日 ready 688，也標 `direction_enabled` 並計 264 個合格投票。候選細節聚合確認 16/16 個候選都帶法人方向（11 正、5 負）。這違反「上市授權批次來源缺失時方向未啟用」及「TPEx feed 尚待多日期成熟成效驗證」規則；不是法人買賣為零。
- 已修改 `dags/source_extension/equity_order_flow.py` 與 `dags/source_extension/market_signals.py`：法人訊號不再查詢 `stock.三大法人買賣` 或以 `twse_t86_stored` 回退；上市來源顯示未授權、買賣超 unknown。TPEx 官方 batch 保留同日接受列、natural PIT、三/五日完整面板及影子條件計數，但 `vote_enabled=false`、`qualified_vote_count=0`，不進 candidates、score 或方向家族。修正後 `flow_count` 預期只計可投方向票列，下一次自然日線掃描應為 0；不回寫或重算 10/12 舊候選和通知。
- 加入 legacy exclusion、影子方向、官方 TPEx readiness 和上市 unknown 的回歸檢查。修正前測試有 5 個預期失敗；修正後法人流/PIT/政策 32 passed，`py_compile` 成功，Airflow DagBag import errors `[]`，本機與 worker 的 `equity_order_flow.py` SHA-256 相同。尚待下一次自然日線驗證 live candidate 不再含法人方向；沒有手動觸發 DAG。
- 同類路徑檢查：舊 `dags/stock/calculate_logic.py` 的主力推薦仍讀 legacy `三大法人買賣`，但所屬 `stock_calculate_everyday_process` 是 `schedule=None`，repo 內沒有排程 DAG trigger；週收集只觸發另一個 `stock_calculate_weekday_process`，其任務不含主力／法人推薦。因此它不是目前排程訊號管線的自動繞道；若有人手動執行舊 DAG，仍會產生 legacy recommendation，不能混入經 PIT/授權驗證的方向訊號。

### TAIFEX 解碼失敗判定（10/12 當時假說，10/13 已修正）

這段保留 10/12 時點的暫定判讀：當時 10/07–10/09 自然 after-close 兩次 JSON 解碼失敗，10/09 稍晚及 10/12 自然 run 可解析 1,386 列；因舊失敗 body 沒有 MIME/長度資訊，當時推測可能是非 JSON 回應。10/13 保留完整回應 metadata 後已證實當日是完整官方 CSV，並確認官方契約也支援 CSV；因此根因應改判為單一 JSON parser 與可變回應格式不相容，詳見本文件上方 10/13 調查。10/12 成功列 effective date 為 10/08，當時可解析不代表新鮮度達標。

### 10/12 候選、通知回執與成熟 outcomes

- 日線 child `market_signal__daily__source_extension_collect_after_close__scheduled__2026-10-09T12:00:00+00:00` 實際 20:32:15–20:37:46 TPE；開市判斷命中 0 列假日表資料，掃描耗時 5 分 25 秒，`ready/partial`。本次程式修正晚於此自然 run。
- 16 candidates 分開看：資料庫 `notified_count=1` 是通知完成旗標；ledger 有 9 個 daily batch rows，9/9 status `sent`、HTTP 200、均有 platform receipt id，payload 合計 10 embeds。這是自然排程的 delivery receipt；不代表 16 個候選全部各有卡片，也不代表人已讀。
- 成熟 outcomes 仍集中於 9/24、10/02 兩個 signal dates：10/02 TPEx H1 next-close 44 rows/12 hits/平均超額 -1.119pp；10/02 TWSE H5 signal-close 6 rows/2 hits/-1.562pp；9/24 legacy H1 81 rows，signal-close 50 hits/+1.776pp、next-close 37 hits/+0.987pp。10/02 TPEx H5 及多數其他價格來源仍 pending；無成熟 H10。來源、basis、日期與樣本覆蓋有限，不用來調權重或宣稱上漲機率。

## 2026-10-09 23:26 根因追查補記（唯讀驗證，非自然 run）

- **TAIFEX large trader：** 10/06 attempt 1 成功 1,386 列；10/07–10/09 每個自然 run 的兩次 Airflow attempt 都在 JSON 第 1 字元解碼失敗，HTTP 200。10/09 稍晚同一 worker 對官方端點的唯讀 adapter 驗證收到 1,386/1,386 accepted、effective date 10/08、0 quarantine。支持排程執行窗口收到空／非 JSON upstream body，而不是契約欄位或 TAIFEX parser 不相容；body 未留存，具體內容未知。現在 source failure 會附 content-type、宣告長度、實收 bytes 等安全診斷，不保存 response body。
- **TWSE margin：** 舊來源 `/v1/exchangeReport/MI_MARGN` 不帶日期欄，`date` query 對 10/08 與 10/09 回傳相同內容；舊 adapter 在缺日期時塞入本機 snapshot date，造成 10/09 假有效日。官方歷史報表核對 10/08 為 1,298 rows、10/09 無資料；TWSE 使用條款限制未經同意的自動腳本下載，因此未把網站報表頁接入排程。核准 OpenAPI 保留原 URL，移除無效 `date` query 及本機日期 fallback，缺日期列隔離。當前唯讀 OpenAPI adapter 驗證 1,040 listed-master rows、0 accepted／1,040 quarantined、無 effective date；10/09 舊保存列的非零餘額與官方 10/08 報表對齊，僅 OpenAPI 空值對 report 明示 0。官方頁說明次日可能調帳。signal reader 會要求 margin 日期出現在觀察到的官方行情 session，擋住歷史錯標 10/09 列；未回寫／刪除舊記錄。日期欄仍需由可排程且授權明確的 API 提供。
- 以上皆為唯讀端點/adapter 驗證，不代表收集寫入完成；未手動觸發 DAG、更新 source tables 或通知。離線 `py_compile` + 3 個針對性測試檔共 21 passed。

## 2026-10-09 20:33 自然來源、訊號、新聞及回執

完整唯讀資料快照見 [20:33 source/run snapshot](stock_signal_inventory_snapshot_20261009_2033.json)，captured_at 20:32:48 Asia/Taipei。registry 67 keys：daily 23、after-close 32、intraday 10、monthly 2；目前普通股分母上市 1,054、上櫃 893。Source audit 的 success 需與 effective date、accepted/quarantine、普通股對碼、PIT 與 source semantics 一起解讀。

| 自然流程 | 最新 run / 狀態 | 主要資料證據 | 判讀 |
|---|---|---|---|
| Daily source process | `scheduled__2026-10-07T20:00:00+00:00`；Airflow success、23/23；audit `all_expected_tasks_succeeded_data_review_required` | 8 個收入表/EPS feed 合計 1,976/1,976 accepted、0 quarantine/duplicate/EPS null，effective 2026-06-30，版本 lineage 1,976/1,976；`twse_quarterly_profitability` 1,054 raw/1,049 accepted/5 quarantine、31 accepted codes off-master；TWSE/TPEx day-trade eligibility 1,220/840 accepted、effective 10/08、L 964/O 723 matched、256/117 unmatched；`cbc_fx_daily` 3,660 accepted、0 quarantine/duplicate、latest effective date 2026-09-30 | 八個 EPS feed 僅有 2026Q2，無可比同期不計同比；去重對目前主檔上市 1,053/1,054、上櫃 892/893。資格名單不是成交量，unmatched 保留 unknown。CBC FX 雖標 daily 並自然成功，但其目前日期截至 9/30、且 `market_signals.py` 無 reader；先核新鮮度/point-in-time contract，再接市場脈絡。 |
| After-close source process | `scheduled__2026-10-08T12:00:00+00:00`；Airflow failed；32/32 reported、31 success/1 failed、missing 0、contract drift 0 | TWSE quotes 1,051 accepted、L 1,051/1,054；TPEx 886 raw/874 accepted/12 quarantine、O 874/893；TPEx institutional flow 892 accepted、O 770/893、122 unmatched、PIT 100%；TWSE margin 1,040 raw/992 accepted/48 quarantine，當時記為 10/09 | TAIFEX large-trader map 26 retry 2 `JSONDecodeError`、HTTP 200、0 rows、無 effective date；`verify_collection_and_triggers` failed。10/09 margin 日期是 adapter 用執行日補出的標籤，不是官方資料日；詳見 23:26 根因追查。`trigger_market_signal_notify` 成功並觸發 daily child，但 child 因休市而 skipped，不能驗收當日 flow readiness。 |
| Other after-close quality | 同一 run；來源 audit `partial` | TDCC 69,326 accepted、effective 10/02、PIT 100%、L 1,054/1,054、O 891/893；TPEx day-trade aggregate 6 accepted、10/01–10/08；TWSE attention HTTP 200、0 rows/無 effective date；TWSE major-shareholder 1,053 accepted rows 但 effective date/entity/PIT observation 不足 | TDCC 缺的 2 檔上櫃列保留 unknown。OAS 11627 只作市場彙總背景並須 freshness gate；空回應及沒有有效日都不等同零事件。 |
| Intraday source process | `scheduled__2026-10-09T12:15:00+00:00`；Airflow success；10/10、audit `data_review_required` | Company events 有效日 10/08：TWSE 116 accepted（84 個上市普通股代碼、4 unmatched）、TPEx 63 accepted（52 個上櫃代碼）；monthly revenue 仍為 8/01 | 事件列是稀疏公告，不是全市場覆蓋。TWSE planned transfer 6 raw/3 accepted/3 quarantined；TPEx 4 raw/0 accepted/4 quarantined；untransferred 空 sentinel 也隔離，不推為零交易。TWSE forecast variance 7 accepted；TPEx 僅 metadata row。 |
| Market-signal scans | After-close child `market_signal__daily__source_extension_collect_after_close__scheduled__2026-10-08T12:00:00+00:00` success，但 `skipped/market_closed`；最新 intraday 20:30 `ready/partial` | Daily scan無當日覆蓋；intraday universe 0、candidate 0、candidate batches 0、source warning unchanged、無新 receipt | 兩次 0 candidate 都不是完整市場「無訊號」結論；候選卡、來源提醒、ledger、HTTP receipt 與人已讀分開。 |
| News collection | `news_ch_collect_process / scheduled__2026-10-09T08:30:00+00:00`；Airflow success；實際 18:30–18:41 TPE | 12 feeds、1,208 URLs、1,204 parsed/valid-time、1,178 accepted/persisted、4 fetch/parse rejects、0 time quarantine、26 content rejects、0 duplicate；拒收 reconciliation pass | 9 complete、3 partial；以 18:41:00 TPE 為觀察時間、10/07 18:41:00 為 48h cutoff，1,028 精確時間在窗內；date-only：49 cutoff 後日期、23 cutoff 日時刻未知、78 較舊、future 0。詳見逐來源表。 |
| Monthly source process | Registry 2 keys；audit `unknown` | 最近 scheduled run logical 8/24；Airflow 另有 9/23 manual initial run；9 筆舊 source_runs rows 缺 dag_id | 不把 manual run 或不可歸屬 rows 當作最近自然 monthly lineage；尚無 10 月自然 run。 |

### 10/09 八個官方綜合損益/EPS feeds

這裡列出新增的八個 `income_statement_*` 來源，不把額外產業財報來源混入 EPS 分母。每筆本輪 natural source_run 均 success/HTTP 200；accepted metric rows 有相同數量 `basic_eps_ntd_per_share` 欄位且 0 缺值；每 key 的 accepted source-feed versions 數等於 accepted rows。effective date 全為 2026-06-30。

| source key | raw/accepted/quarantine/duplicate | listed / OTC 匹配 | EPS versions |
|---|---:|---:|---:|
| `tpex_income_statement_bd` | 7/7/0/0 | O 7 | 7/7 |
| `tpex_income_statement_ci` | 885/885/0/0 | O 885 | 885/885 |
| `twse_income_statement_basi` | 10/10/0/0 | L 10 | 10/10 |
| `twse_income_statement_bd` | 3/3/0/0 | L 3 | 3/3 |
| `twse_income_statement_ci` | 1,049/1,049/0/0 | L 1,018 | 1,049/1,049 |
| `twse_income_statement_fh` | 13/13/0/0 | L 13 | 13/13 |
| `twse_income_statement_ins` | 5/5/0/0 | L 5 | 5/5 |
| `twse_income_statement_mim` | 4/4/0/0 | L 4 | 4/4 |

八來源合計 1,976 accepted。上述小型產業來源的匹配數不能直接相加成市場覆蓋；去重後以 current common-equity master 匹配上市 1,053/1,054、上櫃 892/893。主檔為目前快照，不是 2026Q2 的 point-in-time universe；沒有同年同期資料則不計同比，負 EPS 只保留為獲利反向脈絡，不新增方向 family。

### 10/09 新聞自然批次逐來源拒收與發稿時間精度

XCom `collect_news` 的 source summary 與 `delete_and_insert` receipt 對齊：inserted_count 1,178、`create_time_tpe=2026-10-09 18:41:04`。有效時間數等於 parsed 數；內容/重複拒收 26，其中空白 title 1、空白 content 25、duplicate source article id 0。下表 freshness 按來源記錄的時間精度分類；date-only 不代表精確 48 小時。

| Feed | URLs/parsed/accepted | Fetch/parse reject | Content reject | Freshness by recorded precision |
|---|---:|---:|---:|---|
| ETtoday | 340/340/340 | 0 | 0 | 340 exact timestamps in 48h |
| TVBS | 16/16/16 | 0 | 0 | 16 exact timestamps in 48h |
| iThome | 60/60/60 | 0 | 0 | 42 date-only after cutoff date; 18 cutoff-date time unknown |
| newtalk | 25/25/25 | 0 | 0 | 25 exact timestamps in 48h |
| The News Lens | 90/90/90 | 0 | 0 | 7 date-only after cutoff date; 5 cutoff-date time unknown; 78 older by date precision |
| 三立新聞 | 30/28/28 | 2 | 0 | 28 exact timestamps in 48h |
| 中央社 | 100/100/100 | 0 | 0 | 100 exact timestamps in 48h |
| 中時日報 | 194/194/194 | 0 | 0 | 194 exact timestamps in 48h |
| 公視新聞 | 43/43/43 | 0 | 0 | 43 exact timestamps in 48h |
| 聯合報 | 60/60/60 | 0 | 0 | 60 exact timestamps in 48h |
| 自由時報 | 100/100/77 | 0 | 23 | 77 exact timestamps in 48h; 23 blank-content rejects |
| 鉅亨網 | 150/148/145 | 2 | 3 | 145 exact timestamps in 48h; 1 blank title and 2 blank content |

來源原頁 `created_at` 精度與新聞表 mutable-row 行為仍限制歷史 PIT：重抓可覆寫同一文章列和 `create_time`，沒有不可變正文/revision vintage。scan cutoff 可限制新入庫列，但不能還原過往正文版本；多家轉載不視為獨立佐證。

### Outcomes、通知與當沖限制

成熟 H1 仍只有 signal dates 2026-09-24、2026-10-02；10/02 TPEx next-close 44 rows/12 directional hits、平均 excess -1.119pp，其餘同日 basis/上市樣本 pending；無成熟 H5/H10。不同價格來源與 entry basis 不是獨立日期樣本，未扣成本/公司行動的單日結果不改權重或門檻。最近十筆通知記錄有 sent/HTTP 200 metadata、來自兩個較早 run keys；10/09 20:30 intraday run 沒有候選批次或新 source-warning receipt，均不等同 human-read。TWTB4U 資格、TWTB5U 成交契約與使用權，TPEx 個股 endpoint 空資料/未確認授權維持來源缺口；沒有接入未核准個股路徑。

## 2026-10-08 20:33 自然收集、訊號、結果與回執

完整逐來源明細（effective date、HTTP 狀態、raw/accepted/quarantine/duplicate、L/O 對碼、未配對與 PIT versions）保存在 [20:33 唯讀 snapshot](stock_signal_inventory_snapshot_20261008_2032.json)。此快照捕捉來源 audit；同時另查 Airflow task state、新聞 XCom/persist receipt 與通知 ledger。registry 67 keys：daily 23、after-close 32、intraday 10、monthly 2；普通股主檔分母為上市 1,054、上櫃 893。空回應、隔離、未配對和稀疏事件列都不當零值或全市場覆蓋。

| 自然流程 | 最新 run / 狀態 | 主要有效日／列數及對碼 | 判讀 |
|---|---|---|---|
| Daily source process | `scheduled__2026-10-06T20:00:00+00:00`；Airflow success；23/23 expected/reported/registered/runtime；audit `data_review_required` | 八個 EPS feeds：1,976 accepted、0 quarantine/duplicate/null basic EPS，effective 2026-06-30，PIT 1,976/1,976；去重對碼 L 1,053/1,054、O 892/893 | 目前僅 2026Q2，缺同年同期，不作同比。各 key 分列於 snapshot。 |
| After-close source process | `scheduled__2026-10-07T12:00:00+00:00`；20:33 snapshot 時 Airflow `running`，20:43 台北 task-state follow-up 仍 `running`；32/32 回報，31 success/1 failed；audit `partial`，無 missing key/contract drift | 新批次 quotes effective 10/08：TWSE 1,051/1,051 accepted、L 1,051/1,054；TPEx 886 raw／874 accepted／12 quarantine、O 874/893，其中 7 明確 no-trade、19 master 代碼沒有 accepted 列。TPEx flow 892 accepted、O 770/893、122 off-master、PIT 100%。 | TAIFEX large-trader map 26 第 2 次 `JSONDecodeError`，HTTP 200、0 rows、無有效日；未恢復。`wait_existing_equity_order_flow_collection` 是唯一 rescheduling task；trigger 與 verify 尚未執行，daily signal child 尚未觸發。 |
| Trading-status snapshots | 同一 after-close run；兩來源 success | TWSE 1,054/1,054 L，TPEx 893/893 O；effective 10/08、PIT 100% | 完整名單含明示名單缺席狀態，不能解讀為每檔都被限制。TWSE attention source 雖 HTTP 200，0 accepted／無有效日，仍 unknown。 |
| Intraday source process | `scheduled__2026-10-08T12:15:00+00:00`；Airflow success；10/10 tasks；audit `data_review_required` | 最新新聞事件/公告快照日期由逐 key snapshot 列示 | 最新 20:30 TPE signal run 0 candidates；這是增量掃描與部分來源狀態，不能推論沒有機會。當日較早 3 candidates 有 3 筆 ledger HTTP 200 receipts；不等於人已讀。 |
| News collection | `scheduled__2026-10-08T08:30:00+00:00`；Airflow success | 12 feeds、1,316 URLs、1,312 parsed、1,311 valid time、1,288 persisted；4 fetch/parse rejects、1 invalid-time、23 content rejects；8 complete／4 partial | Reject counts reconciled。48h cutoff 10/06 18:42:25 TPE；1,138 精確 timestamp 在窗內、67 日期精度列在 cutoff 後、8 cutoff-date 時刻未知、75 較舊、0 future。date-only 不當精確 48h。 |
| Market-signal daily / outcomes / notices | 最新 daily `as_of_date=2026-10-07`、`ready/partial`、0 candidates、flow 0、`flow_readiness_json=NULL`；通知最新歷史 3 rows 為 10/08 intraday run receipts | 新 flow source 尚未被 10/08 after-close 後的自然 daily scan 消費；outcomes 仍只有 9/24、10/02 兩個成熟 H1 dates；10/02 TPEx next-close 44 rows、12 hits、-1.119pp | H1 basis/source 不同；上市 10/02 與其他 basis pending，H5/H10 未成熟。source readiness、candidate cards、sent/HTTP receipt 和人已讀分開。 |
| Monthly source process | registry 2 keys；audit `unknown` | 最近可歸屬性不足；9 筆 legacy `source_runs` 無 `dag_id` | 不把重疊 run_id 的資料錯配到 monthly DAG；需自然 run lineage 改善。 |

TPEx OAS 11627／當沖日統計仍是市場彙總並需 freshness gate，不能代替個股當沖訊號。TWTB4U 資格與 TWTB5U 成交資料需分開；上櫃 individual endpoint 空資料／未確認授權仍不接入。未匹配或被隔離的證券列保持 unknown。最新資料的策略計票另見 [2026-10-08 策略稽核](stock_signal_strategy_audit_20261008.md)。

### 新聞自然批次逐來源品質與時間精度

最新 `collect_news` XCom 及 `delete_and_insert` receipt：觀察時間 10/08 18:42:25 台北、48h cutoff 10/06 18:42:25；persisted 1,288 rows，`create_time_tpe=18:42:29`。各來源 rejection 分解已 reconciliation（每來源 delta 0）；「精確」指原記錄含時分秒、落在 cutoff 內，不代表另行核對每篇原始頁。

| Feed | URLs/parsed/valid-time/accepted | Fetch/parse reject | Invalid-time | Content/other reject | Freshness by recorded precision |
|---|---:|---:|---:|---:|---|
| ETtoday | 450/450/450/450 | 0 | 0 | 0 | 450 exact timestamps in 48h |
| TVBS | 16/16/16/16 | 0 | 0 | 0 | 16 exact timestamps in 48h |
| iThome | 60/60/60/60 | 0 | 0 | 0 | 57 date-only after cutoff date; 3 cutoff-date times unknown |
| newtalk | 25/25/25/25 | 0 | 0 | 0 | 25 exact timestamps in 48h |
| TheNewsLens | 90/90/90/90 | 0 | 0 | 0 | 10 date-only after cutoff date; 5 cutoff-date times unknown; 75 date-only older |
| 三立新聞 | 30/28/28/28 | 2 | 0 | 0 | 28 exact timestamps in 48h |
| 中央社 | 100/98/98/98 | 2 | 0 | 0 | 98 exact timestamps in 48h |
| 中時日報 | 196/196/196/196 | 0 | 0 | 0 | 196 exact timestamps in 48h |
| 公視新聞 | 43/43/43/43 | 0 | 0 | 0 | 43 exact timestamps in 48h |
| 聯合報 | 60/60/60/60 | 0 | 0 | 0 | 60 exact timestamps in 48h |
| 自由時報 | 97/97/96/76 | 0 | 1 | 20 | 76 exact timestamps in 48h |
| 鉅亨網 | 149/149/149/146 | 0 | 0 | 3 | 146 exact timestamps in 48h |

彙總為 1,138 exact timestamps、67 date-only after cutoff、8 cutoff-date 時刻未知、75 date-only older，0 future; 8 feeds complete、4 partial。公司 mention 僅供情境，轉載不算獨立證據。

新聞的 point-in-time 證據只有 batch `create_time` receipt 和原頁 `created_at` 發稿欄位。signal reader 先以 `create_time <= scan cutoff` 限制併發新列，但 collector 重抓同一文章時會覆寫既有列並更新 `create_time`；目前沒有不可變文章版本，不能用現表重建早期可得的歷史正文／修訂 vintage。date-only 發稿仍不能當精確 48h。這是回測 PIT 的缺口，不把 ingestion cutoff 宣稱為完整來源版本 lineage。

## 2026-10-07 20:30 自然收集、日線/盤中快照、outcomes 與新聞回執

本節是 10/07 20:30 台北最新 checkpoint。只回看自然排程的全市場批次、Airflow task/audit、market-signal XCom/資料庫與新聞持久化回執，沒有逐家公司搜尋券商報告或手動評分。current common-equity master 分母為上市 1,054、上櫃 893；表中 L/O 是 accepted data 對碼 distinct code 數。稀疏公告/名單的匹配數不是全市場覆蓋。成功來源 accepted rows 的 source-feed PIT version coverage 均 100%；空回應或失敗的 PIT 為 N/A。

### After-close：30 個預期 source keys

自然 parent `source_extension_collect_after_close / scheduled__2026-10-06T12:00:00+00:00` 台北 20:00:00–20:04:45 執行，audit `partial`：30/30 expected/reported/registered/runtime keys、missing 0、contract drift 0。`taifex_futures_large_trader_positions_daily` map index 24 兩次嘗試都在 JSON 解析遇 `JSONDecodeError: Expecting value`；HTTP status 未記錄，raw/accepted/quarantine/duplicate 0/0/0/0，無 effective date。`verify_collection_and_triggers` 第二次嘗試因 map 24 failed 而失敗；下游 trigger 已成功，daily child 仍自然啟動。來源失敗不代表大額交易人部位為零。

| source key | state | effective date | raw/accepted/quarantine/duplicate | accepted L/O 對碼 | unmatched | PIT |
|---|---|---|---:|---|---:|---:|
| `taifex_futures_large_trader_positions_daily` | failed | 無 | 0/0/0/0 | — | — | N/A |
| `taifex_index_futures_positions_daily` | success | 10/06 | 6/6/0/0 | —（期貨彙總） | 0 | 100% |
| `taifex_index_options_positions_daily` | success | 10/06 | 6/6/0/0 | —（選擇權彙總） | 0 | 100% |
| `tdcc_shareholder_distribution_weekly` | success | 10/02 | 69,326/69,326/0/0 | L 1,054；O 891/893 | 0 | 100% |
| `tpex_daytrade_market_summary_daily` | success | 10/01–10/06 | 4/4/0/0 | —（市場彙總） | 0 | 100% |
| `tpex_director_shareholding_shortfall_daily` | success | 08/19 | 15/15/0/0 | O 15（稀疏名單） | 0 | 100% |
| `tpex_equity_quotes_daily` | success | 10/07 | 887/878/9/0 | O 878/893（98.32%） | 0 | 100% |
| `tpex_exright_preannouncement_daily` | success | 09/30–11/05 | 76/76/0/0 | O 14（事件列） | 62 | 100% |
| `tpex_foreign_investor_holdings_daily` | success | 10/07 | 888/888/0/0 | O 887/893（99.33%） | 1 | 100% |
| `tpex_institutional_flow_daily` | success | 10/07 | 901/901/0/0 | O 782/893（87.57%） | 119 | 100% |
| `tpex_major_shareholders_daily` | success | 10/07 | 909/909/0/0 | O 597（稀疏披露） | 0 | 100% |
| `tpex_margin_balance_daily` | success | 10/06 | 802/802/0/0 | O 802 | 0 | 100% |
| `tpex_market_index_daily` | success | 10/01–10/07 | 5/5/0/0 | —（市場指數） | 0 | 100% |
| `tpex_market_total_return_index_daily` | success | 10/01–10/07 | 5/5/0/0 | —（市場指數） | 0 | 100% |
| `tpex_securities_borrowing_daily` | success | 10/06 | 815/815/0/0 | O 815 | 0 | 100% |
| `tpex_trading_attention_daily` | success | 10/06–10/07 | 46/46/0/0 | O 34（事件列） | 0 | 100% |
| `tpex_trading_disposition_daily` | success | 09/30–10/07 | 15/15/0/0 | O 13（事件列） | 0 | 100% |
| `tpex_valuation_metrics_daily` | success | 10/07 | 886/886/0/0 | O 886 | 0 | 100% |
| `twse_director_shareholding_shortfall_daily` | success | 08/19 | 12/12/0/0 | L 12（稀疏名單） | 0 | 100% |
| `twse_equity_quotes_daily` | success | 10/07 | 1,051/1,051/0/0 | L 1,051/1,054（99.72%） | 0 | 100% |
| `twse_exright_preannouncement_daily` | success | 10/02–10/29 | 62/62/0/0 | L 14（事件列） | 48 | 100% |
| `twse_major_shareholders_daily` | success | 10/06 | 721/721/0/0 | L 484（稀疏披露） | 17 | 100% |
| `twse_margin_balance_daily` | success | 10/07 | 1,040/992/48/0 | L 992 | 0 | 100% |
| `twse_market_index_daily` | success | 10/01–10/07 | 5/5/0/0 | —（市場指數） | 0 | 100% |
| `twse_market_total_return_index_daily` | success | 10/01–10/07 | 5/5/0/0 | —（市場指數） | 0 | 100% |
| `twse_quarterly_forecast_attainment` | success | 06/30 | 8/8/0/0 | L 6（稀疏財測） | 0 | 100% |
| `twse_tpex_sbl_available_daily` | success | 10/07 | 1,715/1,715/0/0 | L 978；O 737 | 0 | 100% |
| `twse_trading_attention_daily` | success | 無接受列/無日期 | 0/0/0/0 | —（空回應語意未證實） | 0 | N/A |
| `twse_trading_disposition_daily` | success | 09/30–10/06 | 11/11/0/0 | L 11（事件列） | 0 | 100% |
| `twse_valuation_metrics_daily` | success | 10/06 | 1,083/1,083/0/0 | L 1,052/1,054（99.81%） | 31 | 100% |

TWSE daily quotes 對碼率 99.72%，但掃描可用收盤僅 1,047/1,054（99.34%），另 4 accepted rows 收盤無效。TPEx quotes HTTP 206 經完整 byte-range 驗證後解析，9 列 quarantine；878/893 普通股有 accepted row、873/893 有有效收盤（97.76%），其中 5 個 accepted rows 有明確 no-trade 標記，另 15 個普通股無 accepted quote row。缺列/隔離均保留 unknown，不補零量。兩市場指數和總報酬指數最新有效日都為 10/07，與 scan 對齊。TPEx OAS 11627 有 4 筆 10/01–10/06 市場彙總列，仍需 freshness gate，不能作個股訊號。

### Daily process：23 個預期 source keys

自然 run `source_extension_collect_process / scheduled__2026-10-05T20:00:00+00:00`（10/07 04:00 台北）23/23 source keys，無 missing/contract drift；audit `all_expected_tasks_succeeded_data_review_required`。主檔分母為上市 1,054、上櫃 893。

| source key | effective date | raw/accepted/quarantine/duplicate | accepted L/O 對碼 | unmatched | PIT |
|---|---|---:|---|---:|---:|
| `cbc_fx_daily` | 2025/09/30–2026/09/30 | 3,660/3,660/0/0 | —（匯率） | 0 | 100% |
| `tpex_balance_sheet_bd` | 06/30 | 7/7/0/0 | O 7 | 0 | 100% |
| `tpex_balance_sheet_ci` | 06/30 | 885/885/0/0 | O 885/893（99.10%） | 0 | 100% |
| `tpex_daytrade_eligibility_daily` | 10/07 | 841/841/0/0 | O 725/893（81.19%） | 116 | 100% |
| `tpex_dividends` | 01/29–10/02 | 964/964/0/0 | O 891/893（99.78%） | 2 | 100% |
| `tpex_income_statement_bd` | 06/30 | 7/7/0/0 | O 7 | 0 | 100% |
| `tpex_income_statement_ci` | 06/30 | 885/885/0/0 | O 885/893（99.10%） | 0 | 100% |
| `tpex_quarterly_profitability` | 06/30 | 885/885/0/0 | O 885/893（99.10%） | 0 | 100% |
| `twse_balance_sheet_basi` | 06/30 | 10/10/0/0 | L 10 | 0 | 100% |
| `twse_balance_sheet_bd` | 06/30 | 3/3/0/0 | L 3 | 0 | 100% |
| `twse_balance_sheet_ci` | 06/30 | 1,049/1,049/0/0 | L 1,018/1,054（96.58%） | 31 | 100% |
| `twse_balance_sheet_fh` | 06/30 | 13/13/0/0 | L 13 | 0 | 100% |
| `twse_balance_sheet_ins` | 06/30 | 5/5/0/0 | L 5 | 0 | 100% |
| `twse_balance_sheet_mim` | 06/30 | 4/4/0/0 | L 4 | 0 | 100% |
| `twse_daytrade_eligibility_daily` | 10/06 | 1,227/1,227/0/0 | L 971/1,054（92.13%） | 256 | 100% |
| `twse_dividends` | 01/30–10/05 | 1,229/1,229/0/0 | L 1,053/1,054（99.91%） | 59 | 100% |
| `twse_income_statement_basi` | 06/30 | 10/10/0/0 | L 10 | 0 | 100% |
| `twse_income_statement_bd` | 06/30 | 3/3/0/0 | L 3 | 0 | 100% |
| `twse_income_statement_ci` | 06/30 | 1,049/1,049/0/0 | L 1,018/1,054（96.58%） | 31 | 100% |
| `twse_income_statement_fh` | 06/30 | 13/13/0/0 | L 13 | 0 | 100% |
| `twse_income_statement_ins` | 06/30 | 5/5/0/0 | L 5 | 0 | 100% |
| `twse_income_statement_mim` | 06/30 | 4/4/0/0 | L 4 | 0 | 100% |
| `twse_quarterly_profitability` | 06/30 | 1,054/1,049/5/0 | L 1,018/1,054（96.58%） | 31 | 100% |

10/03 新增的 8 個季損益/EPS feed 已完成第四次自然 daily 收集，8 feed 合計 1,976 raw/accepted、0 quarantine、0 duplicate；basic EPS metric 缺值 0，source-feed version lineage 1,976/1,976（100%），effective date 全為 2026-06-30。八來源去重後匹配目前普通股主檔為上市 1,053/1,054、上櫃 892/893。每個 feed 僅有 2026Q2，不作同比；負 EPS 仍只作反向脈絡，不新增方向家族。當沖資格不是成交量，未匹配的 TWSE/TPEx 代碼各 256/116，保持 unknown。

### Intraday：10 個預期 source keys

最新自然 run `source_extension_collect_intraday / scheduled__2026-10-07T12:15:00+00:00`（20:30 台北邏輯時間）10/10 tasks success；audit `all_expected_tasks_succeeded_data_review_required`，無 missing/contract drift。

| source key | effective date | raw/accepted/quarantine/duplicate | accepted L/O 對碼 | unmatched | PIT |
|---|---|---:|---|---:|---:|
| `tpex_company_events` | 10/06 | 28/28/0/0 | O 23（公告事件） | 0 | 100% |
| `tpex_insider_transfer_planned` | 無接受列/無日期 | 1/0/1/0 | —（空白 sentinel） | 0 | N/A |
| `tpex_insider_transfer_untransferred` | 無接受列/無日期 | 1/0/1/0 | —（空白 sentinel） | 0 | N/A |
| `tpex_monthly_revenue` | 08/01 | 892/892/0/0 | O 891/893（99.78%） | 1 | 100% |
| `tpex_quarterly_forecast_variance_events` | 10/07 | 1/1/0/0 | 無 company event rows；snapshot metadata | 0 | 100% |
| `twse_company_events` | 10/06 | 54/54/0/0 | L 44（公告事件） | 2 | 100% |
| `twse_insider_transfer_planned` | 10/06 | 4/1/3/0 | L 1 | 0 | 100% |
| `twse_insider_transfer_untransferred` | 無接受列/無日期 | 1/0/1/0 | —（空白 sentinel） | 0 | N/A |
| `twse_monthly_revenue` | 08/01 | 1,086/1,086/0/0 | L 1,054/1,054（100%） | 32 | 100% |
| `twse_quarterly_forecast_variance` | 06/30 | 7/7/0/0 | L 5（事件列） | 0 | 100% |

Intraday child 是公告增量掃描，不是全市場日線：0 candidates、0 candidate batches、source warning `unchanged` 且無新 receipt 確認。空白 sentinel、隔離列及只有 snapshot metadata 的 endpoint 不代表零事件或零變動。

### News：12 個自然 feeds

最新自然 `news_ch_collect_process / scheduled__2026-10-07T08:30:00+00:00` 實際約 18:30–18:40 台北；collect 與 `delete_and_insert` 成功，1,369 rows 已持久化。共 1,385 URLs、1,383 parsed/valid-time、2 fetch/parse reject、0 invalid-time quarantine、14 content/duplicate rejects；post-time filter reconciliation 通過。9 feeds complete、3 partial。48h observed/cutoff 為 10/07 18:40:44 / 10/05 18:40:44：1,219 有精確發布時間且落窗，56 篇 date-only 記錄在 cutoff 日期後、18 篇 cutoff 日時刻不明、76 篇依記錄精度較舊、future 0。date-only 不是精確 48h freshness。

| feed | state | URLs/accepted | fetch/parse reject | content/duplicate reject | precise 48h | date-only after cutoff | cutoff-day time unknown | older |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| ETtoday | complete | 520/520 | 0 | 0 | 520 | 0 | 0 | 0 |
| TVBS | complete | 16/16 | 0 | 0 | 16 | 0 | 0 | 0 |
| iThome | complete | 60/60 | 0 | 0 | 0 | 47 | 13 | 0 |
| newtalk | complete | 25/25 | 0 | 0 | 25 | 0 | 0 | 0 |
| The News Lens | complete | 90/90 | 0 | 0 | 0 | 9 | 5 | 76 |
| 三立新聞 | complete | 30/30 | 0 | 0 | 30 | 0 | 0 | 0 |
| 中央社 | partial | 100/98 | 2 | 0 | 98 | 0 | 0 | 0 |
| 中時日報 | complete | 192/192 | 0 | 0 | 192 | 0 | 0 | 0 |
| 公視新聞 | complete | 43/43 | 0 | 0 | 43 | 0 | 0 | 0 |
| 聯合報 | complete | 60/60 | 0 | 0 | 60 | 0 | 0 | 0 |
| 自由時報 | partial | 100/87 | 0 | 13 | 87 | 0 | 0 | 0 |
| 鉅亨網 | partial | 149/148 | 0 | 1 | 148 | 0 | 0 | 0 |

### Daily scan、通知、outcomes 與自動化來源缺口

Daily child `market_signal__daily__source_extension_collect_after_close__scheduled__2026-10-06T12:00:00+00:00` as-of 10/07，`ready/partial`：universe 1,947；price 1,920；flow 0；revenue 1,945；financial 1,903；borrow 1,795；insider 1,939；dividend 1,944；candidate 0。上市可用價格 1,047/1,054（99.34%），上櫃 873/893（97.76%）；兩市場基準 fresh 且 aligned 10/07。DB run `notified_count=1`，ledger 是 1 筆 `新聞即時通` HTTP 200 自動 receipt；candidate 0，所以不是方向卡。

Mature outcomes 仍只有 H1 settled groups：9/24 legacy `signal_close` 81 列/50 directional hits、平均超額 +1.776pp；legacy `next_close` 81/37、+0.987pp。10/02 TPEx-only `next_close` 44/12、-1.119pp，單一日期且未扣交易成本；同日 `signal_close` 44 pending-data。10/02 TWSE 的 signal-close 與 next-close 各有 6 個 pending-data outcomes；H5 rows 仍 pending-data，沒有 mature H5/H10。XCom outcome ledger 本輪 reviewed 131、settled 44、pending-data 380。單日及不同價格來源不作門檻調整依據。

當沖限制沿用：TWTB4U 是資格/限制欄位，不是當沖成交量；TWTB5U entitlement、機器交付、歷史 T/T+1/T+2 vintages、schema、上市櫃完整性與儲存/回測使用權仍待確認。TPEx 個股 individual endpoint 仍空資料且授權未確認，不接入；OAS 11627 只有市場彙總且需 freshness gate。沒有 entitlement 明確、全市場可批次抓取的券商研究 feed/index/API，因此沒有逐份報告比較或把網頁查詢路徑加入訊號。

本次沒有手動觸發/重跑 DAG、手動發 Discord、交易、跑測試或修改程式/分數/門檻/方向家族。下一個自然 run 再判讀 TAIFEX large-trader 回應及各來源 freshness，不回補或重跑這次失敗批次。

## 2026-10-06 20:30 自然來源、日線/盤中快照、outcomes 與新聞回執

### After-close：30 個預期 source keys

自然 parent `source_extension_collect_after_close / scheduled__2026-10-05T12:00:00+00:00` 台北 20:00:00–20:02:05 執行；30/30 預期、registry、runtime、reported keys 一致，無 missing keys、無 contract drift，audit `all_expected_tasks_succeeded_data_review_required`。以下 `L/O` 是 accepted records 對目前普通股主檔的上市/上櫃代碼配對數（本次分母 1,054/893）；稀疏公告、股東清單及市場彙總的配對數不是全市場覆蓋率。明列百分比的行情才是 audit 的全市場代碼覆蓋。PIT 欄是 source-feed version coverage；沒有 accepted rows 時為 N/A。

| source key | effective date | raw/accepted/quarantine/duplicate | accepted L/O 對碼 | PIT % |
|---|---|---:|---|---:|
| `taifex_futures_large_trader_positions_daily` | 10/05 | 1,386/1,386/0/0 | —（期貨彙總） | 100 |
| `taifex_index_futures_positions_daily` | 10/05 | 6/6/0/0 | —（期貨彙總） | 100 |
| `taifex_index_options_positions_daily` | 10/05 | 6/6/0/0 | —（選擇權彙總） | 100 |
| `tdcc_shareholder_distribution_weekly` | 10/02 | 69,326/69,326/0/0 | L 1,054/1,054；O 891/893 | 100 |
| `tpex_daytrade_market_summary_daily` | 10/01–10/05 | 3/3/0/0 | —（市場彙總） | 100 |
| `tpex_director_shareholding_shortfall_daily` | 08/19 | 15/15/0/0 | L 0；O 15 | 100 |
| `tpex_equity_quotes_daily` | 10/06 | 889/872/17/0 | O 872/893 (97.65%) | 100 |
| `tpex_exright_preannouncement_daily` | 09/30–11/05 | 76/76/0/0 | O 14 個代碼；其餘為非普通股/未配列，不作市場覆蓋率 | 100 |
| `tpex_foreign_investor_holdings_daily` | 10/06 | 890/890/0/0 | O 889/893 (99.55%)；1 未配對 | 100 |
| `tpex_institutional_flow_daily` | 10/06 | 904/904/0/0 | O 783/893 (87.68%)；121 未配對維持 unknown | 100 |
| `tpex_major_shareholders_daily` | 10/06 | 712/712/0/0 | O 468 個代碼（稀疏披露） | 100 |
| `tpex_margin_balance_daily` | 10/05 | 802/802/0/0 | O 802 個代碼 | 100 |
| `tpex_market_index_daily` | 10/01–10/06 | 4/4/0/0 | —（市場指數） | 100 |
| `tpex_market_total_return_index_daily` | 10/01–10/06 | 4/4/0/0 | —（市場指數） | 100 |
| `tpex_securities_borrowing_daily` | 10/05 | 815/815/0/0 | O 815 個代碼 | 100 |
| `tpex_trading_attention_daily` | 10/05–10/06 | 47/47/0/0 | O 35 個代碼（事件列） | 100 |
| `tpex_trading_disposition_daily` | 09/23–10/06 | 12/12/0/0 | O 10 個代碼（事件列） | 100 |
| `tpex_valuation_metrics_daily` | 10/06 | 888/888/0/0 | O 888 個代碼 | 100 |
| `twse_director_shareholding_shortfall_daily` | 08/19 | 12/12/0/0 | L 12 | 100 |
| `twse_equity_quotes_daily` | 10/06 | 1,052/1,052/0/0 | L 1,052/1,054 (99.81%) | 100 |
| `twse_exright_preannouncement_daily` | 10/02–10/29 | 62/62/0/0 | L 14 個代碼；事件列非全市場覆蓋 | 100 |
| `twse_major_shareholders_daily` | 10/05 | 398/398/0/0 | L 274 個代碼；稀疏披露 | 100 |
| `twse_margin_balance_daily` | 10/06 | 1,039/994/45/0 | L 994 個代碼；45 quarantine | 100 |
| `twse_market_index_daily` | 10/01–10/06 | 4/4/0/0 | —（市場指數） | 100 |
| `twse_market_total_return_index_daily` | 10/01–10/06 | 4/4/0/0 | —（市場指數） | 100 |
| `twse_quarterly_forecast_attainment` | 06/30 | 8/8/0/0 | L 6 個代碼；非全市場覆蓋 | 100 |
| `twse_tpex_sbl_available_daily` | 10/06 | 1,715/1,715/0/0 | L 978；O 737 | 100 |
| `twse_trading_attention_daily` | 無接受列/無日期 | 0/0/0/0 | —；空回應不是零事件結論 | N/A |
| `twse_trading_disposition_daily` | 09/29–10/05 | 10/10/0/0 | L 10 個代碼（事件列） | 100 |
| `twse_valuation_metrics_daily` | 10/05 | 1,083/1,083/0/0 | L 1,052 個代碼；31 accepted codes 在普通股主檔外 | 100 |

市場及價格判讀：TWSE 股票行情 CSV fallback 已由自然 source run 驗證同日 10/06 收錄。TPEx byte-range 回應 HTTP 206 經完整性檢查後接受 872 列，17 列隔離；目前主檔有 21 個上櫃普通股代碼沒有接受行情列，掃描日有效收盤只有 865/893（96.86%），另 7 列有明確 no-trade 狀態。其餘缺列/隔離不等同零成交。兩市場 index 與 total-return index 有效日都含 10/06，daily child 的 benchmark alignment 均通過。兩個 director-shortfall feed 雖自然收集成功，effective date 都停在 08/19（48 日前），不能當 10/06 即時事件。

### Daily process：23 個預期 source keys

自然 run `source_extension_collect_process / scheduled__2026-10-04T20:00:00+00:00`（10/06 04:00 台北）23/23 source keys；無 missing/契約漂移，audit 仍為 `data_review_required`。下表列 effective date、raw/accepted/quarantine/duplicate、目前主檔匹配代碼數與 PIT；其中 EPS 八來源的合併覆蓋另列在表後，產業 feed 的逐 key 數字不可相加成市場覆蓋。

| source key | effective date | raw/accepted/quarantine/duplicate | accepted L/O 對碼 | PIT % |
|---|---|---:|---|---:|
| `cbc_fx_daily` | 2025/09/30–2026/09/30 | 3,660/3,660/0/0 | —（匯率） | 100 |
| `tpex_balance_sheet_bd` | 06/30 | 7/7/0/0 | L 0；O 7 | 100 |
| `tpex_balance_sheet_ci` | 06/30 | 884/884/0/0 | L 0；O 884 | 100 |
| `tpex_daytrade_eligibility_daily` | 10/06 | 843/843/0/0 | O 727；116 未配對 | 100 |
| `tpex_dividends` | 01/29–10/02 | 964/964/0/0 | O 891 | 100 |
| `tpex_income_statement_bd` | 06/30 | 7/7/0/0 | O 7 | 100 |
| `tpex_income_statement_ci` | 06/30 | 884/884/0/0 | O 884 | 100 |
| `tpex_quarterly_profitability` | 06/30 | 884/884/0/0 | O 884/893 (98.99%) | 100 |
| `twse_balance_sheet_basi` | 06/30 | 10/10/0/0 | L 10 | 100 |
| `twse_balance_sheet_bd` | 06/30 | 3/3/0/0 | L 3 | 100 |
| `twse_balance_sheet_ci` | 06/30 | 1,049/1,049/0/0 | L 1,018；31 master 外 | 100 |
| `twse_balance_sheet_fh` | 06/30 | 13/13/0/0 | L 13 | 100 |
| `twse_balance_sheet_ins` | 06/30 | 5/5/0/0 | L 5 | 100 |
| `twse_balance_sheet_mim` | 06/30 | 4/4/0/0 | L 4 | 100 |
| `twse_daytrade_eligibility_daily` | 10/05 | 1,228/1,228/0/0 | L 972；256 未配對 | 100 |
| `twse_dividends` | 01/30–09/21 | 1,228/1,228/0/0 | L 1,053 | 100 |
| `twse_income_statement_basi` | 06/30 | 10/10/0/0 | L 10 | 100 |
| `twse_income_statement_bd` | 06/30 | 3/3/0/0 | L 3 | 100 |
| `twse_income_statement_ci` | 06/30 | 1,049/1,049/0/0 | L 1,018；31 master 外 | 100 |
| `twse_income_statement_fh` | 06/30 | 13/13/0/0 | L 13 | 100 |
| `twse_income_statement_ins` | 06/30 | 5/5/0/0 | L 5 | 100 |
| `twse_income_statement_mim` | 06/30 | 4/4/0/0 | L 4 | 100 |
| `twse_quarterly_profitability` | 06/30 | 1,054/1,049/5/0 | L 1,018/1,054 (96.58%) | 100 |

八個 `income_statement_*` EPS feeds 10/06 04:00 第三批合計 1,975 accepted、0 EPS null、0 quarantine、0 duplicate，source-feed version counts 1,975/1,975（100%）。`twse_income_statement_fh` 本次有一次 0 列 failed attempt，retry 成功 13/13，最後來源 audit 為 success。去重後合併 accepted entity IDs 對目前普通股主檔為上市 1,053/1,054、上櫃 891/893；主檔外代碼不視為普通股覆蓋。八個 feed 仍只有 2026Q2，沒有同比所需上一年同季；負 EPS 只作反向脈絡，不加方向家族。

### Intraday：10 個預期 source keys

最新自然 run `source_extension_collect_intraday / scheduled__2026-10-06T12:15:00+00:00` 台北 20:30:01–20:32:18；10/10 成功但 audit `data_review_required`。

| source key | effective date | raw/accepted/quarantine/duplicate | accepted L/O 對碼 | PIT % |
|---|---|---:|---|---:|
| `tpex_company_events` | 10/05 | 29/29/0/0 | O 21 個代碼 | 100 |
| `tpex_insider_transfer_planned` | 10/05 | 19/1/17/1 | O 1 | 100 |
| `tpex_insider_transfer_untransferred` | 無接受列/無日期 | 1/0/1/0 | —；隔離空白 sentinel，不代表零 | N/A |
| `tpex_monthly_revenue` | 08/01 | 892/892/0/0 | O 891/893；1 未配對 | 100 |
| `tpex_quarterly_forecast_variance_events` | 10/06 | 1/1/0/0 | 0 company event rows；只有 snapshot metadata | 100 |
| `twse_company_events` | 10/05 | 66/66/0/0 | L 49；4 未配對 | 100 |
| `twse_insider_transfer_planned` | 10/05 | 19/3/15/1 | L 3 | 100 |
| `twse_insider_transfer_untransferred` | 10/05 | 4/3/1/0 | L 3 | 100 |
| `twse_monthly_revenue` | 08/01 | 1,086/1,086/0/0 | L 1,054/1,054 (100%) | 100 |
| `twse_quarterly_forecast_variance` | 06/30 | 7/7/0/0 | L 5 | 100 |

這是公告增量流程，不是全市場日線掃描。Latest child `ready/partial`、0 candidates、candidate batches sent 0；source-warning status unchanged，沒有新 receipt 確認。Blank sentinel、被隔離列及無公司事件的 metadata 不能解讀成零事件或零變動。

### News：12 個 feeds

最新自然批次 `news_ch_collect_process / scheduled__2026-10-06T08:30:00+00:00` logical 16:30，實際 18:30–18:39 台北；collect 與 `delete_and_insert` 均 success，1339 accepted rows 已持久化，回執時間 18:39:47。共 1,372 URLs、1,365 parsed/valid-time、1,339 accepted、7 fetch/parse reject、0 invalid-time quarantine、26 content rejects；8 feeds complete、4 partial。48h observed/cutoff 為 10/06 18:39:43 / 10/04 18:39:43；1,189 篇 exact datetime 在窗內、56 篇 date-only 落在 cutoff 日之後（不是精確 48h 證據）、94 篇按現有時間精度較舊，未來時間 0。

| feed | URL / accepted | fetch/parse reject | content reject | status |
|---|---:|---:|---:|---|
| ETtoday | 500/500 | 0 | 0 | complete |
| TVBS | 16/16 | 0 | 0 | complete |
| iThome | 60/60 | 0 | 0 | complete |
| newtalk | 25/25 | 0 | 0 | complete |
| The News Lens | 90/90 | 0 | 0 | complete |
| 三立新聞 | 30/27 | 3 | 0 | partial |
| 中央社 | 100/96 | 4 | 0 | partial |
| 中時日報 | 198/198 | 0 | 0 | complete |
| 公視新聞 | 43/43 | 0 | 0 | complete |
| 聯合報 | 60/60 | 0 | 0 | complete |
| 自由時報 | 100/75 | 0 | 25 | partial |
| 鉅亨網 | 150/149 | 0 | 1 | partial |

### Scan, outcomes, day-trade boundaries

- 自然 after-close child `market_signal__daily__source_extension_collect_after_close__scheduled__2026-10-05T12:00:00+00:00`：`ready/partial`，as-of 10/06，universe 1,947；price 1,917、flow 0、revenue 1,945、financial 1,902、borrow 1,795、insider 1,939、dividend 1,944、candidate 0。分市場價格覆蓋：上市 1,052/1,054=99.81%；上櫃 accepted row 872/893=97.65%、可用價格 865/893=96.86%，7 個 no-trade marker、21 個主檔代碼沒有 accepted quote。兩市場基準 effective date 均對齊 10/06。自動資料不足通知 1 row 有 platform receipt、HTTP 200；不是方向卡。
- 最新 intraday child 為 `ready/partial`、0 candidate、沒有候選批次 receipt；不能以事件掃描推論全市場方向訊號不存在。
- `market_signal_outcomes`：legacy H1 9/24 signal-close 81 settled（50 hits、平均超額 +1.776pp）、next-close 81 settled（37 hits、+0.987pp）。新增 TPEx-only H1 next-close 10/02 一個 signal date、44 settled、12 hits（27.27%）、平均超額 -1.119pp；同 date 的 signal-close 仍 pending。TWSE 新 quote H1 與 H5/H10 仍 pending；尚無 settled H5/H10。這些 outcome 僅為少數 signal dates、跨價格來源，且未調整公司行動/成本，不改分數或門檻。
- TWSE `daytrade_eligibility_daily` 是資格資料，不是個股當沖量。上市 TWTB5U 尚無已確認 entitlement、機器交付與儲存/外發使用權；TPEx individual endpoint 空資料及授權不確定，維持不接入。TPEx OAS 11627 只含 3 筆市場彙總（有效日 10/01–10/05），freshness gate 未對齊 10/06，不能冒充個股訊號。
- 只保存上述自動 feed/自然 run 證據；不人工巡查券商網站、不逐公司/逐份報告評分。沒有新增授權券商批次 feed，原研究准入缺口不變。

## 2026-10-05 22:37 同日指數補收與 TPEx 行情缺值拆分

- 平日 20:00 的既有 source_extension_collect_after_close 現在會在 TWSE 個股行情、TAIEX 收盤指數及 TAIEX 報酬指數主端點落後時，改用各自政府資料開放授權 CSV；只收最新日期等於掃描日的列。個股行情來源是資料集 [11549](https://data.gov.tw/dataset/11549)，TAIEX 是 [11755](https://data.gov.tw/dataset/11755)，報酬指數是 [11871](https://data.gov.tw/dataset/11871)。保留原始 CSV 欄位並對應到來源必要欄位；不同日期或欄位契約不符就不當成補收成功。
- 22:37 台北記憶體唯讀預檢：三個 TWSE CSV 都回 HTTP 200，個股行情 1,381 raw/1,052 accepted、TAIEX 與報酬指數各 3 raw/3 accepted，三者最新有效日都是 10/05。這是程式與來源預檢，不是自然 DAG 回執；沒有寫入 source_records，10/05 舊 snapshot 仍未回填。
- 同時間 TPEx 官方行情唯讀預檢為 888 列、876 accepted、12 quarantine、有效日 10/05。5 列是官方明確零成交（成交股數、金額、筆數皆 0），1 列收盤價 7.51 可用但漲跌欄是「除息」，其餘 12 列的收盤與漲跌欄皆為 ---，其中仍有成交量；不以均價、昨收或參考價推造收盤。相較之下，10/05 自然 run 入庫狀態仍是 870 accepted/18 quarantine，因新正規化尚未由自然收集執行。普通股主檔另有 4 檔未出現在這 888 列，持續保持 unknown。
- 訊號與結果用法：已知零成交會另列計數，不算有效收盤價或方向票；可用收盤但非數字漲跌標籤的列保留 close，不硬算漲跌；缺收盤列仍排除於價格訊號與 outcome。既有每日掃描會重試尚 pending 的 H1/H5/H10 outcome；只有固定窗口所需個股收盤與同日市場指數都可用時才結算。TradingAgents 參考其按發布日期呈現資料、固定持有期並對區域基準結算的做法；不引入 LLM 交易決策。[TradingAgents](https://github.com/TauricResearch/TradingAgents)
- 仍未能用已核准公開批次補齊的缺口：TWSE 個股法人/外資持股仍需正式授權；TPEx 12 檔缺收盤的來源列及 4 檔未回傳證券沒有可安全推算的數值；尚未成熟的 H5/H10 必須等待後續交易日。這些狀態維持 unknown/pending，不以估算填滿。

## 2026-10-05 21:07 同日 TWSE 行情補收與訊號可判讀狀態

- 20:00 自然 after-close 的 TWSE `STOCK_DAY_ALL` 主端點仍只到 10/02；10/05 21:07 台北唯讀重抓也回 HTTP 200、1,380 列、effective date `1151002`。這解釋為何 10/05 daily snapshot 的上市有效行情是 0/1,054：擷取器原先把有列但過期的回應當成功，沒有同日補收。
- 加入資料集 11549 明列的 TWSE CSV 備援 `STOCK_DAY_ALL?response=open_data`：僅在 OpenAPI 主端點錯誤、無有效日或落後台北當日才呼叫；核對必要欄位與回傳日期等於掃描日，僅正規化目前上市普通股主檔。缺值不補零；備援失敗時留用舊批次資料，訊號仍標 partial。
- 同日 TWSE 開放資料 CSV 唯讀預檢：HTTP 200、1,381 列、有效日 `1151005`。配對目前上市普通股後，adapter 記憶體內正規化得到 1,052 accepted、0 quarantine，覆蓋 1,052/1,054（99.81%）；缺的 2 檔維持 unknown。未寫 `source_records` 或 `source_runs`；這不是自然排程回執，10/05 舊快照也沒有回填。
- 收盤摘要已把 partial/unknown 放到標題與首段：明說「資料不足」時，0 檔只代表沒有可確認候選，不能推論沒有訊號或偏空。directional 分數、三家族候選條件與市場覆蓋門檻均未變。
- 來源依據：[TWSE 個股日成交資料集 11549](https://data.gov.tw/dataset/11549) 明列 CSV 資源，標示每日更新、免費及政府開放資料授權第 1 版。TradingAgents 的 PIT 資料及按固定持有期對基準結算做法作為架構參考；本系統維持官方批次來源與可追溯證據，不移植 LLM 交易決策。[TradingAgents](https://github.com/TauricResearch/TradingAgents)

## 2026-10-05 20:53 自動批次訊號與季 EPS 首次收集核對

- 八個新增的官方季度綜合損益/EPS feed 首次出現在自然 daily run `source_extension_collect_process / scheduled__2026-10-02T20:00:00+00:00`，10/04 04:00–04:00:20 台北。`source_runs` 對每個 source key 都記錄 success，總計 1,975 raw / 1,975 accepted / 0 quarantine / 0 duplicate；所有 normalized rows 的 `created_at` 對應該首次批次，effective date 為 2026-06-30。`basic_eps_ntd_per_share` 是必要欄位；1,975 accepted rows 中 EPS 缺值 0。

| source key | raw / accepted / quarantine / duplicate | effective date | accepted rows 對目前 common-stock master | EPS 缺值 |
|---|---:|---|---|---:|
| `twse_income_statement_basi` | 10 / 10 / 0 / 0 | 2026-06-30 | 上市 10 | 0 |
| `twse_income_statement_bd` | 3 / 3 / 0 / 0 | 2026-06-30 | 上市 3 | 0 |
| `twse_income_statement_ci` | 1,049 / 1,049 / 0 / 0 | 2026-06-30 | 上市 1,018；31 個 accepted codes 在目前普通股主檔外 | 0 |
| `twse_income_statement_fh` | 13 / 13 / 0 / 0 | 2026-06-30 | 上市 13 | 0 |
| `twse_income_statement_ins` | 5 / 5 / 0 / 0 | 2026-06-30 | 上市 5 | 0 |
| `twse_income_statement_mim` | 4 / 4 / 0 / 0 | 2026-06-30 | 上市 4 | 0 |
| `tpex_income_statement_bd` | 7 / 7 / 0 / 0 | 2026-06-30 | 上櫃 7 | 0 |
| `tpex_income_statement_ci` | 884 / 884 / 0 / 0 | 2026-06-30 | 上櫃 884 | 0 |

- 分類產業 feed 的 audit 單列 `market_coverage` 為 null，不能把各分類列數加總當市場完整。合併去重後對目前 common-stock master：上市 1,053/1,054（99.91%）、上櫃 891/892（99.89%）；各市場 1 檔未配對維持 unknown，TWSE CI 的 31 個 master 外代碼也不算普通股覆蓋。這是目前主檔配對，不是歷史 PIT 母體覆蓋。
- 首次 run 沒有任何 `source_record_versions` 帶該 run lineage；不得將後續版本回填成 10/04 當時可見。第二次自然 daily run `scheduled__2026-10-03T20:00:00+00:00`（10/05 04:00 台北）為 1,975 accepted rows 捕捉 source-feed versions，audit 逐來源版本覆蓋 100%。每個 feed 目前只存到 2026Q2 一個有效比較期，無可比上一年同季，不作 EPS/損益同比；候選卡負 EPS 僅作反向脈絡、不加分。
- 10/05 20:00 after-close parent success 但 audit `partial`（30 expected / 28 reported），缺 `twse_director_shareholding_shortfall_daily`、`tpex_director_shareholding_shortfall_daily`。其餘最新核心列：TWSE quotes 1,051/1,051、effective 10/02、上市覆蓋 1,051/1,054；TPEx quotes 888/870/18/0、effective 10/05、上櫃 870/892；TPEx institutional flow 916/916、effective 10/05、795/892 對碼、121 unmatched 保持 unknown；TPEx daytrade market summary 2/2、有效日範圍 10/01–10/02，只有市場彙總。TWSE/TPEx 指數未同日對齊。詳見下方 20:00 來源明細表。
- 最新 intraday parent `scheduled__2026-10-05T12:30:00+00:00`（20:45–20:47:51 台北）10/10 tasks success，audit `data_review_required`；兩個月營收來源 effective 2026-08-01，TWSE 1,086/1,086、上市 1,054/1,054；TPEx 892/892、上櫃 891/892、1 unmatched。公司事件有效日 10/04；計畫轉讓兩市各 1/1 accepted、effective 10/02；未轉讓來源各 1 個空白 sentinel quarantine，不代表零事件。child scan 20:47:46–20:47:56 `ready/partial`、0 candidates、通知 `sent=0`。
- 最新新聞自然批次實際 18:30–18:39:19 台北：12 feeds、1,329 URLs、1,327 parsed/valid-time、1,304 accepted、2 fetch/parse reject、23 blank-body/content rejects、0 invalid-time quarantine；9 complete / 3 partial，`create_time_tpe=18:39:17`，48h cutoff 10/03 18:39:14。
- 成熟 outcomes：只有 9/24 H1 signal-close/next-close 各 81 settled；signal-close 另有 10/02 50 rows pending；沒有 H5/H10，且已結算資料只用 `legacy_stock_table`，不據單一日期調門檻。

## 2026-10-05 20:00 自然收盤批次與自動來源增量

- 自然 run `source_extension_collect_after_close / scheduled__2026-10-02T12:00:00+00:00` 於 20:00:07–20:02:24 台北執行，Airflow parent success。用目前 source definition 唯讀稽核為 30 預期、28 回報、兩個新 source keys 缺席；稽核 `partial`。新 statutory-shortfall source 定義於這輪之後進入 catalog，未手動觸發或回補；明日 10/06 20:00 才等下一次自然收集。28 個實際來源如下，全部已接受列的 PIT version coverage 為 100%；TWSE 注意股 0 列所以 PIT coverage 不適用。

| source key | raw / accepted / quarantine / duplicate | 最新 effective date | 對碼／覆蓋或缺口 |
|---|---:|---|---|
| `taifex_futures_large_trader_positions_daily` | 1,386 / 1,386 / 0 / 0 | 10/02 | 期交所部位彙總，無個股市場分母 |
| `taifex_index_futures_positions_daily` | 6 / 6 / 0 / 0 | 10/02 | 期貨市場彙總，無個股市場分母 |
| `taifex_index_options_positions_daily` | 6 / 6 / 0 / 0 | 10/02 | 選擇權市場彙總，無個股市場分母 |
| `tdcc_shareholder_distribution_weekly` | 69,326 / 69,326 / 0 / 0 | 10/02 | 上市 1,054/1,054（100%）；上櫃 890/892（99.78%，2 檔缺列） |
| `tpex_daytrade_market_summary_daily` | 2 / 2 / 0 / 0 | 10/02（列日含 10/01） | TPEx 市場彙總，無個股列；不得當個股訊號 |
| `tpex_equity_quotes_daily` | 888 / 870 / 18 / 0 | 10/05 | 上櫃 870/892（97.53%），18 quarantine |
| `tpex_exright_preannouncement_daily` | 76 / 76 / 0 / 0 | 11/05 | 14 個上櫃代碼匹配、62 列主檔外；公司行動清單，非全市場分母 |
| `tpex_foreign_investor_holdings_daily` | 889 / 889 / 0 / 0 | 10/05 | 上櫃 888/892（99.55%），1 筆 unmatched；持股快照非當日買賣 |
| `tpex_institutional_flow_daily` | 916 / 916 / 0 / 0 | 10/05 | 上櫃 795/892（89.13%），121 筆 unmatched 保持 unknown |
| `tpex_major_shareholders_daily` | 386 / 386 / 0 / 0 | 10/05 | 253 個上櫃代碼匹配；只列官方揭露持股逾 10% 股東，無全市場分母 |
| `tpex_margin_balance_daily` | 802 / 802 / 0 / 0 | 10/02 | 802 個上櫃代碼匹配；未宣告完整市場覆蓋 |
| `tpex_market_index_daily` | 3 / 3 / 0 / 0 | 10/05 | TPEx 市場指數彙總 |
| `tpex_market_total_return_index_daily` | 3 / 3 / 0 / 0 | 10/05 | TPEx 總報酬指數彙總 |
| `tpex_securities_borrowing_daily` | 815 / 815 / 0 / 0 | 10/02 | 815 個上櫃代碼匹配；不是全市場覆蓋宣告 |
| `tpex_trading_attention_daily` | 40 / 40 / 0 / 0 | 10/05 | 30 個上櫃代碼匹配；公告事件列，不計全市場覆蓋率 |
| `tpex_trading_disposition_daily` | 13 / 13 / 0 / 0 | 10/05 | 12 個上櫃代碼匹配；公告事件列，不計全市場覆蓋率 |
| `tpex_valuation_metrics_daily` | 887 / 887 / 0 / 0 | 10/05 | 887 個上櫃代碼匹配；來源沒有完整覆蓋 gate |
| `twse_equity_quotes_daily` | 1,051 / 1,051 / 0 / 0 | 10/02 | 上市 1,051/1,054（99.72%）；未與 10/05 上櫃行情同日對齊 |
| `twse_exright_preannouncement_daily` | 58 / 58 / 0 / 0 | 10/28 | 10 個上市代碼匹配、48 列主檔外；公司行動清單，非全市場分母 |
| `twse_major_shareholders_daily` | 174 / 174 / 0 / 0 | 10/04 | 120 個上市代碼匹配、5 筆 unmatched；稀疏披露，無全市場分母 |
| `twse_margin_balance_daily` | 1,039 / 993 / 46 / 0 | 10/05 | 993 個上市代碼匹配，46 quarantine |
| `twse_market_index_daily` | 2 / 2 / 0 / 0 | 10/02 | TAIEX 日期落後 10/05 股票掃描日 |
| `twse_market_total_return_index_daily` | 2 / 2 / 0 / 0 | 10/02 | TWSE 總報酬指數日期落後 10/05 股票掃描日 |
| `twse_quarterly_forecast_attainment` | 8 / 8 / 0 / 0 | 06/30 | 6 個上市代碼匹配；事件/財測資料，不是全市場分母 |
| `twse_tpex_sbl_available_daily` | 1,718 / 1,718 / 0 / 0 | 10/05 | 上市 980、上櫃 738 個代碼匹配；可借券資格/供給範圍，缺席不當零量 |
| `twse_trading_attention_daily` | 0 / 0 / 0 / 0 | 無 | 空回應有 `empty_response_requires_source_semantics`；不能推成零注意股 |
| `twse_trading_disposition_daily` | 8 / 8 / 0 / 0 | 10/02 | 8 個上市代碼匹配；公告事件列，不計全市場覆蓋率 |
| `twse_valuation_metrics_daily` | 1,082 / 1,082 / 0 / 0 | 10/02 | 1,051 個上市代碼匹配、31 筆主檔外 |

- 收盤候選子 run 20:02–20:02:30 台北成功，`as_of_date=2026-10-05`、universe 1,946、上市行情 0/1,054、上櫃行情 870/892、法人流 0、候選 0、來源 `partial`、market source `data_review_required`。上市行情有效日仍 10/02，市場基準也未對齊；不能將 0 候選解讀成零訊號。
- 最新成熟 `market_signal_outcomes` 仍只在 2026-09-24，H1 signal-close/next-close 各 81 列，全部是 `legacy_stock_table`；無 H5/H10。這次 after-close XCom 對歷史 131 candidates 回報新結算 0、pending-data 293、最新行情日期 10/05，不憑單輪調方向門檻。
- 新增兩個稀疏、官方、免憑證批次來源：TWSE dataset [20767](https://data.gov.tw/dataset/20767)，endpoint `https://openapi.twse.com.tw/v1/opendata/t187ap08_L`；TPEx dataset [20768](https://data.gov.tw/dataset/20768)，endpoint `https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap08_O`。資料集標示免費、政府開放授權第 1 版，欄位含公司代號、出表日、已發行股數、董事/監察人應持與實持、法定不足股數及通知文字。匿名預檢各回 12/15 rows，出表日均為 2026-08-19；這不是 pipeline accepted count。來源名單只表示當前列出的持股不足，不作全市場合規清單；缺席 unknown。TWSE 頁中英文更新頻率文字不一，TPEx 標不定期，故每日 polling、60 日 stale review，不宣稱日更 SLA。
- 正規化保存原始欄位與董監應持/實持/不足股數。只有明確正不足、有效出表日、股票代碼/公司名和核心數值都有效才接受；只對已經達方向門檻的候選附「治理風險背景」，不新建方向訊號、不增加分數、不單發通知。下一個自然收盤 run 要驗 raw/accepted/quarantine/duplicate、日期、PIT versions、上市櫃匹配與外部通知回執。

## 2026-10-05 18:30 新聞自然批次（20:23 唯讀回看）

- `news_ch_collect_process / scheduled__2026-10-05T08:30:00+00:00` 18:30:00–18:39:19 台北成功：12 feeds、1,329 URLs、1,327 parsed/有效時間、1,304 accepted、2 fetch/parse rejects、23 blank-body/content rejects、0 invalid-time quarantines；時間後置篩選 reconciliation 通過。
- freshness observed 18:39:14、48 小時 cutoff 10/03 18:39:14。精確時間窗 1,141；cutoff 後日期精度 30、cutoff 日時刻未知 5、舊資料 128、未來 0。逐 feed 的 parse/accept/reject 在自然 XCom 核對；中央社 99/100，LTN 77/100（23 空白內文），鉅亨網 149/150，其他 feeds 本輪全數接受。

## 2026-10-05 16:51 最新自然批次狀態

- Scheduler `discover_approved_sources("daily_after_close")` 唯讀回傳 28 個 keys，TDCC 與上市/上櫃大股東來源已在 runtime catalog；10/05 20:00 after-close 尚未執行，不能把 catalog 登記當成已收集。最新實際 after-close parent 是 10/02 failed run；歷史 `source_runs` 有 17 筆因缺 `dag_id` 無法歸屬，稽核狀態維持 unknown。
- 最新 intraday parent `scheduled__2026-10-05T08:15:00+00:00` 於 16:30:00–16:30:36 實際執行，10/10 tasks 成功、audit `data_review_required`。公司事件有效日 10/04；TWSE 月營收 1,086 accepted、上市普通股覆蓋 1,054/1,054，TPEx 892 accepted、891/892、1 unmatched；兩者有效日 08/01。planned transfers 有效日 10/02；untransferred 空白 sentinel 是 quarantine，不是零事件。詳列在品質追蹤文件。
- 最近完整日線仍為 10/02，50 candidates/0 notified；成熟 outcomes 只到 9/24 H1，無 H5/H10。盤中 child 的 universe/price/flow/candidates 均 0，是事件增量掃描，不能解讀成沒有全市場訊號。
- 最新新聞自然批次為 16:30 logical、實際 16:30:02–16:39:13：12 feeds、1,247 URLs、1,246 parsed、1,216 有效時間、1,197 accepted；1 fetch/parse reject、30 invalid-time quarantine、19 content/duplicate rejects。SETN 30 列因時間佐證不足被隔離；CNA 100 URLs/99 accepted、LTN 96 parsed/78 accepted（18 blank body）、CNYES 150 parsed/149 accepted（1 blank body；34 舊列），iThome/TNL 多為舊資料或日期精度。分鐘標籤相對年齡上限窄幅放至 +5 分鐘，仍要求列表佐證，待下一自然批次驗證。

## 2026-10-05 16:25 官方上市/上櫃大股東名單加入自動收集

- 加入兩個官方每日批次端點：TWSE `/v1/opendata/t187ap02_L` 與 TPEx `/openapi/v1/mopsfin_t187ap02_O`，分別對應政府開放資料 [18422](https://data.gov.tw/dataset/18422)、[18423](https://data.gov.tw/dataset/18423)。資料集標示免費、Government Open Data License v1；欄位是出表日期、公司代碼/名稱及大股東名稱，來源不提供股數或精確持股比例。
- 10/05 匿名唯讀預檢為上市 174 列、資料日期 10/04；上櫃 386 列、資料日期 10/05。這些是名單列數，不是市場覆蓋率；因名單只含揭露持股超過 10% 者，不配置全市場覆蓋 gate。每次自然批次仍稽核有效日、接受/隔離/重複列、代碼去重及 unmatched 主檔列。
- 程式化 registry 新增 `twse_major_shareholders_daily`、`tpex_major_shareholders_daily`，排在 `daily_after_close`，5 日 effective-date freshness gate；normalizer 以出表日+代碼+大股東名稱保存 PIT 快照。新型別是名單身分背景，缺席語意為 unknown，不補零、不推定股東離場，不進方向分數或通知。名單跨日變動在累積 PIT 歷史、查清欄位修訂語意並驗證成熟 outcomes 前仍是假說。
- Airflow 的舊 catalog 尚未經今日自然流程 bootstrap 這兩個新 key；目前程式定義中的收盤來源共 28 個，晚間 run 前仍不可宣稱新來源已完成收集。

## 2026-10-05 16:11 官方全市場持股分布 API 加入自動收集候選

- TDCC 官方 OpenAPI 規格有 `GET /v1/opendata/1-5`「集保戶股權分散表」，無日期/證券查詢參數、無分頁欄位，且規格沒有宣告 API key/security。匿名唯讀請求實測 HTTP 200，JSON 全量回傳 69,326 列、4,078 個證券代碼；唯一資料日期為 2026-10-02，每代碼觀察到 17 個分級列。官方原始 [OpenAPI](https://openapi.tdcc.com.tw/tdcc-opendata-api-docs) 與 [Swagger UI](https://openapi.tdcc.com.tw/)；政府資料集 [11452](https://data.gov.tw/dataset/11452) 標示免費、Government Open Data License v1。
- 發布頻率有 metadata 差異：data.gov.tw 把更新頻率列為「每1月」，TDCC 官方個股說明則稱每週最後營業日收盤後編製。實測日期 10/02 與週頻敘述相符，但單一快照不能證明每週交付 SLA；先採官方發布者說明，追蹤連續自然批次日期。
- 與本地普通股主檔唯讀比對：上市 1,054/1,054、上櫃 890/892。兩檔上櫃普通股未出現在資料快照，覆蓋是 99.78%，保留缺口，不補零。非普通股或未匹配代碼仍保存為 `security`，不混入個股訊號覆蓋或方向分數。
- 已在 runtime allowlist/程式化 source definition 加入 `tdcc_shareholder_distribution_weekly`：每日收盤批次輪詢官方週資料，保留有效日與 PIT 版本；10 日有效日 freshness gate；普通股代碼需有觀察到的 1–17 分級且分級不得重複，缺級列隔離並降低上市/上櫃覆蓋。Airflow DB registry 尚待下次自然 run 的 schema/catalog task 自動 bootstrap；這個 endpoint 每次約 9.8 MB，採 20 MiB 單源上限。
- 目前只保存為籌碼背景與未來特徵研究資料，不投方向分數、不單獨發卡。API schema 只定義欄位型別為字串，分級邊界/數值定義未由該 schema 說明；在核對官方級距說明、累積 PIT 週資料並等成熟 outcomes 前，不把級距移動解讀成大戶增減或交易方向。10/05 20:00 自然 after-close 才能首次驗證 registry 自動 bootstrap、實收/effective date、隔離/重複列、上市櫃覆蓋與 PIT versions；本次未手動觸發 DAG。
- 後續特徵假說依 TDCC e 手掌握手冊：以 API 每週級距持股比例計算週變化及連續上升週數，參照官方產品中「股權分散數據連增排行榜」與近 4/12 期趨勢功能；只用核准全市場 API 自行計算，不接個股查詢頁路徑。官方手冊證明這類描述存在，不證明預測力；保留級距定義/公司行動/成熟 outcome 驗證閘門。

## 工作範圍：自動化 feed 優先

這份盤點服務於可排程、可程式化的批次收集與訊號評估，不做逐家公司上網找券商報告、人工評分後彙總。新來源只有在存在官方或明確授權的 API／批次檔案交付時才進入候選；並須記錄使用權、涵蓋證券範圍、發布與有效時間、分頁／總量等完整性契約、修訂版本留存、上市櫃覆蓋，以及每次自然任務的接受／隔離／重複列數。券商原始研究若沒有合法的機器化批次介面與可驗證覆蓋，就保留為來源缺口，不以人工逐家瀏覽代替自動化。

自動收集的新聞和市場背景先保留來源事實、分析內容與未知事項的區別，並受發布時間及 PIT 截止限制；只有在足夠成熟、可點時重建的 outcome 樣本中驗證後，才考慮影響方向分數。成功狀態、候選列或 ledger 都不能單獨代表來源完整、訊號有效或平台已收件。

## 2026-10-05 15:55 官方批次訊號缺口復核

- 重新從 Airflow scheduler 載入 after-close registry：仍為 25 個 approved/enabled keys；個股法人流只有 `tpex_institutional_flow_daily`，持股只有 `tpex_foreign_investor_holdings_daily`；當沖只有 `tpex_daytrade_market_summary_daily`（市場彙總）。沒有上市個股法人 flow 或個股當沖成交量 source key。
- 政府資料開放目錄的上櫃個股三大法人資料集 [11856](https://data.gov.tw/dataset/11856) 是每日、免費、政府開放授權第 1 版，含外資、投信、自營商個股買賣股數；既有 TPEx feed 可作上櫃機器化來源。上市法人資料的 [136987](https://data.gov.tw/suggests/136987) 與 [137032](https://data.gov.tw/suggests/137032) 都是「建議開放」紀錄，不是資料集/API；TWSE Swagger 公開的法人 endpoints 是產業持股比例與前 20 名，TWTB4U 是當沖資格/限制欄位，不能當 T86 個股法人流或成交量。不得改用逐股網站查詢。
- 一個高價值但尚未授權接入的候選是 TWSE Data E-Shop [TWTB5U 當日沖銷交易標的及統計](https://eshop.twse.com.tw/zh/product/detail/0000000071aa258c017259590d8b008d)：每日約 20:00 的 XLS，含市場總量與個股當沖成交股數、買進/賣出金額；T/T+1 可修訂，T+2 才是最終數。頁面價格為內用 NT$1,500/月、外部傳給資訊用戶 NT$3,000/月，提供線上下載或 email；條款限制內外部使用和轉傳，並不保證資料正確或完整，未列出機器 API、排程交付 SLA 或私有 Discord 使用權。連結範例 XLS 未能由本次讀取工具解析，故實際欄位型態、分母與完整性標記仍未知。先把它記為付費授權／交付契約缺口，不購買、不接未授權路徑；若日後 entitlement 與自動交付確認，應留存 T/T+1/T+2 vintages 並把它作量能背景，以成熟成本後 outcomes 再決定是否能投方向票。
- 官方 TPEx OAS [11627](https://data.gov.tw/dataset/11627) 每日提供上櫃市場當沖彙總，不是個股列；維持 freshness gate 並只作市場背景。

### 15:49 盤中通知欄位自然驗證

10/05 15:45–15:47 台北自然盤中 run `scheduled__2026-10-05T07:30:00+00:00` 回傳 10/10 成功但 `data_review_required`。下游 XCom 已包含 `candidate_batches_sent=0`、`source_warning_status=unchanged`、`source_warning_receipt_confirmed=false`；market run 候選及 notified 均為 0，該 run 的通知 ledger 無列。這是欄位語意的 runtime 證據，不是全市場無訊號判定。20:00 after-close 批次尚待自然執行。

## 2026-10-05 15:39 自然批次進度

- Daily source process 最新自然批次 23/23 source keys 回報、無 missing；audit 為 `data_review_required`。財報 effective date 仍是 2026-06-30、月營收 2026-08-01；TWSE/TPEx 當沖資格有效日 2026-10-05，未匹配主檔代碼保留 unknown。來源逐筆列數和 coverage 見品質追蹤文件的 15:39 checkpoint。
- Intraday 最新自然批次 10/10 成功但 `data_review_required`；其 company events 最新有效日 10/04，月營收有效日 08/01，TWSE 普通股覆蓋 100%、TPEx 99.89%（1 未匹配）。它是事件增量流程，不是全市場行情訊號掃描。
- 最新新聞排程 12:30 台北 logical run 實際 14:30–14:39 收集；12 feeds、1,159 accepted。精確時間 48 小時窗內 956 列；日期精度或 cutoff 日時分未知 24 列不計作小時級新鮮訊號。
- 最新完整 daily scan 尚為 10/02；成熟 outcomes 僅 9/24 H1。after-close 10/05 20:00 台北批次仍待自然排程，不能用目前 registry 數量推定當日完整。

## 歷史快照：2026-10-05 16:51 registry 有 28 個 approved/enabled keys

這是 16:51 時點的歷史 catalog 快照，不含之後加入的兩個董監持股不足來源。20:23 時 scheduler 目前 catalog/程式定義為 30 keys；10/05 20:00 自然 after-close 實際回報 28 keys，缺新來源，見本文件開頭的 20:00 自然批次記錄。

| 類別 | source keys | 可用語意與限制 |
|---|---|---|
| 個股行情與估值（4） | `twse_equity_quotes_daily`, `tpex_equity_quotes_daily`, `twse_valuation_metrics_daily`, `tpex_valuation_metrics_daily` | 日行情供價格、量能及市場廣度；本益比、殖利率、股價淨值比只作估值背景，不單獨計方向。TPEx 行情最新失敗 run 曾遇傳輸截斷；現行 adapter 已有 byte-range 完整性驗證，等今日自然 run 實證。 |
| 個股法人與持股（5；16:51 當時） | `tpex_institutional_flow_daily`, `tpex_foreign_investor_holdings_daily`, `tdcc_shareholder_distribution_weekly`, `twse_major_shareholders_daily`, `tpex_major_shareholders_daily` | 上櫃個股法人流及外資持股比例；TDCC 為全市場每週分級快照；上市/上櫃大股東只列持股超過 10% 名稱。都保留脈絡，不把持股當日買賣流；法人流需先驗 PIT、覆蓋和多日成熟結果。上市個股法人批次來源仍缺。 |
| 槓桿、借券與可借券量（4） | `twse_margin_balance_daily`, `tpex_margin_balance_daily`, `tpex_securities_borrowing_daily`, `twse_tpex_sbl_available_daily` | 融資融券、借券餘額/交易與可借券賣出供給，作槓桿及供需風險脈絡，不單獨作多空票。 |
| 注意與處置（4） | `twse_trading_attention_daily`, `tpex_trading_attention_daily`, `twse_trading_disposition_daily`, `tpex_trading_disposition_daily` | 風險、流動性及交易限制；處置或注意標記不等於方向訊號。 |
| 公司行動與財測（3） | `twse_exright_preannouncement_daily`, `tpex_exright_preannouncement_daily`, `twse_quarterly_forecast_attainment` | 除權息用於事件與 outcome 視窗調整；上市財測 API 先保存原始欄位，實績/預測期間配對確認前不計達成率、不通知、不計方向。 |
| 指數、總報酬與市場部位（8） | `twse_market_index_daily`, `tpex_market_index_daily`, `twse_market_total_return_index_daily`, `tpex_market_total_return_index_daily`, `taifex_index_futures_positions_daily`, `taifex_index_options_positions_daily`, `taifex_futures_large_trader_positions_daily`, `tpex_daytrade_market_summary_daily` | 指數用作所屬市場基準；TAIFEX 法人/大額交易人資料是市場彙總背景，不解讀成個股訊號。三個 TAIFEX feed 與 TPEx 當沖彙總的 effective-date audit 均以 5 日曆日作 report-only freshness 門檻；這是警示寬限值，不是交易日計數或「資料新鮮」證明。signal context 仍只接受與 scan date 相同的資料日。TPEx OAS 僅上櫃市場彙總，不得當成個股或上市當沖訊號。 |

### 收盤來源授權和完整度缺口

- TPEx dataset [11856](https://data.gov.tw/dataset/11856) 是每日、免費、政府開放授權的上櫃個股三大法人明細；本專案已登記官方 TPEx OpenAPI。今日 after-close 自然 run 尚未到期，所以最新有效日、實收筆數及覆蓋待驗。
- 上市個股法人建議頁 [137032](https://data.gov.tw/suggests/137032) 不是可抓 dataset/API。當日重新解析 TWSE 官方 Swagger 的 143 個 paths，法人相關僅 `/fund/MI_QFIIS_cat`、`/fund/MI_QFIIS_sort_20`，沒有 T86 個股批次路徑。不得以 T86 網站逐股查詢補缺。
- 2026-10-02 最近一次自然收盤 run 因 `tpex_equity_quotes_daily` 連線截斷而失敗。現行程式以驗證 Content-Range、ETag/Last-Modified 的 64 KiB byte-range 重取處理；本機、scheduler、worker 版本一致，仍須 10/05 自然 run 回執證明修正有效。

## 盤中事件來源：10 個預期 source keys

最新自然 run `source_extension_collect_intraday / scheduled__2026-10-05T08:15:00+00:00`（16:15 logical；16:30:00–16:30:36 實際）的預期、registry、runtime、reported 數均為 10；parent/task maps 成功，但 audit 是 `data_review_required`。

| 類別 | source keys | 2026-10-05 最新批次觀察 |
|---|---|---|
| 公司重大訊息（2） | `twse_company_events`, `tpex_company_events` | raw/accepted 分別 3/3 與 2/2；1 筆 TWSE 代碼未配普通股主檔，保留 unknown。有效日 10/04。 |
| 內部人申報轉讓（4） | `twse_insider_transfer_planned`, `twse_insider_transfer_untransferred`, `tpex_insider_transfer_planned`, `tpex_insider_transfer_untransferred` | planned 各 1/1；untransferred 各 1 raw、0 accepted、1 quarantined 的空白 sentinel，不代表零事件。planned 有效日 10/02。 |
| 月營收（2） | `twse_monthly_revenue`, `tpex_monthly_revenue` | TWSE 1,086 accepted，普通股 1,054/1,054（100%）；TPEx 892 accepted，891/892（99.89%），1 筆未配對保留 unknown。兩者 effective date 8/01；接受列 PIT versioning 100%。 |
| 財測差異（2） | `twse_quarterly_forecast_variance`, `tpex_quarterly_forecast_variance_events` | TWSE 7 accepted、5 筆上市普通股匹配，資料有效日 6/30；TPEx 只有 snapshot metadata，沒有公司事件列。 |

這個盤中 child 是公告增量掃描，不是全市場日線掃描：market universe、price、flow 都是 0，候選 0，通知 sent/failed 0；不可由此推論沒有市場訊號。TPEx OpenAPI 某些完整 byte-range 回應 HTTP 206 是預期傳輸方式，須看完整性與 row audit，不單看 status code 判定截斷。

## 新聞背景來源：12 feeds

自然收集的 feeds：ETtoday、TVBS、iThome、新頭殼、The News Lens、三立、中央社、中時、公視、聯合報、自由時報、鉅亨網。最新批次 logical 16:30／實際 16:30:02–16:39:13：1,247 URLs、1,246 parsed、1,216 有效時間、1,197 accepted；fetch/parse reject 1、invalid-time quarantine 30、content/duplicate rejects 19，reconciliation 與 post-time filter 全過。Freshness cutoff 為 10/03 16:39:08 台北。

SETN 30 列都因 JSON-LD offset-zero 發稿時間欠缺相符的列表相對時間證據而 quarantine。抽查該自然批次 8 篇原頁與保留的列表標籤，將 JSON-LD 當台北 wall time 時都比標籤年齡多約 3.47 分鐘；新聞 parser 只把分鐘標籤容許上限由 +3 放到 +5，仍要求列表佐證，且保留 UTC-offset 衝突不確定性。修正尚未由下一自然批次驗證。

（14:30 checkpoint）依來源時間精度，48 小時內精確時間 956、日期精度在 cutoff 日後 19、cutoff 日時刻未定 5、舊 179、未來 0。日期精度記錄不算精確 48 小時；自由時報空白內文 19/95（20%），先追蹤 parser/來源品質，不因單批改權重。新聞作時間受限背景，未經 outcome 驗證不加方向票。

## 下游訊號、結果和通知證據

- 最近完整 daily scan 仍是 2026-10-02：universe 1,946、price 1,938、flow 1,891、revenue 1,945、financial 1,902、borrowing 1,795、insider 1,944、dividend 1,053、candidate 50、notified 0。10/05 最新 event-only child 不能替代 daily scan。
- `market_signal_outcomes` 目前只有 9/24 legacy H1：signal-close 81 rows、80 可計方向、50 hits；next-close 81、80、37。無 H5/H10，成熟度及 benchmark 日期對齊仍是驗收閘門；不得據此調門檻。
- 10/04 `source_extension_collect_weekend` 自然 run 的 event-only 掃描有 5 個候選：3 個 `company_announcement`、2 個 `insider_transfer_planned`，方向都 unknown；`notified_count=5`。通知 ledger 有 6 rows，但只有 2 個 distinct platform receipts（HTTP 200）：一個來源品質提醒、一個包含 5 個事件候選的批次卡。這證明事件卡送達，不證明方向預測有效。
- 10/05 週末後 interval 的 child（source logical ID `scheduled__2026-10-02T14:45:00+00:00`）候選為 0，另有一筆 `data_review_required` 來源品質提醒 receipt（HTTP 200）；最新 10/05 14:45 logical child 的候選卡 `sent=0`。舊 XCom `sent` 只計候選卡批次，不計來源品質提醒，單看 `sent=0` 會漏掉 warning receipt。
- 已窄幅補上 intraday XCom 欄位 `candidate_batches_sent`、`source_warning_status`、`source_warning_receipt_confirmed`；不改通知發送、去重或 cursor 行為。scheduler AST parse 通過，workspace/scheduler/worker 同檔 SHA-256 一致；下一個自然 run 才能驗證新欄位。
- 截至這次查核，沒有已驗證的多空方向卡片回執；近期實際送達的是官方公告/申報事件卡與來源品質提醒，須和交易方向通知分開統計。

## 下一次自然驗收

待 2026-10-06 台北 20:00 下一次自然 after-close，20:30 按實際 run ID 核 30 個 sources、逐源有效日/raw/accepted/quarantine/duplicate、上市櫃匹配、PIT、TPEx quote byte-range 完整性、daily scan、成熟 outcomes 與實際通知 receipt；特別驗收兩個新 statutory-shortfall sources 是否真正在 catalog discovery、自然 ingest 和 source audit 出現。不得手動觸發或重跑 DAG。
