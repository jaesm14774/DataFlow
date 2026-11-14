# 台股訊號盤點、研究品質與改善順序

> 2026-11-03 20:32 自然驗收：[報告](stock_signal_natural_validation_20261103.md)與[唯讀快照](stock_signal_inventory_snapshot_20261103_2033.json)確認前一輪 TAIFEX index-options JSON/CSV 修正已自然持久化：registry/runtime 均 `taifex_csv`、contract drift 0、6/6 accepted、有效日 11/02、PIT 6/6，6 筆都有 8 種 canonical fields。11/03 daily universe 1,955、source partial、12 個門檻候選（偏空 11／偏多 1），family triggers 法人 721、上市分點 53、價格 487；1,929/1,941 assessed securities 未達三家族。上市法人自然 PIT same-day 仍 0，仍只代表 collection-time usable；上櫃分點仍缺。摘要的 3 張候選卡 receipt 和明示未逐張列出的 9 檔、5 筆 ledger receipts 分開記錄；成熟 1w/1m 仍各有 125 pending，policy 不自動調整。TWSE margin 全隔離、注意股空回應與老舊 shortfall 日期是具體資料缺口，細節見 source inventory。本輪沒有新的可支持門檻改動之證據。

> 2026-11-02 20:40 自然資料刷新與 TAIFEX 選擇權契約修正：[驗證報告](stock_signal_natural_validation_20261102.md)和[唯讀快照](stock_signal_inventory_snapshot_20261102_2040.json)確認 after-close 只有 `taifex_index_options_positions_daily` 失敗，根因為官方端點回 CSV 而來源錯配 `taifex_json`。已改 `taifex_csv`、嚴格驗證 16 欄表頭並映射官方中文欄名；focused suite 19 passed、唯讀官方 request 可解析 6 列，但 DB registry 更新與自然持久化仍待下一個排程驗收。市場彙總不投個股方向票；同 cutoff 日線 1,931 個評估標的均未達三家族、candidate 0，門檻不變。八個季 EPS feed 最新 natural daily run 的資料所屬期間為 2026-09-30；已收 84 個列均有 basic EPS，current-master 對碼 46/1,058 listed、38/897 OTC，其餘空批次與未覆蓋不得當零活動。完整逐來源數字及其他自然 run 見 source inventory。

> **2026-10-16 使用者最新政策，優先於下方歷史限制：** 既有上市／上櫃法人、上市券商及既有行情資料應讀取並依合理條件形成訊號。TPEx 券商分點未收集只略過該項；不得因 legacy 名稱或成效未驗證而全域排除。已收、可判讀、達條件、缺列與成效分開表達。每次排程需檢驗並改善判讀合理性，不止盤點。新外部訂閱與使用條件仍另行確認，不能把這項缺口誤稱既有資料都未接通。[本次實作與驗證](stock_signal_order_flow_repair_20261016.md)：同 cutoff 恢復 1,820 檔法人判讀、20 合併候選；59 tests passed。以下關閉法人方向的舊敘述僅保留為歷史，已被本段取代。

> 2026-10-16 06:53 自然驗收更新：[快照](stock_signal_inventory_snapshot_20261016_065331.json) 與 [新聞落庫證據](stock_signal_news_storage_20261016.json) 確認 DELETE/單一 active run 修正後兩個自然新聞批次成功；最新 1,036 篇逐來源與實際 DB 對平。唯讀腳本新增最後來源嘗試與 Airflow 實際/下次可建時間，釐清 monthly logical 8/24 的 run 實於 9/25 執行、下一次 10/25 04:00 TPE 才到期；source-row lineage 仍 unknown。TPEx 五日完整面板已累積 685 檔，方向票仍待多日期成效驗證。上市法人商品新增 18:00/20:00 兩種 release 的契約紀錄，entitlement/機器交付/使用權未核，未接入。詳見 [source inventory](stock_signal_source_inventory.md) 與來源台帳；下方較早數字保留為歷史紀錄。

> 2026-10-12 20:38 自然資料刷新與方向家族政策修正：[唯讀快照](stock_signal_inventory_snapshot_20261012_2038.json) 有 67 registry keys；daily source process 23/23、after-close 32/32、intraday 10/10 均無 missing key／contract drift；monthly lineage 仍 unknown。After-close run 的 logical id 是 10/09 20:00 TPE，實際於 10/12 20:00–20:32 執行。TAIFEX large-trader 這次 HTTP 200 後解析成功、1,386/1,386 accepted，但有效日仍是 10/08，故只代表解碼失敗可恢復，未恢復 10/12 新鮮度。TWSE margin 1,040 raw、0 accepted、1,040 quarantine、無有效日，確認不再合成本機日期。10/12 日線掃描在舊程式下留下 1,828 flow rows、16 候選；上市 readiness 顯示 0 natural PIT，卻把 `twse_t86_stored` legacy rows 設為 `direction_enabled`，16 個候選均含法人方向。已修正 `equity_order_flow.py`：不再讀 legacy 法人表，上市保持未授權／unknown；TPEx 官方 flow 保留 coverage/PIT/影子診斷，但法人方向家族維持關閉，直到多日期成熟 outcomes 驗證。這次自然掃描早於修正，不回寫其候選或通知；下一次自然掃描才可驗證修正。針對性測試 32 passed，py_compile 成功、Airflow DagBag import errors `[]`、worker 與本機程式雜湊相同。完整批次、候選、通知回執與新聞 freshness 見 [source inventory](stock_signal_source_inventory.md)。

> 2026-10-09 20:33 自然資料刷新：[唯讀快照](stock_signal_inventory_snapshot_20261009_2033.json) 有 67 registry keys；daily 23/23、intraday 10/10 完成；after-close 32/32 回報但 TAIFEX large-trader 再次 HTTP 200 `JSONDecodeError`，verify 失敗。下游 daily child 已觸發但因 10/09 `taiwan_market_closed` 而 skipped，不能視為訊號掃描。新增的 8 個 EPS feed 仍是 1,976 accepted、0 EPS null/quarantine/duplicate、版本 100%，只到 2026Q2。CBC FX batch 有 3,660 accepted 但 latest effective date 僅 09/30、`market_signals.py` 尚無 reader；TWSE margin 顯示的 10/09 有效日後確認是 adapter 合成的執行日，非來源日期。完整逐來源證據見 [source inventory](stock_signal_source_inventory.md)。本輪沒有新券商原文或批次授權契約，研究來源 JSON 未新增。

> 2026-10-09 23:26 根因追查補記：TAIFEX 10/07–09 自然執行均在 JSON char 0 解碼失敗；稍晚同一官方 feed 可正常解析 1,386 rows/有效日 10/08，較符合排程窗口的上游空或非 JSON 回應，parser schema mismatch 尚無證據。TWSE margin 舊 adapter 對無日期 OpenAPI 回應合成本機日 10/09；已移除合成日期並保留核准 OpenAPI，缺日期列隔離。官方有日期報表頁可作根因交叉核對，但依 TWSE 條款未接入自動抓取；signal reader 會排除未被行情 session 證實的日期。這些是 code/live endpoint 驗證，不是下一次自然 source_run；舊資料未回寫，來源缺口與限制見 [source inventory](stock_signal_source_inventory.md)。

> 2026-10-08 更新：可得的上市法人、上市分點及上櫃法人已啟用；缺上櫃分點則略過。全指標盤點、資料群計票、交易日／公司行動修正、營益率同比及兩個免費交易限制來源，以 [現行策略稽核](stock_signal_strategy_audit_20261008.md) 為準。下方保留 10/07 當時紀錄，不能沿用舊方向政策、舊權重或舊候選數。

> 2026-10-08 20:33 自然資料回看：[唯讀快照](stock_signal_inventory_snapshot_20261008_2032.json) 的 registry 為 67 keys；daily process 23/23 success，EPS 八 feeds 有 1,976 accepted、0 basic EPS null、有效日 2026-06-30、PIT 100%。After-close 32/32 有回報但 31 success/1 failed，TAIFEX large-trader 仍 HTTP 200 + `JSONDecodeError`；parent 還在等既有 order-flow collection，新的 daily signal child 尚未執行。當日 TPEx flow 892 accepted、770/893 對碼、122 off-master；不把它當已投票。最新 persisted daily scan 仍 as-of 10/07、0 candidates、flow 0 且 readiness 欄位未留存；自然驗收須等本次 parent 放行後再查。

2026-10-07，Asia/Taipei。最新唯讀快照擷取於 22:40；新聞細目沿用當日 20:30 已核自然批次紀錄，並非在本輪重新查核每篇原頁。目標是可持續、能追溯的全市場訊號與清楚通知；研究品質和交易有效性分別驗證。

## 2026-10-07：免費官方法人資料的接線與資格修正

使用者確認目前沒有上市法人資料訂閱，先完成免費官方來源。原有 `flows_by_code` 刻意保持空集合；因此原本 `flow_count=0` 是「沒有可投方向票的法人列」，不是未收法人資料，也不是市場零買賣超。TWSE 尚缺已授權的上市全市場批次來源；[TWSE 使用條款](https://www.twse.com.tw/zh/terms/use.html) 對自動下載方式有規範。免費的 [TPEx 資料集 11856](https://data.gov.tw/dataset/11856) 已自然收集，這次接到日線的來源資格讀取與通知說明，仍保持背景與驗證用途。

以 10/07 20:02:39 台北的自然日線掃描時間作 cutoff，重新執行純讀取的日線分析；未修改原快照、寫入業務 DB 或發送通知：

| 檢查 | 上市 | 上櫃 |
|---|---|---|
| 普通股主檔 | 1,054 | 893 |
| 法人來源 | 未取得已授權批次；未知 | TPEx 官方免費批次 |
| 同日原始已接受列／對主檔配對 | 不適用 | 901／782，另 119 未配上櫃普通股主檔 |
| 同日可配對／自然 PIT 證據 | 未知 | 782/893（87.57%）；782 列均具成功自然收集與版本證據 |
| 當日有成交但未見同日法人列 | 來源未接入，數量不可判讀 | 91 檔，維持未知 |
| 已存法人有效日 | 未知 | 10/05、10/06、10/07 |
| 最近五個市場交易日 | — | 10/01、10/02、10/05、10/06、10/07；依既有 holiday.taiwan |
| 五日法人與有效成交完整對齊 | 未知 | 0 檔，缺前兩日自然法人資料 |
| 方向投票 | 未啟用 | 未啟用；歷史與多日期成熟成效驗證尚不足 |

新增 `source_extension/institutional_flow.py` 將可得性、單位／加總核對、自然 PIT、五交易日視窗與方向啟用分開。查詢只取 cutoff 前的最新版本；較新的隔離版本不能讓舊好值復活。缺版本的 current-row fallback 只計可得資料，不算自然 PIT。完整五日面板需每一日有官方法人列及同日正值收盤／成交量；缺日、重複、未知單位、未成功收集、手動 run、未對碼與已知零成交均不能冒充完整面板。外資自營商不重複加進三大法人合計。

日線從此會保存各市場 `flow_readiness_json`（新增 schema migration 16），包含 cutoff、預期日期、阻塞原因及符合資格的五日觀察值，供後續按當時可得資料驗證；這些觀察不增加候選分數或投票。`flow_count` 繼續代表可投方向票的列數；`traded_without_flow_count` 的新快照改指已接入來源的同日缺列，上市來源未接入時為 NULL。歷史快照沒有資格分項時明示未留存，不能把原本的 0 或缺列計數重新解釋成官方已收資料。

通知將法人來源資格另列一張背景卡，顯示已收／PIT／五日完整／方向未啟用；零候選仍明示資料不足，不能推成沒有訊號或交易機會。現有盤中卡、三家族門檻、方向權重與通知去重規則保留。

驗證：8 個新增回歸測試加既有來源測試合計 **19 passed**；實際日線純讀取重算候選仍為 0；記憶體內保存／讀取往返一致；scheduler DagBag 可載入 `market_signal_notify`，0 import errors。完整背景資料的通知預覽為兩批，實際序列化 **9,377／1,095 bytes**，均低於 10,000 bytes；這是未發送的預覽，不是平台 receipt。來源查詢及回歸證據留在 [本輪唯讀證據](../tmp/institutional_flow_readiness_20261007.json) 和 [唯讀檢查程式](../tmp/flow_preflight.py)。主檔為本輪讀取的目前主檔，尚非歷史主檔版本；未宣稱存續偏差、公司行動或交易成本已完成驗證。

新 migration、資格快照持久化及新通知實收仍待自然排程驗收。下一個日線收集時段為 **10/08 20:00 台北**：核 source-run 成功、有效日、對碼、PIT／五日面板、保存欄位及通知 receipt。即使累積五日完整資料，也不會自動啟用方向家族；須完成多日期成效驗證。全市場完整方向缺口仍存在，不能將這次接線稱為交易機會已可判定。

## 已完成的盤點

資料庫有 **65 個 approved/enabled 結構化來源**：盤中 10、每日 23、收盤後 30、月頻 2。來源逐列有效日、列數、對碼和 PIT 見 [完整來源盤點](stock_signal_source_inventory.md)；本輪重跑既有唯讀 audit，證據保存於 [22:40 快照](stock_signal_inventory_snapshot_20261007_2245.json)。同時讀取的資料可能受自然排程更新，快照不是所有表的同一交易時點。

| 類別 | 已收集／接入 | 現況與限制 |
|---|---|---|
| 行情與基準 | TWSE/TPEx 日行情、指數、總報酬指數、估值 | 10/07 上市可用價格 1,047/1,054，上櫃 873/893；缺列與 no-trade 分開。指數同日有效不保證完整歷史視窗 |
| 營運與財報 | 月營收、季損益/EPS、資產負債、獲利率、財測達成／差異 | 八個季 EPS feeds 自然收集已存在；僅有可得 2026Q2，不能自造同期比較。月營收批次目前有效月份仍為八月 |
| 股東與內部人 | TDCC 週股權分散、大股東披露、董監持股、轉讓申報、持股不足名單 | 頻率、披露對象與普通股主檔不同；申報不等於完成交易；月頻兩來源缺可歸因的 run lineage，audit=unknown |
| 籌碼／交易風險 | TPEx 法人流、外資持有、融資券、借券、注意／處置、除權息、股利 | 官方 TPEx 法人資料已有 10/05–10/07，但仍未作方向家族；稀疏名單不是全市場覆蓋，轉讓 sentinel 不是零活動 |
| 市場環境 | CBC 匯率、TAIFEX 期貨/選擇權法人部位、大額交易人、TPEx 當沖彙總 | 只提供背景。2026-10-09 自然 CBC batch 3,660 accepted、latest effective date 09/30，且 `market_signals.py` 無 reader；未確認 freshness/PIT 前不當當前匯率訊號。TAIFEX large-trader 自然 run 仍失敗；TPEx day-trade 仍是市場彙總 |
| 新聞 | 中央社、鉅亨、聯合、自由、中時、ETtoday、TVBS、公視、新頭殼、三立、iThome、The News Lens | 20:30 紀錄為 1,369 篇自然批次落庫、9 feeds complete/3 partial。1,219 篇精確時間落在其 48h 窗；其餘日期精度／較舊列不可混算 |
| 券商研究 | 本輪查核五家官方入口，實際抽查凱基 PDF 與永豐歷史 PDF；另核 TEJ 摘要/API 文件 | 保存自有短評和來源欄位於 [研究來源台帳](stock_signal_research_sources.json)。尚無接入可用的券商共識方向 feed |

## 最值得先改善的三件事

1. **說清楚資料不足。** 最新日線 `ready/partial`：普通股 1,947、行情 1,920、方向法人 flow 0、候選 0。`market_signals.py` 明確不讓 legacy 網站法人表投方向票；新的 TPEx 官方 feed 尚待 PIT/成效驗證。因此不能稱「沒有訊號」，也不能把 flow 0 解讀為沒有法人買賣。自然品質通知有 HTTP 200 和 receipt，這不等於已送出交易方向卡。
2. **保留失敗證據。** 收盤後 30/30 source keys 有回報，但 TAIFEX 大額交易人 map 24 兩次 JSONDecodeError，整批為 partial。generic JSON fetch 原先丟失已知 HTTP status；本輪在解析失敗例外上保留 `http_status`，沿用 registry 的失敗寫入與通知路徑。無效 JSON 仍失敗、不保存 body、不補零。來源是否恢復仍須下一次自然 run 驗證。
3. **驗證預測能力之前先累積可比樣本。** 現有 settled outcomes 只有兩個信號日期的 H1 群組；9/24 與 10/02 用途／價格來源不同。9/24 next-close 81 列平均超額約 +0.987pp，10/02 next-close 44 列約 -1.119pp；沒有成熟 H5/H10。不能據此選門檻、校準勝率，或將 entry basis/horizon 重複列視為獨立信號。

## 券商報告：具體評估與可借用的方法

判斷單份原文的論證，不用券商品牌替它背書。不推測作者是不是「為出而出」；可以指出證據薄弱、假設不透明、過時或宣傳性內容。

| 來源 | 已看見的內容 | 研究用途／判斷 | 自動接入狀態 |
|---|---|---|---|
| [凱基官方週報 PDF](https://www.kgi.com.tw/zh-tw/-/media/files/kgis/strategy/ips/weekly.pdf) | 9/24 報告；晶片出貨預測分列歷史與預估期，也列圖表來源 | 有可檢查的產業假說，但出貨成長尚未橋接個別公司 EPS、估值和反證。本次 [入口摘要](https://www.kgi.com.tw/zh-tw/product-market/wealth-enlightened/insights) 題目與實際 PDF 不同；不能把固定 URL 當最新報告 | 背景／方法案例；日期、版本與使用權須核，未接方向 feed |
| [永豐歷史個股 PDF](https://www.sinotrade.com.tw/UploadFiles/2023/202309/65251697-1ac4-4592-a007-13c1ab793100.PDF) | 2023/9/15 保瑞報告，具作者、業務結構、業外項目與 EPS/估值推理 | 成長與評價分開，並說明一次性項目對盈餘的影響；值得借用判讀方法。**歷史樣本，不能當 2026 訊號**；本輪未重建其預測後績效 | 只收方法；目前會員研究及批次用途未確認 |
| [富邦研究入口](https://fubonresearch.fubon.com/Research/Home/) | 題目／分類可見，全文提示登入 | 原文未核；無法評斷 EPS 模型、估值或風險完整性 | 缺 entitlement／bulk schema |
| [國泰官方列表](https://www.cathaysec.com.tw/exchange_new.asp) | 可核 9/08 投資觀點條目；detail 在本 reader 重導首頁 | 只能登記來源；不能從標題推論全文品質 | 缺原文、機器交付、vintage／用途 |
| [國泰投顧全球品質股評論（公開原文）](https://djinfo.cathaysec.com.tw/w/wh/wh01a_%7BE0F8302C-86C7-42F7-80E2-382E17E72B36%7D.djhtm) | 9/29 發布、作者未列；Bloomberg 觀察資料至 9/16，品質股定義列 ROE、應計比率與槓桿 | 可借用作盈餘品質假說；文章是全球／基金評論，沒有台股公司 EPS 橋接、估值與可檢驗個股條件，不能作方向訊號 | 只有公開單篇，未證明全市場批次、版本、授權與回測/外顯權利 |
| [元大台股入口](https://www.yuanta.com.tw/eYuanta/securities/Stock) | 市場／新聞／分析類別 | 不等同取得分析師原始研究，避免重複既有官方資料 | 原文與批次契約未確認 |
| [TEJ EPS 修正摘要](https://eshop.tej.com.tw/E-Shop/journal_detail?journal_id=2025090511) | 作者／日期可核，摘要提出 EPS 修正與分析師分歧分組；全文付費 | 有新訊號假說；完整樣本、成本、結果、方法未核。不能以摘要宣稱有效 | [REST 文件](https://api.tej.com.tw/document_rest.html) 只證明一般 API 能力，版本為 2020；共識特定表與帳號權利未確認 |

上述評估是自有研究短評，未保存／轉發受限報告全文。銀行／期貨／券商新聞入口也要辨識來源組織，不能混稱為券商個股報告。

## 高品質訊號准入與判讀

| 關卡 | 必要證據 | 未達標的處理 |
|---|---|---|
| 可追溯 | 原始發布者、URL、實際報告/資料日期、作者/團隊、可得時間與版本 | metadata-only／原文未核，不能包裝為完整研究 |
| 正確資料 | 對碼、單位、合併/個體、累計/單季、同一估計期間、接受/隔離/缺列 | unknown 保留；不得補零或混算 |
| 有因果鏈 | 催化事件 → 需求/出貨 → 公司營收 → 毛利/費用 → EPS → 估值 | 只有題材熱度者為背景假說 |
| 能被推翻 | 反方風險、失效條件、下一個可核日期、與前次假設的差異 | 不只收支持訊息；缺風險時降低論證品質 |
| 可排程 | 確認授權、bulk API/index/schema、revision vintage、上市櫃覆蓋、留存/回測/顯示用途 | 留在 research/gap；公開可閱讀不等於可批次儲存或交易投票 |
| 可驗證交易用途 | 足夠獨立日期、預先固定比較、樣本外/跨期間、entry basis、成本、公司行動與市場基準 | 先作 context；品質分數不等於報酬、勝率或交易建議 |

## 優先收集或延伸的 backlog

以下都是研究優先級，不是已證明的有效交易因子。

| 優先級／項目 | 可以從現有什麼延伸 | 下一個可驗收成果／缺少什麼 |
|---|---|---|
| P0 來源診斷與月頻可歸因性 | source_runs、自然 audit、HTTP receipt | 下一次自然 TAIFEX 失敗需顯示實際 HTTP；成功需有效日及 rows。月頻需 source_dag_id/最後嘗試可追溯，不能用重疊 run_id 冒認 |
| P1 官方法人流可用性 | TPEx 已有三個有效日；legacy 只可作有限研究 | 找上市授權全市場批次，TPEx 先補自然歷史、近五個市場交易日/PIT/單位/缺列與成交對齊；未驗證前不恢復方向權重 |
| P1 營收成長品質 | 月營收＋獲利率/EPS＋估值 | 區分併購/低基期/匯率/真實需求；同月 PIT 可比、季毛利/EPS佐證與估值反證。量、價和財報可比視窗不足時保留背景 |
| P1 財報現金轉換／盈餘品質 | 季損益＋資產負債已有 | 缺授權 bulk 現金流歷史、原始發布/修訂時間；候選 CFO/淨利、應收/存貨對營收變化、一次性業外項目，金融業另訂口徑 |
| P1 券商 EPS 共識修正／分歧 | 本輪方法台帳＋官方供應商 API 文件 | 缺已授權共識表；固定同一財年／forecast horizon，保存每位分析師原估計/修正/可得時間，計 revision breadth/magnitude 與分歧。EPS 近零/負值時變異係數不穩，不能直接套用 |
| P2 TDCC 持股集中變化 | 已有全市場週快照 | 同級距跨週、股數/人數/比例一起核；辨識除權息、減資及新增上市；集保戶級距不可直接稱法人身分或買進行為。官方定義見 [TDCC](https://original-www.tdcc.com.tw/portal/zh/smWeb/qryStock) |
| P2 新聞事件交叉證據 | 既有 scan-before-ingest cutoff、公司 mention、同標題去重 | 新聞確實指同一事件、原文日期與公告對齊、轉載來源去重；分開官方事實／報導／分析師預測／未知。公司 mention 本身不加多空分數 |
| P2 產業鏈／總經壓力 | TAIFEX／CBC、財報與新聞 | CBC FX 先補有效日 freshness gate 與 as-of reader；自然 source success 不能當新鮮。再將需求假說與可核訂單/CapEx/出貨/毛利對照；衍生合約按契約/到期月分析，不將不同交易人排名相減成方向 |

## 通知與持續執行

沿用既有 heartbeat `automation`「台股自動訊號與來源品質監控」，平日台北 **20:30**；本輪已更新 prompt，加入公開研究原文抽查、品質台帳與本文件，保留全市場批次為重心。沒有新可行動資訊時安靜，有來源失敗/恢復、可審查改善、新高品質來源或成熟驗證才通知；不另建每日重複盤點。

通知至少交代：股票代號＋名稱、訊號/事件與觸發證據、資料有效日/可得時間、支持與反證、未確認條件、下一次可核事件；資料品質通知要與候選卡分開。不能用候選數或來源品牌代替品質。

讀取優先：本文件（方法／優先級）→ source inventory/watchlist（最新狀態）→ 最新快照／原始資料（當期證據）。記憶保存持續偏好、資料邊界與研究入口，舊 snapshot 數字必須刷新。使用者本次已授權寫 memory ad-hoc note；後續只在新的可重用事實/決策時新增小型 note。

## 本輪驗證與限制

- 新增唯讀 snapshot script 已在現有 Airflow worker 執行，包含 65 個 registry rows、四個自然來源 audit、最新 daily/intraday、market coverage、outcomes 和 receipt metadata。
- HTTP 解析失敗保留狀態與原有五個 normalization/contract tests：`python -m pytest -q tests/test_source_http_quality.py tests/test_source_extension.py`，**11 passed**。測試用假的 response，無網路、無 DB、無 Discord。
- worker 沒有 pytest，測試改用已有的 Windows Python/pytest；未為生產容器安裝套件。容器內 snapshot smoke run 成功。解析診斷修正仍待自然收集驗收；preflight 不寫業務資料。
- 本輪未手動觸發/重跑 DAG、交易、人工發 Discord、調整分數或啟用未驗證方向來源；既有 dirty-worktree 變更保留。
