# 台股訊號一年追蹤與回測維護設計

研究日期：2026-10-22（Asia/Taipei）

## 研究問題與章節大綱

1. **現在的資料能回答什麼**：區分訊號事實、通知送達、股價反應和實際交易損益。根據目前程式、TWSE/TPEx 資料欄位和市場總報酬指數定義。[來源 A、B、C、D]
2. **每則訊號的一年追蹤**：固定訊號日資訊，設定 1 週、1 個月、3 個月、6 個月、1 年的到期日，再用官方交易日和行情結算。[來源 C、D、E、F]
3. **怎樣比較訊號成效**：原始股價方向、訊號方向、同時期市場總報酬、訊號日等權結果、成交可行性和成本敏感度分開呈現。[來源 A、B、C、G、H]
4. **如何防止回測騙過自己**：點時資料、下市股票、公司行動、多重嘗試、重疊持有期、保留樣本外期間和版本紀錄。[來源 I–P]
5. **自動年度檢討與迭代門檻**：每次來源流程更新到期追蹤；年度 cohort 完熟後留存回顧；只提出有證據支持的規則候選，不自動放寬通知條件。[來源 G、I–P]

## 研究摘要

- 回測中的上漲比例不等於預測有效：偏空訊號要看方向調整報酬；選股是否有增益還要相對市場基準；「通知真的送出」要從通知 ledger 判定。三者各自回答不同問題。
- TWSE 與 TPEx 的官方資料可提供個股開、高、低、收價；目前此專案正規化行情僅保存收盤價。因此要開始做可執行的隔日開盤代理，必須先把 OHLC 欄位寫入來源快照。OHLC 仍不能證明實際可成交價格，也不能代替個人費率、稅負、滑價或借券資料。
- TWSE 與 TPEx 都發布考量現金股利的市場總報酬指數。不過本系統目前沒有逐檔總報酬價格，因此主要超額報酬改用上市／上櫃價格指數，和未調整個股價格採同一口徑；個股報酬不冒稱股東總報酬。總報酬指數仍可供投資組合層後續參照。
- 每個訊號時間都要鎖住當時實際已知的資料與政策版本。不得用目前仍上市的股票倒推歷史母體，也不得把之後修訂的財報提早使用。
- 每次更改門檻、特徵、家族組合或報酬定義都是一次模型試驗。應保存試驗紀錄與未參與調整的時間段；在同一批行情上反覆挑「最佳」會造成資料窺探和過度擬合。
- 1 年持有期下相鄰日期的報酬高度重疊。年度報表會公布樣本數和訊號日數、覆蓋率和未結算原因；獨立樣本不足時不提供看似精確的信賴區間，也不把 1 年 cohort 直接當成統計證明。

## 來源清單

以下優先使用原始研究、作者版本、交易所原始規範或官方平台文件。SSRN 個別頁面受限時改讀作者或研究者自行存放的論文版本；動態 API 頁面內容有限，故以官方資料集欄位說明和程式現行 adapter 互相核對。

| ID | 一手資料 | 用途 |
|---|---|---|
| A | [TWSE 發行量加權股價指數編製要點](https://twse-regulation.twse.com.tw/TW/law/DAT0201_print.aspx?FLCODE=FL047579) | 價格指數與含現金股利的總報酬指數定義 |
| B | [TPEx 指數簡介](https://www.tpex.org.tw/zh-tw/indices/stock-index/industrial/intro.html) | 櫃買市場總報酬指數的股利處理 |
| C | [TWSE 個股日成交資訊資料集](https://data.gov.tw/dataset/11549) | 官方個股開、高、低、收價與成交量欄位 |
| D | [TPEx 上櫃股票行情資料集](https://data.gov.tw/dataset/11370) | 官方上櫃 OHLC、成交量與資料更新資訊 |
| E | [TWSE OpenAPI](https://openapi.twse.com.tw/v1/swagger.json) | 本專案所用上市行情與指數 API 合約 |
| F | [TPEx OpenAPI](https://www.tpex.org.tw/openapi/swagger.json) | 本專案所用上櫃行情與指數 API 合約 |
| G | [Campbell、Lo、MacKinlay：事件研究法](https://www.bu.edu/econ/files/2011/01/MacKinlay-1996-Event-Studies-in-Economics-and-Finance.pdf) | 事件日、估計期、事件窗與基準報酬的區分 |
| H | [QuantConnect 回測報告統計項目](https://www.quantconnect.com/docs/v2/cloud-platform/backtesting/report) | 報酬、換手、回撤等績效統計清單 |
| I | [QuantConnect Research Guide：look-ahead 與 survivorship bias](https://www.quantconnect.com/docs/v2/writing-algorithms/key-concepts/research-guide) | 點時特徵與歷史下市母體要求 |
| J | [QuantConnect 美國股票資料正規化說明](https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/requesting-data) | 原始、拆股調整與總報酬序列的差異；僅作方法參考，不代表台股資料來源 |
| K | [QuantConnect 費用模型](https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/transaction-fees/key-concepts) | 費用需要按下單與實際券商模型估算 |
| L | [QuantConnect 滑價模型](https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/slippage/key-concepts) | 價格可成交性受延遲、波動與訂單量影響 |
| M | [QuantConnect Walk Forward Optimization](https://www.quantconnect.com/docs/v2/writing-algorithms/optimization/walk-forward-optimization) | 按時間反覆評估並保留向前驗證流程 |
| N | [White：A Reality Check for Data Snooping](https://users.ssc.wisc.edu/~behansen/718/White2000.pdf) | 重複使用同一批資料選模型會抬高最佳結果 |
| O | [Hansen：A Test for Superior Predictive Ability](https://citeseerx.ist.psu.edu/document?doi=83d10adb98319ab3fcc15dfa44dd3f7ff80e4bda&repid=rep1&type=pdf) | 比較多組候選與基準時的 SPA 檢定 |
| P | [Bailey 等：Probability of Backtest Overfitting](https://carmamaths.org/jon/backtest2.pdf) | CSCV / PBO、多次試驗與單一 holdout 的限制 |
| Q | [Bailey、López de Prado：Deflated Sharpe Ratio](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf) | 多重嘗試、偏態與厚尾對 Sharpe 的影響 |
| R | [Harvey、Liu、Zhu：… and the Cross-Section of Expected Returns](https://www.nber.org/papers/w20592.pdf) | 新發現需提高統計門檻，並記錄多重檢定 |
| S | [Lo：The Statistics of Sharpe Ratios](https://traders.berkeley.edu/papers/The-Statistics-of-Sharpe-Ratios.pdf) | Sharpe 的估計誤差與序列相關，不能任意年化 |

## 從研究到系統規格

### 1. 追蹤範圍

只有日線選股/多空候選進入價格預測評估。盤中公告、資料缺失、來源品質告警屬事件或營運監控，另記送達與處理狀態，不混進股價績效。評估單位是每一個日線 run、股票、政策版本及其家族方向快照；未達整體通知門檻但至少有一個方向家族的股票也要留下，以便比較訊號家族，而非只看已挑出的贏家。

每則方向訊號建立五個日曆到期點：訊號日後 7 天、1 個月、3 個月、6 個月、12 個月。月/年日期使用月底截斷規則；結算日是該市場第一個官方交易日且不早於到期日。訊號後下一個交易日的官方開盤價作為可執行價格代理，持有至到期結算日收盤。若開盤價缺失、停牌或未成交，不假設可以用收盤價補成交。

### 2. 每個 outcome 保留兩種觀察

1. **事件研究**：訊號日收盤至到期日收盤。個股與市場價格指數使用同一收盤日期；它回答後續價格方向和相對市場表現，但訊號在收盤後發出時，這不是可成交損益。
2. **隔日開盤代理**：送達訊號後第一個可用交易日開盤至到期日收盤。報個股價格報酬、方向調整報酬及 OHLC 推算的最大有利/不利區間；缺少精確盤中/委託簿和個人費用，不能標為實際成交或淨利。

個股原始價格報酬、TWSE/TPEx 市場價格指數、價格口徑的相對結果和隔日開盤代理分欄存放。另有官方總報酬指數來源可供未來投資組合層參照。個股價格尚未調整股利、分割和減資，因此這些績效只代表價格變化。未送達與送達未知仍可列入政策反事實，但必須跟已送達候選分組；平台 receipt 只證明平台收件，不代表閱讀或成交。

### 3. 每次自動更新及年度回顧

- 市場訊號 capture 在寫入當日候選後，更新仍在 1 年追蹤範圍內的候選，記錄到期日、可用行情、資料缺口、送達狀態及來源版本。狀態為 `awaiting_maturity`、`pending_data` 或 `settled`；只有資料成熟且價格可核對才計績效。
- 每日重算 1 週、1 個月、3 個月、6 個月、1 年五份滾動回顧快照，每個期限只保留最新一份彙總，回看最多 2,000 個日曆日。逐則結果與到期狀態永久留在 follow-up ledger，年度 cohort 留在年度表。每個期限都先核對「到期候選數」和「已建追蹤數」，再分開標記資料成熟、來源完整、通知平台回執和績效分組；尚未到期的訊號不算資料缺漏。
- 成熟通知只摘要已選候選、平台 HTTP 2xx 且有回執、個股／市場基準／執行代理都已結算的結果。回顧以候選與期限去重；已知失敗可重試，平台接受但回執不明的通知不重送。回執不是已讀或實際成交證據。
- 分組解讀只將「新鮮來源、已確認送達、日期區塊區間下界大於零、方向市場超額為正」標為可進入成本模型與未觸碰樣本外檢驗的候選。這仍是未扣成本的觀察；所有家族反事實與門檻試算都留在研究層，正式訊號政策不會自動改動。
- 年報按訊號曆年分 cohort，直到該年最後訊號都達到 1 年期限後才產生。報告分市場、政策版本、訊號方向、家族及送達狀態，輸出候選與已結算數、行情覆蓋、平台 receipt 數與未確認送達數、上漲率、方向命中率、訊號日等權方向報酬、相對市場結果、MFE/MAE 和 break-even 摩擦成本。
- 到期年度回顧也會送到既有通知 ledger；去掉報告日期後，以實際內容雜湊去重。若補齊資料或更正結果改變報告內容，系統會以新版本通知；內容未變則不重複發送。
- 年度回顧保留 `complete_descriptive`、`incomplete_data`、`partial_coverage` 狀態與未結算原因；每個分組另報日期數和證據狀態。報告為描述性監控；訊號日密集或持有期重疊時不能把逐股列當成獨立樣本。
- 每個分組會標示日期區塊區間是否全正、全負、跨過零或樣本不足。這些區間尚未調整多重試驗；不能單靠 `positive_unadjusted_date_block_interval` 自動宣稱訊號有效，仍要先登記候選並通過鎖定樣本外評估。
- 每筆候選快照會記錄當時的 `policy_version`，年度結果按政策版本分組。獨立的試驗登記簿、holdout 鎖定和試過的候選數目前尚未自動化；每次規則改動在用績效決定是否啟用前，仍須人工登記假設、資料窗、候選數與一次性 holdout 結果。資料不足就維持觀察，不自動改門檻。

### 4. 漸進改進規則

先釐清失效來源，再提出版本候選：來源缺漏、訊號方向錯、選股門檻錯、訊號重複/相關、交易不可成交、公司行動或風險暴露。候選版本在離線歷史回放、時間序列樣本外與完整前瞻 shadow cohort 逐階段檢視。報告使用相同市場/期限/價格基準，公開交易日、覆蓋率、訊號日數、試驗數及缺漏；完成一年跟蹤只代表這批訊號已成熟，並不自動證明可投資。

**升級門檻：** 逐家族/方向的樣本外方向報酬與市場相對報酬需達預先登記門檻；成本敏感度不能完全吃掉結果；方向與資料品質分層後不得由少數訊號日主導；樣本數、下市/停牌處理、公司行動覆蓋及通知關聯必須通過完整性檢查；多重比較要調整。每年提供 review，政策版本由人工審核後再啟用。

## 現有系統缺口

- 既有 `market_signal_outcomes` 只有 1/5/10 個交易日、收盤對收盤結果，不能回答 1 週至 1 年、也沒有次日開盤價格代理。
- 既有行情快照只有收盤價與成交量；本次程式新增 OHLC 正規化後，只能對新來源快照建立次日開盤代理，不能假稱補齊舊版原始資料。
- 歷史候選未必有完整落選母體、通知逐卡關聯或 PIT 下市名單。回填期間需按完整證據覆蓋標記；缺失歷史不得推定為中性或完整樣本。
- 現在沒有個股股利/分割調整後價格及個人券商費率、稅費、借券可得性。系統以未調整個股價格和官方價格指數作相同口徑比較；成本 break-even 是可承受摩擦的上限，不是淨報酬。

## 實作與驗收邊界

本次程式增加 OHLC 欄位、五個日曆期限追蹤 ledger、每日更新的逐期限最新回顧快照、年度 cohort 回顧，以及成熟結果與年度回顧的去重通知。詳細分組寫入 `market_signal_horizon_reviews.report_json` 和 `market_signal_outcome_reviews.report_json`；通知送達狀態與平台回執保留在 `market_signal_notifications`，逐筆成熟結果關聯保留在 `market_signal_outcome_notification_candidates`。自動化程式只使用來源快照已知資料，不回寫歷史特徵、不模擬帳戶下單，也不會自動變更通知門檻。

部署後可用以下查詢檢視年度摘要和單一股票的各期追蹤：

```sql
SELECT signal_year, as_of_date, review_due_date, review_status, report_json
FROM market_signal_outcome_reviews
ORDER BY signal_year DESC;

SELECT signal_date, entity_id, market, direction, family_directions_json,
       candidate_selected, delivery_status, horizon_key, target_date,
       stock_return_pct, benchmark_return_pct, excess_return_pp,
       execution_return_pct, status, benchmark_status, execution_status,
       error_reason, benchmark_error_reason, execution_error_reason
FROM market_signal_followups
WHERE entity_id = '2330'
ORDER BY signal_date DESC, horizon_key;

SELECT as_of_date, horizon_key, review_status, report_json
FROM market_signal_horizon_reviews
ORDER BY as_of_date DESC, horizon_key;

SELECT n.notification_channel, n.status, n.platform_http_status,
       n.platform_receipt_id, o.candidate_id, o.horizon_key
FROM market_signal_outcome_notification_candidates o
JOIN market_signal_notifications n ON n.id=o.notification_id
ORDER BY n.updated_at DESC, o.candidate_id, o.horizon_key;
```

本機既有回測焦點測試曾通過；本輪新增的回顧與通知尚未在自然日線排程執行。要確認資料庫新增表已建立、逐日快照實際寫入、通知 ledger 收到回執，需等下一次自然日線流程；各期限績效則要等其到期。未來一年結果不能預先證明。
