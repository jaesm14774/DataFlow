# 2026-11-02 TAIFEX 選擇權解析根因與修正驗證

## 根因判定

根因是 `taifex_index_options_positions_daily` 被登記為 `taifex_json`，但該官方 OpenAPI operation 實際回傳 CSV；`_fetch_source_json_payload` 因而對 CSV 標頭的第一個字元呼叫 `json.loads`，每次都在 line 1 column 1 報 `JSONDecodeError`。這是來源 adapter/schema 程式錯配，不是單一偶發的傳輸抖動。

## 自然失敗證據

- Source key：`taifex_index_options_positions_daily`；官方 endpoint：[TAIFEX index-options positions](https://openapi.taifex.com.tw/v1/MarketDataOfMajorInstitutionalTradersDetailsOfCallsAndPutsBytheDate)。
- After-close logical run：`scheduled__2026-10-30T12:00:00+00:00`；實際在 2026-11-02 20:00–20:03 台北執行。32/32 mapped source keys 有結果、31 success／1 failed，DAG root failed；來源列合計 raw/accepted/quarantined/duplicate 為 83,477/82,415/1,062/0。
- Map index 24、task try 1 和 try 2 都在 `adapters.py::_fetch_source_json_payload` 呼叫 `json.loads` 失敗。每個 task try 記錄的前兩次內層 retry 都回 status 200、`application/octet-stream`、2,789 bytes；第三次仍以 `Expecting value: line 1 column 1 (char 0)` 失敗，source audit 保留 HTTP 200。故 0 raw 是解析前失敗的記帳，不是選擇權淨部位為零。
- 失敗當時來源 registry/runtime 都匹配當時的 `taifex_json` 定義，runtime drift 是 0。原失敗 body 未保存，所以不能逐 byte 證明 10/30 回應內容；但同一官方 URL 在 11/02 對 `Accept: application/json`、`application/json, text/csv`、`text/csv` 三種請求都回 `application/octet-stream`、2,064-byte CSV，首列為 `日期,商品名稱,買賣權別,身份別,...`，下接 20261030 資料列。這與失敗 log 的 status、MIME 和 char-0 症狀一致，確認程式的 JSON 假設錯誤，且不能靠 Accept header 協商成 JSON。

## 修正

- 將唯一該選擇權來源的 allowlisted adapter 從 `taifex_json` 改為既有 `taifex_csv`。
- 只對該 source key 容許官方 `application/octet-stream`；CSV parser 仍要求精確的 16 欄繁體中文 header set，不放寬其他 TAIFEX 或 CSV 來源。
- 在 source config 登記標準欄名到官方 CSV 欄名的 aliases，並於 normalizer 讀取日期、商品、買賣權、身份別、交易淨額及未平倉淨額。Required-field validation 與 source-field persistence 共用 aliases，保留原始 CSV 標籤。
- 仍把選擇權列定位為市場彙總背景，不加入任何個股方向 family 或評分。

## 修正前後驗證

- 先寫 BDD 情境並跑紅燈：官方 CSV 標頭被 `taifex_json` 在字元 0 拒絕，重現自然錯誤。
- 改 adapter/contract/aliases 後，正向解析與欄位漂移拒收兩個驗收情境通過；`tests/test_source_extension.py` 與 `tests/test_source_http_retries.py` 合計 19 passed。`py_compile` 對 `adapters.py`、`registry.py` 通過。
- 修正後以本機 parser 對官方 endpoint 做一次唯讀 request：HTTP 200、6 normalized rows、6 quality `ok`、0 quarantine，effective date 2026-10-30。此 request 沒有經 Airflow、沒有寫 DB，不等於自然收集恢復；資料日也落後 11/02 個股行情。
- 20:32:51 與 20:40:46 兩份同日唯讀 snapshot 指向同一 daily run key、同一 cutoff：universe 1,955、candidate 0；1,931 個已評估標的均 `below_three_directional_families`。方向法人 available/usable 列為 listed 1,045/1,058、OTC 763/897；達多空條件列分別為 448、187。上市分點 same-day 745、accepted panel 737、direction-usable 242、達條件 29；TPEx 分點明確為 `source_unavailable_skipped`。上市 1,045 列的 natural PIT 同日版本數仍 0，故歷史 vintage 限制保留，但不得因此全域忽略既有可判讀值。修正只改市場彙總來源的取檔/解析，不改門檻、分母或個股方向判讀；候選結果無變化。1w outcome review 967 settled/125 pending，1m 6/125，均 `outcome_data_incomplete`，policy decision 為 no automatic changes。
- Post-fix snapshot 因資料庫仍保存舊 `taifex_json` adapter/config，對這個歷史 source row 顯示單一預期 contract drift。DAG 的 `ensure_source_extension_schema` 在每次自然 collection discovery 前會 bootstrap catalog；未手動修改 DB 或觸發 DAG。後續自然 after-close run 須先確認 registry/runtime 一致，再檢查 source accepted/quarantine/duplicate、資料日、source fields 與 PIT versions。

## 同時點來源完整度與邊界

- 最新自然 daily source process：23/23 keys，8,201/8,201/0/0；上市／上櫃普通股主檔 1,058/897。收盤行情日期 11/02；TWSE quote 1,057 accepted、1,057/1,058 配對，TPEx quote 893 raw、874 accepted、19 quarantine、874/897 配對。
- After-close 最新仍是上述失敗的 10/30 logical run；成功也不等於完整。TAIFEX 期貨法人 6/6、大額交易人 1,386/1,386，有效日 10/30，均為市場彙總；期貨資料比 11/02 個股行情舊。TPEx 法人 flow 884 accepted、763/897 配對、121 unmatched；unmatched 不視為零。TWSE margin 1,042 raw 全數 quarantine、無 effective date，不補執行日。TPEx OAS 11627 為 19/19 市場彙總、有效日區間 10/01–10/30，需 freshness gate；TWSE TWTB4U 資格名單和 TWTB5U 個股成交量不可混用。
- 10/31 自然 daily 的八個 `income_statement_*` EPS feeds 均 HTTP 200 success：三個 feed 合計 84 accepted、其餘五個 0 rows；所屬期間 2026-09-30。read-only stored field count 為 84/84 有 numeric basic EPS；目前普通股主檔對碼 listed 46/1,058、OTC 38/897，只代表本批已收且有值列的覆蓋。無可比舊期不推同比；0-row feeds 不解讀成零 EPS 或沒有財報。
- 最新 intraday source process：10/10 keys、1,990/1,990/0/0；9 個選入事件列均未形成新卡，3 個舊 fingerprint 有 HTTP 2xx receipt。最新 monthly source process 仍是 9/24 logical run：2/2 keys、45,074/43,971/0/1,103，沒有更新 lineage 的新 natural run。
- News natural batch：15 feeds、1,115 URLs、1,113 accepted、2 fetch/parse rejects、0 invalid-time quarantine。部分 feed 為中央社 99/100 accepted（1 reject，來源記錄時間近期）及鉅亨網 149/150（1 reject，135 近期、14 較舊）；15 feed freshness 分類為 9 recent、4 mixed、1 無已接受文章、1 stale。記錄 cutoff 為 2026-10-31 18:41:32 台北；這些時間來自來源記錄，不替代逐篇原頁發稿時間。
- Daily notification ledger 有 2 rows，兩筆均有 HTTP 200 與 receipt；這是來源品質/摘要交付回執，不是候選卡或人已讀。日線 candidate 仍為 0，具體原因是三家族門檻未達，不以「全部資料缺失」概括。

## 下一個自然驗收

不手動觸發或重跑 DAG。下一個自然 after-close 要驗證：registry 更新為 `taifex_csv` 和新增 config aliases；endpoint 的 CSV schema 符合 16 欄 allowlist；source run 記錄合理 raw/accepted/quarantine/duplicate、effective date、PIT version fields，並確認 freshness。只有自然批次持久化成功後才標記修正已恢復；TAIFEX 市場彙總仍不作個股方向訊號。
