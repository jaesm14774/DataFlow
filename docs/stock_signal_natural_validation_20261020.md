# 台股訊號自然批次核驗 — 2026-10-20

核驗截止：2026-10-20 20:42 台北時間。證據均為唯讀自然執行記錄、已存通知 payload／回執與同 cutoff XCom；沒有手動觸發／重跑 DAG、手動發通知或交易。完整快照：[20:42 inventory](stock_signal_inventory_snapshot_20261020_2042.json)。

## 自然來源批次

| DAG | 最新 run | 執行時間（台北） | source keys | raw／accepted／quarantine／duplicate | 核心日期與覆蓋 |
|---|---|---|---:|---:|---|
| `source_extension_collect_process` | `scheduled__2026-10-18T20:00:00+00:00` | 10/20 04:00–04:02 | 23/23 | 13,825／13,820／5／0 | 最新 daily source collection；有效日按 source key 分開 |
| `source_extension_collect_after_close` | `scheduled__2026-10-19T12:00:00+00:00` | 10/20 20:00–20:04 | 32/32 | 85,514／84,459／1,055／0 | listed／OTC master 1,056／893；contract drift 0 |
| `source_extension_collect_intraday` | `scheduled__2026-10-20T12:15:00+00:00` | 10/20 20:30–20:30:32 | 10/10 | 2,106／2,103／3／0 | 盤後自然執行；下游 universe 為 0 |
| `source_extension_collect_monthly` | `scheduled__2026-08-24T20:00:00+00:00` | 9/25 04:00–04:00:53 | audit unknown | 無法按本次 run 歸因 | 9 筆 legacy `source_runs` 無 `dag_id`；下次 earliest create 10/25 04:00 台北 |

前三個 DAG 都是所有預期 source keys 回報、registry/runtime contract drift 為 0；source quality 仍有隔離與缺列。After-close 1,055 隔離由 TPEx 報價 13 列與 TWSE margin 1,042 列構成。Intraday 3 隔離列為 TPEx insider planned 2、TWSE insider untransferred 1。TWSE trading-attention 回 HTTP 200 但 0 rows，source semantics 未確認，不解讀為「沒有注意股」。

## 日期、單位與市場覆蓋

- TWSE quote 1,053/1,053 accepted、effective 10/20；其中 1,050 有效收盤、3 無效收盤。上市 master 1,056，另 3 檔當日沒有行情列。
- TPEx quote 891 raw／878 accepted／13 quarantine、effective 10/20；878 個代碼與 OTC master 相符，其中 9 檔是明確 no-trade，869 檔有有效收盤。OTC master 893，另 15 檔無當日行情列。no-trade、未列、隔離分開計，不把未知補成零。
- TWSE stored institutional flow 1,045 檔可判讀，503 達原方向條件；同日自然 PIT 0，但存有 collection time 1,045。這是已讀入的既有上市 flow，不能稱為來源缺失或完整 immutable PIT。
- TPEx institutional flow 882 accepted、有效日 10/20；761 配對 OTC master，121 unmatched；761 same-day PIT、760 direction-usable，204 達方向條件。上櫃當日有成交但同日 flow 缺列 109 檔，仍未知。
- 上市券商分點同日 420、accepted panels 411、direction-usable 139、35 達方向條件；15 panels 因 duplicate/invalid/mixed capture、4 unbalanced。TPEx 券商分點 `source_unavailable_skipped`。
- 合併 flow 1,805 = listed 1,045 + OTC 760；qualified institutional counts 503 + 204 = 707。法人與分點 family count 有跨股／跨族重疊，不能相加推成候選數。
- TWSE margin HTTP 200、1,042 raw、0 accepted、1,042 quarantined、無有效日；不合成本機日期。TPEx margin 801 accepted、effective 10/19，落後 10/20 行情一天。
- TAIFEX large-trader source_run `10843` HTTP 200、1,386 raw／accepted、0 quarantine／duplicate、effective 10/19；落後行情 10/20 一個交易日，維持市場背景用途。成功 Content-Type 未存入 `source_runs`，因此無法證明本次 response 走 `application/octet-stream` 分支。研究來源台帳已加入此自然觀察。
- TPEx OAS/day-trade market summary 12 rows、有效日範圍 10/01–10/19、沒有個股代碼，只能作具 freshness gate 的市場彙總。TWSE／TPEx 當沖資格批次 1,235／842 accepted，effective date 10/19／10/20，與目前普通股 master 配對 979／725，未配對 256／117 仍未知；資格列不等於成交列。

## 季損益與獲利率 family

八個官方季損益/EPS feeds 本次共 1,977 accepted，effective date 全為 2026-06-30；它們是不同報表子集，不把列數相加當唯一公司覆蓋。按 source key 持久化的 1,978 列基本 EPS 全非空；TPEx CI 存量 886 比本次 accepted 885 多 1 列，但 source records 沒有 run-level lineage，不能歸給本次。TWSE CI 有 31 列未配目前上市主檔。這八個 feeds 現有資料只有 2026Q2，沒有比較期，不推同比。

本次 `financial_count=1,904`，代表有財務資料可讀，不等於盈利方向票已成立。Point-in-time 24 個月獲利率集合中，TWSE／TPEx `quarterly_profitability` 都只有 2026-06-30。來源契約明確是財報期累計營收／獲利率；目前沒有同年 Q1 累計列推導 Q2 單季率，也沒有 2025Q2 同季列。`profitability_vote` 需同季 YoY 比較，並且營業利益率變化至少 3 個百分點或跨零才投票。因此 daily family trigger 中沒有 profitability，不是資料欄位可任意放寬後就能成立的零方向。保留現行規則，等待自然累積足夠可比 vintage；不回填假同比。

## 日線、盤中候選與實際送達

10/20 daily capture `ready/partial`，`news_context_status=matched`、universe 1,949、有效價格 1,919、flow 1,805、revenue 1,948、financial 1,904、borrow 1,796、insider 1,939、dividend 1,945。候選 17：12 偏多、5 偏空、0 分歧。Family triggers：price 202、volume 41、institutions 707、broker branches 35、profitability 0；各 family 可能落在同一標的。

候選 filter 計數可完整對平：1,931 個有當日 accepted quote code 的證券進入評估；1,914 因不足 3 個有效方向 family 未通過，17 成為候選。Universe 與 accepted quote-code 的差額 18，正好是上市 3 + 上櫃 15 個缺行情列；不屬於 unexplained filter loss。`source_status=partial` 的已知具體理由包括 TPEx quote 13 quarantine、TWSE margin 1,042 quarantine、TWSE trading-attention 0 rows 語意未明、3 個上市 invalid close 與已辨識行情缺列。這些來源限制沒有被錯寫成「整體沒有訊號」。

Notification payload 對帳發現：daily digest 17 個合格候選中，逐檔卡片顯示偏多最多 6、偏空／分歧最多 2；summary footer 明確寫另 9 檔未逐張列出。序列化 payload 有 8 個 candidate-card embeds，且 8/8 在有 receipt 的 HTTP 2xx ledger rows；另 1 個 summary embed。共 9 notification ledger rows、9 HTTP 2xx、9 receipts、0 errors。`notified_count=1` 是 daily digest flag，ledger rows 是訊息數，candidate cards 是另一口徑；回執不代表人已讀。卡片 cap 與 footer 計數吻合，沒有證據支持改評分或增加通知量。快照工具已加 `latest_daily_digest_content` 與 `latest_signal_analysis_diagnostics`，下一次可直接核候選篩選與卡片展示數，而不讀取全訊息內容。

20:30 intraday child capture `ready/partial`、universe 0、candidate 0、`news_context_status=no_candidates`，沒有候選 delivery rows。這是盤後無掃描母體，不是當日市場沒有訊號。Child `DagRun.run_type=manual` 是 parent-created child run 的 Airflow metadata；`source_dag_id/source_dag_run_id` 指回 scheduled after-close run，並非本次手動觸發。

## 新聞來源批次

自然 `news_ch_collect_process / scheduled__2026-10-20T08:30:00+00:00` 於 18:30–18:42 台北完成：15 feeds、1,130 URLs、1,125 parsed、1,124 valid-time／accepted、5 fetch/parse reject、1 invalid-time quarantine、0 content/duplicate reject。`delete_and_insert` 回報 inserted 1,124。Cutoff 10/18 18:42:16：925 有精確 recorded datetime 且在 48h、54 只有日期且晚於 cutoff 日期、2 筆 cutoff 日時刻未知、143 依記錄精度較舊、future 0；中央社 fetch/parse reject 2、鉅亨網 3、自由時報 invalid timestamp 1。這是 feed 批次保存時間的 freshness profile，不是逐篇原頁時間覆核，也不把轉載當獨立佐證。日線新聞 context matched，但新聞保留在原始事實／背景角色，不自動轉成方向票。

## 成熟 outcomes 與改善驗證

截至 cutoff，10/19 的 H1 `signal_close` 已成熟：TWSE 4 rows、1 directional hit、mean excess +3.6404pp；TPEx 3 rows、1 hit、+1.1243pp。這是單一 signal date 的市場拆分，`next_close` H1 與 H5/H10 尚未成熟；不據此調權重或宣稱上漲機率。

本輪假說：`candidate_count` 與通知回執數相差，可能是漏送候選。核對 capture XCom、`market_signal_candidates` 與每筆 ledger payload 後，確認根因是 digest 的明確展示上限，不是 delivery failure；17 candidates = 8 candidate cards + footer 明示省略 9，9/9 訊息均有 HTTP 2xx receipt。決定只改善唯讀盤點器，新增同 run_key 的候選篩選計數及 digest payload card/omission 對帳，不改生產訊號、門檻或通知行為。

驗證：`python -m py_compile docs/stock_signal_inventory_snapshot.py` 成功；內嵌 targeted assertions 確認 card count、footer omissions、reconciliation、2xx／receipt 與 malformed payload 計數；Airflow worker 實跑唯讀快照取得同 run_key 的 1,931 evaluated／1,914 below threshold／17 candidates、8 cards／9 omitted，且與 source coverage 1,931 quote-row codes 對平。快照新舊使用同一批自然 signal／source runs；改動只增加診斷欄位，未改業務資料或下游結果。
