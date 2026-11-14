# 遊戲攻略知識收集

## 驗收情境

User Given 使用者指定個人用途、資料只留在本機 `game` 資料庫且不呼叫外部 LLM，When Airflow 執行收集 DAG，Then 收集器依 robots 規則與低速限制盤點遊戲專版、索引攻略文章並保存主文與各樓正文；不保存作者名稱或樓層留言；只有逐頁收完且每樓正文都存入後，討論串才標記為完整。

User Given 有多個待回補看板與長討論串，且每次執行最多 50 個 HTTP 請求，When Airflow 執行一批收集，Then 看板與討論串都按最久未處理的頁面輪流推進，並為全文頁保留請求額度，避免單一長看板或長文章壟斷後續批次。

User Given guide 首頁依手機遊戲、PC、TV 掌機、動漫畫、宅生活與站務分組，且遊戲分組內含平台與類型子群，When 收集器映射看板，Then 只自動啟用遊戲根分類下的單一遊戲專版，平台綜合板與其他社群留作候選且不抓正文。

User Given 分類頁頂端同時有手機、PC、TV 掌機、動漫畫、宅生活與站務導航，When 收集器建立分類頁佇列，Then 導航頁以正確根分類記錄，不把它們誤記成目前分類的子類。

User Given 根分類頁與遊戲類型頁都列出同一個看板，When 更新候選分類，Then 較粗的根路徑不得覆蓋同一遊戲根下較細的類型路徑，根路徑改變時須重掃其類型頁。

User Given 已掃描的遊戲類型頁新增一個遊戲專版，When 分類頁超過 7 天未更新並再次執行 DAG，Then 收集器重掃該遊戲根下的分類頁，並自動建立符合遊戲分類條件的遊戲與看板；sitemap-only ID 仍保留待辨識。

User Given 巴哈姆特遊戲根分類直接列出具名看板但沒有更深的類型路徑，When 候選映射看板，Then 根分類中的具體遊戲／系列看板仍可自動建立遊戲實體並啟用；主機平台與泛社群標題仍不啟用。

User Given 一次 DAG run 收完多頁看板列表與討論串正文，When 收集任務結束，Then 執行稽核記錄本次請求數、看板頁數、討論串頁數與新增完整文章數。

User Given 攻略樓層只有圖片且已解析到圖片媒體引用，When 收集器解析該樓層，Then 保存圖片 HTML 與媒體引用並繼續整批收集，不因文字欄位為空而失敗。

User Given 數百個遊戲看板分批回補，When 查詢遊戲或看板收集進度 view，Then 可以看到列表回補狀態、已索引文章數、完整文章數與待抓正文數。

## 可用取得方式與範圍

- 目前沒有找到巴哈姆特公開的官方哈啦區 API 或看板文章 RSS。官方提供 [`robots.txt`](https://forum.gamer.com.tw/robots.txt)、[sitemap 索引](https://forum.gamer.com.tw/sitemap/sitemap.xml) 與[遊戲分類目錄](https://forum.gamer.com.tw/guide.php)。本次本機取樣時，`guide.php` 有 252 個看板連結；sitemap 分成 4 個子檔，列出約 53,168 個不同看板 ID。sitemap 是全站集合，包含非遊戲社群，不能直接視為遊戲看板清單。
- `guide.php` 首頁取樣有 40 個分類 ID 和 252 個看板連結；巢狀分類頁還會列出子分類與其他看板，所以收集器會遞迴掃分類頁。sitemap 補上目錄未列出的看板 ID；它是全站看板集合，不全是遊戲版。巴哈姆特 GNN 在 2026 年 4 月報導站上總板數 51,847，證明要核對「所有看板」需要比熱門首頁更多的盤點範圍。[GNN 統計](https://gnn.gamer.com.tw/detail.php?sn=307448)
- 單一遊戲看板的文章列表可有數百頁；本機抽查的棕色塵埃 2 看板首頁列出 32 篇、總列表 319 頁。討論串也分頁，需靠 Airflow checkpoint 長期回填。
- 看板文章列表使用 `B.php?bsn=<看板ID>&page=<頁碼>`；討論串使用 `C.php?bsn=<看板ID>&snA=<文章ID>&page=<頁碼>`。列表解析只收含攻略、心得、教學、分析、機制或玩法字樣的文章；討論串解析讀取每頁頂層 `.c-section__main.c-post article.c-article`，逐頁續抓，排除 `.c-post__footer` 與巢狀留言。
- 收集器會先讀 robots 規則、只連到 `forum.gamer.com.tw` HTTPS 網址、每次請求至少間隔 5 秒，每次 DAG run 最多 50 個請求。遇到 HTTP 401、403 或 429 即停止該次工作，不繞過封鎖或登入限制。
- 官方會員條款說明會員內容著作權屬於發文會員，並限制未經巴哈姆特書面授權重製或利用站內內容；[站規](https://user.gamer.com.tw/help/rule.php)也適用站內服務。使用者已指定個人用途，因此資料會留在本機資料庫、商業衍生權限關閉；`permission_basis='user_personal_use'` 只記錄使用者指定的用途，不代表取得站方或每位作者的書面授權。([會員條款](https://user.gamer.com.tw/help/detail.php?sn=294))
- 官方 RSS 資料目前查到的是 GNN 新聞，不是哈啦區文章；[非官方 goGamer 專案](https://github.com/davidleitw/goGamer)記錄了 HTML 解析與文章樓層取得方式，但不代表官方 API 或內容授權。

## 收集 DAG

`game_knowledge_collect` 每 10 分鐘執行一次，最多每 run 發出 50 個請求，請求間隔至少 5 秒。每批看板列表最多 40 頁、討論串正文最多 10 頁；看板與討論串都按最久未處理的頁面輪流續抓。分類盤點耗用額度時，本批可用於正文的請求會相應減少。使用者指定個人用途後，DAG 啟用巴哈姆特來源；資料只保存於 `game` 資料庫，不設定外部模型連線，也不把正文傳出。

流程為：

1. `ensure_game_knowledge_schema`：建立 `game` 資料庫知識表、啟用巴哈姆特來源，並記錄使用者指定的個人用途與非商用範圍。
2. `collect_bahamut_game_knowledge`：掃描 guide 首頁、巢狀遊戲分類與 sitemap，將遊戲分類找到的專版以看板名稱映射到共用遊戲實體；每次續處理候選分類、看板文章列表與討論串頁面。文章正文只有在有效來源／看板／文章範圍允許保存全文時才會擷取；使用者用途為個人使用，商業衍生權限維持關閉。
3. `ingest_authorized_packages`：匯入其他已授權來源的 JSONL 資料包；所有來源共用相同資料模型。

看板映射流程：

1. guide 首頁的遊戲根分類為「手機遊戲」、「PC」、「TV 掌機」；解析每個看板所屬的遊戲類型子群，根分類直接列出的具名單一遊戲／系列看板也可映射。排除「主機平台」及名稱含綜合討論／開發工具的泛社群板。若同一看板同時出現在根頁與類型頁，保留同一遊戲根下較細的分類路徑；根分類路徑修正時重掃所屬類型頁。動漫畫、宅生活、站務和未確認分類保留候選、不抓正文。`mapping_source='directory_title'` 表示依目錄分類與看板名稱自動映射，`mapping_source='manual'` 表示人工指定且不會被自動改寫。檢查 `game_knowledge_board_candidates` 可確認映射是否需要修正。
2. sitemap-only ID 沒有名稱，會留在待辨識候選中，不會當成遊戲版抓文章。若之後從分類頁或人工資料取得名稱，可設定 `mapped_game_key` 與 `review_status='approved'`；不相關看板設為 `review_status='rejected'`。
3. 收集器會從 `game_knowledge_directory_pages`、`game_knowledge_board_scan_state` 與文章的 `next_post_page` 繼續尚未完成的頁面。每 10 分鐘按最久未掃順序輪替看板列表，已完成看板則重看第 1 頁發現新攻略；遊戲根下的分類頁至少每 7 天重掃，sitemap 每 30 天重查。來源拒絕、網路中斷或 DAG 超時時不會把未完成討論串標為完整。

`game_knowledge_rights_grants` 記錄用途基礎、範圍、有效日期及 `evidence_ref`。目前依使用者明確指定的個人用途寫入 source 範圍紀錄：允許保存中繼資料與全文、允許本機分析，`can_use_commercial_derivatives=0`。可另外用 board (`scope_type='board', scope_key=<看板ID>`) 或 article (`scope_type='article', scope_key=<看板ID>/<文章ID>`) 縮小範圍；較窄範圍的設定優先。來源停用、用途紀錄失效或沒有全文保存旗標時，收集器不會取得或保存正文。

## 資料模型

資料表位於 `dags/game/database/schema.sql`，都使用 `source_key` 與來源外部 ID 識別資料，未來可加入 GameFAQs 或其他攻略來源：

- `game_knowledge_sources`：來源登錄與啟用狀態。
- `game_knowledge_games`：跨來源共用遊戲實體及別名。
- `game_knowledge_board_candidates`：guide 分類與 sitemap 發現的看板候選；分類頁找到的遊戲看板會以名稱建立或合併遊戲實體，`mapping_source` 記錄自動或人工映射，sitemap-only IDs 留作待辨識候選。`ix_game_knowledge_candidates_discovery` 讓定期映射查詢先按來源與發現方式縮小範圍。
- `game_knowledge_directory_pages`、`game_knowledge_directory_taxonomy`、`game_knowledge_discovery_state`：遞迴分類頁、持久化分類路徑與定期 sitemap 盤點進度。
- `game_knowledge_boards`、`game_knowledge_board_scan_state`：已確認遊戲專版、啟用狀態與文章列表分頁進度。
- `game_knowledge_rights_grants`：來源／看板／文章層級的授權用途及證據。
- `game_knowledge_articles`：文章連結、分類、標籤、正文完整度與續抓頁碼。
- `game_knowledge_article_posts`：主文與各樓層，不含巢狀留言或作者名稱。
- `game_knowledge_article_versions`：授權版本快照、來源 HTML、正規化文字、媒體連結、內容雜湊及授權 ID。HTML 可保存表格、連結、圖片標籤等正文結構；圖片、影片只記錄引用網址，尚未下載附件。
- `game_knowledge_design_analyses`：供後續本機分析工作保存玩法循環、機制、成長、策略、特色與證據，以及分析器、prompt 版本及授權 ID；目前 DAG 不執行分析。
- `game_knowledge_ingestion_runs`、`game_knowledge_crawl_runs`：JSONL 匯入與網頁收集執行紀錄。
- `game_knowledge_analysis_input_posts`：本機分析讀取 view，只顯示完整文章目前版本中、來源啟用、全文與本機分析用途仍有效的樓層正文；不會呼叫外部 LLM。
- `game_knowledge_board_ingestion_status`、`game_knowledge_game_ingestion_status`：按看板或遊戲查看列表回補狀態、已索引文章、完整文章及待抓正文數。

例如用 `SELECT * FROM game_knowledge_game_ingestion_status WHERE source_key='bahamut' ORDER BY pending_article_count DESC;` 找出正文待抓最多的遊戲，或查 `game_knowledge_board_ingestion_status` 檢視該遊戲各看板的列表頁進度。

只有 `posts_complete=1` 且 `content_status='full_text_stored'` 的文章會出現在本機分析 view。完整狀態以頁面走訪完成、資料庫內每樓都有正文及樓層數一致為條件。留言不會進資料庫。每 10 分鐘同步會發現新文章；已完成文章的既有樓層正文目前不會定期重抓，後續可用列表回報的回覆數變動觸發增量更新。

這項工作目前只負責爬取、整理並寫入本機 `game` 資料庫，不呼叫 LLM，也不會把文章全文傳到外部服務。`game_knowledge_analysis_input_posts` 是本機讀取介面，留給之後明確設定的分析流程使用。

若之後使用本機 LLM 分析，應把玩家陳述視為社群回報而非已驗證的遊戲程式事實；整理玩法循環、核心機制、成長／經濟、策略、差異化特色及痛點，並將主張連回 `source_key / board_external_id / article_external_id / post_external_id`。資料留在本機；不呼叫外部模型 API。分析結果可寫入 `game_knowledge_design_analyses`，保留模型／prompt 版本與樓層證據。

## 授權資料包格式

每行代表一篇討論串，可分頁匯入同一篇文章。最後一批設 `posts_complete=true`；資料庫會檢查所有批次是否已存入完整且獲授權的正文：

```json
{"source_key":"bahamut","game_key":"example-game","game_title":"Example Game","board_external_id":"12345","board_name":"Example Game 哈啦板","board_url":"https://forum.gamer.com.tw/B.php?bsn=12345","article_external_id":"67890","article_url":"https://forum.gamer.com.tw/C.php?bsn=12345&snA=67890","title":"攻略標題","article_type":"guide","tags":["攻略"],"posts_complete":true,"expected_post_count":2,"posts":[{"post_external_id":"floor-1","post_number":1,"post_type":"original","content_format":"text/html","body_content":"<p>正文 HTML</p>","body_text":"正文文字","media_refs":[]},{"post_external_id":"floor-2","post_number":2,"post_type":"reply","content_format":"text/markdown","body_content":"樓層回覆","body_text":"樓層回覆","media_refs":[]}]}
```

`content_format` 支援 `text/html`、`text/markdown`、`text/plain`；`body_text` 是利於全文檢索與後續本機分析的正規化文字，`body_content` 保存來源正文，`media_refs` 保存圖片或嵌入媒體引用。JSONL 資料包放在 `dags/game/database/incoming/` 或 `GAME_KNOWLEDGE_INBOX` 指向的目錄；該目錄已排除在版本控制之外。

## 尚未完成的外部邊界

- sitemap 也包含非遊戲社群；只有遊戲分類頁能確認的看板會自動映射遊戲，sitemap-only IDs 仍需後續分類，因此不能宣稱已涵蓋所有歷史／隱藏遊戲版。
- Airflow 不會呼叫外部 LLM；本機模型本身尚未設定或執行。
- 取樣頁面 HTML 可能變動；收到 403/429 時收集器停止，需等候授權來源提供穩定匯出/API 或重新確認可用方式，不繞過封鎖。

## 參考來源

- [巴哈姆特會員條款與會員內容說明](https://user.gamer.com.tw/help/detail.php?sn=294)
- [巴哈姆特站規](https://user.gamer.com.tw/help/rule.php)
- [哈啦區 robots.txt](https://forum.gamer.com.tw/robots.txt)
- [哈啦區 sitemap 索引](https://forum.gamer.com.tw/sitemap/sitemap.xml)
- [哈啦區遊戲分類目錄](https://forum.gamer.com.tw/guide.php)
- [巴哈姆特 GNN 新聞 RSS 說明](https://user.gamer.com.tw/help/detail.php?sn=18)
- [非官方 goGamer HTML 解析專案](https://github.com/davidleitw/goGamer)
