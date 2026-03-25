---
name: python-refactor
description: Systematic methodology for refactoring Python code—architecture, naming (PEP 8), performance, and test-first safety. Includes anti-patterns for AI-assisted coding (over-wrapping, swallowed errors, fake tests). Use when refactoring Python modules, improving maintainability, fixing bottlenecks, or when the user asks for Python cleanup or restructuring.
---

# Python 重構

以**可維護性與架構**為主、**效能**為輔；變更前須有**可自動執行的基準測試**，避免行為倒退。

## 核心原則

1. **測試先行**：未建立基準測試前不改邏輯；若無測試，針對將重構的範圍撰寫小型、聚焦的測試。
2. **可維護優先**：可讀性與結構優於微優化。
3. **效能次之**：處理明顯瓶頸，不為微小速度犧牲可讀性。
4. **不糾結微耦合**：著眼整體架構，不為無礙健康的細部耦合過度重構。
5. **不勉強改動**：程式已清楚且合理時，以建議代替硬改。
6. **不過度封裝**：三行直觀的程式不需要包成一個 function 來「美化命名」——那不是重構，是裝忙。
7. **重構要有感**：每次結構變更後問自己——「開發者找程式碼、改程式碼、看 diff 的路徑有沒有變短？」沒有就回退，不要留半成品。

---

## 禁止的反模式

重構時 **絕對不做** 以下事情，做了反而讓程式更難維護：

### 反模式 1：過度封裝（最常見）

把已經夠直觀的程式硬套一層 function，只為了讓它「看起來有整理」。

```python
# BAD — 原本一行就夠清楚，多包一層毫無意義
def get_stock_close_price(df):
    return df["close"].iloc[-1]

price = get_stock_close_price(df)

# GOOD — 直接寫，讀者一眼就懂
price = df["close"].iloc[-1]
```

**判斷標準：這層封裝是否帶來「複用」或「隱藏複雜度」？** 如果都沒有，就不要包。

需要封裝的情況：
- 邏輯會被 ≥2 處呼叫且未來可能變更
- 內部有 ≥3 步驟且步驟間有依賴關係
- 需要統一錯誤處理或資源管理（如 DB connection）

不需要封裝的情況：
- 單行取值、單行賦值、單行判斷
- 只在一個地方用，且程式碼本身已是最好的文件
- 把 `requests.get(url)` 包成 `fetch_url(url)` 這種「改名」式封裝

### 反模式 2：到處寫 Fallback 掩蓋錯誤

```python
# BAD — 錯誤被靜默吞掉，下游拿到 0 還以為正常
price = product.get("price", 0)

# GOOD — 該有值就該有值，沒有就讓它爆
price = product["price"]
```

Fallback 只用在「值確實可選」的場景。必填欄位用 fallback 是在埋地雷。

### 反模式 3：濫用 try/catch

```python
# BAD — 三段邏輯塞同一個 try，出錯只看到 None
def create_order(data):
    try:
        user = get_user(data["user_id"])
        coupon = validate_coupon(data["coupon_code"])
        order = save_order(user, coupon, data["value"])
        return order
    except Exception:
        logger.error("create order failed")
        return None

# GOOD — 讓錯誤自然冒泡，在 API 最外層統一處理
def create_order(data):
    user = get_user(data["user_id"])
    coupon = validate_coupon(data["coupon_code"])
    return save_order(user, coupon, data["value"])
```

業務邏輯層不要 try/catch，錯誤暴露得越早，排查成本越低。

### 反模式 4：寫「永遠通過」的假測試

```python
# BAD — 只檢查 not None，任何垃圾值都過
def test_process_order():
    result = process_order(mock_order)
    assert result is not None

# GOOD — 驗證具體業務結果
def test_process_order_with_discount():
    result = process_order(mock_order, discount=0.1)
    assert result.total_amount == 500  # 1000 * 0.9 的一半
    assert result.status == "confirmed"
```

測試的關鍵問題：**如果把被測函式的核心邏輯刪掉，這個測試會失敗嗎？** 不會的話就是假測試。

### 反模式 5：先修 Bug 再補測試

正確順序（TDD）：寫復現測試 → 確認失敗 → 修復 → 確認通過。先紅後綠，才能證明測試有效。

### 反模式 6：修 Bug 時順手刪調試日誌

調試日誌由人決定何時清除。修復代碼時不要動日誌，等確認問題真正解決後再統一清理。

### 反模式 7：假結構重構（搬檔案但體驗沒變）

把程式碼從一個大檔搬到幾個中型檔，或加 re-export shim「保持向後相容」，結果開發者找程式碼的路徑沒有變短。

```python
# BAD — 1600 行拆成 1400 + 200，開發者還是在千行大檔裡翻
#   news_sites.py    (1400 行)  ← 幾乎沒改善
#   save_money_sites.py (200 行)
#   module.py        (2 行 re-export)  ← 多一層間接，零價值

# GOOD — 要拆就拆到每個檔案一眼掃完，要嘛別拆
#   anue.py (118 行), cna.py (95 行), ettoday.py (170 行) ...
#   __init__.py 統一匯出，呼叫端改成 explicit import
```

**判斷標準：「如果我現在要修 X 爬蟲的 bug，新結構讓我更快定位嗎？」** 答案是沒有就不算重構。

具體規則：
- **拆檔要嘛拆到底，要嘛別拆。** 1600 行拆成 1400 + 200 是裝忙，不是重構。拆完後每個檔案必須小到開發者不需要搜尋就能掌握全貌。
- **不要為了「不動呼叫端」而留 re-export shim。** 改兩行 import 的成本遠低於維護一個空殼中介檔。向後相容是給外部使用者的，不是給自己同一個 repo 裡的兩個檔案的。
- **做完立刻用具體場景驗證。** 假想一個真實任務（例如「聯合報改版了，要更新 selector」），走一遍「開檔 → 找到目標 → 改完」的路徑。如果跟重構前沒差別，就回退。

---

## 流程

### 1. 分析與盤點

- 目錄與模組邊界、主要資料流與商業邏輯。
- 架構問題、效能熱點、命名與風格不一致處。
- **標記哪些程式已夠直觀不需動、哪些才真正需要重構。**

### 2. 建立基準測試

- 已有 `pytest` / `unittest` 等則先跑通並記錄為基準。
- 不足處補**小而準**的測試，涵蓋將變更的行為。
- 測試必須驗證具體業務結果，不接受 `assert result is not None` 這種空殼。
- **基準通過後**才進入重構。

### 3. 執行重構

**架構**：拆大函式/大類、單一職責、整理依賴。但每次拆分前問自己：這層抽象解決了什麼問題？如果答不出來，就不要拆。每做完一個結構變更，立刻用具體場景驗證（見反模式 7）——不要等全部做完才檢查。

**命名**：一致採 PEP 8；語意清楚、與領域用語一致。

**效能**：演算法與 I/O、查詢；必要時再 profiling 對症下藥。

### 4. 驗證

- 重跑 Step 2 的測試；**全數通過**才算完成。
- 失敗則除錯、修正或回退後再測。

### 5. 收尾

- 檢視是否引入了上述任何反模式（**特別注意反模式 7：結構變了但體驗沒變**）。
- 對每個結構變更，用一句話說明「開發者的哪個具體操作因此變快了」。說不出來的變更應回退。
- 未改動處若已足夠好，註明並給後續可選建議即可。

## 產出格式（重構報告）

```markdown
# Refactoring Report: [專案或模組名稱]

## 1. Initial Analysis
[初始狀態、架構/命名/效能問題摘要]
[標注「不需動」的部分及原因]

## 2. Baseline Testing Strategy
[使用或新增的基準測試說明]

## 3. Refactoring Actions Taken
### Architectural Improvements
[結構變更——每項說明「為何需要這層抽象」]

### Naming Convention Updates
[命名調整]

### Performance Optimizations
[效能相關變更]

### Removed Over-Engineering
[移除的過度封裝——還原為直觀寫法]

## 4. Verification Results
[測試全數通過的確認]

## 5. Recommendations
[後續建議；已足夠良好而未動之處]
```
