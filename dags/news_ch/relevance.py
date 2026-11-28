"""新聞 / 好康通知的相關性分類（純函式，無 DB / 網路依賴，方便測試與 dry-run）。

使用者只想被通知四類資訊，其餘一律視為雜訊：
  holding      我的持股（標題提到持股名稱 / 代號，或內文明確多次提到）
  ai_tech      AI / 科技技術的最新資訊
  deal         超級大打折、好康活動（路由到「好康報」頻道）
  market_core  股市核心訊息（只看標題，且需「市場詞 + 事件詞」，避免例行收盤快訊）

設計原則：標題為主、內文為輔；來源只當先驗，不再整個來源直接推播。
"""
import difflib
import re

def _s(x):
    """None / NaN -> ''，其餘轉字串（避免 'nan' 被當成內容或名稱）。"""
    return '' if x is None or x != x else str(x)


BUCKET_HOLDING = 'holding'
BUCKET_AI = 'ai_tech'
BUCKET_DEAL = 'deal'
BUCKET_MARKET = 'market_core'

# 每次排程（一天 6 次）各類別最多推播幾則；持股不設太緊，避免漏掉重要訊息。
PER_RUN_CAP = {
    BUCKET_HOLDING: 6,
    BUCKET_AI: 1,
    BUCKET_DEAL: 1,
    BUCKET_MARKET: 1,
}

# ---------------------------------------------------------------- AI / 科技
AI_TECH_SOURCES = {'iThome', '泛科學'}
AI_TRUSTED_SOURCES = {'鉅亨網', '中央社', 'thenewslens'}
AI_TRUSTED_CATEGORIES = {'科技', '財經', '證券', '產經', '國際財經', '科技新知', '科技與創新'}

# 硬詞：單一命中即具足夠訊號（公司 / 產品 / 明確技術名詞）
AI_HARD_TERMS = [
    'OpenAI', 'ChatGPT', 'GPT-', 'Anthropic', 'Claude', 'Gemini', 'Copilot', 'DeepSeek',
    'Llama', 'LLM', '大型語言模型', '生成式AI', '生成式 AI', '人工智慧', 'AI代理', 'AI 代理',
    'AI Agent', 'AI agent', '輝達', 'NVIDIA', 'Nvidia', '黃仁勳', 'GPU', 'HBM', 'CoWoS',
    '先進封裝', '量子電腦', '機器人', '自駕', '資料中心', 'AI伺服器', 'AI 伺服器',
]
# AI 本身是常見詞，需獨立比對，避免 AIR / MAIN 等誤中
_AI_WORD = re.compile(r'(?<![A-Za-z])AI(?![A-Za-z])')
# 軟詞：科技來源中才採信
AI_SOFT_TERMS = ['晶片', '半導體', '模型', '演算法', '雲端', '開源', '資安', '台積電']

# 標題含這些字多半是娛樂 / 社會雜訊，不論命中什麼都不當科技新聞
NOISE_TITLE = re.compile(
    r'演唱會|藝人|網紅|明星|偶像|選秀|婚變|外遇|車禍|命案|詐騙|星座|運勢|八卦|粉絲|票房|綜藝|出道'
)

# ---------------------------------------------------------------- 股市核心
MARKET_SOURCES = {'鉅亨網', '中央社'}
MARKET_TERMS = re.compile(
    r'台股|加權指數|大盤|櫃買|美股|費半|費城半導體|那斯達克|道瓊|標普|台幣|新台幣|外資|三大法人|聯準會|Fed|央行|證交所|金管會|ETF'
)
MARKET_EVENT_TERMS = re.compile(
    r'暴跌|崩跌|重挫|急殺|熔斷|大跌逾|狂飆|暴漲|飆漲|創高|歷史新高|創新高|跌破|站上|升息|降息|'
    r'理監事會|利率決策|FOMC|關稅|禁令|制裁|停牌|處置股|實施新制|修正案|救市|國安基金'
)
# 例行收盤 / 盤中快訊，即使命中也降級
MARKET_ROUTINE = re.compile(r'收盤|盤中|開盤|早盤|盤後|今日行情|漲跌幅排行|每日|一圖看')

# ---------------------------------------------------------------- 好康 / 大打折
PROMO_SOURCES = {'CP值', '好康情報誌', 'TVBS_SaveMoney'}
# 一般新聞也可用的「明確折扣」訊號（不含易與股市 / 社會新聞撞詞的字）
DEAL_STRONG_NEWS = re.compile(
    r'(?<![\d.])[1-5]折|(?<![\d.])[1-5]\.\d折|半價|買一送一|買1送1|買二送一|買2送1|第二件[1-5]折|'
    r'全館[1-6]折|黑色購物節|黑五(?!類)|限時[1-5]折'
)
# 好康來源（本身已是促銷專站）才額外採信的訊號
DEAL_STRONG = re.compile(
    DEAL_STRONG_NEWS.pattern + r'|免費送|免費領|免費兌換|(?<!\d)0元(?!\d)|史低|史上最低|1\+1|(?<!\d)1元|(?<!\d)9元|最低價|出清|開倉|廠拍|'
    r'雙11|雙12|年終慶|週年慶|超殺|下殺|國旅補助|旅遊補助|消費券'
)
# 股市 / 社會語境：一般新聞若帶這些字，不當成好康
DEAL_CONTEXT_NOISE = re.compile(r'台股|股價|股市|目標價|持股|法人|選舉|賄選|羈押|房市|建商|欠稅|詐')
DEAL_MEDIUM = re.compile(r'優惠|折扣|特價|限時|特賣|回饋|折價券|優惠碼|促銷|搶購|開賣')
# 活動 / 展演類不是折扣，即使標題寫「優惠一次看」也先降級
DEAL_EVENT_NOISE = re.compile(
    r'演唱會|跨年|聖誕節活動|聖誕市集|耶誕城|燈會|菊展|音樂節|開幕|售票|購票|座位圖|卡司|直播|轉播|育嬰|育兒|舊換新補助'
)
# 機票促銷（原本 news_ch_notify 的規則，改歸好康路徑）
FLIGHT_DEAL = re.compile(r'機票')
FLIGHT_DEAL_WORDS = re.compile(r'促銷|特價|開賣|搶票|出清|限時|折|元起|低價')


class Classification(tuple):
    """(bucket, reason, score)；用 tuple 子類只為了有名稱屬性又維持可比較 / 可打印。"""
    __slots__ = ()

    def __new__(cls, bucket, reason, score):
        return super().__new__(cls, (bucket, reason, score))

    bucket = property(lambda self: self[0])
    reason = property(lambda self: self[1])
    score = property(lambda self: self[2])


def build_holding_matchers(holdings):
    """holdings: [(code, name_or_None), ...]（只放 notify_status=1）。回傳 [(code, name)]。

    名稱過短（<2 字）的略過，避免整串誤中；代號永遠保留，因為 ETF 常查不到名稱。
    """
    out = []
    for code, name in holdings:
        code = _s(code).strip()
        if len(code) < 4:
            code = ''  # 過短代號（如 '1'）會命中「第1名」，只靠名稱比對
        name = _s(name).strip() or None
        if name is not None and len(name) < 2:
            name = None
        if code or name:
            out.append((code, name))
    return out


# 名稱命中需搭配股市語境，避免「台航」命中「塔台航管」、「精誠」命中鸚鵡新聞
STOCK_CONTEXT = re.compile(r'股|公司|法說|營收|配息|除息|董事會|財報|EPS|庫藏|漲停|跌停|目標價|獲利|增資|減資|股東|ETF|上市|上櫃')


def _holding_hit(title, content, matchers, min_content_mentions=2):
    for code, name in matchers:
        if name and name in title and STOCK_CONTEXT.search(title + content[:200]):
            return f'持股:{name}'
        # 代號要求前後不是數字，避免 2617 命中 126177
        if code and re.search(rf'(?<!\d){re.escape(code)}(?!\d)', title):
            # 5 碼以上 / 0 開頭（ETF）幾乎不會與一般數字撞；4 碼個股需股市語境，避免「2617萬」「2002年」
            if len(code) >= 5 or code.startswith('0') or STOCK_CONTEXT.search(title + content[:200]):
                return f'持股:{name or code}'
    for code, name in matchers:
        if name and content.count(name) >= min_content_mentions and STOCK_CONTEXT.search(title + content[:200]):
            return f'持股:{name}(內文×{content.count(name)})'
    return None


def _ai_hit(title, content, source, category):
    if NOISE_TITLE.search(title):
        return None
    hard = [t for t in AI_HARD_TERMS if t in title]
    if _AI_WORD.search(title):
        hard.append('AI')
    trusted = source in AI_TRUSTED_SOURCES or category in AI_TRUSTED_CATEGORIES
    soft = [t for t in AI_SOFT_TERMS if t in title]
    # 非科技來源：需要硬詞，且類別必須是科技 / 財經類，否則視為雜訊
    if hard and source in AI_TECH_SOURCES:
        return hard, 3 + len(hard)
    # 一般 / 財經媒體：需 2 個硬詞，或 1 個硬詞 + 科技類別 / 軟詞，才算科技新聞
    if len(hard) >= 2:
        return hard, 2 + len(hard)
    if hard and trusted and (soft or category in {'科技', '科技新知', '科技與創新'}):
        return hard + soft, 2 + len(hard) + len(soft)
    # 科技來源 + 軟詞，但必須同時帶有 AI 語境（內文前 200 字提到 AI）
    if soft and source in {'iThome', '泛科學'} and _AI_WORD.search(content[:200]):
        return soft, 1 + len(soft)
    return None


def _market_hit(title, source):
    if source not in MARKET_SOURCES:
        return None
    m = MARKET_TERMS.search(title)
    e = MARKET_EVENT_TERMS.search(title)
    if not (m and e):
        return None
    score = 4
    if MARKET_ROUTINE.search(title):
        score -= 2  # 例行快訊降級（低於 3 視為不推）
    return f'{m.group(0)}+{e.group(0)}', score


def deal_score(title, extra_text='', promo_source=True):
    """回傳 (score, reason)；score>=2 才推。標題訊號為主，摘要只能補強不能單獨成立。"""
    title, extra_text = _s(title), _s(extra_text)
    pattern = DEAL_STRONG if promo_source else DEAL_STRONG_NEWS
    if not promo_source and DEAL_CONTEXT_NOISE.search(title):
        return 0, ''
    if DEAL_EVENT_NOISE.search(title) and not pattern.search(title):
        return 0, ''
    strong = pattern.search(title)
    medium = DEAL_MEDIUM.search(title)
    if FLIGHT_DEAL.search(title) and FLIGHT_DEAL_WORDS.search(title):
        return 3, '機票促銷'
    if strong and DEAL_EVENT_NOISE.search(title):
        return 1, ''  # 活動頁順帶提折扣：不夠格
    if strong:
        return 3, strong.group(0)
    if medium and promo_source and DEAL_STRONG.search(extra_text or ''):
        return 2, f'{medium.group(0)}+摘要:{DEAL_STRONG.search(extra_text).group(0)}'
    return 0, ''


def classify(title, content, source, category, matchers, *, allow_deal=True):
    """回傳 Classification 或 None（None = 雜訊，不通知）。優先序：持股 > 好康 > AI > 股市核心。"""
    title = _s(title).strip()
    content = _s(content)
    source = _s(source).strip()
    category = _s(category).strip()
    if not title:
        return None

    reason = _holding_hit(title, content, matchers)
    if reason:
        return Classification(BUCKET_HOLDING, reason, 10)

    if allow_deal:
        score, reason = deal_score(title, content[:120], promo_source=source in PROMO_SOURCES)
        if score >= 2:
            return Classification(BUCKET_DEAL, reason, score)

    ai = _ai_hit(title, content, source, category)
    if ai:
        terms, score = ai
        return Classification(BUCKET_AI, 'AI/科技:' + '、'.join(terms[:3]), score)

    mk = _market_hit(title, source)
    if mk and mk[1] >= 3:
        return Classification(BUCKET_MARKET, '股市:' + mk[0], mk[1])
    return None


def _norm_title(title):
    return re.sub(r'[\s\W_]+', '', str(title or ''))


BUCKET_PRIORITY = [BUCKET_HOLDING, BUCKET_DEAL, BUCKET_AI, BUCKET_MARKET]


def select_for_push(candidates, caps=None):
    """candidates: [(row, Classification)]。每類按分數排序，跨類別去相似標題（同一事件只推一次，
    歸屬優先序：持股 > 好康 > AI > 股市），再套每次上限。

    回傳 {bucket: [(row, cls), ...]}。被上限擠掉的不會補發（寧缺勿濫，且仍會寫入已讀紀錄）。
    """
    caps = caps or PER_RUN_CAP
    grouped = {}
    for row, cls in candidates:
        grouped.setdefault(cls.bucket, []).append((row, cls))
    out, seen_norms = {}, []
    for bucket in BUCKET_PRIORITY:
        items = sorted(grouped.get(bucket, []), key=lambda it: it[1].score, reverse=True)
        kept = []
        for row, cls in items:
            if len(kept) >= caps.get(bucket, 3):
                break
            n = _norm_title(row['title'])
            # 持股：只和「同一持股」的相似標題去重（不同持股的同模板標題如「X 11月營收年增」不可互相吃掉）
            pool = [k for r_, k in seen_norms if r_ == cls.reason] if bucket == BUCKET_HOLDING else                 [k for _, k in seen_norms]
            if any(difflib.SequenceMatcher(None, n, k).ratio() >= 0.7 for k in pool):
                continue
            kept.append((row, cls))
            seen_norms.append((cls.reason, n))
        if kept:
            out[bucket] = kept
    return out


def format_alert(row, cls, brief_chars=120):
    """單則訊息：[標籤] 標題 / 來源 時間 / 摘要 / 連結。取代原本標題 + 500 字分段 + 連結的多則轟炸。"""
    body = re.sub(r'\s+', ' ', _s(row.get('content')) or _s(row.get('brief_content'))).strip()
    if len(body) > brief_chars:
        body = body[:brief_chars].rstrip() + '…'
    lines = [f"【{cls.reason}】{row['title']}", f"{row.get('source', '')} {row.get('created_at', '')}".strip()]
    if body:
        lines.append(body)
    if row.get('article_url'):
        lines.append(str(row['article_url']))
    return '\n'.join(lines)
