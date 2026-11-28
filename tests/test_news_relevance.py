from news_ch import relevance as R

HOLD = R.build_holding_matchers([
    ("2617", "台航"), ("6214", "精誠"), ("00830", None), ("1", "X"),
])


def cls(title, content="", source="中央社", category="", allow_deal=True):
    return R.classify(title, content, source, category, HOLD, allow_deal=allow_deal)


# ---- holdings
def test_holding_name_in_title_with_stock_context():
    c = cls("上品、國精化、台航等9家公司辦法說 展現獲利成長", source="聯合報")
    assert c.bucket == R.BUCKET_HOLDING and "台航" in c.reason


def test_holding_name_substring_in_unrelated_word_is_ignored():
    assert cls("塔台航管姐姐溫柔語音 網友狂讚", source="三立新聞") is None


def test_holding_etf_matched_by_code_not_embedded_in_longer_number():
    assert cls("國泰00830明年2月迎分割", source="聯合報").bucket == R.BUCKET_HOLDING
    assert cls("成交金額1200830元", source="聯合報") is None


def test_holding_content_needs_two_mentions_and_context():
    body = "精誠公布營收，精誠董事會通過配息"
    assert cls("某公司公布營收", body).bucket == R.BUCKET_HOLDING
    assert cls("某公司公布營收", "精誠一次") is None


def test_boilerplate_words_alone_no_longer_trigger():
    assert cls("ETF成分股調整 申報轉讓名單", "成分股 申報轉讓") is None


def test_short_code_and_one_char_name_removed_from_matchers():
    assert all(c != "1" for c, _ in HOLD)


# ---- AI / tech
def test_ai_tech_source_single_hard_term():
    assert cls("OpenAI 發表新模型", source="iThome").bucket == R.BUCKET_AI


def test_general_source_single_term_is_noise():
    assert cls("AI 美食餐廳開幕", source="自由時報") is None


def test_general_source_two_hard_terms_pass():
    assert cls("輝達黃仁勳談 AI 伺服器", source="自由時報").bucket == R.BUCKET_AI


def test_ai_not_matched_inside_english_words():
    assert cls("AIR 航空推出新航線", source="iThome") is None


def test_entertainment_noise_blocked():
    assert cls("AI 偶像演唱會登場", source="iThome") is None


# ---- deals
def test_deal_strong_discount():
    assert cls("全聯湯底第2件5折起", source="中時日報").bucket == R.BUCKET_DEAL


def test_deal_digit_boundaries():
    assert R.deal_score("目標價上看210元")[0] == 0
    assert R.deal_score("台積電1980元")[0] == 0
    assert R.deal_score("好康 4.5折 限時", promo_source=True)[0] >= 2
    assert R.deal_score("優惠 15折")[0] == 0


def test_deal_in_news_blocked_by_market_context():
    assert cls("台股出清全部持股 賠928萬", source="聯合報") is None
    assert R.deal_score("出清持股認賠", promo_source=False)[0] == 0


def test_flight_promo():
    assert R.deal_score("易斯達航空機票限時優惠")[1] == "機票促銷"


def test_event_pages_are_not_deals():
    assert R.deal_score("【2027大港開唱】時間地點/票價/購票/優惠")[0] == 0
    assert R.deal_score("【2026台北聖誕節活動】聖誕市集/優惠")[0] == 0


def test_promo_source_generic_coupon_roundup_dropped_unless_strong():
    assert R.deal_score("【foodpanda優惠碼】外送免運/折扣碼整理")[0] == 0
    assert R.deal_score("【星巴克買一送一】2026優惠時間")[0] >= 2


# ---- market core
def test_market_core_needs_market_plus_event_from_market_source():
    assert cls("台股崩跌 金管會祭禁空令", source="中央社").bucket == R.BUCKET_MARKET
    assert cls("台股崩跌 金管會祭禁空令", source="自由時報") is None
    assert cls("台股收盤 小漲20點", source="鉅亨網") is None


def test_routine_market_wrap_downgraded():
    assert cls("台股盤中重挫 外資賣超", source="鉅亨網") is None


# ---- selection
def _row(title):
    return {"title": title, "source": "x", "created_at": "t", "content": "c", "article_url": "u"}


def test_select_dedupes_same_event_across_sources_and_buckets():
    c1 = R.Classification(R.BUCKET_AI, "a", 5)
    c2 = R.Classification(R.BUCKET_MARKET, "m", 4)
    out = R.select_for_push([
        (_row("川普籲輝達讓黃仁勳回鍋 稱習近平也讚他是人才"), c1),
        (_row("川普籲輝達讓黃仁勳回鍋　稱習近平也讚他是人才"), c2),
    ])
    assert sum(len(v) for v in out.values()) == 1


def test_select_applies_caps_and_prefers_higher_score():
    items = [(_row(f"完全不同的標題{chr(65 + i) * 6}"), R.Classification(R.BUCKET_AI, "a", i)) for i in range(5)]
    out = R.select_for_push(items, caps={R.BUCKET_AI: 2})
    assert [c.score for _, c in out[R.BUCKET_AI]] == [4, 3]


def test_holding_never_dropped_by_dedupe_with_lower_priority():
    h = R.Classification(R.BUCKET_HOLDING, "持股:台航", 10)
    a = R.Classification(R.BUCKET_AI, "ai", 9)
    out = R.select_for_push([(_row("台航法說 AI 需求強勁"), a), (_row("台航法說 AI 需求強勁"), h)])
    assert R.BUCKET_HOLDING in out and R.BUCKET_AI not in out


def test_format_alert_single_message_with_tag_and_trim():
    row = {"title": "T", "source": "S", "created_at": "2026-11-28", "content": "字" * 500, "article_url": "http://u"}
    msg = R.format_alert(row, R.Classification(R.BUCKET_AI, "AI/科技:OpenAI", 3))
    assert msg.startswith("【AI/科技:OpenAI】T") and msg.endswith("http://u") and len(msg) < 250


# ---- audit regressions
def test_distinct_holdings_with_same_title_template_both_survive():
    h1 = R.Classification(R.BUCKET_HOLDING, "持股:台航", 10)
    h2 = R.Classification(R.BUCKET_HOLDING, "持股:精誠", 10)
    out = R.select_for_push([(_row("台航11月營收年增20%"), h1), (_row("精誠11月營收年增20%"), h2)])
    assert len(out[R.BUCKET_HOLDING]) == 2


def test_four_digit_code_needs_stock_context():
    assert cls("台航賺2617萬元的故事", source="聯合報") is None
    assert cls("2617 台航 股價 大漲", source="聯合報").bucket == R.BUCKET_HOLDING


def test_short_code_dropped():
    assert cls("第1名誕生", source="聯合報") is None


def test_none_nan_inputs_do_not_crash():
    nan = float("nan")
    assert R.deal_score(None) == (0, "")
    assert R.classify(nan, nan, nan, nan, HOLD) is None
    assert "nan" not in R.format_alert({"title": "T", "content": nan, "brief_content": nan}, R.Classification("ai_tech", "x", 1))


def test_non_promo_news_false_positives():
    assert R.deal_score("匯率跌至歷史低點", promo_source=False)[0] == 0
    assert R.deal_score("政府免費送口罩", promo_source=False)[0] == 0
    assert R.deal_score("黑五類人物", promo_source=False)[0] == 0
    assert R.deal_score("好市多黑五開搶", promo_source=False)[0] >= 2
