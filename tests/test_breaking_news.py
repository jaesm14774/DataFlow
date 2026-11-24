"""重磅消息輿情助手：評分、決策、解析、推播流程與反應計算（皆為離線純函式測試）。"""

from datetime import date, datetime, timedelta

import pytest

from breaking_news import pipeline, reaction, store
from breaking_news.policy import (
    DAILY_PUSH_CAP,
    HOURLY_PUSH_CAP,
    MAX_AGE_MINUTES,
    PoolEvent,
    decide,
    item_key,
)
from breaking_news.scoring import (
    CRITICAL_THRESHOLD,
    TIER_IGNORE,
    TIER_PUSH,
    TIER_WATCH,
    NewsItem,
    build_tokens,
    same_event,
    score_item,
)
from breaking_news.sources import SOURCES, SourceSpec, is_due, parse_feed, parse_pub_date

NOW = datetime(2026, 11, 24, 12, 0)


def make_item(title, source="wsj_markets", minutes_ago=5, guid=None):
    return NewsItem(
        source_key=source,
        guid=guid or f"{source}:{title}",
        title=title,
        url="https://example.com/a",
        published_at=NOW - timedelta(minutes=minutes_ago),
    )


def run_decide(items, *, seeded=frozenset(), pool=(), hourly=0, daily=0):
    pairs = [(item, score_item(item)) for item in items]
    return decide(
        pairs, now_utc=NOW, seeded_sources=set(seeded), pool=list(pool),
        pushed_last_hour=hourly, pushed_last_day=daily,
    )


# --- 解析 --------------------------------------------------------------------

RSS = """<?xml version="1.0"?>
<rss version="2.0"><channel>
<item><title><![CDATA[Fed raises rates &amp; more]]></title><link>https://x/1</link>
<guid>g1</guid><pubDate>Tue, 24 Nov 2026 07:46:33 +0800</pubDate>
<description>&lt;p&gt;hello&lt;/p&gt;</description></item>
<item><title>No date</title><link>https://x/2</link></item>
<item><title></title><link>https://x/3</link><pubDate>Tue, 24 Nov 2026 07:46:33 +0800</pubDate></item>
<item><title>No guid uses link</title><link>https://x/4</link>
<pubDate>Mon, 23 Nov 2026 22:25:00 GMT</pubDate></item>
</channel></rss>"""


def test_parse_feed_normalizes_dates_to_utc_and_skips_bad_items():
    items = parse_feed(RSS, "wsj_markets")
    assert [i.guid for i in items] == ["g1", "https://x/4"]
    assert items[0].published_at == datetime(2026, 11, 23, 23, 46, 33)  # +0800 → UTC
    assert items[0].title == "Fed raises rates & more"
    assert items[0].summary == "hello"
    assert items[1].published_at == datetime(2026, 11, 23, 22, 25, 0)


def test_parse_pub_date_rejects_garbage():
    assert parse_pub_date("not a date") is None
    assert parse_pub_date(None) is None
    assert parse_pub_date("Wed, 18 Nov 2026 19:00:00 GMT") == datetime(2026, 11, 18, 19, 0)


def test_slow_sources_poll_every_third_slot_and_fast_every_slot():
    slow = SourceSpec("google_news_en", "g", "u", poll_every_minutes=15)
    fast = SourceSpec("cna_finance", "c", "u")
    due = [is_due(slow, datetime(2026, 1, 1, 0, minute)) for minute in range(0, 60, 5)]
    assert due.count(True) == 4  # 每小時 4 次
    assert all(is_due(fast, datetime(2026, 1, 1, 0, minute)) for minute in range(0, 60, 5))


def test_source_keys_are_unique():
    keys = [s.key for s in SOURCES]
    assert len(keys) == len(set(keys))


# --- 評分 --------------------------------------------------------------------

def test_climate_rant_from_trump_is_ignored():
    text = (
        "EXTREME CLIMATE SCIENCE “BITES THE DUST!” Climate Change activists always described "
        "their assumptions as settled science. Thank you for your attention to this matter."
    )
    assert score_item(make_item(text, "trump_truth")).tier == TIER_IGNORE


def test_trump_tariff_on_china_post_reaches_push():
    scored = score_item(make_item("We will impose a 100% tariff on China starting Monday", "trump_truth"))
    assert scored.tier == TIER_PUSH
    assert "trade" in scored.categories


def test_trump_post_only_mentioning_context_words_is_ignored():
    scored = score_item(make_item("China and Nvidia are doing great, big beautiful chips!", "trump_truth"))
    assert scored.tier == TIER_IGNORE  # 只有背景類別，沒有觸發類別


def test_long_trump_post_only_scores_the_opening():
    filler = "Great people and a beautiful country. " * 12
    scored = score_item(make_item(filler + "tariff tariff sanctions missile", "trump_truth"))
    assert scored.tier == TIER_IGNORE


def test_fed_fomc_statement_pushes_but_routine_announcements_do_not():
    assert score_item(make_item("Federal Reserve issues FOMC statement", "fed_monetary")).tier == TIER_PUSH
    routine = score_item(make_item(
        "Federal Reserve announces revised tentative schedule of Federal Open Market Committee meetings",
        "fed_monetary",
    ))
    assert routine.tier == TIER_IGNORE
    task_force = score_item(make_item(
        "Federal Reserve announces the leadership and objectives of its task forces", "fed_monetary",
    ))
    assert task_force.tier == TIER_WATCH  # 命中貨幣政策但標題不是行情公告：最高 watch


def test_single_outlet_war_headline_is_watch_not_push():
    scored = score_item(make_item("Iran launches missile strikes on US bases in the Gulf", "bbc_world"))
    assert scored.tier == TIER_WATCH


def test_famous_ceo_return_is_detected_in_english_and_chinese():
    en = score_item(make_item("Jensen Huang reinstated as Nvidia CEO. Stock skyrockets 31%", "cnbc_top"))
    zh = score_item(make_item("黃仁勳重返輝達台指期夜盤飆漲", "cna_finance"))
    assert "leadership" in en.categories and "leadership" in zh.categories


def test_ceo_change_at_unknown_company_is_not_leadership():
    assert "leadership" not in score_item(make_item("Acme Widgets CEO steps down", "cnbc_top")).categories


def test_rate_and_inflation_talk_does_not_stack_into_push():
    scored = score_item(make_item("Fed official says interest rates may stay high as inflation persists", "cnbc_top"))
    assert scored.tier != TIER_PUSH


def test_summary_text_does_not_influence_score():
    item = NewsItem("wsj_markets", "g", "WSJ Dollar Index Rises 0.22% to 98.64", "u", NOW,
                    summary="Federal Reserve inflation interest rates sanctions war")
    assert score_item(item).tier == TIER_IGNORE


def test_taiwan_strait_conflict_gets_dedicated_category():
    scored = score_item(make_item("共軍在台海舉行軍演 飛彈試射", "cna_finance"))
    assert "taiwan_conflict" in scored.categories


def test_tokens_bridge_chinese_and_english_reports_of_same_event():
    zh = score_item(make_item("黃仁勳重返輝達 台指期夜盤飆漲", "cna_finance"))
    en = score_item(make_item("Jensen Huang reinstated as Nvidia CEO", "cnbc_top"))
    assert same_event(zh.tokens, en.tokens)
    other = score_item(make_item("Fed's Waller says half-point rate hike possible", "cnbc_top"))
    assert not same_event(zh.tokens, other.tokens)
    assert build_tokens("") == frozenset()


# --- 決策 --------------------------------------------------------------------

def test_first_run_seeds_without_pushing_anything():
    items = [make_item("Federal Reserve issues FOMC statement", "fed_monetary")]
    decisions = run_decide(items, seeded={"fed_monetary"})
    assert [d.decision for d in decisions] == ["seeded"]


def test_stale_items_are_recorded_but_never_pushed():
    old = make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=MAX_AGE_MINUTES + 5)
    assert run_decide([old])[0].decision == "stale"


def test_fresh_high_score_item_is_pushed():
    decision = run_decide([make_item("Federal Reserve issues FOMC statement", "fed_monetary")])[0]
    assert decision.decision == "push"


def test_same_event_from_many_outlets_is_pushed_once():
    items = [
        make_item("Jensen Huang reinstated as Nvidia CEO. Stock skyrockets", "cnbc_top", minutes_ago=6),
        make_item("Nvidia reinstates CEO Jensen Huang", "bbc_business", minutes_ago=5),
        make_item("黃仁勳重返輝達 台指期夜盤飆漲", "cna_finance", minutes_ago=4),
        make_item("Tech stocks rally after Jensen Huang returns as Nvidia CEO", "wsj_markets", minutes_ago=3),
    ]
    decisions = run_decide(items)
    assert [d.decision for d in decisions].count("push") == 1
    pushed = next(d for d in decisions if d.decision == "push")
    assert len(pushed.corroborating_sources) >= 2
    assert all(d.decision in {"watch", "push", "duplicate"} for d in decisions)


def test_two_publishers_upgrade_watch_to_push_but_one_publisher_two_feeds_do_not():
    one_publisher = run_decide([
        make_item("Iran launches missile strikes on US bases", "cnbc_top", minutes_ago=6),
        make_item("Iran launches missile strikes on US bases", "cnbc_finance", minutes_ago=5),
    ])
    assert "push" not in [d.decision for d in one_publisher]
    two_publishers = run_decide([
        make_item("Iran launches missile strikes on US bases", "cnbc_top", minutes_ago=6),
        make_item("Iran launches missile strikes on US bases", "bbc_world", minutes_ago=5),
    ])
    assert [d.decision for d in two_publishers] == ["watch", "push"]


def test_google_news_aggregator_does_not_count_as_corroboration():
    decisions = run_decide([
        make_item("Iran launches missile strikes on US bases", "bbc_world", minutes_ago=6),
        make_item("Iran launches missile strikes on US bases - Reuters", "google_news_en", minutes_ago=5),
    ])
    assert "push" not in [d.decision for d in decisions]


def test_already_pushed_event_in_ledger_suppresses_followups():
    first = run_decide([make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=30)])[0]
    pool = [PoolEvent(first.key, "fed_monetary", first.scored.tokens, first.scored.score,
                      True, True, first.item.published_at)]
    followup = make_item("Fed FOMC statement: Federal Reserve holds rates", "cnbc_top", minutes_ago=2)
    assert run_decide([followup], pool=pool)[0].decision == "duplicate"


def test_hourly_cap_limits_pushes_and_overflow_is_recorded_as_capped():
    items = [
        make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=4),
        make_item("Trump: new 50% tariff on Mexico effective today", "trump_truth", minutes_ago=3),
        make_item("Trump orders sanctions on Russian banks tonight", "trump_truth", minutes_ago=2),
        make_item("Trump bans chip exports with new export controls", "trump_truth", minutes_ago=1),
    ]
    decisions = run_decide(items, hourly=HOURLY_PUSH_CAP - 1)
    assert [d.decision for d in decisions].count("push") == 1
    assert "capped" in [d.decision for d in decisions]


def test_daily_cap_blocks_non_critical_pushes():
    decision = run_decide([make_item("Federal Reserve issues FOMC statement", "fed_monetary")],
                          daily=DAILY_PUSH_CAP)[0]
    assert decision.decision == "capped"


def test_single_source_cannot_bypass_caps_even_with_top_score():
    item = make_item("Taiwan Strait: missiles launched as war breaks out, blockade declared", "trump_truth")
    decision = run_decide([item], hourly=HOURLY_PUSH_CAP, daily=DAILY_PUSH_CAP)[0]
    assert decision.final_score >= CRITICAL_THRESHOLD
    assert decision.decision == "capped"  # 單一來源連發不能洗版


def test_critical_event_with_two_publishers_bypasses_caps():
    decisions = run_decide(
        [
            make_item("Taiwan Strait: missiles launched as war breaks out, blockade declared",
                      "trump_truth", minutes_ago=4),
            make_item("Missiles fired in Taiwan Strait as war breaks out, blockade declared",
                      "bbc_world", minutes_ago=3),
        ],
        hourly=HOURLY_PUSH_CAP, daily=DAILY_PUSH_CAP,
    )
    assert decisions[0].decision == "capped"
    assert decisions[1].decision == "push" and decisions[1].final_score >= CRITICAL_THRESHOLD


def test_fed_hold_and_fed_cut_are_different_events():
    hold = score_item(make_item("Fed holds interest rates steady, Powell says", "cnbc_top"))
    cut = score_item(make_item("Fed cuts interest rates by a quarter point, Powell says", "bbc_world"))
    assert "fed" in hold.tokens
    assert not same_event(hold.tokens, cut.tokens)
    zh_hold = score_item(make_item("聯準會維持不變 鮑爾：利率按兵不動", "cna_finance"))
    zh_cut = score_item(make_item("聯準會降息 鮑爾表示", "cna_finance"))
    assert not same_event(zh_hold.tokens, zh_cut.tokens)


def test_eu_and_canada_tariffs_are_different_events():
    eu = score_item(make_item("Trump threatens EU tariffs on autos", "cnbc_top"))
    canada = score_item(make_item("Trump announces Canada tariffs on steel", "bbc_world"))
    assert "europe" in eu.tokens
    assert not same_event(eu.tokens, canada.tokens)


def test_ceasefire_and_strike_on_same_country_are_different_events():
    strike = score_item(make_item("Iran strikes US bases in the Gulf", "cnbc_top"))
    truce = score_item(make_item("Iran agrees to ceasefire with US", "bbc_world"))
    assert not same_event(strike.tokens, truce.tokens)


def test_rehire_is_a_leadership_change():
    assert "leadership" in score_item(make_item("Nvidia to rehire Jensen Huang as CEO", "cnbc_top")).categories
    assert "leadership" in score_item(make_item("輝達宣布黃仁勳復職 執行長回鍋", "cna_finance")).categories


def test_stale_copy_does_not_swallow_a_fresh_report_of_the_same_topic():
    stale = make_item("Jensen Huang reinstated as Nvidia CEO", "cnbc_top", minutes_ago=MAX_AGE_MINUTES + 30)
    fresh_a = make_item("Nvidia reinstates CEO Jensen Huang", "bbc_business", minutes_ago=4)
    fresh_b = make_item("Tech stocks rally as Huang returns as Nvidia CEO", "wsj_markets", minutes_ago=2)
    decisions = run_decide([stale, fresh_a, fresh_b])
    assert [d.decision for d in decisions] == ["stale", "watch", "push"]


def test_duplicates_are_marked_ignore_tier_and_follow_the_push_anchor():
    anchor = make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=80)
    echo = make_item("Fed FOMC statement: Federal Reserve holds rates", "cnbc_top", minutes_ago=60)
    decisions = run_decide([anchor, echo])
    assert [d.decision for d in decisions] == ["push", "duplicate"]
    assert decisions[1].final_tier == TIER_IGNORE


def test_duplicate_is_not_an_anchor_for_later_items():
    # echo1 是 duplicate；late 與 echo1 相似、但與錨點相隔太遠 → 不能靠 echo1 接力被吞掉
    anchor = make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=80)
    echo = make_item("Fed FOMC statement: Federal Reserve holds rates", "cnbc_top", minutes_ago=30)
    pairs = [(item, score_item(item)) for item in (anchor, echo)]
    decisions = decide(pairs, now_utc=NOW, seeded_sources=set(), pool=[],
                       pushed_last_hour=0, pushed_last_day=0)
    assert decisions[1].decision == "duplicate"
    # 把 duplicate 之外的結果當作下一輪的池：duplicate 不在其中
    next_pool = [
        PoolEvent(d.key, d.item.source_key, d.scored.tokens, d.scored.score,
                  d.scored.triggered, True, d.item.published_at)
        for d in decisions if d.decision in {"push", "watch", "seeded"}
    ]
    assert [e.item_key for e in next_pool] == [decisions[0].key]


def test_seeded_fresh_item_anchors_but_stale_seeded_item_does_not():
    fresh_seed = make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=30)
    echo = make_item("Fed FOMC statement: Federal Reserve holds rates", "cnbc_top", minutes_ago=3)
    decisions = run_decide([fresh_seed, echo], seeded={"fed_monetary"})
    assert [d.decision for d in decisions] == ["seeded", "duplicate"]
    stale_seed = make_item("Federal Reserve issues FOMC statement", "fed_monetary",
                           minutes_ago=MAX_AGE_MINUTES + 30)
    decisions = run_decide([stale_seed, echo], seeded={"fed_monetary"})
    # 過期的 seeded 項目既不吞掉新稿，也不當作「第二家媒體印證」替它加分
    assert decisions[1].decision == "watch"
    assert decisions[1].corroborating_sources == ("cnbc",)


def test_different_countries_are_different_events():
    canada = score_item(make_item("Trump announces tariffs on Canada", "cnbc_top"))
    mexico = score_item(make_item("Trump announces tariffs on Mexico", "bbc_world"))
    assert not same_event(canada.tokens, mexico.tokens)
    decisions = run_decide([
        make_item("Trump announces tariffs on Canada", "cnbc_top", minutes_ago=5),
        make_item("Trump announces tariffs on Mexico", "bbc_world", minutes_ago=4),
    ])
    assert "duplicate" not in [d.decision for d in decisions]


def test_same_country_reports_still_match_across_languages():
    zh = score_item(make_item("川普宣布對加拿大課徵關稅", "cna_finance"))
    en = score_item(make_item("Trump announces tariffs on Canada", "cnbc_top"))
    assert same_event(zh.tokens, en.tokens)


def test_item_key_is_stable_and_source_scoped():
    a = make_item("x", "cnbc_top", guid="1")
    b = make_item("x", "bbc_world", guid="1")
    assert item_key(a) == item_key(a)
    assert item_key(a) != item_key(b)


# --- 訊息與推播流程 -----------------------------------------------------------

def test_format_alert_contains_headline_source_time_and_link():
    text = pipeline.format_alert(
        source_key="fed_monetary", title="Federal Reserve issues FOMC statement",
        url="https://fed/x", published_at=datetime(2026, 11, 24, 6, 0), score=7,
        categories="monetary", corroborating_sources="cnbc,fed",
    )
    assert "貨幣政策" in text and "Fed 貨幣政策" in text
    assert "11/24 14:00" in text  # UTC+8
    assert "2 家來源印證" in text and text.endswith("https://fed/x")


def test_format_alert_truncates_long_posts():
    text = pipeline.format_alert(
        source_key="trump_truth", title="tariff " * 200, url="", published_at=NOW,
        score=6, categories="trade", corroborating_sources="",
    )
    assert len(text) < 400


class FakeCursor:
    """依 SQL 開頭簡單分派的假 cursor，記錄寫入。"""

    def __init__(self, known=(), history=(), failed_rows=(), insert_rowcount=1,
                 hourly_pushes=0, daily_pushes=0):
        self.known, self.history = set(known), set(history)
        self.failed_rows = list(failed_rows)
        self.insert_rowcount = insert_rowcount
        self.hourly_pushes, self.daily_pushes = hourly_pushes, daily_pushes
        self.statements = []
        self.inserted = []
        self.updates = []
        self.rowcount = 1
        self._rows = []

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        self.statements.append(sql)
        self.rowcount = 1
        if sql.startswith("SELECT GET_LOCK"):
            self._rows = [(1,)]
        elif sql.startswith("SELECT item_key FROM"):
            self._rows = [(k,) for k in params if k in self.known]
        elif sql.startswith("SELECT DISTINCT source_key"):
            self._rows = [(s,) for s in params if s in self.history]
        elif sql.startswith("SELECT item_key, source_key, tokens"):
            self._rows = []
        elif sql.startswith("SELECT COUNT(*)"):
            # params = (decision, since, *statuses)：以 since 離現在多近判斷是小時窗還是日窗
            hourly = (NOW - params[1]) <= timedelta(hours=1, minutes=1)
            self._rows = [(self.hourly_pushes if hourly else self.daily_pushes,)]
        elif sql.startswith("SELECT item_key, source_key, title"):
            self._rows = list(self.failed_rows)
        elif sql.startswith("INSERT IGNORE"):
            self.inserted.append(params)
            self.rowcount = self.insert_rowcount
        elif sql.startswith("UPDATE"):
            self.updates.append((sql, params))
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return list(self._rows)


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        self.closed = True


class FakeEngine:
    def dispose(self):
        pass


class FakeNotifier:
    def __init__(self, result=None):
        self.sent = []
        self.result = result or {"success": True, "accepted": True, "receipt_id": "1", "http_status": 200}

    def notify_with_receipt(self, **kwargs):
        self.sent.append(kwargs["msg"])
        return self.result


def run_scan_with(items, *, cursor, notifier, errors=None, attempted=None):
    connection = FakeConnection()
    summary = pipeline.run_scan(
        now_utc=NOW,
        open_connection=lambda: (connection, cursor, FakeEngine()),
        fetch=lambda specs, now: (items, errors or {}, attempted or sorted({i.source_key for i in items})),
        notifier_factory=lambda: notifier,
    )
    return summary, connection


def test_run_scan_first_run_records_everything_and_sends_nothing():
    cursor, notifier = FakeCursor(), FakeNotifier()
    items = [make_item("Federal Reserve issues FOMC statement", "fed_monetary")]
    summary, connection = run_scan_with(items, cursor=cursor, notifier=notifier)
    assert notifier.sent == []
    assert summary["decisions"] == {"seeded": 1}
    assert len(cursor.inserted) == 1 and connection.closed


def test_run_scan_pushes_new_important_item_once_and_marks_receipt():
    cursor, notifier = FakeCursor(history={"fed_monetary"}), FakeNotifier()
    items = [make_item("Federal Reserve issues FOMC statement", "fed_monetary")]
    summary, _ = run_scan_with(items, cursor=cursor, notifier=notifier)
    assert len(notifier.sent) == 1 and "FOMC" in notifier.sent[0]
    assert summary["pushed"] == 1
    sent_update = [params for sql, params in cursor.updates if "push_receipt_id" in sql]
    assert sent_update and sent_update[0][0] == "sent" and sent_update[0][1] == "1"


def test_run_scan_skips_items_already_in_ledger():
    item = make_item("Federal Reserve issues FOMC statement", "fed_monetary")
    cursor, notifier = FakeCursor(known={item_key(item)}, history={"fed_monetary"}), FakeNotifier()
    summary, _ = run_scan_with([item], cursor=cursor, notifier=notifier)
    assert summary["new"] == 0 and notifier.sent == [] and cursor.inserted == []


def test_run_scan_unknown_delivery_outcome_is_not_retried():
    cursor = FakeCursor(history={"fed_monetary"})
    notifier = FakeNotifier({"success": False, "accepted": None, "outcome_unknown": True,
                             "http_status": None, "receipt_id": None})
    run_scan_with([make_item("Federal Reserve issues FOMC statement", "fed_monetary")],
                  cursor=cursor, notifier=notifier)
    assert [p[0] for s, p in cursor.updates if "push_receipt_id" in s] == ["delivery_unknown"]


def test_run_scan_raises_only_when_every_source_failed():
    cursor, notifier = FakeCursor(), FakeNotifier()
    with pytest.raises(RuntimeError):
        run_scan_with([], cursor=cursor, notifier=notifier,
                      errors={"a": "ConnectionError", "b": "HTTPError"}, attempted=["a", "b"])
    summary, _ = run_scan_with([], cursor=FakeCursor(), notifier=notifier,
                               errors={"a": "ConnectionError"}, attempted=["a", "b"])
    assert summary["errors"] == {"a": "ConnectionError"}


def test_delivery_status_mapping():
    assert store.delivery_status({"success": True, "receipt_id": "9"}) == "sent"
    assert store.delivery_status({"accepted": True, "success": False}) == "accepted_unverified"
    assert store.delivery_status({"outcome_unknown": True}) == "delivery_unknown"
    assert store.delivery_status({"accepted": False}) == "failed"


# --- 反應檢驗 -----------------------------------------------------------------

CLOSES = [
    (date(2026, 11, 20), 100.0),
    (date(2026, 11, 23), 102.0),
    (date(2026, 11, 24), 99.96),
    (date(2026, 11, 25), 104.0),
]


def test_reaction_uses_last_close_before_alert_and_next_two_closes():
    # 台北 11/23 20:00（收盤後）= UTC 12:00 → 基準 11/23 收盤 102
    result = reaction.compute_reaction(datetime(2026, 11, 23, 12, 0), CLOSES)
    assert result["ref_date"] == date(2026, 11, 23) and result["ref_close"] == 102.0
    assert result["post1_date"] == date(2026, 11, 24)
    assert result["post1_return_pct"] == pytest.approx(-2.0)
    assert result["post2_return_pct"] == pytest.approx((104 / 102 - 1) * 100, abs=1e-3)


def test_reaction_alert_during_session_uses_previous_close_as_reference():
    # 台北 11/24 10:00（盤中）= UTC 02:00 → 基準 11/23，第一個收盤是 11/24 當天
    result = reaction.compute_reaction(datetime(2026, 11, 24, 2, 0), CLOSES)
    assert result["ref_date"] == date(2026, 11, 23) and result["post1_date"] == date(2026, 11, 24)


def test_reaction_returns_none_without_following_close():
    assert reaction.compute_reaction(datetime(2026, 11, 26, 2, 0), CLOSES) is None
    assert reaction.compute_reaction(datetime(2026, 11, 1, 2, 0), CLOSES) is None


# --- 審查後補強的測試 -----------------------------------------------------------

def test_insert_ignore_conflict_is_not_sent():
    cursor, notifier = FakeCursor(history={"fed_monetary"}, insert_rowcount=0), FakeNotifier()
    summary, _ = run_scan_with([make_item("Federal Reserve issues FOMC statement", "fed_monetary")],
                               cursor=cursor, notifier=notifier)
    assert notifier.sent == [] and summary["pushed"] == 0


def test_notifier_failure_aborts_before_ledger_write():
    cursor = FakeCursor(history={"fed_monetary"})

    def broken_factory():
        raise KeyError("token row missing")

    connection = FakeConnection()
    fetched = [make_item("Federal Reserve issues FOMC statement", "fed_monetary")]
    with pytest.raises(KeyError):
        pipeline.run_scan(
            now_utc=NOW,
            open_connection=lambda: (connection, cursor, FakeEngine()),
            fetch=lambda specs, now: (fetched, {}, ["fed_monetary"]),
            notifier_factory=broken_factory,
        )
    assert cursor.inserted == []  # 警示沒有被寫成 pending 而遺失


FAILED_ROW = ("k" * 64, "fed_monetary", "Federal Reserve issues FOMC statement", "https://fed/x",
              NOW - timedelta(minutes=10), 6, "monetary", "fed")


def test_failed_push_is_retried_when_under_caps():
    cursor, notifier = FakeCursor(failed_rows=[FAILED_ROW]), FakeNotifier()
    summary, _ = run_scan_with([], cursor=cursor, notifier=notifier, attempted=["x"])
    assert summary["retried"] == 1 and len(notifier.sent) == 1


def test_retry_respects_hourly_cap_unless_critical():
    cursor = FakeCursor(failed_rows=[FAILED_ROW], hourly_pushes=HOURLY_PUSH_CAP)
    notifier = FakeNotifier()
    summary, _ = run_scan_with([], cursor=cursor, notifier=notifier, attempted=["x"])
    assert summary["retried"] == 0 and notifier.sent == []
    critical = FAILED_ROW[:5] + (CRITICAL_THRESHOLD,) + FAILED_ROW[6:]
    cursor = FakeCursor(failed_rows=[critical], hourly_pushes=HOURLY_PUSH_CAP)
    notifier = FakeNotifier()
    summary, _ = run_scan_with([], cursor=cursor, notifier=notifier, attempted=["x"])
    assert summary["retried"] == 1


def test_retry_skipped_when_claim_is_lost():
    class LostClaim(FakeCursor):
        def execute(self, sql, params=()):
            super().execute(sql, params)
            flat = " ".join(sql.split())
            if flat.startswith("UPDATE") and "AND push_status = %s" in flat:
                self.rowcount = 0

    cursor, notifier = LostClaim(failed_rows=[FAILED_ROW]), FakeNotifier()
    run_scan_with([], cursor=cursor, notifier=notifier, attempted=["x"])
    assert notifier.sent == []


def test_load_pool_excludes_duplicate_and_stale_anchors():
    cursor = FakeCursor()
    store.load_pool(cursor, NOW)
    sql = next(s for s in cursor.statements if s.startswith("SELECT item_key, source_key, tokens"))
    assert "NOT IN ('duplicate', 'stale')" in sql
    assert "decision <> 'seeded' OR published_at >=" in sql


def test_mark_push_only_updates_pending_rows():
    cursor = FakeCursor()
    store.mark_push(cursor, "k", {"success": True, "receipt_id": "1"}, NOW)
    assert "push_status = 'pending'" in cursor.statements[-1]


class SettleCursor:
    def __init__(self, pending, closes):
        self.pending, self.closes = pending, closes
        self.updates = []
        self._rows = []

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        if sql.startswith("SELECT item_key, published_at"):
            self._rows = self.pending
        elif sql.startswith("SELECT effective_date"):
            self._rows = self.closes
        elif sql.startswith("UPDATE"):
            self.updates.append((sql, params))

    def fetchall(self):
        return list(self._rows)


def test_settle_marks_unsettleable_old_rows_so_they_do_not_starve_the_batch():
    old = datetime(2026, 10, 1, 2, 0)  # 超過 10 天，而且沒有任何指數資料
    cursor = SettleCursor([("old", old)], [])
    result = reaction.settle_reactions(cursor, NOW)
    assert result["settled"] == 0
    assert any("SET reaction_settled_at = %s WHERE item_key" in sql for sql, _ in cursor.updates)


def test_settle_waits_for_second_close_but_settles_when_available():
    alert = datetime(2026, 11, 23, 12, 0)
    young = SettleCursor([("a", alert)], CLOSES[:3])  # 只有第 1 個收盤
    assert reaction.settle_reactions(young, datetime(2026, 11, 25, 6, 0))["settled"] == 0
    full = SettleCursor([("a", alert)], CLOSES)
    assert reaction.settle_reactions(full, datetime(2026, 11, 26, 6, 0))["settled"] == 1


def test_baseline_is_mean_absolute_daily_return():
    closes = [(date(2026, 11, 20), 100.0), (date(2026, 11, 23), 102.0), (date(2026, 11, 24), 99.96)]
    assert reaction.mean_abs_daily_return(closes) == pytest.approx(2.0, abs=1e-3)
    assert reaction.mean_abs_daily_return(closes[:1]) is None


# --- 調整：窗口 12 小時、升級才再推、評論稿降級、聚合來源不單獨推 ---------------

def test_same_story_hours_later_is_a_duplicate_unless_it_escalates():
    anchor_item = make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=80)
    anchor = score_item(anchor_item)
    pool = [PoolEvent("a" * 64, "fed_monetary", anchor.tokens, anchor.score, True, True,
                      NOW - timedelta(hours=8))]
    echo = make_item("Fed FOMC statement: Federal Reserve holds rates", "cnbc_top", minutes_ago=3)
    assert run_decide([echo], pool=pool)[0].decision == "duplicate"  # 8 小時前的錨點仍在窗口內
    # 弱錨點（分數 4）+ 之後分數 ≥ 錨點 + 2 且過門檻的單則 → 視為升級，可再推
    weak = score_item(make_item("Fed FOMC statement: Federal Reserve holds rates", "cnbc_top"))
    weak_pool = [PoolEvent("b" * 64, "cnbc_top", weak.tokens, 4, True, True, NOW - timedelta(hours=2))]
    escalation = make_item("Federal Reserve issues FOMC statement", "fed_monetary", minutes_ago=2)
    assert score_item(escalation).score >= 6
    assert run_decide([escalation], pool=weak_pool)[0].decision == "push"


def test_explainer_and_live_headlines_are_downgraded():
    plain = score_item(make_item("Fed cuts interest rates by a quarter point", "aljazeera"))
    explainer = score_item(make_item("Why the Fed cuts interest rates by a quarter point", "aljazeera"))
    live = score_item(make_item("Fed live: cuts interest rates by a quarter point", "aljazeera"))
    for downgraded in (explainer, live):
        assert downgraded.score == plain.score - 2
        assert "commentary:explainer-or-live" in downgraded.reasons


def test_google_news_never_pushes_by_itself_but_still_corroborates():
    decisions = run_decide([
        make_item("Iran launches missile strikes on US bases", "bbc_world", minutes_ago=6),
        make_item("Iran launches missile strikes on US bases - Reuters", "google_news_en", minutes_ago=5),
    ])
    assert [d.decision for d in decisions] == ["watch", "watch"]
    again = run_decide([
        make_item("Iran launches missile strikes on US bases", "bbc_world", minutes_ago=6),
        make_item("Iran launches missile strikes on US bases", "cnbc_top", minutes_ago=5),
        make_item("Iran launches missile strikes on US bases - Reuters", "google_news_en", minutes_ago=4),
    ])
    assert [d.decision for d in again] == ["watch", "push", "duplicate"]
