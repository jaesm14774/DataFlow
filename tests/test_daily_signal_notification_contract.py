import datetime as dt
import json

from source_extension.market_signals import (
    DAILY_DIGEST_LIMIT,
    DISCORD_EMBED_COUNT_LIMIT,
    DISCORD_REQUEST_BYTE_LIMIT,
    _discord_request_size,
    _format_daily_embeds,
    _format_daily_message,
    _split_discord_embed_batches,
)


def test_user_receives_all_selected_daily_cards_when_background_context_is_long():
    """User receives a readable digest.

    Given a daily signal scan includes long background and source-quality details
    When the digest is formatted into Discord requests
    Then every selected candidate card and background detail fits the request budget.
    """
    candidates = []
    for index in range(8):
        direction = "偏多" if index < 6 else "偏空"
        sign = 1 if direction == "偏多" else -1
        details = {
            "direction": direction,
            "family_directions": {"price": sign, "institutions": sign, "revenue": sign},
            "signals": {
                "price_5d_return_pct": 3.2 * sign,
                "institutional_net_intensity_pct": 12.0 * sign,
                "revenue_yoy_pct": 15.0 * sign,
            },
            "price_levels": {
                "return_5d_pct": 3.2 * sign,
                "benchmark_return_5d_pct": 1.0,
                "relative_5d_return_pp": 2.2 * sign,
            },
        }
        candidates.append({
            "candidate_hash": f"candidate-{index}",
            "entity_id": f"{2330 + index}",
            "security_name": f"測試公司{index}",
            "market": "上市",
            "industry": "半導體業",
            "as_of_date": dt.date(2026, 10, 22),
            "signal_count": 3,
            "signal_keys": "price_5d_return_pct,institutional_net_intensity_pct,revenue_yoy_pct",
            "score": 3.0,
            "details_text": json.dumps(details, ensure_ascii=False),
        })

    run = {
        "as_of_date": dt.date(2026, 10, 22),
        "source_status": "partial",
        "universe_count": 1952,
        "price_count": 1915,
        "flow_count": 1826,
        "revenue_count": 1948,
        "financial_count": 1905,
        "borrow_count": 1797,
        "insider_count": 1939,
        "dividend_count": 1946,
        "candidate_count": len(candidates),
        "family_trigger_counts": {"price": 171, "volume": 56, "institutions": 777,
                                  "broker_branches": 77},
        "candidate_filter_counts": {"evaluated_stocks": 1929,
                                    "below_three_directional_families": 1907},
        "market_coverage_by_market": {
            "上市": {"universe_count": 1056, "price_count": 1047,
                    "price_coverage_pct": 99.1, "flow_count": 1039,
                    "flow_coverage_pct": 98.4, "price_row_code_count": 1053,
                    "price_invalid_close_code_count": 6, "price_no_trade_code_count": 0},
            "上櫃": {"universe_count": 896, "price_count": 868,
                    "price_coverage_pct": 96.9, "flow_count": 787,
                    "flow_coverage_pct": 87.8, "price_row_code_count": 876,
                    "price_invalid_close_code_count": 0, "price_no_trade_code_count": 8},
        },
        "benchmark_alignment_by_market": {
            market: {"latest_effective_date": "2026-10-22",
                     "latest_valid_effective_date": "2026-10-22",
                     "aligned_to_scan_date": True,
                     "window_point_count": 6, "window_valid_point_count": 6}
            for market in ("上市", "上櫃")
        },
        "market_breadth_context": {"markets": {
            "上市": {"quote_count": 1053, "accepted_count": 1053,
                    "universe_count": 1056, "quarantined_count": 0,
                    "advancers": 500, "decliners": 450, "unchanged": 100,
                    "unclassified": 3},
            "上櫃": {"quote_count": 876, "accepted_count": 876,
                    "universe_count": 896, "quarantined_count": 0,
                    "advancers": 400, "decliners": 350, "unchanged": 110,
                    "unclassified": 16},
        }},
        "taifex_market_positioning": {},
        "source_issues": [f"來源待核：公告日期或有效值待人工核對 {i}；" + "需要分開查明日期語義與市場覆蓋" * 4
                          for i in range(7)],
    }

    message = _format_daily_message(run, candidates)
    embeds = _format_daily_embeds(run, candidates)
    batches = _split_discord_embed_batches(message, embeds)

    delivered_cards = [
        embed["title"].split()[1]
        for batch in batches
        for embed in batch
        if embed.get("title", "").endswith(("偏多觀察", "偏空觀察", "訊號分歧觀察"))
    ]
    assert set(delivered_cards) == {candidate["entity_id"] for candidate in candidates}
    assert len(batches) >= 1
    for batch_index, batch in enumerate(batches, start=1):
        request_message = f"{message}｜第 {batch_index}/{len(batches)} 批"
        assert len(batch) <= DISCORD_EMBED_COUNT_LIMIT
        assert _discord_request_size(request_message, batch) <= DISCORD_REQUEST_BYTE_LIMIT
    assert DAILY_DIGEST_LIMIT >= len(batches)


def _plain_run(**overrides):
    run = {
        "as_of_date": dt.date(2026, 11, 6),
        "source_status": "partial",
        "universe_count": 1955,
        "market_coverage_by_market": {
            "上市": {"universe_count": 1058, "price_coverage_pct": 99.6,
                    "flow_coverage_pct": 99.2},
            "上櫃": {"universe_count": 897, "price_coverage_pct": 97.5,
                    "flow_coverage_pct": 89.3},
        },
        "market_breadth_context": {"markets": {
            "上市": {"quote_count": 1056, "advancers": 101, "decliners": 928,
                    "unchanged": 27},
            "上櫃": {"quote_count": 892, "advancers": 104, "decliners": 733,
                    "unchanged": 38},
        }},
        "market_index_moves": {
            "上市": {"date": dt.date(2026, 11, 6), "close": 39701.64,
                    "change_pct": -4.51},
            "上櫃": {"date": dt.date(2026, 11, 6), "close": 304.02,
                    "change_pct": -5.43},
        },
        "signal_scoreboard": {"days": 15, "rows": {
            ("偏多", 5): {"n": 1338, "hit_pct": 60.0, "avg_excess_pp": 1.9},
        }},
        "source_issues": [
            "TWSE margin｜任務成功，但有隔離列｜批次列 正規化/接受/隔離/重複 1043/0/1043/0",
            "TAIFEX｜HTTP 200｜後處理失敗（ValueError）",
        ],
    }
    run.update(overrides)
    return run


def test_summary_leads_with_market_breadth_and_hides_run_logs():
    """User sees up/flat/down counts and a verdict, never pipeline diagnostics."""
    embeds = _format_daily_embeds(_plain_run(), [])
    text = json.dumps(embeds, ensure_ascii=False)

    assert "▲ **101**" in embeds[0]["description"]
    assert "▼ **928**" in embeds[0]["description"]
    assert "＝ 27" in embeds[0]["description"]
    assert "多數個股下跌" in embeds[0]["description"]
    assert "加權 **39,701.64** ▼ 4.51%" in embeds[0]["description"]
    assert "今日結論" in embeds[0]["description"]
    for log_noise in ("批次列", "HTTP", "ValueError", "隔離", "來源待核",
                      "Airflow", "DAG run", "版本留存"):
        assert log_noise not in text
    assert len(embeds) == 1


def test_summary_states_backtest_sample_size_and_single_data_caveat():
    embeds = _format_daily_embeds(_plain_run(), [])
    names = [field["name"] for field in embeds[0]["fields"]]
    scoreboard = next(f["value"] for f in embeds[0]["fields"] if "成績單" in f["name"])
    caveat = next(f["value"] for f in embeds[0]["fields"] if "資料提醒" in f["name"])

    assert names.count("⚠️ 資料提醒") == 1
    assert "上櫃法人買賣超僅涵蓋 89%" in caveat
    assert "上市" not in caveat
    assert "尚不能證明有效" in scoreboard
    assert "15 個訊號日" in scoreboard


def test_single_batch_digest_has_no_part_marker_budget_problem():
    embeds = _format_daily_embeds(_plain_run(), [])
    batches = _split_discord_embed_batches(_format_daily_message(_plain_run(), []), embeds)
    assert len(batches) == 1


def test_missing_breadth_is_stated_not_zero():
    run = _plain_run(market_breadth_context={"markets": {}})
    embeds = _format_daily_embeds(run, [])
    assert "尚未取得同日行情" in embeds[0]["description"]


def test_partial_source_status_is_stated_in_plain_words_when_no_specific_gap():
    run = _plain_run(market_coverage_by_market={}, source_status="partial")
    caveat = next(f["value"] for f in _format_daily_embeds(run, [])[0]["fields"]
                  if "資料提醒" in f["name"])
    assert "缺資料的個股會略過" in caveat


def test_unknown_source_status_warns_that_list_is_not_usable():
    run = _plain_run(market_coverage_by_market={}, source_status="unknown")
    caveat = next(f["value"] for f in _format_daily_embeds(run, [])[0]["fields"]
                  if "資料提醒" in f["name"])
    assert "暫不可作為判讀依據" in caveat


def test_candidate_cards_drop_methodology_provenance_and_links():
    from source_extension.market_signals import _daily_candidate_embed

    details = {
        "direction": "偏空",
        "family_directions": {"price": -1, "institutions": -1, "revenue": -1},
        "signals": {"price_5d_return_pct": -9.0},
        "price_levels": {"return_5d_pct": -9.0, "benchmark_return_5d_pct": -2.0,
                          "relative_5d_return_pp": -7.0, "benchmark_code": "TAIEX"},
        "valuation_context": {"effective_date": "2026-11-06", "pe_ratio": 12.0,
                              "source_url": "https://www.twse.com.tw/x"},
        "balance_sheet_context": {"period": "2026Q2", "current_ratio": 2.0,
                                  "source_url": "https://www.twse.com.tw/y"},
    }
    row = {"entity_id": "2328", "security_name": "測試", "industry": "電子",
           "as_of_date": dt.date(2026, 11, 6)}
    embed = _daily_candidate_embed(row, details)
    text = json.dumps(embed, ensure_ascii=False)

    assert embed["title"].endswith("偏空觀察")
    assert "](http" not in text
    assert "至少 3 個有效資料家族" not in text
    assert "資產負債脈絡" not in text
    assert "法人與分點資料" not in text
    assert len(text) < 2500


def test_compaction_keeps_facts_inside_annotated_parentheses():
    from source_extension.market_signals import _compact_candidate_embed

    embed = {
        "title": "t", "description": "資料日 2026/11/06",
        "fields": [{
            "name": "相反訊號與風險",
            "value": "• 董監設質 35.0%（2026/10 期、3 位；背景，不計分）\n• 股利（背景，不計分）",
        }],
        "footer": {"text": "x｜較早行情取既有股票表"},
    }
    out = _compact_candidate_embed(embed)
    value = out["fields"][0]["value"]

    assert "2026/10 期、3 位" in value
    assert "不計分" not in value
    assert "較早行情" in out["footer"]["text"]


def test_discord_request_budget_stays_below_the_size_discord_rejected():
    """Discord webhooks reject large bodies with HTTP 413 even under the embed character limit.

    Given CJK embeds that are under the 5,500-character budget but ~6 bytes per character once serialized
    When they are split into Discord requests
    Then no request exceeds the 10,000-byte budget that has delivered successfully
    (a 22,586-byte request was rejected in production, and 13,624 bytes before that).
    """
    embeds = [
        {
            "title": f"{2300 + index} 測試公司｜偏多",
            "description": "重大訊息",
            "fields": [
                {"name": "公告內容重點", "value": "年增率與營收說明" * 100, "inline": False},
            ],
            "footer": {"text": "TWSE 上市重大訊息"},
        }
        for index in range(8)
    ]
    batches = _split_discord_embed_batches("⚠️ 台股訊號雷達", embeds)

    assert len(batches) > 1
    assert sum(len(batch) for batch in batches) == len(embeds)
    for batch in batches:
        assert _discord_request_size("⚠️ 台股訊號雷達", batch) <= DISCORD_REQUEST_BYTE_LIMIT
