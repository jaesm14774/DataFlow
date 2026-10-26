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
        "taifex_market_option_positioning": {},
        "taifex_large_trader_context": {},
        "tpex_daytrade_market_context": {},
        "market_total_return_context": {},
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
