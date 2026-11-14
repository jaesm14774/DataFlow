from portfolio_alerts import (
    DEFAULT_ALERT_RULES,
    evaluate_record,
    normalize_stock_code,
    sanitize_discord_text,
)


def _rule(rule_key):
    return next(rule for rule in DEFAULT_ALERT_RULES if rule["rule_key"] == rule_key)


def test_normalize_stock_code_preserves_market_code_shape():
    assert normalize_stock_code("2330") == "2330"
    assert normalize_stock_code(2330) == "2330"
    assert normalize_stock_code("50") == "0050"
    assert normalize_stock_code("") is None


def test_material_event_requires_important_keyword():
    rule = _rule("held_company_material_event")
    important = {
        "title": "董事會決議現金增資",
        "value_text": "補充說明",
    }
    ordinary = {"title": "一般公告", "value_text": "例行資訊"}
    assert evaluate_record(important, rule) == "命中重要關鍵字：現金增資"
    assert evaluate_record(ordinary, rule) is None


def test_revenue_growth_alone_never_triggers_an_alert():
    """Reported revenue growth has no validated surprise definition, so it must not push."""
    rule = _rule("held_revenue_warning")
    for record, previous in (({"metric_value": -25.0}, None),
                             ({"metric_value": -3.0}, 8.0),
                             ({"metric_value": 12.0}, 10.0)):
        assert evaluate_record(record, rule, previous_revenue_yoy=previous) is None


def test_dividend_rule_notifies_new_record():
    rule = _rule("held_dividend_announcement")
    assert evaluate_record({}, rule) == "出現新的股利／除權息資料"


def test_discord_text_cannot_trigger_mentions():
    sanitized = sanitize_discord_text("@everyone 重大公告", max_chars=8)
    assert "@\u200b" in sanitized
    assert "@everyone" not in sanitized
