import datetime as dt

import pandas as pd
import pytest

from source_extension import market_signals, validated_signals

AS_OF = dt.date(2026, 11, 13)


def _params():
    rule = next(
        r for r in validated_signals.active_rules(validated_signals.load_registry())
        if r["id"] == validated_signals.BREAKOUT_RULE_ID
    )
    return rule["params"]


def _prices(final_overrides, sessions=75):
    """Flat 100-yuan stocks with 10,000 lots/day, then override the final session."""
    days = pd.bdate_range(end=pd.Timestamp(AS_OF), periods=sessions)
    rows = []
    codes = {"2330", "2317", "1101", "0050", "9999", "2603"}
    for code in codes:
        for index, day in enumerate(days):
            rows.append([day, code, 100.0 + (index % 3) * 0.1, 10_000.0])
    frame = pd.DataFrame(rows, columns=["date", "code", "close", "vol"])
    for code, (close, vol) in final_overrides.items():
        mask = (frame["code"] == code) & (frame["date"] == days[-1])
        frame.loc[mask, ["close", "vol"]] = [close, vol]
    return frame


def test_user_only_gets_stocks_that_break_out_with_heavy_volume():
    """Given one stock breaks its 60-day high on 3x volume, one only jumps on volume,
    one breaks out on normal volume, and 0050 is an ETF
    When the as-of session is scanned
    Then only the true breakout (4-digit stock) is a hit."""
    prices = _prices({
        "2330": (120.0, 30_000.0),   # new high + 3x volume -> hit
        "2317": (99.0, 40_000.0),    # volume only, no new high
        "1101": (130.0, 10_000.0),   # new high, normal volume
        "0050": (150.0, 50_000.0),   # ETF excluded by 4-digit rule
    })
    detection = validated_signals.detect_breakouts(prices, AS_OF, _params())
    assert detection["status"] == "ok"
    assert [hit["code"] for hit in detection["hits"]] == ["2330"]
    assert detection["hits"][0]["volume_ratio"] == pytest.approx(3.0, rel=0.05)
    assert detection["breadth"]["stocks"] == 5  # 4-digit liquid names only


def test_illiquid_breakouts_are_not_pushed():
    prices = _prices({"9999": (140.0, 50_000.0)})
    prices.loc[prices["code"] == "9999", "vol"] = prices.loc[prices["code"] == "9999", "vol"] / 1000
    prices.loc[(prices["code"] == "9999") & (prices["date"] == prices["date"].max()), "vol"] = 50.0
    detection = validated_signals.detect_breakouts(prices, AS_OF, _params())
    assert "9999" not in [hit["code"] for hit in detection["hits"]]


def test_missing_as_of_prices_is_data_not_ready_not_no_signal():
    prices = _prices({})
    prices = prices[prices["date"] < pd.Timestamp(AS_OF)]
    detection = validated_signals.detect_breakouts(prices, AS_OF, _params())
    assert detection["status"] == "data_not_ready"
    message, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), detection
    )
    text = message + "".join(e["title"] + e["description"] for e in embeds)
    assert "價格資料尚未更新" in text
    assert "今日無通過驗證的訊號" not in text
    assert "通過驗證的訊號 0 檔" not in text


def test_quiet_day_is_one_plain_line_not_a_failure():
    detection = validated_signals.detect_breakouts(_prices({}), AS_OF, _params())
    message, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), detection
    )
    titles = [e["title"] for e in embeds]
    assert "今日無通過驗證的訊號" in titles
    assert "未能產出" not in message + "".join(e["description"] for e in embeds)
    assert "通過驗證的訊號 0 檔" in message


def test_digest_with_hits_states_evidence_limits_and_fits_discord_budget():
    hits = [
        {"code": f"{1000 + i}", "close": 50.0 + i, "volume_ratio": 8.0 - i * 0.2, "day_return_pct": 6.5}
        for i in range(12)
    ]
    detection = {"status": "ok", "hits": hits,
                 "breadth": {"stocks": 1500, "up": 800, "down": 600, "median_return_pct": 0.3}}
    names = {h["code"]: "測試公司" * 3 for h in hits}
    message, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), detection, names
    )
    text = "".join(e["description"] for e in embeds)
    assert "通過驗證的訊號 12 檔" in message
    assert text.count("**1000 ") + text.count("**1001 ") == 2
    assert "另有 7 檔" in text  # 12 hits, 5 shown
    assert "不是明牌" in text and "不構成投資建議" in text
    assert "+0.17%" in text  # after-cost edge is disclosed, not just the headline
    assert market_signals._discord_request_size(message, embeds) <= market_signals.DISCORD_REQUEST_BYTE_LIMIT
    assert len(market_signals._split_discord_embed_batches(message, embeds)) == 1


def test_retired_rule_cannot_push_names():
    registry = validated_signals.load_registry()
    for rule in registry["rules"]:
        rule["status"] = "retired"
    hits = [{"code": "2330", "close": 100.0, "volume_ratio": 5.0, "day_return_pct": 5.0}]
    message, embeds = validated_signals.build_validated_digest(
        AS_OF, registry, {"status": "ok", "hits": hits, "breadth": {}}
    )
    text = message + "".join(e["description"] + e["title"] for e in embeds)
    assert "2330" not in text
    assert "通過驗證的訊號 0 檔" in message


class _Closable:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    def dispose(self):
        self.closed = True


def test_capture_failure_notice_uses_ledger_dedupe_so_retries_do_not_repeat(monkeypatch):
    conn, cur, engine = _Closable(), object(), _Closable()
    seen = {}
    monkeypatch.setattr(market_signals, "_open_connection", lambda: (conn, cur, engine))
    monkeypatch.setattr(market_signals, "create_schema", lambda cursor: None)

    def fake_send(payload, notifier, *, cursor=None, connection=None):
        seen.update(cursor=cursor, connection=connection)
        return True

    monkeypatch.setattr(market_signals, "_send_failure_notice", fake_send)
    assert market_signals._send_capture_failure_notice({"run_key": "abc"}, object()) is True
    assert seen["cursor"] is cur and seen["connection"] is conn
    assert conn.closed and engine.closed


def test_capture_failure_notice_still_sends_when_ledger_is_unreachable(monkeypatch):
    def boom():
        raise ConnectionError("db down")

    seen = {}
    monkeypatch.setattr(market_signals, "_open_connection", boom)
    monkeypatch.setattr(
        market_signals, "_send_failure_notice",
        lambda payload, notifier, *, cursor=None, connection=None: seen.update(cursor=cursor) or True,
    )
    assert market_signals._send_capture_failure_notice({"run_key": "abc"}, object()) is True
    assert seen["cursor"] is None


def test_partial_price_landing_is_not_reported_as_no_signal():
    """Only a few names landed for the as-of session -> not ready, never 'no signal'."""
    prices = _prices({})
    last = prices["date"].max()
    keep = prices[(prices["date"] < last) | (prices["code"] == "2330")]
    detection = validated_signals.detect_breakouts(keep, AS_OF, _params())
    assert detection["status"] == "data_not_ready"


def test_zero_prior_volume_cannot_create_infinite_ratio_hit():
    prices = _prices({"2330": (150.0, 20_000.0)})
    mask = (prices["code"] == "2330") & (prices["date"] < prices["date"].max())
    prices.loc[mask, "vol"] = 0.0
    detection = validated_signals.detect_breakouts(prices, AS_OF, _params())
    assert all(hit["volume_ratio"] != float("inf") for hit in detection["hits"])


def test_daily_dedupe_key_cannot_collide_with_legacy_or_not_ready_message():
    legacy = market_signals._daily_digest_dedupe_key("2026-11-13", 1, 1)
    ok = market_signals._daily_digest_dedupe_key("2026-11-13", 1, 1, status="ok", version="validated_v1")
    late = market_signals._daily_digest_dedupe_key(
        "2026-11-13", 1, 1, status="data_not_ready", version="validated_v1"
    )
    assert len({legacy, ok, late}) == 3


def test_intraday_failure_notice_is_silent_while_intraday_push_is_off(monkeypatch):
    monkeypatch.setattr(market_signals, "INTRADAY_PUSH_ENABLED", False)
    sent = []

    class Notifier:
        def notify_with_receipt(self, **kwargs):
            sent.append(kwargs)
            return {"success": True, "receipt_id": "x"}

    payload = {"scan_mode": "intraday", "run_key": "r1", "error_message": "boom"}
    assert market_signals._send_failure_notice(payload, Notifier()) is True
    assert market_signals._send_capture_failure_notice(payload, Notifier()) is True
    assert sent == []
    daily = dict(payload, scan_mode="daily")
    assert market_signals._send_failure_notice(daily, Notifier()) is True
    assert len(sent) == 1


def test_tracking_failure_is_visible_in_the_digest_not_silent(monkeypatch):
    class Conn:
        rolled_back = False

        def rollback(self):
            Conn.rolled_back = True

    def boom(*args, **kwargs):
        raise ConnectionError("ledger down")

    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", boom)
    rule = next(iter(validated_signals.active_rules(validated_signals.load_registry())))
    embeds = market_signals._update_validated_tracking(
        object(), Conn(), object(), rule=rule, registry=validated_signals.load_registry(),
        as_of_date=AS_OF, detection={"status": "ok", "hits": [], "breadth": {}}, names={},
    )
    assert embeds[0]["title"] == "上線追蹤暫時無法更新"
    assert Conn.rolled_back


def test_tracking_is_not_recorded_while_prices_are_not_ready(monkeypatch):
    calls = []
    monkeypatch.setattr(
        market_signals.validated_tracking, "record_picks", lambda *a, **k: calls.append("record")
    )
    monkeypatch.setattr(
        market_signals.validated_tracking, "settle_pending_picks", lambda *a, **k: calls.append("settle")
    )
    monkeypatch.setattr(
        market_signals.validated_tracking, "live_summary",
        lambda cursor, rule_id: validated_tracking_summary(),
    )

    class Conn:
        def commit(self):
            calls.append("commit")

    rule = next(iter(validated_signals.active_rules(validated_signals.load_registry())))
    embeds = market_signals._update_validated_tracking(
        object(), Conn(), object(), rule=rule, registry=validated_signals.load_registry(),
        as_of_date=dt.date(2026, 11, 12),  # a Thursday: no weekly scorecard
        detection={"status": "data_not_ready", "hits": [], "breadth": {}}, names={},
    )
    assert calls == []
    assert embeds[0]["title"] == "上線後成績（不是回測）"


def validated_tracking_summary():
    from source_extension import validated_tracking

    return validated_tracking.summarize_excess([], pending=3)
