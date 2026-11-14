import datetime as dt

import pytest

from source_extension import market_signals, rule_lifecycle, signal_rules, validated_signals, validated_tracking

AS_OF = dt.date(2026, 11, 13)  # a Friday


def _evidence(horizon=10, after_cost=0.17):
    return {
        "horizon": horizon, "oos_dates": 886, "mean_excess_pct": after_cost + 0.6,
        "mean_excess_after_cost_pct": after_cost, "t_nw": 3.0, "win_rate_pct": 41.0,
        "base_win_rate_pct": 39.0, "years_total": 11, "years_positive": 9,
        "recent_reference": {"dates": 63, "mean_excess_after_cost_pct": -0.19},
    }


def _states(active=("breakout_60d_volume",), max_names=5):
    states = []
    for rule in signal_rules.RULES:
        states.append({
            "id": rule.id, "label": rule.label, "meaning": rule.meaning,
            "metric_label": rule.metric_label, "metric_format": rule.metric_format,
            "sources": rule.sources, "is_control": rule.is_control,
            "status": "active" if rule.id in active else "candidate", "horizon_sessions": 10,
            "evidence": _evidence(), "from_database": True,
            "params": {"max_names_shown": max_names, "min_turnover_ntd": 50_000_000},
        })
    return states


def _hits(count, offset=0):
    return [
        {"code": f"{1000 + offset + i}", "close": 50.0 + i, "day_return_pct": 6.5, "metric": 8.0 - i * 0.1}
        for i in range(count)
    ]


def _detection(rule_hits, status="ok"):
    rules = {
        rule.id: {"status": "ok", "missing_feeds": [], "hits": rule_hits.get(rule.id, [])}
        for rule in signal_rules.RULES
    }
    return {"status": status, "rules": rules,
            "breadth": {"stocks": 1500, "up": 800, "down": 600, "median_return_pct": 0.3}}


def _digest(states, detection, **kwargs):
    return validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), detection, states, **kwargs
    )


def _text(message, embeds):
    return message + "".join(e["title"] + e["description"] for e in embeds)


def test_missing_prices_say_not_ready_never_no_signal():
    detection = {"status": "data_not_ready", "rules": {}, "breadth": {}}
    message, embeds = _digest(_states(), detection)
    text = _text(message, embeds)
    assert "價格資料尚未更新" in text
    assert "今日沒有符合" not in text


def test_quiet_day_is_one_plain_line_not_a_failure():
    message, embeds = _digest(_states(), _detection({}))
    titles = [e["title"] for e in embeds]
    assert "今日沒有符合推播中規則的股票" in titles
    assert "未能產出" not in _text(message, embeds)
    assert "推播中規則的訊號 0 檔" in message


def test_a_rule_whose_feed_is_late_is_reported_as_waiting_not_as_no_hit():
    detection = _detection({})
    detection["rules"]["breakout_60d_volume"] = {"status": "data_not_ready", "missing_feeds": ["price"], "hits": []}
    message, embeds = _digest(_states(), detection)
    text = _text(message, embeds)
    assert "今天不判斷，不代表沒有訊號" in text
    assert "今日沒有符合" not in text


def test_digest_states_long_term_evidence_and_labels_the_recent_quarter_as_reference():
    hits = _hits(12)
    message, embeds = _digest(
        _states(), _detection({"breakout_60d_volume": hits}),
        names={h["code"]: "測試公司" * 3 for h in hits}, evidence_as_of=dt.date(2026, 11, 14),
    )
    text = _text(message, embeds)
    assert "目前長期證據（截至 2026-11-14）" in text
    assert "通過驗證" not in text  # status is a standing long-term record, not a one-off pass
    assert "最近一季" in text and "不影響規則去留" in text
    assert sum(1 for h in hits if f"**{h['code']} " in text) == 5
    assert "另有 7 檔" in text
    assert "不是明牌" in text and "不構成投資建議" in text
    assert "+0.17%" in text  # the after-cost edge is disclosed, not just the headline
    assert market_signals._discord_request_size(message, embeds) <= market_signals.DISCORD_REQUEST_BYTE_LIMIT
    assert len(market_signals._split_discord_embed_batches(message, embeds)) == 1


def test_only_active_rules_can_push_names():
    hits = _hits(3)
    states = _states(active=())
    message, embeds = _digest(states, _detection({rule.id: hits for rule in signal_rules.RULES}))
    text = _text(message, embeds)
    assert "1000" not in text and "目前沒有推播中的規則" in text
    assert "推播中規則的訊號 0 檔" in message


def test_a_suspended_rule_stops_pushing_immediately():
    states = _states()
    states[4]["status"] = rule_lifecycle.SUSPENDED
    assert states[4]["id"] == "breakout_60d_volume"
    message, embeds = _digest(states, _detection({"breakout_60d_volume": _hits(2)}))
    assert "1000" not in _text(message, embeds)


def test_a_normal_digest_fits_one_discord_request_on_the_wire_encoding():
    """The sender measures requests with every CJK character escaped; the digest must pass that test."""
    hits = _hits(12)
    states = _states(active=("breakout_60d_volume", "foreign_trust_both_buy_3d"))
    detection = _detection({"breakout_60d_volume": hits, "foreign_trust_both_buy_3d": _hits(8, offset=40)})
    message, embeds = _digest(states, detection, names={h["code"]: "測試公司名稱" for h in hits})
    assert market_signals._discord_request_size(message, embeds) <= market_signals.DISCORD_REQUEST_BYTE_LIMIT
    assert len(market_signals._split_discord_embed_batches(message, embeds)) == 1


def test_all_twelve_rules_active_with_many_hits_still_sends_without_any_oversized_embed():
    states = _states(active=[rule.id for rule in signal_rules.RULES])
    detection = _detection({rule.id: _hits(15, offset=i * 20) for i, rule in enumerate(signal_rules.RULES)})
    names = {h["code"]: "測試公司名稱很長" for i in range(12) for h in _hits(15, offset=i * 20)}
    live = {s["id"]: validated_tracking.summarize_by_date(
        [(AS_OF - dt.timedelta(days=d), 0.4, 3) for d in range(1, 40)], 10) for s in states}
    card = {
        "before_per_week": {"messages": 30.0, "failed": 2.0, "failure_notices": 1.0},
        "after_week": {"messages": 6, "failed": 0, "failure_notices": 0},
        "live": live, "shadow": validated_tracking.summarize_excess([], pending=4),
    }
    message, embeds = _digest(
        states, detection, names=names,
        tracking_embeds=[validated_tracking.scorecard_embed(card, states)],
        transitions=[{"rule_id": "breakout_60d_volume", "from": "candidate", "to": "active",
                      "reason": "長期證據通過完整門檻", "decided_on": AS_OF}],
    )
    # too much for one request, but the sender must be able to split it (it raises on a single oversized embed)
    batches = market_signals._split_discord_embed_batches(message, embeds)
    assert len(batches) >= 2
    for batch in batches:
        assert market_signals._discord_request_size(message, batch) <= market_signals.DISCORD_REQUEST_BYTE_LIMIT
    assert "規則狀態變更" in _text(message, embeds)


def test_long_embeds_are_split_on_line_boundaries_without_losing_text():
    lines = [f"line {i} " + "字" * 40 for i in range(60)]
    parts = validated_signals.split_long_embeds([{"title": "T", "description": chr(10).join(lines)}])
    assert len(parts) > 1 and parts[1]["title"].startswith("T（續")
    assert chr(10).join(p["description"] for p in parts) == chr(10).join(lines)
    assert all(len(p["description"]) <= validated_signals.EMBED_MAX_CHARS for p in parts)


def test_unreadable_rule_status_is_disclosed_not_silently_replaced_by_the_seed():
    states = _states()
    for state in states:
        state["status_source"] = "fallback_error"
    message, embeds = _digest(states, _detection({}))
    assert "規則狀態讀取失敗" in [e["title"] for e in embeds]


def test_status_changes_are_announced_in_plain_words():
    transitions = [{"rule_id": "breakout_60d_volume", "from": "active", "to": "suspended",
                    "reason": "長期扣成本後超額報酬轉為負", "decided_on": AS_OF}]
    message, embeds = _digest(_states(), _detection({}), transitions=transitions)
    change = next(e for e in embeds if e["title"] == "規則狀態變更")
    assert "推播中" in change["description"] and "暫停推播" in change["description"]


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


def test_daily_dedupe_key_cannot_collide_with_legacy_or_not_ready_message():
    legacy = market_signals._daily_digest_dedupe_key("2026-11-13", 1, 1)
    ok = market_signals._daily_digest_dedupe_key("2026-11-13", 1, 1, status="ok", version="validated_v2")
    late = market_signals._daily_digest_dedupe_key(
        "2026-11-13", 1, 1, status="data_not_ready", version="validated_v2"
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


class _Conn:
    def __init__(self):
        self.events = []

    def commit(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")


def test_tracking_failure_is_visible_in_the_digest_not_silent(monkeypatch):
    def boom(*args, **kwargs):
        raise ConnectionError("ledger down")

    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", boom)
    conn = _Conn()
    embeds = market_signals._update_validated_tracking(
        object(), conn, object(), states=_states(), registry=validated_signals.load_registry(),
        as_of_date=AS_OF, detection=_detection({}), names={},
    )
    assert embeds[0]["title"] == "上線追蹤暫時無法更新"
    assert "rollback" in conn.events


def test_one_rules_tracking_failure_does_not_discard_the_other_rules_records(monkeypatch):
    recorded = []

    def settle(cursor, stock_cursor, *, rule, **kwargs):
        if rule["id"] == "trust_buy_5d_top":
            raise ConnectionError("deadlock")

    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", settle)
    monkeypatch.setattr(
        market_signals.validated_tracking, "record_picks",
        lambda cursor, *, rule, **kwargs: recorded.append(rule["id"]),
    )
    monkeypatch.setattr(
        market_signals.validated_tracking, "live_summary",
        lambda cursor, rule_id, horizon=None: validated_tracking.summarize_by_date([], 10),
    )
    conn = _Conn()
    embeds = market_signals._update_validated_tracking(
        object(), conn, object(), states=_states(), registry=validated_signals.load_registry(),
        as_of_date=dt.date(2026, 11, 12), detection=_detection({}), names={},
    )
    assert "trust_buy_5d_top" not in recorded and "breakout_60d_volume" in recorded
    assert len(recorded) == len(signal_rules.RULES) - 1
    assert conn.events.count("rollback") == 1 and conn.events.count("commit") == len(signal_rules.RULES) - 1
    assert embeds[0]["title"] == "上線追蹤暫時無法更新" and "不會自動補記" in embeds[0]["description"]


def test_tracking_is_not_recorded_while_prices_are_not_ready(monkeypatch):
    calls = []
    monkeypatch.setattr(market_signals.validated_tracking, "record_picks", lambda *a, **k: calls.append("record"))
    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", lambda *a, **k: calls.append("settle"))
    monkeypatch.setattr(
        market_signals.validated_tracking, "live_summary",
        lambda cursor, rule_id, horizon=None: validated_tracking.summarize_by_date([], 10, pending=3),
    )
    conn = _Conn()
    embeds = market_signals._update_validated_tracking(
        object(), conn, object(), states=_states(), registry=validated_signals.load_registry(),
        as_of_date=dt.date(2026, 11, 12),  # a Thursday: no weekly scorecard
        detection={"status": "data_not_ready", "rules": {}, "breadth": {}}, names={},
    )
    assert calls == [] and conn.events == []
    assert embeds[0]["title"].startswith("上線後成績")


def test_every_rules_hits_are_tracked_but_only_active_rules_are_ever_marked_shown(monkeypatch):
    recorded = {}

    def fake_record(cursor, *, rule, version, as_of_date, hits, names, now):
        recorded[rule["id"]] = (rule["params"]["max_names_shown"], len(hits))

    monkeypatch.setattr(market_signals.validated_tracking, "record_picks", fake_record)
    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", lambda *a, **k: {})
    monkeypatch.setattr(
        market_signals.validated_tracking, "live_summary",
        lambda cursor, rule_id, horizon=None: validated_tracking.summarize_by_date([], 10),
    )
    detection = _detection({"breakout_60d_volume": _hits(4), "trust_buy_5d_top": _hits(6)})
    market_signals._update_validated_tracking(
        object(), _Conn(), object(), states=_states(), registry=validated_signals.load_registry(),
        as_of_date=dt.date(2026, 11, 12), detection=detection, names={},
    )
    assert recorded["breakout_60d_volume"] == (5, 4)
    assert recorded["trust_buy_5d_top"] == (0, 6)  # candidate: tracked, shown=0 for every row
    assert len(recorded) == len(signal_rules.RULES)


def test_a_rule_whose_feed_has_not_landed_records_nothing_that_day(monkeypatch):
    recorded = []
    monkeypatch.setattr(
        market_signals.validated_tracking, "record_picks",
        lambda cursor, *, rule, **kwargs: recorded.append(rule["id"]),
    )
    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", lambda *a, **k: {})
    monkeypatch.setattr(
        market_signals.validated_tracking, "live_summary",
        lambda cursor, rule_id, horizon=None: validated_tracking.summarize_by_date([], 10),
    )
    detection = _detection({})
    detection["rules"]["margin_cleanup_up"] = {"status": "data_not_ready", "missing_feeds": ["margin"], "hits": []}
    market_signals._update_validated_tracking(
        object(), _Conn(), object(), states=_states(), registry=validated_signals.load_registry(),
        as_of_date=dt.date(2026, 11, 12), detection=detection, names={},
    )
    assert "margin_cleanup_up" not in recorded and "breakout_60d_volume" in recorded


class _LedgerCursor:
    def __init__(self):
        self.statements = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.statements.append((sql, params))


def test_intraday_cursor_advance_survives_a_shadow_tracking_failure(monkeypatch):
    """The cursor advance is committed before best-effort tracking, so a tracking
    error can never roll it back (it would otherwise repeat on every run)."""
    cursor, conn = _LedgerCursor(), _Conn()

    def boom(*args, **kwargs):
        raise ValueError("bad candidate")

    monkeypatch.setattr(market_signals.validated_tracking, "record_shadow_candidates", boom)
    result = market_signals._record_suppressed_intraday(
        cursor, conn, {"proposed_source_cursor_id": 123}, [{"entity_id": "2330"}]
    )
    assert result["suppressed"] == 1 and result["sent"] == 0
    assert any("market_signal_state" in sql for sql, _ in cursor.statements)
    assert conn.events == ["commit", "rollback"]  # advance committed first


def test_intraday_candidates_are_recorded_after_the_cursor_commit(monkeypatch):
    cursor, conn = _LedgerCursor(), _Conn()
    seen = {}
    monkeypatch.setattr(
        market_signals.validated_tracking, "record_shadow_candidates",
        lambda cur, **kwargs: seen.update(kwargs) or len(kwargs["candidates"]),
    )
    rows = [{"entity_id": "2330"}, {"entity_id": "2317"}]
    market_signals._record_suppressed_intraday(cursor, conn, {}, rows)
    assert seen["candidates"] == rows
    assert seen["rule"]["id"] == market_signals.validated_tracking.SHADOW_INTRADAY_RULE["id"]
    assert conn.events == ["commit", "commit"]


def test_shadow_settlement_failure_does_not_touch_the_live_tracking_transaction(monkeypatch):
    conn = _Conn()

    def boom(*args, **kwargs):
        raise ConnectionError("stock db down")

    monkeypatch.setattr(market_signals.validated_tracking, "settle_pending_picks", boom)
    market_signals._settle_shadow_picks(_LedgerCursor(), conn, object(), AS_OF)  # must not raise
    assert conn.events == ["rollback"]
