import datetime as dt

import pandas as pd

from source_extension import flow_context as fc
from source_extension import signal_rules, validated_signals, validated_tracking

D = [dt.date(2026, 11, d) for d in (17, 18, 19, 20, 23)]
CODE = "2597"


def _volume(per_day=866.0, days=D):
    return {(CODE, day): per_day for day in days}


def _row(day, branch, name, buy, sell, created=None):
    return {"day": day, "branch": branch, "name": name, "buy": buy, "sell": sell, "created": created}


def _balanced(day):
    """Mirrors 2597 on 2026-11-16 (866 lots): foreign houses buy big, 凱基 has two branches,
    元大/富邦/國泰 sell in pieces, and many tiny branches fill the rest of both sides."""
    rows = [
        _row(day, "1480", "美商高盛", 156, 22),
        _row(day, "8440", "摩根大通", 108, 12),
        _row(day, "9268", "凱基-台北", 106, 16),
        _row(day, "9227", "凱基-城中", 77, 3),
        _row(day, "1470", "台灣摩根士丹利", 88, 17),
        _row(day, "9896", "元大-台南", 0, 25),
        _row(day, "981p", "元大-松山", 0, 21),
        _row(day, "9835", "元大-員林中山", 0, 18),
        _row(day, "9676", "富邦-仁愛", 0, 42),
        _row(day, "8888", "國泰-敦南", 5, 32),
    ]
    buy = sum(r["buy"] for r in rows)
    sell = sum(r["sell"] for r in rows)
    for i in range((866 - buy) // 10):
        rows.append(_row(day, f"B{i:03d}", f"散買{i}-分", 10, 0))
    for i in range((866 - sell) // 10):
        rows.append(_row(day, f"S{i:03d}", f"散賣{i}-分", 0, 10))
    buy = sum(r["buy"] for r in rows)
    sell = sum(r["sell"] for r in rows)
    rows.append(_row(day, "Z001", "尾數-分", 866 - buy, 866 - sell))  # lands both sides exactly on 866
    return rows


def test_firm_key_merges_branches_and_survives_a_missing_name():
    assert fc.firm_key("凱基-台北", "9268") == "凱基"
    assert fc.firm_key("凱基-城中", "9227") == "凱基"
    assert fc.firm_key("美商高盛", "1480") == "美商高盛"
    assert fc.firm_key(None, "9B2Z") == "分點9B2Z"
    assert fc.firm_key("  ", "9B2Z") == "分點9B2Z"


def test_foreign_house_detection_does_not_flag_local_firms():
    assert all(fc.is_foreign_broker(n) for n in ("美商高盛", "摩根大通", "台灣摩根士丹利", "新加坡商瑞銀", "香港上海匯豐"))
    assert not any(fc.is_foreign_broker(n) for n in ("元大-台南", "凱基-台北", "大和國泰", "富邦-仁愛", None))


def test_institution_line_shows_trust_and_dealer_not_only_foreign():
    sessions = D
    inst = {(CODE, day): (0.0, 0.0, 0.0) for day in sessions}
    inst[(CODE, D[3])] = (521_083.0, 1_000.0, 12_982.0)
    inst[(CODE, D[4])] = (93_449.0, 308_825.0, -16_589.0)
    info = fc.institution_context(CODE, sessions, inst, _volume())
    assert round(info["trust_lots"]) == 310 and round(info["foreign_lots"]) == 615
    text = fc.institution_line(info)
    assert "外資" in text and "投信 +310 張" in text and "自營" in text
    assert info["alignment"] == "外資、投信同買"


def test_a_negligible_share_never_prints_a_signed_zero():
    inst = {(CODE, D[0]): (500_000.0, 0.0, -3_000.0)}  # -3 lots on an 866-lot day is -0.35%
    text = fc.institution_line(fc.institution_context(CODE, D, inst, _volume()))
    assert "-0%" not in text and "+0%" not in text
    assert "自營 -3 張（量 ~0%）" in text and "投信 +0 張（量 ~0%）" in text


def test_foreign_buy_trust_sell_is_flagged_as_divergence():
    inst = {(CODE, D[0]): (600_000.0, -500_000.0, 0.0)}
    info = fc.institution_context(CODE, D, inst, _volume())
    assert info["alignment"] == "外資買、投信賣（分歧）"
    assert "（分歧）" in fc.institution_line(info)


def test_institution_context_is_none_without_rows_and_flags_partial_windows():
    assert fc.institution_context(CODE, D, {}, _volume()) is None
    info = fc.institution_context(CODE, D, {(CODE, D[0]): (1000.0, 0.0, 0.0)}, _volume())
    assert "（1/5 日）" in fc.institution_line(info)


def test_broker_context_nets_same_firm_branches_and_reads_the_sides():
    rows = _balanced(D[0])
    info = fc.broker_context(CODE, D, rows, _volume(), {D[0]: 453_630.0})
    assert info["status"] == "ok" and info["days"] == [D[0]]
    firms = {b["firm"]: b["net"] for b in info["top_buyers"]}
    assert firms["凱基"] == 164  # 90 + 74: two branches of one firm are one buyer
    assert info["top_buyers"][0]["firm"] == "凱基"
    assert info["foreign_broker_net_lots"] == (156 - 22) + (108 - 12) + (88 - 17)
    assert info["foreign_inst_lots"] == 453.63


def test_a_branch_buy_offset_by_its_own_firms_other_branches_is_called_out():
    day = D[0]
    rows = _balanced(day) + [
        _row(day, "9801", "元大-大買", 120, 0),
        _row(day, "9802", "元大-甲", 0, 70),
        _row(day, "9803", "元大-乙", 0, 60),
    ]
    volume = {(CODE, day): sum(r["buy"] for r in rows)}
    info = fc.broker_context(CODE, [day], rows, volume)
    assert any(o["branch"] == "元大-大買" and o["firm_net"] < 0.5 * o["branch_net"] for o in info["offsets"])
    assert any("被元大其他分點抵銷" in line for line in fc.broker_lines(info, "上市"))


def test_the_same_firm_buying_and_selling_is_flagged_as_a_swap():
    day = D[0]
    rows = _balanced(day) + [_row(day, "9810", "某券商-甲", 400, 0), _row(day, "9811", "某券商-乙", 0, 250),
                             _row(day, "9812", "補賣-分", 0, 150)]  # keeps the book balanced like a real one
    volume = {(CODE, day): sum(r["buy"] for r in rows)}
    info = fc.broker_context(CODE, [day], rows, volume)
    assert [w["firm"] for w in info["wash"]] == ["某券商"]  # net +150 is a top-3 buyer, yet it sold 250 of the 400 it bought
    text = "\n".join(fc.broker_lines(info, "上市"))
    assert "某券商 毛買 400／毛賣 250 張（賣出占買進 62%）" in text
    assert "互換" not in text and "對敲" not in text  # gross figures only; no claim about intent


def test_foreign_houses_are_not_flagged_for_two_way_trading():
    day = D[0]
    rows = _balanced(day) + [_row(day, "1500", "美商某投行", 500, 400), _row(day, "9950", "補賣-分", 0, 100)]
    volume = {(CODE, day): sum(r["buy"] for r in rows)}
    info = fc.broker_context(CODE, [day], rows, volume)
    assert any(b["firm"] == "美商某投行" for b in info["top_buyers"])
    assert info["wash"] == []


def test_one_big_buyer_and_scattered_sellers_is_described_without_a_verdict():
    day = D[0]
    rows = [_row(day, "9900", "大買-總", 600, 0)]
    rows += [_row(day, f"95{i:02d}", f"小賣{i}-分行", 0, 12) for i in range(50)]
    volume = {(CODE, day): 600.0}
    info = fc.broker_context(CODE, [day], rows, volume)
    assert info["pattern"] == "買方集中、賣方分散"
    text = "\n".join(fc.broker_lines(info, "上市"))
    assert "買方集中、賣方分散" in text and "50 個分點" in text
    assert "出貨" not in text and "吃貨" not in text  # unvalidated: describe, never conclude


def test_duplicate_rows_from_a_recollection_are_not_double_counted():
    day = D[0]
    old, new = dt.datetime(2026, 11, 17, 8, 0), dt.datetime(2026, 11, 17, 9, 0)
    rows = [_row(day, "9B2Z", None, 400, 0, old), _row(day, "9B2Z", None, 400, 0, new),
            _row(day, "9B2Y", None, 0, 400, new)]
    info = fc.broker_context(CODE, [day], rows, {(CODE, day): 400.0})
    assert info["status"] == "ok"
    assert info["top_buyers"][0]["net"] == 400  # one 400-lot row, not 800
    assert info["top_buyers"][0]["firm"] == "分點9B2Z"


def test_a_panel_that_disagrees_with_the_days_volume_is_rejected_not_interpreted():
    day = D[0]
    rows = _balanced(day)
    info = fc.broker_context(CODE, [day], rows, {(CODE, day): 5000.0})
    assert info["status"] == "no_valid_day" and info["rejected_days"] == 1
    assert fc.broker_lines(info, "上市")[0].startswith("分點：近 1 日沒有可用資料")


def test_tiny_volume_days_are_not_interpreted():
    day = D[0]
    rows = [_row(day, "9900", "大買-總", 50, 0), _row(day, "9901", "小賣-分", 0, 50)]
    assert fc.broker_context(CODE, [day], rows, {(CODE, day): 50.0})["status"] == "no_valid_day"


def test_only_valid_days_are_listed_with_their_dates_and_coverage():
    rows = _balanced(D[0]) + _balanced(D[2])
    info = fc.broker_context(CODE, D, rows, _volume())
    assert info["days"] == [D[0], D[2]]
    head = fc.broker_lines(info, "上市")[0]
    assert "11/17、11/19" in head and "2/5 日" in head


def test_latest_data_day_is_reported_when_nothing_in_the_window_is_usable():
    old = dt.date(2026, 11, 10)
    info = fc.broker_context(CODE, D, _balanced(old), _volume())
    assert info["status"] == "no_valid_day" and info["latest_data_day"] == old
    assert "最新只到 11/10" in fc.broker_lines(info, "上市")[0]


def test_otc_and_unknown_markets_are_labelled_honestly():
    assert fc.broker_lines({}, "上櫃") == ["分點：上櫃沒有分點資料"]
    assert "市場別不明" in fc.broker_lines({}, "")[0]


def test_foreign_broker_vs_institution_mismatch_is_shown():
    day = D[0]
    info = fc.broker_context(CODE, [day], _balanced(day), {(CODE, day): 866.0}, {day: -200_000.0})
    assert any("兩者方向相反" in line for line in fc.broker_lines(info, "上市"))


def test_a_day_missing_from_the_institution_table_is_unknown_not_zero():
    day = D[0]
    info = fc.broker_context(CODE, [day], _balanced(day), {(CODE, day): 866.0}, {})  # no 三大法人 row for the day
    assert info["foreign_inst_lots"] is None
    assert not any("法人外資" in line for line in fc.broker_lines(info, "上市"))


def test_a_book_where_buys_and_sells_do_not_match_is_rejected():
    day = D[0]
    rows = [_row(day, "9900", "大買-總", 790, 0), _row(day, "9901", "小賣-分", 0, 950)]
    # buy 0.91x and sell 1.10x of the 866-lot day: each passes the volume band, but they disagree with each other
    info = fc.broker_context(CODE, [day], rows, {(CODE, day): 866.0})
    assert info["status"] == "no_valid_day"


def test_nat_and_null_dates_are_ignored_not_fatal():
    assert fc._to_date(pd.NaT) is None and fc._to_date(None) is None and fc._to_date("") is None
    rows = _balanced(D[0]) + [_row(pd.NaT, "Q1", "壞日期-分", 5, 5), _row(None, "Q2", "壞日期2-分", 5, 5)]
    info = fc.broker_context(CODE, D, rows, _volume())
    assert info["status"] == "ok" and info["days"] == [D[0]]


def test_sublines_are_never_split_away_from_their_stock_row():
    lines = []
    for i in range(30):
        lines.append(f"**{3000 + i} 測試公司**｜收 50｜今日 +1.0%")
        lines.append(f"{fc.SUBLINE_PREFIX}法人 5 日：外資 +100 張｜投信 +50 張")
        lines.append(f"{fc.SUBLINE_PREFIX}分點（11/17，1/5 日）：大買 甲 +50｜大賣 乙 -40")
    parts = validated_signals.split_long_embeds([{"title": "T", "description": "\n".join(lines)}])
    assert len(parts) > 1
    for part in parts:
        first = part["description"].split("\n")[0]
        assert not first.startswith(fc.SUBLINE_PREFIX), "a continuation chunk must start with a stock row"
    assert "\n".join(p["description"] for p in parts) == "\n".join(lines)


def test_inst_only_digest_does_not_mention_broker_lines_in_its_disclaimer():
    assert "分點" in fc.DISCLAIMER and "分點" not in fc.DISCLAIMER_INST_ONLY


def test_context_lines_respect_the_detail_level():
    ctx = {"market": "上市",
           "institution": fc.institution_context(CODE, D, {(CODE, D[0]): (500_000.0, 0.0, 0.0)}, _volume()),
           "broker": fc.broker_context(CODE, D, _balanced(D[0]), _volume())}
    assert any("分點" in line for line in fc.context_lines(ctx, "full"))
    inst_only = fc.context_lines(ctx, "inst")
    assert len(inst_only) == 1 and "法人" in inst_only[0]
    assert fc.context_lines(ctx, "none") == [] and fc.context_lines(None, "full") == []


class _Cursor:
    """Answers the three queries build_flow_context makes, and records the SQL it was given."""

    def __init__(self, fail=False):
        self.sql, self.fail, self._rows = [], fail, []

    def execute(self, sql, params=()):
        self.sql.append(sql)
        if self.fail:
            raise RuntimeError("db down")
        if "全證券代碼相關資料表" in sql:
            self._rows = [(CODE, "上市"), ("6488", "上櫃")]
        elif "三大法人買賣" in sql:
            self._rows = [(D[4], CODE, 93_449.0, 308_825.0, -16_589.0), (D[4], "6488", 1000.0, 0.0, 0.0)]
        elif "券商進出總表" in sql:
            self._rows = [(r["day"], CODE, r["branch"], r["name"], r["buy"], r["sell"], None) for r in _balanced(D[4])]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


def _price(codes=(CODE, "6488")):
    rows = []
    for day in D:
        for code in codes:
            rows.append({"date": pd.Timestamp(day), "code": code, "close": 100.0, "vol": 866.0})
        for i in range(200):  # a normal-sized market so the session filter keeps these days
            rows.append({"date": pd.Timestamp(day), "code": f"{3000 + i}", "close": 10.0, "vol": 100.0})
    return pd.DataFrame(rows)


def _states_and_detection(codes=(CODE, "6488")):
    states = [{"id": "foreign_buy_5d_top", "status": "active", "params": {"max_names_shown": 5}}]
    hits = [{"code": c, "close": 100.0, "day_return_pct": 0.5, "metric": 440.1} for c in codes]
    return states, {"status": "ok", "rules": {"foreign_buy_5d_top": {"status": "ok", "hits": hits}}}


def test_build_flow_context_queries_the_index_safely_and_covers_both_markets():
    cursor = _Cursor()
    states, detection = _states_and_detection()
    result = fc.build_flow_context(cursor, {"price": _price()}, states, detection, D[-1])
    assert "error" not in result
    assert result["codes"][CODE]["broker"]["status"] == "ok"
    assert result["codes"]["6488"]["broker"] == {"status": "not_listed"}
    assert result["codes"][CODE]["institution"]["trust_lots"] > 300
    broker_sql = next(s for s in cursor.sql if "券商進出總表" in s)
    assert "TRIM(證券代號)" not in broker_sql  # TRIM would defeat the (資料日期, 證券代號, 券商名稱) index
    assert "資料日期 BETWEEN" in broker_sql and "IN (" in broker_sql and "DISTINCT" not in broker_sql


def test_build_flow_context_failure_is_contained_and_reported():
    states, detection = _states_and_detection()
    result = fc.build_flow_context(_Cursor(fail=True), {"price": _price()}, states, detection, D[-1])
    assert result["codes"] == {} and "RuntimeError" in result["error"]


def test_build_flow_context_skips_work_when_prices_are_not_ready():
    cursor = _Cursor()
    states, detection = _states_and_detection()
    detection["status"] = "data_not_ready"
    assert fc.build_flow_context(cursor, {"price": _price()}, states, detection, D[-1]) == {"codes": {}}
    assert cursor.sql == []


# ------------------------------------------------------------------ digest integration

AS_OF = dt.date(2026, 11, 23)


def _digest_states(active, max_names=5):
    states = []
    for rule in signal_rules.RULES:
        states.append({
            "id": rule.id, "label": rule.label, "meaning": rule.meaning, "metric_label": rule.metric_label,
            "metric_format": rule.metric_format, "sources": rule.sources, "is_control": rule.is_control,
            "status": "active" if rule.id in active else "candidate", "horizon_sessions": 10,
            "evidence": {"horizon": 10, "oos_dates": 886, "mean_excess_pct": 0.8, "mean_excess_after_cost_pct": 0.2, "t_nw": 3.0,
                         "win_rate_pct": 41.0, "base_win_rate_pct": 39.0, "years_total": 11, "years_positive": 9,
                         "recent_reference": {"dates": 63, "mean_excess_after_cost_pct": -0.19}},
            "from_database": True, "params": {"max_names_shown": max_names, "min_turnover_ntd": 50_000_000},
        })
    return states


def _digest_detection(hits_by_rule):
    return {"status": "ok", "breadth": {"stocks": 1500, "up": 800, "down": 600, "median_return_pct": 0.3},
            "rules": {r.id: {"status": "ok", "missing_feeds": [], "hits": hits_by_rule.get(r.id, [])}
                      for r in signal_rules.RULES}}


def _real_context():
    cursor = _Cursor()
    states, detection = _states_and_detection()
    return fc.build_flow_context(cursor, {"price": _price()}, states, detection, D[-1])


def test_digest_shows_trust_and_broker_lines_for_the_stock_and_labels_them_unvalidated():
    hits = [{"code": CODE, "close": 193.0, "day_return_pct": 0.5, "metric": 440.1}]
    message, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), _digest_detection({"foreign_buy_5d_top": hits}),
        _digest_states(("foreign_buy_5d_top",)), {CODE: "潤弘"}, flow_context=_real_context(),
    )
    text = message + "".join(e["title"] + "\n" + e["description"] for e in embeds)
    assert "**2597 潤弘**｜收 193｜今日 +0.5%｜外資 5 日買超占 20 日均量 440.1%" in text
    assert "　└ 法人" in text and "投信" in text and "　└ 分點（" in text and "大買" in text
    assert "沒有經過長期驗證" in text and "不影響規則命中與排序" in text


def test_digest_is_unchanged_without_a_flow_context():
    hits = [{"code": CODE, "close": 193.0, "day_return_pct": 0.5, "metric": 440.1}]
    args = (AS_OF, validated_signals.load_registry(), _digest_detection({"foreign_buy_5d_top": hits}),
            _digest_states(("foreign_buy_5d_top",)), {CODE: "潤弘"})
    plain = validated_signals.build_validated_digest(*args)
    empty = validated_signals.build_validated_digest(*args, flow_context={"codes": {}})
    assert plain == empty
    assert "　└" not in "".join(e["description"] for e in plain[1])


def test_a_flow_failure_adds_one_notice_and_the_signal_list_still_goes_out():
    hits = [{"code": CODE, "close": 193.0, "day_return_pct": 0.5, "metric": 440.1}]
    message, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), _digest_detection({"foreign_buy_5d_top": hits}),
        _digest_states(("foreign_buy_5d_top",)), {CODE: "潤弘"}, flow_context={"codes": {}, "error": "RuntimeError: x"},
    )
    titles = [e["title"] for e in embeds]
    assert "籌碼補充資料暫時無法取得" in titles and "訊號：外資 5 日買超強度居前 5%" in titles
    assert "　└" not in "".join(e["description"] for e in embeds)


def test_heavy_digests_drop_annotations_before_they_drop_stock_names():
    rules = [r.id for r in signal_rules.RULES if not r.is_control][:4]
    states = _digest_states(rules)
    codes = [f"{2000 + i}" for i in range(20)]
    hits = [{"code": c, "close": 50.0, "day_return_pct": 1.0, "metric": 9.0 - i * 0.1} for i, c in enumerate(codes)]
    detection = _digest_detection({r: hits for r in rules})
    info = fc.institution_context("x", D, {("x", D[0]): (900_000.0, 400_000.0, 5_000.0)}, {("x", day): 866.0 for day in D})
    broker = fc.broker_context("x", D, [dict(r, branch=r["branch"]) for r in _balanced(D[0])], {("x", day): 866.0 for day in D})
    flow = {"codes": {c: {"market": "上市", "institution": info, "broker": broker} for c in codes}}
    names = {c: "測試公司名稱" for c in codes}
    registry = validated_signals.load_registry()
    full_msg, full_embeds = validated_signals.build_validated_digest(AS_OF, registry, detection, states, names, flow_context=flow)
    bare_msg, bare_embeds = validated_signals.build_validated_digest(AS_OF, registry, detection, states, names)
    full_text = "".join(e["description"] for e in full_embeds)
    # 4 rules x 5 names with full annotations cannot fit one request: the broker lines shrink to
    # the brief institution line, but no annotation is dropped and no stock name is cut for them.
    assert "　└ 分點" not in full_text and "　└ 法人" in full_text and "投信 +400" in full_text
    for code in codes:
        assert full_text.count(f"**{code} ") >= sum(1 for e in bare_embeds if f"**{code} " in e["description"])
    assert all(len(e["description"]) <= validated_signals.EMBED_MAX_CHARS for e in full_embeds)


def test_the_same_stock_under_two_rules_is_annotated_once():
    hits = [{"code": CODE, "close": 193.0, "day_return_pct": 0.5, "metric": 440.1}]
    rules = ("foreign_buy_5d_top", "inst_buy_streak_5d")
    _, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), _digest_detection({r: hits for r in rules}),
        _digest_states(rules), {CODE: "潤弘"}, flow_context=_real_context(),
    )
    text = "".join(e["description"] for e in embeds)
    assert text.count("　└ 法人") == 1 and text.count(fc.SAME_AS_ABOVE) == 1
    assert text.count(f"**{CODE} 潤弘**") == 2  # both rule rows still list the stock


def test_the_brief_form_keeps_all_three_institutions_in_one_short_line():
    info = fc.institution_context(CODE, D, {(CODE, D[0]): (2_571_000.0, 312_000.0, -14_000.0)}, _volume())
    ctx = {"market": "上市", "institution": info, "broker": None}
    lines = fc.context_lines(ctx, "brief")
    assert len(lines) == 1 and "外資 +2,571／投信 +312／自營 -14 張" in lines[0]
    assert len(lines[0]) < len(fc.context_lines(ctx, "inst")[0])


def test_a_realistic_busy_day_with_tracking_lines_keeps_the_institution_numbers():
    """With the live tracking lines present the digest is tight; the trust figure must survive."""
    rules = ["foreign_trust_both_buy_3d", "breakout_60d_volume"]  # what is actually pushing today
    states = _digest_states(rules)
    codes = [f"{2000 + i}" for i in range(7)]
    hits = [{"code": c, "close": 50.0, "day_return_pct": 1.0, "metric": 9.0 - i * 0.1} for i, c in enumerate(codes)]
    detection = _digest_detection({"foreign_trust_both_buy_3d": hits[:5], "breakout_60d_volume": hits[2:7]})
    info = fc.institution_context("x", D, {("x", D[0]): (2_571_000.0, 312_000.0, -14_000.0)}, {("x", d): 866.0 for d in D})
    flow = {"codes": {c: {"market": "上市", "institution": info, "broker": None} for c in codes}}
    names = {c: "測試公司名稱" for c in codes}
    live = {s["id"]: validated_tracking.summarize_by_date(
        [(AS_OF - dt.timedelta(days=d), 0.4, 3) for d in range(1, 40)], 10) for s in states}
    card = {"before_per_week": {"messages": 30.0, "failed": 2.0, "failure_notices": 1.0},
            "after_week": {"messages": 6, "failed": 0, "failure_notices": 0},
            "live": live, "shadow": validated_tracking.summarize_excess([], pending=4)}
    msg, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), detection, states, names, flow_context=flow,
        tracking_embeds=[validated_tracking.scorecard_embed(card, states)],
    )
    text = "".join(e["description"] for e in embeds)
    assert "投信 +312" in text, "the trust figure must not be dropped on a Friday (scorecard) digest"
    assert all(f"**{c} " in text for c in codes)  # and no stock name was cut to make room
    # whatever the size, each embed alone is sendable, so the sender can split the digest in two
    assert all(len(e["description"]) <= validated_signals.EMBED_MAX_CHARS for e in embeds)


def test_a_light_digest_keeps_every_annotation():
    hits = [{"code": CODE, "close": 193.0, "day_return_pct": 0.5, "metric": 440.1}]
    _, embeds = validated_signals.build_validated_digest(
        AS_OF, validated_signals.load_registry(), _digest_detection({"foreign_buy_5d_top": hits}),
        _digest_states(("foreign_buy_5d_top",)), {CODE: "潤弘"}, flow_context=_real_context(),
    )
    text = "".join(e["description"] for e in embeds)
    assert text.count("　└ 法人") == 1 and text.count("　└ 分點（") == 1
