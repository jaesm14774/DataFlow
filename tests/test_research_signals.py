import math

from source_extension import research_signals as rs


def test_family_table_is_pre_registered_and_unique():
    names = [spec.name for spec in rs.FAMILIES]
    assert len(names) == len(set(names))
    for spec in rs.FAMILIES:
        assert spec.sign in (-1, 1)
        assert spec.horizons and all(h > 0 for h in spec.horizons)
        assert spec.hypothesis and spec.publish_lag


def test_cross_sectional_directions_use_signed_tails_and_skip_thin_days():
    scores = {f"{1000 + i}": float(i) for i in range(300)}
    bullish = rs.cross_sectional_directions(scores, sign=1)
    assert sum(value > 0 for value in bullish.values()) == 30
    assert sum(value < 0 for value in bullish.values()) == 30
    assert bullish["1299"] == 1 and bullish["1000"] == -1
    # a negative hypothesis sign flips which tail is bullish
    bearish = rs.cross_sectional_directions(scores, sign=-1)
    assert bearish["1299"] == -1 and bearish["1000"] == 1
    # too few names: no fabricated directions
    assert rs.cross_sectional_directions({"1101": 1.0, "1102": 2.0}, sign=1) == {}


def test_cross_sectional_directions_ignore_missing_and_huge_ties():
    scores = {f"{1000 + i}": None for i in range(50)}
    scores.update({f"{2000 + i}": 0.0 for i in range(250)})
    # 250 identical zeros straddle both tails: they must stay neutral
    assert rs.cross_sectional_directions(scores, sign=1) == {}


def test_benjamini_hochberg_matches_known_values_and_keeps_none():
    adjusted = rs.benjamini_hochberg([0.01, 0.04, 0.03, None, 0.20])
    assert adjusted[3] is None
    assert math.isclose(adjusted[0], 0.04)
    assert math.isclose(adjusted[1], 0.0533333, rel_tol=1e-4)
    assert math.isclose(adjusted[2], 0.0533333, rel_tol=1e-4)
    assert math.isclose(adjusted[4], 0.20)


def test_mean_t_stat_and_bootstrap_are_stable():
    values = [0.02, 0.03, 0.01, 0.04, 0.02, 0.03, 0.02, 0.01]
    result = rs.mean_t_stat(values)
    assert result["n"] == 8 and result["t"] > 5
    low, high = rs.bootstrap_mean_ci(values, seed=3)
    assert 0 < low < result["mean"] < high
    assert rs.mean_t_stat([1.0])["t"] is None
    assert rs.bootstrap_mean_ci([1.0, 2.0]) is None


def _block(mean, p, n=60):
    return {"n": n, "mean": mean, "p": p, "t": 3.0}


def test_family_verdict_requires_holdout_confirmation_and_clean_control():
    good = dict(discovery=_block(0.03, 0.001), holdout=_block(0.02, 0.01), q_value=0.01, shuffle_ok=True)
    assert rs.family_verdict(**good) == "confirmed"
    assert rs.family_verdict(**{**good, "shuffle_ok": False}) == "control_failed"
    assert rs.family_verdict(**{**good, "q_value": 0.30}) == "discovery_only"
    flipped = {**good, "holdout": _block(-0.02, 0.01)}
    assert rs.family_verdict(**flipped) == "unstable_sign"
    negative = dict(discovery=_block(-0.03, 0.001), holdout=_block(-0.02, 0.01), q_value=0.01, shuffle_ok=True)
    assert rs.family_verdict(**negative) == "reversed"
    assert rs.family_verdict(**{**good, "holdout": _block(0.02, 0.01, n=5)}) == "insufficient_dates"
    noise = dict(discovery=_block(0.001, 0.6), holdout=_block(0.002, 0.5), q_value=0.9, shuffle_ok=True)
    assert rs.family_verdict(**noise) == "noise"


def test_research_family_key_is_idempotent():
    assert rs.research_family_key("margin_utilization") == "research:margin_utilization"
    assert rs.research_family_key("research:margin_utilization") == "research:margin_utilization"


def test_collect_forward_scores_maps_existing_context_without_inventing_values():
    scores = rs.collect_forward_scores(
        {
            "margin_balance_change_5d_pct": 12.5, "short_balance_change_5d_pct": -3.0,
            "margin_utilization_pct": 40.0, "margin_balance": 10_000, "short_balance": 500,
        },
        {"revenue_yoy_pct": 25.0},
    )
    assert scores == {
        "margin_balance_change_5d": 12.5, "short_balance_change_5d": -3.0,
        "margin_utilization": 40.0, "short_to_margin_ratio": 5.0, "revenue_yoy": 25.0,
    }
    assert rs.collect_forward_scores({}, None) == {}
    # zero margin balance would divide by zero; bool/NaN are rejected
    partial = rs.collect_forward_scores(
        {"margin_balance": 0, "short_balance": 10, "margin_utilization_pct": float("nan")}, {},
    )
    assert partial == {}


def test_assign_forward_directions_applies_pre_registered_signs_and_prefix():
    scores = {f"{1000 + i}": {"margin_utilization": float(i), "revenue_yoy": float(i)} for i in range(300)}
    directions, counts = rs.assign_forward_directions(scores, {code: "上櫃" for code in scores})
    # margin_utilization is hypothesised bearish when high; revenue_yoy bullish when high
    assert directions["1299"]["research:margin_utilization"] == -1
    assert directions["1299"]["research:revenue_yoy"] == 1
    assert directions["1000"]["research:margin_utilization"] == 1
    assert "research:margin_balance_change_5d" not in directions["1299"]
    assert counts["research:margin_utilization"] == {"上櫃": 300}


def test_directions_are_assigned_per_market_and_stale_market_is_visible():
    # 上市 margin source stale: only 上櫃 names carry a score.
    scores = {f"{6000 + i}": {"margin_utilization": float(i)} for i in range(300)}
    scores.update({f"{2000 + i}": {"revenue_yoy": float(i)} for i in range(300)})
    markets = {code: ("上櫃" if code.startswith("6") else "上市") for code in scores}
    directions, counts = rs.assign_forward_directions(scores, markets)
    assert counts["research:margin_utilization"] == {"上櫃": 300}
    assert "上市" not in counts["research:margin_utilization"]
    assert counts["research:revenue_yoy"] == {"上市": 300}
    # a market below the floor gets no directions even if the other market is large
    thin = {f"{2000 + i}": {"margin_utilization": float(i)} for i in range(50)}
    thin.update({f"{6000 + i}": {"margin_utilization": float(i)} for i in range(300)})
    thin_markets = {code: ("上市" if code.startswith("2") else "上櫃") for code in thin}
    directions, counts = rs.assign_forward_directions(thin, thin_markets)
    assert counts["research:margin_utilization"] == {"上市": 50, "上櫃": 300}
    assert not any(code.startswith("2") for code in directions)
    assert any(code.startswith("6") for code in directions)


def test_verdict_flags_rank_only_effects_when_decile_spread_disagrees():
    base = dict(discovery=_block(0.03, 0.001), holdout=_block(0.02, 0.01), q_value=0.01, shuffle_ok=True)
    agree = {"spread_discovery": {"mean": 0.4}, "spread_holdout": {"mean": 0.2}}
    assert rs.family_verdict(**base, **agree) == "confirmed"
    skew = {"spread_discovery": {"mean": -0.4}, "spread_holdout": {"mean": -0.9}}
    assert rs.family_verdict(**base, **skew) == "rank_only_skew"
    missing = {"spread_discovery": {"mean": 0.4}, "spread_holdout": {"mean": None}}
    assert rs.family_verdict(**base, **missing) == "rank_only_skew"


def test_tie_group_straddling_the_boundary_is_neutral_as_a_whole():
    scores = {f"{1000 + i}": float(i) for i in range(14)}
    scores.update({f"{2000 + i}": 100.0 for i in range(10)})  # ranks 15-24 straddle the 20-name cut
    scores.update({f"{3000 + i}": 200.0 + i for i in range(176)})
    directions = rs.cross_sectional_directions(scores, sign=1)
    assert sum(value < 0 for value in directions.values()) == 14
    assert not any(code.startswith("2") for code in directions)


def test_student_t_p_value_matches_reference_values():
    # reference two-sided p-values from standard t tables
    assert math.isclose(rs.t_two_sided_p(2.228, 10), 0.05, abs_tol=2e-3)
    assert math.isclose(rs.t_two_sided_p(2.086, 20), 0.05, abs_tol=2e-3)
    assert math.isclose(rs.t_two_sided_p(1.0, 1), 0.5, abs_tol=1e-3)
    assert rs.t_two_sided_p(0.0, 5) == 1.0
    # small samples are more conservative than the normal approximation
    assert rs.t_two_sided_p(2.0, 5) > rs.normal_two_sided_p(2.0)


def test_forward_scores_include_price_structure_only_with_clean_inputs():
    closes = [100 + (i % 3) for i in range(21)]
    scores = rs.collect_forward_scores({}, {}, return_5d_pct=-4.5, closes=closes)
    assert scores["short_term_reversal_5d"] == -4.5
    assert scores["volatility_20d"] > 0
    # corporate-action guard upstream empties closes: no fabricated volatility
    assert "volatility_20d" not in rs.collect_forward_scores({}, {}, closes=[])
    assert "volatility_20d" not in rs.collect_forward_scores({}, {}, closes=[100] * 10)
    assert "volatility_20d" not in rs.collect_forward_scores({}, {}, closes=[100, None] + [100] * 19)


# ---------------------------------------------------------------------------
# News-title polarity (shared by the study and the daily scan)
# ---------------------------------------------------------------------------

import datetime as _dt


def _sessions():
    # 10-30 precedes the five-session window
    return [_dt.date(2026, 10, 30)] + [_dt.date(2026, 11, day) for day in (2, 3, 4, 5, 6)]


def _trie():
    return rs.build_name_trie([("2330", "台積電"), ("2317", "鴻海精密"), ("1101", "台泥"), ("1102", "台泥")])


def test_news_polarity_assigns_late_titles_to_next_session_and_drops_future():
    trie, codes = _trie()
    titles = [
        ("台積電 大漲 創高", _dt.datetime(2026, 11, 6, 9, 0)),       # in window
        ("台積電 看好 受惠", _dt.datetime(2026, 11, 5, 13, 30)),     # at close: same session
        ("台積電 利空", _dt.datetime(2026, 11, 6, 13, 31)),           # after close: belongs to 11-09
        ("台積電 大漲", _dt.datetime(2026, 11, 1, 10, 0)),            # Sunday: next session is 11-02
        ("台積電 大漲", _dt.datetime(2026, 10, 29, 10, 0)),           # before the window
    ]
    stats = rs.news_polarity_by_code(titles, _sessions(), trie, codes)["2330"]
    assert stats["news_n"] == 3 and stats["bull"] == 3 and stats["bear"] == 0
    assert stats["polarity"] == round(3 / 4, 4)
    assert rs.news_polarity_direction(stats) == 1


def test_news_polarity_needs_five_sessions_and_two_labelled_titles():
    trie, codes = _trie()
    assert rs.news_polarity_by_code([], _sessions()[1:5], trie, codes) == {}
    one = rs.news_polarity_by_code([("鴻海精密 利空", _dt.datetime(2026, 11, 6, 9, 0))], _sessions(), trie, codes)
    assert rs.news_polarity_direction(one["2317"]) == 0
    mixed = rs.news_polarity_by_code(
        [("鴻海精密 大漲", _dt.datetime(2026, 11, 6, 9, 0)), ("鴻海精密 大跌", _dt.datetime(2026, 11, 6, 10, 0))],
        _sessions(), trie, codes,
    )
    assert rs.news_polarity_direction(mixed["2317"]) == 0
    bearish = rs.news_polarity_by_code(
        [("鴻海精密 重挫", _dt.datetime(2026, 11, 5, 9, 0)), ("(2317) 虧損 示警", _dt.datetime(2026, 11, 6, 10, 0))],
        _sessions(), trie, codes,
    )
    assert rs.news_polarity_direction(bearish["2317"]) == -1


def test_title_flags_handle_negation_and_ambiguous_names():
    assert rs.title_polarity_flags("法人不看好 後市") == (False, False)
    assert rs.title_polarity_flags("利多 與 利空 並陳") == (False, False)
    assert rs.title_polarity_flags("營收創高 大漲") == (True, False)
    trie, codes = _trie()
    assert rs.match_codes("台泥 大漲", trie, codes, bare_codes=False) == set()
    assert rs.match_codes("買進2330", trie, codes, bare_codes=False) == set()
    assert rs.match_codes("買進2330", trie, codes, bare_codes=True) == {"2330"}


def test_news_window_start_covers_weekend_and_holiday_gaps():
    # first window session is a Monday: Friday's late titles and the weekend still count
    assert rs.news_window_start(_sessions()) == _dt.date(2026, 10, 30)
    assert rs.news_window_start(_sessions()[1:]) == _dt.date(2026, 10, 29)  # only five sessions
    assert rs.news_window_start(_sessions()[2:]) is None


def test_news_polarity_dedupes_syndicated_titles_and_treats_midnight_as_after_close():
    trie, codes = _trie()
    syndicated = [("鴻海精密 大漲", _dt.datetime(2026, 11, 5, 9, 0)), ("鴻海精密  大漲", _dt.datetime(2026, 11, 5, 9, 30))]
    row = rs.news_polarity_by_code(syndicated, _sessions(), trie, codes)["2317"]
    assert row["news_n"] == 1 and rs.news_polarity_direction(row) == 0
    # a date-only timestamp on the as-of date may be an evening article: it must not count
    date_only = [("鴻海精密 大漲", _dt.datetime(2026, 11, 6, 0, 0))]
    assert rs.news_polarity_by_code(date_only, _sessions(), trie, codes) == {}
    assert rs.title_polarity_flags("營收創新低") == (False, False)  # mixed words: neutral, never bullish
    assert rs.title_polarity_flags("股價創波段新低") == (False, True)
    assert rs.title_polarity_flags("營收創高") == (True, False)


def test_news_polarity_with_exactly_five_sessions_drops_prior_week_titles():
    trie, codes = _trie()
    five = _sessions()[1:]  # Mon 11-02 .. Fri 11-06
    titles = [
        ("鴻海精密 大漲", _dt.datetime(2026, 10, 29, 10, 0)),   # prior week: too old
        ("鴻海精密 大漲", _dt.datetime(2026, 10, 31, 10, 0)),   # Saturday before Monday: counts
    ]
    row = rs.news_polarity_by_code(titles, five, trie, codes)["2317"]
    assert row["news_n"] == 1


def test_valuation_families_are_value_signed_and_skip_nonpositive_denominators():
    scores = rs.collect_forward_scores(
        {}, {}, valuation_context={"pe_ratio": 20.0, "price_to_book": 2.0, "dividend_yield_pct": 4.0},
    )
    assert scores == {"earnings_yield": 5.0, "book_to_market": 0.5, "dividend_yield": 4.0}
    # loss-making (PE<=0 / missing) and negative-equity names get no score, never zero
    partial = rs.collect_forward_scores(
        {}, {}, valuation_context={"pe_ratio": 0, "price_to_book": -1.0, "dividend_yield_pct": 0.0},
    )
    assert partial == {}  # non-payers (0) stay unscored to avoid a tie block over the tail cut
    assert rs.collect_forward_scores({}, {}, valuation_context={"pe_ratio": float("nan")}) == {}
    for name in ("earnings_yield", "book_to_market", "dividend_yield"):
        spec = rs.FAMILY_BY_NAME[name]
        assert spec.sign == 1 and spec.forward_scoring


def test_valuation_directions_use_top_decile_as_bullish():
    scores = {f"{1000 + i}": {"earnings_yield": float(i)} for i in range(300)}
    directions, _ = rs.assign_forward_directions(scores, {code: "上市" for code in scores})
    assert directions["1299"]["research:earnings_yield"] == 1
    assert directions["1000"]["research:earnings_yield"] == -1
