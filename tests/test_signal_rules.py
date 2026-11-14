import numpy as np
import pandas as pd
import pytest

from market_fixture import AS_OF, make_market, truncate
from source_extension import rule_detection, rule_lifecycle, signal_rules


@pytest.fixture(scope="module")
def market():
    return make_market()


def _panel(data, **kwargs):
    return signal_rules.compute_panel(
        data["price"], data["inst"], data["margin"], data["tdcc"], with_forward=False, **kwargs
    )


def test_margin_signal_is_attributed_to_the_session_after_its_data_date(market):
    """Margin balances land the next morning, so a margin rule fires one session later
    than the data date; entry therefore also moves one session later."""
    code, pos = market["codes"][0], 200
    days = market["days"]
    price, margin = market["price"].copy(), market["margin"].copy()
    on_code = price["code"] == code
    price.loc[on_code & (price["date"] == days[pos]), "close"] = (
        price.loc[on_code & (price["date"] == days[pos - 5]), "close"].iloc[0] * 1.06
    )
    margin_on = margin["code"] == code
    margin.loc[margin_on & (margin["date"] == days[pos]), "margin_bal"] = (
        margin.loc[margin_on & (margin["date"] == days[pos - 5]), "margin_bal"].iloc[0] * 0.85
    )
    data = {**market, "price": price, "margin": margin}
    immediate = _panel(data, margin_lag=0).signals["margin_cleanup_up"]
    proper = _panel(data).signals["margin_cleanup_up"]
    assert immediate.loc[days[pos], code]
    assert proper.loc[days[pos + 1], code]
    # the lag is a pure shift: nothing is created or lost, only moved to when it is knowable
    warm = 30  # the liquidity filter needs a few sessions of history before it can say yes
    assert (proper.iloc[warm + 1:].to_numpy() == immediate.iloc[warm:-1].to_numpy()).all()


def test_tdcc_signal_waits_for_its_publication_lag(market):
    days, code = market["days"], market["codes"][1]
    fridays = [d for d in days if d.weekday() == 4]
    friday = fridays[20]
    tdcc = market["tdcc"].copy()
    on_code = tdcc["code"] == code
    previous = tdcc.loc[on_code & (tdcc["date"] == fridays[19]), "big_pct"].iloc[0]
    tdcc.loc[on_code & (tdcc["date"] == friday), "big_pct"] = previous + 3.0
    data = {**market, "tdcc": tdcc}
    pos = days.get_loc(friday)
    lagged = _panel(data).signals["tdcc_big_holder_up"]
    instant = _panel(data, tdcc_lag=0).signals["tdcc_big_holder_up"]
    assert instant.loc[friday, code]
    assert not lagged.loc[friday, code]
    assert lagged.loc[days[pos + 3], code]


def test_rule_hits_never_use_information_from_after_the_signal_date(market):
    """Point in time: computing on data cut at day T gives the same hits at T as the full history."""
    full = _panel(market)
    days = market["days"]
    checked = 0
    for pos in (150, 220, 300):
        cut = _panel(truncate(market, days[pos]))
        for rule in signal_rules.RULES:
            expected = set(full.signals[rule.id].loc[days[pos]].pipe(lambda s: s[s].index))
            got = set(cut.signals[rule.id].loc[days[pos]].pipe(lambda s: s[s].index))
            assert got == expected, (rule.id, days[pos])
            checked += len(expected)
    assert checked > 0  # the comparison must not be vacuous


def test_a_rolling_window_reproduces_full_history_evidence_rows(market):
    """The weekly job only re-reads a recent window; it must give the same rows as a full run."""
    days = market["days"]
    start, end = days[230], days[290]
    full = rule_lifecycle.daily_excess_table(signal_rules.compute_panel(
        market["price"], market["inst"], market["margin"], market["tdcc"]
    ))
    window_data = truncate(
        market, end + pd.Timedelta(days=60),
        first_day=start - pd.Timedelta(days=60 + 200),
    )
    windowed = rule_lifecycle.daily_excess_table(signal_rules.compute_panel(
        window_data["price"], window_data["inst"], window_data["margin"], window_data["tdcc"]
    ))
    key = ["rule_id", "horizon", "signal_date"]
    inside = lambda frame: frame[(frame["signal_date"] >= start) & (frame["signal_date"] <= end)]
    a = inside(full).sort_values(key).reset_index(drop=True)
    b = inside(windowed).sort_values(key).reset_index(drop=True)
    assert len(a) > 500
    pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-9)


def test_detection_reports_feed_not_landed_instead_of_no_hit(market):
    data = truncate(market, AS_OF)
    days = market["days"]
    inst = data["inst"][data["inst"]["date"] != days[-1]]
    margin = data["margin"][data["margin"]["date"] != days[-2]]  # margin of the previous session is what rules need
    detection = rule_detection.detect_rules({**data, "inst": inst, "margin": margin}, AS_OF.date())
    assert detection["status"] == "ok"
    rules = detection["rules"]
    for rule in signal_rules.RULES:
        result = rules[rule.id]
        needs_missing = ("inst" in rule.sources) or ("margin" in rule.sources)
        if needs_missing:
            assert result["status"] == "data_not_ready", rule.id
            assert result["hits"] == [] and result["missing_feeds"]
        else:
            assert result["status"] == "ok", rule.id


def test_a_stale_weekly_holder_feed_pauses_only_tdcc_rules(market):
    data = truncate(market, AS_OF)
    stale = data["tdcc"][data["tdcc"]["date"] < AS_OF - pd.Timedelta(days=40)]
    detection = rule_detection.detect_rules({**data, "tdcc": stale}, AS_OF.date())
    assert detection["rules"]["tdcc_big_holder_up"]["status"] == "data_not_ready"
    assert detection["rules"]["breakout_60d_volume"]["status"] == "ok"


def test_partial_price_landing_is_not_ready_for_any_rule(market):
    data = truncate(market, AS_OF)
    price = data["price"]
    keep = price[(price["date"] < AS_OF) | (price["code"].isin(market["codes"][:3]))]
    detection = rule_detection.detect_rules({**data, "price": keep}, AS_OF.date())
    assert detection["status"] == "data_not_ready" and detection["rules"] == {}


def test_missing_as_of_prices_is_not_ready(market):
    data = truncate(market, AS_OF - pd.Timedelta(days=1))
    detection = rule_detection.detect_rules(data, AS_OF.date())
    assert detection["status"] == "data_not_ready"


def test_a_real_breakout_is_found_with_a_finite_volume_multiple(market):
    code = market["codes"][2]
    days = market["days"]
    price = market["price"].copy()
    prior_high = price[(price["code"] == code) & (price["date"] < days[-1])]["close"].max()
    mask = (price["code"] == code) & (price["date"] == days[-1])
    price.loc[mask, "close"] = prior_high * 1.05
    price.loc[mask, "vol"] = 30_000.0
    detection = rule_detection.detect_rules({**market, "price": price}, AS_OF.date(), ["breakout_60d_volume"])
    hits = detection["rules"]["breakout_60d_volume"]["hits"]
    assert [hit["code"] for hit in hits] == [code]
    assert np.isfinite(hits[0]["metric"]) and hits[0]["metric"] == pytest.approx(3.0, rel=0.25)
    assert detection["breadth"]["stocks"] == len(market["codes"])


def test_zero_prior_volume_cannot_create_an_infinite_volume_multiple(market):
    code = market["codes"][3]
    price = market["price"].copy()
    before = (price["code"] == code) & (price["date"] < market["days"][-1])
    price.loc[before, "vol"] = 0.0
    price.loc[(price["code"] == code) & (price["date"] == market["days"][-1]), ["close", "vol"]] = [500.0, 90_000.0]
    detection = rule_detection.detect_rules({**market, "price": price}, AS_OF.date())
    for result in detection["rules"].values():
        assert all(np.isfinite(hit["metric"]) for hit in result["hits"] if hit["metric"] is not None)


def test_margin_rule_ranking_value_is_the_one_that_was_knowable_at_signal_time(market):
    """Margin data of the signal session has not landed yet; the rank metric must come from the data the signal used."""
    days = market["days"]
    full = _panel(market)
    pos = next(
        i for i in range(150, 330)
        if full.signals["margin_cleanup_up"].iloc[i].any() and full.signals["short_squeeze_setup"].iloc[i].any()
    )
    data = truncate(market, days[pos])
    data["margin"] = data["margin"][data["margin"]["date"] != days[pos]]  # today's margin file not in yet
    panel = _panel(data)
    for rule_id in ("margin_cleanup_up", "short_squeeze_setup"):
        hits = signal_rules.rule_hits(panel, rule_id, days[pos])
        assert hits and all(hit["metric"] is not None for hit in hits)


def test_a_missing_weekly_holder_snapshot_is_not_reported_as_no_change():
    """Wednesday's session is the one where Friday's snapshot becomes usable (3-session lag)."""
    wednesday = pd.Timestamp("2026-11-18")
    market = make_market(end=wednesday)
    friday = pd.Timestamp("2026-11-13")
    assert (market["tdcc"]["date"] == friday).any()
    ok = rule_detection.detect_rules(market, wednesday.date())
    assert ok["rules"]["tdcc_big_holder_up"]["status"] == "ok"
    missing = {**market, "tdcc": market["tdcc"][market["tdcc"]["date"] != friday]}
    late = rule_detection.detect_rules(missing, wednesday.date())
    assert late["rules"]["tdcc_big_holder_up"]["status"] == "data_not_ready"
    assert late["rules"]["breakout_60d_volume"]["status"] == "ok"
