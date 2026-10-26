"""Available institutional data must be read and evaluated, not policy-disabled."""
import datetime as dt
import json

from source_extension.equity_order_flow import (
    institutional_signal, load_equity_order_flow, select_institutional_observations,
)
from source_extension.institutional_flow import format_flow_readiness

DAY = dt.date(2026, 10, 15)
CUTOFF = dt.datetime(2026, 10, 15, 13)
DAYS = [dt.date(2026, 10, d) for d in (8, 12, 13, 14, 15)]
MASTER = {'2330': {'market': '上市'}, '3105': {'market': '上櫃'}}
PRICES = [(day, 50., 1_000_000., False) for day in DAYS]


def stored(day=DAY, code='2330', **updates):
    return dict(code=code, date=day, total=300000, foreign=100000,
                trust=200000, dealer=0, captured_at=CUTOFF-dt.timedelta(minutes=1),
                **updates)


def official(day=DAY, **updates):
    row = dict(entity_id='3105', effective_date=day, quality_status='ok', unit='shares',
               captured_at=CUTOFF-dt.timedelta(minutes=1), capture_basis='source_feed',
               dag_id='source_extension_collect_after_close', dag_run_id='scheduled__test',
               successful_source_run=True, value_text=json.dumps(dict(
                   market='上櫃', three_institution_total_net_shares=300000,
                   foreign_net_shares_excluding_foreign_dealers=100000,
                   investment_trust_net_shares=200000, dealers_net_shares=0)))
    row.update(updates)
    return row


def test_existing_listed_table_is_accepted_with_collection_time_provenance():
    observations, rejected = select_institutional_observations([], [stored()], master=MASTER,
        as_of_date=DAY, cutoff_utc=CUTOFF)
    assert observations['2330'][DAY]['total'] == 300000
    assert observations['2330'][DAY]['natural_pit'] is False
    assert observations['2330'][DAY]['capture_basis'] == 'stored_collection_time'
    assert not rejected


def test_available_institutional_panel_votes_without_claiming_predictive_validation():
    observations, _ = select_institutional_observations([], [stored(d) for d in DAYS],
        master=MASTER, as_of_date=DAY, cutoff_utc=CUTOFF)
    result = institutional_signal(observations.get('2330', {}), PRICES, DAYS, DAY)
    assert result['vote_enabled'] is True
    assert result['direction'] == 1
    assert result['selected_window']['days'] == 5
    assert result['selected_window']['natural_pit_complete'] is False


def test_neutral_valid_data_is_readable_and_not_counted_as_a_missing_source():
    rows = [dict(stored(d), total=0, foreign=-200000) for d in DAYS]
    observations, _ = select_institutional_observations([], rows, master=MASTER,
        as_of_date=DAY, cutoff_utc=CUTOFF)
    result = institutional_signal(observations.get('2330', {}), PRICES, DAYS, DAY)
    assert result['vote_enabled'] is True
    assert result['direction'] == 0
    assert result['status'] == 'below_threshold'


def test_quarantine_is_not_hidden_by_stored_otc_fallback_and_bad_rows_stay_excluded():
    observations, _ = select_institutional_observations([official(quality_status='quarantine')],
        [stored(code='3105')], master=MASTER, as_of_date=DAY, cutoff_utc=CUTOFF)
    assert '3105' not in observations
    for bad in (dict(stored(), foreign=123), dict(stored(), total=None),
                dict(stored(), captured_at=CUTOFF+dt.timedelta(seconds=1)),
                stored(DAY+dt.timedelta(days=1))):
        observations, _ = select_institutional_observations([], [bad], master=MASTER,
            as_of_date=DAY, cutoff_utc=CUTOFF)
        assert not observations
    observations, _ = select_institutional_observations([], [stored(), stored()],
        master=MASTER, as_of_date=DAY, cutoff_utc=CUTOFF)
    assert not observations


def test_both_markets_activate_and_missing_otc_branches_only_skips_that_component():
    official_rows = [official(d) for d in DAYS]
    class Sources:
        def execute(self, sql, params): self.sql = sql
        def fetchall(self):
            if 'holiday.taiwan' in self.sql:
                return [(DAY-dt.timedelta(days=i),) for i in range(45)
                        if (DAY-dt.timedelta(days=i)).weekday() >= 5] + [(dt.date(2026,10,9),)]
            return [(str(i), row['entity_id'], row['effective_date'], row['quality_status'],
                row['unit'], row['value_text'], row['captured_at'], row['capture_basis'],
                row['dag_id'], row['dag_run_id'], row['successful_source_run'])
                for i,row in enumerate(official_rows)]
    class Stocks:
        def execute(self, sql, params): self.sql = sql
        def fetchall(self):
            if 'FROM 三大法人買賣' in self.sql:
                return [('2330', d, 300000, 100000, 200000, 0, CUTOFF) for d in DAYS]
            return []
    contexts, readiness = load_equity_order_flow(Sources(), Stocks(), master=MASTER,
        as_of_date=DAY, cutoff_utc=CUTOFF, price_rows_by_code={c:PRICES for c in MASTER},
        session_dates_by_market={m:set(DAYS) for m in ('上市','上櫃')})
    for market, code in [('上市','2330'),('上櫃','3105')]:
        assert contexts[code]['institutions']['direction'] == 1
        assert readiness[market]['same_day_count'] == 1
        assert readiness[market]['direction_usable_count'] == 1
        assert readiness[market]['qualified_vote_count'] == 1
        assert '已納入判讀' in format_flow_readiness(market,readiness[market],1)
    assert contexts['3105']['broker_branches']['direction'] == 0
    assert readiness['上櫃']['broker']['status'] == 'source_unavailable_skipped'
    assert '上櫃券商分點買賣資料未收集' in format_flow_readiness('上櫃',readiness['上櫃'],1)
