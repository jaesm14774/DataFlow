"""Existing date-bounded history must complete windows without replacing live quotes."""
import datetime as dt
import source_extension.signal_policy as policy

END = dt.date(2026, 10, 15)
DAYS = [dt.date(2026, 10, d) for d in (8, 12, 13, 14, 15)]
CUTOFF = dt.datetime(2026, 10, 15, 13)
MASTER = {'2330': {'market': '上市'}, '3105': {'market': '上櫃'}}


def merge(current, stored, **kwargs):
    # The pre-fix scanner used current source rows without the stored history.
    fn = getattr(policy, 'merge_stored_quote_history', lambda a,b,**kw:(a,{}))
    return fn(current, stored, master=MASTER, cutoff_utc=CUTOFF, **kwargs)


def row(day, code='2330', **changes):
    return dict(code=code, date=day, close=50., volume_lots=1000.,
                captured_at=CUTOFF-dt.timedelta(seconds=1), **changes)


def test_existing_earlier_rows_complete_window_with_lots_converted_to_shares():
    current = {'2330': [(d,50.,1_000_000.,False) for d in DAYS[-2:]]}
    merged, provenance = merge(current,[row(d) for d in DAYS[:-2]])
    assert len(policy.quote_window(merged['2330'],DAYS,5)) == 5
    assert all(r[2]==1_000_000 for r in merged['2330'])
    assert provenance['2330']==3
    assert len(current['2330'])==2  # Caller-owned data is preserved.


def test_current_source_rows_and_explicit_quarantined_dates_are_never_overwritten():
    current = {'2330': [(DAYS[1],50.,1_000_000.,False), (END,None,0,True)]}
    merged, provenance = merge(current,[row(DAYS[0]), row(DAYS[2]), row(END)],
                               known_quote_keys={('2330',DAYS[2])})
    assert len(merged['2330'])==3
    assert merged['2330'][-1] == current['2330'][-1]
    assert DAYS[2] not in {r[0] for r in merged['2330']}


def test_stored_data_fills_a_market_session_missing_from_the_new_feed():
    current = {'2330': [(DAYS[0],50.,1_000_000.,False),(END,50.,1_000_000.,False)]}
    merged, provenance = merge(current,[row(d) for d in DAYS[1:-1]])
    assert len(policy.quote_window(merged['2330'],DAYS,5))==5
    assert provenance['2330']==3


def test_duplicate_late_bad_and_unknown_market_rows_do_not_certify_history():
    current = {'2330': [(END,50.,1_000_000.,False)]}
    rows = [row(DAYS[0]),row(DAYS[0]),dict(row(DAYS[1]),captured_at=CUTOFF+dt.timedelta(seconds=1)),
            dict(row(DAYS[2]),volume_lots=-1), row(DAYS[3],code='9999')]
    merged, provenance = merge(current,rows)
    assert merged == current
    assert not provenance
    merged, provenance = merge(current,[row(DAYS[0])])
    assert len(merged['2330'])==2


def test_one_market_cannot_supply_another_markets_missing_source_boundary():
    current = {'2330': [(END,50.,1_000_000.,False)]}
    merged, provenance = merge(current,[row(DAYS[0],code='3105')])
    assert '3105' not in merged
    assert not provenance
