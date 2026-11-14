"""Read-only recomputation and notification preview; never trigger or deliver."""
import datetime as dt
import json
from collections import Counter
from pathlib import Path
from airflow.models import DagBag
from source_extension.registry import _open_connection
import source_extension.market_signals as signals

conn, cursor, engine = _open_connection()
stock_conn, stock_cursor, stock_engine = signals._open_stock_connection()
report = {}
try:
    cursor.execute("""SELECT run_key,as_of_date,started_at,candidate_count,source_dag_id,source_dag_run_id
        FROM market_signal_runs WHERE scan_mode='daily' ORDER BY id DESC LIMIT 1""")
    latest = cursor.fetchone()
    now = signals._now_taipei()
    snapshots = {}
    for label, cutoff in (("historical_cutoff", latest[2]), ("available_now", now)):
        candidates, metrics = signals._load_daily_candidates(
            cursor, stock_cursor, today=signals._date_only(cutoff), started_at=cutoff,
            source_dag_id=latest[4], source_dag_run_id=latest[5], source_schedule_hint='daily_after_close',
        )
        snapshots[label] = {"cutoff_taipei": str(cutoff), "candidate_count": len(candidates),
                            "direction_counts": dict(Counter(c['details']['direction'] for c in candidates)),
                            "factor_candidate_counts": dict(Counter(f for c in candidates
                                for f in c['details']['family_directions'])),
                            "metrics": metrics, "candidates": candidates}
    report['latest_natural_run'] = latest
    report['snapshots'] = snapshots
    current = snapshots['available_now']
    metrics = current['metrics']
    for key, fn in (
        ('market_breadth_context', signals._load_daily_market_breadth),
        ('taifex_market_positioning', signals._load_taifex_market_positioning),
    ):
        args = (cursor, metrics['as_of_date'], now)
        if key == 'market_breadth_context':
            args += (metrics['universe_market_counts'],)
        metrics[key] = fn(*args)
    metrics['source_issues'] = signals._source_collection_issues(cursor, latest[5], 'daily_after_close', latest[4]) + metrics['market_coverage_issues']
    rows = [{**c, 'details_text': json.dumps(c['details'], ensure_ascii=False, default=str)} for c in current['candidates']]
    embeds = signals._format_daily_embeds(metrics, rows)
    message = signals._format_daily_message(metrics, rows)
    batches = signals._split_discord_embed_batches(message, embeds)
    report['preview'] = embeds
    report['preview_request_bytes'] = [signals._discord_request_size(message+f'｜第 {i+1}/{len(batches)} 批', b) for i,b in enumerate(batches)]
    assert all(n <= signals.DISCORD_REQUEST_BYTE_LIMIT for n in report['preview_request_bytes'])
    # Exercise every candidate's card, including omitted candidates, against
    # Discord's actual per-field and individual-request limits.
    report['all_candidate_cards_verified'] = len(rows)
    for row in rows:
        embed = signals._daily_candidate_embed(row, json.loads(row['details_text']))
        assert all(len(f['value']) <= 1024 for f in embed.get('fields', [])), row['entity_id']
        assert len(embed.get('fields', [])) <= 25
        signals._split_discord_embed_batches(message, [embed])
    class CoverageCursor:
        def executemany(self, query, values):
            assert query.count('%s') == len(values[0]); self.values = values
        def execute(self, query, params): pass
        def fetchall(self): return [v[1:2]+v[3:13] for v in self.values]
    fake = CoverageCursor()
    signals._persist_market_coverage(fake, run_key=latest[0], as_of_date=metrics['as_of_date'],
                                    coverage_by_market=metrics['market_coverage_by_market'], observed_at=now)
    assert signals._load_market_coverage(fake, latest[0]) == metrics['market_coverage_by_market']
    report['coverage_roundtrip_verified'] = True
    bag = DagBag(dag_folder='/opt/airflow/dags/market_signal_notify.py', include_examples=False)
    sources = DagBag(dag_folder='/opt/airflow/dags/source_extension/dag.py', include_examples=False)
    report['dag_import_errors'] = {**bag.import_errors, **sources.import_errors}
    assert not report['dag_import_errors']
    daily = sources.get_dag('source_extension_collect_after_close')
    assert 'trigger_market_signal_notify' in daily.get_task('wait_existing_equity_order_flow_collection').downstream_task_ids
    assert 'wait_existing_equity_order_flow_collection' not in bag.get_dag('market_signal_notify').task_ids
    for dag in sources.dags.values():
        if dag.dag_id != 'source_extension_collect_after_close':
            assert 'wait_existing_equity_order_flow_collection' not in dag.task_ids
    report['wait_sensor_verified'] = True
    report['wait_sensor_dag'] = daily.dag_id
    report['intraday_wait_absent'] = True
finally:
    stock_conn.close(); stock_engine.dispose(); conn.close(); engine.dispose()
out = Path('/tmp/equity_order_flow_preflight_20261008.json')
def json_safe(value):
    if isinstance(value, dict): return {str(k):json_safe(v) for k,v in value.items()}
    if isinstance(value, (list,tuple)): return [json_safe(v) for v in value]
    return value
out.write_text(json.dumps(json_safe(report),ensure_ascii=False,default=str,indent=2),encoding='utf-8')
print(json.dumps({
    'latest_natural_run':report['latest_natural_run'],
    'snapshots':{label:{k:v for k,v in data.items() if k not in ('metrics','candidates')}
                 for label,data in report['snapshots'].items()},
    'coverage':report['snapshots']['available_now']['metrics']['market_coverage_by_market'],
    'preview_request_bytes':report['preview_request_bytes'],
    'all_candidate_cards_verified':report['all_candidate_cards_verified'],
    'coverage_roundtrip_verified':report['coverage_roundtrip_verified'],
    'dag_import_errors':report['dag_import_errors'], 'output':str(out),
},ensure_ascii=False,default=str))
