"""Read-only official adapter/card preflight. Does not ingest, trigger or send."""
import json
from collections import Counter
from pathlib import Path

from airflow.models import DagBag
from source_extension.adapters import fetch_and_normalize, validate_source_contract
from source_extension.registry import (
    SOURCE_DEFINITIONS, SOURCE_DEFINITIONS_BY_KEY, _open_connection,
    _required_fields_missing, discover_approved_source_keys,
)
import source_extension.market_signals as signals

conn, cursor, engine = signals._open_stock_connection()
try:
    cursor.execute("SELECT TRIM(證券代號),TRIM(市場別) FROM 全證券代碼相關資料表 WHERE 市場別 IN ('上市','上櫃')")
    allowed = set(cursor.fetchall())
finally:
    conn.close(); engine.dispose()

report = {"mode": "read_only_live_preflight_not_natural_ingestion",
          "cutoff_taipei": str(signals._now_taipei()), "sources": {}}
status_by_code = {}
for key in ("twse_trading_status_daily", "tpex_trading_status_daily"):
    source = SOURCE_DEFINITIONS_BY_KEY[key]
    assert source['schedule_hint'] == 'daily_after_close' and validate_source_contract(source)
    records, status = fetch_and_normalize(source, allowed_stock_market_codes=allowed)
    fields = [{"source_field": f[0], "required_flag": bool(f[4])} for f in source['fields']]
    assert all(not _required_fields_missing({"fields": fields}, row) for row in records)
    assert len({row['entity_id'] for row in records}) == len(records)
    assert all(row['quality_status'] == 'ok' for row in records)
    flagged = {}
    for row in records:
        details = json.loads(row['value_text'])
        context = {'event_type': row['event_type'], 'effective_date': str(row['effective_date']),
                   'details': details, 'source_url': row['source_url']}
        status_by_code[row['entity_id']] = context
        if details['listed_in_restricted_snapshot']:
            flagged[row['entity_id']] = details
    report['sources'][key] = {'http_status': status, 'configured_schedule': source['schedule_hint'],
        'snapshot_records_including_explicit_absences': len(records),
        'matched_restricted_code_count': len(flagged),
        'date_basis': source['config']['date_basis'],
        'effective_dates': sorted({str(row['effective_date']) for row in records}),
        'flagged_stocks': flagged, 'source_url': source['endpoint_url']}

for title, body in (("訂單說明", "本公司尚未接獲訂單"), ("藥證說明", "本公司未取得藥證"),
                    ("盈虧說明", "本公司尚未轉虧為盈"), ("風險說明", "本公司並無虧損")):
    result = signals._classify_company_event(title, body)
    assert result['direction'] not in ('偏多候選', '偏空'), result
assert signals._classify_company_event("訂單公告", "本公司接獲訂單")['direction'] == '偏多候選'
report['negated_catalysts_verified'] = True
for context in status_by_code.values():
    if not signals.trading_risk_action([context]):
        continue
    for direction in ('偏多', '偏空', '訊號分歧'):
        action, _ = signals._daily_candidate_action({
            'direction': direction, 'regulatory_context': [context],
            'price_levels': {'return_5d_pct': 30., 'relative_5d_return_pp': 10.},
        })
        assert '停止交易' in action or '暫緩新增' in action
report['restricted_stock_action_override_verified'] = True

prior = json.loads(Path('/tmp/equity_order_flow_preflight_20261008.json').read_text(encoding='utf8'))
current = prior['snapshots']['available_now']
report['recomputation_cutoff'] = current['cutoff_taipei']
report['candidate_count'] = current['candidate_count']
report['direction_counts'] = current['direction_counts']
report['history_qualification'] = current['metrics'].get('history_qualification')
report['policy_version'] = current['metrics'].get('policy_version')
report['risk_preview_changes'] = {}
for candidate in current['candidates']:
    detail = candidate['details']
    code = candidate['entity_id']
    # Separate what-if preview from currently persisted source snapshots.
    detail['regulatory_context'] = [status_by_code[code]] + detail.get('regulatory_context', [])
    if signals.trading_risk_action(detail['regulatory_context']):
        report['risk_preview_changes'][code] = signals._daily_candidate_action(detail)[0]
    embed = signals._daily_candidate_embed(candidate, detail)
    assert all(len(field['value']) <= 1024 for field in embed['fields'])
    signals._split_discord_embed_batches('唯讀預覽', [embed])
report['live_risk_overlay_cards_verified'] = current['candidate_count']
report['configured_registry_count'] = len(SOURCE_DEFINITIONS)
keys = discover_approved_source_keys('daily_after_close')
assert {'twse_trading_status_daily', 'tpex_trading_status_daily'}.issubset(keys)
report['runtime_after_close_mapped_source_count'] = len(keys)
bag = DagBag(dag_folder='/opt/airflow/dags/source_extension/dag.py', include_examples=False)
assert not bag.import_errors
report['dag_import_errors'] = bag.import_errors
report['after_close_schedule'] = "0 20 * * 1-5"
report['after_close_timezone'] = "Asia/Taipei"
source_conn, source_cursor, source_engine = _open_connection()
try:
    source_cursor.execute("SELECT source_key,schedule_hint,enabled FROM source_registry WHERE source_key IN ('twse_trading_status_daily','tpex_trading_status_daily')")
    report['naturally_bootstrapped_catalog_rows'] = source_cursor.fetchall()
finally:
    source_conn.close(); source_engine.dispose()
out = Path('/tmp/stock_signal_policy_validation_20261008.json')
out.write_text(json.dumps(report,ensure_ascii=False,default=str,indent=2),encoding='utf8')
print(json.dumps({k:v for k,v in report.items() if k != 'sources'},ensure_ascii=False,default=str))
print(json.dumps({'sources': {key:{k:v for k,v in value.items() if k!='flagged_stocks'}
                             for key,value in report['sources'].items()}},ensure_ascii=False))
