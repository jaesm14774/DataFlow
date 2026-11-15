"""Shared lib helpers: discord_notifier, get_sql_from_config, replace_by_keys."""
from unittest.mock import MagicMock, patch

import pandas as pd

from lib.db_upsert import replace_by_keys
from lib.get_sql import get_sql_from_config
from lib import log_process_execution
from lib.log_process_execution import BaseLogRecord
from lib.notify import discord_notifier


def test_discord_notifier_reads_label_from_explicit_path(tmp_path):
    token_file = tmp_path / 'Discord.csv'
    token_file.write_text('name,token\nalpha,https://hook/alpha\nbeta,https://hook/beta\n',
                          encoding='utf_8_sig')

    notifier = discord_notifier('beta', str(token_file))

    assert notifier.webhook_url == 'https://hook/beta'


def test_discord_notifier_defaults_to_common_config(tmp_path):
    token_file = tmp_path / 'Discord.csv'
    token_file.write_text('name,token\nalpha,https://hook/alpha\n', encoding='utf_8_sig')

    with patch('common.config.discord_token_path', str(token_file)):
        assert discord_notifier('alpha').webhook_url == 'https://hook/alpha'


def test_get_sql_from_config_uses_given_path_and_database(tmp_path):
    cfg = tmp_path / 'sql_config.csv'
    cfg.write_text('name,value\nhost,db.local\nport,3307\nuser,u\npassword,p\n',
                   encoding='utf_8_sig')

    with patch('lib.get_sql.get_sql') as get_sql:
        get_sql.return_value = ('conn', 'cursor', 'engine')
        result = get_sql_from_config('stock', str(cfg))

    assert result == ('conn', 'cursor', 'engine')
    # values come straight from the CSV as strings; get_sql() does the int() conversion
    get_sql.assert_called_once_with('db.local', '3307', 'u', 'p', 'stock')


def test_replace_by_keys_truncates_deletes_then_inserts(monkeypatch):
    df = pd.DataFrame({'code': ['1101', '2330'], 'day': ['d1', 'd1'], 'value': [1, 2]})
    conn, cursor, engine = MagicMock(), MagicMock(), MagicMock()
    cursor.execute.side_effect = lambda sql: 2 if sql.lstrip().startswith('delete') else None
    written = []
    monkeypatch.setattr(
        pd.DataFrame, 'to_sql',
        lambda self, name, *args, **kwargs: written.append((name, list(self.columns))),
    )

    deleted, inserted = replace_by_keys(df, 'tbl', 'tbl_diff', ['code', 'day'],
                                        conn, cursor, engine)

    assert (deleted, inserted) == (2, 2)
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[0] == 'TRUNCATE TABLE tbl_diff'
    assert 'delete tb from tbl as tb' in statements[1]
    assert 'tb.code=tb2.code and tb.day=tb2.day' in statements[1]
    # diff table only receives the key columns; the main table receives the full frame
    assert written == [('tbl_diff', ['code', 'day']), ('tbl', ['code', 'day', 'value'])]
    assert conn.commit.call_count == 2


def test_replace_by_keys_diff_columns_can_be_a_subset_of_keys(monkeypatch):
    df = pd.DataFrame({'market': ['tpex'], 'trigger': [1]})
    conn, cursor, engine = MagicMock(), MagicMock(), MagicMock()
    cursor.execute.return_value = 0
    written = []
    monkeypatch.setattr(
        pd.DataFrame, 'to_sql',
        lambda self, name, *args, **kwargs: written.append((name, list(self.columns))),
    )

    replace_by_keys(df, 'tbl', 'tbl_diff', ['market', 'day'], conn, cursor, engine,
                    diff_columns=['market'])

    assert written[0] == ('tbl_diff', ['market'])


def test_failure_callback_names_the_failed_task_not_the_first_one():
    from lib.notify import DiscordNotify

    failed = MagicMock(run_id='r1', dag_id='d1', task_id='the_failed_task', operator='PythonOperator')
    first = MagicMock(task_id='first_task')
    dag_run = MagicMock()
    dag_run.get_task_instances.return_value = [first, failed]
    msg = DiscordNotify()._build_callback_msg({'task_instance': failed, 'dag_run': dag_run}, 'failed !!!')

    assert 'task_id : the_failed_task' in msg
    assert 'first_task' not in msg


def test_base_log_record_defaults_process_date_to_taipei_today(monkeypatch):
    import datetime
    monkeypatch.setattr(
        log_process_execution, 'taipei_now', lambda: datetime.datetime(2026, 3, 16, 23, 59, 1),
    )
    record = BaseLogRecord(app_name='demo')
    assert record.process_date == '2026-03-16'
    assert record.process_start_time == '2026-03-16 23:59:01'
    assert BaseLogRecord(app_name='demo', process_date='2026-03-16').process_date == '2026-03-16'
