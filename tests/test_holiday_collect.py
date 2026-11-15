"""collect_holiday_taiwan must never leave holiday.taiwan empty when a reload step fails."""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text

import collect_holiday_info as dag_module


def _engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'holiday.db'}")
    with engine.begin() as conn:
        conn.execute(text(
            "create table taiwan (now_date text, isholiday text, chinese_name text, "
            "holidayCategory text, description text)"
        ))
        conn.execute(text("insert into taiwan (now_date, chinese_name) values ('20260101', 'old row')"))
    return engine


def _patch_sources(monkeypatch, engine):
    api_rows = [{
        'date': '20991231', 'year': '2099', 'isHoliday': '是', 'chinese': '測試日',
    }]
    response = MagicMock()
    response.json.return_value = api_rows
    monkeypatch.setattr(dag_module.requests, 'get', lambda *a, **k: response)
    monkeypatch.setattr(
        dag_module, 'get_sql_from_config', lambda db: (MagicMock(), MagicMock(), engine),
    )


def test_failed_reload_rolls_back_to_previous_calendar(monkeypatch, tmp_path):
    engine = _engine(tmp_path)
    _patch_sources(monkeypatch, engine)
    # sqlite has no ON DUPLICATE KEY, so the closure-restore step fails after the reload succeeded
    with pytest.raises(Exception):
        dag_module.collect_holiday_taiwan()

    check = create_engine(f"sqlite:///{tmp_path / 'holiday.db'}")
    with check.connect() as conn:
        names = [row[0] for row in conn.execute(text("select chinese_name from taiwan"))]
    assert names == ['old row']


def test_successful_reload_replaces_rows_and_restores_closures(monkeypatch, tmp_path):
    engine = _engine(tmp_path)
    _patch_sources(monkeypatch, engine)
    monkeypatch.setattr(dag_module, 'MARKET_ONLY_CLOSURES', [])

    dag_module.collect_holiday_taiwan()

    check = create_engine(f"sqlite:///{tmp_path / 'holiday.db'}")
    with check.connect() as conn:
        names = [row[0] for row in conn.execute(text("select chinese_name from taiwan"))]
    assert names == ['測試日']
