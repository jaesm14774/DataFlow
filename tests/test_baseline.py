"""Baseline tests for small pure helpers shared across the DAG modules."""
from unittest.mock import patch, MagicMock

import numpy as np
import pytest

# ── lib/common_tool.py ──
from lib.common_tool import task_wrapper


class TestTaskWrapper:
    def test_calls_wrapped_function(self):
        called = []

        @task_wrapper
        def dummy(x, y):
            called.append((x, y))
            return x + y

        result = dummy(1, 2)
        assert result == 3
        assert called == [(1, 2)]

    def test_filters_unexpected_kwargs(self):
        @task_wrapper
        def dummy(a):
            return a

        result = dummy(a=10, unexpected_kwarg=99)
        assert result == 10

    def test_gc_runs_after_exception(self):
        @task_wrapper
        def explode():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            explode()

    def test_preserves_function_metadata(self):
        @task_wrapper
        def my_func():
            """docstring"""
            pass

        assert my_func.__name__ == 'my_func'
        assert my_func.__doc__ == 'docstring'


# ── lib/db_connection.py context manager test ──
class TestMySQLConnectionContextManager:
    @patch('lib.get_sql.pd.read_csv')
    @patch('lib.get_sql.pymysql.connect')
    @patch('lib.get_sql.create_engine')
    def test_context_manager_calls_end(self, mock_engine, mock_connect, mock_csv):
        loc_data = {
            ('host', 'value'): 'localhost',
            ('port', 'value'): '3306',
            ('user', 'value'): 'root',
            ('password', 'value'): 'pass',
        }
        mock_df = MagicMock()
        mock_df.loc.__getitem__ = MagicMock(side_effect=lambda key: loc_data[key])
        mock_csv.return_value = mock_df

        from lib.db_connection import MySQLConnection
        with MySQLConnection(db_name='test_db', sql_configure_path='fake.csv') as db:
            assert db.db_name == 'test_db'
        mock_connect.return_value.close.assert_called_once()
        mock_engine.return_value.dispose.assert_called_once()


# ── stock/parsing.py (shared by stock collect/calculate/notify modules) ──
from stock.parsing import (
    KEEP, clean_comma, make_regular_date_format,
    roc_to_gregorian_text, gregorian_to_roc_text,
)


class TestCleanComma:
    def test_remove_comma(self):
        assert clean_comma('1,234') == '1234'

    def test_no_comma(self):
        assert clean_comma('1234') == '1234'

    def test_output_int(self):
        assert clean_comma('1,234', output_not_string=True) == 1234

    def test_output_float(self):
        assert clean_comma('1,234.5', output_not_string=True) == 1234.5

    def test_non_numeric_returns_nan(self):
        assert np.isnan(clean_comma('abc', output_not_string=True))

    def test_non_numeric_can_keep_original_text(self):
        assert clean_comma('a,bc', output_not_string=True, unparsable=KEEP) == 'abc'

    def test_whitespace_stripped(self):
        assert clean_comma('  1,000  ') == '1000'


class TestMakeRegularDateFormat:
    def test_roc_to_western(self):
        assert make_regular_date_format('112/03/15') == '2023-03-15'

    def test_western_date_slash(self):
        assert make_regular_date_format('2023/03/15') == '2023-03-15'

    def test_western_date_dash(self):
        assert make_regular_date_format('2023-03-15') == '2023-03-15'

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError):
            make_regular_date_format('20230315')


class TestRocYearText:
    def test_roc_to_gregorian(self):
        assert roc_to_gregorian_text('112/03/15') == '2023/03/15'

    def test_gregorian_to_roc(self):
        assert gregorian_to_roc_text('2023/03/15') == '112/03/15'


# ── Pure functions from news_ch/utils.py ──
from news_ch.utils import is_valid_url, clean_text, clean_time_format


class TestIsValidUrl:
    def test_valid_https(self):
        assert is_valid_url('https://www.google.com') is True

    def test_valid_http(self):
        assert is_valid_url('http://example.com/path') is True

    def test_empty(self):
        assert is_valid_url('') is False

    def test_none(self):
        assert is_valid_url(None) is False

    def test_no_scheme(self):
        assert is_valid_url('www.google.com') is False


class TestCleanText:
    def test_remove_double_quote(self):
        assert clean_text('hello "world"') == 'hello world'

    def test_custom_sign(self):
        assert clean_text('a#b#c', special_sign='#') == 'abc'


class TestCleanTimeFormat:
    def test_standard_format(self):
        assert clean_time_format('2023-03-15 10:30:00') == '2023-03-15 10:30:00'

    def test_slash_format(self):
        assert clean_time_format('2023/03/15 10:30:00') == '2023-03-15 10:30:00'

    def test_no_seconds(self):
        assert clean_time_format('2023-03-15 10:30') == '2023-03-15 10:30:00'

    def test_leading_zero_returns_none(self):
        assert clean_time_format('0023-03-15 10:30:00') is None

    def test_invalid_returns_none(self):
        assert clean_time_format('not-a-date') is None


# ── news_ch/news_crawler/base_process.py ──
from news_ch.news_crawler.base_process import NewsLogic

_ARTICLE_COLUMNS = [
    'article_id', 'title', 'created_at', 'create_time',
    'content', 'author', 'category', 'keyword',
    'article_url', 'img', 'source',
]


def _build_article(source='s', now_time='now', **overrides):
    logic = NewsLogic()
    logic._source = source
    logic._now_time = now_time
    fields = dict(
        article_id='1', title='T', created_at='2023-01-01', content='C', author='A',
        category='Cat', keyword='K', article_url='https://ex.com', img='i',
    )
    fields.update(overrides)
    return logic.build_article_dataframe(**fields)


class TestBuildArticleDataframe:
    def test_returns_expected_columns(self):
        assert list(_build_article().columns) == _ARTICLE_COLUMNS

    def test_source_injected(self):
        assert _build_article(source='my_src')['source'].iloc[0] == 'my_src'

    def test_single_row_index_1(self):
        df = _build_article()
        assert len(df) == 1
        assert df.index[0] == 1

    def test_create_time_uses_now_time(self):
        df = _build_article(now_time='2025-06-15 08:00:00')
        assert df['create_time'].iloc[0] == '2025-06-15 08:00:00'
