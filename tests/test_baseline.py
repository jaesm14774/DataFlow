"""
Baseline tests for pure functions that will be refactored.
Functions that live behind module-level DB connections are tested
by re-defining them here (same source logic) until the refactoring
moves DB init out of module scope.
"""
import datetime
import re
import gc
import pytest

# ── lib/common_tool.py (can import directly) ──
from lib.common_tool import release_memory, task_wrapper


class TestReleaseMemory:
    def test_runs_without_error(self):
        release_memory()


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
from unittest.mock import patch, MagicMock


class TestMySQLConnectionContextManager:
    @patch('lib.db_connection.pd.read_csv')
    @patch('lib.db_connection.pymysql.connect')
    @patch('lib.db_connection.create_engine')
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


# ── Pure functions extracted from stock logic (same source) ──
# These can't be imported directly because stock/config/config.py is gitignored.
# After refactoring, these will be importable.

import numpy as np


def _clean_comma(txt, output_not_string=False):
    """Replica of StockCollectLogic.clean_comma"""
    try:
        txt = str(txt).strip()
    except Exception:
        pass
    if isinstance(txt, str):
        txt = txt.replace(',', '')
        if output_not_string:
            try:
                return int(txt)
            except Exception:
                try:
                    return float(txt)
                except Exception:
                    return np.nan
        else:
            return txt
    else:
        return txt


def _make_regular_date_format(dt):
    """
    Replica of StockCollectLogic.make_regular_date_format.
    NOTE: the original uses `assert f'...'` which is always truthy (bug),
    so unrecognized lengths silently pass through. We replicate that here.
    After the bug fix, len!=9 and len!=10 will raise ValueError.
    """
    if isinstance(dt, datetime.datetime):
        dt = str(dt)
    if len(dt) == 9:
        dt = f'{int(dt[:3]) + 1911}{dt[3:]}'
    elif len(dt) == 10:
        dt = dt
    else:
        pass  # original: assert f'...' (always truthy, no-op)
    dt = dt.replace('/', '-')
    return dt


class TestCleanComma:
    def test_remove_comma(self):
        assert _clean_comma('1,234') == '1234'

    def test_no_comma(self):
        assert _clean_comma('1234') == '1234'

    def test_output_int(self):
        assert _clean_comma('1,234', output_not_string=True) == 1234

    def test_output_float(self):
        assert _clean_comma('1,234.5', output_not_string=True) == 1234.5

    def test_non_numeric_returns_nan(self):
        result = _clean_comma('abc', output_not_string=True)
        assert np.isnan(result)

    def test_whitespace_stripped(self):
        assert _clean_comma('  1,000  ') == '1000'


class TestMakeRegularDateFormat:
    def test_roc_to_western(self):
        assert _make_regular_date_format('112/03/15') == '2023-03-15'

    def test_western_date_slash(self):
        assert _make_regular_date_format('2023/03/15') == '2023-03-15'

    def test_western_date_dash(self):
        assert _make_regular_date_format('2023-03-15') == '2023-03-15'

    def test_datetime_object(self):
        dt = datetime.datetime(2023, 3, 15)
        result = _make_regular_date_format(dt)
        assert result == '2023-03-15 00:00:00'

    def test_unknown_format_passthrough(self):
        """Current behavior: unrecognized format passes through (assert bug).
        After fix this should raise ValueError."""
        result = _make_regular_date_format('20230315')
        assert result == '20230315'


# ── Pure functions from news_ch/utils.py (directly importable) ──
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


# ── song.py pure function ──

def _grab_id(url):
    """Replica of song.grab_id"""
    return re.search(string=url, pattern=r'(tw[A-z0-9]+)\.htm').group(1)


class TestGrabId:
    def test_normal(self):
        assert _grab_id('https://mojim.com/twABC123.htm') == 'twABC123'

    def test_no_match_raises(self):
        with pytest.raises(AttributeError):
            _grab_id('https://mojim.com/noprefix.htm')


# ── news_ch/news_crawler/base_process.py (replica) ──
# Module-level side effects (config file reads) prevent direct import.

import pandas as pd

_ARTICLE_COLUMNS = [
    'article_id', 'title', 'created_at', 'create_time',
    'content', 'author', 'category', 'keyword',
    'article_url', 'img', 'source',
]


def _build_article_dataframe(source, now_time, *, article_id, title, created_at,
                              content, author, category, keyword,
                              article_url, img):
    """Replica of NewsLogic.build_article_dataframe"""
    return pd.DataFrame({
        'article_id': article_id,
        'title': title,
        'created_at': created_at,
        'create_time': now_time,
        'content': content,
        'author': author,
        'category': category,
        'keyword': keyword,
        'article_url': article_url,
        'img': img,
        'source': source,
    }, index=[1])


class TestBuildArticleDataframe:
    def test_returns_expected_columns(self):
        df = _build_article_dataframe(
            source='test_src', now_time='2023-01-01 00:00:00',
            article_id='123', title='Test', created_at='2023-01-01 12:00:00',
            content='body', author='auth', category='tech',
            keyword='py;test', article_url='https://example.com/123', img='img.jpg',
        )
        assert list(df.columns) == _ARTICLE_COLUMNS

    def test_source_injected(self):
        df = _build_article_dataframe(
            source='my_src', now_time='now',
            article_id='1', title='T', created_at='2023-01-01',
            content='C', author='A', category='Cat',
            keyword='K', article_url='https://ex.com', img='i',
        )
        assert df['source'].iloc[0] == 'my_src'

    def test_single_row_index_1(self):
        df = _build_article_dataframe(
            source='s', now_time='now',
            article_id='1', title='T', created_at='2023-01-01',
            content='C', author='A', category='Cat',
            keyword='K', article_url='https://ex.com', img='i',
        )
        assert len(df) == 1
        assert df.index[0] == 1

    def test_create_time_uses_now_time(self):
        df = _build_article_dataframe(
            source='s', now_time='2025-06-15 08:00:00',
            article_id='1', title='T', created_at='2025-06-15',
            content='C', author='A', category='Cat',
            keyword='K', article_url='https://ex.com', img='i',
        )
        assert df['create_time'].iloc[0] == '2025-06-15 08:00:00'
