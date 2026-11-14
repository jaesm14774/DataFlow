"""Stock mention matching used by the news / PTT sentiment study."""

import datetime as dt
import importlib.util
from pathlib import Path

import pandas as pd

_SPEC = importlib.util.spec_from_file_location(
    "research_signal_family_backtest",
    Path(__file__).resolve().parents[1] / "docs" / "research_signal_family_backtest.py",
)
study = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(study)


def _master():
    return pd.DataFrame({
        "code": ["2330", "2317", "1101", "1102", "9999", "0050"],
        "name": ["台積電", "鴻海", "台泥", "台泥", "測試科技公司", "元大台灣50"],
    })


def test_name_and_bracketed_code_matching():
    trie, codes = study._build_name_trie(_master())
    assert study._match_codes("台積電 法說會前夕", trie, codes, bare_codes=False) == {"2330"}
    assert study._match_codes("[標的] 2317 鴻海", trie, codes, bare_codes=True) == {"2317"}
    # two-character names are skipped (too ambiguous), shared names are dropped
    assert study._match_codes("鴻海大漲", trie, codes, bare_codes=False) == set()
    assert study._match_codes("台泥", trie, codes, bare_codes=False) == set()


def test_bare_codes_only_for_ptt_and_not_inside_other_numbers():
    trie, codes = study._build_name_trie(_master())
    assert study._match_codes("買 2330 好嗎", trie, codes, bare_codes=False) == set()
    assert study._match_codes("買 2330 好嗎", trie, codes, bare_codes=True) == {"2330"}
    assert study._match_codes("2330年報 1101點", trie, codes, bare_codes=True) == set()
    assert study._match_codes("(2330)", trie, codes, bare_codes=False) == {"2330"}
    assert study._match_codes("12330 與 23301", trie, codes, bare_codes=True) == set()
    assert study._match_codes("ETF 0050", trie, codes, bare_codes=True) == set()


def test_after_cutoff_assigns_late_text_to_next_session():
    assert not study._after_cutoff(dt.datetime(2026, 1, 5, 13, 30))
    assert study._after_cutoff(dt.datetime(2026, 1, 5, 13, 31))
    assert study._after_cutoff(dt.datetime(2026, 1, 5, 23, 59))
    assert study._after_cutoff(dt.datetime(2026, 1, 5, 0, 0))  # date-only: unknown time
    assert not study._after_cutoff(dt.datetime(2026, 1, 5, 9, 0))


def test_ptt_code_skips_years_and_ambiguous_names():
    names = {"台積電": "2330", "鴻海": "2317"}
    valid = {"2330", "2317"}
    assert study.parse_ptt_code("[標的] 台積電", "標的：2026年Q3 台積電", names, valid) == "2330"
    assert study.parse_ptt_code("t", "標的：2317", names, valid) == "2317"
    # unlisted four-digit numbers never end the search
    assert study.parse_ptt_code("t", "標的：9999", names, valid) is None
    # two different names in one field are ambiguous
    assert study.parse_ptt_code("t", "標的：台積電與鴻海", names, valid) is None


def test_ptt_direction_maps_late_posts_to_next_session():
    dates = pd.DatetimeIndex(["2026-01-05", "2026-01-06", "2026-01-07"])
    codes = pd.Index(["2330"])
    content = "分類：多\n標的：2330\n分析："
    raw = {
        "master": pd.DataFrame({"code": ["2330"], "name": ["台積電"]}),
        "ptt": pd.DataFrame({
            "created_at": ["2026-01-05 13:30:00", "2026-01-06 13:30:01"],
            "title": ["[標的] 2330", "[標的] 2330"],
            "content": [content, content],
        }),
    }
    direction = study.build_ptt_direction(raw, dates, codes)
    assert direction.loc["2026-01-05", "2330"] == 1.0  # at the close: same session
    assert direction.loc["2026-01-06", "2330"] == 0.0
    assert direction.loc["2026-01-07", "2330"] == 1.0  # one second late: next session
