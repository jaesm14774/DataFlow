"""Regression tests for the TWSE broker-branch collector (券商進出總表).

Production symptom: on a typical day only ~25% of 上市 stocks got broker data and on several days
(11/12, 11/18, 11/19, 11/25 in 2026-11) nothing was stored at all. Root causes covered here:

* the site now answers a wrong captcha with an (almost) empty page and a 403 result page, which the
  old code did not recognise, so wrong captchas were filed under "no data" and never retried;
* failures were silent (no per-run summary) and a total outage burned hours without any warning;
* unmapped branches were stored with a NULL name, which breaks the (date, code, name) unique key.
"""
import io

import pandas as pd
import pytest

from stock import collect_logic as cl
from stock.collect_logic import StockBrokerInOutTWSECollectProcess as Collector

BLANK = "﻿"  # what the site returns after a wrong captcha (236 bytes in production)
REAL_MENU_TEXT = "未命名頁面 <<買賣日報表查詢系統>> 下載 CSV bsContent.aspx " * 120  # the real page is ~15,000 characters

MENU_HTML = (
    '<html><body><img src="CaptchaImage.aspx?guid=abc">'
    '<input type="hidden" name="__VIEWSTATE" value="v"><input type="hidden" name="__EVENTVALIDATION" value="e">'
    '<input type="hidden" name="__VIEWSTATEENCRYPTED" value=""><input type="text" name="CaptchaControl1"></body></html>'
)
DATA_HTML = (
    '<html><body><table><tr><td id="receive_date">2026/11/25</td></tr></table>'
    '<table><tr class="column_title_1"><td>序</td><td>證券商</td><td>成交單價</td><td>買進股數</td><td>賣出股數</td></tr>'
    "<tr><td>1</td><td>1480 美商高盛</td><td>193</td><td>156000</td><td>22000</td></tr>"
    "<tr><td>2</td><td>9268 凱基-台北</td><td>193</td><td>106000</td><td>16000</td></tr></table></body></html>"
)
NO_ROWS_HTML = '<html><body><table><tr><td id="receive_date">2026/11/25</td></tr></table></body></html>'


def test_a_blank_or_old_style_rejection_is_recognised_as_a_failed_captcha():
    assert cl.is_bsr_menu_rejected(BLANK)
    assert cl.is_bsr_menu_rejected("")
    assert cl.is_bsr_menu_rejected(None)
    assert cl.is_bsr_menu_rejected("<html>驗證碼錯誤!" + "x" * 500)  # the old message still counts
    assert not cl.is_bsr_menu_rejected(REAL_MENU_TEXT)


class _Resp:
    def __init__(self, text="", status=200, content=b""):
        self.text, self.status_code, self.content = text, status, content
        self.raw = io.BytesIO(content)


class _Cookies:
    cleared = 0

    def clear(self):
        self.cleared += 1


class _Session:
    """Plays the three calls the collector makes: menu page + captcha image, the POST, the result page."""

    def __init__(self, post_text, content_resp, image=b"\xff\xd8fake"):
        self.post_text, self.content_resp, self.image = post_text, content_resp, image
        self.closed, self.cookies = 0, _Cookies()

    def close(self):
        self.closed += 1

    def get(self, url, **kwargs):
        if "bsMenu" in url:
            return _Resp(MENU_HTML)
        if "CaptchaImage" in url:
            return _Resp(content=self.image)
        return self.content_resp

    def post(self, url, **kwargs):
        self.posted = kwargs.get("data")
        return _Resp(self.post_text)


@pytest.fixture
def collector(tmp_path, monkeypatch):
    import cv2
    import numpy as np

    good = tmp_path / "processed.png"
    cv2.imwrite(str(good), np.zeros((8, 8, 3), dtype="uint8"))
    monkeypatch.setattr(cl, "img_path", str(tmp_path / "raw.jpg"))
    obj = object.__new__(Collector)
    obj.headers = {}
    obj.processed_img_path = str(good)
    obj._captcha_preprocessing = lambda src, dst: None
    obj._captcha_one_hot_decoding = lambda prediction, chars: "ABCDE"

    class _Model:
        def predict(self, img):
            return None

    obj.model = _Model()
    return obj


def test_wrong_captcha_blank_page_and_403_is_a_retryable_failure_not_no_data(collector):
    session = _Session(BLANK, _Resp("禁止直接存取此頁面。", status=403))
    assert collector._geo_broker_twse_with_session(session, "2597") == cl.BSR_CAPTCHA_FAILED


def test_an_unreadable_captcha_image_is_a_retryable_failure_not_a_crash(collector):
    collector.processed_img_path = collector.processed_img_path + ".missing"
    session = _Session(REAL_MENU_TEXT, _Resp(DATA_HTML))
    assert collector._geo_broker_twse_with_session(session, "2597") == cl.BSR_CAPTCHA_FAILED


def test_an_accepted_captcha_followed_by_an_empty_result_page_is_final_no_data_not_a_retry(collector):
    # stock 1802 on 2026-11-26: the answer is accepted (download link present) but the result page is blank.
    session = _Session(REAL_MENU_TEXT, _Resp("<html><body></body></html>"))
    assert collector._geo_broker_twse_with_session(session, "1802") == cl.BSR_NO_DATA


def test_a_post_response_without_the_download_link_is_a_wrong_answer_even_if_it_looks_like_a_page(collector):
    session = _Session("<html>" + "x" * 5000 + "</html>", _Resp(DATA_HTML))
    assert collector._geo_broker_twse_with_session(session, "2597") == cl.BSR_CAPTCHA_FAILED


def test_a_real_result_page_without_rows_is_final_no_data(collector):
    session = _Session(REAL_MENU_TEXT, _Resp(NO_ROWS_HTML))
    assert collector._geo_broker_twse_with_session(session, "2597") == cl.BSR_NO_DATA


def test_a_correct_captcha_returns_the_branch_table(collector):
    session = _Session(REAL_MENU_TEXT, _Resp(DATA_HTML))
    table = collector._geo_broker_twse_with_session(session, "2597")
    assert isinstance(table, pd.DataFrame)
    by_branch = table.set_index("券商代號")
    assert by_branch.loc["1480", "買賣超"] == 134 and by_branch.loc["9268", "買賣超"] == 90
    assert set(table["證券代號"]) == {"2597"}


def test_an_unparseable_result_is_reported_as_a_parse_failure(collector):
    broken = DATA_HTML.replace("<td>156000</td>", "<td>abc</td>")
    session = _Session(REAL_MENU_TEXT, _Resp(broken))
    assert collector._geo_broker_twse_with_session(session, "2597") == cl.BSR_PARSE_FAILED


class _FlakyImageSession(_Session):
    """The site answers the captcha image request with a 404 HTML page most of the time."""

    def __init__(self, bad_images, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bad_images, self.image_calls = bad_images, 0

    def get(self, url, **kwargs):
        if "CaptchaImage" in url:
            self.image_calls += 1
            if self.image_calls <= self.bad_images:
                resp = _Resp(text="404 - File or directory not found.", status=404, content=b"<!DOCTYPE html>")
                resp.headers = {"content-type": "text/html"}
                return resp
        return super().get(url, **kwargs)


def test_a_404_captcha_image_is_refetched_instead_of_crashing_image_processing(collector):
    collector._sleep = lambda seconds: None
    session = _FlakyImageSession(3, REAL_MENU_TEXT, _Resp(DATA_HTML))
    assert isinstance(collector._geo_broker_twse_with_session(session, "2597"), pd.DataFrame)
    assert session.image_calls == 4
    # the 404 sticks to the session, so each bad image must reset it (close + drop cookies)
    assert session.closed == 3 and session.cookies.cleared == 3


def test_no_captcha_image_at_all_is_a_retryable_failure(collector):
    collector._sleep = lambda seconds: None
    session = _FlakyImageSession(10**6, REAL_MENU_TEXT, _Resp(DATA_HTML))
    assert collector._geo_broker_twse_with_session(session, "2597") == cl.BSR_CAPTCHA_FAILED
    assert session.image_calls == Collector.CAPTCHA_FETCH_TRIES


def test_captcha_image_check_requires_a_real_jpeg():
    ok = _Resp(content=b"\xff\xd8abc")
    assert cl.is_captcha_image(ok)
    assert not cl.is_captcha_image(_Resp(content=b"<!DOCTYPE html>"))
    assert not cl.is_captcha_image(_Resp(content=b"\xff\xd8abc", status=404))


# ---------------------------------------------------------------- collect() loop


def _collector(codes, script, **limits):
    """A collector whose per-attempt outcomes come from ``script[code]`` (a list consumed in order)."""
    obj = object.__new__(Collector)
    obj.twse_code = list(codes)
    obj.sleeps = []
    obj._sleep = obj.sleeps.append
    obj._pace = lambda streak: None  # pacing has its own tests; keep the sleep log about pauses only
    queues = {code: list(items) for code, items in script.items()}

    def attempt(code):
        queue = queues[code]
        kind, payload = queue.pop(0) if len(queue) > 1 else queue[0]
        return kind, (pd.DataFrame({"證券代號": [code]}) if kind == "ok" else payload)

    obj._attempt = attempt
    for name, value in limits.items():
        setattr(obj, name, value)
    return obj


def test_wrong_captchas_are_retried_until_the_stock_is_collected():
    obj = _collector(["2597"], {"2597": [("retry", "驗證碼錯誤")] * 3 + [("ok", None)]})
    obj.collect()
    assert obj.collect_summary["ok"] == 1 and obj.collect_summary["attempts"] == 4
    assert obj.collect_summary["given_up"] == 0 and len(obj.result) == 1


def test_a_stock_that_never_passes_is_given_up_and_recorded_not_hidden():
    obj = _collector(["A", "B"], {"A": [("retry", "驗證碼錯誤")], "B": [("ok", None)]}, MAX_ATTEMPTS_PER_CODE=5,
                     BLOCK_STREAK_LIMIT=1000)
    obj.collect()
    summary = obj.collect_summary
    assert summary["ok"] == 1 and summary["given_up"] == 1 and summary["given_up_codes"] == ["A"]
    assert summary["no_data"] == 0  # a failed captcha is never filed under "no data"
    assert summary["attempts"] == 6  # 5 attempts for A, 1 for B


def test_no_data_is_final_and_does_not_loop():
    obj = _collector(["A", "B"], {"A": [("no_data", None)], "B": [("ok", None)]})
    obj.collect()
    assert obj.collect_summary["no_data"] == 1 and obj.collect_summary["attempts"] == 2


def test_a_site_outage_pauses_then_aborts_instead_of_spinning_for_hours():
    codes = [f"{1000 + i}" for i in range(50)]
    obj = _collector(codes, {c: [("retry", "驗證碼錯誤")] for c in codes}, MAX_ATTEMPTS_PER_CODE=12,
                     BLOCK_STREAK_LIMIT=10, BLOCK_PAUSE_SECONDS=600, BLOCK_MAX_PAUSES=2)
    obj.collect()
    summary = obj.collect_summary
    assert obj.sleeps == [600, 1200]  # two pauses (doubling each time), then give up
    assert "封鎖或改版" in summary["aborted"]
    assert summary["ok"] == 0 and summary["unprocessed"] > 0
    assert summary["attempts"] <= 40  # bounded: never anywhere near 50 stocks x 12 attempts


def test_a_run_that_hits_the_time_limit_stops_and_keeps_what_it_has(monkeypatch):
    clock = iter([0.0, 0.0, 10.0, 99999.0, 99999.0, 99999.0])
    monkeypatch.setattr(cl.time, "monotonic", lambda: next(clock))
    obj = _collector(["A", "B", "C"], {c: [("ok", None)] for c in "ABC"}, MAX_RUNTIME_SECONDS=100)
    obj.collect()
    assert obj.collect_summary["ok"] == 2 and "最長執行時間" in obj.collect_summary["aborted"]
    assert obj.collect_summary["unprocessed_codes"] == ["C"] and len(obj.result) == 2


def test_attempt_turns_exceptions_into_retryable_failures(monkeypatch):
    obj = object.__new__(Collector)
    obj._sleep = lambda seconds: None

    def boom(code):
        raise RuntimeError("SSLError")

    obj.geo_broker_twse = boom
    assert obj._attempt("2597") == ("retry", "RuntimeError")
    obj.geo_broker_twse = lambda code: cl.BSR_NO_DATA
    assert obj._attempt("2597") == ("no_data", None)
    obj.geo_broker_twse = lambda code: pd.DataFrame({"a": [1]})
    assert obj._attempt("2597")[0] == "ok"
    obj.geo_broker_twse = lambda code: cl.BSR_PARSE_FAILED
    assert obj._attempt("2597") == ("retry", cl.BSR_PARSE_FAILED)


def test_an_explicit_block_signal_pauses_immediately_without_waiting_for_a_failure_streak():
    obj = _collector(["A", "B"], {"A": [("blocked", "Access is denied")], "B": [("ok", None)]},
                     MAX_ATTEMPTS_PER_CODE=2, BLOCK_STREAK_LIMIT=30, BLOCK_PAUSE_SECONDS=600, BLOCK_MAX_PAUSES=5)
    obj.collect()
    assert obj.sleeps[0] == 600 and obj.collect_summary["attempts"] <= 4


def test_a_plain_wrong_captcha_never_pauses_on_its_own():
    obj = _collector(["A", "B"], {"A": [("retry", "x")] * 5 + [("ok", None)], "B": [("ok", None)]},
                     BLOCK_STREAK_LIMIT=30)
    obj.collect()
    assert obj.sleeps == [] and obj.collect_summary["ok"] == 2


def test_a_single_dropped_connection_does_not_pause():
    obj = _collector(["A", "B"], {"A": [("conn", "SSLError"), ("ok", None)], "B": [("ok", None)]})
    obj.collect()
    assert obj.sleeps == [] and obj.collect_summary["ok"] == 2


def test_repeated_dropped_connections_still_end_in_a_pause():
    obj = _collector(["A"], {"A": [("conn", "SSLError")]}, MAX_ATTEMPTS_PER_CODE=12, BLOCK_STREAK_LIMIT=9,
                     CONN_FAIL_WEIGHT=3, BLOCK_PAUSE_SECONDS=600)
    obj.collect()
    assert obj.sleeps and obj.sleeps[0] == 600


def test_pause_count_resets_after_the_site_recovers():
    # a block signal, 10 good stocks, again, 10 good, again, 10 good: 3 pauses spread over the run must not abort it
    codes = [f"{1000 + i}" for i in range(33)]
    script = {c: [("ok", None)] for c in codes}
    for c in (codes[0], codes[11], codes[22]):
        script[c] = [("blocked", "denied"), ("ok", None)]
    obj = _collector(codes, script, BLOCK_MAX_PAUSES=1, BLOCK_PAUSE_SECONDS=600, PAUSE_RESET_AFTER_OK=10)
    obj.collect()
    assert not obj.collect_summary["aborted"] and obj.collect_summary["ok"] == 33
    assert obj.sleeps == [600, 600, 600]  # every pause was the "first" one, so no doubling either


def test_the_block_pause_doubles_each_time():
    codes = [f"{1000 + i}" for i in range(40)]
    obj = _collector(codes, {c: [("retry", "x")] for c in codes}, BLOCK_STREAK_LIMIT=5, MAX_ATTEMPTS_PER_CODE=2,
                     BLOCK_PAUSE_SECONDS=600, BLOCK_MAX_PAUSES=3)
    obj.collect()
    assert obj.sleeps == [600, 1200, 2400]


def test_connection_errors_and_denied_pages_are_blocks_not_captcha_errors(collector):
    import requests

    obj = object.__new__(Collector)
    obj._sleep = lambda seconds: None

    def refused(code):
        raise requests.exceptions.SSLError("EOF")

    obj.geo_broker_twse = refused
    assert obj._attempt("2597")[0] == "conn"  # one dropped connection is not proof of a block
    obj.geo_broker_twse = lambda code: cl.BSR_BLOCKED
    assert obj._attempt("2597")[0] == "blocked"

    denied = _Session(REAL_MENU_TEXT, _Resp("403 - Forbidden: Access is denied.", status=403))
    assert collector._geo_broker_twse_with_session(denied, "2597") == cl.BSR_BLOCKED
    rejected = _Session(BLANK, _Resp("禁止直接存取此頁面。", status=403))
    assert collector._geo_broker_twse_with_session(rejected, "2597") == cl.BSR_CAPTCHA_FAILED


def test_captcha_images_are_archived_with_their_label_only_when_the_site_accepted_them(collector, tmp_path):
    import os

    collector._geo_broker_twse_with_session(_Session(BLANK, _Resp("禁止直接存取此頁面。", status=403)), "2597")
    collector._geo_broker_twse_with_session(_Session(REAL_MENU_TEXT, _Resp(DATA_HTML)), "2597")
    base = tmp_path / "captcha_dataset"
    assert [f.startswith("ABCDE_") for f in os.listdir(base / "accepted")] == [True]
    assert [f.startswith("ABCDE_") for f in os.listdir(base / "rejected")] == [True]


def test_the_form_post_carries_every_hidden_field_including_the_new_encrypted_viewstate(collector):
    session = _Session(REAL_MENU_TEXT, _Resp(DATA_HTML))
    collector._geo_broker_twse_with_session(session, "2597")
    sent = session.posted
    assert sent["__VIEWSTATEENCRYPTED"] == "" and "__VIEWSTATEENCRYPTED" in sent  # missing => every answer is rejected
    assert sent["__VIEWSTATE"] == "v" and sent["__EVENTVALIDATION"] == "e"
    assert sent["TextBox_Stkno"] == "2597" and sent["CaptchaControl1"] == "ABCDE" and sent["RadioButton_Normal"]


def test_the_result_page_is_requested_in_its_html_form(collector):
    urls = []

    class Spy(_Session):
        def get(self, url, **kwargs):
            urls.append(url)
            return super().get(url, **kwargs)

    collector._geo_broker_twse_with_session(Spy(REAL_MENU_TEXT, _Resp(DATA_HTML)), "2597")
    assert any(u.endswith("bsContent.aspx?v=Display") for u in urls)


# ---------------------------------------------------------- incremental writes


def _batched(codes, batch_size, fail_writes=0):
    obj = _collector(codes, {c: [("ok", None)] for c in codes}, BATCH_SIZE=batch_size)
    obj.writes, obj.fail_writes = [], fail_writes

    def write(frames):
        if obj.fail_writes > 0:
            obj.fail_writes -= 1
            raise RuntimeError("db down")
        obj.writes.append([f["證券代號"].iloc[0] for f in frames])

    obj._write_frames = write
    return obj


def test_results_are_written_in_batches_while_collecting_and_the_rest_on_save():
    obj = _batched(list("ABCDEFG"), batch_size=3)
    obj.collect()
    assert obj.writes == [["A", "B", "C"], ["D", "E", "F"]]  # written before the run ends
    assert [f["證券代號"].iloc[0] for f in obj.pending] == ["G"]
    obj.save()
    assert obj.writes[-1] == ["G"] and obj.pending == []


def test_an_interrupted_run_has_already_stored_the_finished_batches():
    obj = _batched(list("ABCDE"), batch_size=2)
    obj.collect()  # the process dies here: save() is never reached
    assert obj.writes == [["A", "B"], ["C", "D"]]


def test_a_failed_batch_write_does_not_stop_collection_and_falls_back_to_one_stock_at_a_time():
    obj = _batched(list("ABCD"), batch_size=2, fail_writes=1)
    obj.collect()
    assert obj.collect_summary["ok"] == 4  # collection carried on
    assert obj.writes == [["A"], ["B"], ["C", "D"]]  # the failed batch was retried stock by stock
    assert obj.pending == []


def test_one_poisoned_stock_does_not_block_the_other_stocks_in_its_batch():
    obj = _collector(list("ABC"), {c: [("ok", None)] for c in "ABC"}, BATCH_SIZE=3)
    written = []

    def write(frames):
        if any(f["證券代號"].iloc[0] == "B" for f in frames):
            raise RuntimeError("duplicate key")
        written.extend(f["證券代號"].iloc[0] for f in frames)

    obj._write_frames = write
    obj.collect()
    assert written == ["A", "C"]
    assert [f["證券代號"].iloc[0] for f in obj.pending] == ["B"]


def test_collection_aborts_when_the_database_never_accepts_a_write():
    codes = [f"{1000 + i}" for i in range(30)]
    obj = _collector(codes, {c: [("ok", None)] for c in codes}, BATCH_SIZE=2, FLUSH_FAIL_LIMIT=3)

    def down(frames):
        raise RuntimeError("db down")

    obj._write_frames = down
    obj.collect()
    assert "寫入 DB" in obj.collect_summary["aborted"]
    assert obj.collect_summary["ok"] < 30 and obj.collect_summary["unsaved"] == obj.collect_summary["ok"]


def test_parse_failures_do_not_trigger_the_block_detector():
    codes = [f"{1000 + i}" for i in range(10)]
    obj = _collector(codes, {c: [("retry", cl.BSR_PARSE_FAILED)] for c in codes}, MAX_ATTEMPTS_PER_CODE=3,
                     BLOCK_STREAK_LIMIT=4)
    obj.collect()
    assert not obj.collect_summary["aborted"] and obj.collect_summary["given_up"] == 10
    assert obj.sleeps == []


def test_the_time_limit_is_checked_between_attempts_of_one_stock(monkeypatch):
    clock = iter([0.0, 0.0, 0.0, 99999.0, 99999.0, 99999.0, 99999.0])
    monkeypatch.setattr(cl.time, "monotonic", lambda: next(clock))
    obj = _collector(["A"], {"A": [("retry", "x")]}, MAX_RUNTIME_SECONDS=100, MAX_ATTEMPTS_PER_CODE=12)
    obj.collect()
    assert "最長執行時間" in obj.collect_summary["aborted"] and obj.collect_summary["attempts"] == 2


def test_the_second_run_is_judged_against_the_whole_day_not_only_its_leftovers():
    from stock.collect_process import broker_collect_problem

    second_run = {"target": 50, "ok": 5, "no_data": 20, "given_up": 25, "unprocessed": 0, "aborted": "", "already": 950}
    assert broker_collect_problem(second_run) is None


def test_save_never_raises_and_reports_what_could_not_be_written():
    obj = _collector(list("AB"), {c: [("ok", None)] for c in "AB"}, BATCH_SIZE=10)

    def down(frames):
        raise RuntimeError("db down")

    obj._write_frames = down
    obj.collect()
    obj.save()  # must not raise: a failed task would rerun everything and skip downstream tasks
    assert obj.collect_summary["unsaved"] == 2
    from stock.collect_process import broker_collect_problem

    assert "沒寫入 DB 2" in broker_collect_problem(obj.collect_summary)


def test_after_a_failed_flush_the_retry_waits_for_a_full_new_batch():
    codes = [f"{1000 + i}" for i in range(7)]
    obj = _collector(codes, {c: [("ok", None)] for c in codes}, BATCH_SIZE=3, FLUSH_FAIL_LIMIT=99)
    calls = []

    def down(frames):
        calls.append(len(frames))
        raise RuntimeError("db down")

    obj._write_frames = down
    obj.collect()
    # 7 stocks / batch 3 -> flush attempts after the 3rd and 6th only (each: 1 batch call + one call per stock)
    assert calls == [3, 1, 1, 1, 6, 1, 1, 1, 1, 1, 1]


# ------------------------------------------------------------ names and notification


def test_unmapped_branches_get_their_code_as_name_never_null():
    result = pd.DataFrame({"券商代號": ["1480", "9B2Z", "9B2Y", "9B2Z"], "券商名稱": ["x", "y", "z", "w"]})
    names, unmapped = Collector.fill_branch_names(result, {"1480": "美商高盛"})
    assert list(names) == ["美商高盛", "9B2Z", "9B2Y", "9B2Z"]
    assert unmapped == ["9B2Y", "9B2Z"] and names.notna().all()


def test_branch_names_that_differ_only_by_case_are_kept_apart_per_stock():
    # production: 9B2Q / 9B2q on one stock collide on the unique key under MySQL's case-insensitive collation
    result = pd.DataFrame({"證券代號": ["1303", "1303", "2330", "1303"],
                           "券商代號": ["9B2Q", "9B2q", "9B2q", "1480"]})
    names, unmapped = Collector.fill_branch_names(result, {"1480": "美商高盛"})
    assert list(names) == ["9B2Q", "9B2q[9B2q]", "9B2q", "美商高盛"]  # 2330 has no clash, name untouched
    assert len({(s, n.casefold()) for s, n in zip(result["證券代號"], names)}) == 4


def test_a_unique_key_conflict_is_quarantined_not_retried_forever():
    from sqlalchemy.exc import IntegrityError

    obj = _collector(list("ABCD"), {c: [("ok", None)] for c in "ABCD"}, BATCH_SIZE=2)
    calls = []

    def write(frames):
        calls.append([f["證券代號"].iloc[0] for f in frames])
        if any(f["證券代號"].iloc[0] == "B" for f in frames):
            raise IntegrityError("insert", {}, Exception("1062 duplicate"))

    obj._write_frames = write
    obj.collect()
    assert obj.pending == [] and [f["證券代號"].iloc[0] for f in obj.rejected] == ["B"]
    assert sum(c == ["B"] for c in calls) == 1  # tried once on its own, never again
    assert obj.collect_summary["unsaved"] == 1  # still reported to the notification


def test_a_failed_flush_waits_with_exponential_backoff_before_trying_again():
    obj = _collector(list("ABCDEF"), {c: [("ok", None)] for c in "ABCDEF"}, BATCH_SIZE=2, FLUSH_FAIL_LIMIT=99)

    def down(frames):
        raise RuntimeError("db down")

    obj._write_frames = down
    obj.collect()
    assert obj.sleeps == [15, 30]  # no wait before the first flush, then 15 s, 30 s


def test_the_dag_helper_reports_incomplete_collection_but_not_a_finished_day():
    from stock.collect_process import broker_collect_problem

    ok = {"target": 1000, "ok": 950, "no_data": 20, "given_up": 30, "unprocessed": 0, "aborted": ""}
    assert broker_collect_problem(ok) is None
    low = dict(ok, ok=300, given_up=680)
    assert "完成率 32%" in broker_collect_problem(low)
    aborted = dict(ok, aborted="網站可能封鎖或改版", unprocessed=900, ok=100)
    assert "網站可能封鎖或改版" in broker_collect_problem(aborted)
    assert broker_collect_problem({"target": 0, "ok": 0, "no_data": 0, "given_up": 0, "unprocessed": 0, "aborted": ""}) is None
    assert broker_collect_problem(None) is None


def test_min_confidence_is_the_weakest_character_and_never_raises():
    import numpy as np

    prediction = [np.array([[0.1, 0.9]]), np.array([[0.6, 0.4]]), np.array([[0.0, 1.0]])]
    assert Collector._min_confidence(prediction) == pytest.approx(0.6)
    assert Collector._min_confidence(None) is None


def test_order_by_volume_puts_liquid_stocks_first_and_unknown_last():
    import math

    volumes = {"A": 10, "B": 500, "C": math.nan, "D": 500}
    assert cl.order_by_volume(["A", "B", "C", "D", "E"], volumes) == ["B", "D", "A", "C", "E"]


def test_pacing_waits_a_random_base_interval_and_slows_down_as_rejections_pile_up():
    obj = object.__new__(Collector)
    obj.sleeps = []
    obj._sleep = obj.sleeps.append
    calm = obj._pace(0)
    assert Collector.PACE_SECONDS[0] <= calm <= Collector.PACE_SECONDS[1]
    streak = Collector.SOFT_BACKOFF_STREAK + 4
    slow = obj._pace(streak)
    assert slow >= Collector.PACE_SECONDS[0] + 4 * Collector.SOFT_BACKOFF_STEP
    assert obj._pace(10_000) <= Collector.PACE_SECONDS[1] + Collector.SOFT_BACKOFF_MAX


def test_row_count_uses_the_cheap_estimate_for_huge_tables_and_exact_count_for_small_ones(monkeypatch):
    from stock.collect_logic import StockCollectLogic

    queries = []

    def fake_read_sql(sql, engine):
        queries.append(sql)
        if "information_schema" in sql:
            return pd.DataFrame({"N": [HUGE]})
        return pd.DataFrame({"N": [123]})

    obj = object.__new__(StockCollectLogic)
    obj.db_name, obj.app_name, obj.engine = "stock", "t", None
    monkeypatch.setattr(cl.pd, "read_sql_query", fake_read_sql)
    HUGE = 349_000_000
    assert obj.table_row_count("券商進出總表") == HUGE
    assert not any("count(1)" in q for q in queries)  # the 5-minute full scan never ran
    HUGE = 50_000
    assert obj.table_row_count("小表") == 123  # small table: exact count
