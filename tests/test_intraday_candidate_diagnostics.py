import datetime as dt
import json
import unittest

from source_extension import market_signals


class _IntradayCursor:
    def __init__(self, rows, dedupe_results):
        self.rows = rows
        self.dedupe_results = list(dedupe_results)
        self.last_query = ""

    def execute(self, query, _params=None):
        self.last_query = query

    def fetchall(self):
        return self.rows

    def fetchone(self):
        assert "market_signal_notifications" in self.last_query
        return self.dedupe_results.pop(0)


def _company_event_row(source_id, code, title):
    return (
        source_id,
        "twse_company_events",
        f"{source_id:064x}",
        code,
        "company_event",
        dt.datetime(2026, 10, 21),
        dt.date(2026, 10, 21),
        title,
        None,
        None,
        "https://example.test/event",
        "官方事件正文",
        json.dumps({"CompanyName": "測試公司"}, ensure_ascii=False),
        dt.datetime(2026, 10, 21, 20, 0),
    )


class IntradayCandidateDiagnosticsTest(unittest.TestCase):
    def test_intraday_diagnostics_separate_filtered_rows_sent_dedupes_and_new_items(self):
        original_classifier = market_signals._classify_company_event

        def classify(title, _body):
            if title.startswith("重要"):
                return {"materiality": "key", "label": "風險事件", "direction": "bearish"}
            return {"materiality": "routine", "label": "例行公告", "direction": "neutral"}

        market_signals._classify_company_event = classify
        try:
            self._assert_diagnostics()
        finally:
            market_signals._classify_company_event = original_classifier

    def _assert_diagnostics(self):
        rows = [
            _company_event_row(1, "2330", "重要事件甲"),
            _company_event_row(2, "2317", "重要事件乙"),
            _company_event_row(3, "2454", "例行公告丙"),
        ]
        call = {
            "last_cursor": 0,
            "upper_cursor": 3,
            "today": dt.date(2026, 10, 21),
            "started_at": dt.datetime(2026, 10, 21, 20, 30),
        }

        all_sent_diagnostics = {}
        all_sent, _ = market_signals._load_intraday_candidates(
            _IntradayCursor(rows, [("sent", 200, "receipt-a"), ("sent", None, None)]),
            **call,
            diagnostics=all_sent_diagnostics,
        )
        self.assertEqual(all_sent, [])
        self.assertEqual(all_sent_diagnostics["selected_source_row_count"], 3)
        self.assertEqual(all_sent_diagnostics["selected_source_rows_by_event_type"], {
            "twse_company_events:company_event": 3,
        })
        self.assertEqual(all_sent_diagnostics["company_event_materiality_counts"], {
            "key": 2,
            "routine": 1,
        })
        self.assertEqual(all_sent_diagnostics["excluded_by_policy_or_quality"], {
            "company_event_not_key_materiality": 1,
        })
        self.assertEqual(all_sent_diagnostics["candidate_fingerprints_checked"], 2)
        self.assertEqual(all_sent_diagnostics["already_sent_suppressed"], 2)
        self.assertEqual(all_sent_diagnostics["already_sent_with_http_2xx_and_receipt"], 1)
        self.assertEqual(all_sent_diagnostics["already_sent_without_confirmed_receipt"], 1)
        self.assertEqual(all_sent_diagnostics["already_sent_by_event_type"], {
            "company_event": {
                "sent_ledger_count": 2,
                "http_2xx_and_receipt_count": 1,
                "without_confirmed_receipt_count": 1,
            },
        })
        self.assertEqual(all_sent_diagnostics["new_candidate_count"], 0)

        new_item_diagnostics = {}
        new_items, _ = market_signals._load_intraday_candidates(
            _IntradayCursor(rows, [("sent", 200, "receipt-a"), None]),
            **call,
            diagnostics=new_item_diagnostics,
        )
        self.assertEqual(len(new_items), 1)
        self.assertEqual(new_items[0]["entity_id"], "2317")
        self.assertEqual(new_item_diagnostics["already_sent_suppressed"], 1)
        self.assertEqual(new_item_diagnostics["new_candidate_count"], 1)


if __name__ == "__main__":
    unittest.main()
