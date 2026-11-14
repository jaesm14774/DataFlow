import unittest

from source_extension.dag import (
    source_extension_collect_after_close,
    source_extension_collect_intraday,
    source_extension_collect_monthly,
    source_extension_collect_process,
    source_extension_collect_weekend,
)


class SourceIngestionConcurrencyTests(unittest.TestCase):
    def test_user_source_batches_bound_shared_database_writer_concurrency(self):
        """User can run approved source batches without four-way DB write bursts.

        Given the source collection DAGs write batches to shared MySQL tables
        When their scheduled DAG definitions are loaded
        Then no more than two source tasks can run concurrently per DAG.
        """
        source_dags = (
            source_extension_collect_process,
            source_extension_collect_after_close,
            source_extension_collect_intraday,
            source_extension_collect_monthly,
            source_extension_collect_weekend,
        )

        self.assertTrue(all(1 <= dag.max_active_tasks <= 2 for dag in source_dags))
        self.assertEqual(
            {dag.get_task("ingest_approved_source").pool for dag in source_dags},
            {"source_records_write"},
        )


if __name__ == "__main__":
    unittest.main()
