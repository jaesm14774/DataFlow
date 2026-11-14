import importlib
import sys
from pathlib import Path


def test_user_can_import_source_extension_package():
    """User can load the scheduled market-signal package.

    Given the Airflow DAG directory is on Python's import path
    When the source_extension package is imported
    Then its package metadata is available without a syntax error.
    """
    dags_directory = str(Path(__file__).resolve().parents[1] / "dags")
    sys.path.insert(0, dags_directory)
    try:
        package = importlib.import_module("source_extension")
        assert package.__doc__ == "Purpose-driven, metadata-driven source extensions for DataFlow."
    finally:
        sys.path.remove(dags_directory)
