"""Collect permissioned Bahamut guides and ingest source-neutral packages."""

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from game.bahamut_collector import collect_bahamut_game_knowledge
from game.knowledge_ingest import ensure_schema_and_catalog, ingest_authorized_packages


@dag(
    dag_id="game_knowledge_collect",
    description="Discover game boards and collect rights-authorized guide content",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "jaesm14774",
        "retries": 0,
        "retry_delay": pendulum.duration(minutes=2),
        "execution_timeout": pendulum.duration(minutes=20),
    },
    tags=["game", "licensed-content"],
)
def game_knowledge_collect():
    @task(task_id="ensure_game_knowledge_schema")
    def ensure_schema_task():
        ensure_schema_and_catalog()

    @task(task_id="ingest_authorized_packages")
    def ingest_packages_task():
        context = get_current_context()
        result = ingest_authorized_packages(context["dag_run"].run_id)
        print(result)
        return result

    @task(task_id="collect_bahamut_game_knowledge")
    def collect_bahamut_task():
        context = get_current_context()
        result = collect_bahamut_game_knowledge(context["dag_run"].run_id)
        print(result)
        return result

    ensure_schema_task() >> collect_bahamut_task() >> ingest_packages_task()


game_knowledge_collect()
