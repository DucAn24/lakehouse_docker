"""Airflow DAGs: import cleanly, expected structure, every Spark task points at a real job."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAGS = ROOT / "airflow" / "dags"
CONTAINER_APP_DIR = "/opt/spark/app"

# the repo-level airflow/ folder is itself importable, so probe a real Airflow module
pytest.importorskip("airflow.dag_processing.dagbag", reason="apache-airflow not installed (see scripts/check_dags.py)")


@pytest.fixture(scope="module")
def bag():
    sys.path.insert(0, str(DAGS))
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_dags_import_without_errors(bag):
    assert bag.import_errors == {}


def test_expected_dags_exist(bag):
    assert {
        "lakehouse_etl_pipeline",
        "lakehouse_bronze_ingestion",
        "lakehouse_bronze_to_silver",
        "lakehouse_silver_to_gold",
    } <= set(bag.dag_ids)


def test_every_spark_task_submits_an_existing_job(bag):
    checked = 0
    for dag in bag.dags.values():
        for task in dag.tasks:
            application = getattr(task, "application", None)
            if not application:
                continue
            checked += 1
            assert application.startswith(CONTAINER_APP_DIR), f"{dag.dag_id}.{task.task_id}: {application}"
            local = ROOT / "spark" / "app" / application.removeprefix(CONTAINER_APP_DIR + "/")
            assert local.is_file(), f"{dag.dag_id}.{task.task_id}: {local} not found"
    assert checked > 0


def test_every_silver_table_has_a_task(bag):
    from common.tables import TABLES

    submitted = {
        Path(task.application).stem
        for dag in bag.dags.values()
        for task in dag.tasks
        if getattr(task, "application", None)
    }
    missing = set(TABLES["silver"]) - submitted
    assert not missing, f"silver tables without a DAG task: {sorted(missing)}"


@pytest.mark.parametrize("dag_id", ["lakehouse_etl_pipeline", "lakehouse_silver_to_gold"])
def test_gold_is_built_by_dbt_before_dq(bag, dag_id):
    from gold_tasks import DBT_PROJECT_DIR

    (build,) = [t for t in bag.dags[dag_id].tasks if t.task_id.endswith("dbt_build")]
    assert f"--project-dir {DBT_PROJECT_DIR}" in build.bash_command
    assert (ROOT / "dbt" / "dbt_project.yml").is_file()
    downstream = {t.task_id.rsplit(".", 1)[-1] for t in build.get_flat_relatives(upstream=False)}
    assert "dq_gold" in downstream


def test_no_catalog_registration_tasks(bag):
    """Polaris is shared by Spark and Trino: tables need no register step."""
    task_ids = {t.task_id.rsplit(".", 1)[-1] for dag in bag.dags.values() for t in dag.tasks}
    assert not {t for t in task_ids if t.startswith("register_")}
    assert "maintain_iceberg_tables" in task_ids
