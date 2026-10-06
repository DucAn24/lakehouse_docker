"""Parse every Airflow DAG and verify the Spark job each task submits exists in the repo.

    pip install "apache-airflow==3.2.1" apache-airflow-providers-apache-spark \
        --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.11.txt
    python scripts/check_dags.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAGS = ROOT / "airflow" / "dags"
CONTAINER_APP_DIR = "/opt/spark/app"  # where spark/app is mounted in the airflow container
EXPECTED_DAGS = {
    "lakehouse_etl_pipeline",
    "lakehouse_bronze_ingestion",
    "lakehouse_bronze_to_silver",
    "lakehouse_silver_to_gold",
}

sys.path.insert(0, str(DAGS))  # DAGs import spark_submit_defaults from the dags folder

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

bag = DagBag(dag_folder=str(DAGS), include_examples=False)
problems = [f"import error in {path}:\n{err}" for path, err in bag.import_errors.items()]

missing = EXPECTED_DAGS - set(bag.dag_ids)
if missing:
    problems.append(f"DAGs not found: {sorted(missing)}")

spark_tasks = 0
for dag in bag.dags.values():
    for task in dag.tasks:
        application = getattr(task, "application", None)
        if not application:
            continue
        spark_tasks += 1
        local = ROOT / "spark" / "app" / application.removeprefix(CONTAINER_APP_DIR).lstrip("/")
        if not local.is_file():
            problems.append(f"{dag.dag_id}.{task.task_id}: application not found: {application}")

print(f"Parsed {len(bag.dags)} DAGs, {spark_tasks} Spark tasks")
if problems:
    print("\n".join(f"  - {p}" for p in problems))
    sys.exit(1)
print("OK")
