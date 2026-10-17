"""dbt task of the gold DAGs: `dbt build` runs every gold model on Trino in dependency order, with its data tests.

The dbt project (repo `dbt/`) is bind-mounted at DBT_PROJECT_DIR. dbt lives in its own venv in the
airflow image because dbt-core's pins clash with Airflow's constraints.
Don't name this module dbt_*: dbt imports every top-level dbt_* module it can find as a plugin.
"""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/dbt-venv/bin/dbt"


def dbt_build(task_id: str = "dbt_build") -> BashOperator:
    """Build the gold layer; the task fails if a model or a data test fails."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"{DBT_BIN} build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
        # run artifacts stay out of the bind-mounted project folder
        env={"DBT_TARGET_PATH": "/tmp/dbt/target", "DBT_LOG_PATH": "/tmp/dbt/logs"},
        append_env=True,
    )
