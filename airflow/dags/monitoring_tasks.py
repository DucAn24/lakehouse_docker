"""Shared tail of every lakehouse DAG: publish monitoring tables, then report the real run outcome."""

from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from spark_submit_defaults import monitoring_kwargs


def finish_with_monitoring(*leaves) -> None:
    """Chain ``register_monitoring`` after ``leaves`` and fail the run if any of them failed.

    The metrics / DQ log tables are most useful when something broke, so ``register_monitoring``
    runs once the leaves are done whatever their state. Because that would make it the only
    deciding leaf (and the run look successful), ``pipeline_done`` (default all_success) depends
    on the leaves too and carries the real outcome.
    """
    register = SparkSubmitOperator(task_id="register_monitoring", trigger_rule="all_done", **monitoring_kwargs())
    done = EmptyOperator(task_id="pipeline_done")
    for leaf in leaves:
        leaf >> register
    [*leaves, register] >> done
