"""Expose the pipeline observability tables to Trino (and Grafana / Metabase) and Unity Catalog.

    spark-submit jobs/ops/register_monitoring.py

Registers ``monitoring.pipeline_metrics`` (job runs) and ``monitoring.data_quality_log`` (DQ results).
Tables that no job has written yet are skipped, so this is safe to run at the end of any pipeline.
"""

import sys

from delta.tables import DeltaTable

from common.catalog import register_catalog_tables
from common.config import create_spark_session, get_logger
from common.tables import MONITORING_SCHEMA, MONITORING_TABLES

logger = get_logger("ops.register_monitoring")


def main():
    spark = create_spark_session("Ops_RegisterMonitoring", "s3a://gold/")
    try:
        existing = {name: path for name, path in MONITORING_TABLES.items() if DeltaTable.isDeltaTable(spark, path)}
        for name in sorted(set(MONITORING_TABLES) - set(existing)):
            logger.warning("monitoring.%s has no data yet -- skipping", name)
        if not existing:
            return
        _, failed = register_catalog_tables(spark, MONITORING_SCHEMA, existing)
        if failed:
            sys.exit(1)
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
