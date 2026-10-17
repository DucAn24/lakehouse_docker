"""Iceberg table maintenance (Airflow task ``maintain_iceberg_tables``).

    spark-submit jobs/ops/maintain_tables.py [--retain-days 7] [--retain-last 5] [--orphan-days 3]

For every registered table (bronze / silver / gold + monitoring):
  expire_snapshots     drop snapshots older than --retain-days (always keeping the last --retain-last)
                       and the data files only those snapshots referenced
  remove_orphan_files  delete files no snapshot references (left by failed writes), older than --orphan-days
  rewrite_manifests    compact manifests so query planning stays fast

Gold tables are written by Trino (dbt) into the same Polaris catalog, so Spark maintains them too.
Tables that do not exist yet are skipped. Exits 1 if any table failed.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from common.config import create_spark_session, get_logger
from common.tables import MONITORING_TABLES, TABLES

logger = get_logger("ops.maintain_tables")


def _cutoff(days: int) -> str:
    # Spark reads TIMESTAMP literals in the session time zone (the JVM's local zone)
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


def maintain(spark: SparkSession, table: str, retain_days: int, retain_last: int, orphan_days: int) -> None:
    catalog, name = table.split(".", 1)
    expired = spark.sql(
        f"CALL {catalog}.system.expire_snapshots(table => '{name}', "
        f"older_than => TIMESTAMP '{_cutoff(retain_days)}', retain_last => {retain_last})"
    ).first()
    orphans = spark.sql(
        f"CALL {catalog}.system.remove_orphan_files(table => '{name}', older_than => TIMESTAMP '{_cutoff(orphan_days)}')"
    ).count()
    spark.sql(f"CALL {catalog}.system.rewrite_manifests(table => '{name}')")
    logger.info(
        "%s: expired %s data files, removed %d orphan files",
        table,
        expired["deleted_data_files_count"] if expired else 0,
        orphans,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Iceberg snapshot expiry + orphan file cleanup")
    parser.add_argument("--retain-days", type=int, default=7)
    parser.add_argument("--retain-last", type=int, default=5)
    parser.add_argument("--orphan-days", type=int, default=3)
    args = parser.parse_args()

    tables = [t for layer in TABLES.values() for t in layer.values()] + list(MONITORING_TABLES.values())
    spark = create_spark_session("LakehouseMaintenance")
    failed: list[str] = []
    try:
        for table in tables:
            if not spark.catalog.tableExists(table):
                logger.warning("%s does not exist yet -- skipping", table)
                continue
            try:
                maintain(spark, table, args.retain_days, args.retain_last, args.orphan_days)
            except Exception:
                logger.exception("Maintenance of %s failed", table)
                failed.append(table)
    finally:
        spark.stop()
    if failed:
        logger.error("Maintenance failed for: %s", ", ".join(failed))
        sys.exit(1)
    logger.info("Maintenance completed for %d tables", len(tables))


if __name__ == "__main__":
    main()
