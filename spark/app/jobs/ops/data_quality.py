"""Data quality gate for one layer (called by Airflow ``dq_<layer>`` tasks).

    spark-submit jobs/ops/data_quality.py --layer silver [--table olist_orders]

Exits 1 when any check fails so the downstream layer does not run.
Rules live in ``common/quality.py``.
"""

import argparse
import sys

from common.config import create_spark_session, get_logger
from common.quality import CHECKS, run_quality_checks
from common.tables import TABLES

logger = get_logger("data_quality")


def main(layer: str, table: str | None = None):
    # Session first: the rule definitions use pyspark `col()`, which needs an active SparkContext.
    spark = create_spark_session(f"DQ_{layer.capitalize()}" + (f"_{table}" if table else ""), f"s3a://{layer}/")
    try:
        all_checks = CHECKS[layer]()
        if table and table not in all_checks:
            logger.error("Unknown %s table: %s", layer, table)
            sys.exit(1)
        checks = {table: all_checks[table]} if table else all_checks

        if not run_quality_checks(spark, layer, checks):
            logger.error("%s quality gate FAILED", layer.capitalize())
            sys.exit(1)
        logger.info("%s quality gate PASSED", layer.capitalize())
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data quality gate")
    parser.add_argument("--layer", required=True, choices=list(TABLES))
    parser.add_argument("--table", help="Validate only this single table in the chosen layer")
    args = parser.parse_args()

    main(args.layer, args.table)
