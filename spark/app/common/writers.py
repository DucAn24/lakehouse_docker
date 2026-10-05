"""Delta writers that record pipeline metrics."""

from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame, SparkSession

from common.metrics import record_metric

logger = logging.getLogger(__name__)


def write_with_metrics(
    df: DataFrame,
    spark: SparkSession,
    layer: str,
    table_name: str,
    path: str,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> int:
    """Write a DataFrame to Delta and record execution metrics.

    Returns the row count written.
    """
    start = time.time()
    row_count = -1
    try:
        writer = df.write.format("delta").mode(mode)
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        writer.save(path)

        row_count = df.count()
        duration = round(time.time() - start, 2)

        record_metric(spark, layer, table_name, row_count, duration, "success")
        logger.info(
            "Wrote %s.%s — %d rows in %.1fs",
            layer,
            table_name,
            row_count,
            duration,
        )
    except Exception as exc:
        duration = round(time.time() - start, 2)
        record_metric(spark, layer, table_name, 0, duration, "failed", str(exc))
        raise

    return row_count
