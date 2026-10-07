"""Pipeline metrics tracker.

Logs execution metadata (row counts, duration, status) to a Delta table
at METRICS_PATH so runs can be monitored via Trino / Metabase.
"""

from __future__ import annotations

import time
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from common.config import METRICS_PATH

logger = logging.getLogger(__name__)

_METRICS_SCHEMA = [
    "run_id",  # ISO-8601 timestamp of the run
    "layer",  # bronze | silver | gold
    "table_name",  # e.g. olist_orders
    "row_count",  # number of rows written
    "duration_sec",  # wall-clock seconds
    "status",  # success | failed
    "error_message",  # empty on success
    "recorded_at",  # write timestamp (UTC, 'yyyy-MM-dd HH:mm:ss')
    "write_mode",  # overwrite | merge
    "rows_inserted",  # merge: rows inserted (overwrite: all rows)
    "rows_updated",  # merge: rows changed
    "rows_deleted",  # merge: rows removed because the key vanished from the source
]

# Explicit DDL: the optional counters may be NULL, which Row-based inference cannot type.
_METRICS_DDL = (
    "run_id string, layer string, table_name string, row_count long, duration_sec double, "
    "status string, error_message string, recorded_at string, write_mode string, "
    "rows_inserted long, rows_updated long, rows_deleted long"
)


class MetricsTimer:
    """Context manager that records write metrics automatically."""

    def __init__(self, spark: SparkSession, layer: str, table_name: str):
        self.spark = spark
        self.layer = layer
        self.table_name = table_name
        self._start: float = 0.0

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = round(time.time() - self._start, 2)
        status = "failed" if exc_type else "success"
        error_msg = str(exc_val) if exc_val else ""

        # Row count is set externally via .set_row_count()
        row_count = getattr(self, "_row_count", -1)

        _write_metric(
            self.spark,
            layer=self.layer,
            table_name=self.table_name,
            row_count=row_count,
            duration_sec=duration,
            status=status,
            error_message=error_msg,
        )
        # Don't suppress exceptions
        return False

    def set_row_count(self, count: int):
        self._row_count = count


def _write_metric(
    spark: SparkSession,
    *,
    layer: str,
    table_name: str,
    row_count: int,
    duration_sec: float,
    status: str,
    error_message: str = "",
    write_mode: str = "overwrite",
    rows_inserted: int | None = None,
    rows_updated: int | None = None,
    rows_deleted: int | None = None,
):
    """Append a single metric row to the metrics Delta table."""
    now = datetime.now(timezone.utc)
    row = (
        now.isoformat(),
        layer,
        table_name,
        int(row_count),
        float(duration_sec),
        status,
        error_message[:500],  # truncate long errors
        now.strftime("%Y-%m-%d %H:%M:%S"),
        write_mode,
        rows_inserted,
        rows_updated,
        rows_deleted,
    )
    try:
        df = spark.createDataFrame([row], _METRICS_DDL)
        # mergeSchema: tables written before the merge counters existed gain the new columns
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(METRICS_PATH)
    except Exception:
        # Metrics should never break the pipeline
        logger.warning(
            "Failed to write metric for %s.%s — continuing",
            layer,
            table_name,
            exc_info=True,
        )


def record_metric(
    spark: SparkSession,
    layer: str,
    table_name: str,
    row_count: int,
    duration_sec: float,
    status: str = "success",
    error_message: str = "",
    **extra,
):
    """Public convenience function to record a single metric.

    ``extra`` carries the optional write_mode / rows_inserted / rows_updated / rows_deleted.
    """
    _write_metric(
        spark,
        layer=layer,
        table_name=table_name,
        row_count=row_count,
        duration_sec=duration_sec,
        status=status,
        error_message=error_message,
        **extra,
    )
