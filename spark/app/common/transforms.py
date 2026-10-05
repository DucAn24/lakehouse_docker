"""Reusable DataFrame transforms: CDC dedup, timestamp parsing, surrogate keys."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    desc,
    from_unixtime,
    monotonically_increasing_id,
    row_number,
    when,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window


def safe_to_timestamp(column):
    """Convert column to timestamp, handling both TIMESTAMP and LONG (Debezium ms) types."""
    return when(
        column.cast("string").rlike("^[0-9]+$"),
        from_unixtime(column.cast("long") / 1000).cast(TimestampType()),
    ).otherwise(
        column.cast(TimestampType()),
    )


def generate_surrogate_key(df: DataFrame, key_column_name: str = "sk") -> DataFrame:
    """Add an auto-incrementing surrogate key column starting from 1."""
    return df.withColumn(key_column_name, monotonically_increasing_id() + 1)


def extract_cdc_latest(
    df: DataFrame,
    key_cols: list[str],
    ts_col: str = "ts_ms",
    select_after: bool = True,
) -> DataFrame:
    """Extract the latest CDC record per business key.

    Filters out deletes (op='d'), optionally unpacks the 'after' struct,
    and deduplicates by key_cols using ts_col descending.
    """
    df_active = df.filter(col("op") != "d")

    if select_after:
        df_active = df_active.select("after.*", ts_col)

    window_spec = Window.partitionBy(*key_cols).orderBy(desc(ts_col))
    return (
        df_active.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", ts_col)
    )
