"""Reusable DataFrame transforms: CDC dedup, timestamp parsing, surrogate keys."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
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

    Keeps the newest event per key (by ts_col descending) and then drops keys whose newest
    event is a delete (op='d'), so rows deleted at the source do not survive. Optionally
    unpacks the 'after' struct. A delete has no 'after' image, so with select_after the key
    is read from 'before' when that column exists; without it deletes cannot be matched to
    their key and are only filtered out.
    """
    if select_after and "before" in df.columns:
        keys = [coalesce(col(f"after.{k}"), col(f"before.{k}")).alias(f"_key_{i}") for i, k in enumerate(key_cols)]
        key_names = [f"_key_{i}" for i in range(len(key_cols))]
        events = df.select("op", ts_col, *keys, "after.*")
        window_spec = Window.partitionBy(*key_names).orderBy(desc(ts_col))
        return (
            events.withColumn("_row_num", row_number().over(window_spec))
            .filter((col("_row_num") == 1) & (col("op") != "d"))
            .drop("_row_num", "op", ts_col, *key_names)
        )

    df_active = df.filter(col("op") != "d")

    if select_after:
        df_active = df_active.select("after.*", ts_col)

    window_spec = Window.partitionBy(*key_cols).orderBy(desc(ts_col))
    return (
        df_active.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", ts_col)
    )
