"""Reusable DataFrame transforms: CDC dedup, timestamp parsing, surrogate keys."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    desc,
    from_unixtime,
    lit,
    monotonically_increasing_id,
    row_number,
    when,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window


# helper columns used by extract_cdc_latest; underscore-prefixed so they cannot clash with payload columns
_OP = "_cdc_op"
_TS = "_cdc_ts"


def _op_priority(op):
    return when(op == "d", 2).when(op == "r", 0).otherwise(1)


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
    their key and are only filtered out. Ties on ts_col are broken deterministically: a
    delete wins over other ops, which win over snapshot reads.
    """
    if select_after and "before" in df.columns:
        keys = [coalesce(col(f"after.{k}"), col(f"before.{k}")).alias(f"_key_{i}") for i, k in enumerate(key_cols)]
        key_names = [f"_key_{i}" for i in range(len(key_cols))]
        events = df.select(col("op").alias(_OP), col(ts_col).alias(_TS), *keys, "after.*")
        window_spec = Window.partitionBy(*key_names).orderBy(desc(_TS), _op_priority(col(_OP)).desc())
        return (
            events.withColumn("_row_num", row_number().over(window_spec))
            .filter((col("_row_num") == 1) & (col(_OP) != "d"))
            .drop("_row_num", _OP, _TS, *key_names)
        )

    if select_after:
        # no 'before' image: a delete cannot be matched to its key, so it is only filtered out
        df_events = df.filter(col("op") != "d").select("after.*", col("op").alias(_OP), col(ts_col).alias(_TS))
    else:
        # flat frame: keys are on every event, so window first and drop keys whose newest event is a delete
        df_events = df.withColumn(_OP, col("op")).withColumn(_TS, col(ts_col))

    window_spec = Window.partitionBy(*key_cols).orderBy(desc(_TS), _op_priority(col(_OP)).desc())
    return (
        df_events.withColumn("_row_num", row_number().over(window_spec))
        .filter((col("_row_num") == 1) & (col(_OP) != "d"))
        .drop("_row_num", _OP, _TS)
    )


def with_unknown_member(df: DataFrame, values: dict[str, object]) -> DataFrame:
    """Append the -1 UNKNOWN dimension row, built by column name from df's schema.

    Columns not in `values` are NULL, except `etl_loaded_at` which gets current_timestamp()
    (same clock as the real rows). Unknown names in `values` raise, so a renamed or dropped
    column fails loudly instead of misaligning the row.
    """
    extra = set(values) - set(df.columns)
    if extra:
        raise ValueError(f"unknown member values for columns not in schema: {sorted(extra)}")
    exprs = []
    for field in df.schema.fields:
        if field.name == "etl_loaded_at":
            expr = current_timestamp()
        else:
            expr = lit(values.get(field.name))
        exprs.append(expr.cast(field.dataType).alias(field.name))
    return df.unionByName(df.sparkSession.range(1).select(*exprs))
