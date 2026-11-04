"""Data quality checks run inside the Spark jobs before writing silver.

If a check fails the job raises, the Airflow task fails and the previous
silver data stays as it was (nothing is overwritten with bad data).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityError(Exception):
    pass


def not_empty(df: DataFrame, name: str) -> None:
    if df.limit(1).count() == 0:
        raise QualityError(f"{name}: no rows")


def no_nulls(df: DataFrame, name: str, cols: list[str]) -> None:
    # One pass over the data for every column, not one count per column.
    row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    for c in cols:
        if row[c]:
            raise QualityError(f"{name}: {row[c]} nulls in {c}")


def unique(df: DataFrame, name: str, keys: list[str]) -> None:
    dup = df.groupBy(*keys).count().filter("count > 1").count()
    if dup:
        raise QualityError(f"{name}: {dup} duplicated keys {keys}")


def positive(df: DataFrame, name: str, col: str) -> None:
    n = df.filter(F.col(col) <= 0).count()
    if n:
        raise QualityError(f"{name}: {n} rows with {col} <= 0")


def no_gaps(df: DataFrame, name: str, date_col: str, group_col: str) -> None:
    """Every group has one row per calendar day between its first and last date."""
    g = df.groupBy(group_col).agg(F.min(date_col).alias("a"), F.max(date_col).alias("b"), F.count("*").alias("n"))
    bad = g.filter(F.datediff("b", "a") + 1 != F.col("n")).count()
    if bad:
        raise QualityError(f"{name}: {bad} groups with missing days in {date_col}")
