"""Individual data quality checks. Each returns (failed_count, failing_rows_df)."""
from typing import Callable, Dict, Optional, Tuple

from pyspark.sql import DataFrame, functions as F

CheckFn = Callable[..., Tuple[int, DataFrame]]


def not_null(df: DataFrame, column: str, **_) -> Tuple[int, DataFrame]:
    bad = df.filter(F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == ""))
    return bad.count(), bad


def unique(df: DataFrame, column: str, **_) -> Tuple[int, DataFrame]:
    dupes = df.groupBy(column).count().filter("count > 1")
    bad = df.join(dupes.select(column), on=column, how="inner")
    return bad.count(), bad


def range_check(df: DataFrame, column: str, min: Optional[float] = None, max: Optional[float] = None, **_):
    cond = F.lit(False)
    if min is not None:
        cond = cond | (F.col(column) < min)
    if max is not None:
        cond = cond | (F.col(column) > max)
    bad = df.filter(F.col(column).isNotNull() & cond)
    return bad.count(), bad


def regex(df: DataFrame, column: str, pattern: str, **_):
    bad = df.filter(F.col(column).isNotNull() & ~F.col(column).cast("string").rlike(pattern))
    return bad.count(), bad


def allowed_values(df: DataFrame, column: str, values, **_):
    bad = df.filter(F.col(column).isNotNull() & ~F.col(column).isin(list(values)))
    return bad.count(), bad


def referential(df: DataFrame, column: str, ref_df: DataFrame, ref_column: str, **_):
    """Rows whose key does not exist in the reference table (orphans)."""
    keys = ref_df.select(F.col(ref_column).alias("_ref_key")).distinct()
    bad = (
        df.filter(F.col(column).isNotNull())
        .join(keys, df[column] == keys["_ref_key"], "left_anti")
    )
    return bad.count(), bad


def row_count_reconciliation(df: DataFrame, ref_df: DataFrame, tolerance_pct: float = 0.0, **_):
    """Source-to-target row count check. Fails when counts differ by more than tolerance_pct."""
    src, tgt = ref_df.count(), df.count()
    diff_pct = 0.0 if src == 0 else abs(src - tgt) * 100.0 / src
    failed = 1 if diff_pct > tolerance_pct else 0
    empty = df.limit(0)
    return failed, empty


REGISTRY: Dict[str, CheckFn] = {
    "not_null": not_null,
    "unique": unique,
    "range": range_check,
    "regex": regex,
    "allowed_values": allowed_values,
    "referential": referential,
    "row_count_reconciliation": row_count_reconciliation,
}
