import pytest
from pyspark.sql import SparkSession

from src.dq import checks
from src.dq.engine import run_checks, summarise


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[2]").appName("t").config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2").getOrCreate()
    yield s
    s.stop()


def test_not_null_catches_null_and_blank(spark):
    df = spark.createDataFrame([("a",), (None,), ("  ",)], ["c"])
    assert checks.not_null(df, "c")[0] == 2


def test_unique_returns_all_duplicate_rows(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["k"])
    n, bad = checks.unique(df, "k")
    assert n == 2 and bad.select("k").distinct().count() == 1


def test_range_bounds_inclusive_and_ignores_nulls(spark):
    df = spark.createDataFrame([(0.0,), (5.0,), (10.0,), (11.0,), (None,)], "v double")
    assert checks.range_check(df, "v", min=0, max=10)[0] == 1


def test_regex_and_allowed_values(spark):
    df = spark.createDataFrame([("a@b.com",), ("nope",)], ["e"])
    assert checks.regex(df, "e", pattern=r"^[^@]+@[^@]+\.[a-z]+$")[0] == 1
    df2 = spark.createDataFrame([("x",), ("y",), ("z",)], ["s"])
    assert checks.allowed_values(df2, "s", values=["x", "y"])[0] == 1


def test_referential_finds_orphans(spark):
    parent = spark.createDataFrame([("C1",), ("C2",)], ["id"])
    child = spark.createDataFrame([("C1",), ("C9",), (None,)], ["cid"])
    assert checks.referential(child, "cid", ref_df=parent, ref_column="id")[0] == 1


def test_row_count_reconciliation_tolerance(spark):
    src = spark.range(100)
    tgt = spark.range(98)
    assert checks.row_count_reconciliation(tgt, ref_df=src, tolerance_pct=0)[0] == 1
    assert checks.row_count_reconciliation(tgt, ref_df=src, tolerance_pct=5)[0] == 0


def test_engine_severity_gates_overall_result(spark):
    df = spark.createDataFrame([("a",), (None,)], ["c"])
    cfg_warn = {"rules": [{"dataset": "d", "check": "not_null", "column": "c", "severity": "warn"}]}
    cfg_err = {"rules": [{"dataset": "d", "check": "not_null", "column": "c", "severity": "error"}]}
    assert summarise(run_checks(cfg_warn, {"d": df}))["ok"] is True
    assert summarise(run_checks(cfg_err, {"d": df}))["ok"] is False
