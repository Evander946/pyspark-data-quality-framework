"""    python -m src.dq.cli --config config/orders_rules.yaml --report reports/dq_report.json

Exit code is 1 when any rule with severity "error" fails, so it can gate a pipeline step in CI/Airflow.
"""
import argparse
import sys

from pyspark.sql import SparkSession

from .engine import load_config, load_datasets, run_checks, summarise, write_report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--report", default="reports/dq_report.json")
    ap.add_argument("--base", default=".")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("dq").master("local[2]")
             .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    cfg = load_config(a.config)
    results = run_checks(cfg, load_datasets(spark, cfg, a.base))
    write_report(results, a.report)

    for r in results:
        flag = "PASS" if r.passed else ("FAIL" if r.severity == "error" else "WARN")
        print(f"[{flag}] {r.dataset}.{r.column:<12} {r.check:<26} failed={r.failed}")
    s = summarise(results)
    print(s)
    return 0 if s["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
