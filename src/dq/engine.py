"""Config-driven runner: load datasets and a YAML rule file, run checks, build a result report."""
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List

import yaml
from pyspark.sql import DataFrame, SparkSession

from .checks import REGISTRY


@dataclass
class CheckResult:
    dataset: str
    check: str
    column: str
    severity: str
    total_rows: int
    failed: int
    passed: bool
    sample: List[dict] = field(default_factory=list)


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_datasets(spark: SparkSession, cfg: dict, base: str = ".") -> Dict[str, DataFrame]:
    out = {}
    for name, spec in cfg["datasets"].items():
        p = str(Path(base) / spec["path"])
        reader = spark.read.option("header", True).option("inferSchema", True)
        out[name] = reader.csv(p) if spec.get("format", "csv") == "csv" else spark.read.parquet(p)
    return out


def run_checks(cfg: dict, datasets: Dict[str, DataFrame], sample_size: int = 3) -> List[CheckResult]:
    results: List[CheckResult] = []
    for rule in cfg["rules"]:
        name, kind = rule["dataset"], rule["check"]
        params = {k: v for k, v in rule.items() if k not in ("dataset", "check", "severity")}
        if "reference" in params:                       # name of another dataset
            params["ref_df"] = datasets[params.pop("reference")]
        df = datasets[name]
        failed, bad = REGISTRY[kind](df, **params)
        sample = [r.asDict(recursive=True) for r in bad.limit(sample_size).collect()] if failed else []
        results.append(CheckResult(
            dataset=name, check=kind, column=str(params.get("column", "-")),
            severity=rule.get("severity", "error"), total_rows=df.count(),
            failed=failed, passed=failed == 0,
            sample=json.loads(json.dumps(sample, default=str)),
        ))
    return results


def summarise(results: List[CheckResult]) -> dict:
    errors = [r for r in results if not r.passed and r.severity == "error"]
    warnings = [r for r in results if not r.passed and r.severity == "warn"]
    return {
        "checks": len(results),
        "passed": sum(r.passed for r in results),
        "failed_errors": len(errors),
        "failed_warnings": len(warnings),
        "ok": not errors,
    }


def write_report(results: List[CheckResult], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"summary": summarise(results), "results": [asdict(r) for r in results]}, f, indent=2)
