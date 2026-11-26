# PySpark Data Quality Framework

A personal learning project: a small, config-driven data quality framework on PySpark. Rules live in a
YAML file, so adding or changing a check needs no code change. It runs on **synthetic data** with
problems injected on purpose; it is not based on any employer or client data.

## Checks

| Check | What it catches |
|---|---|
| `not_null` | Nulls and blank strings |
| `unique` | Duplicate keys (returns every duplicated row) |
| `range` | Values below `min` or above `max` (nulls ignored) |
| `regex` | Values not matching a pattern (for example malformed emails) |
| `allowed_values` | Values outside an allowed list |
| `referential` | Orphan keys with no match in a reference dataset |
| `row_count_reconciliation` | Source-to-target row count mismatch beyond a tolerance |

Each rule has a `severity`: `error` fails the run, `warn` is reported only. The CLI exits with code 1
when any `error` rule fails, so it can gate a step in CI or an Airflow task.

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # needs Java 11+ for Spark

python data/make_sample.py               # regenerates the synthetic CSVs
python -m src.dq.cli --config config/orders_rules.yaml --report reports/dq_report.json
pytest -q
```

Sample output on the bundled data (3 datasets, 11 rules):

```
[PASS] customers.customer_id  not_null                   failed=0
[FAIL] customers.customer_id  unique                     failed=2
[WARN] customers.email        regex                      failed=1
[FAIL] orders.customer_id     referential                failed=1
[WARN] orders.-               row_count_reconciliation   failed=1
{'checks': 11, 'passed': 1, 'failed_errors': 7, 'failed_warnings': 3, 'ok': False}
```

The failures are expected: the sample data contains duplicate keys, a bad email, an out-of-range age,
an orphan customer, a negative amount and an invalid status.

The JSON report holds the per-rule counts and up to three failing sample rows per rule.

## Writing rules

```yaml
datasets:
  orders: {path: data/orders.csv}
  customers: {path: data/customers.csv}
rules:
  - {dataset: orders, check: range, column: amount, min: 0.01, severity: error}
  - {dataset: orders, check: referential, column: customer_id,
     reference: customers, ref_column: customer_id, severity: error}
```

## Layout

```
src/dq/    checks.py (check functions), engine.py (config + runner + report), cli.py
config/    orders_rules.yaml
data/      make_sample.py and the generated sample CSVs
tests/     test_checks.py (7 tests)
```

## Limitations

Local single-machine runs on small files; not benchmarked at scale. Checks are computed one rule at a
time, so a production version would batch rules into fewer passes over the data.
