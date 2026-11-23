---
id: betoj9
status: closed
type: task
effort: normal
created: 2026-11-21T03:12:24Z
---
# 0.3a-2 Suites, repeat runs and timing collection in the Databricks runner

Child of 0.3a. run_databricks.sh: SUITE (scoring|training) with its artifacts and worker count, RUNS sequential runs, per-run timing JSON (in-script phases from the notebook exit value, platform phases from the Jobs API), aggregate with median/min/max, TIMINGS_OUT, JAVA_OPTIONS. Verify: dry-run for both suites; aggregation checked on sample records; scoring suite passes on Databricks; shellcheck clean.

## Note 2026-11-21T03:12:56Z

Dry-run checked for both suites; aggregation checked on synthetic records (odd/even medians, phase order); 3 scoring runs green on DBR 16.4 (runs in 0.3a-3 baseline).