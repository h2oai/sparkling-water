---
id: hurel4
status: closed
type: task
effort: easy
created: 2026-11-21T03:12:24Z
---
# 0.3a-1 Phase timings in the scoring scripts and the local runner

Child of 0.3a. Timing helpers (timing.scala, timing.py) that the runners load before each script; scoring scripts time their phases; runners move to ci/databricksTests/ with SUITE in common.sh; run_local.sh prints the timings. Verify: run_local.sh passes and prints timings; shellcheck clean.

## Note 2026-11-21T03:12:56Z

run_local.sh passes locally with timings (scala/python, e.g. score 0.6 s); shellcheck clean; run_databricks.sh dry-run returns the timings from the notebooks.