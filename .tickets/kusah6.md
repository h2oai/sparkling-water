---
id: kusah6
status: open
type: task
effort: normal
created: 2026-11-21T02:21:10Z
---
# 0.3b Autoscaling interaction of the internal backend

Child of 0.3. Run the 0.3a training suite on an autoscaling DBR 16.4 LTS cluster (for example 1–4 workers, dedicated access) with release `3.46.0.6-1-3.5` and record how the internal backend behaves: the H2O cloud size at start versus the Spark executors present, what happens when Databricks adds workers after `H2OContext.getOrCreate` and when it removes one that hosts an H2O node (cloud locked, job failure, recovery), and the phase timings in the 0.3a JSON format next to the fixed-size baseline. Adjust the cloud-size assertion only through a parameter, never by weakening the fixed-size check.

Record: findings and recommended cluster settings (for example a minimum worker count, or autoscaling off for training) in `design/databricks-baseline-2026-11.md`; timings in `design/baseline-timings/`. Feeds ADR-005 (0.5, supported Databricks mode).

Verify: at least one run with a scale-up and one with a scale-down event, each documented with the outcome and timings.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under kusah6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.