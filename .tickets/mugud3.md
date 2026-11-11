---
id: mugud3
status: open
type: task
effort: normal
created: 2026-11-11T03:51:25Z
---
# 0.2b Databricks driver script for the scoring tests

Child of 0.2. `ci/databricksTests/scoring/run_databricks.sh` using the current `databricks` CLI: upload the 0.2a scripts and fixtures to a Unity Catalog volume or workspace folder, install the scoring jar (Maven coordinate or uploaded file) and the scoring wheel as cluster libraries on a dedicated-access cluster (create DBR 16.4 LTS or reuse a given cluster id), run both scripts as job tasks with the 0.2a Spark conf, fail unless both report `SCORING TEST PASSED`. Workspace host, cluster and upload path are parameters, never committed. Verify: `--dry-run` prints the CLI calls; `shellcheck` clean.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under mugud3`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.