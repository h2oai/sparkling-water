---
id: ebile8
status: open
type: task
effort: hard
children:
  - hemlu7
  - mugud3
  - dizuj4
created: 2026-11-10T23:07:35Z
---
# 0.2 Databricks scoring test runner

Step 0.2 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

`∥` `ci/databricksTests/scoring/test.scala` and `test.py`: install the given scoring jar + `h2o_pysparkling_scoring` wheel on a dedicated cluster, load a MOJO from the repo fixtures, transform a DataFrame, save and reload a Spark ML pipeline containing it, assert no `H2OContext` was created, print Sparkling Water / Spark / Scala / JDK / Python versions. New driver script using the current `databricks` CLI and workspace-file or volume library installs (the existing `ci/Jenkinsfile-databricks` is AWS-specific and uses the legacy CLI, so only its shape is reused). Verify: runs green against release 3.46.0.6 on DBR 16.4 LTS.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under ebile8`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.

## Note 2026-11-11T03:51:30Z

Split into 0.2a (local scripts, hemlu7), 0.2b (Databricks driver, mugud3), 0.2c (Azure verification, dizuj4). 0.2b/0.2c need the user's workspace access.