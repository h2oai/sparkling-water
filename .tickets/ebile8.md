---
id: ebile8
status: open
type: task
effort: hard
created: 2026-11-10T23:07:35Z
---
# 0.2 Databricks scoring test runner

Step 0.2 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

`∥` `ci/databricksTests/scoring/test.scala` and `test.py`: install the given scoring jar + `h2o_pysparkling_scoring` wheel on a dedicated cluster, load a MOJO from the repo fixtures, transform a DataFrame, save and reload a Spark ML pipeline containing it, assert no `H2OContext` was created, print Sparkling Water / Spark / Scala / JDK / Python versions. New driver script using the current `databricks` CLI and workspace-file or volume library installs (the existing `ci/Jenkinsfile-databricks` is AWS-specific and uses the legacy CLI, so only its shape is reused). Verify: runs green against release 3.46.0.6 on DBR 16.4 LTS.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.