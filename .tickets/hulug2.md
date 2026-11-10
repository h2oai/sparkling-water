---
id: hulug2
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 5.3 Databricks on AWS/GCP

Step 5.3 of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

`AzureDatabricksUtils` → `DatabricksUtils` keyed on `spark.databricks.clusterUsageTags.clusterId`; unit test with synthetic `SparkConf`s; one manual run on AWS. Best-effort.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.