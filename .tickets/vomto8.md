---
id: vomto8
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 2.8 M1b: training on Azure Databricks

Step 2.8 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

Full `_2.13` assembly + wheel, `ci/databricksTests`, 1.3 fixtures load. Output: update `design/databricks-baseline-2026-11.md`. **Milestone M1.**

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.