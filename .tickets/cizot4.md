---
id: cizot4
status: open
type: task
effort: hard
created: 2026-11-10T23:07:37Z
---
# 3.16 M2b: training on Azure Databricks

Step 3.16 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

DBR 17.3 LTS dedicated cluster, `ci/databricksTests`, 1.3 fixtures; Spark UI tab renders. Flip `spark=4.0` default. **Milestone M2.**

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under cizot4`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.