---
id: dizuj4
status: open
type: task
effort: normal
created: 2026-11-11T03:51:25Z
---
# 0.2c Verify the scoring runner on Azure Databricks

Child of 0.2. Run 0.2b against release 3.46.0.6 on DBR 16.4 LTS (Scala 2.12) on an Azure dedicated cluster. Record the result, versions printed and cluster settings in `design/databricks-baseline-2026-11.md` (started here, completed by 0.3). Needs: Databricks CLI logged in to the Azure workspace, cluster permission or id, upload path. Verify: both job tasks green.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under dizuj4`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.