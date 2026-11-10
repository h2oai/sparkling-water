---
id: laldo7
status: open
type: task
effort: hard
created: 2026-11-10T23:07:37Z
---
# 5.1 Azure Databricks CI job

Step 5.1 of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

`ci/Jenkinsfile-databricks` rewritten for Azure and the current CLI; reads `databricksVersion` per property file; matrix over 16.4 LTS (2.13), 17.3 LTS, 18 LTS, 19; scoring runner + full tests; dedicated access mode in the cluster JSON.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.