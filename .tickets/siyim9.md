---
id: siyim9
status: open
type: chore
effort: normal
created: 2026-11-10T23:07:35Z
---
# 0.3 Azure Databricks baseline

Step 0.3 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

Run 0.2 and the existing `ci/databricksTests/test.scala`/`test.py` with release `3.46.0.6-1-3.5` on DBR 16.4 LTS (Scala 2.12). Record internal backend start time, autoscaling interaction, library install path. Output: `design/databricks-baseline-2026-11.md`. This is the behaviour M1–M3 must preserve.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.