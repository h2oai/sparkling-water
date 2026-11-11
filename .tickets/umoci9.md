---
id: umoci9
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 3.11 `core` – external backend rest + `DefaultParams*` SparkSession overloads

Step 3.11 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

Verify: core tests, external backend suite.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under umoci9`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.