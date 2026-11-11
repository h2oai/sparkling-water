---
id: fihek3
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 4.6 PySparkling on 4.1/4.2

Step 4.6 of design/RENOVATION_PLAN.md (Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)).

Verify: `testPython` per minor.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under fihek3`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.