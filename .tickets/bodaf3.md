---
id: bodaf3
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 5.5 Standalone Spark 4.x CI matrix

Step 5.5 of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

with `integTest`/`integTestPython`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under bodaf3`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.