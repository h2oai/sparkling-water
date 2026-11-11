---
id: niwal6
status: open
type: task
effort: hard
created: 2026-11-10T23:07:36Z
---
# 2.6 Spark 3.5 / Scala 2.13: `extensions`, `core`

Step 2.6 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

Verify: `:sparkling-water-core:test` with the 2.3 flags.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under niwal6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.