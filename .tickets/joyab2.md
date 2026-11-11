---
id: joyab2
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 1.17 Collections compat: `core`, `ml` main sources

Step 1.17 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Same rules as 1.9. Verify: `:sparkling-water-core:test :sparkling-water-ml:test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under joyab2`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.