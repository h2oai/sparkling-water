---
id: gunwe5
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 1.15 Remove Flow exposure from the public API and UI

Step 1.15 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

`flowURL`/`openFlow` throw per ADR-003 in Scala, Python, R; `H2OClusterInfo` and `SparklingWaterInfoPage` lose the Flow link; `AzureDatabricksUtils` loses proxy-URL logic; docs and examples. Verify: `:sparkling-water-core:test :sparkling-water-py:testPython :sparkling-water-r:test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under gunwe5`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.