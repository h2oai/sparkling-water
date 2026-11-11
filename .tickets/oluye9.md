---
id: oluye9
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 1.13 Remove the REPL module

Step 1.13 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

`settings.gradle`, `core`/`utils`/`assembly` project references, `H2OConf` interpreter options and defaults, `spark.ext.h2o.repl.*`, delete `core/.../backend/api/scalainterpreter/*` and its tests (they import the repl), REPL docs. Verify: `./gradlew :sparkling-water-core:test` + build gate.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under oluye9`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.