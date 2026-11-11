---
id: magan1
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 3.2 `macros`, `utils` on 4.0

Step 3.2 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

`hiveClassesArePresent`; 4.x copy of `expose.SparkUserDefinedFunction` (constructor takes `Option[Encoder[_]]`) next to the 3.5 one; cut `utils`'s test dependency on `:sparkling-water-core`/`testArchives` by moving the one `TestUtils` helper into `utils/src/test` (`getLocalDir` needs no change: `SparkConf` extends `ReadOnlySparkConf`). Verify: `-Pspark=4.0 :sparkling-water-utils:test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under magan1`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.