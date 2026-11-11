---
id: fopim6
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 2.3 Spark 3.5 / Scala 2.13: `macros`, `utils`, `api-generation`, `scoring`

Step 2.3 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

`scalaTestVersions['2.13']`, 2.13 `scalaVersion` set to the patch DBR 16.4 LTS ships (recorded in `gradle-spark3.5.properties`); `-Ymacro-annotations` replaces macro-paradise in `gradle/scala.gradle` for 2.13 (`scoring`'s `generateApi` builds `api-generation`, so it must be on 2.13 here). Verify: `./gradlew :sparkling-water-scoring:test -Pspark=3.5 -PscalaVersion=2.13.<dbr>`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under fopim6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.