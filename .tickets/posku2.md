---
id: posku2
status: open
type: chore
effort: easy
created: 2026-11-10T23:07:35Z
---
# 1.4 Delete Spark 2.x

Step 1.4 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Property files 2.3/2.4, `core/src/main/scala_spark_2.1|2.2|2.3`, `resources_spark_2.*`, `sparkMajorVersion <= "2.4"` branches (`ml`, `examples`, `gradle/utils.gradle`), 2.x in `ci/build.gradle`, docs. Verify: `./gradlew build -x check -Pspark=3.5`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under posku2`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.