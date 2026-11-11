---
id: bogad5
status: open
type: chore
effort: hard
created: 2026-11-10T23:07:35Z
---
# 1.8 ScalaTest 3.2.x, all modules

Step 1.8 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

One PR: scalatest 3.2.19, `scalatestplus-junit`, junit 4.13.2 in `build.gradle` (`scalaTestVersion` is a single root property and `utils`/`macros` tests depend on `core`'s test jar, so modules cannot migrate separately); migrate every `org.scalatest.junit.JUnitRunner`, `FunSuite`, `Matchers` import (~100 files, one or two lines each). Verify: `./gradlew test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under bogad5`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.