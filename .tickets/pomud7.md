---
id: pomud7
status: open
type: task
effort: easy
created: 2026-11-10T23:07:36Z
---
# 3.1 `gradle-spark4.0.properties`

Step 3.1 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

(Spark 4.0.0, Scala 2.13.16, JDK 17 target, Python 3.12, `databricksVersion=17.3`); not yet in `supportedSparkVersions`. Verify: `./gradlew help -Pspark=4.0`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under pomud7`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.