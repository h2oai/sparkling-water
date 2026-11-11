---
id: oyoka9
status: open
type: chore
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.5 Delete Spark 3.0–3.4

Step 1.5 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Property files 3.0–3.4, `utils/src/main/scala_spark_3.0…3.4`, fold 3.5 shims into `scala_spark_others`, `supportedSparkVersions=3.5`, CI matrix, docs. Verify as 1.4.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under oyoka9`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.