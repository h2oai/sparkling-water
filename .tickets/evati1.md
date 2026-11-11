---
id: evati1
status: open
type: task
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.3 ML persistence fixtures + audit

Step 1.3 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Pipelines with H2O estimators/models and every collection-typed `Param` saved by the current release under `ml/src/test/resources/sw_ml_scala_2.12_3.46/`; `CrossVersionPipelineLoadTestSuite`; table of Java-serialised Scala-collection params in `design/persistence-audit.md`. Verify: `./gradlew :sparkling-water-ml:test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under evati1`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.