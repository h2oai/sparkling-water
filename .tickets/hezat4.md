---
id: hezat4
status: open
type: task
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.1 Scoring test source set

Step 1.1 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Create `scoring/src/test` with its own Spark-only `SparkTestContext` (local `SparkSession`, no H2O); add `testImplementation` spark-core/sql/mllib, scalatest, junit to `scoring/build.gradle` with **no** dependency on `:sparkling-water-core` or its `testArchives`, so the suite compiles on 2.13 / 4.0 / JDK 21 ahead of `core`. Move the H2O-free MOJO suites (`H2OMOJOModelSparkRuntimeIndependencyTestSuite`, `H2OMOJOModelParallelTestSuite`, `H2OMOJOPipelineModelTestSuite`) from `ml` to `scoring`. Verify: `./gradlew :sparkling-water-scoring:test` runs the moved suites.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under hezat4`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.

## Note 2026-11-11T03:13:04Z

Baseline 0.1 finding: H2OMOJOPipelineModelTestSuite needs a Driverless AI license (17 tests fail with 'License could not be found!' without one). Moving it into scoring makes the scoring gate depend on a license. Move only the two H2O-3 MOJO suites, or keep the pipeline suite behind a license-present assumption (assume/cancel when DRIVERLESS_AI_LICENSE_FILE/KEY is unset).