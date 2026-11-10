---
id: rajij9
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 3.8 `core` – Spark UI tab sources per Spark line

Step 3.8 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

Move `SparklingWaterInfoPage`, `SparklingWaterUITab`, `CrossSparkUtils` implementations into `core/src/main/scala_spark_3.5` (javax) and `scala_spark_others` (jakarta); keep the `headerSparkPage` reflection shim; H2O-side `javax` contract untouched (`extensions` keeps `compileOnly servlet-api`, assembly keeps excluding it). Verify: `:sparkling-water-core:test -Pspark=3.5` green; the 4.0 run is 3.9.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.