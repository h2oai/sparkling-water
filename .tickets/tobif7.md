---
id: tobif7
status: open
type: task
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.6 Collapse the UDF shim

Step 1.6 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Keep `ai.h2o.sparkling.sql.functions.udf` and `org.apache.spark.sql.expose.SparkUserDefinedFunction` as one copy in `utils/src/main/scala_spark_others`. Do **not** switch the 12 call sites to the public `functions.udf(f, dataType)`: it throws unless `spark.sql.legacy.allowUntypedScalaUDF` is set (Spark ≥ 3.0) and the call sites return dynamic `StructType` rows. Verify: `:sparkling-water-scoring:test :sparkling-water-ml:test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under tobif7`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.