---
id: viyon8
status: open
type: task
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.7 Replace the RowEncoder shim

Step 1.7 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

`ExpressionEncoder(schema)`/`Encoders.row`; delete per-version `RowEncoder.scala`. After this `utils` has no version-specific `scala_spark_<X.Y>` dirs; `scala_spark_others` remains with the shared shims. Verify: core + ml tests + build gate.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.