---
id: ozigu3
status: open
type: task
effort: hard
created: 2026-11-10T23:07:36Z
---
# 3.3 `api-generation`, `scoring` on 4.0

Step 3.3 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

`GenericRowWithSchema`/`ExpressionEncoder` usage in `H2OMOJOPrediction*`, `DefaultParams{Reader,Writer}` SparkSession overloads, ANSI-mode conversions; regenerate scoring API. Verify: `:sparkling-water-scoring:test -Pspark=4.0` with `spark.sql.ansi.enabled=true`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under ozigu3`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.