---
id: dobos9
status: open
type: task
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.2 Scoring persistence fixtures

Step 1.2 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Models and `H2OMOJOModel`/`H2OMOJOPipelineModel` pipelines saved by the current 2.12 release under `scoring/src/test/resources/sw_scoring_scala_2.12_3.46/` (same convention as `ml/src/test/resources/sw_mojo_scala_2.12_df_java_serde`); `CrossVersionMOJOLoadTestSuite` loads, transforms and round-trips them. Captured **before** any serialization-affecting change. Verify: `./gradlew :sparkling-water-scoring:test`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.