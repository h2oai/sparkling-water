---
id: hemlu7
status: closed
type: task
effort: normal
created: 2026-11-11T03:51:25Z
---
# 0.2a Scoring test scripts, verified locally

Child of 0.2. `ci/databricksTests/scoring/test.scala`, `test.py` and `run_local.sh`. Inputs come from Spark conf (`spark.sw.scoring.test.mojo`, `.data`, `.out`) so the same scripts run under spark-submit/spark-shell and on Databricks. Each script loads `binom_model_prostate.mojo`, transforms `binom_model_prostate_prediction.csv`, checks `prediction` and both probabilities per ID against the expected columns (tolerance 1e-6), saves and reloads a Spark ML `PipelineModel` containing the MOJO model and checks again, asserts no `H2OContext` exists, prints Sparkling Water / Spark / Scala / JDK / Python versions and ends with `SCORING TEST PASSED`. `run_local.sh` fetches the released 3.46.0.6 scoring artifacts (Maven `ai.h2o:sparkling-water-scoring-package_2.12:3.46.0.6-1-3.5`, PyPI `h2o_pysparkling_scoring_3.5==3.46.0.6.post1`) into `build/`, runs both against `$SPARK_HOME` and fails unless both markers appear. Verify: `ci/databricksTests/scoring/run_local.sh` exits 0 with Spark 3.5.x.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under hemlu7`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.

## Note 2026-11-11T03:56:52Z

run_local.sh green against released 3.46.0.6 (Spark 3.5.9, JDK 17): Scala and Python each score 380 rows matching expected predictions, reload the pipeline, no H2OContext. Negative check: one altered probability -> 'MOJO model: 1 of 380 rows differ'. Local-only workarounds in run_local.sh: loopback driver host (Spark 'Too large frame' on jar transfer), numpy<2, setuptools<80 (pkg_resources bug -> ticket rigub9 / step 1.12a). Scoring package jar has no sw.version resource, so Scala reports the jar name. shellcheck not installed locally; bash -n clean.