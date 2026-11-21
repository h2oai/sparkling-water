# Databricks baseline – 2026-11-19

Released Sparkling Water 3.46.0.6 on Azure Databricks, before any renovation change: scoring (0.2c), training and phase
timings (0.3a), autoscaling (0.3b). Workspace, user and paths are not recorded.

## Scoring-only tests (0.2c)

Command: `UPLOAD_PATH=/Volumes/<catalog>/<schema>/<volume>/tests ci/databricksTests/run_databricks.sh`

| Item | Value |
|---|---|
| Artifacts | Maven `ai.h2o:sparkling-water-scoring-package_2.12:3.46.0.6-1-3.5`, PyPI `h2o_pysparkling_scoring_3.5==3.46.0.6.post1` |
| Runtime | DBR 16.4 LTS (`16.4.x-scala2.12`): Spark 3.5.2, Scala 2.12.15, JDK 17.0.20, Python 3.12.3 |
| Cluster | one job cluster per task: `Standard_D4ds_v5`, 1 worker, dedicated access (`SINGLE_USER`) |
| Result | both tasks SUCCESS: 380/380 rows match, reloaded pipeline matches, no `H2OContext` |
| Duration | 11 min total; cluster setup 9 min, Scala task 83 s, Python task 104 s |

## Training tests and phase timings (0.3a, 2026-11-20)

Timing baseline: [`baseline-timings/databricks-16.4-3.46.0.6.json`](baseline-timings/databricks-16.4-3.46.0.6.json),
median, min and max of 3 sequential runs per suite. Reproduce (each suite stores its aggregate under its own key):

```bash
export UPLOAD_PATH=/Volumes/<catalog>/<schema>/<volume>/tests TIMINGS_OUT=design/baseline-timings/databricks-16.4-3.46.0.6.json
SUITE=training RUNS=3 ci/databricksTests/run_databricks.sh && SUITE=scoring RUNS=3 ci/databricksTests/run_databricks.sh
```

| Item | Value |
|---|---|
| Artifacts | Maven `ai.h2o:sparkling-water-package_2.12:3.46.0.6-1-3.5`, PyPI `h2o_pysparkling_3.5==3.46.0.6.post1` |
| Cluster | one job cluster per task: `Standard_D4ds_v5`, 3 workers, dedicated access; JDK 17 `--add-exports` (below) |
| Result | 3 of 3 runs SUCCESS for both tasks: 3 H2O nodes, XGBoost trained and predicted on prostate |

Median seconds (min–max) of the in-script phases:

| Phase | Training Scala | Training Python | | Phase | Scoring Scala | Scoring Python |
|---|---|---|---|---|---|---|
| `h2o_context` | 27.4 (27.2–30.0) | 31.6 (30.6–31.7) | | `data_read` | 16.9 (13.6–18.2) | 17.8 (17.7–18.5) |
| `data_import` | 1.6 (1.6–2.3) | 2.0 (1.8–2.1) | | `mojo_load` | 0.7 (0.7–1.1) | 0.9 (0.9–1.0) |
| `as_spark_frame` | 8.0 (7.8–9.1) | 10.1 (9.6–10.6) | | `score` | 3.6 (3.6–4.4) | 4.4 (4.2–4.5) |
| `train` | 11.3 (10.6–13.3) | 19.8 (16.8–21.4) | | `pipeline_save` | 5.7 (5.5–5.8) | 11.4 (11.0–11.8) |
| `predict` | 1.2 (1.2–1.5) | 1.1 (1.1–1.2) | | `pipeline_load_score` | 2.6 (2.6–3.0) | 3.2 (3.2–3.2) |

Task execution: training 92 s (Scala) and 129 s (Python), scoring 75 s and 90 s. Cluster setup, which includes the
library install, is bimodal (about 80–110 s or 530 s) depending on Azure VM provisioning, so compare in-script phases
and execution, not setup or run totals.

## Observations

- One-time runs (`jobs submit`) reject shared `job_clusters`, so each task starts its own cluster; a run with
  `CLUSTER_ID` avoids the setup time.
- The released PySparkling's `pkg_resources` import (step 1.12a) works on DBR 16.4, whose setuptools still ships it.
- The Maven library is installed under a renamed file
  (`ai_h2o_sparkling_water_scoring_package_2_12_3_46_0_6_1_3_5.jar`), so the Scala summary shows that name.
- **The internal backend of 3.46.0.6 does not start on JDK 17 without flags.** `H2OContext.getOrCreate` fails with
  `IllegalAccessError: ... cannot access class sun.net.www.protocol.http.Handler ... does not export` because
  `RestCommunication` uses JDK-internal URL handlers. The baseline sets
  `--add-exports=java.base/sun.net.www.protocol.http=ALL-UNNAMED --add-exports=java.base/sun.net.www.protocol.https=ALL-UNNAMED`
  in `spark.driver.extraJavaOptions` and `spark.executor.extraJavaOptions` (the training default of `JAVA_OPTIONS`).
  Fixed in code by step 2.1b.
- Library install path: PyPI libraries go to `/local_disk0/.ephemeral_nfs/cluster_libraries/python/lib/python3.12/site-packages`;
  Maven libraries are installed as renamed jars on the cluster classpath. Both install during cluster setup, per task.
- The full PySparkling (`h2o_pysparkling_3.5`) also imports and starts on DBR 16.4 without extra Python pins.
