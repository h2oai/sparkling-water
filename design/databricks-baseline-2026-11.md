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
| Seed | fixed `0x6A09E667F3BCC908` for the 80/20 split and XGBoost, so every run trains on the same rows (training re-recorded 2026-11-23) |

Median seconds (min–max) of the in-script phases:

| Phase | Training Scala | Training Python | | Phase | Scoring Scala | Scoring Python |
|---|---|---|---|---|---|---|
| `h2o_context` | 28.1 (27.7–29.4) | 30.4 (28.6–32.0) | | `data_read` | 16.9 (13.6–18.2) | 17.8 (17.7–18.5) |
| `data_import` | 2.4 (1.9–2.5) | 1.9 (1.9–2.7) | | `mojo_load` | 0.7 (0.7–1.1) | 0.9 (0.9–1.0) |
| `as_spark_frame` | 8.9 (8.1–9.9) | 9.5 (9.1–9.6) | | `score` | 3.6 (3.6–4.4) | 4.4 (4.2–4.5) |
| `train` | 14.0 (8.6–14.5) | 15.1 (13.5–19.0) | | `pipeline_save` | 5.7 (5.5–5.8) | 11.4 (11.0–11.8) |
| `predict` | 1.5 (1.2–1.6) | 1.2 (1.1–1.2) | | `pipeline_load_score` | 2.6 (2.6–3.0) | 3.2 (3.2–3.2) |

Task execution: training 95 s (Scala) and 126 s (Python), scoring 75 s and 90 s. Cluster setup, which includes the
library install, is bimodal (about 80–110 s or 530 s) depending on Azure VM provisioning, so compare in-script phases
and execution, not setup or run totals.

## Autoscaling (0.3b, 2026-11-23)

Timings: [`baseline-timings/databricks-16.4-3.46.0.6-autoscaling.json`](baseline-timings/databricks-16.4-3.46.0.6-autoscaling.json)
(single runs: probe run 3, training suite). Reproduce:

```bash
export UPLOAD_PATH=/Volumes/<catalog>/<schema>/<volume>/tests TIMINGS_OUT=design/baseline-timings/databricks-16.4-3.46.0.6-autoscaling.json
SUITE=autoscale ci/databricksTests/run_databricks.sh && SUITE=training WORKERS=1-4 ci/databricksTests/run_databricks.sh
```

The probe ([`ci/databricksTests/autoscale/test.scala`](../ci/databricksTests/autoscale/test.scala)) runs two Scala
scenarios, each on its own job cluster with autoscaling 1–4 workers (`Standard_D4ds_v5`, dedicated access, the JDK 17
`--add-exports`). It makes Databricks scale by queueing sleeping Spark tasks (scale-up) or by staying idle (scale-down),
and trains and scores XGBoost before and after the event. The down event is the loss of a worker that hosts an H2O node.
Results of three runs each (run 3 with the final probe, which also collects the predictions and waits for the load job to
end):

| Scenario | H2O cloud at start | Event | Training after the event |
|---|---|---|---|
| up: H2O starts first, then load | 1 node on 1 executor | 1 → 2–3 executors in 75–580 s | succeeds on the 1 original node; the new executors get no H2O node |
| down: load first, then H2O, then idle | 2 nodes on 3 executors (run 1), 3 on 3 (runs 2, 3) | an H2O worker removed after 185–490 s idle | fails in 0.4–35 s with `RestApiNotReachableException` for the lost node: from the driver when it was the node the driver talks to (runs 1, 3; run 3 then finds the whole cloud unreachable), from Spark tasks writing to it otherwise (run 2) |

The training suite on a 1–4 worker autoscaling cluster passes for Scala and Python: both start on the minimum, 1 worker,
so H2O runs 1 node and no scaling happens during the 2-minute workload. In-script phases (seconds, one run) are close to
or below the 3-worker baseline: `h2o_context` 17.8 / 20.8, `as_spark_frame` 7.7 / 10.0, `train` 7.5 / 13.1,
`predict` 1.2 / 1.3 (Scala / Python); fewer H2O nodes mean less coordination for this small data set.

Findings:

- **The cloud is fixed at `H2OContext.getOrCreate`.** Workers that Databricks adds later only produce the warning
  "New spark executor joined the cloud, however it won't be used" (`InternalH2OBackend`); H2O keeps its start size.
- **The start size depends on timing.** It is the number of executors registered when the context starts, not the
  autoscaling maximum and not always the executors present: run 1 of the down scenario started 2 H2O nodes with 3
  executors registered right after a scale-up. The training suite's cloud-size check therefore takes
  `H2O_NODES=executors` on autoscaling clusters (the default for `WORKERS=MIN-MAX`).
- **Removing a worker that hosts an H2O node breaks the H2OContext for good.** Nothing in the internal backend reacts to
  a lost executor (the heartbeat thread acts on an unhealthy cloud only in external mode). Run 2 shows the H2O REST API
  still reporting 3 nodes and a healthy cloud after one was gone, so the failure surfaces only when work reaches the lost
  node. Databricks picks the workers to remove; with H2O on every worker any removal hits a node.
- Scaling is slow next to the workload: adding a worker took 1.3–10 min (Azure VM provisioning), removing an idle one
  3–8 min, while training on the prostate data takes 10–20 s.

Recommended cluster settings for the internal backend (input to ADR-005):

- **Training: fixed-size clusters (autoscaling off)**, with the H2OContext started after all workers are up. Autoscaling
  gives no benefit (new workers are never used) and can remove a worker that holds H2O data at any idle moment.
- If a workspace policy enforces autoscaling, set the minimum to the wanted H2O size and start H2O at the minimum; a
  removal is still possible above the minimum after a scale-up, so long-lived contexts stay at risk.
- Scoring with `H2OMOJOModel` starts no H2O nodes (0.2c), so the node loss above does not apply to it; scoring on an
  autoscaling cluster was not measured here.

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
