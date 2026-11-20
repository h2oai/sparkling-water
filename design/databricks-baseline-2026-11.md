# Databricks baseline – 2026-11-19

Released Sparkling Water 3.46.0.6 scoring artifacts on Azure Databricks, before any renovation change. Started by
step 0.2c, completed by 0.3. Workspace, user and paths are not recorded.

## Scoring-only tests (0.2c)

Command: `UPLOAD_PATH=/Volumes/<catalog>/<schema>/<volume>/scoring ci/databricksTests/scoring/run_databricks.sh`

| Item | Value |
|---|---|
| Artifacts | Maven `ai.h2o:sparkling-water-scoring-package_2.12:3.46.0.6-1-3.5`, PyPI `h2o_pysparkling_scoring_3.5==3.46.0.6.post1` |
| Runtime | DBR 16.4 LTS (`16.4.x-scala2.12`): Spark 3.5.2, Scala 2.12.15, JDK 17.0.20, Python 3.12.3 |
| Cluster | one job cluster per task: `Standard_D4ds_v5`, 1 worker, dedicated access (`SINGLE_USER`) |
| Result | both tasks SUCCESS: 380/380 rows match, reloaded pipeline matches, no `H2OContext` |
| Duration | 11 min total; cluster setup 9 min, Scala task 83 s, Python task 104 s |

## Observations

- One-time runs (`jobs submit`) reject shared `job_clusters`, so each task starts its own cluster; a run with
  `CLUSTER_ID` avoids the setup time.
- The released PySparkling's `pkg_resources` import (step 1.12a) works on DBR 16.4, whose setuptools still ships it.
- The Maven library is installed under a renamed file
  (`ai_h2o_sparkling_water_scoring_package_2_12_3_46_0_6_1_3_5.jar`), so the Scala summary shows that name.
