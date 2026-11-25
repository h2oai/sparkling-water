---
id: jipel4
status: open
type: task
effort: normal
created: 2026-11-25T05:42:51Z
---
# 5.11 Research: elastic H2O on newer Spark and Databricks autoscaling

Placeholder for future research; not part of the M1–M3 path.

0.3b (`design/databricks-baseline-2026-11.md`, Autoscaling) found that the internal backend cannot follow an autoscaling
cluster: the H2O cloud is fixed at `H2OContext.getOrCreate`, workers added later are never used, and removing a worker
that hosts an H2O node breaks the context. The current recommendation is fixed-size clusters for training (ADR-005).

Research whether newer Spark (4.x) and Databricks releases make better scaling possible, for example:
- executor decommissioning (`spark.decommission.enabled`, storage decommissioning) and its listener events: can the
  backend learn of a removal early enough to stop or move H2O work, or to fail fast with a clear error?
- dynamic allocation with shuffle tracking, and stage-level scheduling: can H2O nodes be pinned so Spark never removes
  them, while other executors still scale?
- Databricks autoscaling controls (minimum workers, graceful decommission, cluster policies) on current DBR LTS;
- whether H2O-3 can grow or shrink a cloud at all, or whether elasticity needs another design (for example a
  separate fixed H2O cluster via the external backend next to an autoscaling Spark cluster).

Reuse the probe: `SUITE=autoscale ci/databricksTests/run_databricks.sh` (`ci/databricksTests/autoscale/test.scala`).

Verify: a short write-up of the options with evidence (probe runs on the newer runtime where relevant) and a
recommendation: keep fixed-size clusters, or file the follow-up tickets.