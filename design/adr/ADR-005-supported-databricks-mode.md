# ADR-005: Supported Databricks mode

- Status: accepted
- Date: 2026-11-28
- Context documents: `design/RENOVATION_GOAL_SPARK_4.x.md` (section 1 and risks), `design/databricks-baseline-2026-11.md`
  (0.2c, 0.3a, 0.3b), [ADR-002](ADR-002-support-matrix.md), `design/RENOVATION_PLAN.md` (step 0.5)

## Context

The primary objective of the renovation is Sparkling Water on Azure Databricks, with `H2OMOJOModel` scoring first and
training with the internal backend second. Databricks offers several compute modes, and Sparkling Water cannot run in
all of them:

- **Standard (shared) access mode and serverless compute** run Spark Connect. They forbid RDD APIs, `sparkContext` in
  Scala, custom jars, and (from DBR 19) driver and executor JVM options. Sparkling Water depends on all of these: RDD-pinned
  conversions, Spark RPC to start H2O in executors, and Py4J `_jvm` access in PySparkling.
- **Dedicated (single user) access mode on classic compute** runs classic Spark. The 3.46.0.6 baseline passes there on
  DBR 16.4 LTS: scoring, training on a fixed-size cluster, and installation as Maven and PyPI cluster libraries
  (0.2c, 0.3a).
- **Autoscaling** does not work with the internal backend (0.3b): the H2O cloud is fixed at `H2OContext.getOrCreate`,
  workers added later are never used, and removing a worker that hosts an H2O node breaks the context.
- **Databricks on AWS and GCP** offers the same modes, but `AzureDatabricksUtils` detects Azure only
  (`spark.databricks.cloudProvider == Azure`), and nothing has been tested there.
- On JDK 17 the released internal backend needs `--add-exports` for JDK-internal URL handlers (0.3a); step 2.1b
  removes that need.

## Decision

1. **The supported mode is Azure Databricks, classic compute, dedicated access mode**, on the Databricks Runtime
   versions whose Spark minor is supported by ADR-002 (DBR 16.4 LTS today). Sparkling Water is installed as cluster
   libraries: the Maven or jar artifact for Scala, the PyPI wheel for Python. All-purpose and job clusters are both
   supported.
2. **Out of scope:** standard (shared) access mode, serverless compute, Spark Connect (including Databricks Connect
   clients) and the `spark.api.mode=connect` PySpark client. No work goes into making Sparkling Water run there, and
   new code adds no private Py4J accesses (`_jvm`, `_java_obj` and similar).
3. **Training needs a fixed-size cluster (autoscaling off)**, with `H2OContext` started after all workers are up. One
   H2O node runs per worker, so the worker count is the H2O cloud size. Elastic H2O is a research item (step 5.11).
4. **Scoring (`H2OMOJOModel`, `H2OMOJOPipelineModel`) runs on any cluster size** in the supported mode, since it
   starts no H2O nodes. Scoring on autoscaling clusters has not been measured yet.
5. **The supported setup must not need cluster JVM options.** The `--add-exports` flags are a workaround for the
   3.46.0.6 release only; step 2.1b removes the cause. A new requirement for JVM options needs a new ADR.
6. **Databricks on AWS and GCP is best-effort:** expected to work in the same mode, not tested in CI, and fixed when
   reported. Detecting them (`AzureDatabricksUtils`) is step 5.3.
7. **The Azure Databricks CI job (step 5.1) tests exactly this mode**: dedicated classic job clusters on each supported
   DBR, scoring and training suites, with timings compared to `design/baseline-timings/`.

## Consequences

- The user docs (step 5.9) state the supported mode prominently: dedicated access mode, classic compute, a
  fixed-size cluster for training, and that standard, serverless and Spark Connect are not supported.
- Each milestone (M1–M3) is verified on Azure Databricks in this mode only, with the runners in `ci/databricksTests`.
- Databricks steers users towards standard and serverless compute, so the supported audience will shrink over time. A
  scoring-only path that works without RDDs, Spark RPC or Py4J could run on standard compute; it is a possible future
  goal and needs its own ADR.
- AWS and GCP users get no guarantee until a CI job covers them.
