# Renovation Plan: from today's tree to Spark 4.x on Azure Databricks

Start: `design/REPO_INDEX.md` (Spark 2.3–3.5, Scala 2.11/2.12, Java 8 target, javax servlet, Gradle 7.6).
Destination: `design/RENOVATION_GOAL_SPARK_4.x.md` (Azure Databricks dedicated compute; `H2OMOJOModel` deployment first,
training second; Flow UI removed, not ported).

Milestones (each is a demo on a real Azure Databricks cluster, scoring (a) before training (b)):
- **M1** – current code, Scala 2.13, Spark 3.5 → DBR 16.4 LTS (Scala 2.13 variant, JDK 17).
- **M2** – Spark 4.0 → DBR 17.3 LTS (JDK 17).
- **M3** – Spark 4.1 / 4.2 → DBR 18 LTS / 19 (JDK 21).
- **M4** – Spark 4.3 upstream build (no Databricks runtime yet).

Rules for every step:
- Tracking lives in `.tickets/` (`tk ready` gives the next step; `tk start` / `tk note` / `tk close`). This file is the
  narrative; the ticket tree is the order. Keep both in sync when a step is added or dropped.
- One step = one PR of **< 300 changed lines** (whole-directory deletions and regenerated code excluded). A step
  that will exceed it is split into ordered child tickets (`tk new ... --under <id>`), each within budget, as soon
  as that is clear, before or during the work; the parent closes when its children do.
- Each step names its verification command and is done only when it passes. Whenever a step touches the build graph,
  shared Gradle logic, source-set selection or generated code, the gate is additionally
  `./gradlew build -x check -Pspark=3.5` (Phase 1–2) or `-Pspark=4.0` (Phase 3+).
- Decisions go to `design/adr/ADR-NNN-*.md`. `∥` marks steps that may run in parallel with their neighbours.
- Scoring lane first: `utils` → `scoring` → `assembly-scoring` → `py-scoring` → Databricks demo. Training-only and
  non-Databricks work never blocks it.

---

## Phase 0 – Baseline, harness and decisions

- **0.1 Baseline build on current tree.** `./gradlew build -x check` then
  `./gradlew :sparkling-water-core:test :sparkling-water-utils:test :sparkling-water-ml:test` on JDK 11 (reference)
  and JDK 17 (target; the test JVMs get no Spark `--add-opens` flags, so failures there are expected and recorded).
  Record pass/fail per module in `design/baseline-2026-11.md`. No code changes.
- **0.2 Databricks scoring test runner.** `∥` Split into three steps:
  - **0.2a Scoring test scripts, verified locally.** `ci/databricksTests/scoring/test.scala`, `test.py`, `run_local.sh`;
    inputs via Spark conf; load `binom_model_prostate.mojo`, score `binom_model_prostate_prediction.csv` and compare with
    its expected columns, save/reload a `PipelineModel`, assert no `H2OContext`, print versions, end with
    `SCORING TEST PASSED`. Verify: `run_local.sh` green with released 3.46.0.6 scoring artifacts and Spark 3.5.x.
  - **0.2b Databricks driver script.** `run_databricks.sh` on the current `databricks` CLI: upload, install libraries on a
    dedicated cluster, run both scripts as job tasks. Workspace details are parameters. Verify: `--dry-run`, `shellcheck`.
  - **0.2c Verify on Azure Databricks.** 3.46.0.6 on DBR 16.4 LTS; result starts `design/databricks-baseline-2026-11.md`.
- **0.3 Azure Databricks baseline.** Run 0.2 and the existing `ci/databricksTests/test.scala`/`test.py` with release
  `3.46.0.6-1-3.5` on DBR 16.4 LTS (Scala 2.12). Record internal backend start time, autoscaling interaction, library
  install path. Output: `design/databricks-baseline-2026-11.md`. This is the behaviour M1–M3 must preserve.
- **0.4 ADR-002 Support matrix.** Keep only 3.5 on `master`; drop 2.3, 2.4, 3.0–3.4 (no Databricks runtime ships 3.4).
  A 4.x minor is added to `supportedSparkVersions` only when its build is green; default follows the Databricks LTS in use.
  Verify: `design/adr/ADR-002-*.md` merged and linked from this plan.
- **0.5 ADR-005 Supported Databricks mode.** Azure Databricks dedicated classic compute only; standard/serverless and
  Spark Connect out of scope; AWS/GCP best-effort. Verify: `design/adr/ADR-005-*.md` merged and linked from this plan.
- **0.6 ADR-003 Flow UI + ADR-004 REPL removal contract.** Remove `ProxyStarter`, `water/webserver/jetty9/*`,
  `backend/api/*`, the `repl` module and `core`'s driver-side Jetty 9 dependencies. API contract: `H2OContext.flowURL`
  and `openFlow` (Scala, Python, R) throw `UnsupportedOperationException` with a pointer to the migration guide;
  `H2OClusterInfo` loses `flowURL`; proxy-only options (`spark.ext.h2o.flow.ip/port`, proxy login, `repl.*`) are removed;
  options forwarded to H2O nodes (`flow.dir`, extra HTTP headers) stay. Replacement out of scope.
  Verify: `design/adr/ADR-003-*.md` and `ADR-004-*.md` merged and linked from this plan.
- **0.7 ADR-001 H2O-3 line and JDK 21.** `∥` Options: 3.46.0.6 OSS + `sys.ai.h2o.debug.allowJavaVersions=21` set by
  Sparkling Water in the executor before `H2OStarter.start`; H2O-3 Secure with Java 21; external backend only on DBR 18/19.
  Needed before M3, not before M1/M2 (both are JDK 17). Ticket 4.5 is rewritten from the chosen option when this closes.
  Verify: `design/adr/ADR-001-*.md` merged and linked from this plan.

## Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)

Scoring lane first (1.1–1.12), then training-only removals and ports (1.13–1.18).

- **1.1 Scoring test source set.** Create `scoring/src/test` with its own Spark-only `SparkTestContext` (local
  `SparkSession`, no H2O); add `testImplementation` spark-core/sql/mllib, scalatest, junit to `scoring/build.gradle`
  with **no** dependency on `:sparkling-water-core` or its `testArchives`, so the suite compiles on 2.13 / 4.0 / JDK 21
  ahead of `core`. Move the H2O-free MOJO suites (`H2OMOJOModelSparkRuntimeIndependencyTestSuite`,
  `H2OMOJOModelParallelTestSuite`, `H2OMOJOPipelineModelTestSuite`) from `ml` to `scoring`.
  Verify: `./gradlew :sparkling-water-scoring:test` runs the moved suites.
- **1.2 Scoring persistence fixtures.** Models and `H2OMOJOModel`/`H2OMOJOPipelineModel` pipelines saved by the current
  2.12 release under `scoring/src/test/resources/sw_scoring_scala_2.12_3.46/` (same convention as
  `ml/src/test/resources/sw_mojo_scala_2.12_df_java_serde`); `CrossVersionMOJOLoadTestSuite` loads, transforms and
  round-trips them. Captured **before** any serialization-affecting change. Verify: `./gradlew :sparkling-water-scoring:test`.
- **1.3 ML persistence fixtures + audit.** Pipelines with H2O estimators/models and every collection-typed `Param`
  saved by the current release under `ml/src/test/resources/sw_ml_scala_2.12_3.46/`; `CrossVersionPipelineLoadTestSuite`;
  table of Java-serialised Scala-collection params in `design/persistence-audit.md`. Verify: `./gradlew :sparkling-water-ml:test`.
- **1.4 Delete Spark 2.x.** Property files 2.3/2.4, `core/src/main/scala_spark_2.1|2.2|2.3`, `resources_spark_2.*`,
  `sparkMajorVersion <= "2.4"` branches (`ml`, `examples`, `gradle/utils.gradle`), 2.x in `ci/build.gradle`, docs.
  Verify: `./gradlew build -x check -Pspark=3.5`.
- **1.5 Delete Spark 3.0–3.4.** Property files 3.0–3.4, `utils/src/main/scala_spark_3.0…3.4`, fold 3.5 shims into
  `scala_spark_others`, `supportedSparkVersions=3.5`, CI matrix, docs. Verify as 1.4.
- **1.6 Collapse the UDF shim.** Keep `ai.h2o.sparkling.sql.functions.udf` and `org.apache.spark.sql.expose.SparkUserDefinedFunction`
  as one copy in `utils/src/main/scala_spark_others`. Do **not** switch the 12 call sites to the public
  `functions.udf(f, dataType)`: it throws unless `spark.sql.legacy.allowUntypedScalaUDF` is set (Spark ≥ 3.0) and the
  call sites return dynamic `StructType` rows. Verify: `:sparkling-water-scoring:test :sparkling-water-ml:test`.
- **1.7 Replace the RowEncoder shim.** `ExpressionEncoder(schema)`/`Encoders.row`; delete per-version `RowEncoder.scala`.
  After this `utils` has no version-specific `scala_spark_<X.Y>` dirs; `scala_spark_others` remains with the shared shims.
  Verify: core + ml tests + build gate.
- **1.8 ScalaTest 3.2.x, all modules.** One PR: scalatest 3.2.19, `scalatestplus-junit`, junit 4.13.2 in `build.gradle`
  (`scalaTestVersion` is a single root property and `utils`/`macros` tests depend on `core`'s test jar, so modules cannot
  migrate separately); migrate every `org.scalatest.junit.JUnitRunner`, `FunSuite`, `Matchers` import (~100 files, one or
  two lines each). Verify: `./gradlew test`.
- **1.9 Collections compat: `utils`, `extensions`, `scoring` main sources.** Add `scala-collection-compat`;
  `JavaConverters` → `scala.jdk.CollectionConverters`; procedure syntax, `WrappedArray`/`Traversable`/`Stream`.
  Verify: `compileScala` of the three modules with `-Xfatal-warnings` (pre-existing unrelated warnings are fixed or
  excluded in the same PR), then their tests.
- **1.10 Code-generation templates 2.13-ready.** Apply 1.9's rules in `api-generation/.../scala/*Template.scala`;
  regenerate; diff of `src-gen` reviewed. Verify: `:sparkling-water-ml:generateApi :sparkling-water-scoring:generateApi build`.
- **1.11 Fix unsafe params.** Store "needs JSON" params from the 1.3 audit as JSON/arrays via the Spark ML paramMap;
  keep reading the old form. Verify: both `CrossVersion*` suites + scoring + ml tests.
- **1.12 Rewire `py-scoring` to the scoring assembly.** `distPython` depends on `:sparkling-water-assembly-scoring:shadowJar`
  and the `apiGeneration` configuration uses `assembly-scoring` (shadow) + `api-generation` (shadow) + spark-repl, with no
  reference to `:sparkling-water-assembly`, so the scoring wheel never compiles `core`/`ml`. Verify: generated `src-gen`
  byte-identical on 3.5; `:sparkling-water-py-scoring:testPython`.
- **1.13 Remove the REPL module.** `settings.gradle`, `core`/`utils`/`assembly` project references, `H2OConf` interpreter
  options and defaults, `spark.ext.h2o.repl.*`, delete `core/.../backend/api/scalainterpreter/*` and its tests (they
  import the repl), REPL docs. Verify: `./gradlew :sparkling-water-core:test` + build gate.
- **1.14 Remove the driver Flow proxy and servlets.** Delete `ProxyStarter`, `water/webserver/jetty9/*`, the rest of
  `backend/api/*`, proxy-only tests (`LdapProxyOnlyModeAuthTestSuite`, `LdapAuthTestSuite`,
  `LdapAesEncryptedBindPasswordLoginModuleSuite`, SSL-proxy suites), driver Jetty 9 dependencies in `core/build.gradle`
  (H2O's own `h2o-jetty-9` transitive stays). Node-forwarded `flow.*` options stay. Verify: `:sparkling-water-core:test`;
  `H2OContextTestSuite` asserts no driver Jetty server is started.
- **1.15 Remove Flow exposure from the public API and UI.** `flowURL`/`openFlow` throw per ADR-003 in Scala, Python, R;
  `H2OClusterInfo` and `SparklingWaterInfoPage` lose the Flow link; `AzureDatabricksUtils` loses proxy-URL logic;
  docs and examples. Verify: `:sparkling-water-core:test :sparkling-water-py:testPython :sparkling-water-r:test`.
- **1.16 log4j2 test config.** Replace `core/src/test/resources/log4j.properties` and the other modules' test logging
  config with log4j2. Verify: `:sparkling-water-core:test :sparkling-water-ml:test` (no "log4j:WARN" output).
- **1.17 Collections compat: `core`, `ml` main sources.** Same rules as 1.9. Verify: `:sparkling-water-core:test :sparkling-water-ml:test`.
- **1.18 Collections compat: `api-generation`, `examples`, `benchmarks`, `doc`, all test sources.** Verify: build gate + `./gradlew test`.

## Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)

- **2.1 Gradle 8.x.** Wrapper 8.10+, shadow 8.x, `gradle-use-python` 4.x, spotless 6.x, docker plugin 9.x, release 3.x.
  Verify: `./gradlew build -x check`, `distPython`, `spotlessCheck`.
- **2.1a Spark Java 17 module flags for test JVMs.** Add Spark's `--add-opens` options (the list in Spark's
  `JavaModuleOptions`) as `jvmArgs` of the `test`, `integTest` and `bench` tasks in `gradle/sparkTest.gradle` and
  `gradle/scala.gradle`, only when the test JVM is 17 or newer (baseline 0.1: every Spark suite aborts on JDK 17 without
  them). Verify: `:sparkling-water-utils:test :sparkling-water-core:test` on JDK 17 match the JDK 11 baseline.
- **2.2 Separate JDK toolchain from bytecode target.** New properties `javaToolchainVersion=17` (global) and
  `bytecodeTargetVersion` (8 in `gradle-spark3.5.properties`, 17 for 4.x files) replacing the dual use of
  `minSupportedJavaVersion` in `gradle/scala.gradle`; scalac `-release`, javac `--release`; CI images `eclipse-temurin:17`.
  Verify: `./gradlew clean build -x check -Pspark=3.5` on JDK 17, class files report major version 52.
- **2.3 Spark 3.5 / Scala 2.13: `macros`, `utils`, `api-generation`, `scoring`.** `scalaTestVersions['2.13']`, 2.13
  `scalaVersion` set to the patch DBR 16.4 LTS ships (recorded in `gradle-spark3.5.properties`); `-Ymacro-annotations`
  replaces macro-paradise in `gradle/scala.gradle` for 2.13 (`scoring`'s `generateApi` builds `api-generation`, so it
  must be on 2.13 here). Verify: `./gradlew :sparkling-water-scoring:test -Pspark=3.5 -PscalaVersion=2.13.<dbr>`.
- **2.4 Spark 3.5 / Scala 2.13: scoring artifacts.** `assembly-scoring` shadow jar and `py-scoring` wheel with `_2.13`
  artifact names; `scoring-package` POM artifact id. Verify: `:sparkling-water-assembly-scoring:shadowJar
  :sparkling-water-py-scoring:distPython -Pspark=3.5 -PscalaVersion=2.13.<dbr>`.
- **2.5 M1a: scoring on Azure Databricks.** 2.4 jar + wheel on DBR 16.4 LTS (Scala 2.13) through the 0.2 runner with the
  1.2 fixtures. **Milestone M1a.**
- **2.6 Spark 3.5 / Scala 2.13: `extensions`, `core`.** Verify: `:sparkling-water-core:test` with the 2.3 flags.
- **2.7 Spark 3.5 / Scala 2.13: `ml`, `examples`, `py`/`r`, `package`, `assembly` (`_2.13` artifact names).**
  Verify: `./gradlew test -Pspark=3.5 -PscalaVersion=2.13.<dbr>`.
- **2.8 M1b: training on Azure Databricks.** Full `_2.13` assembly + wheel, `ci/databricksTests`, 1.3 fixtures load.
  Output: update `design/databricks-baseline-2026-11.md`. **Milestone M1.**

## Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)

Scoring lane (3.1–3.7, blocking), then training lane (3.8–3.16, does not block scoring).

- **3.1 `gradle-spark4.0.properties`** (Spark 4.0.0, Scala 2.13.16, JDK 17 target, Python 3.12, `databricksVersion=17.3`);
  not yet in `supportedSparkVersions`. Verify: `./gradlew help -Pspark=4.0`.
- **3.2 `macros`, `utils` on 4.0.** `hiveClassesArePresent`; 4.x copy of `expose.SparkUserDefinedFunction` (constructor takes
  `Option[Encoder[_]]`) next to the 3.5 one; cut `utils`'s test dependency on `:sparkling-water-core`/`testArchives` by
  moving the one `TestUtils` helper into `utils/src/test` (`getLocalDir` needs no change: `SparkConf` extends
  `ReadOnlySparkConf`). Verify: `-Pspark=4.0 :sparkling-water-utils:test`.
- **3.3 `api-generation`, `scoring` on 4.0.** `GenericRowWithSchema`/`ExpressionEncoder` usage in `H2OMOJOPrediction*`,
  `DefaultParams{Reader,Writer}` SparkSession overloads, ANSI-mode conversions; regenerate scoring API.
  Verify: `:sparkling-water-scoring:test -Pspark=4.0` with `spark.sql.ansi.enabled=true`.
- **3.4 `assembly-scoring` on 4.0.** Exclusions re-derived from the 4.0 dependency tree; `bin/check-duplicate-classes.sh`
  committed. Verify: `:sparkling-water-assembly-scoring:shadowJar -Pspark=4.0` + the checker; 3.5 still green.
- **3.5 `py-scoring` on 4.0** (Python 3.12). Verify: `:sparkling-water-py-scoring:testPython -Pspark=4.0`.
- **3.6 M2a: MOJO scoring on Azure Databricks.** 0.2 runner with the 1.2 fixtures on DBR 17.3 LTS dedicated cluster.
  **Milestone M2a.**
- **3.7 `extensions` on 4.0.** Verify: module test.
- **3.8 `core` – Spark UI tab sources per Spark line.** Move `SparklingWaterInfoPage`, `SparklingWaterUITab`,
  `CrossSparkUtils` implementations into `core/src/main/scala_spark_3.5` (javax) and `scala_spark_others` (jakarta);
  keep the `headerSparkPage` reflection shim; H2O-side `javax` contract untouched (`extensions` keeps `compileOnly
  servlet-api`, assembly keeps excluding it). Verify: `:sparkling-water-core:test -Pspark=3.5` green; the 4.0 run is 3.9.
- **3.9 `core` – internal backend on 4.0.** RPC/scheduler private APIs (`RpcEnv`, `ThreadSafeRpcEndpoint`,
  `CoarseGrainedSchedulerBackend`). Verify: `-Pspark=4.0 :sparkling-water-core:test` incl. `H2OContextTestSuite`; no
  `javax.servlet` import outside `extensions`.
- **3.10 `core` – Kubernetes external backend on fabric8 7.x.** `K8sExternalBackendClient`, `K8sHeadlessService`,
  `K8sH2OStatefulSet`. Verify: core compiles on 3.5 and 4.0; K8s unit tests.
- **3.11 `core` – external backend rest + `DefaultParams*` SparkSession overloads.** Verify: core tests, external backend suite.
- **3.12 `ml` on 4.0** (+ regenerate). Verify: `:sparkling-water-ml:test -Pspark=4.0`, ANSI on.
- **3.13 `examples`, `benchmarks`, `doc` on 4.0.** Verify: `./gradlew build test -Pspark=4.0`.
- **3.14 Full `assembly` + `assembly-extensions` on 4.0; advertise 4.0.** Exclusions, checker, `supportedSparkVersions=3.5 4.0`.
  Verify: `:sparkling-water-assembly:shadowJar -Pspark=4.0` + checker; `./gradlew build -Pspark=3.5` still green.
- **3.15 PySparkling on 4.0** (Python 3.12). Verify: `:sparkling-water-py:testPython -Pspark=4.0`.
- **3.16 M2b: training on Azure Databricks.** DBR 17.3 LTS dedicated cluster, `ci/databricksTests`, 1.3 fixtures; Spark UI
  tab renders. Flip `spark=4.0` default. **Milestone M2.**

## Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)

- **4.1 Property files 4.1 and 4.2 + scoring lane build.** `macros`, `utils`, `api-generation`, `scoring`,
  `assembly-scoring`, `py-scoring` on `-Pspark=4.1` and `-Pspark=4.2`; cross-minor shims only.
  Verify: `:sparkling-water-scoring:test :sparkling-water-py-scoring:testPython` per minor.
- **4.2 M3a: scoring on JDK 21.** `scoring` + `py-scoring` need no H2O node. Verify: scoring tests on JDK 21; 0.2 runner
  with the 1.2 fixtures on DBR 18 LTS and 19. **Milestone M3a.**
- **4.3 Full-tree build on 4.1 and 4.2; advertise.** `./gradlew build test -Pspark=4.1` / `-Pspark=4.2`; add both to
  `supportedSparkVersions`.
- **4.4 Cross-minor guard test.** Each artifact refuses the other minor (4.1 vs 4.2). Verify: new test in `core`.
- **4.5 JDK 21 for training per ADR-001.** Rewritten from the 0.7 decision. If OSS H2O-3: set
  `sys.ai.h2o.debug.allowJavaVersions` programmatically in `InternalH2OBackend.startH2OWorker` (and driver client start)
  gated by an `H2OConf` flag; H2O-dependent suites on JDK 21. Verify: `./gradlew test -Pspark=4.2` on JDK 21.
- **4.6 PySparkling on 4.1/4.2.** Verify: `testPython` per minor.
- **4.7 M3b: training on Azure Databricks.** DBR 18 LTS and 19 dedicated clusters, `ci/databricksTests`. **Milestone M3.**

## Phase 5 – Breadth, CI, docs, release

- **5.1 Azure Databricks CI job.** `ci/Jenkinsfile-databricks` rewritten for Azure and the current CLI; reads
  `databricksVersion` per property file; matrix over 16.4 LTS (2.13), 17.3 LTS, 18 LTS, 19; scoring runner + full
  tests; dedicated access mode in the cluster JSON.
- **5.2 Spark 4.3 upstream build** (M4). Property file + `-Pspark=4.3` build and tests on standalone; 1.2 fixtures load in
  the 4.3 scoring suite; guard test extended so the 4.3 artifact refuses 4.1/4.2; CI entry.
- **5.3 Databricks on AWS/GCP.** `AzureDatabricksUtils` → `DatabricksUtils` keyed on
  `spark.databricks.clusterUsageTags.clusterId`; unit test with synthetic `SparkConf`s; one manual run on AWS. Best-effort.
- **5.4 rsparkling** on 4.x with sparklyr ≥ 1.9. Low priority.
- **5.5 Standalone Spark 4.x CI matrix** with `integTest`/`integTestPython`.
- **5.6 Kubernetes images** for 4.x with `integTest` on K8s.
- **5.7 EMR 8 templates.**
- **5.8 YARN external backend** launcher on 4.x.
- **5.9 Docs.** `migration_guide.rst` 3.46→4.x incl. Flow/REPL removal and `flowURL` behaviour; Azure Databricks pages
  updated with dedicated-mode, MOJO deployment and autoscaling notes; README badges; `REPO_INDEX.md` refresh.
- **5.10 Release.** Version suffixes `-4.0/-4.1/-4.2/-4.3`, Maven `_2.13`, PyPI `h2o_pysparkling_4.x` and
  `h2o_pysparkling_scoring_4.x`, S3 layout.

## Done when

All "Definition of done" items in `design/RENOVATION_GOAL_SPARK_4.x.md` §6 pass and every ticket under the
renovation epics is closed.
