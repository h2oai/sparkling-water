# Renovation Goal: Sparkling Water on Apache Spark 4.x, Databricks first

Status: target definition (analysis only, no code changed). Written 2026-11-06 against commit `1fc226532`
(Sparkling Water `3.46.0.2-1-SNAPSHOT`, H2O-3 `3.46.0.6`, default build Spark 3.5 / Scala 2.12.18 / Java 8 target).
Companion to `design/REPO_INDEX.md`. Decisions taken while executing this goal belong in `design/adr/`.

## 1. Goal statement

**Primary objective: Sparkling Water runs on Azure Databricks (dedicated compute), and `H2OMOJOModel` deployment
(scoring of trained H2O models inside Spark pipelines) is the first-class use case.** Order of priority:

1. MOJO deployment: `H2OMOJOModel` / `H2OMOJOPipelineModel` load, transform and persist on each target runtime,
   from the `scoring` module (depends only on `utils`, needs no H2O cluster) and the `py-scoring` wheel.
2. Training: `H2OContext` with the internal backend (H2O nodes in executors) and the `ml` algorithms.
3. Everything else: Databricks on AWS/GCP, standalone Spark, Kubernetes, EMR, YARN. Secondary; must not block 1–2.

**Flow UI is not a priority and will be replaced.** The driver-side Jetty 9 proxy (`ProxyStarter`), the Flow extension
servlets (`backend/api/*`) and the embedded Scala REPL exist only to serve Flow; they are removed from the Spark 4 line
rather than ported (ADR-003/004). H2O-3's own Jetty 9 inside the executors is untouched.

Make Sparkling Water build, test, and ship for the Spark versions that Databricks Runtime ships, from one source tree,
with one artifact per Spark minor version (the model already used for 3.4 and 3.5). "Ship" means: Scala assembly jar
and PySparkling wheel installable as Databricks cluster libraries, docs, and a Databricks CI job, for each target minor.

### Databricks Runtime matrix (verified 2026-11-06 from Databricks release notes)

| DBR | Spark | Scala | JDK | Python | Status | Sparkling Water target |
|---|---|---|---|---|---|---|
| 16.4 LTS | 3.5.2 | 2.12 **or 2.13** (two variants) | 17 | 3.12 | LTS (2025-05) | current release works (2.12); **stepping stone for the Scala 2.13 port** (3.5 + 2.13 variant) |
| 17.3 LTS | 4.0.0 | 2.13.16 | 17 default, 21 preview | 3.12 | LTS (2025-10) | **M2: first Spark 4 target** (JDK 17 keeps H2O-3 OSS supported) |
| 18 LTS | 4.1.0 | 2.13.16 | **21 default**, 17 optional fallback | 3.12 | LTS (2026-07) | M3 |
| 19 | 4.2.0 | 2.13.18 | **21 only** | 3.12 | GA (2026-07) | M3, requires JDK 21 decision (ADR-001) |
| 20 (future) | 4.3? | 2.13 | 21 | 3.12/3.13 | not released | 4.3 stays an upstream-only target until then |

Consequences:
- Target Spark minors are **4.0, 4.1, 4.2** (Databricks-driven) plus **4.3** (upstream, lower priority). 3.5 stays
  supported for DBR 16.4 LTS. Milestones: **M1** = Scala 2.13 build on Spark 3.5 running on DBR 16.4 LTS (2.13 variant);
  **M2** = Spark 4.0 on DBR 17.3 LTS (JDK 17); **M3** = Spark 4.1/4.2 on DBR 18/19 (JDK 21); **M4** = 4.3 upstream.
- Python matrix collapses to **3.12** for Databricks; broader ranges are nice-to-have for PyPI only.
- **JDK 21 is unavoidable** for DBR 18 (default) and 19 (only). H2O-3 OSS supports Java ≤ 17 and refuses to start
  otherwise; its documented override is the system property `sys.ai.h2o.debug.allowJavaVersions=21` ("only for
  experiments"). Internal backend on DBR 18/19 therefore needs either that override set programmatically in
  `InternalH2OBackend.startH2OWorker` before `H2OStarter.start` (Databricks blocks `spark.executor.extraJavaOptions` on
  DBR 19 standard mode and discourages it elsewhere), or an H2O-3 Secure build with Java 21 support, or the external
  backend (H2O cluster outside Databricks, driver only talks REST). ADR-001 decides.
- **Scoring before training inside every milestone.** `scoring` + `py-scoring` are ported and verified on the
  milestone's runtime first (no `H2OContext` needed), then `core`/`ml`. A milestone is "scoring-complete" before it is
  "training-complete".
- **Dedicated access mode only.** Standard (shared) and serverless compute run Spark Connect and forbid RDD APIs,
  `sparkContext` in Scala, custom JARs/`spark.jars`, driver/executor JVM options (DBR 19+), and R. Sparkling Water
  depends on all of these (RDD-pinned conversions, Spark RPC, Py4J `_jvm`). ADR-005 becomes "Databricks dedicated
  classic compute only; Spark Connect/serverless out of scope", stated prominently in docs.
- `AzureDatabricksUtils` recognises Azure only (`spark.databricks.cloudProvider == Azure`), which matches the primary
  target. Generalising to AWS/GCP (`spark.databricks.clusterUsageTags.*`) is secondary; after the Flow proxy is removed
  the class only tunes timeouts.
- Internal backend vs. Databricks autoscaling: H2O cluster size is fixed at start; document "disable autoscaling or use
  external backend", and make `spreadRDD` retries robust to Databricks' slow executor start.
- Deprioritised (keep building, do not gate milestones): Databricks on AWS/GCP, Kubernetes images, EMR templates,
  YARN external backend launcher, booklet, rsparkling (R works only on dedicated compute), Flow UI and its replacement.

### Spark 4.x minor-version policy

- Spark keeps public APIs source- and binary-stable within the 4.x line, but Sparkling Water uses private APIs
  (`org.apache.spark.*` packages, Catalyst internals, UI internals). Those do drift between minors (see table 2b), so
  the per-minor artifact and the runtime guard in `H2OContext` (`runningOnCorrectSpark`, artifact suffix `-4.1`/`-4.2`/`-4.3`)
  stay in place.
- All 4.x minors must compile from the **same sources**: one `scala_spark_others` (or none) for 4.x, no
  `scala_spark_4.0`/`4.1`/`4.2`/`4.3` directories. Where a private signature differs between minors (today only
  `UIUtils.headerSparkPage`: 7 params on 4.0/4.1, 8 on 4.2/4.3), keep a reflection-based shim rather than per-version sources.
- Differences between minors are expressed only in `gradle-spark4.0/4.1/4.2/4.3.properties` (Spark version, Scala
  patch, fabric8 version, Python range, `databricksVersion`).
- Default build follows the current Databricks LTS: `spark=4.0` at M2, `spark=4.1` once DBR 18 LTS is the primary
  target. 4.3 is a CI matrix entry only. A minor is dropped from `supportedSparkVersions` only by an ADR.

## 2. What Spark 4.x changes versus the current build (verified facts)

### 2a. Common to 4.1, 4.2, 4.3 (versus the current Spark 3.5 / Scala 2.12 build)

| Dimension | Sparkling Water today | Spark 4.x requires |
|---|---|---|
| Scala | 2.12.18 (2.11 for Spark 2.x) | **2.13 only** (2.12 dropped in 4.0, SPARK-45314) |
| Java | compiled with `-target:jvm-1.8`, CI/Docker on JDK 8/11 | **17 or 21** (8 and 11 dropped in 4.0, SPARK-45315); 25 added in 4.2; Databricks defaults to 21 from DBR 18 |
| Python | 3.6–3.11 in `pythonEnvironments`, numpy pins for 3.6/3.7 | **3.9+ (4.0), 3.10+ (4.1, 4.2), 3.11+ (4.3)**; Databricks ships 3.12 |
| R | sparklyr >= 0.3 | sparklyr 1.9.x supports Spark 4 / Scala 2.13; SparkR itself is deprecated |
| Servlet API | `javax.servlet` everywhere (Spark UI page, Flow proxy, H2O-side servlets) | Spark internals moved to **`jakarta.servlet`** (4.0 core migration guide); Jetty 11 or 12 depending on minor |
| Hadoop | binaries hadoop2/hadoop3 chosen by version in `ci/build.gradle` | Hadoop 3.4/3.5 client libs; Hadoop 2 binaries gone |
| Kubernetes client | fabric8 6.4.1 (compileOnly, "available via Spark") | fabric8 **7.x** (`DefaultKubernetesClient` removed) |
| Logging | `log4j.properties` (log4j 1 syntax) in tests | log4j2 2.24+; `log4j2.properties`, JSON log output by default from spark-submit |
| JSON | json4s 3.x API | json4s 4.0.7 |
| ScalaTest | 3.0.8 (`FunSuite`, `org.scalatest.junit.JUnitRunner`) | 2.13 artifacts only exist for 3.2.x (`AnyFunSuite`, `org.scalatestplus.junit.JUnitRunner`) |
| SQL defaults | ANSI off | `spark.sql.ansi.enabled=true` by default (SPARK-44444) |
| Spark private API | `UIUtils.headerSparkPage` via reflection, `DefaultParamsReader.loadMetadata(path, sc)` | `headerSparkPage` takes a jakarta request and a minor-dependent parameter list (see 2b); `loadMetadata/saveMetadata(.., SparkContext)` are **deprecated** since 4.0, SparkSession overloads replace them (same in 4.1–4.3) |
| SparkSession | `SparkSession.hiveClassesArePresent` via `org.apache.spark.ExposeUtils` | class split: `org.apache.spark.sql.SparkSession` (API) vs `org.apache.spark.sql.classic.SparkSession` (impl); `hiveClassesArePresent` is `private[spark]` on the classic companion |
| Utils | `Utils.getLocalDir(conf: SparkConf)` | `getLocalDir(conf: ReadOnlySparkConf)`; `createTempDir` still present (via `SparkFileUtils`) |
| REPL | custom `H2OIMain extends scala.tools.nsc.interpreter.IMain(settings, JPrintWriter)` copied from the 2.11 `SparkILoop` | Spark 4 `SparkILoop extends scala.tools.nsc.interpreter.shell.ILoop(ShellConfig, in, out)`; 2.13 `IMain` has a different constructor, `JPrintWriter` and `Naming` moved/removed |
| Platforms | Databricks `14.0.x-cpu-ml-scala2.12`, EMR 6.x | Databricks Runtime 18 = Spark 4.1, Runtime 19 = Spark 4.2 (**JDK 21 only**); EMR 8.x = Spark 4.0; no platform ships 4.3 yet |

### 2b. Differences between the target minors (verified from each release's `pom.xml` and sources)

| Item | 4.0.0 | 4.1.0 | 4.2.0 | 4.3.0 | Effect on Sparkling Water |
|---|---|---|---|---|---|
| Java | 17, 21 | 17, 21 | 17, 21, 25 | 17, 21, 25 | compile with `-release 17`; test on 17 and 21 |
| Scala | 2.13.16 | 2.13.17 | 2.13.18 | 2.13.18 | patch only; one `scalaVersion` per property file |
| Python | 3.9+ | 3.10+ | 3.10+ | 3.11+ | `supportedPythonVersions` per property file; Databricks is 3.12 on all |
| Jetty / jakarta servlet | 11.0.24 / 5.0.0 | 11.0.26 / 5.0.0 | 12.1.8 / 6.0.0 | 12.1.8 / 6.0.0 | Spark relocates Jetty; only the servlet API namespace is visible. Code compiled against servlet 6 runs on 5 if it avoids methods removed in 6. Compile the UI page against the lowest (4.0) in CI once |
| `UIUtils.headerSparkPage` | 7 params | 7 params | 8 params (`useTimeline`) | 8 params | **binary-incompatible between 4.0/4.1 and 4.2+**; keep the reflection shim in `SparkSpecificUtils` |
| fabric8 kubernetes-client | 7.1.0 | 7.4.0 | 7.6.1 | 7.6.1 | same major; `fabricK8sClientVersion` per property file; irrelevant on Databricks |
| Hadoop | 3.4.1 | 3.4.2 | 3.5.0 | 3.5.0 | excluded from assembly; CI tarball naming only |
| log4j2 / jackson / netty / guava | 2.24.3 / 2.18.2 / 4.1.118 / 33.4.0 | 2.24.3 / 2.20.0 / 4.2.7 / 33.4.8 | 2.25.4 / 2.21.2 / 4.2.13 / 33.6.0 | same as 4.2 | assembly exclusions derived once from the union; no code impact |
| `Utils.getLocalDir(ReadOnlySparkConf)`, `ExpressionEncoder(schema)`, `DefaultParams{Reader,Writer}` SparkSession overloads, `AppHistoryServerPlugin` (with `displayOrder`) | identical (verified 4.1/4.3; 4.0 to confirm at compile) | identical | identical | identical | one implementation for all 4.x |
| scalatest / json4s | 3.2.19 / 4.0.7 | same | same | same | one test dependency set |

### H2O-3 side constraints (strategic, must be decided first)

- H2O-3 open source supports **Java 8–17** only (3.46.0.12 docs). It ships `h2o-jetty-8`, `h2o-jetty-9`,
  `h2o-jetty-9-minimal`, `h2o-webserver-iface`; there is **no jakarta/Jetty 11+ web server module** in the public repo.
- H2O.ai announced (blog, 2026-08-12) that from **3.46.0.12** the MOJO runtime, Hadoop, Kubernetes deployment and
  **Sparkling Water** move out of the open-source channel into "H2O-3 Secure", and that "Sparkling Water is locked to
  3.46.0.6". This repo pins `h2oBuild=6`, which matches.
- Consequence: a Spark 4.x port needs a decision on which H2O-3 line it targets (3.46.0.6 OSS frozen, or an H2O-3
  Secure build with Java 21 / Jetty 12 support) and whether Java 21 is in scope at all. See Phase 0.

## 3. Inventory of coupling points to change

Grouped by module; file paths relative to repo root. Counts are from grep on 2026-11-06.

### 3.1 Build system (`build.gradle`, `gradle/*.gradle`, `gradle-spark*.properties`)
- `gradle.properties`: `supportedSparkVersions=2.3 … 3.5`, `spark=3.5`, `pythonEnvironments=3.6 … 3.11`,
  `jettyVersion=9.4.53`, `testingBaseImage` (CDH 6.3 image), `dockerImageVersion`.
- Four new property files, differing only in these keys:
  - `gradle-spark4.0.properties`: `sparkVersion=4.0.x`, `scalaVersion=2.13.16`, `fabricK8sClientVersion=7.1.0`,
    `supportedPythonVersions=3.9 … 3.12`, `databricksVersion=17.3.x-scala2.13` (LTS, JDK 17).
  - `gradle-spark4.1.properties`: `sparkVersion=4.1.x`, `scalaVersion=2.13.17`, `fabricK8sClientVersion=7.4.0`,
    `supportedPythonVersions=3.10 … 3.12`, `databricksVersion=18.x-scala2.13` (LTS, JDK 21 default).
  - `gradle-spark4.2.properties`: `sparkVersion=4.2.x`, `scalaVersion=2.13.18`, `fabricK8sClientVersion=7.6.1`,
    `supportedPythonVersions=3.10 … 3.12`, `databricksVersion=19.x-scala2.13` (JDK 21 only).
  - `gradle-spark4.3.properties`: `sparkVersion=4.3.0`, `scalaVersion=2.13.18`, `fabricK8sClientVersion=7.6.1`,
    `supportedPythonVersions=3.11 3.12 3.13`, no `databricksVersion` yet.
  - All: `minSupportedJavaVersion=17`, `unsupportedMinorSparkVersions=` (fill from Spark release notes).
- `gradle-spark3.5.properties` additionally needs a Scala 2.13 variant (`-PscalaVersion=2.13.x` override already
  works through `loadVersionSpecificProp`) so that M1 can be built and tested on DBR 16.4 LTS (2.13).
- `gradle.properties`: `supportedSparkVersions` grows from `3.5` as each 4.x minor turns green (ADR-002), `spark=4.0` at M2.
- `build.gradle`: `scalaTestVersions` map only knows `2.11`/`2.12` (line 81); `numpyVersions` keyed by 3.6/3.7;
  version suffix logic assumes `-<sparkMajor>` and `supportedSparkVersions` list.
- `gradle/scala.gradle`: `-target:jvm-${minSupportedJavaVersion}` must become `-release 17`; macro paradise plugin is
  gated on `scalaVersion < '2.13'` (fine) but `macros` module itself needs 2.13 `scala-reflect`/macro API check;
  `sourceCompatibility` derived from `1.8` string; Gradle 7.6 toolchain: OK for JDK 17, **JDK 21 needs Gradle >= 8.5**,
  shadow 7.1.2 and `gradle-use-python` 2.3.0 also need bumps for Gradle 8.
- `gradle/utils.gradle`: `getSparkSpecificSourceDir` picks `scala_spark_<major>` or `scala_spark_others`; 2.x branches
  (`sparkMajorVersion < "2.4"`, `<= "2.4"` in `ml`, `examples`) are dead once 2.x is dropped.
- `assembly/build.gradle`: exclusion list is hand-maintained against Spark 2.11-era transitive deps (comments still say
  `spark-core_2.11`); must be re-derived against Spark 4.3's dependency tree (guava 33, jackson 2.21, netty 4.2,
  protobuf 4, json4s 4, log4j2, no `jackson-core-asl`, no `aws-java-sdk-bundle`).
- `core/build.gradle`: pulls Jetty 9.4 (`jetty-server/servlets/jaas/proxy/servlet/websocket`) and `h2o-jetty-9` as `api`.
- `ci/build.gradle`: Spark tarball naming (`hadoop2.7`/`hadoop2`/`hadoop3`), Python/R install scripts, JDK in images;
  `kubernetes/build.gradle` external-backend image is `FROM openjdk:11`.

### 3.2 `utils` (Spark private API shims)
- `org.apache.spark.ExposeUtils.hiveClassesArePresent` → classic `SparkSession` companion, `private[spark]`; keep the
  `org.apache.spark` package trick or replace with `Utils.classIsLoadable("org.apache.hadoop.hive.conf.HiveConf")`.
- `org.apache.spark.expose.Utils.getLocalDir(SparkConf)` → `ReadOnlySparkConf`; `postToListenerBus` uses
  `sparkContext.listenerBus` (private[spark], still present).
- `scala_spark_3.0 … 3.5/org/apache/spark/sql/expose/SparkUserDefinedFunction.scala` + `ai/h2o/sparkling/sql/functions.scala`:
  wraps the private `SparkUserDefinedFunction` case class; in 4.x it still exists in `org.apache.spark.sql.expressions`
  but the Dataset/Column API moved to `sql-api` + `sql.classic`. Preferred fix: drop the shim and use public
  `functions.udf(f, dataType)` (12 call sites in `scoring`/`ml`/`core`).
- `ai/h2o/sparkling/sql/catalyst/encoders/RowEncoder.scala` (one per Spark version): 4.1, 4.2 and 4.3 all have
  `ExpressionEncoder(schema: StructType)`, so one `scala_spark_others` shim covers 4.x (or drop the shim entirely).
- `org.apache.spark.sql.{DatasetExtensions, DataTypeExtensions, SWGenericRow}`: `DataType.jsonValue`/`parseDataType`
  are `private[sql]` and still exist; `GenericRow` moved to `sql-api` but same package. Verify at compile time.
- `SparkSessionUtils`: `StaticSQLConf.CATALOG_IMPLEMENTATION` still internal; `SparkSession.builder().getOrCreate()` fine.

### 3.3 `core`
- **Servlet namespace** (33 files import `javax.servlet` or `org.eclipse.jetty`): two distinct uses.
  1. Spark UI integration (`org/apache/spark/h2o/ui/*`, `SparkSpecificUtils`, `CrossSparkUtils`): must switch to
     `jakarta.servlet.http.HttpServletRequest`. `UIUtils.headerSparkPage` has 7 parameters on 4.1 and 8 on 4.2/4.3, so
     the existing reflection-based call in `SparkSpecificUtils` **stays** (it already handles an extra trailing Boolean);
     it is the only Spark private signature that differs across the target minors. Spark relocates its Jetty to
     `org.sparkproject.jetty`, so only the servlet API namespace (5.0 on 4.1, 6.0 on 4.2+) matters here; restrict the
     page code to the servlet 5 API surface.
  2. Flow proxy + driver REST servlets (`ProxyStarter`, `water.webserver.jetty9.SparklingWaterJettyHelper`,
     `backend/api/*Servlet`): these run on Sparkling Water's **own** Jetty 9 server embedded in the driver and extend
     H2O's `Jetty9Helper`/`H2OHttpView`. They can stay on javax + Jetty 9.4 only if `javax.servlet-api` and
     unrelocated `org.eclipse.jetty` 9.4 are bundled in the assembly and do not clash with Spark 4's classpath
     (Spark no longer ships `javax.servlet-api`; Hadoop 3.5 may). Alternative: move the proxy to Jetty 12 EE10
     (needs an H2O web-server module that does not exist in OSS) or replace the proxy with a servlet-free HTTP
     reverse proxy (e.g. JDK `HttpClient` + `com.sun.net.httpserver` or Netty). **Decision direction (goal
     statement, ADR-003): remove.** Flow is not a priority; deleting `ProxyStarter`, `SparklingWaterJettyHelper`,
     `backend/api/*` and the Jetty 9 dependencies of `core` takes the driver-side servlet problem off the table.
     `H2OContext.flowURL`/`openFlow` throw `UnsupportedOperationException` pointing to the migration guide; proxy-only
     options (`spark.ext.h2o.flow.ip/port`, proxy login, `repl.*`) are removed; options forwarded to H2O nodes stay.
- **Spark UI listener/store** (`scala_spark_others/org/apache/spark/h2o/ui/{AppStatusListener,AppStatusStore,AppStatusPlugin}`):
  `AppHistoryServerPlugin`, `ElementTrackingStore`, `LiveEntity`, `KVIndexParam` all still exist in 4.3 and are
  `private[spark]`; code stays in `org.apache.spark.h2o.ui`. `AppHistoryServerPlugin` gained `displayOrder` (has default).
- **Internal backend** (`org/apache/spark/h2o/backends/internal/*`): `RpcEnv.create`, `ThreadSafeRpcEndpoint`,
  `SparkEnv.get.securityManager`, `CoarseGrainedSchedulerBackend.getExecutorIds`, `sc.schedulerBackend`,
  `blockManager.master.getStorageStatus` are private[spark] APIs that still exist but must be recompiled against 4.3
  and re-tested (executor pod batching and `ReadWriteOncePod` changes affect Kubernetes-internal mode).
- **External backend**: `K8sExternalBackendClient` uses `DefaultKubernetesClient` (removed in fabric8 7.x; use
  `KubernetesClientBuilder`), `K8sH2OStatefulSet`, `K8sHeadlessService` need the 7.6.1 API.
- **Converters/data path**: `H2ODataFrame` produces `InternalRow`, `Reader` uses `UTF8String`,
  `TimeZoneConversions` uses `DateTimeUtils.fromUTCTime/toUTCTime` (catalyst internals, still present in 4.x);
  `H2OFrameRelation`/`DefaultSource` are DataSource V1 (`BaseRelation`, `PrunedScan`, `RelationProvider`), still
  supported in 4.3 but legacy; consider DSv2 `Table`/`ScanBuilder` as a follow-up, not a prerequisite.
- **ANSI mode**: conversions and tests that rely on lenient casts (`DataFrameConverterTestSuite`, string→number
  columns, overflow on timestamp→int) will change behaviour under `spark.sql.ansi.enabled=true`.
- `SparkTimeZone` (`sessionState.conf.sessionLocalTimeZone`) is `private[sql]` access; still compiles from
  `ai.h2o.sparkling` only because `sessionState` is exposed; verify.
- `water.webserver.jetty9.LdapAesEncryptedBindPasswordLoginModule` depends on `jetty-jaas` 9.4.

### 3.4 `repl` (removal candidate; its only consumer is the Flow Scala cell, see 3.3)
- `H2OIMain extends IMain(settings, JPrintWriter)`, `IntpResponseWriter extends JPrintWriter`, `H2OIMainHelper`
  uses `scala.tools.nsc.interpreter.Naming`, `BaseH2OInterpreter` uses `intp.beQuietDuring/beSilentDuring/valueOfTerm/
  compileString/initializeSynchronous/global`. In Scala 2.13 the REPL was restructured (`scala.tools.nsc.interpreter.shell`,
  `IMain(settings, parentClassLoader: Option[ClassLoader], compilerSettings, reporter: ReplReporter)`), `JPrintWriter`
  is gone, and `org.apache.spark.repl.Main.interp.intp.classLoader` access changes. Options: (a) port to the 2.13
  `IMain` API, (b) replace the Flow "Scala cell" backend with the JSR-223 `scala.tools.nsc.interpreter.shell.Scripted`
  engine, (c) drop the embedded interpreter (it is already disabled in tests via `spark.ext.h2o.repl.enabled=false`).
  Scope decision required (Phase 0).

### 3.5 `scoring`
- `org.apache.spark.ml.util.expose.DefaultParams{Reader,Writer}` wrap the `SparkContext` overloads, deprecated in 4.0;
  switch to the `SparkSession` overloads and extend the exposed `Metadata` (now has `timestamp`, `sparkVersion`,
  `defaultParams`, `metadata`).
- 13 `H2OMOJOPrediction*`/base classes use `GenericRowWithSchema`, `ExpressionEncoder`, and the UDF shim; move to
  public `functions.udf` + `Encoders.row(schema)`/`ExpressionEncoder(schema)`.
- `H2OMOJOModel.scala` has the `SerializationWarningsObject`/`CompatibilityObjectInputStream` logic for cross-version
  model loading; Scala 2.12→2.13 changes serialized forms of collections (`WrappedArray`→`ArraySeq`, `List`, `Map`
  implementations). Models saved with 3.x/2.12 will not deserialize Scala collection params on 2.13 unless params are
  stored as JSON/arrays. Audit every `Param` type in `scoring/params` and `ml/params` for Java-serialized Scala collections.

### 3.6 `ml`, `api-generation`, generated code
- `api-generation` templates emit Scala using `Seq`, `Array`, `Map` and implicit conversions; 2.13 differences that
  matter: `Seq` is immutable by default (86 main files mention `Seq`/`toSeq`), `JavaConverters` is deprecated (22 files)
  but still works, procedure syntax (`def f() {`) is deprecated (approx. 8 occurrences), symbol literals none,
  `Stream`/`view` 1 occurrence. Fix generated templates once, then regenerate.
- `H2OGridSearch`, `H2OAutoML`, `H2OStackedEnsemble`, `HyperParamsParam`, `NullableAlgoArrayParam`, `DictionaryParam`
  rely on `Map[String, Any]`/`Array[Any]` conversions; compile-check under 2.13.
- `macros/DeprecatedMethodMacro` (whitebox macro with `scala.reflect.macros.whitebox.Context`): works on 2.13 without
  paradise; `gradle/scala.gradle` already skips paradise for >= 2.13.
- `DeprecatableParams`/`H2OAlgorithmCommonParams` carry deprecated parameters; decide what to drop in a major bump.

### 3.7 `py`, `py-scoring`
- Everything is Py4J-based (`_java_obj` 85 uses, `_jvm` 21, `_jconf` 10, `_jdf`, `_jsparkSession`,
  `SparkSession._instantiatedSession`). All of this still works on PySpark 4.1–4.3 **classic** mode but is impossible under
  Spark Connect (`spark.api.mode=connect`, `pyspark-client`). Decision: declare Spark Connect out of scope for this
  goal, but stop adding new private-attribute accesses.
- `py/build.gradle` pins `pytest:6.2.5`, `requests:2.27.1`, `numpy` by Python minor, `pyspark:${sparkVersion}`;
  `setup.py` uses `SUBST_PYTHON_VERSIONS` from `supportedPythonVersions`; conda recipe directory name carries the Spark
  major. Need Python 3.11–3.13 wheels, NumPy >= 1.23.2, pytest 8.
- `ml/H2OStageBase._setDefaultValuesFromJava` iterates `extractParamMap().toList()` (Scala `List` through Py4J);
  2.13 `List` still has `toList`; verify Py4J conversion of 2.13 `Seq` (`ArraySeq`) in `H2OTypeConverters`.

### 3.8 `r`, `r-cran`
- `spark_dependencies` ships `sparkling_water_assembly.jar`; sparklyr >= 1.9.0 needed for Spark 4 / Scala 2.13.
  Jar naming with `_2.13` suffix; `r/build.gradle` test runner uses `SPARK_HOME`. `testthat` suite small (4 files).

### 3.9 Tests, CI, docs, distribution
- 76 suites `extends FunSuite` with `org.scalatest.junit.JUnitRunner` → ScalaTest 3.2.19 (`AnyFunSuite`,
  `org.scalatestplus.junit.JUnitRunner`, `scalatestplus-junit` dependency).
- `core/src/test/resources/log4j.properties` and the LDAP integ suites that ship it → `log4j2.properties`.
- `ci/sparklingWaterPipeline.groovy`, `Jenkinsfile*`, `ci/build.gradle`: Spark matrix from `supportedSparkVersions`
  (3.5, 4.1, 4.2, 4.3), Python matrix per Spark minor (3.10 only for 4.1/4.2), JDK 17 and 21 images, Hadoop 3 tarballs
  only, Databricks jobs on DBR 18 (4.1) and DBR 19 (4.2, JDK 21), EMR 8 templates in `templates/`.
- Cross-minor guard test: a small CI job that takes the 4.3 assembly and starts `H2OContext` on 4.1 and 4.2 must
  **fail** with the version-mismatch message (protects the policy in section 1), and the 4.1 assembly must start on 4.1.
- Docs: `doc/src/site/sphinx/{install,deployment,devel}` mention Spark 2.x/3.x, Java 8, Python 3.6; `migration_guide.rst`
  needs a 3.46→4.x chapter; `README.rst` badges reference `_2.12` artifacts.
- `dist/build.gradle` S3 layout `sparkling-water/spark-<major>/…` and PyPI name `h2o_pysparkling_<major>` keep working
  with `4.1`/`4.2`/`4.3`; `package`/`scoring-package` POMs get `_2.13` artifact ids.

## 4. Target architecture after renovation

1. **Single Scala version per Spark line**: 2.13 for Spark 4.x. Keep 2.12/3.5 buildable from the same tree only if the
   cost of dual-building is paid by CI; otherwise branch `rel-3.46` for 3.x and move `master` to 4.x only.
   Within 4.x, **one source tree, three property files, three artifacts** (see the minor-version policy in section 1).
2. **Drop Spark 2.x and 3.0–3.4 support** on `master` (ADR-002; no Databricks runtime ships 3.4). Delete `scala_spark_2.1/2.2/2.3`,
   `resources_spark_2.*`, `utils/scala_spark_3.0–3.4`, and the 2.x branches in Gradle. This removes most of the
   version-shim surface before the 4.x work starts.
3. **Minimize `org.apache.spark.*` packages**: keep only what has no public equivalent (UI tab/listener/store,
   internal-backend RPC). Replace UDF, encoder, `DefaultParams*`, `hiveClassesArePresent`, `getLocalDir` shims with
   public API.
4. **Servlet strategy**: jakarta for the Spark UI tab (the only servlet code left in Sparkling Water). No driver-side
   Jetty: the Flow proxy and its extension servlets are removed (ADR-003); a Flow replacement, if any, is a separate
   project outside this renovation.
5. **REPL**: removed together with Flow (ADR-004); the `repl` module and `scalainterpreter` servlets are deleted.
6. **Scoring is standalone**: `scoring`/`py-scoring` depend on `utils` only, work without `H2OContext`, and are the
   first artifacts verified on every target runtime.
7. **Serialization**: persisted Spark ML params must be language-version independent (JSON via
   `DefaultParamsWriter` paramMap, arrays, Java collections), never Java-serialized Scala collections.
8. **Runtime matrix** encoded once per Spark minor in `gradle-spark4.{1,2,3}.properties` and consumed by Gradle, CI,
   `setup.py`, Docker, docs (no duplicated version lists). Anything that is the same for all 4.x lives in `gradle.properties`.
9. **Test environment**: JDK 17 default, JDK 21 in the matrix if H2O-3 target supports it; ANSI mode on in all tests.

## 5. Phased plan

**Phase 0 – Decisions (ADRs, no code)**
- ADR-001: H2O-3 target line for Spark 4 (3.46.0.6 OSS frozen vs. H2O-3 Secure) and how JDK 21 (DBR 18/19) is
  handled: `sys.ai.h2o.debug.allowJavaVersions` set by Sparkling Water in the executor before `H2OStarter.start`,
  H2O-3 Secure, or external backend only.
- ADR-002: Support matrix for `master`: keep only 3.5; drop 2.x and 3.0–3.4 (no Databricks runtime ships 3.4, decided
  2026-11-10); 4.0, 4.1, 4.2, 4.3 as per-minor artifacts from one source tree, each added only when its build is green.
- ADR-003: Flow UI: remove the driver-side proxy and extension servlets from the Spark 4 line (replacement out of scope).
- ADR-004: Embedded Scala REPL: drop (only consumer is the Flow Scala cell).
- ADR-005: Azure Databricks dedicated (classic) compute is the supported mode; standard/serverless and Spark Connect are
  out of scope; AWS/GCP Databricks are best-effort.

**Phases 1–5** are maintained in `design/RENOVATION_PLAN.md` (narrative) and as `tk` tickets in `.tickets/`
(execution order). In short: shrink the surface and remove Flow/REPL on Spark 3.5/2.12 → Gradle 8/JDK 17/Scala 2.13 →
M1 on DBR 16.4 LTS → Spark 4.0 (`utils` → `scoring` → `py-scoring` first, then `core`/`ml`) → M2 on DBR 17.3 LTS →
Spark 4.1/4.2 + JDK 21 → M3 on DBR 18/19 → Spark 4.3, CI, docs, release.

## 6. Definition of done

- **MOJO deployment on Azure Databricks**: the `scoring` assembly jar and the `h2o_pysparkling_scoring` wheel install
  as cluster libraries on DBR 16.4 LTS (2.13), 17.3 LTS, 18 LTS and 19 (dedicated mode); an `H2OMOJOModel` saved by
  3.46.0.x loads, transforms a DataFrame and round-trips through Spark ML pipeline persistence, in Scala and Python.
- **Training on Azure Databricks**: the full assembly jar and the PySparkling wheel install as cluster libraries and the
  Databricks CI job (`ci/Jenkinsfile-databricks`, Scala + Python) passes on the same runtimes; `H2OContext` starts with
  the internal backend. AWS Databricks is best-effort.
- `./gradlew build check -Pspark=<v>` passes for `v` in 4.0, 4.1, 4.2, 4.3 on JDK 17 (and JDK 21 if ADR-001 includes it),
  internal and external backend, from the same commit with no per-minor source directories.
- `./gradlew :sparkling-water-py:testPython -Pspark=<v>` passes for Python 3.10–3.13 on 4.1/4.2 and 3.11–3.13 on 4.3.
- `./gradlew :sparkling-water-r:test -Pspark=<v>` passes with sparklyr 1.9.x for all three.
- Integration tests (`integTest`, `integTestPython`) pass on standalone and Kubernetes clusters for all three minors.
- The cross-minor guard test passes (4.3 artifact refuses 4.1/4.2; each artifact starts on its own minor).
- A model trained and saved with Sparkling Water 3.46.0.x on Spark 3.5 loads and scores on Spark 4.1, 4.2 and 4.3.
- No driver-side Jetty remains in Sparkling Water (`ProxyStarter`, `backend/api`, `repl` deleted) and no `javax.servlet`
  import remains in `org.apache.spark.h2o.ui`; the H2O-side `javax` servlets in `extensions` (`compileOnly servlet-api`)
  are the only javax code left. No reflection against Spark internals that has a public replacement.
- `design/REPO_INDEX.md`, `migration_guide.rst`, and the install docs describe the new matrix; ADR-001…005 merged.

## 7. Known risks

- **H2O-3 availability**: the public H2O-3 line is frozen at 3.46.0.6 for Sparkling Water and supports Java <= 17; Java 21
  platforms (Databricks Runtime 18 default, 19 only) need the documented-as-experimental `allowJavaVersions` override or
  an H2O-3 build that supports Java 21. This is the single biggest risk for the primary objective.
- **Databricks access modes**: Sparkling Water can only ever run on dedicated compute; Databricks steers users to
  standard/serverless. Document loudly; consider a long-term scoring-only path (MOJO scoring via UDF needs no RDD/RPC)
  that could work on standard compute.
- **Servlet classpath clash**: reduced to H2O-3's own Jetty 9 inside executors once the driver-side proxy is removed.
  The H2O-side `javax` contract stays as today (`extensions` compiles against `servlet-api` as `compileOnly`, the
  assembly keeps excluding it); only the Spark UI tab moves to jakarta. A duplicate-class check guards the assembly.
- **Flow removal** changes user-visible behaviour (`flowURL`, Flow notebook docs); must be announced in the migration guide.
- **Scala 2.13 serialization** breaking existing saved pipelines; mitigated by Phase 1 persistence audit.
- **ANSI mode** changing conversion results silently in user pipelines; needs explicit documentation and tests.
- **Spark Connect** adoption on Databricks/serverless makes Py4J-based PySparkling unusable there; out of scope but
  should be stated in docs.
- **Low repo activity** (8 commits in two years) means no incremental 3.x→4.x work exists upstream to reuse.
- **Minor-version drift**: future 4.x minors may change more private APIs than 4.1→4.3 did (`headerSparkPage` is the
  precedent); the policy of "one source tree per major" holds only while shims stay small. Re-evaluate per minor.

## 8. Sources

- Spark 4.3.0 docs overview (runtime matrix): https://spark.apache.org/docs/latest/
- Spark 4.3.0 release notes: https://spark.apache.org/releases/spark-release-4-3-0.html
- Spark 4.0.0 release notes: https://spark.apache.org/releases/spark-release-4-0-0.html
- Spark Core migration guide 4.0: https://spark.apache.org/docs/4.0.0/core-migration-guide.html
- Spark SQL migration guide 4.0: https://spark.apache.org/docs/4.0.0/sql-migration-guide.html
- Spark 4.1.0 / 4.2.0 docs overview: https://spark.apache.org/docs/4.1.0/ , https://spark.apache.org/docs/4.2.0/
- Spark root `pom.xml` per tag: https://raw.githubusercontent.com/apache/spark/v4.1.0/pom.xml ,
  https://raw.githubusercontent.com/apache/spark/v4.2.0/pom.xml , https://raw.githubusercontent.com/apache/spark/v4.3.0/pom.xml
- `UIUtils.headerSparkPage` at v4.1.0 (7 params), v4.2.0 and v4.3.0 (8 params); `Utils.getLocalDir`,
  `ExpressionEncoder`, `ReadWrite.scala`, `AppHistoryServerPlugin` checked at v4.1.0 and v4.3.0 (identical)
- Spark 4.3.0 sources checked: `core/.../ui/UIUtils.scala`, `core/.../status/AppHistoryServerPlugin.scala`,
  `sql/catalyst/.../encoders/ExpressionEncoder.scala`, `mllib/.../ml/util/ReadWrite.scala`,
  `sql/core/.../classic/SparkSession.scala`, `core/.../util/Utils.scala`, `repl/.../SparkILoop.scala`
- sparklyr NEWS: https://raw.githubusercontent.com/sparklyr/sparklyr/main/NEWS.md
- H2O-3 repository layout: https://github.com/h2oai/h2o-3
- H2O-3 Java support (3.46.0.12 docs): https://docs.h2o.ai/h2o/latest-stable/h2o-docs/welcome.html
- H2O.ai, "Changes coming to H2O-3 open source" (2026-08-12): https://h2o.ai/blog/2026/changes-coming-to-h2o-3-open-source/
- Databricks Runtime release notes: https://docs.databricks.com/aws/en/release-notes/runtime/16.4lts ,
  https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts , https://docs.databricks.com/aws/en/release-notes/runtime/18 ,
  https://docs.databricks.com/aws/en/release-notes/runtime/19
- Databricks access mode limitations: https://docs.databricks.com/aws/en/compute/access-mode-limitations
- Spark 4.0.0 docs overview and `pom.xml`: https://spark.apache.org/docs/4.0.0/ , https://raw.githubusercontent.com/apache/spark/v4.0.0/pom.xml
