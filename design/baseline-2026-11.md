# Baseline build and tests – 2026-11-10

State of the unmodified tree (`master` at `1fc226532`, Spark 3.5.0, Scala 2.12.18, H2O-3 3.46.0.6) before any
renovation change. Every later step must keep the JDK 11 results below at least as green.

## Environment

| Item | Value |
|---|---|
| OS | macOS 26.5, arm64 (Apple Silicon) |
| JDK | OpenJDK 11.0.32.1 (reference), OpenJDK 17.0.21 (target) |
| Gradle | 7.6 wrapper |
| Python | 3.11 via `-PpythonBinary=/opt/homebrew/bin/python3.11` (no `python` on PATH) |
| R | not installed |
| `SPARK_HOME` | first run: not set; rerun: `~/opt/spark-3.5.9-bin-hadoop3` (Scala 2.12; the build compiles against 3.5.0) |
| Driverless AI license | first run: none; rerun: `DRIVERLESS_AI_LICENSE_FILE` set |

Commands, run with `--continue` so one failing module does not hide another:

```bash
./gradlew build -x check --continue -PpythonBinary=/opt/homebrew/bin/python3.11
./gradlew --continue :sparkling-water-core:test :sparkling-water-utils:test :sparkling-water-ml:test \
  -PpythonBinary=/opt/homebrew/bin/python3.11
```

## Build

| JDK | Result | Duration | Failed tasks |
|---|---|---|---|
| 11 | all Scala, Java and Python modules compile and package | 42 s (warm cache) | `:sparkling-water-r:distR`, `:sparkling-water-r-cran:distR` (R not installed) |
| 17 | same | 74 s (after `clean`) | same two R tasks |

## Unit tests

| Module | JDK 11 passed / run | JDK 11 failed | JDK 17 passed / run | JDK 17 failed |
|---|---|---|---|---|
| core (25 suites) | 208 / 210 | 2 | 13 / 35 | 22 suites abort at start |
| utils (1 suite) | 12 / 12 | 0 | 0 / 1 | 1 suite aborts at start |
| ml (40 suites) | 314 / 322 (1 skipped) | 7 | 11 / 50 | 39 suites abort at start |
| Duration | 22.5 min | | 3.7 min | |

JDK 11 numbers include the 2026-11-11 rerun of the configuration and MOJO pipeline suites with `SPARK_HOME` and the
license set; both now pass. JDK 17 numbers are from the first run only.

## JDK 11 failures, by cause

None of the remaining 9 failures is a defect in code the renovation keeps. Two environment causes from the first run
are resolved by the rerun and kept in the table for reference.

| Cause | Tests | Suites | Kind |
|---|---|---|---|
| XGBoost not available: H2O-3 ships no native XGBoost for macOS arm64, so `/3/ModelBuilders/xgboost` returns 404 | 7 | `H2OSupervisedMOJOModelTestSuite`, `H2OTreeBasedSupervisedMOJOModelTestSuite`, `MOJOParameterTestSuite`, `H2OGridSearchTestSuite` | environment; still failing, verify on Linux x86_64 |
| Driverless AI MOJO runtime: "License could not be found!" | 17 | `H2OMOJOPipelineModelTestSuite` | environment; **passes (19/19) with a DAI license** |
| Local-cluster mode needs a Spark distribution: executor launch fails, `assembly/target/scala-2.12/jars does not exist` | 2 | `ConfigurationPropertiesTestSuite_HttpHeadersOnNode`, `..._SetNotifyLocalViaNodeExtraProperties` | environment; **pass with `SPARK_HOME` set** |
| Scala REPL: `RemoteClassLoaderError` in the executor class loader | 2 | `ScalaInterpreterServletTestSuite` | code removed by step 1.13; still failing on rerun |

## JDK 17 failures

Every suite that starts a `SparkContext` aborts in initialization with
`IllegalAccessError: class org.apache.spark.storage.StorageUtils$ cannot access class sun.nio.ch.DirectBuffer`.
Spark 3.5 on Java 17 needs `--add-opens` JVM flags; `spark-submit` adds them, but the Gradle-forked test JVMs
(`gradle/sparkTest.gradle`, `gradle/scala.gradle`) set none. Follow-up step 2.1a adds them. Only Spark-free tests run.

## Observations for later steps

- The project `version` property is `3.46.0.2-1-SNAPSHOT` while H2O-3 is 3.46.0.6, so local artifacts carry a
  misleading version (`sparkling-water-assembly_2.12-3.46.0.2-1-3.5-all.jar`). Step 5.10 (release) sets it.
- `H2OMOJOPipelineModelTestSuite` needs a Driverless AI license; step 1.1 must not make the scoring gate depend on it.
- XGBoost-dependent tests cannot pass on Apple Silicon; they need Linux x86_64 (CI) to verify.
- PySparkling, R and integration tests are outside this baseline.
