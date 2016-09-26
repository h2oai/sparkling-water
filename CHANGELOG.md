ChangeLog
=========

##v1.6.8 (2016-09-26)
  - Bug
    - [SW-197](https://0xdata.atlassian.net/browse/SW-197) - Fix all mentions of H2OContext(sc) to H2OContext.getOrCreate(sc) in pysparkling
    - [SW-201](https://0xdata.atlassian.net/browse/SW-201) - Methods in water.support classes should use  [T <: Frame] instead of H2OFrame
    - [SW-202](https://0xdata.atlassian.net/browse/SW-202) - Pipeline scripts are not tested!
    - [SW-205](https://0xdata.atlassian.net/browse/SW-205) - PySparkling tests launcher does not report error correctly
  - Improvement
    - [SW-163](https://0xdata.atlassian.net/browse/SW-163) - Upgrade H2O dependency to the latest Turing release
    - [SW-182](https://0xdata.atlassian.net/browse/SW-182) - RSparkling R package
    - [SW-204](https://0xdata.atlassian.net/browse/SW-204) - Distribute tests log4j logs to corresponding build directories
    - [SW-210](https://0xdata.atlassian.net/browse/SW-210) - Log H2O client arguments before launching the client 
    - [SWPRIVATE-18](https://0xdata.atlassian.net/browse/SWPRIVATE-18) - SVM progress bar should notify the user about changes
  - Breaking API changes
    - The enum `hex.Distribution.Family` is now `hex.genmodel.utils.DistributionFamily`

##v1.6.7 (2016-09-01)
  - Bug
    - [SW-71](https://0xdata.atlassian.net/browse/SW-71) - Expose the method `H2OContext#setLogLevel` to setup log level of H2O
    - [SW-128](https://0xdata.atlassian.net/browse/SW-128) - Publish flows pack in GitHub repo and embed them in distribution JAR
    - [SW-153](https://0xdata.atlassian.net/browse/SW-153) - ModelMetricsSupport is duplicated in code, removed
    - [SW-168](https://0xdata.atlassian.net/browse/SW-168) - Solved slow-down for fat-dataset with many categorical columns
    - [SW-176](https://0xdata.atlassian.net/browse/SW-176) - H2O context is failing on CDH-5.7.1 with Spark Version 1.6.0-CDH.5.7.1
    - [SW-185](https://0xdata.atlassian.net/browse/SW-185) - Methods on frame can't be called in compute method on external cluster 
    - [SW-186](https://0xdata.atlassian.net/browse/SW-186) - Hide checks whether incoming data is NA into convertorCtx
    - [SW-191](https://0xdata.atlassian.net/browse/SW-191) - Better exception message in case dataframe with the desired key already exist when saving using datasource api
    - [SW-192](https://0xdata.atlassian.net/browse/SW-192) - Add `org.apache.spark.sql._` to packages imported by default in REPL

  - Improvement
    - [SW-158](https://0xdata.atlassian.net/browse/SW-158) - Support Spark DataSet in the same way as RDD and DataFrame
    - [SW-163](https://0xdata.atlassian.net/browse/SW-163) - Upgrade H2O dependency to the latest Turing release 3.10.0.6
    - [SW-165](https://0xdata.atlassian.net/browse/SW-165) - Change default schema for Scala code to black one.
    - [SW-170](https://0xdata.atlassian.net/browse/SW-170) - Unify H2OFrame datasource and asDataFrame API
    - [SW-171](https://0xdata.atlassian.net/browse/SW-171) - Internal API refactoring to allow multiple backends
    - [SW-172](https://0xdata.atlassian.net/browse/SW-172) - NodeDesc should be interned or use H2OKey instead of NodeDesc
    - [SW-174](https://0xdata.atlassian.net/browse/SW-174) - Remove unused fields from H2ORDD
    - [SW-177](https://0xdata.atlassian.net/browse/SW-177) - Refactor & Simplify REPL

##v1.6.6 (2016-08-03)
  - Bug
    - [SW-154](https://0xdata.atlassian.net/browse/SW-154) - Network mask is used in the wrong way on executors
    - [SW-156](https://0xdata.atlassian.net/browse/SW-156) - Option to control console verbosity mode
    - [SW-157](https://0xdata.atlassian.net/browse/SW-157) - Include new S3 persist layer in Sparkling Water assembly

  - Improvement
    - [SW-163](https://0xdata.atlassian.net/browse/SW-163) - Upgrade H2O dependency to 3.10.0.4
    - Streaming example

##v1.6.5 (2016-06-06)
  - Hotfix
    - Fix of REPL initialization

##v1.6.4 (2016-06-02)
  - Bug fixes
    - [SW-125](https://0xdata.atlassian.net/browse/SW-125) - PySparkling changes in order to upload the package to PyPI
    - [SW-130](https://0xdata.atlassian.net/browse/SW-130) - Launch spark cloud script is obsolete 
    - [SW-132](https://0xdata.atlassian.net/browse/SW-132) - Hotfix for missing REPL field if running on top of CDH5.7
    - [SW-137](https://0xdata.atlassian.net/browse/SW-137) - Fix classloading of sparkling water jar in pysparkling in Databricks 
    - [SW-141](https://0xdata.atlassian.net/browse/SW-141) - Timestamp in hierarchical Spark structure is incorrectly transformed into H2OFrame
    - [SW-149](https://0xdata.atlassian.net/browse/SW-149) - Use H2OContext.get() method where H2O Context is expected to be running
  
  - Improvements
    - [SW-127](https://0xdata.atlassian.net/browse/SW-127) - Add Kerberos authentication to Flow
    - [SW-131](https://0xdata.atlassian.net/browse/SW-131) - Refactor DemoUtils into multiple Support traits and remove it
    - [SW-134](https://0xdata.atlassian.net/browse/SW-134) - Prototype implementation of ML pipelines with H2O algorithms (motivation example HamOrSpam)
    - [SW-135](https://0xdata.atlassian.net/browse/SW-135) - Integrate GBM into Spark ML pipelines
    - [SW-136](https://0xdata.atlassian.net/browse/SW-136) - Replace strictly type API using H2OFrame by `T <: Frame`
    - [SW-138](https://0xdata.atlassian.net/browse/SW-138) - Upgrade H2O version to the latest Turchin version
    - [SW-142](https://0xdata.atlassian.net/browse/SW-142) - Report unsupported parameters
    - [SW-143](https://0xdata.atlassian.net/browse/SW-143) - Travis build now performs junit testing
    - [SW-144](https://0xdata.atlassian.net/browse/SW-144) - Enable Junit testing under Travis
    - [SW-146](https://0xdata.atlassian.net/browse/SW-146) - How to run Sparkling Water on top of Zeppelin
    - [SW-148](https://0xdata.atlassian.net/browse/SW-148) - Documentation for pySparkling on top of Databricks Cloud
    - [SW-150](https://0xdata.atlassian.net/browse/SW-150) - Introduce Scala API for Frame join operations

##v1.6.3 (2016-04-27)
  - Bug fixes
    - [SW-124](https://0xdata.atlassian.net/browse/SW-124)  - Hotfix to avoid NPE in MetadataHandler caused by PUBDEV-2879

##v1.6.2 (2016-04-25)
  - Bug fixes
    - [SW-12](https://0xdata.atlassian.net/browse/SW-12) - Provides an inner class encapsulating implicit conversion like SQLContext does
    - [SW-50](https://0xdata.atlassian.net/browse/SW-50) - Reject attempts to run Sparkling Water/pySparkling with wrong version of Spark
    - [SW-80](https://0xdata.atlassian.net/browse/SW-80) - Add all pySparkling dependencies to pySparkling egg file
    - [SW-104](https://0xdata.atlassian.net/browse/SW-104) - pysparkling overrides extraClassPath configuration
    - [SW-105](https://0xdata.atlassian.net/browse/SW-105) - Have an option to disable listener for changes of Spark cluster topology
    - [SW-107](https://0xdata.atlassian.net/browse/SW-107) - Initiation of H2OContext in pySparkling ends up with "Calling a package" py4j exception
    - [SW-109](https://0xdata.atlassian.net/browse/SW-109) - Make PySparkling up-to-date with current H2O and SW and major pySparkling bug fixing and refactoring
    - [SW-110](https://0xdata.atlassian.net/browse/SW-110) - Wrong Spark location in travis build
    - [SW-116](https://0xdata.atlassian.net/browse/SW-116) - Sparkling Water examples/README needs update
    - [SW-119](https://0xdata.atlassian.net/browse/SW-119) - Return correct executor hostname during spreadRDD phase
  - Improvements
    - [SW-37](https://0xdata.atlassian.net/browse/SW-37) - During test run warn/fail if the `SPARK_HOME` version is different from spark version used during build
    - [SW-85](https://0xdata.atlassian.net/browse/SW-85) - Follow Spark way in using implicits
    - [SW-98](https://0xdata.atlassian.net/browse/SW-98) - Upgrade H2O dependency to 3.8.1.4
    - [SW-99](https://0xdata.atlassian.net/browse/SW-99) - Allow disabling/enabling REPL using configuration property
    - [SW-102](https://0xdata.atlassian.net/browse/SW-102) - Speedup tests by disabling REPL in tests which don't require it
    - [SW-115](https://0xdata.atlassian.net/browse/SW-115) - Increase H2O dependency to rel-turchin
    - [SW-120](https://0xdata.atlassian.net/browse/SW-120) - Remove deprecated VecUtils

##v1.6.1 (2016-03-15)
  - Bug fixes
    - Fix idea setup script
    - Fix cloud name - make it unique
    - Fix bug in launching scripts which were overriding default Spark settings provide by use in `cond/spark-defaults.conf`
    - [PUBDEV-282](https://0xdata.atlassian.net/browse/PUBDEV-282) Create windows batch scripts for starting sparkling-shell and running examples
    - [SW-4](https://0xdata.atlassian.net/browse/SW-4) - InvokeOnNodesRDD locality fix
    - [SW-5, SW-17, SW-25](https://0xdata.atlassian.net/browse/SW-25) Remove categorical handling during asH2OFrame() transformation
    - [SW-10](https://0xdata.atlassian.net/browse/SW-10) - Use new Spark 1.5 RpcEnv to obtain executor IPs
    - [SW-16](https://0xdata.atlassian.net/browse/SW-16) - Update docker file based on current version
    - [SW-20](https://0xdata.atlassian.net/browse/SW-20) H2OFrame provides nicer API accepting parser setup
    - [SW-32](https://0xdata.atlassian.net/browse/SW-32) Update documentation and remove top-level images folder
    - [SW-33](https://0xdata.atlassian.net/browse/SW-33) Remove usage of deprecated VecUtils class
    - [SW-38](https://0xdata.atlassian.net/browse/SW-38)  Introduces Sparkling Water parameter to setup location of H2O logs
    - [SW-39](https://0xdata.atlassian.net/browse/SW-39)  PySparkling: Support of Sparkling Water from PySpark
    - [SW-40](https://0xdata.atlassian.net/browse/SW-40)  PySparkling: as\_h2o\_frame method accepts name of target H2O Frame
    - [SW-41](https://0xdata.atlassian.net/browse/SW-41) H2OContext#asH2OFrame now
    - [SW-43](https://0xdata.atlassian.net/browse/SW-43) - Fix script tests
    - [SW-45](https://0xdata.atlassian.net/browse/SW-45) - Fix interpreter initialization
    - [SW-47](https://0xdata.atlassian.net/browse/SW-47) - Server test.h2o.ai: Enable python tests for post-push tests and relese 1.5 branch
    - [SW-48](https://0xdata.atlassian.net/browse/SW-48) - Fix H2O jetty webport to listen on 0.0.0.0 not on given ip
    - [SW-61](https://0xdata.atlassian.net/browse/SW-61) - Remove `--driver-classpath` parameter from sparkling-shell
    - [SW-65](https://0xdata.atlassian.net/browse/SW-65) - Add pysparkling instruction to download page
    - [SW-68](https://0xdata.atlassian.net/browse/SW-68) - AskCraig list demo always returns accounting category
    - [SW-69](https://0xdata.atlassian.net/browse/SW-69) - Flow: getRDDs does not show id
    - [SW-70](https://0xdata.atlassian.net/browse/SW-70) - Support for Spark `LabeledPoint` in `RDD[T]`
    - [SW-94](https://0xdata.atlassian.net/browse/SW-94) - Fix Maven dependency between projects
    - [SW-97](https://0xdata.atlassian.net/browse/SW-97) - Spark 1.6 support
  - Improvements
    - Attach metadata derived from H2OFrame to Spark DataFrame
    - Improved logging subsystem
    - Model serialization support
    - Expose new REST end-points
      - to interpret Scala code
      - to perform transformation between Spark DataFrame and H2O Frame
    - Fix all scripts and create automatic tests for them
    - [SW-39](https://0xdata.atlassian.net/browse/SW-39) - pySparkling: use Sparkling Water from Python
    - [SW-27](https://0xdata.atlassian.net/browse/SW-27) - Support Spark SQL data sources
    - [SW-63](https://0xdata.atlassian.net/browse/SW-63) - Repl separation into a dedicated sparkling-water-repl module
    - [SW-66](https://0xdata.atlassian.net/browse/SW-66) - Warn if neither one of `H2O_HOME` or `H2O_PYTHON_WHEEL` properties is not set
    - [SW-73](https://0xdata.atlassian.net/browse/SW-73) - List all available branches in README.md
    - [SW-75](https://0xdata.atlassian.net/browse/SW-75) - RDDHandler should expose REST api for transformation from RDD to H2OFrame
    - [SW-76](https://0xdata.atlassian.net/browse/SW-76) - Upgrade H2O version to Tukey release (3.8.0.3)
    - [SW-78](https://0xdata.atlassian.net/browse/SW-78) - Sparking-shell: Change default spark master to `local[*]`
    - [SW-91](https://0xdata.atlassian.net/browse/SW-91) - Update Sparkling Water tuning documentation
    - [SW-92](https://0xdata.atlassian.net/browse/SW-92) - Update development doc with information how to submit app on yarn
    - [SW-93](https://0xdata.atlassian.net/browse/SW-78) - Upgrade H2O dependency to Turan release (3.8.1.1)


##v1.4.0 (2015-07-06)
  - Support of primitives type in transformation from RDD to H2OFrame
  - Support of Spark 1.4
  - New applications
    - Craigslist job predictions
    - Streaming craigslist demo
  - use H2O version 3.0.0.26 (algorithms weights, offsets, fixes)
  - API improvements
  - follow Spark way to provide implicit conversions

##v1.3.0 (2015-05-25)
  - Major release of Sparkling Water
  - Depends on:
    - Spark 1.3.1
    - H2O 3.0 Shannon release
  - It contains major renaming of API: 
    - H2O's DataFrame was renamed to H2OFrame
    - Spark's SchemaRDD was renamed to DataFrame

##v1.2.0 (2015-05-18)
  - Major release of Sparkling Water
  - Depends on:
    - Spark 1.2.0
    - H2O 3.0 Shannon release

##v0.2.14 (2015-05-14)
  - Upgrade h2o dependency to build 1205 including fixes in algos, infrastructure,
    and improvements in UI
  - Examples changed to support modified h2o API
  - Updated documentation
    - list of demos and applications
    - list of scripts for Sparkling Shell
    - list of meetups with links to code and instructions
  - Fix a limit on number of columns in SchemaRDD (thanks @nfergu)

##v0.2.13 (2015-05-01)
  - Upgrade h2o dependency to build 1165
  - Introduce type alias DataFrame pointing to `water.fvec.H2OFrame`
  - Change naming of implicit operations `toDataFrame` to `toH2OFrame`
  - Chicago crime shell script 

##v0.2.12 (2015-04-21)
  - Upgraded H2O dev to 1109 build.
  - Applications 
    - Chicago crime application 
    - Ham or Spam application
    - MLConf 2015 demo
  - More unit testing for transformation between RDD and DataFrame
  - Fix in handling string columns.
  - Removed used of ExistingRdd which was deprecated in Spark 1.2.0
  - Added joda-convert library into resulting assembly
  - Parquet import test.
  - Prototype of Sparkling Water ML pipelines
  - Added quiet mode for h2o client.
  - Devel Documentation
  - Fixes
    - [PUBDEV-771] Fix handling of UUIDs.
    - [PUBDEV-767] Missing LongType handling.
    - [PUBDEV-766] Fix wrong test.
    - [PUBDEV-625] MLConf demo is now integration test.
    - [PUBDEV-457] Array of strings is represented as set of String columns in H2O.
    - [PUBDEV-457] Test scenario mentioned in the test.
    - [PUBDEV-457] Support for hierarchical schemas including vectors and arrays
    - [PUBDEV-483] Introduce option to setup client web port.
    - [PUBDEV-357] Change of clouding strategy - now cloud members report themselves to a driver

