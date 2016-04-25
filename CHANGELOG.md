ChangeLog
=========

#v1.4.13 (2016-04-25)
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

#v1.4.12 (2016-03-18)
  - Improvements
    - [SW-98](https://0xdata.atlassian.net/browse/SW-98) - Upgrade H2O dependency to version including security support

#v1.4.11 (2016-03-17)
  - Bug fixes
    - [SW-4](https://0xdata.atlassian.net/browse/SW-4) - InvokeOnNodesRDD task locality fixed
    - [SW-16](https://0xdata.atlassian.net/browse/SW-16) - Update docker file based on current version
    - [SW-61](https://0xdata.atlassian.net/browse/SW-61) - Remove `--driver-classpath` parameter from sparkling-shell
    - [SW-68](https://0xdata.atlassian.net/browse/SW-68) - AskCraig list demo always returns accounting category
    - [SW-69](https://0xdata.atlassian.net/browse/SW-69) - Flow: getRDDs does not show id
    - [SW-70](https://0xdata.atlassian.net/browse/SW-70) - Support for Spark `LabeledPoint` in `RDD[T]` 
    - [SW-94](https://0xdata.atlassian.net/browse/SW-94) - Fix Maven dependency between projects
  - Improvements
    - [SW-63](https://0xdata.atlassian.net/browse/SW-63) - Repl separation into a dedicated sparkling-water-repl module
    - [SW-66](https://0xdata.atlassian.net/browse/SW-66) - Warn if neither one of `H2O_HOME` or `H2O_PYTHON_WHEEL` properties is not set
    - [SW-73](https://0xdata.atlassian.net/browse/SW-73) - List all available branches in README.md
    - [SW-74](https://0xdata.atlassian.net/browse/SW-74) - Ensure we are consistent with supported types for conversion.
    - [SW-75](https://0xdata.atlassian.net/browse/SW-75) - RDDHandler should expose REST api for transformation from RDD to H2OFrame
    - [SW-76](https://0xdata.atlassian.net/browse/SW-76) - Upgrade H2O version to Tukey release (3.8.0.3)
    - [SW-77](https://0xdata.atlassian.net/browse/SW-77) - pySparkling integration tests
    - [SW-78](https://0xdata.atlassian.net/browse/SW-78) - Sparking-shell: Change default spark master to `local[*]`
    - [SW-82](https://0xdata.atlassian.net/browse/SW-82) - Rest API enhancement and cleaning
    - [SW-83](https://0xdata.atlassian.net/browse/SW-83) - Increase scala interpreter timeout & make corresponding values configurable 
    - [SW-84](https://0xdata.atlassian.net/browse/SW-84) - Make local tests consistent and move tests running on cluster to integration tests
    - [SW-86](https://0xdata.atlassian.net/browse/SW-86) - Integration tests for sparkling-water-core
    - [SW-87](https://0xdata.atlassian.net/browse/SW-87) - Try to help H2O clouding process with providing more information about Spark environment
    - [SW-90](https://0xdata.atlassian.net/browse/SW-90) - Remove scala interpreter timeout so the interpreters are bound to H2O lifecycle
    - [SW-91](https://0xdata.atlassian.net/browse/SW-91) - Update Sparkling Water tuning documentation
    - [SW-92](https://0xdata.atlassian.net/browse/SW-92) - Update development doc with information how to submit app on yarn
    - [SW-93](https://0xdata.atlassian.net/browse/SW-93) - Upgrade H2O dependency to Turan release (3.8.1.1)

#v1.4.10 (2016-01-13)
  - [SW-51](https://0xdata.atlassian.net/browse/SW-51) Bundle pySparkling with release and enable its testing on test machines
  - [SW-60](https://0xdata.atlassian.net/browse/SW-60) Move ModelUtils to sparkling-water-core module

#v1.4.9 (2015-12-18)
  - Upgrade of H2O dependency to 3.6.0.8
  - Fixes of typos in code
  - Fix a bug in launch script overriding default value of spark.driver.extraJavaOptions

#v1.4.8 (2015-11-24)
  - Attach metadata derived from H2OFrame to Spark DataFrame
  - Improved logging subsystem
  - Expose new REST end-points
    - to interpret Scala code
    - to perform transformation between Spark DataFrame and H2O Frame
  - Fix all scripts and create automatic tests for them
  - Upgrade H2O dependency to rel-tibshirani v1.6.0.3
  - Upgrade Spark dependency to 1.4.1
  - Added support for buildinfo.json
  - Fixes
    - [PUBDEV-282](https://0xdata.atlassian.net/browse/PUBDEV-282) Create windows batch scripts for starting sparkling-shell and running examples
    - [SW-5, SW-17, SW-25](https://0xdata.atlassian.net/browse/SW-25) Remove categorical handling during asH2OFrame() transformation
    - [SW-20](https://0xdata.atlassian.net/browse/SW-20) H2OFrame provides nicer API accepting parser setup
    - [SW-32](https://0xdata.atlassian.net/browse/SW-32) Update documentation and remove top-level images folder
    - [SW-33](https://0xdata.atlassian.net/browse/SW-33) Remove usage of deprecated VecUtils class
    - [SW-38](https://0xdata.atlassian.net/browse/SW-38)  Introduces Sparkling Water parameter to setup location of H2O logs
    - [SW-39](https://0xdata.atlassian.net/browse/SW-39)  PySparkling: Support of Sparkling Water from PySpark
    - [SW-40](https://0xdata.atlassian.net/browse/SW-40)  PySparkling: as\_h2o\_frame method accepts name of target H2O Frame
    - [SW-41](https://0xdata.atlassian.net/browse/SW-41) H2OContext#asH2OFrame now
      accepts name for resulting H2OFrame.
 
##v1.4.7 (2015-10-20)
  - [SW-25] - remove handling of categorical on Spark side during asH2OFrame
    transformation
  - usa Gradle 2.7

##v1.4.6 (2015-09-28)
  - Fix assembly error

##v1.4.5 (2015-09-28)
   - H2O version 3.2.0.5 (slater)

##v1.4.4 (2015-09-23)
   - Attach metadata derived from H2OFrame to Spark DataFrame
   - Improved logging subsystem
   - "Ask Craig" streaming example
   - minor fixes in API
   - minor fixes in documentation 
   - use H2O version 3.2.0.3 (slater)


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

