Change Log
==========

v2.2.3 (2017-11-25)
-------------------

-  Bug

   -  `SW-320 <https://0xdata.atlassian.net/browse/SW-320>`__ - H2OConfTest Python test blocks test run
   -  `SW-499 <https://0xdata.atlassian.net/browse/SW-499>`__ - BinaryType handling is not implemented in SparkDataFrameConverter
   -  `SW-535 <https://0xdata.atlassian.net/browse/SW-535>`__ - asH2OFrame gives error if column names have DOT in it
   -  `SW-547 <https://0xdata.atlassian.net/browse/SW-547>`__ - Don't use md5skip in external mode
   -  `SW-569 <https://0xdata.atlassian.net/browse/SW-569>`__ - pysparkling: h2o on exit does not shut down cleanly
   -  `SW-572 <https://0xdata.atlassian.net/browse/SW-572>`__ - Additional fix for [SW-571]
   -  `SW-573 <https://0xdata.atlassian.net/browse/SW-573>`__ - Minor Gradle build improvements and fixes
   -  `SW-575 <https://0xdata.atlassian.net/browse/SW-575>`__ - Incorrect comment in hamOrSpamMojo pipeline
   -  `SW-576 <https://0xdata.atlassian.net/browse/SW-576>`__ - Cleanup pysparkling test infrastructure
   -  `SW-577 <https://0xdata.atlassian.net/browse/SW-577>`__ - Fix conditions in jenkins file
   -  `SW-580 <https://0xdata.atlassian.net/browse/SW-580>`__ - Fix composite build in Jenkins
   -  `SW-581 <https://0xdata.atlassian.net/browse/SW-581>`__ - Fix H2OConf test on external cluster
   -  `SW-582 <https://0xdata.atlassian.net/browse/SW-582>`__ - Opening Chicago Crime Demo Notebook errors on the first opening
   -  `SW-584 <https://0xdata.atlassian.net/browse/SW-584>`__ - Create extended directory automatically
   -  `SW-588 <https://0xdata.atlassian.net/browse/SW-588>`__ - Fix links in README
   -  `SW-589 <https://0xdata.atlassian.net/browse/SW-589>`__ - Wrap stages in try finally in jenkins file
   -  `SW-592 <https://0xdata.atlassian.net/browse/SW-592>`__ - Properly pass all parameters to algorithm
   -  `SW-593 <https://0xdata.atlassian.net/browse/SW-593>`__ - H2Conf cannot be initialized on windows
   -  `SW-594 <https://0xdata.atlassian.net/browse/SW-594>`__ - Gradle ml submodule reports success even though tests fail
   -  `SW-595 <https://0xdata.atlassian.net/browse/SW-595>`__ - Fix ML tests

-  New Feature

   -  `SW-519 <https://0xdata.atlassian.net/browse/SW-519>`__ - Introduce SW Models into Spark python pipelines

-  Task

   -  `SW-609 <https://0xdata.atlassian.net/browse/SW-609>`__ - Upgrade H2O dependency to 3.16.0.1


-  Improvement

   -  `SW-318 <https://0xdata.atlassian.net/browse/SW-318>`__ - Keep H2O version inside sparklin-water-core.jar and provide utility to query it
   -  `SW-420 <https://0xdata.atlassian.net/browse/SW-420>`__ - Shell scripts miss-leading error message
   -  `SW-504 <https://0xdata.atlassian.net/browse/SW-504>`__ - Provides Sparkling Water Spark Uber package which can be used in `--packages`
   -  `SW-570 <https://0xdata.atlassian.net/browse/SW-570>`__ - Stop previous jobs in jenkins in case of PR
   -  `SW-571 <https://0xdata.atlassian.net/browse/SW-571>`__ - In PySparkling, getOrCreate(spark) still incorrectly complains that we should use spark session
   -  `SW-583 <https://0xdata.atlassian.net/browse/SW-583>`__ - Upgrade to Gradle 4.3
   -  `SW-585 <https://0xdata.atlassian.net/browse/SW-585>`__ - Add the custom commit status for internal and external pipelines
   -  `SW-586 <https://0xdata.atlassian.net/browse/SW-586>`__ - [ML] Remove some duplicities, enable mojo for deep learning
   -  `SW-590 <https://0xdata.atlassian.net/browse/SW-590>`__ - Replace deprecated method call in ChicagoCrime python example
   -  `SW-591 <https://0xdata.atlassian.net/browse/SW-591>`__ - Repl doesn't require H2O dependencies to compile
   -  `SW-596 <https://0xdata.atlassian.net/browse/SW-596>`__ - Minor build improvements
   -  `SW-603 <https://0xdata.atlassian.net/browse/SW-603>`__ - Upgrade Gradle to 4.3.1
   -  `SW-605 <https://0xdata.atlassian.net/browse/SW-605>`__ - addFiles doesn't accept sparkSession
   -  `SW-610 <https://0xdata.atlassian.net/browse/SW-610>`__ - Change default client mode to INFO, let user to change it at runtime

v2.2.2 (2017-10-23)
-------------------

-  Bug

   -  `SW-555 <https://0xdata.atlassian.net/browse/SW-555>`__ - Fix documentation issue in PySparkling
   -  `SW-558 <https://0xdata.atlassian.net/browse/SW-558>`__ - Increase default value for client connection retry timeout in
   -  `SW-560 <https://0xdata.atlassian.net/browse/SW-560>`__ - SW documentation for nthreads is inconsistent with code
   -  `SW-561 <https://0xdata.atlassian.net/browse/SW-561>`__ - Fix reporting artefacts in Jenkins and remove use of h2o-3-shared-lib
   -  `SW-564 <https://0xdata.atlassian.net/browse/SW-564>`__ - Clean test workspace in jenkins
   -  `SW-565 <https://0xdata.atlassian.net/browse/SW-565>`__ - Fix creation of extended jar in jenkins
   -  `SW-567 <https://0xdata.atlassian.net/browse/SW-567>`__ - Fix failing tests on external backend
   -  `SW-568 <https://0xdata.atlassian.net/browse/SW-568>`__ - Remove obsolete and failing idea configuration
   -  `SW-559 <https://0xdata.atlassian.net/browse/SW-559>`__ - GLM fails to build model when weights are specified

-  Improvement

   -  `SW-557 <https://0xdata.atlassian.net/browse/SW-557>`__ - Create 2 jenkins files ( for internal and external backend ) backed by configurable pipeline
   -  `SW-562 <https://0xdata.atlassian.net/browse/SW-562>`__ - Disable web on external H2O nodes in external cluster mode
   -  `SW-563 <https://0xdata.atlassian.net/browse/SW-563>`__ - In external cluster mode, print also YARN job ID of the external cluster once context is available
   -  `SW-566 <https://0xdata.atlassian.net/browse/SW-566>`__ - Upgrade H2O to 3.14.0.7
   -  `SW-553 <https://0xdata.atlassian.net/browse/SW-553>`__ - Improve handling of sparse vectors in internal cluster


v2.2.1 (2017-10-10)
-------------------

-  Bug

   -  `SW-423 <https://0xdata.atlassian.net/browse/SW-423>`__ - Tests of External Cluster mode fails
   -  `SW-516 <https://0xdata.atlassian.net/browse/SW-516>`__ - External cluster improperly convert RDD[ml.linalg.Vector]
   -  `SW-524 <https://0xdata.atlassian.net/browse/SW-524>`__ - Ensure Jenkins uses Java 8 for Sparkling Water 2.2.x
   -  `SW-525 <https://0xdata.atlassian.net/browse/SW-525>`__ - Don't use GPU nodes for sparkling water testing in Jenkins
   -  `SW-526 <https://0xdata.atlassian.net/browse/SW-526>`__ - Add missing when clause to scripts test stage in Jenkinsfile
   -  `SW-527 <https://0xdata.atlassian.net/browse/SW-527>`__ - Use dX cluster for Jenkins testing
   -  `SW-529 <https://0xdata.atlassian.net/browse/SW-529>`__ - Code defect in Scala example
   -  `SW-531 <https://0xdata.atlassian.net/browse/SW-531>`__ - Use code which is compatible between Scala 2.10 and 2.11
   -  `SW-532 <https://0xdata.atlassian.net/browse/SW-532>`__ - Make auto mode in external cluster default for tests in jenkins
   -  `SW-534 <https://0xdata.atlassian.net/browse/SW-534>`__ - Ensure that all tests run on both, internal and external backends
   -  `SW-536 <https://0xdata.atlassian.net/browse/SW-536>`__ - Allow to test sparkling water against specific h2o branch
   -  `SW-537 <https://0xdata.atlassian.net/browse/SW-537>`__ - Update Gradle to 4.2RC2
   -  `SW-538 <https://0xdata.atlassian.net/browse/SW-538>`__ - Fix problem in Jenkinsfile where H2O_HOME has higher priority then H2O_PYTHON_WHEEL
   -  `SW-539 <https://0xdata.atlassian.net/browse/SW-539>`__ - Fix PySparkling issue when running multiple times on the same node
   -  `SW-541 <https://0xdata.atlassian.net/browse/SW-541>`__ - Model training hangs in SW
   -  `SW-542 <https://0xdata.atlassian.net/browse/SW-542>`__ - Sparkling Water does not support parquet import
   -  `SW-552 <https://0xdata.atlassian.net/browse/SW-552>`__ - Fix documentation bug

-  New Feature

   -  `SW-521 <https://0xdata.atlassian.net/browse/SW-521>`__ - Fix typo in documentation
   -  `SW-523 <https://0xdata.atlassian.net/browse/SW-523>`__ - Use linux label to determine which nodes are used for Jenkins testing
   -  `SW-533 <https://0xdata.atlassian.net/browse/SW-533>`__ - In external cluster, remove notification file at the end. This affects nothing, it is just cleanup.

-  Improvement

   -  `SW-543 <https://0xdata.atlassian.net/browse/SW-543>`__ - Upgrade Gradle to 4.2
   -  `SW-544 <https://0xdata.atlassian.net/browse/SW-544>`__ - Improve exception in ExternalH2OBackend
   -  `SW-545 <https://0xdata.atlassian.net/browse/SW-545>`__ - Stop H2O in afterAll in tests
   -  `SW-546 <https://0xdata.atlassian.net/browse/SW-546>`__ - Add sw version to name of h2odriver obtained using get-extended-h2o script
   -  `SW-549 <https://0xdata.atlassian.net/browse/SW-549>`__ - Upgrade gradle to 4.2.1
   -  `SW-550 <https://0xdata.atlassian.net/browse/SW-550>`__ - Upgrade H2O to 3.14.0.6


v2.2.0 (2017-08-23)
-------------------

-  Bug

   -  `SW-449 <https://0xdata.atlassian.net/browse/SW-449>`__ - Support Sparse Data during spark-h2o conversions
   -  `SW-510 <https://0xdata.atlassian.net/browse/SW-510>`__ - the link `Demo Example from Git` is broken on the download page

-  New Feature

   -  `SW-481 <https://0xdata.atlassian.net/browse/SW-481>`__ - MOJO for Spark SVM
   -  `SW-497 <https://0xdata.atlassian.net/browse/SW-497>`__ - Integration with Spark 2.2

-  Improvement

   -  `SW-395 <https://0xdata.atlassian.net/browse/SW-395>`__ - bin/sparkling-shell should fail if assembly `jar` file does not exist
   -  `SW-471 <https://0xdata.atlassian.net/browse/SW-471>`__ - Use mojo in pipelines if possible, remove H2OPipeline and OneTimeTransformers
   -  `SW-512 <https://0xdata.atlassian.net/browse/SW-512>`__ - Make JenkinsFile up-to-date with sparkling_yarn_branch
   -  `SW-513 <https://0xdata.atlassian.net/browse/SW-513>`__ - Upgrade to Gradle 4.1
   -  `SW-514 <https://0xdata.atlassian.net/browse/SW-514>`__ - Upgrade H2O to 3.14.0.2


v2.1.x (2017-03-02)
-------------------

-  Sparkling Water 2.1 brings support of Spark 2.1.
-  For detailed changelog, please read `rel-2.1/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.1/doc/CHANGELOG.rst>`__.

v2.0.x (2016-09-26)
-------------------

-  Sparkling Water 2.0 brings support of Spark 2.0.
-  For detailed changelog, please read `rel-2.0/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.0/doc/CHANGELOG.rst>`__.

v1.6.x (2016-03-15)
-------------------

-  Sparkling Water 1.6 brings support of Spark 1.6.
-  For detailed changelog, please read `rel-1.6/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.6/CHANGELOG.md>`__.

v1.5.x (2015-09-28)
-------------------

-  Sparkling Water 1.5 brings support of Spark 1.5.
-  For detailed changelog, please read `rel-1.5/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.5/CHANGELOG.md>`__.

v1.4.x (2015-07-06)
-------------------

-  Sparkling Water 1.4 brings support of Spark 1.4.
-  For detailed changelog, please read `rel-1.4/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.4/CHANGELOG.md>`__.

v1.3.x (2015-05-25)
-------------------

-  Sparkling Water 1.3 brings support of Spark 1.3.
-  For detailed changelog, please read `rel-1.3/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.3/CHANGELOG.md>`__.

v1.2.x (2015-05-18) and older
-----------------------------

-  Sparkling Water 1.2 brings support of Spark 1.2.
-  For detailed changelog, please read `rel-1.2/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-1.2/CHANGELOG.md>`__.
