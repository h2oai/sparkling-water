Change Log
==========

v2.0.12 (2017-07-12)
--------------------

-  Bug

   -  `SW-407 <https://0xdata.atlassian.net/browse/SW-407>`__ - Make scala H2OConf consistent and allow to set and get all propertties

-  Improvement

   -  `SW-485 <https://0xdata.atlassian.net/browse/SW-485>`__ - Update instructions for a new PYPI.org
   -  `SW-489 <https://0xdata.atlassian.net/browse/SW-489>`__ - Upgrade H2O to 3.10.5.3

v2.0.11 (2017-06-29)
--------------------

-  Bug
   
   -  `SW-469 <https://0xdata.atlassian.net/browse/SW-469>`__ - Remove accidentally added kerb.conf file
   -  `SW-470 <https://0xdata.atlassian.net/browse/SW-470>`__ - Allow to pask sparkSession to Security.enableSSL and deprecate sparkContext
   -  `SW-474 <https://0xdata.atlassian.net/browse/SW-474>`__ - Use deprecated HTTPClient as some CDH versions does not have the new method
   -  `SW-475 <https://0xdata.atlassian.net/browse/SW-475>`__ - Handle duke library in case it's loaded using --packages
   -  `SW-479 <https://0xdata.atlassian.net/browse/SW-479>`__ - Fix CHANGELOG location in make-dist.sh

-  Improvement
   
   -  `SW-457 <https://0xdata.atlassian.net/browse/SW-457>`__ - Clean up windows scripts
   -  `SW-466 <https://0xdata.atlassian.net/browse/SW-466>`__ - Separate Devel.md into multiple rst files
   -  `SW-472 <https://0xdata.atlassian.net/browse/SW-472>`__ - Convert to rst README in gradle dir
   -  `SW-473 <https://0xdata.atlassian.net/browse/SW-473>`__ - Upgrade to gradle 4.0
   -  `SW-477 <https://0xdata.atlassian.net/browse/SW-477>`__ - Upgrade H2O to 3.10.5.2
   -  `SW-480 <https://0xdata.atlassian.net/browse/SW-480>`__ - Bring back publishToMavenLocal task
   -  `SW-482 <https://0xdata.atlassian.net/browse/SW-482>`__ - Updates to change log location
   -  `SW-484 <https://0xdata.atlassian.net/browse/SW-484>`__ - Make rel-2.0 changelog consistent and also rst

v2.0.10 (2017-06-15)
--------------------

-  Technical task

   -  `SW-211 <https://0xdata.atlassian.net/browse/SW-211>`__ - In PySparkling for spark 2.0 document how to build the package

-  Bug

   -  `SW-448 <https://0xdata.atlassian.net/browse/SW-448>`__ - Add missing jar into the assembly
   -  `SW-450 <https://0xdata.atlassian.net/browse/SW-450>`__ - Fix instructions on the download site
   -  `SW-453 <https://0xdata.atlassian.net/browse/SW-453>`__ - Use size method to get attr num
   -  `SW-454 <https://0xdata.atlassian.net/browse/SW-454>`__ - Replace sparkSession with spark in backends documentation
   -  `SW-456 <https://0xdata.atlassian.net/browse/SW-456>`__ - Make shell scripts safe
   -  `SW-459 <https://0xdata.atlassian.net/browse/SW-459>`__ - Update PySparkling run-time dependencies
   -  `SW-461 <https://0xdata.atlassian.net/browse/SW-461>`__ - Fix wrong getters and setters in pysparkling
   -  `SW-467 <https://0xdata.atlassian.net/browse/SW-467>`__ - Fix typo in the FAQ documentation
   -  `SW-468 <https://0xdata.atlassian.net/browse/SW-468>`__ - Fix make-dist

-  New Feature

   -  `SW-455 <https://0xdata.atlassian.net/browse/SW-455>`__ - Replace the remaining references to egg files

-  Improvement

   -  `SW-24 <https://0xdata.atlassian.net/browse/SW-24>`__ - Append tab on Sparkling Water download page - how to use Sparkling Water package
   -  `SW-111 <https://0xdata.atlassian.net/browse/SW-111>`__ - Update FAQ with information about hive metastore location
   -  `SW-112 <https://0xdata.atlassian.net/browse/SW-112>`__ - Sparkling Water Tunning doc: add heartbeat dcoumentation
   -  `SW-311 <https://0xdata.atlassian.net/browse/SW-311>`__ - Please report Application Type to Yarn Resource Manager
   -  `SW-340 <https://0xdata.atlassian.net/browse/SW-340>`__ - Improve structure of SW README
   -  `SW-426 <https://0xdata.atlassian.net/browse/SW-426>`__ - Allow to download sparkling water logs from the spark UI
   -  `SW-444 <https://0xdata.atlassian.net/browse/SW-444>`__ - Remove references to Spark 1.5, 1.4 ( as it's old ) in README.rst and other docs
   -  `SW-447 <https://0xdata.atlassian.net/browse/SW-447>`__ - Upgrade H2O to 3.10.5.1
   -  `SW-452 <https://0xdata.atlassian.net/browse/SW-452>`__ - Add missing spaces after "," in H2OContextImplicits
   -  `SW-460 <https://0xdata.atlassian.net/browse/SW-460>`__ - Allow to configure flow dir location in SW
   -  `SW-463 <https://0xdata.atlassian.net/browse/SW-463>`__ - Extract sparkling water configuration to extra doc in rst format
   -  `SW-465 <https://0xdata.atlassian.net/browse/SW-465>`__ - Mark tensorflow demo as experimental

v2.0.9 (2017-05-25)
-------------------

-  Bug

   -  `SW-263 <https://0xdata.atlassian.net/browse/SW-263>`__ - Cannot run build in parallel because of Python module
   -  `SW-336 <https://0xdata.atlassian.net/browse/SW-336>`__ - Wrong documentation of PyPi h2o_pysparkling_2.0 package
   -  `SW-421 <https://0xdata.atlassian.net/browse/SW-421>`__ - External cluster: Job is reporting exit status as FAILED even all mappers return 0
   -  `SW-429 <https://0xdata.atlassian.net/browse/SW-429>`__ - Different cluster name between client and h2o nodes in case of external cluster
   -  `SW-430 <https://0xdata.atlassian.net/browse/SW-430>`__ - pysparkling: adding a column to a data frame does not work when  parse the original frame in spark
   -  `SW-431 <https://0xdata.atlassian.net/browse/SW-431>`__ - Allow to pass additional arguments to run-python-script.sh
   -  `SW-436 <https://0xdata.atlassian.net/browse/SW-436>`__ - Fix getting of sparkling water jar in pysparkling
   -  `SW-437 <https://0xdata.atlassian.net/browse/SW-437>`__ - Don't call atexit in case of pysparkling in cluster deploy mode
   -  `SW-438 <https://0xdata.atlassian.net/browse/SW-438>`__ - store h2o logs int unique directories
   -  `SW-439 <https://0xdata.atlassian.net/browse/SW-439>`__ - handle interrupted exception in H2ORuntimeInfoUIThread
   -  `SW-335 <https://0xdata.atlassian.net/browse/SW-335>`__ - Cannot install pysparkling from PyPi

-  Improvement

   -  `SW-445 <https://0xdata.atlassian.net/browse/SW-445>`__ - Remove information from README.pst that pip cannot be used
   -  `SW-341 <https://0xdata.atlassian.net/browse/SW-341>`__ - Support Python 3 distribution
   -  `SW-380 <https://0xdata.atlassian.net/browse/SW-380>`__ - Define Jenkins pipeline via Jenkinsfile
   -  `SW-422 <https://0xdata.atlassian.net/browse/SW-422>`__ - Upgrade H2O dependency to 3.10.4.6
   -  `SW-424 <https://0xdata.atlassian.net/browse/SW-424>`__ - Add SW tab in Spark History Server
   -  `SW-427 <https://0xdata.atlassian.net/browse/SW-427>`__ - Upgrade H2O dependency to 3.10.4.7
   -  `SW-433 <https://0xdata.atlassian.net/browse/SW-433>`__ - Add change logs link to the sw download page
   -  `SW-435 <https://0xdata.atlassian.net/browse/SW-435>`__ - Upgrade shadow jar plugin to 2.0.0
   -  `SW-440 <https://0xdata.atlassian.net/browse/SW-440>`__ - Sparkling Water cluster name should contain spark app id instead of random number
   -  `SW-441 <https://0xdata.atlassian.net/browse/SW-441>`__ - Replace deprecated DefaultHTTPClient in AnnouncementService
   -  `SW-442 <https://0xdata.atlassian.net/browse/SW-442>`__ - Get array size from metadata in case of ml.lilang.VectorUDT
   -  `SW-443 <https://0xdata.atlassian.net/browse/SW-443>`__ - Upgrade H2O version to 3.10.4.8

v2.0.8 (2017-04-07)
-------------------

-  Bug

   -  `SW-365 <https://0xdata.atlassian.net/browse/SW-365>`__ - Proper exit status handling of external cluster
   -  `SW-398 <https://0xdata.atlassian.net/browse/SW-398>`__ - Use timeout for read/write confirmation in external cluster mode
   -  `SW-400 <https://0xdata.atlassian.net/browse/SW-400>`__ - Fix stopping of H2OContext in case of running standalone application
   -  `SW-401 <https://0xdata.atlassian.net/browse/SW-401>`__ - Add configuration property to external backend allowing to specify the maximal timeout the cloud will wait for watchdog client to connect
   -  `SW-405 <https://0xdata.atlassian.net/browse/SW-405>`__ - Use correct quote in backend documentation
   -  `SW-408 <https://0xdata.atlassian.net/browse/SW-408>`__ - Use kwargs for h2o.connect in pysparkling
   -  `SW-409 <https://0xdata.atlassian.net/browse/SW-409>`__ - Fix stopping of python tests
   -  `SW-410 <https://0xdata.atlassian.net/browse/SW-410>`__ - Honor --core Spark settings in H2O executors
   -  `SW-419 <https://0xdata.atlassian.net/browse/SW-419>`__ - Fixlf4JLoggerFactory creating on Spark 2.0

-  Improvement

   -  `SW-231 <https://0xdata.atlassian.net/browse/SW-231>`__ - Sparkling Water download page is missing PySParkling/RSparkling info
   -  `SW-404 <https://0xdata.atlassian.net/browse/SW-404>`__ - Upgrade H2O dependency to 3.10.4.4
   -  `SW-406 <https://0xdata.atlassian.net/browse/SW-406>`__ - Download page should list available jars for external cluster.
   -  `SW-411 <https://0xdata.atlassian.net/browse/SW-411>`__ - Migrate Pysparkling tests and examples to SparkSession
   -  `SW-412 <https://0xdata.atlassian.net/browse/SW-412>`__ - Upgrade H2O dependency to 3.10.4.5

2.0.7 (2017-04-07)
------------------

-  Bug

   -  `SW-334 <https://0xdata.atlassian.net/browse/SW-334>`__ - as_factor() 'corrupts' dataframe if it fails
   -  `SW-353 <https://0xdata.atlassian.net/browse/SW-353>`__ - Kerberos for SW not loading JAAS module
   -  `SW-364 <https://0xdata.atlassian.net/browse/SW-364>`__ - Repl session not set on scala 2.11
   -  `SW-368 <https://0xdata.atlassian.net/browse/SW-368>`__ - bin/pysparkling.cmd is missing
   -  `SW-371 <https://0xdata.atlassian.net/browse/SW-371>`__ - Fix MarkDown syntax
   -  `SW-372 <https://0xdata.atlassian.net/browse/SW-372>`__ - Run negative test for PUBDEV-3808 multiple times to observe failure
   -  `SW-375 <https://0xdata.atlassian.net/browse/SW-375>`__ - Documentation fix in external cluster manual
   -  `SW-376 <https://0xdata.atlassian.net/browse/SW-376>`__ - Tests for DecimalType and DataType fail on external backend
   -  `SW-377 <https://0xdata.atlassian.net/browse/SW-377>`__ - Implement stopping of external H2O cluster in external backend mode
   -  `SW-383 <https://0xdata.atlassian.net/browse/SW-383>`__ - Update PySparkling README with info about SW-335 and using SW from Pypi
   -  `SW-385 <https://0xdata.atlassian.net/browse/SW-385>`__ - Fix residual plot R code generator
   -  `SW-386 <https://0xdata.atlassian.net/browse/SW-386>`__ - SW REPL cannot be used in combination with Spark Dataset
   -  `SW-387 <https://0xdata.atlassian.net/browse/SW-387>`__ - Fix typo in setClientIp method
   -  `SW-388 <https://0xdata.atlassian.net/browse/SW-388>`__ - Stop h2o when running inside standalone pysparkling job
   -  `SW-389 <https://0xdata.atlassian.net/browse/SW-389>`__ - Extending h2o jar from SW doesn't work when the jar is already downloaded
   -  `SW-392 <https://0xdata.atlassian.net/browse/SW-392>`__ - Python in gradle is using wrong python - it doesn't respect the PATH variable
   -  `SW-393 <https://0xdata.atlassian.net/browse/SW-393>`__ - Allow to specify timeout for h2o cloud up in external backend mode
   -  `SW-394 <https://0xdata.atlassian.net/browse/SW-394>`__ - Allow to specify log level to external h2o cluster
   -  `SW-396 <https://0xdata.atlassian.net/browse/SW-396>`__ - Create setter in pysparkling conf for h2o client log level
   -  `SW-397 <https://0xdata.atlassian.net/browse/SW-397>`__ - Better error message covering the most often case when cluster info file doesn't exist

-  Improvement

   -  `SW-296 <https://0xdata.atlassian.net/browse/SW-296>`__ - H2OConf remove nulls and make it more Scala-like
   -  `SW-367 <https://0xdata.atlassian.net/browse/SW-367>`__ - Add task to Gradle build which prints all available Hadoop distributions for the corresponding h2o
   -  `SW-382 <https://0xdata.atlassian.net/browse/SW-382>`__ - Upgrade of H2O dependency to 3.10.4.3

2.0.6 (2017-03-21)
------------------

-  Bug

   -  `SW-306 <https://0xdata.atlassian.net/browse/SW->`__ - KubasCluster: Notify file fails on failure
   -  `SW-308 <https://0xdata.atlassian.net/browse/SW->`__ - Intermittent failure in creating H2O cloud
   -  `SW-321 <https://0xdata.atlassian.net/browse/SW->`__ - composite function fail when inner cbind()
   -  `SW-331 <https://0xdata.atlassian.net/browse/SW->`__ - Security.enableSSL does not work
   -  `SW-347 <https://0xdata.atlassian.net/browse/SW->`__ - Cannot start Sparkling Water at HDP Yarn cluster
   -  `SW-349 <https://0xdata.atlassian.net/browse/SW->`__ - Sparkling Shell scripts for Windows do not work
   -  `SW-350 <https://0xdata.atlassian.net/browse/SW->`__ - Fix command line environment for Windows
   -  `SW-357 <https://0xdata.atlassian.net/browse/SW->`__ - PySparkling in Zeppelin environment using wrong class loader
   -  `SW-361 <https://0xdata.atlassian.net/browse/SW->`__ - Flow is not available in Sparkling Water
   -  `SW-362 <https://0xdata.atlassian.net/browse/SW->`__ - PySparkling does not work

-  Improvement

   -  `SW-333 <https://0xdata.atlassian.net/browse/SW->`__ - ApplicationMaster info in Yarn for external cluster
   -  `SW-337 <https://0xdata.atlassian.net/browse/SW->`__ - Use ``h2o.connect`` in PySpark to connect to H2O cluster
   -  `SW-338 <https://0xdata.atlassian.net/browse/SW->`__ - h2o.init in PySpark prints internal IP. We should remove it or replace it with actual IP of driver node (based on spark_DNS settings)
   -  `SW-344 <https://0xdata.atlassian.net/browse/SW->`__ - Use Spark public DNS if available to report Flow UI
   -  `SW-345 <https://0xdata.atlassian.net/browse/SW->`__ - Create configuration manual for External cluster
   -  `SW-356 <https://0xdata.atlassian.net/browse/SW->`__ - Fix documentation for spark.ext.h2o.fail.on.unsupported.spark.param
   -  `SW-359 <https://0xdata.atlassian.net/browse/SW->`__ - Upgrade H2O dependency to 3.10.4.1
   -  `SW-360 <https://0xdata.atlassian.net/browse/SW->`__ - Upgrade H2O dependency to 3.10.4.2
   -  `SW-363 <https://0xdata.atlassian.net/browse/SW->`__ - Use Spark public DNS if available to report Flow UI

2.0.5 (2017-02-10)
------------------

-  Improvement

   -  `SW-325 <https://0xdata.atlassian.net/browse/SW-325>`__ - Implement a generic announcement mechanism
   -  `SW-327 <https://0xdata.atlassian.net/browse/SW-327>`__ - Enrich Spark UI with Sparkling Water specific tab
   -  `SW-328 <https://0xdata.atlassian.net/browse/SW-328>`__ - Put link to h2oai github into README.md

2.0.4 (2017-01-02)
------------------

-  Bug

   -  `SW-303 <https://0xdata.atlassian.net/browse/SW-303>`__ - Failure on DecimalType conversion
   -  `SW-305 <https://0xdata.atlassian.net/browse/SW-305>`__ - Failure on DateType
   -  `SW-309 <https://0xdata.atlassian.net/browse/SW-309>`__ - Handling for Spark DateType in SW
   -  `SW-310 <https://0xdata.atlassian.net/browse/SW-310>`__ - Decimal(2,1) not compatible in h2o frame
   -  `SW-322 <https://0xdata.atlassian.net/browse/SW-322>`__ - Python README.md says it does not support Spark 2.0

-  Improvement

   -  `SW-313 <https://0xdata.atlassian.net/browse/SW-313>`__ - Document and test SSL support
   -  `SW-314 <https://0xdata.atlassian.net/browse/SW-314>`__ - Document SSL security for sparkling water
   -  `SW-317 <https://0xdata.atlassian.net/browse/SW-317>`__ - Upgrade to H2O version 3.10.3.2

2.0.3 (2017-01-04)
------------------

-  Bug

   -  `SW-152 <https://0xdata.atlassian.net/browse/SW-152>`__ - ClassNotFound with spark-submit
   -  `SW-266 <https://0xdata.atlassian.net/browse/SW-266>`__ - H2OContext shouldn't be Serializable
   -  `SW-276 <https://0xdata.atlassian.net/browse/SW-276>`__ - ClassLoading issue when running code using SparkSubmit
   -  `SW-281 <https://0xdata.atlassian.net/browse/SW-281>`__ - Update sparkling water tests so they use correct frame locking
   -  `SW-283 <https://0xdata.atlassian.net/browse/SW-283>`__ - Set spark.sql.warehouse.dir explicitly in tests because of SPARK-17810
   -  `SW-284 <https://0xdata.atlassian.net/browse/SW-284>`__ - Fix CraigsListJobTitlesApp to use local file instead of trying to get one from hdfs
   -  `SW-285 <https://0xdata.atlassian.net/browse/SW-285>`__ - Disable timeline service also in python integration tests
   -  `SW-286 <https://0xdata.atlassian.net/browse/SW-286>`__ - Add missing test in pysparkling for conversion RDD[Double] -> H2OFrame
   -  `SW-287 <https://0xdata.atlassian.net/browse/SW-287>`__ - Fix bug in SparkDataFrame converter where key wasn't random if not specified
   -  `SW-288 <https://0xdata.atlassian.net/browse/SW-288>`__ - Improve performance of Dataset tests and call super.afterAll
   -  `SW-289 <https://0xdata.atlassian.net/browse/SW-289>`__ - Fix PySparkling numeric handling during conversions
   -  `SW-290 <https://0xdata.atlassian.net/browse/SW-290>`__ - Fixes and improvements of task used to extended h2o jars by sparkling-water classes
   -  `SW-292 <https://0xdata.atlassian.net/browse/SW-292>`__ - Fix ScalaCodeHandlerTestSuite

-  New Feature

   -  `SW-178 <https://0xdata.atlassian.net/browse/SW-178>`__ - Allow external h2o cluster to act as h2o backend in Sparkling Water

-  Improvement

   -  `SW-282 <https://0xdata.atlassian.net/browse/SW-282>`__ - Integrate SW with H2O 3.10.1.2 ( Support for external cluster )
   -  `SW-291 <https://0xdata.atlassian.net/browse/SW-291>`__ - Use absolute value for random number in sparkling-water in internal backend
   -  `SW-295 <https://0xdata.atlassian.net/browse/SW-295>`__ - H2OConf should be parameterized by SparkConf and not by SparkContext

2.0.2 (2016-12-09)
------------------

-  Bug

   -  `SW-271 <https://0xdata.atlassian.net/browse/SW-271>`__ - SparklingWater Driver is not using SparkSession
   -  `SW-272 <https://0xdata.atlassian.net/browse/SW-272>`__ - Microsoft Azure: deployment of pysparkling is failing
   -  `SW-274 <https://0xdata.atlassian.net/browse/SW-274>`__ - When grep options are configured, Spark version detection does not work

2.0.1 (2016-12-04)
------------------

-  Bug

   -  `SW-196 <https://0xdata.atlassian.net/browse/SW-196>`__ - Fix wrong output of **str** on H2OContext
   -  `SW-212 <https://0xdata.atlassian.net/browse/SW-212>`__ - Fix depreciation warning regarding the compiler in scala.gradle
   -  `SW-221 <https://0xdata.atlassian.net/browse/SW-221>`__ - SVM: the model is not unlocked after building
   -  `SW-226 <https://0xdata.atlassian.net/browse/SW-226>`__ - SVM: binomial model - AUC curves are missing
   -  `SW-227 <https://0xdata.atlassian.net/browse/SW-227>`__ - java.lang.ClassCastException: com.sun.proxy.$Proxy19 cannot be cast to water.api.API
   -  `SW-242 <https://0xdata.atlassian.net/browse/SW-242>`__ - Fix Python build process
   -  `SW-248 <https://0xdata.atlassian.net/browse/SW-248>`__ - Fix TensorFlow notebook to support Python 3
   -  `SW-264 <https://0xdata.atlassian.net/browse/SW-264>`__ - PySparkling is not using existing SQLContext
   -  `SW-268 <https://0xdata.atlassian.net/browse/SW-268>`__ - Databricks cloud: Jetty class loading problem.

-  New Feature

   -  `SW-267 <https://0xdata.atlassian.net/browse/SW-267>`__ - Add assembly-h2o module which will extend h2o/h2odriver jar by additional classes

-  Improvement

   -  `SW-129 <https://0xdata.atlassian.net/browse/SW-129>`__ - Add support for transformation from H2OFrame -> RDD in PySparkling
   -  `SW-169 <https://0xdata.atlassian.net/browse/SW-169>`__ - Remove deprecated calls
   -  `SW-193 <https://0xdata.atlassian.net/browse/SW-193>`__ - Append scala version to pysparkling package name
   -  `SW-200 <https://0xdata.atlassian.net/browse/SW-200>`__ - Add flows from presentation in Budapest and Paris to flows dir
   -  `SW-208 <https://0xdata.atlassian.net/browse/SW-208>`__ - Generate all PySparkling artefacts into build directory
   -  `SW-209 <https://0xdata.atlassian.net/browse/SW-209>`__ - RSparkling: improve handling of Sparkling Water package ependencies
   -  `SW-215 <https://0xdata.atlassian.net/browse/SW-215>`__ - Improve internal type handling
   -  `SW-219 <https://0xdata.atlassian.net/browse/SW-219>`__ - RSparkling: as_h2o_frame should properly name the frame
   -  `SW-230 <https://0xdata.atlassian.net/browse/SW-230>`__ - Fix sparkling-shell windows script
   -  `SW-235 <https://0xdata.atlassian.net/browse/SW-235>`__ - Discover py4j package version automatically from SPARK_HOME
   -  `SW-243 <https://0xdata.atlassian.net/browse/SW-243>`__ - Remove all references to local-cluster[...] in our doc
   -  `SW-245 <https://0xdata.atlassian.net/browse/SW-245>`__ - Upgrade of H2O dependency to the latest turing release (3.10.0.10)

2.0.0 (2016-09-26)
------------------

-  Bugs

   -  `SW-57 <https://0xdata.atlassian.net/browse/SW-57>`__ - Produce artifacts for Scala 2.11
   -  `SW-71 <https://0xdata.atlassian.net/browse/SW-71>`__ - Expose method ``H2OContext#setLogLevel`` to setup log level of H2O
   -  `SW-128 <https://0xdata.atlassian.net/browse/SW-128>`__ - Publish flows pack in GitHub repo and embed them in distributed JAR
   -  `SW-168 <https://0xdata.atlassian.net/browse/SW-168>`__ - Explore slow-down for fat-dataset with many categorical columns
   -  `SW-172 <https://0xdata.atlassian.net/browse/SW-172>`__ - ``NodeDesc`` should be interned or use ``H2OKey`` instead of ``NodeDesc``
   -  `SW-176 <https://0xdata.atlassian.net/browse/SW-176>`__ - H2O context is failing on CDH-5.7.1 with Spark Version 1.6.0-CDH.5.7.1
   -  `SW-185 <https://0xdata.atlassian.net/browse/SW-185>`__ - Methods on frame can't be called in compute method on external cluster
   -  `SW-186 <https://0xdata.atlassian.net/browse/SW-186>`__ - Hide checks whether incoming data is NA into convertorCtx
   -  `SW-191 <https://0xdata.atlassian.net/browse/SW-191>`__ - Better exception message in case dataframe with the desired key already exist when saving using datasource api
   -  `SW-192 <https://0xdata.atlassian.net/browse/SW-192>`__ - Add ``org.apache.spark.sql._`` to packages imported by default in REPL
   -  `SW-197 <https://0xdata.atlassian.net/browse/SW-197>`__ - Fix all mentions of ``H2OContext(sc)`` to ``H2OContext.getOrCreate(sc)`` in PySparkling
   -  `SW-201 <https://0xdata.atlassian.net/browse/SW-201>`__ - Methods in water.support classes should use ``[T <: Frame]`` instead of ``H2OFrame``
   -  `SW-202 <https://0xdata.atlassian.net/browse/SW-202>`__ - Pipeline scripts are not tested!
   -  `SW-205 <https://0xdata.atlassian.net/browse/SW-205>`__ - PySparkling tests launcher does not report error correctly
   -  `SW-210 <https://0xdata.atlassian.net/browse/SW-210>`__ - Change log level of arguments used to start client to Info

-  New Features

   -  `SW-182 <https://0xdata.atlassian.net/browse/SW-182>`__ - RSparkling: use Sparkling Water API directly from R
   -  `SW-206 <https://0xdata.atlassian.net/browse/SW-206>`__ - Support Spark 2.0

-  Improvements

   -  `SW-158 <https://0xdata.atlassian.net/browse/SW-158>`__ - Support Spark DataSet in the same way as RDD and DataFrame
   -  `SW-163 <https://0xdata.atlassian.net/browse/SW-163>`__ - Upgrade H2O dependency to the latest Turing release
   -  `SW-164 <https://0xdata.atlassian.net/browse/SW-164>`__ - Replace usage of ``SQLContext`` by ``SparkSession``
   -  `SW-165 <https://0xdata.atlassian.net/browse/SW-165>`__ - Change default schema for Scala code to black one.
   -  `SW-170 <https://0xdata.atlassian.net/browse/SW-170>`__ - Unify H2OFrame datasource and asDataFrame API
   -  `SW-171 <https://0xdata.atlassian.net/browse/SW-171>`__ - Internal API refactoring to allow multiple backends
   -  `SW-174 <https://0xdata.atlassian.net/browse/SW-174>`__ - Remove unused fields from H2ORDD
   -  `SW-177 <https://0xdata.atlassian.net/browse/SW-177>`__ - Refactor and simplify REPL
   -  `SW-204 <https://0xdata.atlassian.net/browse/SW-204>`__ - Distribute tests log4j logs to corresponding build directories

-  Breaking API changes

   -  The enum ``hex.Distribution.Family`` is now ``hex.genmodel.utils.DistributionFamily``
   -  The deprecated methods (e.g., ``H2OContext#asSchemaRDD``) were removed

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
