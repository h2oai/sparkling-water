Change Log
==========

v2.1.10 (2017-06-29)
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
   -  `SW-483 <https://0xdata.atlassian.net/browse/SW-483>`__ - Make rel-2.1 changelog consistent and also rst

v2.1.9 (2017-06-15)
-------------------

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

v2.1.8 (2017-05-25)
-------------------

-  Bug

   -  `SW-263 <https://0xdata.atlassian.net/browse/SW-263>`__ - Cannot run build in parallel because of Python module
   -  `SW-336 <https://0xdata.atlassian.net/browse/SW-336>`__ - Wrong documentation of PyPi h2o_pysparkling_2.0 package
   -  `SW-430 <https://0xdata.atlassian.net/browse/SW-430>`__ - pysparkling: adding a column to a data frame does not work when parse the original frame in spark
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
   -  `SW-433 <https://0xdata.atlassian.net/browse/SW-433>`__ - Add change logs link to the sw download page
   -  `SW-435 <https://0xdata.atlassian.net/browse/SW-435>`__ - Upgrade shadow jar plugin to 2.0.0
   -  `SW-440 <https://0xdata.atlassian.net/browse/SW-440>`__ - Sparkling Water cluster name should contain spark app id instead of random number
   -  `SW-441 <https://0xdata.atlassian.net/browse/SW-441>`__ - Replace deprecated DefaultHTTPClient in AnnouncementService
   -  `SW-442 <https://0xdata.atlassian.net/browse/SW-442>`__ - Get array size from metadata in case of ml.lilang.VectorUDT
   -  `SW-443 <https://0xdata.atlassian.net/browse/SW-443>`__ - Upgrade H2O version to 3.10.4.8

v2.1.7 (2017-05-10)
-------------------

-  Bug

   -  `SW-429 <https://0xdata.atlassian.net/browse/SW-429>`__ - Different cluster name between client and h2o nodes in case of external cluster

v2.1.6 (2017-05-09)
-------------------

-  Improvement

   -  `SW-424 <https://0xdata.atlassian.net/browse/SW-424>`__ - Add SW tab in Spark History Server
   -  `SW-427 <https://0xdata.atlassian.net/browse/SW-427>`__ - Upgrade H2O dependency to 3.10.4.7

v2.1.5 (2017-04-27)
-------------------

-  Bug

   -  `SW-421 <https://0xdata.atlassian.net/browse/SW-421>`__ - Externar cluster: Job is reporting exit status as FAILED even all mappers return 0

-  Improvement

   -  `SW-422 <https://0xdata.atlassian.net/browse/SW-422>`__ - Upgrade H2O dependency to 3.10.4.6

v2.1.4 (2017-04-20)
-------------------

-  Bug

   -  `SW-65 <https://0xdata.atlassian.net/browse/SW-65>`__ - Add pysparkling instruction to download page
   -  `SW-365 <https://0xdata.atlassian.net/browse/SW-365>`__ - Properexit status handling of external cluster
   -  `SW-398 <https://0xdata.atlassian.net/browse/SW-398>`__ - Usetimeout for read/write confirmation in external cluster mode
   -  `SW-400 <https://0xdata.atlassian.net/browse/SW-400>`__ - Fix stopping of H2OContext in case of running standalone application
   -  `SW-401 <https://0xdata.atlassian.net/browse/SW-401>`__ - Add configuration property to external backend allowing to specify the maximal timeout the cloud will wait for watchdog client to connect
   -  `SW-405 <https://0xdata.atlassian.net/browse/SW-405>`__ - Use correct quote in backend documentation
   -  `SW-408 <https://0xdata.atlassian.net/browse/SW-408>`__ - Use kwargs for h2o.connect in pysparkling
   -  `SW-409 <https://0xdata.atlassian.net/browse/SW-409>`__ - Fix stopping of python tests
   -  `SW-410 <https://0xdata.atlassian.net/browse/SW-410>`__ - Honor --core Spark settings in H2O executors

-  Improvement

   -  `SW-231 <https://0xdata.atlassian.net/browse/SW-231>`__ - Sparkling Water download page is missing PySParkling/RSparkling info
   -  `SW-404 <https://0xdata.atlassian.net/browse/SW-404>`__ - Upgrade H2O dependency to 3.10.4.4
   -  `SW-406 <https://0xdata.atlassian.net/browse/SW-406>`__ - Download page should list available jars for external cluster.
   -  `SW-411 <https://0xdata.atlassian.net/browse/SW-411>`__ - Migrate Pysparkling tests and examples to SparkSession
   -  `SW-412 <https://0xdata.atlassian.net/browse/SW-412>`__ - Upgrade H2O dependency to 3.10.4.5

v2.1.3 (2017-04-7)
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

v2.1.2 (2017-03-20)
-------------------

-  Bug

   -  `SW-361 <https://0xdata.atlassian.net/browse/SW-361>`__ - Flow is not available in Sparkling Water
   -  `SW-362 <https://0xdata.atlassian.net/browse/SW-362>`__ - PySparkling does not work

-  Improvement

   -  `SW-344 <https://0xdata.atlassian.net/browse/SW-344>`__ - Use Spark public DNS if available to report Flow UI

v2.1.1 (2017-03-18)
-------------------

-  Bug

   -  `SW-308 <https://0xdata.atlassian.net/browse/SW-308>`__ - Intermittent failure in creating H2O cloud
   -  `SW-321 <https://0xdata.atlassian.net/browse/SW-321>`__ - composite function fail when inner cbind()
   -  `SW-342 <https://0xdata.atlassian.net/browse/SW-342>`__ - Environment detection does not work with Spark2.1
   -  `SW-347 <https://0xdata.atlassian.net/browse/SW-347>`__ - Cannot start Sparkling Water at HDP Yarn cluster
   -  `SW-349 <https://0xdata.atlassian.net/browse/SW-349>`__ - Sparkling Shell scripts for Windows do not work
   -  `SW-350 <https://0xdata.atlassian.net/browse/SW-350>`__ - Fix command line environment for Windows
   -  `SW-357 <https://0xdata.atlassian.net/browse/SW-357>`__ - PySparkling in Zeppelin environment using wrong class loader

-  Improvement

   -  `SW-333 <https://0xdata.atlassian.net/browse/SW-333>`__ - ApplicationMaster info in Yarn for external cluster
   -  `SW-337 <https://0xdata.atlassian.net/browse/SW-337>`__ - Use ``h2o.connect`` in PySpark to connect to H2O cluster
   -  `SW-345 <https://0xdata.atlassian.net/browse/SW-345>`__ - Create configuration manual for External cluster
   -  `SW-356 <https://0xdata.atlassian.net/browse/SW-356>`__ - Improve documentation for spark.ext.h2o.fail.on.unsupported.spark.param
   -  `SW-360 <https://0xdata.atlassian.net/browse/SW-360>`__ - Upgrade H2O dependency to 3.10.4.2

v2.1.0 (2017-03-02)
-------------------

-  Bug

   -  `SW-331 <https://0xdata.atlassian.net/browse/SW-331>`__ - Security.enableSSL does not work

-  Improvement

   -  `SW-302 <https://0xdata.atlassian.net/browse/SW-302>`__ - Support Spark 2.1.0
   -  `SW-325 <https://0xdata.atlassian.net/browse/SW-325>`__ - Implement a generic announcement mechanism
   -  `SW-326 <https://0xdata.atlassian.net/browse/SW-326>`__ - Add support to Spark 2.1 in Sparkling Water
   -  `SW-327 <https://0xdata.atlassian.net/browse/SW-327>`__ - Enrich Spark UI with Sparkling Water specific tab

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
