Change Log
==========

v3.26.5 (2019-09-16)
--------------------
Downloads:
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/3.26.5-2.1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/3.26.5-2.1/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/3.26.5-2.2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/3.26.5-2.2/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/3.26.5-2.3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/3.26.5-2.3/index.html>`__
   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/3.26.5-2.4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/3.26.5-2.4/index.html>`__
-  Bug
        
   -  `SW-1570 <https://0xdata.atlassian.net/browse/SW-1570>`__ - Fix typo contribution -&gt; contributions on SHAPLY documentation page
   -  `SW-1580 <https://0xdata.atlassian.net/browse/SW-1580>`__ - importing pysparkling in Zeppelin fails on SW for 3.26.3
   -  `SW-1583 <https://0xdata.atlassian.net/browse/SW-1583>`__ - Add missing namedMojoOutputParameter to PySparkling Algo constructors
   -  `SW-1584 <https://0xdata.atlassian.net/browse/SW-1584>`__ - Remove extra asserts for types already covered by type converters
   -  `SW-1585 <https://0xdata.atlassian.net/browse/SW-1585>`__ - Improve H2oGridSearch internal handling of Algo + improve API of ordering
   -  `SW-1592 <https://0xdata.atlassian.net/browse/SW-1592>`__ - [BUILD] Use numpy compatible with python 2 and python 3
   -  `SW-1596 <https://0xdata.atlassian.net/browse/SW-1596>`__ - Jupyter notebook is unable to start kernel for Spark 2.4
   -  `SW-1598 <https://0xdata.atlassian.net/browse/SW-1598>`__ - Deprecate cases in H2OMojoPrediction where the prediction column does not directly contain the predicted value.
   -  `SW-1600 <https://0xdata.atlassian.net/browse/SW-1600>`__ - Fix IIOB when using calibrated probabilities on MOJO
   -  `SW-1604 <https://0xdata.atlassian.net/browse/SW-1604>`__ - PySparkling fails to parse pyspark version with build number, such as 2.3.1.dev0
   -  `SW-1605 <https://0xdata.atlassian.net/browse/SW-1605>`__ - Gradle reported success even though the python test failed
   -  `SW-1610 <https://0xdata.atlassian.net/browse/SW-1610>`__ - Fix running python tests by changing the env directly
   -  `SW-1611 <https://0xdata.atlassian.net/browse/SW-1611>`__ - Script tests are not being executed correctly - some tests are not being executed
   -  `SW-1614 <https://0xdata.atlassian.net/browse/SW-1614>`__ - Fix running python integ tests on external backend
                
-  Epic
        
   -  `SW-1291 <https://0xdata.atlassian.net/browse/SW-1291>`__ - Benchmarks
                
-  New Feature
        
   -  `SW-1547 <https://0xdata.atlassian.net/browse/SW-1547>`__ - Update H2OTargetEncoder according to changes in H2O-3 3.26.0.4 
   -  `SW-1593 <https://0xdata.atlassian.net/browse/SW-1593>`__ - Upgrade to Spark 2.4.4
   -  `SW-1609 <https://0xdata.atlassian.net/browse/SW-1609>`__ - Automatically configure H2OContext in case we run on DBC in order to correctly show Flow
                
-  Task
        
   -  `SW-1295 <https://0xdata.atlassian.net/browse/SW-1295>`__ - Benchmarks: Configuration for External Backend
   -  `SW-1296 <https://0xdata.atlassian.net/browse/SW-1296>`__ - Benchmarks: Code Clean up
   -  `SW-1553 <https://0xdata.atlassian.net/browse/SW-1553>`__ - Benchmarks: Jenkins Pipeline
   -  `SW-1616 <https://0xdata.atlassian.net/browse/SW-1616>`__ - Benchmarks: Automatic Cluster Shutdown after Timeout
                
-  Improvement
        
   -  `SW-1564 <https://0xdata.atlassian.net/browse/SW-1564>`__ - Create  gradle task to build scala image for kubernetes
   -  `SW-1573 <https://0xdata.atlassian.net/browse/SW-1573>`__ - Remove use of deprecated option spark.ext.h2o.external.cluster.num.h2o.nodes
   -  `SW-1582 <https://0xdata.atlassian.net/browse/SW-1582>`__ - Make sure H2OGLM API is consistent with others and does not use the labmda_ hack
   -  `SW-1591 <https://0xdata.atlassian.net/browse/SW-1591>`__ - Don&#39;t use strings to define algo name
   -  `SW-1597 <https://0xdata.atlassian.net/browse/SW-1597>`__ - Add a note to documentation informing that terraform template is available only for Spark 2.4
   -  `SW-1613 <https://0xdata.atlassian.net/browse/SW-1613>`__ - Upgrade H2O to 3.26.0.4
   -  `SW-1615 <https://0xdata.atlassian.net/browse/SW-1615>`__ - Remove examples from assembly jar
                
-  Engineering Story
        
   -  `SW-1567 <https://0xdata.atlassian.net/browse/SW-1567>`__ - Upgrade version of h2o docker image from 58 to the 64
   -  `SW-1571 <https://0xdata.atlassian.net/browse/SW-1571>`__ - Upgrade CI docker image for 19 ( build on latest hadoop docker image hd2.2:64)
   -  `SW-1576 <https://0xdata.atlassian.net/browse/SW-1576>`__ - Retrain sms_pipeline_model for PipelinePredictionTest so it uses ai.h2o package
   -  `SW-1577 <https://0xdata.atlassian.net/browse/SW-1577>`__ - Generic logic to test parametr passing to algo wrappers on PySParkling
   -  `SW-1594 <https://0xdata.atlassian.net/browse/SW-1594>`__ - Upgrade to a docker image with Spark 2.4.4
   -  `SW-1599 <https://0xdata.atlassian.net/browse/SW-1599>`__ - Target Encoder Tests Covering Various Order of Columns
   -  `SW-1603 <https://0xdata.atlassian.net/browse/SW-1603>`__ - Install Terraform to Docker Image Running Tests 
                
-  Docs
        
   -  `SW-1581 <https://0xdata.atlassian.net/browse/SW-1581>`__ - Add Rule of Thumb for Data Conversion
   -  `SW-1595 <https://0xdata.atlassian.net/browse/SW-1595>`__ - Document How to Generate Prediction Contributions from an Existing MOJO 
                
    
v3.26.3 (2019-08-28)
--------------------
Downloads:
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/3.26.3-2.1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/3.26.3-2.1/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/3.26.3-2.2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/3.26.3-2.2/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/3.26.3-2.3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/3.26.3-2.3/index.html>`__
   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/3.26.3-2.4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/3.26.3-2.4/index.html>`__
-  Bug
        
   -  `SW-1477 <https://0xdata.atlassian.net/browse/SW-1477>`__ - Fix bug with setting init on KMeans
   -  `SW-1478 <https://0xdata.atlassian.net/browse/SW-1478>`__ - Don&#39;t need to start H2O to initialize algo on PySparkling side
   -  `SW-1486 <https://0xdata.atlassian.net/browse/SW-1486>`__ - Remove extra argument on H2OAUtoML pyspark wrapper
   -  `SW-1491 <https://0xdata.atlassian.net/browse/SW-1491>`__ - Fix wrong statement regarding version in terraform documentation 
   -  `SW-1493 <https://0xdata.atlassian.net/browse/SW-1493>`__ - Properly apply type checks even for None values on all H2OAutoML parameters
   -  `SW-1498 <https://0xdata.atlassian.net/browse/SW-1498>`__ - Check if Jar is already attached to the cluster in Initializer
   -  `SW-1499 <https://0xdata.atlassian.net/browse/SW-1499>`__ - Extract zip file into temp directory owned and configured by Spark configured by spark.local.dir 
   -  `SW-1509 <https://0xdata.atlassian.net/browse/SW-1509>`__ - Rename  colsampleBytree to  colSampleByTree on H2OXGBoost in Scala &amp; Python
   -  `SW-1512 <https://0xdata.atlassian.net/browse/SW-1512>`__ - Fix broken link to Running Sparkling Water on supported platforms in the doc
   -  `SW-1516 <https://0xdata.atlassian.net/browse/SW-1516>`__ - latest-stable/doc/deployment/pysparkling_pipeline.html is refering to old ratio and predictionCol parameters
   -  `SW-1517 <https://0xdata.atlassian.net/browse/SW-1517>`__ - Update doc for RSparkling on Windows for latest RSparkling changes
   -  `SW-1520 <https://0xdata.atlassian.net/browse/SW-1520>`__ - Fix warning &#39;H2OMOJOSettings&#39; object has no attribute &#39;_java_obj&#39;
   -  `SW-1521 <https://0xdata.atlassian.net/browse/SW-1521>`__ - Fix wrong link to latest spark version in main README
   -  `SW-1522 <https://0xdata.atlassian.net/browse/SW-1522>`__ - createTempDit in SharedBackendUtils should create temp fiels in the Spark temp dir
   -  `SW-1523 <https://0xdata.atlassian.net/browse/SW-1523>`__ - Fix IP based cloud-up on client side
   -  `SW-1524 <https://0xdata.atlassian.net/browse/SW-1524>`__ - Fix getters on MOJO models
   -  `SW-1525 <https://0xdata.atlassian.net/browse/SW-1525>`__ - Fix broken links in the documentation (poiting to old release or unexistent locations)
   -  `SW-1526 <https://0xdata.atlassian.net/browse/SW-1526>`__ - PySparkling cannot parse embedded version.txt file.
   -  `SW-1527 <https://0xdata.atlassian.net/browse/SW-1527>`__ - Verify version between H2O external back-end &amp; H2O client on Spark driver
   -  `SW-1541 <https://0xdata.atlassian.net/browse/SW-1541>`__ - Fix get-extended-h2o script to reflect new location of sparkling water releases
   -  `SW-1543 <https://0xdata.atlassian.net/browse/SW-1543>`__ - Expose driver if, port, port range and extra memory percent configuration for external H2O cluster
   -  `SW-1546 <https://0xdata.atlassian.net/browse/SW-1546>`__ - Fix SpreadRDDBuilder not serializable exception
   -  `SW-1548 <https://0xdata.atlassian.net/browse/SW-1548>`__ - Move all pysparkling source files into single src dir
   -  `SW-1551 <https://0xdata.atlassian.net/browse/SW-1551>`__ - Fix path to external jars generated by ./gradlew extendJar
   -  `SW-1555 <https://0xdata.atlassian.net/browse/SW-1555>`__ - Fix obtaining the version when pysparkling is installed via pip
   -  `SW-1558 <https://0xdata.atlassian.net/browse/SW-1558>`__ - Use absolute imports in tests as the relative ones are removed in python3
   -  `SW-1565 <https://0xdata.atlassian.net/browse/SW-1565>`__ - startWorkerNodes and startClient was returning hostname instead of ip address
                
-  Story
        
   -  `SW-1530 <https://0xdata.atlassian.net/browse/SW-1530>`__ - Conversion to H2OFrame needs to work without running H2O client
                
-  New Feature
        
   -  `SW-1557 <https://0xdata.atlassian.net/browse/SW-1557>`__ - GLM no longer use MissingValuesHandling enum from DeepLearning
                
-  Task
        
   -  `SW-1487 <https://0xdata.atlassian.net/browse/SW-1487>`__ - Update examples/README file
   -  `SW-1506 <https://0xdata.atlassian.net/browse/SW-1506>`__ - Benchmarks: Terraform template for running benchmarks in EMR
   -  `SW-1542 <https://0xdata.atlassian.net/browse/SW-1542>`__ - Benchmarks: Name of result file should contain backed and master
   -  `SW-1552 <https://0xdata.atlassian.net/browse/SW-1552>`__ - Benchmarks: Gradle Task for Execution of Benchmarks
                
-  Improvement
        
   -  `SW-1368 <https://0xdata.atlassian.net/browse/SW-1368>`__ - MOJO depploymet package
   -  `SW-1475 <https://0xdata.atlassian.net/browse/SW-1475>`__ - Expose predict_contributions for H2OMOJOModel
   -  `SW-1481 <https://0xdata.atlassian.net/browse/SW-1481>`__ - Deprecate H2OMOJOModel, H2OMOJOPipelineModel and H2OMOJOSettings in the org.apache.spark package
   -  `SW-1483 <https://0xdata.atlassian.net/browse/SW-1483>`__ - Deprecate algos and features in org.apache.spark package
   -  `SW-1485 <https://0xdata.atlassian.net/browse/SW-1485>`__ - Handle sortMetric param in H2OAutoML the same way as other enums
   -  `SW-1490 <https://0xdata.atlassian.net/browse/SW-1490>`__ - Immutable projectName on H2OAUtoML
   -  `SW-1494 <https://0xdata.atlassian.net/browse/SW-1494>`__ - Upgrade Terraform Templates to AWS Provider 2.23
   -  `SW-1501 <https://0xdata.atlassian.net/browse/SW-1501>`__ -  Fix &#39;ai.h2o:sparkling-water-package_2.11:2.4.13&#39;/&#39;h2o_pysparkling_2.4&#39; conflict on Azure Databricks
   -  `SW-1502 <https://0xdata.atlassian.net/browse/SW-1502>`__ - Upgrade to mojo2 library v2.1.3
   -  `SW-1503 <https://0xdata.atlassian.net/browse/SW-1503>`__ - Avoid null on AutoML include &amp; exlude Algos params
   -  `SW-1504 <https://0xdata.atlassian.net/browse/SW-1504>`__ - Apply type converterts to rest of the PySparkling
   -  `SW-1515 <https://0xdata.atlassian.net/browse/SW-1515>`__ - Upgrade to H2O 3.26.0.3
   -  `SW-1528 <https://0xdata.atlassian.net/browse/SW-1528>`__ - Upgrade Gradle to Gradle 5.6
   -  `SW-1540 <https://0xdata.atlassian.net/browse/SW-1540>`__ - Remove unnecessary read confirmation timeout
   -  `SW-1549 <https://0xdata.atlassian.net/browse/SW-1549>`__ - Upgrade default instances in terraform templates to M5.xlarge
   -  `SW-1550 <https://0xdata.atlassian.net/browse/SW-1550>`__ - Remove unsupported notebook (referencing dead deepwater)
                
-  Engineering Story
        
   -  `SW-1476 <https://0xdata.atlassian.net/browse/SW-1476>`__ - Avoid duplication between mojo params and algo params
   -  `SW-1480 <https://0xdata.atlassian.net/browse/SW-1480>`__ - Cleanup of PySparkling package -&gt; moving to new package ai.h2o
   -  `SW-1511 <https://0xdata.atlassian.net/browse/SW-1511>`__ - Remove unused init_scala_int_session() from PySparkling
   -  `SW-1539 <https://0xdata.atlassian.net/browse/SW-1539>`__ - Avoid boiler plate code when introducing new test suite in PySpakrling
                
                            
v3.26.2 (2019-07-30)
--------------------
Downloads:
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/3.26.2-2.1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/3.26.2-2.1/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/3.26.2-2.2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/3.26.2-2.2/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/3.26.2-2.3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/3.26.2-2.3/index.html>`__
   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/3.26.2-2.4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/3.26.2-2.4/index.html>`__
-  Bug
        
   -  `SW-1337 <https://0xdata.atlassian.net/browse/SW-1337>`__ - Restarting h2o cluster makes all Spark Sessions connected to it unusable
   -  `SW-1379 <https://0xdata.atlassian.net/browse/SW-1379>`__ - Fix IOOB exception when converting H2OFrame to DataFrame
   -  `SW-1381 <https://0xdata.atlassian.net/browse/SW-1381>`__ - Bad quotes in documentation
   -  `SW-1382 <https://0xdata.atlassian.net/browse/SW-1382>`__ - Remove extra quote in exception on ExternalH2OBackend
   -  `SW-1383 <https://0xdata.atlassian.net/browse/SW-1383>`__ - Fix cloud up in external backend manual mode
   -  `SW-1384 <https://0xdata.atlassian.net/browse/SW-1384>`__ - Fix wrong statement in rsparkling documentation
   -  `SW-1390 <https://0xdata.atlassian.net/browse/SW-1390>`__ - Fix NPE when reading modelDetails in Mojo
   -  `SW-1393 <https://0xdata.atlassian.net/browse/SW-1393>`__ - Use Python formatting for Python in secured_flow.rst
   -  `SW-1396 <https://0xdata.atlassian.net/browse/SW-1396>`__ - Fix wrong exception in H2OAutoML sort metric handling
   -  `SW-1397 <https://0xdata.atlassian.net/browse/SW-1397>`__ - User setClusterSize instead of deprecated setter in tests
   -  `SW-1400 <https://0xdata.atlassian.net/browse/SW-1400>`__ - Nullability tests in DataFrameConverterTest should use data frames with an explicit schema
   -  `SW-1413 <https://0xdata.atlassian.net/browse/SW-1413>`__ - Use VectorUDT in RowConverter
   -  `SW-1418 <https://0xdata.atlassian.net/browse/SW-1418>`__ - Lower memory requirements in tests
   -  `SW-1439 <https://0xdata.atlassian.net/browse/SW-1439>`__ - [Prototype] Switch to using String value on the setters &amp; getters in the ml API on distribution param
   -  `SW-1441 <https://0xdata.atlassian.net/browse/SW-1441>`__ - PySparkling can&#39;t be started after version change using pysparkling.sh
   -  `SW-1447 <https://0xdata.atlassian.net/browse/SW-1447>`__ - Remove missingValuesHandling param from XGBoost wrapper
   -  `SW-1454 <https://0xdata.atlassian.net/browse/SW-1454>`__ - It is no longer possible to specify predictionCol :(
   -  `SW-1462 <https://0xdata.atlassian.net/browse/SW-1462>`__ - convertInvalidNumbersToNa missing on PySparkling
   -  `SW-1463 <https://0xdata.atlassian.net/browse/SW-1463>`__ - Fix setters which accept both int and float 
   -  `SW-1464 <https://0xdata.atlassian.net/browse/SW-1464>`__ - Fix nullableArrayArray param for pyspakrling
   -  `SW-1468 <https://0xdata.atlassian.net/browse/SW-1468>`__ - Use absolute imports as the relative ones are removed in python3
   -  `SW-1470 <https://0xdata.atlassian.net/browse/SW-1470>`__ - DatasetWrapper should use withColumn insteadOf withColumns method
   -  `SW-1472 <https://0xdata.atlassian.net/browse/SW-1472>`__ - Fix tests after modifying allStringsToCategorical
                
-  New Feature
        
   -  `SW-1425 <https://0xdata.atlassian.net/browse/SW-1425>`__ - Add Target Encoding to Sparkling Water Python API
   -  `SW-1446 <https://0xdata.atlassian.net/browse/SW-1446>`__ - Implement H2OKmeans pipeline wrapper
   -  `SW-1455 <https://0xdata.atlassian.net/browse/SW-1455>`__ - Introduce NullableDoubleArrayParam for KMeans
   -  `SW-1456 <https://0xdata.atlassian.net/browse/SW-1456>`__ - Documentation of Target Encoder
                
-  Task
        
   -  `SW-1294 <https://0xdata.atlassian.net/browse/SW-1294>`__ - Benchmarks: Infrastructure for Getting Information about Execution Details
                
-  Improvement
        
   -  `SW-1207 <https://0xdata.atlassian.net/browse/SW-1207>`__ - Add Target Encoding to Sparkling Water Scala API
   -  `SW-1344 <https://0xdata.atlassian.net/browse/SW-1344>`__ - Unify ml package accross rel branches
   -  `SW-1351 <https://0xdata.atlassian.net/browse/SW-1351>`__ - Unify jenkins scripts &amp; create gradle profiles
   -  `SW-1375 <https://0xdata.atlassian.net/browse/SW-1375>`__ - Single execution path for all spark-&gt;h2o frame conversions
   -  `SW-1387 <https://0xdata.atlassian.net/browse/SW-1387>`__ - Handle vectors in SparkDataFrameConverter more explicitly
   -  `SW-1388 <https://0xdata.atlassian.net/browse/SW-1388>`__ - Specify spark specific source dir per project, so they can differ in subprojects
   -  `SW-1392 <https://0xdata.atlassian.net/browse/SW-1392>`__ - Document an example of training AutoML model
   -  `SW-1394 <https://0xdata.atlassian.net/browse/SW-1394>`__ - Modify sw_xgboost.rst to use tabs for Python and Scala code
   -  `SW-1395 <https://0xdata.atlassian.net/browse/SW-1395>`__ - ML Code simplifications &amp; improvements
   -  `SW-1402 <https://0xdata.atlassian.net/browse/SW-1402>`__ - [MAJOR_RELESE] Remove deprecated methods
   -  `SW-1412 <https://0xdata.atlassian.net/browse/SW-1412>`__ - Integrate generic conversion logic to data frame conversion to H2O frames
   -  `SW-1417 <https://0xdata.atlassian.net/browse/SW-1417>`__ - Improve SNAPSHOT handling
   -  `SW-1419 <https://0xdata.atlassian.net/browse/SW-1419>`__ - Jenkins file improvements -&gt; publish nihhtly only if both External &amp; internal test pass for all Spark versions
   -  `SW-1421 <https://0xdata.atlassian.net/browse/SW-1421>`__ - Upgrade to H2O 3.26.0.1
   -  `SW-1422 <https://0xdata.atlassian.net/browse/SW-1422>`__ - Switch to one version of Sparkling Water
   -  `SW-1424 <https://0xdata.atlassian.net/browse/SW-1424>`__ - Upgrade to H2O 3.26.0.2
   -  `SW-1430 <https://0xdata.atlassian.net/browse/SW-1430>`__ - Use downloadLogs method from H2O and remove relevant methods on Sparkling Water side
   -  `SW-1436 <https://0xdata.atlassian.net/browse/SW-1436>`__ - Fix warninig in python package as SW version no longer starts with spark major version
   -  `SW-1437 <https://0xdata.atlassian.net/browse/SW-1437>`__ - Remove duplicate spark version specifier on pysparkling 
   -  `SW-1442 <https://0xdata.atlassian.net/browse/SW-1442>`__ - Update build SW doc
   -  `SW-1443 <https://0xdata.atlassian.net/browse/SW-1443>`__ - Ignore local-cluster failing tests
   -  `SW-1444 <https://0xdata.atlassian.net/browse/SW-1444>`__ - Use string representations instead of enums on Pipeline API
   -  `SW-1448 <https://0xdata.atlassian.net/browse/SW-1448>`__ - Refactor parameters into supervised &amp; unsupervised
   -  `SW-1449 <https://0xdata.atlassian.net/browse/SW-1449>`__ - Create Supervised &amp; Unsupervised Algorithm
   -  `SW-1451 <https://0xdata.atlassian.net/browse/SW-1451>`__ - Document H2OKmeans pipeline wrapper
   -  `SW-1452 <https://0xdata.atlassian.net/browse/SW-1452>`__ - Refactor params to supervised and unsupervised on PySparklin side
   -  `SW-1453 <https://0xdata.atlassian.net/browse/SW-1453>`__ - Put back constructor checks for Enums on PySparkling side ( accidentally removed)
   -  `SW-1473 <https://0xdata.atlassian.net/browse/SW-1473>`__ - Rename H2OTargetEncoderMojoModel to H2OTargetEncoderMOJOModel
                
-  Engineering Story
        
   -  `SW-1378 <https://0xdata.atlassian.net/browse/SW-1378>`__ - Integration test for flattening logic
   -  `SW-1386 <https://0xdata.atlassian.net/browse/SW-1386>`__ - Micro benchmark for conversion from a DataFrame to H2OFrame 
   -  `SW-1404 <https://0xdata.atlassian.net/browse/SW-1404>`__ - Unification of creating header page across different spark versions
   -  `SW-1457 <https://0xdata.atlassian.net/browse/SW-1457>`__ - Test passing params to pipeline wrappers of H2O Algos
   -  `SW-1458 <https://0xdata.atlassian.net/browse/SW-1458>`__ - No longer need to H2OContext.getOrCreate ini __init__ methods of pysparkling algo wrappers
   -  `SW-1459 <https://0xdata.atlassian.net/browse/SW-1459>`__ - Avoid duplicating MojoParams on PySparkling side
   -  `SW-1460 <https://0xdata.atlassian.net/browse/SW-1460>`__ - Infrastructure for prediction column with a simple prediction value
   -  `SW-1461 <https://0xdata.atlassian.net/browse/SW-1461>`__ - prepare ai.h2o.sparkling structure on PySpakrling side
   -  `SW-1466 <https://0xdata.atlassian.net/browse/SW-1466>`__ - Move logic for converting columns to categorical to prepareDatasetForFitting method
                
                            
v2.1.56, v2.2.42, v2.3.31, v2.4.13 (2019-06-24)
-----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/13/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/13/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/31/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/31/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/42/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/42/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/56/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/56/index.html>`__

-  Bug

   -  `SW-1140 <https://0xdata.atlassian.net/browse/SW-1140>`__ - Add more logging to discover intermittent RSparkling Issue in jenkins tests
   -  `SW-1318 <https://0xdata.atlassian.net/browse/SW-1318>`__ - add back to JavaH2OContext method asDataFrame(.., SQLContext) but deprecated
   -  `SW-1321 <https://0xdata.atlassian.net/browse/SW-1321>`__ - Remove mention of H2O UDP from user documentation
   -  `SW-1322 <https://0xdata.atlassian.net/browse/SW-1322>`__ - Fix wrong doc in ssl.rst -&gt; val conf: H2OConf = // generate H2OConf file
   -  `SW-1323 <https://0xdata.atlassian.net/browse/SW-1323>`__ - Model ID not available on our algo pipeline wrappers
   -  `SW-1338 <https://0xdata.atlassian.net/browse/SW-1338>`__ - Follow up fixes after RSparkling change
   -  `SW-1339 <https://0xdata.atlassian.net/browse/SW-1339>`__ - Use s3-cli instead of s3cmd because of performance reasons on nightlies
   -  `SW-1340 <https://0xdata.atlassian.net/browse/SW-1340>`__ - Fix spinx warning
   -  `SW-1342 <https://0xdata.atlassian.net/browse/SW-1342>`__ - Fix dist
   -  `SW-1343 <https://0xdata.atlassian.net/browse/SW-1343>`__ - Fix dist structure
   -  `SW-1345 <https://0xdata.atlassian.net/browse/SW-1345>`__ - Fix missing rsparkling in dist package
   -  `SW-1347 <https://0xdata.atlassian.net/browse/SW-1347>`__ - Scaladoc not uploaded to S3 after porting make-dist to gradle
   -  `SW-1359 <https://0xdata.atlassian.net/browse/SW-1359>`__ - Fix wrong links on nightly build page
   -  `SW-1360 <https://0xdata.atlassian.net/browse/SW-1360>`__ - Explicitly send hearbeat after we have complete flatfile
   -  `SW-1361 <https://0xdata.atlassian.net/browse/SW-1361>`__ - sparkling water package on maven should assembly jar
   -  `SW-1362 <https://0xdata.atlassian.net/browse/SW-1362>`__ - gradle.properties in distribution contains wrong version
   -  `SW-1364 <https://0xdata.atlassian.net/browse/SW-1364>`__ - Rename SVM to SparkSVM
   -  `SW-1374 <https://0xdata.atlassian.net/browse/SW-1374>`__ - Minor documentation fixes

-  New Feature

   -  `SW-1021 <https://0xdata.atlassian.net/browse/SW-1021>`__ - Upload RSparkling to S3 in a form of R repository
   -  `SW-1353 <https://0xdata.atlassian.net/browse/SW-1353>`__ - Introduce logic flatting data frames with arbitrarily nested structures

-  Improvement

   -  `SW-554 <https://0xdata.atlassian.net/browse/SW-554>`__ - Include all used dependency licenses in the uber jar.
   -  `SW-1308 <https://0xdata.atlassian.net/browse/SW-1308>`__ - Bundle Sparkling Water jar into rsparkling -&gt; making rsparkling version dependent on specific sparkling water
   -  `SW-1317 <https://0xdata.atlassian.net/browse/SW-1317>`__ - Unify repl acros different rel branches
   -  `SW-1325 <https://0xdata.atlassian.net/browse/SW-1325>`__ - Expose jks_alias in Sparkling Water
   -  `SW-1326 <https://0xdata.atlassian.net/browse/SW-1326>`__ - Include SW version in more log statements
   -  `SW-1330 <https://0xdata.atlassian.net/browse/SW-1330>`__ - Add additional log to H2O cloudup in internal backend mode
   -  `SW-1331 <https://0xdata.atlassian.net/browse/SW-1331>`__ - Create local repo with RSparkling
   -  `SW-1332 <https://0xdata.atlassian.net/browse/SW-1332>`__ - [RSparkling] Make installation from S3 the default recommended option
   -  `SW-1333 <https://0xdata.atlassian.net/browse/SW-1333>`__ - Move the conversion logic from Spark Row to H2O RowData to a separate entity
   -  `SW-1334 <https://0xdata.atlassian.net/browse/SW-1334>`__ - Store H2O models in transient lazy variables of  SW Mojo models
   -  `SW-1335 <https://0xdata.atlassian.net/browse/SW-1335>`__ - Make automl tests more deterministic by using max_models instead of max_runtime_secs
   -  `SW-1341 <https://0xdata.atlassian.net/browse/SW-1341>`__ - Use readme as main dispatch for documentation
   -  `SW-1346 <https://0xdata.atlassian.net/browse/SW-1346>`__ - Remove chache and unpersist call in SpreadRDDBuilder
   -  `SW-1348 <https://0xdata.atlassian.net/browse/SW-1348>`__ - Switch to s3 cli on release pipelines
   -  `SW-1349 <https://0xdata.atlassian.net/browse/SW-1349>`__ - Use withColumn instead of select in MOJO models
   -  `SW-1350 <https://0xdata.atlassian.net/browse/SW-1350>`__ - Fix links to doc &amp; scaladoc on nightly builds
   -  `SW-1352 <https://0xdata.atlassian.net/browse/SW-1352>`__ - Upgrade H2O to 3.24.0.5
   -  `SW-1365 <https://0xdata.atlassian.net/browse/SW-1365>`__ - Run only last build in jenkins
   -  `SW-1369 <https://0xdata.atlassian.net/browse/SW-1369>`__ - Download page is missing one step on RSparkling tab -&gt; library(rsparkling)
   -  `SW-1371 <https://0xdata.atlassian.net/browse/SW-1371>`__ - Create maven repo on our s3 for each release and nightly
   -  `SW-1373 <https://0xdata.atlassian.net/browse/SW-1373>`__ - Update DBC documentation with respoect to latest RSparkling development


v2.1.55, v2.2.41, v2.3.30, v2.4.12 (2019-06-03)
-----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/12/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/12/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/30/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/30/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/41/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/41/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/55/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/55/index.html>`__

-  Bug

   -  `SW-1259 <https://0xdata.atlassian.net/browse/SW-1259>`__ - Unify ratio param across pipeline api
   -  `SW-1287 <https://0xdata.atlassian.net/browse/SW-1287>`__ - Use RPC endpoints to orchestrate cloud in internal mode
   -  `SW-1290 <https://0xdata.atlassian.net/browse/SW-1290>`__ - Fix doc
   -  `SW-1301 <https://0xdata.atlassian.net/browse/SW-1301>`__ - Fix class-loading for Sparkling Water assembly JAR in PySparkling
   -  `SW-1311 <https://0xdata.atlassian.net/browse/SW-1311>`__ - Add numpy as PySparkling dependency ( it is required because of Spark but missing from list of dependencies)
   -  `SW-1312 <https://0xdata.atlassian.net/browse/SW-1312>`__ - Warn that default value of convertUnknownCategoricalLevelsToNa will be changed to false on GridSearch &amp; AutoML
   -  `SW-1316 <https://0xdata.atlassian.net/browse/SW-1316>`__ - Fix wrong fat jar name

-  Task

   -  `SW-1292 <https://0xdata.atlassian.net/browse/SW-1292>`__ - Benchmarks: Subproject Skeleton

-  Improvement

   -  `SW-1212 <https://0xdata.atlassian.net/browse/SW-1212>`__ - Make sure python zip/wheel is downloadable from our release s3
   -  `SW-1274 <https://0xdata.atlassian.net/browse/SW-1274>`__ - On download page -&gt; list all supported minor versions
   -  `SW-1286 <https://0xdata.atlassian.net/browse/SW-1286>`__ - Remove Param propagation of MOJOModels from Python to Java
   -  `SW-1288 <https://0xdata.atlassian.net/browse/SW-1288>`__ - H2OCommonParams in pysparkling
   -  `SW-1289 <https://0xdata.atlassian.net/browse/SW-1289>`__ - Move shared params to H2OCommonParams
   -  `SW-1298 <https://0xdata.atlassian.net/browse/SW-1298>`__ - Don&#39;t use deprecated methods
   -  `SW-1299 <https://0xdata.atlassian.net/browse/SW-1299>`__ - Warn user that default value of predictionCol on H2OMOJOModel will change in the next major release to  &#39;prediction&#39;
   -  `SW-1300 <https://0xdata.atlassian.net/browse/SW-1300>`__ - Upgrade to H2O 3.24.0.4
   -  `SW-1304 <https://0xdata.atlassian.net/browse/SW-1304>`__ - Definition of assembly jar via transitive exclusions
   -  `SW-1305 <https://0xdata.atlassian.net/browse/SW-1305>`__ - Move ability to change behavior of MOJO models to MOJOLoader
   -  `SW-1306 <https://0xdata.atlassian.net/browse/SW-1306>`__ - Move make-dist logic to gradle
   -  `SW-1307 <https://0xdata.atlassian.net/browse/SW-1307>`__ - Expose binary model in spark pipeline stage
   -  `SW-1309 <https://0xdata.atlassian.net/browse/SW-1309>`__ - Fix xgboost doc
   -  `SW-1313 <https://0xdata.atlassian.net/browse/SW-1313>`__ - Rename the &#39;create_from_mojo&#39; method of H2OMOJOModel and H2OMOJOPipelineModel to &#39;createFromMojo&#39;


v2.1.54, v2.2.40, v2.3.29, v2.4.11 (2019-05-17)
-----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/11/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/11/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/29/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/29/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/40/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/40/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/54/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/54/index.html>`__

-  Bug

   -  `SW-1256 <https://0xdata.atlassian.net/browse/SW-1256>`__ - Fix constructor of H2OMojoModel
   -  `SW-1258 <https://0xdata.atlassian.net/browse/SW-1258>`__ - Remove internal constructors &amp; Deprecate implicit constructor parameters for H2O Algo Spark Estimators( to be the same as in PySparkling)
   -  `SW-1270 <https://0xdata.atlassian.net/browse/SW-1270>`__ - Fix version check in PySpakrling shell
   -  `SW-1278 <https://0xdata.atlassian.net/browse/SW-1278>`__ - Clean workspace on the hadoop node in integ tests
   -  `SW-1279 <https://0xdata.atlassian.net/browse/SW-1279>`__ - Fix inconsistencies between H2OAutoML, H2OGridSearch &amp; H2OALgorithm
   -  `SW-1281 <https://0xdata.atlassian.net/browse/SW-1281>`__ - Fix bad representation of predictionCol on H2OMOJOModel
   -  `SW-1282 <https://0xdata.atlassian.net/browse/SW-1282>`__ - XGBoost can&#39;t be used in H2OGridSearch pipeline wrapper
   -  `SW-1283 <https://0xdata.atlassian.net/browse/SW-1283>`__ - Correctly return mojo model in pysparkling after fit

-  Story

   -  `SW-1271 <https://0xdata.atlassian.net/browse/SW-1271>`__ - Remove SparkContext from H2OSchemaUtils
   -  `SW-1273 <https://0xdata.atlassian.net/browse/SW-1273>`__ - Upgrade to H2O 3.24.0.3

-  New Feature

   -  `SW-1248 <https://0xdata.atlassian.net/browse/SW-1248>`__ - getFeaturesCols() should not return the fold column or weight column
   -  `SW-1249 <https://0xdata.atlassian.net/browse/SW-1249>`__ - probability calibration does not work in Sparkling Water Dataframe API

-  Improvement

   -  `SW-369 <https://0xdata.atlassian.net/browse/SW-369>`__ - Override spark locality so we use only nodes on which h2o is running.
   -  `SW-1216 <https://0xdata.atlassian.net/browse/SW-1216>`__ - Improve PySparkling README
   -  `SW-1261 <https://0xdata.atlassian.net/browse/SW-1261>`__ - Remove binary H2O model from ML pipelines
   -  `SW-1263 <https://0xdata.atlassian.net/browse/SW-1263>`__ - Don&#39;t require initializer call to be called during pysparkling pipelines
   -  `SW-1264 <https://0xdata.atlassian.net/browse/SW-1264>`__ - Use default params reader in pipelines
   -  `SW-1268 <https://0xdata.atlassian.net/browse/SW-1268>`__ - Non-named columns are long time deprecated. Switch to named columns by default
   -  `SW-1269 <https://0xdata.atlassian.net/browse/SW-1269>`__ - Remove six as dependency from PySparkling launcher ( six is no longer dependency)
   -  `SW-1275 <https://0xdata.atlassian.net/browse/SW-1275>`__ - Remove unnecessary constructor in helper class
   -  `SW-1280 <https://0xdata.atlassian.net/browse/SW-1280>`__ - Add predictionCol to mojo pipeline model


v2.1.53, v2.2.39, v2.3.28, v2.4.10 (2019-04-26)
-----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/10/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/28/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/28/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/39/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/39/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/53/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/53/index.html>`__

-  Bug

   -  `SW-1189 <https://0xdata.atlassian.net/browse/SW-1189>`__ - Fix Sparkling Water 2.1.x compile on Scala 2.10
   -  `SW-1194 <https://0xdata.atlassian.net/browse/SW-1194>`__ - RSparkling Can&#39;t be used on Spark 2.4
   -  `SW-1195 <https://0xdata.atlassian.net/browse/SW-1195>`__ - Disable gradle daemon via gradle.properties
   -  `SW-1196 <https://0xdata.atlassian.net/browse/SW-1196>`__ - Fix org.apache.spark.ml.spark.models.PipelinePredictionTest
   -  `SW-1203 <https://0xdata.atlassian.net/browse/SW-1203>`__ - Custom metric not evaluated in internal mode of Sparkling Water
   -  `SW-1227 <https://0xdata.atlassian.net/browse/SW-1227>`__ - Change get-extended-jar to use https instead of http
   -  `SW-1230 <https://0xdata.atlassian.net/browse/SW-1230>`__ - Fix typo in GLM API - getRemoteCollinearColumns, setRemoteCollinearColumns
   -  `SW-1232 <https://0xdata.atlassian.net/browse/SW-1232>`__ - Fix RUnits after upgrading to Gradle 5.3.1
   -  `SW-1234 <https://0xdata.atlassian.net/browse/SW-1234>`__ - Deprecate asDataFrame with implicit argument

-  Story

   -  `SW-1198 <https://0xdata.atlassian.net/browse/SW-1198>`__ - Introduce new annotation deprecating legacy methods in API
   -  `SW-1209 <https://0xdata.atlassian.net/browse/SW-1209>`__ - Rename the &#39;predictionCol&#39; model parameter to &#39;labelCol&#39;
   -  `SW-1226 <https://0xdata.atlassian.net/browse/SW-1226>`__ - Introduce mechanism for enabling backward compatibility of MOJO files when properties are renamed

-  New Feature

   -  `SW-1193 <https://0xdata.atlassian.net/browse/SW-1193>`__ - Expose weights_column parameter

-  Improvement

   -  `SW-1188 <https://0xdata.atlassian.net/browse/SW-1188>`__ - RSparkling: Add ability to add authentication details when calling h2o_context(sc)
   -  `SW-1190 <https://0xdata.atlassian.net/browse/SW-1190>`__ - Improve hint description for disabling automatic usage of broadcast joins
   -  `SW-1199 <https://0xdata.atlassian.net/browse/SW-1199>`__ - Improve memory efficiency of H2OMOJOPipelineModel
   -  `SW-1202 <https://0xdata.atlassian.net/browse/SW-1202>`__ - Simplify Sparkling Water build
   -  `SW-1204 <https://0xdata.atlassian.net/browse/SW-1204>`__ - Fix formating in python tests
   -  `SW-1208 <https://0xdata.atlassian.net/browse/SW-1208>`__ - Create pysparkling tests report file if it does not exist
   -  `SW-1210 <https://0xdata.atlassian.net/browse/SW-1210>`__ - Add fold column to python and scala pipelines
   -  `SW-1211 <https://0xdata.atlassian.net/browse/SW-1211>`__ - Automatically download H2O Wheel
   -  `SW-1213 <https://0xdata.atlassian.net/browse/SW-1213>`__ - Upgrade to H2O 3.24.0.2
   -  `SW-1214 <https://0xdata.atlassian.net/browse/SW-1214>`__ - Remove PySparkling six dependency as it was removed in H2O
   -  `SW-1215 <https://0xdata.atlassian.net/browse/SW-1215>`__ - Automatically generate PySparkling README
   -  `SW-1217 <https://0xdata.atlassian.net/browse/SW-1217>`__ - Automatically generate last pieces of doc subproject
   -  `SW-1219 <https://0xdata.atlassian.net/browse/SW-1219>`__ - Remove suport for testing external cluster in manual mode
   -  `SW-1221 <https://0xdata.atlassian.net/browse/SW-1221>`__ - Remove unnecessary branch check
   -  `SW-1222 <https://0xdata.atlassian.net/browse/SW-1222>`__ - Remove duplicate readme file (contains old info &amp; the correct info is in doc)
   -  `SW-1223 <https://0xdata.atlassian.net/browse/SW-1223>`__ - Remove confusing meetup dir
   -  `SW-1224 <https://0xdata.atlassian.net/browse/SW-1224>`__ - Upgrade to Gradle 5.3.1
   -  `SW-1228 <https://0xdata.atlassian.net/browse/SW-1228>`__ - Rename the &#39;ignoredColumns&#39; parameter of H2OAutoML to &#39;ignoredCols&#39;
   -  `SW-1229 <https://0xdata.atlassian.net/browse/SW-1229>`__ - Remove dependencies to Scala 2.10
   -  `SW-1235 <https://0xdata.atlassian.net/browse/SW-1235>`__ - Remove support for Python 2.6 on rel-2.1
   -  `SW-1236 <https://0xdata.atlassian.net/browse/SW-1236>`__ - Reformat few python classes
   -  `SW-1238 <https://0xdata.atlassian.net/browse/SW-1238>`__ - Parametrize EMR version in templates generation
   -  `SW-1239 <https://0xdata.atlassian.net/browse/SW-1239>`__ - Remove old README and DEVEL doc files (not just pointer to new doc)
   -  `SW-1240 <https://0xdata.atlassian.net/browse/SW-1240>`__ - Use minSupportedJava for source and target compatibility in build.gradle


v2.1.52, v2.2.38, v2.3.27, v2.4.9 (2019-04-03)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/9/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/27/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/27/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/38/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/38/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/52/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/52/index.html>`__

-  Bug

   -  `SW-1162 <https://0xdata.atlassian.net/browse/SW-1162>`__ - Exception when there is a column with BOOLEAN type in dataset during H2OMOJOModel transformation
   -  `SW-1177 <https://0xdata.atlassian.net/browse/SW-1177>`__ - In Pysparkling script, setting --driver-class-path influences the environment
   -  `SW-1178 <https://0xdata.atlassian.net/browse/SW-1178>`__ - Upgrade to h2O 3.24.0.1
   -  `SW-1180 <https://0xdata.atlassian.net/browse/SW-1180>`__ - Use specific metrics in grid search, in the same way as H2O Grid
   -  `SW-1181 <https://0xdata.atlassian.net/browse/SW-1181>`__ - Document off heap memory configuration for Spark in Standalone mode/IBM conductor
   -  `SW-1182 <https://0xdata.atlassian.net/browse/SW-1182>`__ - Fix random project name generation in H2OAutoML Spark Wrapper

-  New Feature

   -  `SW-1167 <https://0xdata.atlassian.net/browse/SW-1167>`__ - Expose *search_criteria* for H2OGridSearch
   -  `SW-1174 <https://0xdata.atlassian.net/browse/SW-1174>`__ - expose H2OGridSearch models
   -  `SW-1183 <https://0xdata.atlassian.net/browse/SW-1183>`__ - Add includeAlgos to H2o AutoML pipeline stage &amp; ability to ignore XGBoost

-  Improvement

   -  `SW-1164 <https://0xdata.atlassian.net/browse/SW-1164>`__ - Add Sparkling Water to Jupyter spark/pyspark kernels in EMR terraform template
   -  `SW-1171 <https://0xdata.atlassian.net/browse/SW-1171>`__ - Upgrade build to Gradle 5.2.1
   -  `SW-1175 <https://0xdata.atlassian.net/browse/SW-1175>`__ - Integrate with H2O native hive support


v2.1.51, v2.2.37, v2.3.26, v2.4.8 (2019-03-15)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/8/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/26/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/26/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/37/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/37/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/51/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/51/index.html>`__

-  Bug

   -  `SW-1163 <https://0xdata.atlassian.net/browse/SW-1163>`__ - Expose missing variables in shared TF EMR SW tamplate

-  Improvement

   -  `SW-1145 <https://0xdata.atlassian.net/browse/SW-1145>`__ - Start jupyter notebook with Scala &amp; Python Spark in AWS EMR Terraform template
   -  `SW-1165 <https://0xdata.atlassian.net/browse/SW-1165>`__ - Upgrade to H2O 3.22.1.6


v2.1.50, v2.2.36, v2.3.25, v2.4.7 (2019-03-07)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/7/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/25/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/25/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/36/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/36/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/50/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/50/index.html>`__

-  Bug

   -  `SW-1150 <https://0xdata.atlassian.net/browse/SW-1150>`__ - hc.stop() shows &#39;exit&#39; not defined error
   -  `SW-1152 <https://0xdata.atlassian.net/browse/SW-1152>`__ - Fix RSparkling in case the jars are being fetched from maven
   -  `SW-1156 <https://0xdata.atlassian.net/browse/SW-1156>`__ - H2OXgboost pipeline stage does not define updateH2OParams method
   -  `SW-1159 <https://0xdata.atlassian.net/browse/SW-1159>`__ - Unique project name in automl to avoid sharing one leaderboard
   -  `SW-1161 <https://0xdata.atlassian.net/browse/SW-1161>`__ - Fix grid search pipeline step on pyspark side

-  Improvement

   -  `SW-1052 <https://0xdata.atlassian.net/browse/SW-1052>`__ - Document teraform scripts for AWS
   -  `SW-1089 <https://0xdata.atlassian.net/browse/SW-1089>`__ - Document using Google Cloud Storage In Sparkling Water
   -  `SW-1135 <https://0xdata.atlassian.net/browse/SW-1135>`__ - Speed up conversion between sparse spark vectors  and h2o frames by using sparse new chunk
   -  `SW-1141 <https://0xdata.atlassian.net/browse/SW-1141>`__ - Improve terraform templates for AWS EMR and make them part of the release process
   -  `SW-1149 <https://0xdata.atlassian.net/browse/SW-1149>`__ - Allow login via ssh to created cluster using terraform
   -  `SW-1153 <https://0xdata.atlassian.net/browse/SW-1153>`__ - Add H2OGridSearch pipeline stage to PySpark
   -  `SW-1155 <https://0xdata.atlassian.net/browse/SW-1155>`__ - Test GBM Grid Search Scala pipeline step
   -  `SW-1158 <https://0xdata.atlassian.net/browse/SW-1158>`__ - Generalize H2OGridSearch Pipeline step to support other available algos
   -  `SW-1160 <https://0xdata.atlassian.net/browse/SW-1160>`__ - Upgrade to H2O 3.22.1.5


v2.1.49, v2.2.35, v2.3.24, v2.4.6 (2019-02-18)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/6/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/6/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/24/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/24/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/35/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/35/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/49/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/49/index.html>`__

-  Bug

   -  `SW-1136 <https://0xdata.atlassian.net/browse/SW-1136>`__ - Fix bug affecting loading pipeline in python when stored in scala
   -  `SW-1138 <https://0xdata.atlassian.net/browse/SW-1138>`__ - Fix several cases in spark vector -&gt; h2o conversion

-  Improvement

   -  `SW-1134 <https://0xdata.atlassian.net/browse/SW-1134>`__ - Add H2OGLM Wrapper to Sparkling Water
   -  `SW-1139 <https://0xdata.atlassian.net/browse/SW-1139>`__ - Update mojo2 to 0.3.16
   -  `SW-1143 <https://0xdata.atlassian.net/browse/SW-1143>`__ - Fix s3 bootstrap templates for nightly builds
   -  `SW-1144 <https://0xdata.atlassian.net/browse/SW-1144>`__ - Upgrade to H2O 3.22.1.4


v2.1.48, v2.2.34, v2.3.23, v2.4.5 (2019-01-29)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/5/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/5/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/23/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/23/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/34/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/34/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/48/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/48/index.html>`__

-  Bug

   -  `SW-1133 <https://0xdata.atlassian.net/browse/SW-1133>`__ - Upgrade to H2O 3.22.1.3


v2.1.47, v2.2.33, v2.3.22, v2.4.4 (2019-01-21)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/4/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/22/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/22/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/33/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/33/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/47/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/47/index.html>`__

-  Bug

   -  `SW-1129 <https://0xdata.atlassian.net/browse/SW-1129>`__ - Fix support for unsupervised mojo models

-  Improvement

   -  `SW-1101 <https://0xdata.atlassian.net/browse/SW-1101>`__ - Update code to work with latest jetty changes
   -  `SW-1127 <https://0xdata.atlassian.net/browse/SW-1127>`__ - Upgrade H2O to 3.22.1.2


v2.1.46, v2.2.32, v2.3.21, v2.4.3 (2019-01-17)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/3/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/21/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/21/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/32/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/32/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/46/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/46/index.html>`__

-  Bug

   -  `SW-1116 <https://0xdata.atlassian.net/browse/SW-1116>`__ - Cannot serialize DAI model

-  Improvement

   -  `SW-1113 <https://0xdata.atlassian.net/browse/SW-1113>`__ - Update to H2O 3.22.0.5
   -  `SW-1115 <https://0xdata.atlassian.net/browse/SW-1115>`__ - Enable tabs in the documentation based on the language
   -  `SW-1120 <https://0xdata.atlassian.net/browse/SW-1120>`__ - Prepare Terraform scripts for Sparkling Water on EMR
   -  `SW-1121 <https://0xdata.atlassian.net/browse/SW-1121>`__ - Use getTimestamp method instead of _timestamp directly


v2.1.45, v2.2.31, v2.3.20, v2.4.2 (2019-01-08)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/2/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/20/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/20/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/31/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/31/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/45/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/45/index.html>`__

-  Bug

   -  `SW-1107 <https://0xdata.atlassian.net/browse/SW-1107>`__ - NullPointerException at water.H2ONode.openChan(H2ONode.java:417) after upgrade to H2O 3.22.0.3
   -  `SW-1110 <https://0xdata.atlassian.net/browse/SW-1110>`__ - Fix test suite to test PySparkling YARN integration tests on external backend as well

-  Task

   -  `SW-1109 <https://0xdata.atlassian.net/browse/SW-1109>`__ - Docs: Change copyright year in docs to include 2019

-  Improvement

   -  `SW-464 <https://0xdata.atlassian.net/browse/SW-464>`__ - Publish PySparkling as conda package
   -  `SW-1111 <https://0xdata.atlassian.net/browse/SW-1111>`__ - Update H2O to 3.22.0.4


v2.1.44, v2.2.30, v2.3.19, v2.4.1 (2018-12-27)
----------------------------------------------
Downloads:

   - for Spark 2.4: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/1/index.html>`__
   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/19/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/19/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/30/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/30/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/44/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/44/index.html>`__

-  Bug

   -  `SW-1084 <https://0xdata.atlassian.net/browse/SW-1084>`__ - Documentation link does not work on the Nightly Bleeding Edge download page
   -  `SW-1100 <https://0xdata.atlassian.net/browse/SW-1100>`__ - Fix Travis builds
   -  `SW-1102 <https://0xdata.atlassian.net/browse/SW-1102>`__ - Fix Travis builds (test just scala unit tests)

-  Improvement

   -  `SW-464 <https://0xdata.atlassian.net/browse/SW-464>`__ - Publish PySparkling as conda package
   -  `SW-1080 <https://0xdata.atlassian.net/browse/SW-1080>`__ - Fix deprecation warning regarding automl -&gt; AutoML
   -  `SW-1092 <https://0xdata.atlassian.net/browse/SW-1092>`__ - Updates to streaming app
   -  `SW-1093 <https://0xdata.atlassian.net/browse/SW-1093>`__ - Update to H2O 3.22.0.3
   -  `SW-1094 <https://0xdata.atlassian.net/browse/SW-1094>`__ - Upgrade gradle to 4.10.3
   -  `SW-1095 <https://0xdata.atlassian.net/browse/SW-1095>`__ - Enable GCS in Sparkling Water
   -  `SW-1097 <https://0xdata.atlassian.net/browse/SW-1097>`__ - Properly integrate GCS with Sparkling Water, including test in PySparkling

-  Docs

   -  `SW-1083 <https://0xdata.atlassian.net/browse/SW-1083>`__ - Add Installation and Starting instructions to the docs


v2.1.43, v2.2.29, v2.3.18 (2018-11-27)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/18/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/18/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/29/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/29/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/43/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/43/index.html>`__

-  Improvement

   -  `SW-1078 <https://0xdata.atlassian.net/browse/SW-1078>`__ - Upgrade H2O to 3.22.0.2


v2.1.42, v2.2.28, v2.3.17 (2018-10-27)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/17/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/17/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/28/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/28/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/42/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/42/index.html>`__

-  Bug

   -  `SW-1071 <https://0xdata.atlassian.net/browse/SW-1071>`__ - Fallback to original IP discovery in case we can&#39;t find the same network
   -  `SW-1072 <https://0xdata.atlassian.net/browse/SW-1072>`__ - Fix handling time column for mojo pipeline
   -  `SW-1073 <https://0xdata.atlassian.net/browse/SW-1073>`__ - Upgrade MOJO to 0.3.17

-  Improvement

   -  `SW-1045 <https://0xdata.atlassian.net/browse/SW-1045>`__ - Upgrade H2O to 3.22.0.1


v2.1.41, v2.2.27, v2.3.16 (2018-10-17)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/16/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/16/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/27/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/27/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/41/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/41/index.html>`__

-  Bug

   -  `SW-930 <https://0xdata.atlassian.net/browse/SW-930>`__ - Enable AutoML tests in Sparkling Water
   -  `SW-1065 <https://0xdata.atlassian.net/browse/SW-1065>`__ - Fix isssue with empty queue name by default
   -  `SW-1066 <https://0xdata.atlassian.net/browse/SW-1066>`__ - In PySparkling, don&#39;t reconnect if already connected
   -  `SW-1068 <https://0xdata.atlassian.net/browse/SW-1068>`__ - Fix warning in doc

-  Improvement

   -  `SW-1057 <https://0xdata.atlassian.net/browse/SW-1057>`__ - Sparkling shell ignores parameters after last updates
   -  `SW-1058 <https://0xdata.atlassian.net/browse/SW-1058>`__ - Automatic detection of client ip in external backend
   -  `SW-1059 <https://0xdata.atlassian.net/browse/SW-1059>`__ - Pysparkling in external backend, manual mode stops the backend cluster, but the cluster should be left intact
   -  `SW-1060 <https://0xdata.atlassian.net/browse/SW-1060>`__ - Create nightly release for 2.1, 2.2 and 2.3
   -  `SW-1061 <https://0xdata.atlassian.net/browse/SW-1061>`__ - Upgrade to Mojo 0.3.15
   -  `SW-1062 <https://0xdata.atlassian.net/browse/SW-1062>`__ - Don&#39;t expose mojo internal types
   -  `SW-1063 <https://0xdata.atlassian.net/browse/SW-1063>`__ - More explicit checks for valid values of Backend mode and external backend start mode
   -  `SW-1064 <https://0xdata.atlassian.net/browse/SW-1064>`__ - Expose run_as_user for External H2O Backend
   -  `SW-1069 <https://0xdata.atlassian.net/browse/SW-1069>`__ - Upgrade H2O to 3.20.0.10


v2.1.40, v2.2.26, v2.3.15 (2018-10-02)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/15/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/15/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/26/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/26/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/40/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/40/index.html>`__

-  Bug

   -  `SW-1041 <https://0xdata.atlassian.net/browse/SW-1041>`__ - Fix passing --jars to sparkling-shell
   -  `SW-1042 <https://0xdata.atlassian.net/browse/SW-1042>`__ - More robust check for python package in PySparkling shell
   -  `SW-1048 <https://0xdata.atlassian.net/browse/SW-1048>`__ - Add missing six dependency to setup.py for PySparkling

-  Improvement

   -  `SW-1043 <https://0xdata.atlassian.net/browse/SW-1043>`__ - Mojo pipeline with multiple output columns (and also with dots in the names) does not work in SW
   -  `SW-1054 <https://0xdata.atlassian.net/browse/SW-1054>`__ - Upgrade H2O dependency to 3.20.0.9


v2.1.39, v2.2.25, v2.3.14 (2018-09-24)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/14/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/14/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/25/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/25/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/39/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/39/index.html>`__

-  New Feature

   -  `SW-1020 <https://0xdata.atlassian.net/browse/SW-1020>`__ - Expose leaderboard on H2OAutoML
   -  `SW-1022 <https://0xdata.atlassian.net/browse/SW-1022>`__ - Display Release creation date on the download page

-  Improvement

   -  `SW-1024 <https://0xdata.atlassian.net/browse/SW-1024>`__ - remove call to ./gradlew --help in jenkins pipeline
   -  `SW-1025 <https://0xdata.atlassian.net/browse/SW-1025>`__ - Ensure that release does not depend on build id
   -  `SW-1026 <https://0xdata.atlassian.net/browse/SW-1026>`__ - Automatically update master after RSparkling release with latest version
   -  `SW-1030 <https://0xdata.atlassian.net/browse/SW-1030>`__ - [RSparkling] In case only path to SW jar file is specified, discover the version from JAR file instead of requiring it as parameter
   -  `SW-1031 <https://0xdata.atlassian.net/browse/SW-1031>`__ - Enable installation ot RSparkling using devtools from Github repo
   -  `SW-1032 <https://0xdata.atlassian.net/browse/SW-1032>`__ - Upgrade mojo pipeline to 0.13.2
   -  `SW-1033 <https://0xdata.atlassian.net/browse/SW-1033>`__ - Document automatic certificate creation for Flow UI
   -  `SW-1034 <https://0xdata.atlassian.net/browse/SW-1034>`__ - PySparkling fails if we specify https argument as part of getOrCreate()
   -  `SW-1035 <https://0xdata.atlassian.net/browse/SW-1035>`__ - Document using s3a and s3n on Sparkling Water
   -  `SW-1036 <https://0xdata.atlassian.net/browse/SW-1036>`__ - Upgrade to H2O 3.20.0.8
   -  `SW-1038 <https://0xdata.atlassian.net/browse/SW-1038>`__ - The shell script bin/pysparkling should print missing dependencies
   -  `SW-1039 <https://0xdata.atlassian.net/browse/SW-1039>`__ - Upgrade Gradle to 4.10.2

-  Docs

   -  `SW-1018 <https://0xdata.atlassian.net/browse/SW-1018>`__ - Fix link to Installing RSparkling on Windows


v2.1.38, v2.2.24, v2.3.13 (2018-09-14)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/13/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/13/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/24/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/24/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/38/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/38/index.html>`__

-  New Feature

   -  `SW-1023 <https://0xdata.atlassian.net/browse/SW-1023>`__ - Upgrade Gradle to 4.10.1

-  Improvement

   -  `SW-1019 <https://0xdata.atlassian.net/browse/SW-1019>`__ - Upgrade H2O to 3.20.0.7
   -  `SW-1027 <https://0xdata.atlassian.net/browse/SW-1027>`__ - Revert Upgrade to Gradle 4.10.1(bug in Gradle) and upgrade to Gradle 4.0
   -  `SW-1028 <https://0xdata.atlassian.net/browse/SW-1028>`__ - Update docs and mention that ORC is supported

-  Docs

   -  `SW-1017 <https://0xdata.atlassian.net/browse/SW-1017>`__ - Docs: Add Parquet to list of supported data formats


v2.1.37, v2.2.23, v2.3.12 (2018-08-28)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/12/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/12/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/23/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/23/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/37/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/37/index.html>`__

-  Bug

   -  `SW-270 <https://0xdata.atlassian.net/browse/SW-270>`__ - Add test for RDD[TimeStamp] -&gt; H2OFrame[Time] -&gt; RDD[Timestamp] conversion
   -  `SW-319 <https://0xdata.atlassian.net/browse/SW-319>`__ - SVMModelTest is failing
   -  `SW-986 <https://0xdata.atlassian.net/browse/SW-986>`__ - Fix links on RSparkling Readme page
   -  `SW-996 <https://0xdata.atlassian.net/browse/SW-996>`__ - Fix typos in documentation
   -  `SW-997 <https://0xdata.atlassian.net/browse/SW-997>`__ - Fix javadoc on JavaH2OContext
   -  `SW-1000 <https://0xdata.atlassian.net/browse/SW-1000>`__ - Setting context path in pysparkling fails to launch h2o
   -  `SW-1001 <https://0xdata.atlassian.net/browse/SW-1001>`__ - RSparkling does not respect context path
   -  `SW-1002 <https://0xdata.atlassian.net/browse/SW-1002>`__ - Automatically generate the keystore for H2O Flow ssl (self-signed certificates)
   -  `SW-1003 <https://0xdata.atlassian.net/browse/SW-1003>`__ - When running in Local mode, we ignore some configuration
   -  `SW-1004 <https://0xdata.atlassian.net/browse/SW-1004>`__ - Fix context path value checks
   -  `SW-1005 <https://0xdata.atlassian.net/browse/SW-1005>`__ - Use correct scheme in sparkling water when ssl on flow is enabled
   -  `SW-1006 <https://0xdata.atlassian.net/browse/SW-1006>`__ - Fix context path setting on RSparkling
   -  `SW-1015 <https://0xdata.atlassian.net/browse/SW-1015>`__ - Add context path after value of spark.ext.h2o.client.flow.baseurl.override when specified

-  New Feature

   -  `SW-980 <https://0xdata.atlassian.net/browse/SW-980>`__ - Integrate XGBoost in Sparkling Water
   -  `SW-1012 <https://0xdata.atlassian.net/browse/SW-1012>`__ - Sparkling water External Backend Support in kerberized cluster

-  Task

   -  `SW-988 <https://0xdata.atlassian.net/browse/SW-988>`__ - Add to docs that pysparkling has a new dependency pyspark

-  Improvement

   -  `SW-175 <https://0xdata.atlassian.net/browse/SW-175>`__ - JavaH2OContext#asRDD implementation is missing
   -  `SW-920 <https://0xdata.atlassian.net/browse/SW-920>`__ - Sparkling Water/RSparkling needs to declare additional repository
   -  `SW-989 <https://0xdata.atlassian.net/browse/SW-989>`__ - Improve Scala Doc API of the support classes
   -  `SW-991 <https://0xdata.atlassian.net/browse/SW-991>`__ - Update Gradle Spinx libraries - faster documentation builds
   -  `SW-992 <https://0xdata.atlassian.net/browse/SW-992>`__ - Create abstract class from creating parameters from Enum for Sparkling Water pipelines
   -  `SW-993 <https://0xdata.atlassian.net/browse/SW-993>`__ - [PySparkling] Fix Wrong H2O version detection on latest bundled H2Os
   -  `SW-994 <https://0xdata.atlassian.net/browse/SW-994>`__ - Add timeouts &amp; retries for docker pull
   -  `SW-998 <https://0xdata.atlassian.net/browse/SW-998>`__ - Document using PySparkling on the edge node ( EMR)
   -  `SW-1007 <https://0xdata.atlassian.net/browse/SW-1007>`__ - Upgrade H2O to 3.20.0.6
   -  `SW-1011 <https://0xdata.atlassian.net/browse/SW-1011>`__ - Fix EMR bootstrap scripts
   -  `SW-1013 <https://0xdata.atlassian.net/browse/SW-1013>`__ - Add option which can be used to change the flow address which is printed out after H2OConetext started
   -  `SW-1014 <https://0xdata.atlassian.net/browse/SW-1014>`__ - Document how to run Sparkling Water on kerberized cluster


v2.1.36, v2.2.22, v2.3.11 (2018-08-09)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/11/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/11/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/22/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/22/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/36/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/36/index.html>`__

-  Bug

   -  `SW-971 <https://0xdata.atlassian.net/browse/SW-971>`__ - Change maintainer of RSparkling to jakub@h2o.ai
   -  `SW-972 <https://0xdata.atlassian.net/browse/SW-972>`__ - Fix Content of RSparkling release table
   -  `SW-973 <https://0xdata.atlassian.net/browse/SW-973>`__ - Allow passing custom cars when running ./bin/sparkling/shell
   -  `SW-975 <https://0xdata.atlassian.net/browse/SW-975>`__ - Fix CRAN issues of Rsparkling
   -  `SW-981 <https://0xdata.atlassian.net/browse/SW-981>`__ - Fix wrong comparison of versions when detecing other h2o versions in PySparkling
   -  `SW-982 <https://0xdata.atlassian.net/browse/SW-982>`__ - Set up client_disconnect_timeout correctly in context on External backend, auto  mode
   -  `SW-983 <https://0xdata.atlassian.net/browse/SW-983>`__ - Fix missing mojo impl artifact when running pysparkling tests in jenkins

-  Task

   -  `SW-633 <https://0xdata.atlassian.net/browse/SW-633>`__ - Add to doc that  100 columns are displayed in the preview data by default

-  Improvement

   -  `SW-528 <https://0xdata.atlassian.net/browse/SW-528>`__ - Update PySparkling Notebooks to work for Python 3
   -  `SW-548 <https://0xdata.atlassian.net/browse/SW-548>`__ - List nodes and driver memory in Spark UI - SParkling Water Tab
   -  `SW-910 <https://0xdata.atlassian.net/browse/SW-910>`__ - Use Mojo Pipeline API in Sparkling Water
   -  `SW-969 <https://0xdata.atlassian.net/browse/SW-969>`__ - Port documentation for mojo pipeline on Spark to SW repo
   -  `SW-970 <https://0xdata.atlassian.net/browse/SW-970>`__ - Upgrade Mojo 2 in SW to 0.11.0
   -  `SW-976 <https://0xdata.atlassian.net/browse/SW-976>`__ - Upgrade H2O to 3.20.0.5
   -  `SW-977 <https://0xdata.atlassian.net/browse/SW-977>`__ - Need ability to disable Flow UI for Sparkling-Water
   -  `SW-979 <https://0xdata.atlassian.net/browse/SW-979>`__ - Verify that we are running on correct Spark for PySparkling at init time
   -  `SW-984 <https://0xdata.atlassian.net/browse/SW-984>`__ - Cache also test and runtime dependencies in docker image

-  Docs

   -  `SW-946 <https://0xdata.atlassian.net/browse/SW-946>`__ - Add &quot;How to&quot; for using Sparkling Water on Google Cloud Dataproc


v2.1.35, v2.2.21, v2.3.10 (2018-08-01)
--------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/10/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/21/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/21/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/35/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/35/index.html>`__

-  Bug

   -  `SW-903 <https://0xdata.atlassian.net/browse/SW-903>`__ - Automate releases of RSparkling and create release pipeline for this release proccess
   -  `SW-911 <https://0xdata.atlassian.net/browse/SW-911>`__ - Add missing repository to the documentation
   -  `SW-944 <https://0xdata.atlassian.net/browse/SW-944>`__ - Fix Sphinx gradle plugin, the latest version does not work
   -  `SW-945 <https://0xdata.atlassian.net/browse/SW-945>`__ - Stabilize releasing to Nexus Repository
   -  `SW-953 <https://0xdata.atlassian.net/browse/SW-953>`__ - Do not stop external H2O backend in case of manual start mode
   -  `SW-958 <https://0xdata.atlassian.net/browse/SW-958>`__ - Fix RSparkling README style issues
   -  `SW-959 <https://0xdata.atlassian.net/browse/SW-959>`__ - Fix address for fetching H2O R package in nightly tests
   -  `SW-961 <https://0xdata.atlassian.net/browse/SW-961>`__ - Add option to ignore SPARK_PUBLIC_DNS
   -  `SW-962 <https://0xdata.atlassian.net/browse/SW-962>`__ - Add option which ensures that items in flatfile are translated to IP address
   -  `SW-967 <https://0xdata.atlassian.net/browse/SW-967>`__ - Deprecate old behaviour of mojo pipeline output in SW

-  Improvement

   -  `SW-233 <https://0xdata.atlassian.net/browse/SW-233>`__ - Warn if user&#39;s h2o in python env is different then the one bundled in pysparkling
   -  `SW-921 <https://0xdata.atlassian.net/browse/SW-921>`__ - Move Rsparkling to Sparkling Water repo
   -  `SW-941 <https://0xdata.atlassian.net/browse/SW-941>`__ - Upgrade Gradle to 4.9
   -  `SW-952 <https://0xdata.atlassian.net/browse/SW-952>`__ - Fix issues when stopping Sparkling Water (Scala) in yarn-cluster mode for external Backend
   -  `SW-957 <https://0xdata.atlassian.net/browse/SW-957>`__ - RSparkling should run tests in both, external and internal mode
   -  `SW-963 <https://0xdata.atlassian.net/browse/SW-963>`__ - Upgrade H2O to 3.20.0.4
   -  `SW-965 <https://0xdata.atlassian.net/browse/SW-965>`__ - Expose port offset in Sparkling Water
   -  `SW-968 <https://0xdata.atlassian.net/browse/SW-968>`__ - Remove confusing message about stopping H2OContext in PySparkling


v2.1.34, v2.2.20, v2.3.9 (2018-07-16)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/9/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/20/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/20/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/34/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/34/index.html>`__

-  Bug

   -  `SW-902 <https://0xdata.atlassian.net/browse/SW-902>`__ - Upgrade Gradle to 4.8.1
   -  `SW-904 <https://0xdata.atlassian.net/browse/SW-904>`__ - Upgrade Mojo2 version to 0.10.7
   -  `SW-909 <https://0xdata.atlassian.net/browse/SW-909>`__ - Fix issues when stopping Sparkling Water (Scala) in yarn-cluster mode
   -  `SW-925 <https://0xdata.atlassian.net/browse/SW-925>`__ - Fix missing aposthrope in documentation
   -  `SW-929 <https://0xdata.atlassian.net/browse/SW-929>`__ - Disable temporarily AutoML tests in Sparkling Water

-  New Feature

   -  `SW-826 <https://0xdata.atlassian.net/browse/SW-826>`__ - Implement Synchronous and Asynchronous Scala cell behaviour

-  Improvement

   -  `SW-846 <https://0xdata.atlassian.net/browse/SW-846>`__ - Don&#39;t parse types again when passing data to mojo pipeline
   -  `SW-886 <https://0xdata.atlassian.net/browse/SW-886>`__ - Several Scala cell improvements in H2O flow
   -  `SW-887 <https://0xdata.atlassian.net/browse/SW-887>`__ - Make sure that we can use schemes unsupported by H2O in H2O Confoguration
   -  `SW-889 <https://0xdata.atlassian.net/browse/SW-889>`__ - Port AWS preparation scripts into SW codebase
   -  `SW-894 <https://0xdata.atlassian.net/browse/SW-894>`__ - Add support for queuing of Scala cell jobs
   -  `SW-914 <https://0xdata.atlassian.net/browse/SW-914>`__ - Wrong Spark version in documentation
   -  `SW-915 <https://0xdata.atlassian.net/browse/SW-915>`__ - Upgrade to Spark 2.1.3
   -  `SW-917 <https://0xdata.atlassian.net/browse/SW-917>`__ - Dockerize Sparkling Water release pipeline
   -  `SW-919 <https://0xdata.atlassian.net/browse/SW-919>`__ - Clean gradle build with regards to mojo2
   -  `SW-922 <https://0xdata.atlassian.net/browse/SW-922>`__ - Upgrade H2O to 3.20.0.3
   -  `SW-928 <https://0xdata.atlassian.net/browse/SW-928>`__ - Expose AutoML max models

-  Docs

   -  `SW-878 <https://0xdata.atlassian.net/browse/SW-878>`__ - Add section for using Sparkling Water with AWS


v2.1.33, v2.2.19, v2.3.8 (2018-06-18)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/8/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/19/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/19/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/33/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/33/index.html>`__

-  Improvement

   -  `SW-885 <https://0xdata.atlassian.net/browse/SW-885>`__ - Upgrade H2O to 3.20.0.2


v2.1.32, v2.2.18, v2.3.7 (2018-06-18)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/7/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/18/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/18/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/32/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/32/index.html>`__

-  Bug

   -  `SW-861 <https://0xdata.atlassian.net/browse/SW-861>`__ - Upgrade Gradle to 4.8 (publishing plugin)
   -  `SW-872 <https://0xdata.atlassian.net/browse/SW-872>`__ - Fix reference to local-cluster on download page
   -  `SW-880 <https://0xdata.atlassian.net/browse/SW-880>`__ - Update Hadoop version on download page
   -  `SW-881 <https://0xdata.atlassian.net/browse/SW-881>`__ - Fix Script tests on Dockerized Jenkins infrastructure
   -  `SW-882 <https://0xdata.atlassian.net/browse/SW-882>`__ - Call h2oContext.stop after ham or spam Scala example
   -  `SW-883 <https://0xdata.atlassian.net/browse/SW-883>`__ - Add mising description in publish.gradle

-  Improvement

   -  `SW-860 <https://0xdata.atlassian.net/browse/SW-860>`__ - Modify the hadoop launch command on download page
   -  `SW-873 <https://0xdata.atlassian.net/browse/SW-873>`__ - Upgrade H2O to 3.20.0.1
   -  `SW-874 <https://0xdata.atlassian.net/browse/SW-874>`__ - Update Mojo2 to 0.10.4
   -  `SW-879 <https://0xdata.atlassian.net/browse/SW-879>`__ - Print output of script tests


v2.1.31, v2.2.17, v2.3.6 (2018-06-13)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/6/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/6/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/17/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/17/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/31/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/31/index.html>`__

-  Bug

   -  `SW-850 <https://0xdata.atlassian.net/browse/SW-850>`__ - Expose methods to get input/output names in H2OMOJOPipelineModel
   -  `SW-859 <https://0xdata.atlassian.net/browse/SW-859>`__ - Print Warning when spark-home is defined on PATH
   -  `SW-862 <https://0xdata.atlassian.net/browse/SW-862>`__ - Create &amp; fix test in PySparkling for named mojo columns
   -  `SW-864 <https://0xdata.atlassian.net/browse/SW-864>`__ - Fix &amp; more readable test
   -  `SW-865 <https://0xdata.atlassian.net/browse/SW-865>`__ - Better Naming of the UDF method to obtain predictions
   -  `SW-869 <https://0xdata.atlassian.net/browse/SW-869>`__ - Add repository to build required by xgboost-predictor

-  Story

   -  `SW-856 <https://0xdata.atlassian.net/browse/SW-856>`__ - Upgrade Mojo2 to latest version

-  Improvement

   -  `SW-839 <https://0xdata.atlassian.net/browse/SW-839>`__ - Verify that Spark time column representation can be digested by Mojo2
   -  `SW-848 <https://0xdata.atlassian.net/browse/SW-848>`__ - Document Kerberos on Sparkling Water
   -  `SW-849 <https://0xdata.atlassian.net/browse/SW-849>`__ - Update use from maven on sparkling water download page
   -  `SW-851 <https://0xdata.atlassian.net/browse/SW-851>`__ - Make use of output types when creating Spark DataFrame out of mojo2 predicted values
   -  `SW-852 <https://0xdata.atlassian.net/browse/SW-852>`__ - Create spark UDF used to extract predicted values
   -  `SW-853 <https://0xdata.atlassian.net/browse/SW-853>`__ - Sparkling Water py should require pyspark dependency
   -  `SW-854 <https://0xdata.atlassian.net/browse/SW-854>`__ - Upgrade MojoPipeline to 0.10.0
   -  `SW-855 <https://0xdata.atlassian.net/browse/SW-855>`__ - Upgrade H2O to 3.18.0.11


v2.1.30, v2.2.16, v2.3.5 (2018-05-23)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/5/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/5/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/16/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/16/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/30/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/30/index.html>`__

-  Bug

   -  `SW-842 <https://0xdata.atlassian.net/browse/SW-842>`__ - Enforce system level properties in SW

-  Improvement

   -  `SW-845 <https://0xdata.atlassian.net/browse/SW-845>`__ - Upgrade H2O to 3.18.0.10
   -  `SW-847 <https://0xdata.atlassian.net/browse/SW-847>`__ - Remove GA from Sparkling Water


v2.1.29, v2.2.15, v2.3.4 (2018-05-18)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/4/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/15/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/15/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/29/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/29/index.html>`__

-  Bug

   -  `SW-836 <https://0xdata.atlassian.net/browse/SW-836>`__ - Add support for converting empty dataframe/RDD in Python and Scala to H2OFrame
   -  `SW-841 <https://0xdata.atlassian.net/browse/SW-841>`__ - Remove withCustomCommitsState in pipelines as it&#39;s now duplicating Github
   -  `SW-843 <https://0xdata.atlassian.net/browse/SW-843>`__ - Fix data obtaining for mojo pipeline
   -  `SW-844 <https://0xdata.atlassian.net/browse/SW-844>`__ - Upgrade Mojo pipeline to 0.9.9


v2.1.28, v2.2.14, v2.3.3 (2018-05-15)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/3/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/14/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/14/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/28/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/28/index.html>`__

-  Bug

   -  `SW-817 <https://0xdata.atlassian.net/browse/SW-817>`__ - Enable running MOJO spark pipeline without H2O init
   -  `SW-825 <https://0xdata.atlassian.net/browse/SW-825>`__ - Local creation of Sparkling Water does not work anymore.
   -  `SW-831 <https://0xdata.atlassian.net/browse/SW-831>`__ - Check shape of H2O frame after the conversion from Spark frame
   -  `SW-834 <https://0xdata.atlassian.net/browse/SW-834>`__ - External Backend stored sparse vector values incorrectly

-  Improvement

   -  `SW-829 <https://0xdata.atlassian.net/browse/SW-829>`__ - Type checking in PySparkling pipelines
   -  `SW-832 <https://0xdata.atlassian.net/browse/SW-832>`__ - Small refactoring in identifiers
   -  `SW-833 <https://0xdata.atlassian.net/browse/SW-833>`__ - Explicitly set source and target java versions
   -  `SW-837 <https://0xdata.atlassian.net/browse/SW-837>`__ - Upgrade H2O to 3.18.0.9
   -  `SW-838 <https://0xdata.atlassian.net/browse/SW-838>`__ - Upgrade Mojo pipeline dependency to 0.9.8
   -  `SW-840 <https://0xdata.atlassian.net/browse/SW-840>`__ - Add test checking column names and types between spark and mojo2


v2.1.27, v2.2.13, v2.3.2 (2018-05-02)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/2/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/13/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/13/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/27/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/27/index.html>`__

-  Bug

   -  `SW-574 <https://0xdata.atlassian.net/browse/SW-574>`__ - Process steam handle and use it for connection to external h2o cluster
   -  `SW-822 <https://0xdata.atlassian.net/browse/SW-822>`__ - Require correct colorama version
   -  `SW-823 <https://0xdata.atlassian.net/browse/SW-823>`__ - Fix Windows starting scripts
   -  `SW-824 <https://0xdata.atlassian.net/browse/SW-824>`__ - Fix NPE in mojo pipeline predictions

-  New Feature

   -  `SW-827 <https://0xdata.atlassian.net/browse/SW-827>`__ - Change color highlight in scala cell as it is too dark

-  Improvement

   -  `SW-815 <https://0xdata.atlassian.net/browse/SW-815>`__ - Upgrade H2O to 3.18.0.8
   -  `SW-816 <https://0xdata.atlassian.net/browse/SW-816>`__ - Update Mojo2 dependency to one which is compatible with Java7
   -  `SW-818 <https://0xdata.atlassian.net/browse/SW-818>`__ - Spark Pipeline imports do not work in PySparkling
   -  `SW-819 <https://0xdata.atlassian.net/browse/SW-819>`__ - Add ability to convert specific columns to categoricals in Sparkling Water pipelines
   -  `SW-820 <https://0xdata.atlassian.net/browse/SW-820>`__ - Sparkling Water pipelines add duplicate response column to the list of features


v2.1.26, v2.2.12, v2.3.1 (2018-04-19)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/1/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/12/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/12/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/26/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/26/index.html>`__

-  Bug

   -  `SW-672 <https://0xdata.atlassian.net/browse/SW-672>`__ - Enable using sparkling water maven packages in databricks cloud
   -  `SW-787 <https://0xdata.atlassian.net/browse/SW-787>`__ - Documentation fixes
   -  `SW-790 <https://0xdata.atlassian.net/browse/SW-790>`__ - Add missing seed argument to H2OAutoml pipeline step
   -  `SW-794 <https://0xdata.atlassian.net/browse/SW-794>`__ - Point to proper web-based docs
   -  `SW-796 <https://0xdata.atlassian.net/browse/SW-796>`__ - Use parquet provided by Spark
   -  `SW-797 <https://0xdata.atlassian.net/browse/SW-797>`__ - Automatically update redirect table as part of release pipeline
   -  `SW-806 <https://0xdata.atlassian.net/browse/SW-806>`__ - Fix exporting and importing of pipeline steps and mojo models to and from HDFS

-  Improvement

   -  `SW-772 <https://0xdata.atlassian.net/browse/SW-772>`__ - Integrate &amp; Test Mojo Pipeline with Sparkling Water
   -  `SW-789 <https://0xdata.atlassian.net/browse/SW-789>`__ - Upgrade H2O to 3.18.0.7
   -  `SW-791 <https://0xdata.atlassian.net/browse/SW-791>`__ - Expose context_path in Sparkling Water
   -  `SW-793 <https://0xdata.atlassian.net/browse/SW-793>`__ - Create additional test verifying that the new light endpoint works as expected
   -  `SW-798 <https://0xdata.atlassian.net/browse/SW-798>`__ - Additional link to documentation
   -  `SW-800 <https://0xdata.atlassian.net/browse/SW-800>`__ - Remove references to Sparkling Water 2.0
   -  `SW-804 <https://0xdata.atlassian.net/browse/SW-804>`__ - Reduce time of H2OAutoml step in pipeline tests to 1 minute
   -  `SW-808 <https://0xdata.atlassian.net/browse/SW-808>`__ - Upgrade to Gradle 4.7


v2.1.25, v2.2.11, v2.3.0 (2018-03-29)
-------------------------------------
Downloads:

   - for Spark 2.3: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/0/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.3/0/index.html>`__
   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/11/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/11/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/25/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/25/index.html>`__

-  Bug

   -  `SW-696 <https://0xdata.atlassian.net/browse/SW-696>`__ - Intermittent script test issue on external backend
   -  `SW-726 <https://0xdata.atlassian.net/browse/SW-726>`__ - Mark Spark dependencies as provided on artefacts published to maven
   -  `SW-740 <https://0xdata.atlassian.net/browse/SW-740>`__ - Increase timeout for conversion in pyunit test for external cluster
   -  `SW-760 <https://0xdata.atlassian.net/browse/SW-760>`__ - Fix doc artefact publication
   -  `SW-763 <https://0xdata.atlassian.net/browse/SW-763>`__ - Remove support for downloading H2O logs from Spark UI
   -  `SW-766 <https://0xdata.atlassian.net/browse/SW-766>`__ - Fix coding style issue
   -  `SW-769 <https://0xdata.atlassian.net/browse/SW-769>`__ - Fix import
   -  `SW-776 <https://0xdata.atlassian.net/browse/SW-776>`__ - sparkling water from maven does not know the stacktrace_collector_interval option
   -  `SW-778 <https://0xdata.atlassian.net/browse/SW-778>`__ - Handle nulls properly in H2OMojoModel

-  New Feature

   -  `SW-722 <https://0xdata.atlassian.net/browse/SW-722>`__ - [PySparkling] Check for correct data type as part of as_h2o_frame

-  Improvement

   -  `SW-733 <https://0xdata.atlassian.net/browse/SW-733>`__ - Parametrize pipeline scripts to be able to specify different algorithms
   -  `SW-746 <https://0xdata.atlassian.net/browse/SW-746>`__ - Log chunk layout after the conversion of data to external H2O cluster
   -  `SW-755 <https://0xdata.atlassian.net/browse/SW-755>`__ - Document GBM Grid Search Pipeline Step
   -  `SW-765 <https://0xdata.atlassian.net/browse/SW-765>`__ - Remove test artefacts from the sparkling-water assembly
   -  `SW-768 <https://0xdata.atlassian.net/browse/SW-768>`__ - Add missing import
   -  `SW-773 <https://0xdata.atlassian.net/browse/SW-773>`__ - Don&#39;t use default value for output dir in external backend, it&#39;s not required
   -  `SW-780 <https://0xdata.atlassian.net/browse/SW-780>`__ - Upgrade H2O to 3.18.0.5

-  Docs

   -  `SW-775 <https://0xdata.atlassian.net/browse/SW-775>`__ - Fix link for documentation on DEVEL.md


v2.1.24, v2.2.10 (2018-03-08)
-----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/10/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/24/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/24/index.html>`__

-  Bug

   -  `SW-739 <https://0xdata.atlassian.net/browse/SW-739>`__ - Sparkling Water Doc artefact is still missing Scala version
   -  `SW-742 <https://0xdata.atlassian.net/browse/SW-742>`__ - Fix setting up node network mask on external cluster
   -  `SW-743 <https://0xdata.atlassian.net/browse/SW-743>`__ - Allow to set LDAP and different security options in external backend as well
   -  `SW-747 <https://0xdata.atlassian.net/browse/SW-747>`__ - Fix bug in documentation for manual mode of external backend
   -  `SW-757 <https://0xdata.atlassian.net/browse/SW-757>`__ - Fix tests after enabling the stack-trace collection

-  Improvement

   -  `SW-744 <https://0xdata.atlassian.net/browse/SW-744>`__ - Document how to use Sparkling Water with LDAP in Sparkling Water docs
   -  `SW-745 <https://0xdata.atlassian.net/browse/SW-745>`__ - Expose Grid search as Spark pipeline step in the Scala API
   -  `SW-748 <https://0xdata.atlassian.net/browse/SW-748>`__ - Upgrade to Gradle 4.6
   -  `SW-752 <https://0xdata.atlassian.net/browse/SW-752>`__ - Collect stack traces on each h2o node as part of log collecting extension
   -  `SW-754 <https://0xdata.atlassian.net/browse/SW-754>`__ - Upgrade H2O to 3.18.0.3
   -  `SW-756 <https://0xdata.atlassian.net/browse/SW-756>`__ - Upgrade H2O to 3.18.0.4

-  Docs

   -  `SW-753 <https://0xdata.atlassian.net/browse/SW-753>`__ - Add &quot;How to&quot; for changing the default H2O port


v2.1.23, v2.2.9 (2018-02-26)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/9/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/23/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/23/index.html>`__

-  Bug

   -  `SW-723 <https://0xdata.atlassian.net/browse/SW-723>`__ - Sparkling water doc artefact is missing scala version
   -  `SW-727 <https://0xdata.atlassian.net/browse/SW-727>`__ - Improve method for downloading H2O logs
   -  `SW-728 <https://0xdata.atlassian.net/browse/SW-728>`__ - Use new light endpoint introduced in 3.18.0.1
   -  `SW-734 <https://0xdata.atlassian.net/browse/SW-734>`__ - Make sure we use the unique key names in split method
   -  `SW-736 <https://0xdata.atlassian.net/browse/SW-736>`__ - Document how to download logs on Databricks cluster
   -  `SW-737 <https://0xdata.atlassian.net/browse/SW-737>`__ - Expose downloadH2OLogs on H2OContext in PySparkling
   -  `SW-738 <https://0xdata.atlassian.net/browse/SW-738>`__ - Move spark.ext.h2o.node.network.mask setter to SharedArguments

-  Improvement

   -  `SW-702 <https://0xdata.atlassian.net/browse/SW-702>`__ - Create Spark Transformer for AutoML
   -  `SW-725 <https://0xdata.atlassian.net/browse/SW-725>`__ - create an an equvivalent of h2o.download_all_logs in scala
   -  `SW-730 <https://0xdata.atlassian.net/browse/SW-730>`__ - Upgrade H2O to 3.18.0.2


v2.1.22, v2.2.8 (2018-02-14)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/8/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/22/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/22/index.html>`__

-  Technical task

   -  `SW-652 <https://0xdata.atlassian.net/browse/SW-652>`__ - Deliver SW documentation in HTML output

-  Bug

   -  `SW-685 <https://0xdata.atlassian.net/browse/SW-685>`__ - Fix Typo in documentation
   -  `SW-695 <https://0xdata.atlassian.net/browse/SW-695>`__ - Make printHadoopDistributions gradle task available again for testing
   -  `SW-701 <https://0xdata.atlassian.net/browse/SW-701>`__ - Kill the client when one of the h2o nodes went OOM in external mode
   -  `SW-706 <https://0xdata.atlassian.net/browse/SW-706>`__ - Fix pysparkling.ml import for non-interactive sessions
   -  `SW-707 <https://0xdata.atlassian.net/browse/SW-707>`__ - parquet import fails on HDP with Spark 2.0 (azure hdi cluster)
   -  `SW-708 <https://0xdata.atlassian.net/browse/SW-708>`__ - Make sure H2OMojoModel does not required H2OContext to be initialized
   -  `SW-709 <https://0xdata.atlassian.net/browse/SW-709>`__ - Fix mojo predictions tests
   -  `SW-710 <https://0xdata.atlassian.net/browse/SW-710>`__ - In PySparkling pipelines, ensure that if users pass integer to double type we handle that correctly for all possible double values
   -  `SW-713 <https://0xdata.atlassian.net/browse/SW-713>`__ - Write a simple test for parquet import in Sparkling Water
   -  `SW-714 <https://0xdata.atlassian.net/browse/SW-714>`__ - Add option to H2OModel pipeline step allowing us to convert unknown categoricals to NAs
   -  `SW-715 <https://0xdata.atlassian.net/browse/SW-715>`__ - Fix driverif configuration on the external backend

-  Improvement

   -  `SW-606 <https://0xdata.atlassian.net/browse/SW-606>`__ - Verify &amp; Document run of RSparkling on top of Databricks Azure cluster
   -  `SW-678 <https://0xdata.atlassian.net/browse/SW-678>`__ - Document how to change log location
   -  `SW-683 <https://0xdata.atlassian.net/browse/SW-683>`__ - H2OContext can&#39;t be initalized on Databricks cloud
   -  `SW-686 <https://0xdata.atlassian.net/browse/SW-686>`__ - Fix typo in documentation
   -  `SW-687 <https://0xdata.atlassian.net/browse/SW-687>`__ - Upgrade Gradle to 4.5
   -  `SW-688 <https://0xdata.atlassian.net/browse/SW-688>`__ - Update docs - SparklyR supports Spark 2.2.1 in the latest release
   -  `SW-690 <https://0xdata.atlassian.net/browse/SW-690>`__ - Log Sparkling Water version during startup of Sparkling Water
   -  `SW-693 <https://0xdata.atlassian.net/browse/SW-693>`__ - Allow to set driverIf on external H2O backend
   -  `SW-694 <https://0xdata.atlassian.net/browse/SW-694>`__ - Fix creation of Extended JAR in gradle task
   -  `SW-700 <https://0xdata.atlassian.net/browse/SW-700>`__ - Report Yarn App ID of spark application in H2OContext
   -  `SW-703 <https://0xdata.atlassian.net/browse/SW-703>`__ - Upload generated sphinx documentation to S3
   -  `SW-704 <https://0xdata.atlassian.net/browse/SW-704>`__ - Update links on the download page to point to the new docs
   -  `SW-705 <https://0xdata.atlassian.net/browse/SW-705>`__ - Increase memory for JUNIT tests
   -  `SW-718 <https://0xdata.atlassian.net/browse/SW-718>`__ - Upgrade to Gradle 4.5.1
   -  `SW-719 <https://0xdata.atlassian.net/browse/SW-719>`__ - Upgrade to H2O 3.18.0.1
   -  `SW-720 <https://0xdata.atlassian.net/browse/SW-720>`__ - Fix parquet import test on external backend

-  Docs

   -  `SW-697 <https://0xdata.atlassian.net/browse/SW-697>`__ - Final updates for Sparkling Water html output
   -  `SW-698 <https://0xdata.atlassian.net/browse/SW-698>`__ - Update &quot;Contributing&quot; section in Sparkling Water


v2.1.21, v2.2.7 (2018-01-18)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/7/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/21/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/21/index.html>`__

-  Bug

   -  `SW-273 <https://0xdata.atlassian.net/browse/SW-273>`__ - Remove workaround introduced by SW-272 for yarn/cluster mode
   -  `SW-551 <https://0xdata.atlassian.net/browse/SW-551>`__ - Remove hotfix introduced by [SW-541] and implement proper fix
   -  `SW-662 <https://0xdata.atlassian.net/browse/SW-662>`__ - Remove extra files that got into the repo
   -  `SW-666 <https://0xdata.atlassian.net/browse/SW-666>`__ - Kill the cluster when a new executors joins in the internal backend
   -  `SW-668 <https://0xdata.atlassian.net/browse/SW-668>`__ - Generate download link as part of the release notes
   -  `SW-669 <https://0xdata.atlassian.net/browse/SW-669>`__ - Remove mentions of local-cluster in public docs
   -  `SW-670 <https://0xdata.atlassian.net/browse/SW-670>`__ - Deprecated call in H2OContextInitDemo
   -  `SW-671 <https://0xdata.atlassian.net/browse/SW-671>`__ - Fix jenkinsfile for builds again specific h2o branches

-  Improvement

   -  `SW-674 <https://0xdata.atlassian.net/browse/SW-674>`__ - Update H2O to 3.16.0.4
   -  `SW-675 <https://0xdata.atlassian.net/browse/SW-675>`__ - Tiny clean up of the release code
   -  `SW-679 <https://0xdata.atlassian.net/browse/SW-679>`__ - Cleaner release script
   -  `SW-680 <https://0xdata.atlassian.net/browse/SW-680>`__ - Ensure S3 in release pipeline does depend only on credentials provided from Jenkins
   -  `SW-681 <https://0xdata.atlassian.net/browse/SW-681>`__ - Separate releasing on Github and Publishing artifacts


v2.1.20, v2.2.6 (2018-01-03)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/6/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/6/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/20/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/20/index.html>`__

-  Bug

   -  `SW-627 <https://0xdata.atlassian.net/browse/SW-627>`__ - [PySparkling] calling as_spark_frame for the second time results in exception
   -  `SW-630 <https://0xdata.atlassian.net/browse/SW-630>`__ - Fix ham or spam flow to reflect latest changes in pipelines
   -  `SW-631 <https://0xdata.atlassian.net/browse/SW-631>`__ - Ensure that we do not access RDDs in pipelines ( to unblock the deployment)
   -  `SW-646 <https://0xdata.atlassian.net/browse/SW-646>`__ - Fix incosistencies in ham or spam examples between scala and python
   -  `SW-648 <https://0xdata.atlassian.net/browse/SW-648>`__ - Fix ham or spam pipeline tests
   -  `SW-649 <https://0xdata.atlassian.net/browse/SW-649>`__ - Fix ham or spam tests for deeplearning pipeline
   -  `SW-661 <https://0xdata.atlassian.net/browse/SW-661>`__ - Use always correct Spark version on the R download page

-  Improvement

   -  `SW-608 <https://0xdata.atlassian.net/browse/SW-608>`__ - Measure time of conversions to H2OFrame in debug mode
   -  `SW-612 <https://0xdata.atlassian.net/browse/SW-612>`__ - Port all arguments available to Scala ML to PySparkling ML
   -  `SW-617 <https://0xdata.atlassian.net/browse/SW-617>`__ - Support for exporting mojo to hdfs
   -  `SW-632 <https://0xdata.atlassian.net/browse/SW-632>`__ - Dump full spark configuration during H2OContext.getOrCreate into DEBUG
   -  `SW-635 <https://0xdata.atlassian.net/browse/SW-635>`__ - Fix wrong instruction at PySparkling download page
   -  `SW-637 <https://0xdata.atlassian.net/browse/SW-637>`__ - Create new DataFrame with new schema when it actually contain any dot in names
   -  `SW-638 <https://0xdata.atlassian.net/browse/SW-638>`__ - Port release script into the sw repo
   -  `SW-639 <https://0xdata.atlassian.net/browse/SW-639>`__ - Use persist layer for exportPOJOModel
   -  `SW-640 <https://0xdata.atlassian.net/browse/SW-640>`__ - export H2OMOJOMOdel.createFromMOJO to pysparkling
   -  `SW-642 <https://0xdata.atlassian.net/browse/SW-642>`__ - Create test for mojo predictions in PySparkling
   -  `SW-643 <https://0xdata.atlassian.net/browse/SW-643>`__ - Add tests for H2ODeeplearning in Scala and Python and Fix potential problems
   -  `SW-644 <https://0xdata.atlassian.net/browse/SW-644>`__ - Log spark configuration to INFO level
   -  `SW-650 <https://0xdata.atlassian.net/browse/SW-650>`__ - Upgrade Gradle to 4.4.1
   -  `SW-656 <https://0xdata.atlassian.net/browse/SW-656>`__ - Upgrade ShadowJar to 2.0.2


v2.1.19, v2.2.5 (2017-12-11)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/5/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/5/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/19/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/19/index.html>`__

-  Bug

   -  `SW-615 <https://0xdata.atlassian.net/browse/SW-615>`__ - pysparkling.__version__ returns incorrectly 'SUBST_PROJECT_VERSION'
   -  `SW-616 <https://0xdata.atlassian.net/browse/SW-616>`__ - PySparkling fails on python 3.6 because long time does not exist in python 3.6
   -  `SW-621 <https://0xdata.atlassian.net/browse/SW-621>`__ - PySParkling failing on Python3.6
   -  `SW-624 <https://0xdata.atlassian.net/browse/SW-624>`__ - Python build does not support H2O_PYTHON_WHEEL when building against h2o older then 3.16.0.1
   -  `SW-628 <https://0xdata.atlassian.net/browse/SW-628>`__ - PySparkling fails when installed from pypi

-  Improvement

   -  `SW-626 <https://0xdata.atlassian.net/browse/SW-626>`__ - Upgrade Gradle to 4.4


v2.1.18, v2.2.4 (2017-12-01)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/4/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/18/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/18/index.html>`__

-  Bug

   -  `SW-602 <https://0xdata.atlassian.net/browse/SW-602>`__ - conversion of sparse data DataFrame to H2OFrame is slow
   -  `SW-620 <https://0xdata.atlassian.net/browse/SW-620>`__ - Fix obtaining version from bundled h2o inside pysparkling

-  Improvement

   -  `SW-613 <https://0xdata.atlassian.net/browse/SW-613>`__ - Append dynamic allocation option into SW tuning documentation.
   -  `SW-618 <https://0xdata.atlassian.net/browse/SW-618>`__ - Integration with H2O 3.16.0.2


v2.1.17, v2.2.3 (2017-11-25)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/3/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/17/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/17/index.html>`__

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


v2.1.16, v2.2.2 (2017-10-23)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/2/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/16/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/16/index.html>`__

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


v2.1.15, v2.2.1 (2017-10-10)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/1/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/15/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/15/index.html>`__

-  Bug

   -  `SW-423 <https://0xdata.atlassian.net/browse/SW-423>`__ - Tests of External Cluster mode fails
   -  `SW-516 <https://0xdata.atlassian.net/browse/SW-516>`__ - External cluster improperly convert RDD[ml.linalg.Vector]
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
   -  `SW-542 <https://0xdata.atlassian.net/browse/SW-542>`__ - sw does not support parquet import
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


v2.1.14, v2.2.0 (2017-08-23)
----------------------------
Downloads:

   - for Spark 2.2: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/0/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/0/index.html>`__
   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/14/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/14/index.html>`__

-  Bug

   -  `SW-449 <https://0xdata.atlassian.net/browse/SW-449>`__ - Support Sparse Data during spark-h2o conversions
   -  `SW-510 <https://0xdata.atlassian.net/browse/SW-510>`__ - The link `Demo Example from Git` is broken on the download page

-  New Feature

   -  `SW-481 <https://0xdata.atlassian.net/browse/SW-481>`__ - MOJO for Spark SVM

-  Improvement

   -  `SW-514 <https://0xdata.atlassian.net/browse/SW-514>`__ - Upgrade H2O to 3.14.0.2
   -  `SW-395 <https://0xdata.atlassian.net/browse/SW-395>`__ - bin/sparkling-shell should fail if assembly `jar` file does not exist
   -  `SW-471 <https://0xdata.atlassian.net/browse/SW-471>`__ - Use mojo in pipelines if possible, remove H2OPipeline and OneTimeTransformers
   -  `SW-512 <https://0xdata.atlassian.net/browse/SW-512>`__ - Make JenkinsFile up-to-date with sparkling_yarn_branch
   -  `SW-513 <https://0xdata.atlassian.net/browse/SW-513>`__ - Upgrade to Gradle 4.1


v2.1.13 (2017-08-02)
--------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/13/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/13/index.html>`__

-  Bug

   -  `SW-501 <https://0xdata.atlassian.net/browse/SW-501>`__ - Security Bug when using Security.enableSSL(spark)
   -  `SW-505 <https://0xdata.atlassian.net/browse/SW-505>`__ - Travis build is failing on missing OracleJdk7

-  Improvement

   -  `SW-355 <https://0xdata.atlassian.net/browse/SW-355>`__ - Include H2O R client distribution in Sparkling Water binary
   -  `SW-500 <https://0xdata.atlassian.net/browse/SW-500>`__ - Warehouse dir does not have to be set in tests on Spark from 2.1+
   -  `SW-506 <https://0xdata.atlassian.net/browse/SW-506>`__ - Documentation for the backends should mention get-extended-h2o.sh instead of manual jar extending
   -  `SW-507 <https://0xdata.atlassian.net/browse/SW-507>`__ - Upgrade to Gradle 4.0.2
   -  `SW-508 <https://0xdata.atlassian.net/browse/SW-508>`__ - More robust get-extended-h2o.sh
   -  `SW-509 <https://0xdata.atlassian.net/browse/SW-509>`__ - Add back DEVEL.md and CHANGELOG.md and redirect to new versions


v2.1.12 (2017-07-17)
--------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/12/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/12/index.html>`__

-  Improvement

   -  `SW-490 <https://0xdata.atlassian.net/browse/SW-490>`__ - Upgrade Gradle to 4.0.1
   -  `SW-491 <https://0xdata.atlassian.net/browse/SW-491>`__ - Increase default value for Write and Read confirmation timeout
   -  `SW-492 <https://0xdata.atlassian.net/browse/SW-492>`__ - Remove dead code and deprecation warning in tests
   -  `SW-493 <https://0xdata.atlassian.net/browse/SW-493>`__ - Enforce Scala Style rules
   -  `SW-494 <https://0xdata.atlassian.net/browse/SW-494>`__ - Remove hard dependency on RequestServer by using RestApiContext
   -  `SW-496 <https://0xdata.atlassian.net/browse/SW-496>`__ - Remove ignored empty "H2OFrame[Time] to DataFrame[TimeStamp]" test
   -  `SW-498 <https://0xdata.atlassian.net/browse/SW-498>`__ - Upgrade H2O to 3.10.5.4


v2.1.11 (2017-07-12)
--------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/11/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/11/index.html>`__

-  Bug

   -  `SW-407 <https://0xdata.atlassian.net/browse/SW-407>`__ - Make scala H2OConf consistent and allow to set and get all propertties

-  Improvement

   -  `SW-485 <https://0xdata.atlassian.net/browse/SW-485>`__ - Update instructions for a new PYPI.org
   -  `SW-489 <https://0xdata.atlassian.net/browse/SW-489>`__ - Upgrade H2O to 3.10.5.3


v2.1.10 (2017-06-29)
--------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/10/index.html>`__

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
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/9/index.html>`__

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
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/8/index.html>`__

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
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/7/index.html>`__

-  Bug

   -  `SW-429 <https://0xdata.atlassian.net/browse/SW-429>`__ - Different cluster name between client and h2o nodes in case of external cluster


v2.1.6 (2017-05-09)
-------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/6/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/6/index.html>`__

-  Improvement

   -  `SW-424 <https://0xdata.atlassian.net/browse/SW-424>`__ - Add SW tab in Spark History Server
   -  `SW-427 <https://0xdata.atlassian.net/browse/SW-427>`__ - Upgrade H2O dependency to 3.10.4.7


v2.1.5 (2017-04-27)
-------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/5/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/5/index.html>`__

-  Bug

   -  `SW-421 <https://0xdata.atlassian.net/browse/SW-421>`__ - Externar cluster: Job is reporting exit status as FAILED even all mappers return 0

-  Improvement

   -  `SW-422 <https://0xdata.atlassian.net/browse/SW-422>`__ - Upgrade H2O dependency to 3.10.4.6


v2.1.4 (2017-04-20)
-------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/4/index.html>`__

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
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/3/index.html>`__

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
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/2/index.html>`__

-  Bug

   -  `SW-361 <https://0xdata.atlassian.net/browse/SW-361>`__ - Flow is not available in Sparkling Water
   -  `SW-362 <https://0xdata.atlassian.net/browse/SW-362>`__ - PySparkling does not work

-  Improvement

   -  `SW-344 <https://0xdata.atlassian.net/browse/SW-344>`__ - Use Spark public DNS if available to report Flow UI


v2.1.1 (2017-03-18)
-------------------
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/1/index.html>`__

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
Downloads:

   - for Spark 2.1: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/0/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/0/index.html>`__

-  Bug

   -  `SW-331 <https://0xdata.atlassian.net/browse/SW-331>`__ - Security.enableSSL does not work

-  Improvement

   -  `SW-302 <https://0xdata.atlassian.net/browse/SW-302>`__ - Support Spark 2.1.0
   -  `SW-325 <https://0xdata.atlassian.net/browse/SW-325>`__ - Implement a generic announcement mechanism
   -  `SW-326 <https://0xdata.atlassian.net/browse/SW-326>`__ - Add support to Spark 2.1 in Sparkling Water
   -  `SW-327 <https://0xdata.atlassian.net/browse/SW-327>`__ - Enrich Spark UI with Sparkling Water specific tab
