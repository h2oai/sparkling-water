Change Log
==========

v2.4.10 (2019-04-26)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/10/index.html>`__

-  Bug
        
   -  `SW-1186 <https://0xdata.atlassian.net/browse/SW-1186>`__ - No need to pass properties defined in spark-defaults.conf to cli
   -  `SW-1189 <https://0xdata.atlassian.net/browse/SW-1189>`__ - Fix Sparkling Water 2.1.x compile on Scala 2.10 
   -  `SW-1194 <https://0xdata.atlassian.net/browse/SW-1194>`__ - RSparkling Can&#39;t be used on Spark 2.4
   -  `SW-1195 <https://0xdata.atlassian.net/browse/SW-1195>`__ - Disable gradle daemon via gradle.properties
   -  `SW-1196 <https://0xdata.atlassian.net/browse/SW-1196>`__ - Fix org.apache.spark.ml.spark.models.PipelinePredictionTest
   -  `SW-1203 <https://0xdata.atlassian.net/browse/SW-1203>`__ - Custom metric not evaluated in internal mode of Sparkling Water
   -  `SW-1227 <https://0xdata.atlassian.net/browse/SW-1227>`__ - Change get-extended-jar to use https instead of http
   -  `SW-1230 <https://0xdata.atlassian.net/browse/SW-1230>`__ - Fix typo in GLM API - getRemoteCollinearColumns, setRemoteCollinearColumns
   -  `SW-1232 <https://0xdata.atlassian.net/browse/SW-1232>`__ - Fix RUnits after upgrading to Gradle 5.3.1
                
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
   -  `SW-1236 <https://0xdata.atlassian.net/browse/SW-1236>`__ - Reformat few python classes
   -  `SW-1238 <https://0xdata.atlassian.net/browse/SW-1238>`__ - Parametrize EMR version in templates generation
   -  `SW-1239 <https://0xdata.atlassian.net/browse/SW-1239>`__ - Remove old README and DEVEL doc files (not just pointer to new doc)
   -  `SW-1240 <https://0xdata.atlassian.net/browse/SW-1240>`__ - Use minSupportedJava for source and target compatibility in build.gradle
                
                                                                                
v2.4.9 (2019-04-03)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/9/index.html>`__

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
                
                                                                                
v2.4.8 (2019-03-15)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/8/index.html>`__

-  Bug
        
   -  `SW-1163 <https://0xdata.atlassian.net/browse/SW-1163>`__ - Expose missing variables in shared TF EMR SW tamplate
                
-  Improvement
        
   -  `SW-1145 <https://0xdata.atlassian.net/browse/SW-1145>`__ - Start jupyter notebook with Scala &amp; Python Spark in AWS EMR Terraform template
   -  `SW-1165 <https://0xdata.atlassian.net/browse/SW-1165>`__ - Upgrade to H2O 3.22.1.6
                
                                                                                
v2.4.7 (2019-03-07)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/7/index.html>`__

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
                
                                                                        
v2.4.6 (2019-02-18)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/6/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/6/index.html>`__

-  Bug
        
   -  `SW-1136 <https://0xdata.atlassian.net/browse/SW-1136>`__ - Fix bug affecting loading pipeline in python when stored in scala
   -  `SW-1138 <https://0xdata.atlassian.net/browse/SW-1138>`__ - Fix several cases in spark vector -&gt; h2o conversion
                
-  Improvement
        
   -  `SW-1134 <https://0xdata.atlassian.net/browse/SW-1134>`__ - Add H2OGLM Wrapper to Sparkling Water
   -  `SW-1139 <https://0xdata.atlassian.net/browse/SW-1139>`__ - Update mojo2 to 0.3.16
   -  `SW-1143 <https://0xdata.atlassian.net/browse/SW-1143>`__ - Fix s3 bootstrap templates for nightly builds
   -  `SW-1144 <https://0xdata.atlassian.net/browse/SW-1144>`__ - Upgrade to H2O 3.22.1.4
                
                                                                        
v2.4.5 (2019-01-29)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/5/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/5/index.html>`__

-  Bug
        
   -  `SW-1133 <https://0xdata.atlassian.net/browse/SW-1133>`__ - Upgrade to H2O 3.22.1.3
                
                                                                                        
v2.4.4 (2019-01-21)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/4/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/4/index.html>`__

-  Bug
        
   -  `SW-1129 <https://0xdata.atlassian.net/browse/SW-1129>`__ - Fix support for unsupervised mojo models
                
-  Improvement
        
   -  `SW-1101 <https://0xdata.atlassian.net/browse/SW-1101>`__ - Update code to work with latest jetty changes
   -  `SW-1127 <https://0xdata.atlassian.net/browse/SW-1127>`__ - Upgrade H2O to 3.22.1.2
                
                                                                        
v2.4.3 (2019-01-17)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/3/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/3/index.html>`__

-  Bug
        
   -  `SW-1116 <https://0xdata.atlassian.net/browse/SW-1116>`__ - Cannot serialize DAI model
                
-  Improvement
        
   -  `SW-1113 <https://0xdata.atlassian.net/browse/SW-1113>`__ - Update to H2O 3.22.0.5
   -  `SW-1115 <https://0xdata.atlassian.net/browse/SW-1115>`__ - Enable tabs in the documentation based on the language
   -  `SW-1120 <https://0xdata.atlassian.net/browse/SW-1120>`__ - Prepare Terraform scripts for Sparkling Water on EMR
   -  `SW-1121 <https://0xdata.atlassian.net/browse/SW-1121>`__ - Use getTimestamp method instead of _timestamp directly
                
                                                                        
v2.4.2 (2019-01-08)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/2/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/2/index.html>`__

-  Bug
        
   -  `SW-1107 <https://0xdata.atlassian.net/browse/SW-1107>`__ - NullPointerException at water.H2ONode.openChan(H2ONode.java:417) after upgrade to H2O 3.22.0.3
   -  `SW-1110 <https://0xdata.atlassian.net/browse/SW-1110>`__ - Fix test suite to test PySparkling YARN integration tests on external backend as well
                
-  Task
        
   -  `SW-1109 <https://0xdata.atlassian.net/browse/SW-1109>`__ - Docs: Change copyright year in docs to include 2019
                
-  Improvement
        
   -  `SW-464 <https://0xdata.atlassian.net/browse/SW-464>`__ - Publish PySparkling as conda package
   -  `SW-1111 <https://0xdata.atlassian.net/browse/SW-1111>`__ - Update H2O to 3.22.0.4
                
                                                                
v2.4.1 (2018-12-27)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/1/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/1/index.html>`__

-  Bug
        
   -  `SW-1084 <https://0xdata.atlassian.net/browse/SW-1084>`__ - Documentation link does not work on the Nightly Bleeding Edge download page
   -  `SW-1100 <https://0xdata.atlassian.net/browse/SW-1100>`__ - Fix Travis builds
   -  `SW-1102 <https://0xdata.atlassian.net/browse/SW-1102>`__ - Fix Travis builds (test just scala unit tests)
                
-  Task
        
   -  `SW-857 <https://0xdata.atlassian.net/browse/SW-857>`__ - Make behaviour introduced by SW-851 as default in Spark 2.4 and up
                
-  Improvement
        
   -  `SW-464 <https://0xdata.atlassian.net/browse/SW-464>`__ - Publish PySparkling as conda package
   -  `SW-995 <https://0xdata.atlassian.net/browse/SW-995>`__ - Don&#39;t require implicit sqlContext parameter as part of asDataFrame as we can get it in spark session internally
   -  `SW-1079 <https://0xdata.atlassian.net/browse/SW-1079>`__ - Upgrade to Spark 2.4 (Without making use the barier API so far)
   -  `SW-1080 <https://0xdata.atlassian.net/browse/SW-1080>`__ - Fix deprecation warning regarding automl -&gt; AutoML
   -  `SW-1086 <https://0xdata.atlassian.net/browse/SW-1086>`__ - Re-enable RSparkling tests for master &amp; rel-2.4 when SparklyR supports Spark 2.4
   -  `SW-1090 <https://0xdata.atlassian.net/browse/SW-1090>`__ - Upgrade shadowJar plugin
   -  `SW-1091 <https://0xdata.atlassian.net/browse/SW-1091>`__ - Upgrade to Gradle 5.0
   -  `SW-1092 <https://0xdata.atlassian.net/browse/SW-1092>`__ - Updates to streaming app
   -  `SW-1093 <https://0xdata.atlassian.net/browse/SW-1093>`__ - Update to H2O 3.22.0.3
   -  `SW-1095 <https://0xdata.atlassian.net/browse/SW-1095>`__ - Enable GCS in Sparkling Water
   -  `SW-1097 <https://0xdata.atlassian.net/browse/SW-1097>`__ - Properly integrate GCS with Sparkling Water, including test in PySparkling
   -  `SW-1098 <https://0xdata.atlassian.net/browse/SW-1098>`__ - Fix pyspark dependency for pysparkling for Spark 2.4
   -  `SW-1106 <https://0xdata.atlassian.net/browse/SW-1106>`__ - Remove deprecated Gradle option in Gradle 5
                
-  Docs
        
   -  `SW-1083 <https://0xdata.atlassian.net/browse/SW-1083>`__ - Add Installation and Starting instructions to the docs
                
    
v2.3.x (2018-03-29)
-------------------

-  Sparkling Water 2.3 brings support of Spark 2.3.
-  For detailed changelog, please read `rel-2.3/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.3/doc/CHANGELOG.rst>`__.


v2.2.x (2017-08-17)
-------------------

-  Sparkling Water 2.2 brings support of Spark 2.2.
-  For detailed changelog, please read `rel-2.2/CHANGELOG <https://github.com/h2oai/sparkling-water/blob/rel-2.2/doc/CHANGELOG.rst>`__.

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