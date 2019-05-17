Change Log
==========

v2.2.40 (2019-05-17)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/40/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/40/index.html>`__

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
                
                                                                                        
v2.2.39 (2019-05-17)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/39/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/39/index.html>`__

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
   -  `SW-1236 <https://0xdata.atlassian.net/browse/SW-1236>`__ - Reformat few python classes
   -  `SW-1238 <https://0xdata.atlassian.net/browse/SW-1238>`__ - Parametrize EMR version in templates generation
   -  `SW-1239 <https://0xdata.atlassian.net/browse/SW-1239>`__ - Remove old README and DEVEL doc files (not just pointer to new doc)
   -  `SW-1240 <https://0xdata.atlassian.net/browse/SW-1240>`__ - Use minSupportedJava for source and target compatibility in build.gradle
                
                                                                                        
v2.2.39 (2019-04-25)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/39/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/39/index.html>`__

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
   -  `SW-1236 <https://0xdata.atlassian.net/browse/SW-1236>`__ - Reformat few python classes
   -  `SW-1238 <https://0xdata.atlassian.net/browse/SW-1238>`__ - Parametrize EMR version in templates generation
   -  `SW-1239 <https://0xdata.atlassian.net/browse/SW-1239>`__ - Remove old README and DEVEL doc files (not just pointer to new doc)
   -  `SW-1240 <https://0xdata.atlassian.net/browse/SW-1240>`__ - Use minSupportedJava for source and target compatibility in build.gradle
                
                                                                                
v2.2.38 (2019-04-03)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/38/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/38/index.html>`__

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
                
                                                                                
v2.2.37 (2019-03-15)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/37/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/37/index.html>`__

-  Bug
        
   -  `SW-1163 <https://0xdata.atlassian.net/browse/SW-1163>`__ - Expose missing variables in shared TF EMR SW tamplate
                
-  Improvement
        
   -  `SW-1145 <https://0xdata.atlassian.net/browse/SW-1145>`__ - Start jupyter notebook with Scala &amp; Python Spark in AWS EMR Terraform template
   -  `SW-1165 <https://0xdata.atlassian.net/browse/SW-1165>`__ - Upgrade to H2O 3.22.1.6
                
                                                                                
v2.2.36 (2019-03-07)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/36/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/36/index.html>`__

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
                
                                                                        
v2.2.35 (2019-02-18)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/35/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/35/index.html>`__

-  Bug
        
   -  `SW-1136 <https://0xdata.atlassian.net/browse/SW-1136>`__ - Fix bug affecting loading pipeline in python when stored in scala
   -  `SW-1138 <https://0xdata.atlassian.net/browse/SW-1138>`__ - Fix several cases in spark vector -&gt; h2o conversion
                
-  Improvement
        
   -  `SW-1134 <https://0xdata.atlassian.net/browse/SW-1134>`__ - Add H2OGLM Wrapper to Sparkling Water
   -  `SW-1139 <https://0xdata.atlassian.net/browse/SW-1139>`__ - Update mojo2 to 0.3.16
   -  `SW-1143 <https://0xdata.atlassian.net/browse/SW-1143>`__ - Fix s3 bootstrap templates for nightly builds
   -  `SW-1144 <https://0xdata.atlassian.net/browse/SW-1144>`__ - Upgrade to H2O 3.22.1.4
                
                                                                        
v2.2.34 (2019-01-29)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/34/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/34/index.html>`__

-  Bug
        
   -  `SW-1133 <https://0xdata.atlassian.net/browse/SW-1133>`__ - Upgrade to H2O 3.22.1.3
                
                                                                                        
v2.2.33 (2019-01-21)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/33/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/33/index.html>`__

-  Bug
        
   -  `SW-1129 <https://0xdata.atlassian.net/browse/SW-1129>`__ - Fix support for unsupervised mojo models
                
-  Improvement
        
   -  `SW-1101 <https://0xdata.atlassian.net/browse/SW-1101>`__ - Update code to work with latest jetty changes
   -  `SW-1127 <https://0xdata.atlassian.net/browse/SW-1127>`__ - Upgrade H2O to 3.22.1.2
                
                                                                        
v2.2.32 (2019-01-17)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/32/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/32/index.html>`__

-  Bug
        
   -  `SW-1116 <https://0xdata.atlassian.net/browse/SW-1116>`__ - Cannot serialize DAI model
                
-  Improvement
        
   -  `SW-1113 <https://0xdata.atlassian.net/browse/SW-1113>`__ - Update to H2O 3.22.0.5
   -  `SW-1115 <https://0xdata.atlassian.net/browse/SW-1115>`__ - Enable tabs in the documentation based on the language
   -  `SW-1120 <https://0xdata.atlassian.net/browse/SW-1120>`__ - Prepare Terraform scripts for Sparkling Water on EMR
   -  `SW-1121 <https://0xdata.atlassian.net/browse/SW-1121>`__ - Use getTimestamp method instead of _timestamp directly
   -  `SW-1125 <https://0xdata.atlassian.net/browse/SW-1125>`__ - Upgrade to Spark 2.2.3
                
                                                                        
v2.2.31 (2019-01-08)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/31/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/31/index.html>`__

-  Bug
        
   -  `SW-1107 <https://0xdata.atlassian.net/browse/SW-1107>`__ - NullPointerException at water.H2ONode.openChan(H2ONode.java:417) after upgrade to H2O 3.22.0.3
   -  `SW-1110 <https://0xdata.atlassian.net/browse/SW-1110>`__ - Fix test suite to test PySparkling YARN integration tests on external backend as well
                
-  Task
        
   -  `SW-1109 <https://0xdata.atlassian.net/browse/SW-1109>`__ - Docs: Change copyright year in docs to include 2019
                
-  Improvement
        
   -  `SW-464 <https://0xdata.atlassian.net/browse/SW-464>`__ - Publish PySparkling as conda package
   -  `SW-1111 <https://0xdata.atlassian.net/browse/SW-1111>`__ - Update H2O to 3.22.0.4
                
                                                                
v2.2.30 (2018-12-27)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/30/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/30/index.html>`__

-  Bug
        
   -  `SW-1084 <https://0xdata.atlassian.net/browse/SW-1084>`__ - Documentation link does not work on the Nightly Bleeding Edge download page
   -  `SW-1100 <https://0xdata.atlassian.net/browse/SW-1100>`__ - Fix Travis builds
   -  `SW-1102 <https://0xdata.atlassian.net/browse/SW-1102>`__ - Fix Travis builds (test just scala unit tests)
                
-  Improvement
        
   -  `SW-464 <https://0xdata.atlassian.net/browse/SW-464>`__ - Publish PySparkling as conda package
   -  `SW-1080 <https://0xdata.atlassian.net/browse/SW-1080>`__ - Fix deprecation warning regarding automl -&gt; AutoML
   -  `SW-1090 <https://0xdata.atlassian.net/browse/SW-1090>`__ - Upgrade shadowJar plugin
   -  `SW-1091 <https://0xdata.atlassian.net/browse/SW-1091>`__ - Upgrade to Gradle 5.0
   -  `SW-1092 <https://0xdata.atlassian.net/browse/SW-1092>`__ - Updates to streaming app
   -  `SW-1093 <https://0xdata.atlassian.net/browse/SW-1093>`__ - Update to H2O 3.22.0.3
   -  `SW-1095 <https://0xdata.atlassian.net/browse/SW-1095>`__ - Enable GCS in Sparkling Water
   -  `SW-1097 <https://0xdata.atlassian.net/browse/SW-1097>`__ - Properly integrate GCS with Sparkling Water, including test in PySparkling
   -  `SW-1106 <https://0xdata.atlassian.net/browse/SW-1106>`__ - Remove deprecated Gradle option in Gradle 5
                
-  Docs
        
   -  `SW-1083 <https://0xdata.atlassian.net/browse/SW-1083>`__ - Add Installation and Starting instructions to the docs
                
    
v2.2.29 (2018-11-27)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/29/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/29/index.html>`__

-  Improvement
        
   -  `SW-1078 <https://0xdata.atlassian.net/browse/SW-1078>`__ - Upgrade H2O to 3.22.0.2
                
                                
v2.2.28 (2018-10-27)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/28/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/28/index.html>`__

-  Bug
        
   -  `SW-1071 <https://0xdata.atlassian.net/browse/SW-1071>`__ - Fallback to original IP discovery in case we can&#39;t find the same network
   -  `SW-1072 <https://0xdata.atlassian.net/browse/SW-1072>`__ - Fix handling time column for mojo pipeline
   -  `SW-1073 <https://0xdata.atlassian.net/browse/SW-1073>`__ - Upgrade MOJO to 0.3.17
                
-  Improvement
        
   -  `SW-1045 <https://0xdata.atlassian.net/browse/SW-1045>`__ - Upgrade H2O to 3.22.0.1
                
                                
v2.2.27 (2018-10-17)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/27/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/27/index.html>`__

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
                
                                
v2.2.26 (2018-10-02)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/26/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/26/index.html>`__

-  Bug
        
   -  `SW-1041 <https://0xdata.atlassian.net/browse/SW-1041>`__ - Fix passing --jars to sparkling-shell
   -  `SW-1042 <https://0xdata.atlassian.net/browse/SW-1042>`__ - More robust check for python package in PySparkling shell
   -  `SW-1048 <https://0xdata.atlassian.net/browse/SW-1048>`__ - Add missing six dependency to setup.py for PySparkling
                
-  Improvement
        
   -  `SW-1043 <https://0xdata.atlassian.net/browse/SW-1043>`__ - Mojo pipeline with multiple output columns (and also with dots in the names) does not work in SW
   -  `SW-1054 <https://0xdata.atlassian.net/browse/SW-1054>`__ - Upgrade H2O dependency to 3.20.0.9
                
                                
v2.2.25 (2018-09-24)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/25/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/25/index.html>`__

-  New Feature
        
   -  `SW-1020 <https://0xdata.atlassian.net/browse/SW-1020>`__ - Expose leaderboard on H2OAutoML
   -  `SW-1022 <https://0xdata.atlassian.net/browse/SW-1022>`__ - Display Release creation date on the download page
                
-  Improvement
        
   -  `SW-1024 <https://0xdata.atlassian.net/browse/SW-1024>`__ - remove call to ./gradlew --help in jenkins pipeline
   -  `SW-1025 <https://0xdata.atlassian.net/browse/SW-1025>`__ - Ensure that release does not depend on build id
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
                
                            
v2.2.24 (2018-09-14)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/24/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/24/index.html>`__

-  New Feature
        
   -  `SW-1023 <https://0xdata.atlassian.net/browse/SW-1023>`__ - Upgrade Gradle to 4.10.1
                
-  Improvement
        
   -  `SW-1019 <https://0xdata.atlassian.net/browse/SW-1019>`__ - Upgrade H2O to 3.20.0.7
   -  `SW-1027 <https://0xdata.atlassian.net/browse/SW-1027>`__ - Revert Upgrade to Gradle 4.10.1(bug in Gradle) and upgrade to Gradle 4.0
   -  `SW-1028 <https://0xdata.atlassian.net/browse/SW-1028>`__ - Update docs and mention that ORC is supported
                
-  Docs
        
   -  `SW-1017 <https://0xdata.atlassian.net/browse/SW-1017>`__ - Docs: Add Parquet to list of supported data formats 
                
                            
v2.2.23 (2018-08-28)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/23/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/23/index.html>`__

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
                
                                
v2.2.22 (2018-08-09)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/22/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/22/index.html>`__

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
                
                            
v2.2.21 (2018-08-01)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/21/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/21/index.html>`__

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
                
                                
v2.2.20 (2018-07-16)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/20/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/20/index.html>`__

-  Bug
        
   -  `SW-902 <https://0xdata.atlassian.net/browse/SW-902>`__ - Upgrade Gradle to 4.8.1
   -  `SW-904 <https://0xdata.atlassian.net/browse/SW-904>`__ - Upgrade Mojo2 version to 0.10.7
   -  `SW-908 <https://0xdata.atlassian.net/browse/SW-908>`__ - Exclude Hadoop dependencies as they are provided by Spark
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
   -  `SW-917 <https://0xdata.atlassian.net/browse/SW-917>`__ - Dockerize Sparkling Water release pipeline
   -  `SW-919 <https://0xdata.atlassian.net/browse/SW-919>`__ - Clean gradle build with regards to mojo2
   -  `SW-922 <https://0xdata.atlassian.net/browse/SW-922>`__ - Upgrade H2O to 3.20.0.3
   -  `SW-928 <https://0xdata.atlassian.net/browse/SW-928>`__ - Expose AutoML max models
   -  `SW-933 <https://0xdata.atlassian.net/browse/SW-933>`__ - Upgrade Spark to 2.2.2 
                
-  Docs
        
   -  `SW-878 <https://0xdata.atlassian.net/browse/SW-878>`__ - Add section for using Sparkling Water with AWS
                
                            
v2.2.19 (2018-06-18)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/19/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/19/index.html>`__

-  Improvement
        
   -  `SW-885 <https://0xdata.atlassian.net/browse/SW-885>`__ - Upgrade H2O to 3.20.0.2
                
                                
v2.2.18 (2018-06-18)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/18/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/18/index.html>`__

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
   -  `SW-876 <https://0xdata.atlassian.net/browse/SW-876>`__ - FIx local PySparkling integtest on jenkins infrastracture
   -  `SW-879 <https://0xdata.atlassian.net/browse/SW-879>`__ - Print output of script tests
                
                                
v2.2.17 (2018-06-13)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/17/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/17/index.html>`__

-  Bug
        
   -  `SW-712 <https://0xdata.atlassian.net/browse/SW-712>`__ - Test non-distributed sparkling water tests in docker
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
                
                                
v2.2.16 (2018-05-23)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/16/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/16/index.html>`__

-  Bug
        
   -  `SW-842 <https://0xdata.atlassian.net/browse/SW-842>`__ - Enforce system level properties in SW
                
-  Improvement
        
   -  `SW-845 <https://0xdata.atlassian.net/browse/SW-845>`__ - Upgrade H2O to 3.18.0.10
   -  `SW-847 <https://0xdata.atlassian.net/browse/SW-847>`__ - Remove GA from Sparkling Water
                
                                
v2.2.15 (2018-05-18)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/15/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/15/index.html>`__

-  Bug
        
   -  `SW-836 <https://0xdata.atlassian.net/browse/SW-836>`__ - Add support for converting empty dataframe/RDD in Python and Scala to H2OFrame
   -  `SW-841 <https://0xdata.atlassian.net/browse/SW-841>`__ - Remove withCustomCommitsState in pipelines as it&#39;s now duplicating Github 
   -  `SW-843 <https://0xdata.atlassian.net/browse/SW-843>`__ - Fix data obtaining for mojo pipeline
   -  `SW-844 <https://0xdata.atlassian.net/browse/SW-844>`__ - Upgrade Mojo pipeline to 0.9.9
                
                                                    
v2.2.14 (2018-05-15)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/14/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/14/index.html>`__

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
                
                                
v2.2.13 (2018-05-02)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/13/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/13/index.html>`__

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
                
                                
v2.2.12 (2018-04-19)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/12/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/12/index.html>`__

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
                
                                
v2.2.11 (2018-03-29)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/11/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/11/index.html>`__

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
   -  `SW-783 <https://0xdata.atlassian.net/browse/SW-783>`__ - Make H2OAutoML pipeline tests deterministic by setting the seed
                
-  New Feature
        
   -  `SW-722 <https://0xdata.atlassian.net/browse/SW-722>`__ - [PySparkling] Check for correct data type as part of as_h2o_frame
                
-  Improvement
        
   -  `SW-733 <https://0xdata.atlassian.net/browse/SW-733>`__ - Parametrize pipeline scripts to be able to specify different algorithms
   -  `SW-746 <https://0xdata.atlassian.net/browse/SW-746>`__ - Log chunk layout after the conversion of data to external H2O cluster
   -  `SW-755 <https://0xdata.atlassian.net/browse/SW-755>`__ - Document GBM Grid Search Pipeline Step
   -  `SW-765 <https://0xdata.atlassian.net/browse/SW-765>`__ - Remove test artefacts from the sparkling-water assembly
   -  `SW-768 <https://0xdata.atlassian.net/browse/SW-768>`__ - Add missing import
   -  `SW-771 <https://0xdata.atlassian.net/browse/SW-771>`__ - Travis edits - no longer need the workaround for JDK7
   -  `SW-773 <https://0xdata.atlassian.net/browse/SW-773>`__ - Don&#39;t use default value for output dir in external backend, it&#39;s not required
   -  `SW-780 <https://0xdata.atlassian.net/browse/SW-780>`__ - Upgrade H2O to 3.18.0.5
                
-  Docs
        
   -  `SW-775 <https://0xdata.atlassian.net/browse/SW-775>`__ - Fix link for documentation on DEVEL.md
                
                            
v2.2.10 (2018-03-08)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/10/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/10/index.html>`__

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
                
                            
v2.2.9 (2018-02-26)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/9/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/9/index.html>`__

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
                
                                
v2.2.8 (2018-02-14)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/8/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/8/index.html>`__

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
                
                            
v2.2.7 (2018-01-18)
-------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/7/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.2/7/index.html>`__

-  Bug
        
   -  `SW-273 <https://0xdata.atlassian.net/browse/SW-273>`__ - Remove workaround introduced by SW-272 for yarn/cluster mode
   -  `SW-551 <https://0xdata.atlassian.net/browse/SW-551>`__ - Remove hotfix introduced by [SW-541] and implement proper fix
   -  `SW-661 <https://0xdata.atlassian.net/browse/SW-661>`__ - Use always correct Spark version on the R download page
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
                
                                
v2.2.6 (2018-01-03)
-------------------

-  Bug
        
   -  `SW-627 <https://0xdata.atlassian.net/browse/SW-627>`__ - [PySparkling] calling as_spark_frame for the second time results in exception
   -  `SW-630 <https://0xdata.atlassian.net/browse/SW-630>`__ - Fix ham or spam flow to reflect latest changes in pipelines
   -  `SW-631 <https://0xdata.atlassian.net/browse/SW-631>`__ - Ensure that we do not access RDDs in pipelines ( to unblock the deployment)
   -  `SW-645 <https://0xdata.atlassian.net/browse/SW-645>`__ - Fix H2OInterpreter on Scala 2.10
   -  `SW-646 <https://0xdata.atlassian.net/browse/SW-646>`__ - Fix incosistencies in ham or spam examples between scala and python
   -  `SW-647 <https://0xdata.atlassian.net/browse/SW-647>`__ - PySparkling shell is failing in Spark 2.2.0
   -  `SW-648 <https://0xdata.atlassian.net/browse/SW-648>`__ - Fix ham or spam pipeline tests
   -  `SW-649 <https://0xdata.atlassian.net/browse/SW-649>`__ - Fix ham or spam tests for deeplearning pipeline
                
-  Improvement
        
   -  `SW-608 <https://0xdata.atlassian.net/browse/SW-608>`__ - Measure time of conversions to H2OFrame in debug mode
   -  `SW-612 <https://0xdata.atlassian.net/browse/SW-612>`__ - Port all arguments available to Scala ML to PySparkling ML
   -  `SW-617 <https://0xdata.atlassian.net/browse/SW-617>`__ - Support for exporting mojo to hdfs
   -  `SW-632 <https://0xdata.atlassian.net/browse/SW-632>`__ - Dump full spark configuration during H2OContext.getOrCreate into DEBUG
   -  `SW-634 <https://0xdata.atlassian.net/browse/SW-634>`__ - Integrate with Spark 2.2.1
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
                
                                
v2.2.5 (2017-12-11)
-------------------

-  Bug

   -  `SW-615 <https://0xdata.atlassian.net/browse/SW-615>`__ - pysparkling.__version__ returns incorrectly 'SUBST_PROJECT_VERSION'
   -  `SW-616 <https://0xdata.atlassian.net/browse/SW-616>`__ - PySparkling fails on python 3.6 because long time does not exist in python 3.6
   -  `SW-621 <https://0xdata.atlassian.net/browse/SW-621>`__ - PySParkling failing on Python3.6
   -  `SW-624 <https://0xdata.atlassian.net/browse/SW-624>`__ - Python build does not support H2O_PYTHON_WHEEL when building against h2o older then 3.16.0.1
   -  `SW-628 <https://0xdata.atlassian.net/browse/SW-628>`__ - PySparkling fails when installed from pypi

-  Improvement

   -  `SW-626 <https://0xdata.atlassian.net/browse/SW-626>`__ - Upgrade Gradle to 4.4


v2.2.4 (2017-12-01)
-------------------

-  Bug

   -  `SW-602 <https://0xdata.atlassian.net/browse/SW-602>`__ - conversion of sparse data DataFrame to H2OFrame is slow
   -  `SW-620 <https://0xdata.atlassian.net/browse/SW-620>`__ - Fix obtaining version from bundled h2o inside pysparkling

-  Improvement

   -  `SW-613 <https://0xdata.atlassian.net/browse/SW-613>`__ - Append dynamic allocation option into SW tuning documentation.
   -  `SW-618 <https://0xdata.atlassian.net/browse/SW-618>`__ - Integration with H2O 3.16.0.2

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
