Change Log
==========

v2.4.13 (2019-06-24)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/13/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/13/index.html>`__

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
   -  `SW-1327 <https://0xdata.atlassian.net/browse/SW-1327>`__ - Support Spark 2.4.1 and 2.4.3
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
                
                                                                                                    
v2.4.12 (2019-06-03)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/12/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/12/index.html>`__

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
                
                                                                                        
v2.4.11 (2019-05-17)
--------------------
Download at: `http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/11/index.html <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.4/11/index.html>`__

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
   -  `SW-1269 <https://0xdata.atlassian.net/browse/SW-1269>`__ - Remove six as dependency from PySparkling launcher ( six is no longer dependency)
   -  `SW-1275 <https://0xdata.atlassian.net/browse/SW-1275>`__ - Remove unnecessary constructor in helper class
   -  `SW-1280 <https://0xdata.atlassian.net/browse/SW-1280>`__ - Add predictionCol to mojo pipeline model
                
                                                                                        
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