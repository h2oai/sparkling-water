Sparkling Water Table of Contents
=================================

-  `Compiling examples <#CompileExample>`__
-  `Running examples <#RunExample>`__
-  `Running on a local cluster <#LocalCluster>`__
-  `Running on a Spark cluster <#SparkCluster>`__
-  `Configuring variables <#ConfigVar>`__
-  `Step-by-step weather example <#WeatherExample>`__
-  `Running Sparkling Water on Hadoop <#Hadoop>`__
-  `Importing data from HDFS <#ImportData>`__

Sparkling Water Examples
========================

Available Demos And Applications
--------------------------------

-  ```CraigslistJobTitlesStreamingApp`` <src/main/scala/org/apache/spark/examples/h2o/CraigslistJobTitlesStreamingApp.scala>`__
   - **stream** application - it predicts job category based on incoming
   job description
-  ```CraigslistJobTitlesApp`` <src/main/scala/org/apache/spark/examples/h2o/CraigslistJobTitlesApp.scala>`__
   - predict job category based on posted job description
-  ```ChicagoCrimeAppSmall`` <src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeAppSmall.scala>`__
   - builds a model predicting a probability of arrest for given crime
   in Chicago using data in ```smalldata`` directory <smalldata/>`__
-  ```ChicagoCrimeApp`` <src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeApp.scala>`__
   - implementation of Chicago Crime demo with setup for data stored on
   HDFS
-  ```CitiBikeSharingDemo`` <src/main/scala/org/apache/spark/examples/h2o/CitiBikeSharingDemo.scala>`__
   - predicts occupancy of Citi bike stations in NYC
-  ```HamOrSpamDemo`` <src/main/scala/org/apache/spark/examples/h2o/HamOrSpamDemo.scala>`__
   - shows Spam detector with Spark and H2O's DeepLearning
-  ```ProstateDemo`` <src/main/scala/org/apache/spark/examples/h2o/ProstateDemo.scala>`__
   - running K-means on prostate dataset (see *smalldata/prostate.csv*)
-  ```DeepLearningDemo`` <src/main/scala/org/apache/spark/examples/h2o/DeepLearningDemo.scala>`__
   - running DeepLearning on a subset of airlines dataset (see
   *smalldata/allyears2k\_headers.csv.gz*)
-  ```AirlinesWithWeatherDemo`` <src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo.scala>`__
   - joining flights data with weather data and running Deep Learning
-  ```AirlinesWithWeatherDemo2`` <src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo2.scala>`__
   - new iteration of ``AirlinesWithWeatherDemo``

    Run examples by typing ``bin/run-example.sh <name of demo>`` or
    follow text below.

Available Demos for Sparkling Shell
-----------------------------------

-  ```chicagoCrimeSmallShell.script.scala`` <scripts/chicagoCrimeSmallShell.script.scala>`__
   - demo showing full source code of predicting arrest probability for
   a given crime. It covers whole machine learning process from loading
   and transforming data, building models, scoring incoming events.
-  ```chicagoCrimeSmall.script.scala`` <scripts/chicagoCrimeSmall.script.scala>`__
   - example of using
   `ChicagoCrimeApp <src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeApp.scala>`__
   - creating application and using it for scoring individual crime
   events.
-  ```mlconf_2015_hamSpam.script.scala`` <scripts/mlconf_2015_hamSpam.script.scala>`__
   - HamOrSpam application which detectes Spam messages. Presented at
   MLConf 2015 NYC.
-  ```strata2015_demo.scala`` <scripts/strata2015_demo.scala>`__ - NYC
   CitiBike demo presented at Strata 2015 in San Jose.
-  ```StrataAirlines.scala`` <scripts/StrataAirlines.scala>`__ - example
   of using flights and weather data to predict delay of a flight

    Run examples by typing
    ``bin/sparkling-shell -i <path to file with demo script>``

--------------

Compiling Examples
------------------

To compile, use top-level ``gradlew``:

::

    ./gradlew build -x check

On a Spark Cluster
~~~~~~~~~~~~~~~~~~

-  Run the Spark cluster, for example via ``bin/launch-spark-cloud.sh``

   -  Verify that Spark is running: The Spark UI on
      ``http://localhost:8080/`` should show 3 worker nodes

-  Export ``MASTER`` address of Spark master using
   ``export MASTER="spark://localhost:7077"``
-  Run ``bin/run-example.sh <name of demo>``
-  Observe status of the application via Spark UI on
   ``http://localhost:8080/``

Configuring Sparkling Water Variables
-------------------------------------

Please see `Available Sparkling Water Configuration Properties <../doc/configuration/configuration_properties>`__.

Step-by-Step Weather Data Example
---------------------------------

1.  Run Sparkling shell with an embedded cluster:

.. code:: bash

    export SPARK_HOME="/path/to/spark/installation"   export MASTER="local[*]"   bin/sparkling-shell

2.  To see the Sparkling shell (i.e., Spark driver) status, go to http://localhost:4040/.

3.  Initialize H2O services on top of Spark cluster:

.. code:: scala

    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)
    import h2oContext._
    import h2oContext.implicits._

4.  Load weather data for Chicago international airport (ORD), with help
    from the RDD API:

.. code:: scala

    import org.apache.spark.examples.h2o._
    val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
    val wrawdata = spark.sparkContext.textFile(weatherDataFile,3).cache()
    val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

5.  Load airlines data using the H2O parser:

.. code:: scala

    import java.io.File
    val dataFile = "examples/smalldata/allyears2k_headers.csv.gz"
    val airlinesData = new H2OFrame(new File(dataFile))

6.  Select flights destined for Chicago (ORD):

.. code:: scala

    val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
    val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

7.  Compute the number of these flights:

.. code:: scala

    flightsToORD.count

8.  Use Spark SQL to join the flight data with the weather data:

.. code:: scala

    implicit val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    flightsToORD.toDF.createOrReplaceTempView("FlightsToORD")
    weatherTable.toDF.createOrReplaceTempView("WeatherORD")

9.  Perform SQL JOIN on both tables:

.. code:: scala

    val bigTable = sqlContext.sql(
            """SELECT
                |f.Year,f.Month,f.DayofMonth,
                |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
                |f.UniqueCarrier,f.FlightNum,f.TailNum,
                |f.Origin,f.Distance,
                |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
                |f.ArrDelay
                |FROM FlightsToORD f
                |JOIN WeatherORD w
                |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day""".stripMargin)

10. Transform the first 3 columns containing date information into enum columns:

.. code:: scala

    val bigDataFrame: H2OFrame = h2oContext.asH2OFrame(bigTable)
    for( i <- 0 to 2) bigDataFrame.replace(i, bigDataFrame.vec(i).toCategoricalVec)
    bigDataFrame.update()

11. Run deep learning to produce a model estimating arrival delay:

.. code:: scala

    import _root_.hex.deeplearning.DeepLearning
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
    val dlParams = new DeepLearningParameters()
    dlParams._train = bigDataFrame
    dlParams._response_column = "ArrDelay"
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100, 100)

    // Create a job
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get


12. Use the model to estimate the delay on the training data:

.. code:: scala

    val predictionH2OFrame = dlModel.score(bigTable)("predict")
    val predictionsFromModel = asDataFrame(predictionH2OFrame)(sqlContext).collect.map{
        row => if (row.isNullAt(0)) Double.NaN else row(0)
    }

13. Generate an R-code producing residual plot:

.. code:: scala

    import org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2.residualPlotRCode
    residualPlotRCode(predictionH2OFrame, "predict", bigTable, "ArrDelay", h2oContext)

14. Execute generated R-code in RStudio:

.. code:: R

    #
    # R script for residual plot
    #
    # Import H2O library
    library(h2o)
    # Initialize H2O R-client
    h2o.init()
    # Fetch prediction and actual data, use remembered keys
    pred = h2o.getFrame("dframe_b5f449d0c04ee75fda1b9bc865b14a69")
    act = h2o.getFrame ("frame_rdd_14_b429e8b43d2d8c02899ccb61b72c4e57")
    # Select right columns
    predDelay = pred$predict
    actDelay = act$ArrDelay
    # Make sure that number of rows is same
    nrow(actDelay) == nrow(predDelay)
    # Compute residuals
    residuals = predDelay - actDelay
    # Plot residuals
    compare = cbind (as.data.frame(actDelay$ArrDelay), as.data.frame(residuals$predict))
    nrow(compare)
    plot( compare[,1:2] )