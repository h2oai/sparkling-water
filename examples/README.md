Sparkling Water Examples
========================

Examples are available in the **examples/** folder. Refer to the following sections for specific examples.

- `Available Demos and Applications <#available-demos-and-applications>`__
- `Compiling Examples <#compiling-examples>`__
- `Running Examples on a Local Cluster <#running-examples-on-a-local-cluster>`__
- `Running Examples on a Spark Cluster <#running-examples-on-a-spark-cluster>`__
- `Configuring Sparkling Water Variables <#configuring-sparkling-water-variables>`__
- `Step-by-Step Weather Data Example <#step-by-step-weather-data-example>`__

Available Demos and Applications
--------------------------------

The Sparkling Water distribution includes a variety of Sparkling Water and Sparkling Shell example demos. 

Sparkling Water Demos
~~~~~~~~~~~~~~~~~~~~~

You can run these examples by typing ``bin/run-example.sh <name of demo>``. The following list includes the available demos. Additional demos are updated periodically and can be found in `this folder <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/>`__ in github.

- `CraigslistJobTitlesStreamingApp <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/CraigslistJobTitlesStreamingApp.scala>`__: This is a **stream** application that predicts a job category based on an incoming job description.

- `CraigslistJobTitlesApp <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/CraigslistJobTitlesApp.scala>`__: This predicts a job category based on a posted job description.

- `ChicagoCrimeApp Small <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeAppSmall.scala>`__: This builds a model that predicts the probability of an arrest for a given crime in Chicago using data in the **/examples/smalldata** directory.

- `ChicagoCrimeApp <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeApp.scala>`__: This is an example implementation of Chicago Crime demo with setup for data stored on HDFS.

- `CitiBikeSharingDemo <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/CitiBikeSharingDemo.scala>`__: This predicts the occupancy of CitiBike stations in NYC.

- `HamOrSpam Demo <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/HamOrSpamDemo.scala>`__: This shows a Spam detector using Spark and H2O's DeepLearning algorithm.

- `ProstateDemo <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/ProstateDemo.scala>`__: This is an example run of K-means on the prostate dataset, which is available in the **/examples/smalldata** directory.

- `DeepLearningDemo <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/DeepLearningDemo.scala>`__: This is an example run of DeepLearning on a subset of the airlines (allyears2k_headers.csv.gz) dataset, which is available in the **/examples/smalldata** folder.

- `AirlinesWithWeatherDemo <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo.scala>`__: This example joins flights data with weather data and using Deep Learning.

- `AirlinesWithWeatherDemo2 <https://github.com/h2oai/sparkling-water/tree/master/examples/src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo2.scala>`__: This is an updated iteration of  **AirlinesWithWeatherDemo**.


Sparkling Shell Demos
~~~~~~~~~~~~~~~~~~~~~

Run any of the examples by typing  ``bin/sparkling-shell -i <path to file with demo script>`` 

- `chicagoCrimeSmallShell.script.scala <https://github.com/h2oai/sparkling-water/tree/master/examples/scripts/chicagoCrimeSmallShell.script.scala>`__ - This demo shows the full source code of predicting arrest probability for a given crime. It covers the entire machine learning process from loading and transforming data, to building models, and to finally scoring incoming events.

- `chicagoCrimeSmall.script.scala <https://github.com/h2oai/sparkling-water/tree/master/examples/scripts/chicagoCrimeSmall.script.scala>`__: This example uses the **ChicagoCrimeApp.scala** file (in src/main/scala/org/apache/spark/examples/h2o/), creating an application and using it for scoring individual crime events.

- `hamOrSpam.script.scala <https://github.com/h2oai/sparkling-water/tree/master/examples/scripts/hamOrSpam.script.scala>`__: The HamOrSpam application detectes Spam messages.

- `strata2015.script.scala <https://github.com/h2oai/sparkling-water/tree/master/examples/scripts/strata2015.script.scala>`__: This is the NYC CitiBike demo presented at Strata 2015 in San Jose.

- `StrataAirlines.script.scala <https://github.com/h2oai/sparkling-water/tree/master/examples/scripts/StrataAirlines.script.scala>`__: This is an example that uses flights and weather data to predict the delay of a flight.
  

  
-----

Compiling Examples
------------------

To compile examples, use top-level ``gradlew``:

::

 ./gradlew assemble


-----

Running Examples On a Local Cluster
-----------------------------------
 
Run a given example on a local cluster. The cluster is defined by **MASTER** address ``local-cluster[3,2,3072]``, which means that cluster contains 3 worker nodes, each having 2CPUs and 3GB of memory:

:: 
 
 bin/run-example.sh <name of demo>
 
----

Running Examples on a Spark Cluster
-----------------------------------

1. Run the Spark cluster using ``bin/launch-spark-cloud.sh``.
2. Verify that Spark is running. The Spark UI on **http://localhost:8080/** should show 3 worker nodes.
3. Export the **MASTER** address of the Spark master using ``export MASTER="spark://localhost:7077"``
4. Run ``bin/run-example.sh <name of demo>``.
5. Observe the status of the application via the Spark UI on **http://localhost:8080/**.

----

Configuring Sparkling Water Variables
-------------------------------------

You can configure Sparkling Water using the following variables:

- ``spark.h2o.cloud.timeout``: The number of milliseconds to wait for cloud formation.
- ``spark.h2o.workers``: The number of expected H<sub>2</sub>O workers. This should be the same as the number of Spark workers.
- ``spark.h2o.preserve.executors``: Specify to not kill executors with the ``sc.stop()`` call.

----

Step-by-Step Weather Data Example
---------------------------------

1. Run Sparkling shell with an embedded cluster:

 ::

  export SPARK_HOME="/path/to/spark/installation"
  export MASTER="local[*]"
  bin/sparkling-shell


2. To see the Sparkling shell (i.e., Spark driver) status, go to [http://localhost:4040/](http://localhost:4040/).

3. Initialize H2O services on top of Spark cluster:

 ::

  scala
  import org.apache.spark.h2o._
  val h2oContext = H2OContext.getOrCreate(sc)
  import h2oContext._
  import h2oContext.implicits._
  

4. Load weather data for Chicago international airport (ORD), with help from the RDD API:

 ::

  scala
  import org.apache.spark.examples.h2o._
  val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
  val wrawdata = sc.textFile(weatherDataFile,3).cache()
  val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())
  

5. Load airlines data using the H<sub>2</sub>O parser:

 ::

  scala
  import java.io.File
  val dataFile = "examples/smalldata/allyears2k_headers.csv.gz"
  val airlinesData = new H2OFrame(new File(dataFile))


6. Select flights destined for Chicago (ORD):

 ::

  scala
  val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
  val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))
  
  
7. Compute the number of these flights:

 ::

  scala
  flightsToORD.count


8. Use Spark SQL to join the flight data with the weather data:

 ::

  scala
  import sqlContext.implicits._
  flightsToORD.toDF.registerTempTable("FlightsToORD")
  weatherTable.toDF.registerTempTable("WeatherORD")
  

9. Perform SQL JOIN on both tables:

 ::

  scala
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

 ::

  scala
  val bigDataFrame: H2OFrame = h2oContext.asH2OFrame(bigTable)
  for( i <- 0 to 2) bigDataFrame.replace(i, bigDataFrame.vec(i).toCategoricalVec)
  bigDataFrame.update()

11. Run deep learning to produce a model estimating arrival delay:

 ::

  scala
  import _root_.hex.deeplearning.DeepLearning
  import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters  
  import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
  val dlParams = new DeepLearningParameters()
  dlParams._train = bigDataFrame
  dlParams._response_column = 'ArrDelay
  dlParams._epochs = 5
  dlParams._activation = Activation.RectifierWithDropout
  dlParams._hidden = Array[Int](100, 100)
  
  // Create a job  
  val dl = new DeepLearning(dlParams)
  val dlModel = dl.trainModel.get

12. Use the model to estimate the delay on the training data:

 ::

  scala
  val predictionH2OFrame = dlModel.score(bigTable)('predict)
  val predictionsFromModel = asDataFrame(predictionH2OFrame)(sqlContext).collect.map(row => if (row.isNullAt(0)) Double.NaN else row(0))
  
13. Generate an R-code producing residual plot:

 ::

  scala
  import org.apache.spark.examples.h2o.DemoUtils.residualPlotRCode
  residualPlotRCode(predictionH2OFrame, 'predict, bigTable, 'ArrDelay)  
  
14. Execute generated R-code in RStudio:

 ::

  R
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