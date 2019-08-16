Sparkling Water Examples
========================

Available Demos And Applications
--------------------------------

+-----------------------------------+--------------------------------------------------------------------------+
| Example                           | Description                                                              |
+===================================+==========================================================================+
| |CraigslistJobTitlesStreamingApp| | **Stream** application - it predicts job category based on incoming job  |
|                                   | description.                                                             |
+-----------------------------------+--------------------------------------------------------------------------+
| |CraigslistJobTitlesApp|          | Predict job category based on posted job description.                    |
+-----------------------------------+--------------------------------------------------------------------------+
| |ChicagoCrimeAppSmall|            | Builds a model predicting a probability of arrest for given crime in     |
|                                   | Chicago using data in |ChicagoDataset|.                                  |
+-----------------------------------+--------------------------------------------------------------------------+
| |ChicagoCrimeApp|                 | Implementation of Chicago Crime demo with setup for data stored on HDFS. |
+-----------------------------------+--------------------------------------------------------------------------+
| |CitiBikeSharingDemo|             | Predicts occupancy of Citi bike stations in NYC.                         |
+-----------------------------------+--------------------------------------------------------------------------+
| |HamOrSpamDemo|                   | Shows Spam detector with Spark and H2O's DeepLearning.                   |
+-----------------------------------+--------------------------------------------------------------------------+
| |ProstateDemo|                    | Running K-means on |ProstateDataset|.                                    |
+-----------------------------------+--------------------------------------------------------------------------+
| |DeepLearningDemo|                | Running DeepLearning on a subset of |AirlinesDataset|.                   |
+-----------------------------------+--------------------------------------------------------------------------+
| |AirlinesWithWeatherDemo|         | Joining flights data with weather data and running Deep Learning.        |
+-----------------------------------+--------------------------------------------------------------------------+
| |AirlinesWithWeatherDemo2|        | New iteration of ``AirlinesWithWeatherDemo``.                            |
+-----------------------------------+--------------------------------------------------------------------------+

    Run examples by typing ``bin/run-example.sh <name of demo>`` or follow text below.

Available Demos for Sparkling Shell
-----------------------------------

+-----------------------------------+--------------------------------------------------------------------------+
| Example                           | Description                                                              |
+===================================+==========================================================================+
| |chicagoCrimeSmallShellScript|    | Demo showing full source code of predicting arrest probability for a     |
|                                   | given crime. It covers whole machine learning process from loading and   |
|                                   | transforming data, building models, scoring incoming events.             |
+-----------------------------------+--------------------------------------------------------------------------+
| |chicagoCrimeSmallScript|         | Example of using |ChicagoCrimeApp|. Creating application and using it    |
|                                   | for scoring individual crime events.                                     |
+-----------------------------------+--------------------------------------------------------------------------+
| |hamOrSpamScript|                 | HamOrSpam application which detects Spam messages. Presented at          |
|                                   | MLConf 2015 NYC.                                                         |
+-----------------------------------+--------------------------------------------------------------------------+
| |strata2015Script|                | NYC CitiBike demo presented at Strata 2015 in San Jose.                  |
+-----------------------------------+--------------------------------------------------------------------------+
| |StrataAirlinesScript|            | Example of using flights and weather data to predict delay of a flight.  |
+-----------------------------------+--------------------------------------------------------------------------+

    Run examples by typing ``bin/sparkling-shell -i <path to file with demo script>``

--------------

Building and Running Examples
-----------------------------

Please see `Running Sparkling Water Examples <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/devel/running_examples.html>`__ for more information how to build
and run examples.

Configuring Sparkling Water Variables
-------------------------------------

Please see `Available Sparkling Water Configuration Properties <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/configuration/configuration_properties.html>`__ for
more information about possible Sparkling Water configurations.

Step-by-Step Weather Data Example
---------------------------------

1.  Run Sparkling shell with an embedded cluster:

.. code:: bash

    export SPARK_HOME="/path/to/spark/installation"
    export MASTER="local[*]"
    bin/sparkling-shell

2.  To see the Sparkling shell (i.e., Spark driver) status, go to http://localhost:4040/.

3.  Initialize H2O services on top of Spark cluster:

.. code:: scala

    import org.apache.spark.h2o._
    val hc = H2OContext.getOrCreate(spark)
    import hc.implicits._
    import spark.implicits._

4.  Load weather data for Chicago international airport (ORD):

.. code:: scala

    val weatherDataFile = "examples/smalldata/chicago/Chicago_Ohare_International_Airport.csv"
    val weatherTable = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(weatherDataFile)
      .withColumn("Date", to_date('Date, "MM/dd/yyyy"))
      .withColumn("Year", year('Date))
      .withColumn("Month", month('Date))
      .withColumn("DayofMonth", dayofmonth('Date))

5.  Load airlines data using the H2O parser:

.. code:: scala

    import java.io.File
    val dataFile = "examples/smalldata/airlines/allyears2k_headers.zip"
    val airlinesH2OFrame = new H2OFrame(new File(dataFile))

6.  Select flights destined for Chicago (ORD):

.. code:: scala

    val airlinesTable = hc.asDataFrame(airlinesH2OFrame)
    val flightsToORD = airlinesTable.filter('Dest === "ORD")

7.  Compute the number of these flights:

.. code:: scala

    flightsToORD.count

8.  Join the flights data frame with the weather data frame:

.. code:: scala

    val joinedDf = flightsToORD.join(weatherTable, Seq("Year", "Month", "DayofMonth"))

9. Transform the columns containing date information into enum columns:

.. code:: scala

    import water.support.H2OFrameSupport._
    val joinedHf = columnsToCategorical(hc.asH2OFrame(joinedDf), Array("Year", "Month", "DayofMonth"))

10. Run deep learning to produce a model estimating arrival delay:

.. code:: scala

    import _root_.hex.deeplearning.DeepLearning
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
    import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
    val dlParams = new DeepLearningParameters()
    dlParams._train = joinedHf
    dlParams._response_column = "ArrDelay"
    dlParams._epochs = 5
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](100, 100)

    // Create a job
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

11. Use the model to estimate the delay on the training data:

.. code:: scala

    val predictionsHf = dlModel.score(joinedHf)
    val predictionsDf = hc.asDataFrame(predictionsHf)

12. Generate an R-code producing residual plot:

The generated code can be run in RStudio to produce residual plots.

.. code:: scala

    import org.apache.spark.examples.h2o.AirlinesWithWeatherDemo2.residualPlotRCode
    residualPlotRCode(predictionsHf, "predict", joinedDf, "ArrDelay", hc)


.. Links to the examples

.. |CraigslistJobTitlesStreamingApp| replace:: `CraigslistJobTitlesStreamingApp <src/main/scala/org/apache/spark/examples/h2o/CraigslistJobTitlesStreamingApp.scala>`__
.. |CraigslistJobTitlesApp| replace:: `CraigslistJobTitlesApp <src/main/scala/org/apache/spark/examples/h2o/CraigslistJobTitlesApp.scala>`__
.. |ChicagoCrimeAppSmall| replace:: `ChicagoCrimeAppSmall <src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeAppSmall.scala>`__
.. |ChicagoCrimeApp| replace:: `ChicagoCrimeApp <src/main/scala/org/apache/spark/examples/h2o/ChicagoCrimeApp.scala>`__
.. |CitiBikeSharingDemo| replace:: `CitiBikeSharingDemo <src/main/scala/org/apache/spark/examples/h2o/CitiBikeSharingDemo.scala>`__
.. |HamOrSpamDemo| replace:: `HamOrSpamDemo <src/main/scala/org/apache/spark/examples/h2o/HamOrSpamDemo.scala>`__
.. |ProstateDemo| replace:: `ProstateDemo <src/main/scala/org/apache/spark/examples/h2o/ProstateDemo.scala>`__
.. |DeepLearningDemo| replace:: `DeepLearningDemo <src/main/scala/org/apache/spark/examples/h2o/DeepLearningDemo.scala>`__
.. |AirlinesWithWeatherDemo| replace:: `AirlinesWithWeatherDemo <src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo.scala>`__
.. |AirlinesWithWeatherDemo2| replace:: `AirlinesWithWeatherDemo2 <src/main/scala/org/apache/spark/examples/h2o/AirlinesWithWeatherDemo2.scala>`__
.. |chicagoCrimeSmallShellScript| replace:: `chicagoCrimeSmallShell.script.scala <scripts/chicagoCrimeSmallShell.script.scala>`__
.. |chicagoCrimeSmallScript| replace:: `chicagoCrimeSmall.script.scala <scripts/chicagoCrimeSmall.script.scala>`__
.. |hamOrSpamScript| replace:: `hamOrSpam.script.scala <scripts/hamOrSpam.script.scala>`__
.. |strata2015Script| replace:: `strata2015.script.scala <scripts/strata2015.script.scala>`__
.. |StrataAirlinesScript| replace:: `StrataAirlines.script.scala <scripts/StrataAirlines.script.scala>`__
.. |ProstateDataset| replace:: `prostate dataset <smalldata/prostate/prostate.csv>`__
.. |AirlinesDataset| replace:: `airlines dataset <smalldata/airlines/allyears2k_headers.zip>`__
.. |ChicagoDataset| replace:: `chicago datasets <smalldata/chicago/>`__