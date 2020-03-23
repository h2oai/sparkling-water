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
| |ChicagoCrimeAppSmallScript|      | Demo showing full source code of predicting arrest probability for a     |
|                                   | given crime. It covers whole machine learning process from loading and   |
|                                   | transforming data, building models, scoring incoming events.             |
+-----------------------------------+--------------------------------------------------------------------------+
| |hamOrSpamScript|                 | HamOrSpam application which detects Spam messages. Presented at          |
|                                   | MLConf 2015 NYC.                                                         |
+-----------------------------------+--------------------------------------------------------------------------+
| |CityBikeSharingScript|           | NYC Bike Sharing Demo.                                                   |
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
    val hc = H2OContext.getOrCreate()
    import spark.implicits._

4.  Load weather data for Chicago international airport (ORD):

.. code:: scala

    val weatherDataFile = "examples/smalldata/chicago/Chicago_Ohare_International_Airport.csv"
    val weatherTable = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(weatherDataFile)
      .withColumn("Date", to_date(regexp_replace('Date, "(\\d+)/(\\d+)/(\\d+)", "$3-$2-$1")))
      .withColumn("Year", year('Date))
      .withColumn("Month", month('Date))
      .withColumn("DayofMonth", dayofmonth('Date))

5.  Load airlines data:

.. code:: scala

    val airlinesDataFile = "examples/smalldata/airlines/allyears2k_headers.csv"
    val airlinesTable = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .csv(airlinesDataFile)

6.  Select flights destined for Chicago (ORD):

.. code:: scala

    val flightsToORD = airlinesTable.filter('Dest === "ORD")

7.  Compute the number of these flights:

.. code:: scala

    flightsToORD.count

8.  Join the flights data frame with the weather data frame:

.. code:: scala

    val joined = flightsToORD.join(weatherTable, Seq("Year", "Month", "DayofMonth"))

9. Run deep learning to produce a model estimating arrival delay:

.. code:: scala

    import ai.h2o.sparkling.ml.algos.H2ODeepLearning
    val dl = new H2ODeepLearning()
        .setLabelCol("ArrDelay")
        .setColumnsToCategorical(Array("Year", "Month", "DayofMonth"))
        .setEpochs(5)
        .setActivation("RectifierWithDropout")
        .setHidden(Array(100, 100))

    val model = dl.fit(joined)

11. Use the model to estimate the delay on the training data:

.. code:: scala

    val predictions = model.transform(joined)


.. Links to the examples

.. |CraigslistJobTitlesStreamingApp| replace:: `CraigslistJobTitlesStreamingApp <src/main/scala/ai/h2o/sparkling/examples/CraigslistJobTitlesStreamingApp.scala>`__
.. |CraigslistJobTitlesApp| replace:: `CraigslistJobTitlesApp <src/main/scala/ai/h2o/sparkling/examples/CraigslistJobTitlesApp.scala>`__
.. |ChicagoCrimeAppSmall| replace:: `ChicagoCrimeAppSmall <src/main/scala/ai/h2o/sparkling/examples/ChicagoCrimeAppSmall.scala>`__
.. |ChicagoCrimeApp| replace:: `ChicagoCrimeApp <src/main/scala/ai/h2o/sparkling/examples/ChicagoCrimeApp.scala>`__
.. |CitiBikeSharingDemo| replace:: `CitiBikeSharingDemo <src/main/scala/ai/h2o/sparkling/examples/CitiBikeSharingDemo.scala>`__
.. |HamOrSpamDemo| replace:: `HamOrSpamDemo <src/main/scala/ai/h2o/sparkling/examples/HamOrSpamDemo.scala>`__
.. |ProstateDemo| replace:: `ProstateDemo <src/main/scala/ai/h2o/sparkling/examples/ProstateDemo.scala>`__
.. |DeepLearningDemo| replace:: `DeepLearningDemo <src/main/scala/ai/h2o/sparkling/examples/DeepLearningDemo.scala>`__
.. |AirlinesWithWeatherDemo| replace:: `AirlinesWithWeatherDemo <src/main/scala/ai/h2o/sparkling/examples/AirlinesWithWeatherDemo.scala>`__
.. |AirlinesWithWeatherDemo2| replace:: `AirlinesWithWeatherDemo2 <src/main/scala/ai/h2o/sparkling/examples/AirlinesWithWeatherDemo2.scala>`__
.. |ChicagoCrimeAppSmallScript| replace:: `ChicagoCrimeAppSmall.script.scala <scripts/ChicagoCrimeAppSmall.script.scala>`__
.. |hamOrSpamScript| replace:: `hamOrSpam.script.scala <scripts/hamOrSpam.script.scala>`__
.. |CityBikeSharingScript| replace:: `CityBikeSharing.script.scala <scripts/CityBikeSharing.script.scala>`__
.. |StrataAirlinesScript| replace:: `StrataAirlines.script.scala <scripts/StrataAirlines.script.scala>`__
.. |ProstateDataset| replace:: `prostate dataset <smalldata/prostate/prostate.csv>`__
.. |AirlinesDataset| replace:: `airlines dataset <smalldata/airlines/allyears2k_headers.csv>`__
.. |ChicagoDataset| replace:: `chicago datasets <smalldata/chicago/>`__