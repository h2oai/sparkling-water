.. _isolation_forest:

Train Isolation Forest Model in Sparkling Water
-----------------------------------------------

Sparkling Water provides API for H2O Isolation Forest in Scala and Python.
The following sections describe how to train the Isolation Forest model in Sparkling Water in both languages.
See also :ref:`parameters_H2OIsolationForest`.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        Start H2O cluster inside the Spark environment

        .. code:: scala

            import ai.h2o.sparkling._
            import java.net.URI
            val hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame

        .. code:: scala

	        import org.apache.spark.SparkFiles
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/anomaly/ecg_discord_train.csv")
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/anomaly/ecg_discord_test.csv")
	        val trainingDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("ecg_discord_train.csv"))
            val testingDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("ecg_discord_test.csv"))

        Train the model. You can configure all the available Isolation Forest arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OIsolationForest
            val estimator = new H2OIsolationForest()
            val model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: scala

            model.getModelDetails()

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)


    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell as

        .. code:: shell

            ./bin/pysparkling

        Start H2O cluster inside the Spark environment

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame

        .. code:: python

            import h2o
            trainingFrame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/anomaly/ecg_discord_train.csv")
            trainingDF = hc.asSparkFrame(trainingFrame)
            testingFrame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/anomaly/ecg_discord_test.csv")
            testingDF = hc.asSparkFrame(testingFrame)

        Train the model. You can configure all the available Isolation Forest arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OIsolationForest
            estimator = H2OIsolationForest()
            model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            model.getModelDetails()

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)
