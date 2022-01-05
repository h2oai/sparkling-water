.. _isolation_forest:

Train Isolation Forest Model in Sparkling Water
-----------------------------------------------

Sparkling Water provides API for H2O Isolation Forest in Scala and Python.
The following sections describe how to train the Isolation Forest model in Sparkling Water in both languages.
See also :ref:`parameters_H2OIsolationForest` and :ref:`model_details_H2OIsolationForestMOJOModel`.

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

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        You can also get model details via calling methods listed in :ref:`model_details_H2OIsolationForestMOJOModel`.


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

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2OIsolationForestMOJOModel`.


Train Isolation Forest with H2OGridSearch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're not sure about exact values for hyper-parameters of Isolation Forest, you can plug ``H2OIsolationForest`` to
``H2OGridSearch`` and define a hyper-parameter space to be walked through. Unlike other Sparkling Water algorithms used in
``H2OGridSearch``, you must pass ``validationDataFrame`` to ``H2OIsolationForest`` as a parameter in order to
``H2OGridSearch`` be able to evaluate produced models. The validation data frame has to have an extra column identifying
whether the row represents an anomaly or not. The column can contain only two string values, where a value for the negative
case, must be alphabetically smaller then the value for the positive case. E.g.: ``"0"``/``"1"``, ``"no"``/``"yes"``,
``"false"``/``"true"``, etc.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Let's load a training and validation dataset at first:

        .. code:: scala

            import org.apache.spark.SparkFiles
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate_anomaly_validation.csv")
            val trainingDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val validationDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate_anomaly_validation.csv"))

        Create an algorithm instance, pass validation data frame, and specify a column identifying an anomaly:

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OIsolationForest
            val algorithm = new H2OIsolationForest()
            algorithm.setValidationDataFrame(validationDF)
            algorithm.setValidationLabelCol("isAnomaly")

        Define a hyper-parameter space:

        .. code:: scala

            import scala.collection.mutable
            val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
            hyperParams += "ntrees" -> Array(10, 20, 30).map(_.asInstanceOf[AnyRef])
            hyperParams += "maxDepth" -> Array(5, 10, 20).map(_.asInstanceOf[AnyRef])

        Pass the prepared hyper-parameter space and algorithm to ``H2OGridSearch`` and run it:

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGridSearch
            val grid = new H2OGridSearch()
            grid.setAlgo(algorithm)
            grid.setHyperParameters(hyperParams)
            val model = grid.fit(trainingDF)

        ``Logloss`` is a default metric for the model comparision produced by grids and can be changed via the method
        ``setSelectBestModelBy`` on ``H2OGridSearch``.

    .. tab-container:: Python
        :title: Python

        Let's load a training and validation dataset at first:

        .. code:: python

            import h2o
            trainingFrame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            trainingDF = hc.asSparkFrame(trainingFrame)
            validationFrame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate_anomaly_validation.csv")
            validationDF = hc.asSparkFrame(validationFrame)

        Create an algorithm instance, pass validation data frame, and specify a column identifying an anomaly:

        .. code:: python

            from pysparkling.ml import H2OIsolationForest
            algorithm = H2OIsolationForest(validationDataFrame=validationDF, validationLabelCol="isAnomaly")

        Define a hyper-parameter space:

        .. code:: python

            hyperSpace = {"ntrees": [10, 20, 30], "maxDepth": [5, 10, 20]}

        Pass the prepared hyper-parameter space and algorithm to ``H2OGridSearch`` and run it:

        .. code:: python

            from pysparkling.ml import H2OGridSearch
            grid = H2OGridSearch(hyperParameters=hyperSpace, algo=algorithm)
            model = grid.fit(trainingDF)

        ``Logloss`` is a default metric for the model comparision produced by grids and can be changed via the method
        ``setSelectBestModelBy`` on ``H2OGridSearch``.
