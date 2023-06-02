.. _drf:

Train DRF Model in Sparkling Water
----------------------------------

Introduction
~~~~~~~~~~~~

Distributed Random Forest (DRF) is a powerful classification and regression tool. When given a set of data, DRF generates a forest of classification or regression trees, rather than a single classification or regression tree.
For more more comprehensive description see `H2O-3 DRF documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/drf.html>`__.

Example
~~~~~~~

The following section describes how to train the Distributed Random Forest model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2ODRF`
and :ref:`model_details_H2ODRFMOJOModel`.

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
            val datasetUrl = "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/cars_20mpg.csv"
            spark.sparkContext.addFile(datasetUrl) //for example purposes, on a real cluster it's better to load directly from distributed storage
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("cars_20mpg.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Set the predictors and response columns

        .. code:: scala

            val predictors = Array("displacement", "power", "weight", "acceleration", "year")
            val response = "economy_20mpg"

        Build and train the model. You can configure all the available DRF arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2ODRF
            val estimator = new H2ODRF()
                  .setNtrees(10)
                  .setMaxDepth(5)
                  .setMinRows(10)
                  .setCalibrateModel(true)
                  .setCalibrationDataFrame(testingDF)
                  .setBinomialDoubleTrees(true)
                  .setFeaturesCols(predictors)
                  .setLabelCol(response)
                  .setColumnsToCategorical(response) //set the response as a factor, please see the comment below
            val model = estimator.fit(trainingDF)

        By default, the ``H2ODRF`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2ODRFClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2ODRFRegressor`` instead.

        Eval performance

        .. code:: scala

            val metrics = model.getTrainingMetrics()
            println(metrics)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        You can also get model details via calling methods listed in :ref:`model_details_H2ODRFMOJOModel`.


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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/cars_20mpg.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Set the predictors and response columns

        .. code:: python

            predictors = ["displacement", "power","weight","acceleration","year"]
            response = "economy_20mpg"

        Train the model. You can configure all the available DRF arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2ODRF
            estimator = H2ODRF(
                            ntrees = 10,
                            maxDepth = 5,
                            minRows = 10,
                            calibrateModel = True,
                            calibrationDataFrame = testingDF,
                            binomialDoubleTrees = True,
                            featuresCols = predictors,
                            labelCol = response,
                            columnsToCategorical = [response])
            model = estimator.fit(trainingDF)

        By default, the ``H2ODRF`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2ODRFClassifier`` or ``H2ODRFRegressor`` instead.

        Eval performance

        .. code:: python

            metrics = model.getTrainingMetrics()
            print(metrics)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2ODRFMOJOModel`.
