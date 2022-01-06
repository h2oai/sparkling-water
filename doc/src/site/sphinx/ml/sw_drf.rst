.. _drf:

Train DRF Model in Sparkling Water
----------------------------------

Sparkling Water provides API for H2O DRF in Scala and Python.
The following sections describe how to train the DRF model in Sparkling Water in both languages. See also :ref:`parameters_H2ODRF`
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
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            val rawSparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val sparkDF = rawSparkDF.withColumn("CAPSULE", $"CAPSULE" cast "string")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available DRF arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2ODRF
            val estimator = new H2ODRF().setLabelCol("CAPSULE")
            val model = estimator.fit(trainingDF)

        By default, the ``H2ODRF`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2ODRFClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2ODRFRegressor`` instead.

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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available DRF arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2ODRF
            estimator = H2ODRF(labelCol = "CAPSULE")
            model = estimator.fit(trainingDF)

        By default, the ``H2ODRF`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2ODRFClassifier`` or ``H2ODRFRegressor`` instead.

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2ODRFMOJOModel`.
