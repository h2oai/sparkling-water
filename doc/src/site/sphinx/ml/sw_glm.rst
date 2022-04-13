Train GLM Model in Sparkling Water
----------------------------------
Introduction
~~~~~~~~~~~~

Generalized Linear Models (GLM) estimate regression models for outcomes following exponential distributions.
In addition to the Gaussian (i.e. normal) distribution, these include Poisson, binomial, and gamma distributions.
Each serves a different purpose, and depending on distribution and link function choice, can be used either for prediction or classification.
For more more comprehensive description see `H2O-3 GLM documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/glm.html>`__.

Example
~~~~~~~

The following section describes how to train the GLM model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2OGLM` and :ref:`model_details_H2OGLMMOJOModel`.

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
            val datasetUrl = "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv"
            spark.sparkContext.addFile(datasetUrl) //for example purposes, on a real cluster it's better to load directly from distributed storage
            val rawSparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val sparkDF = rawSparkDF.withColumn("CAPSULE", $"CAPSULE" cast "string")
                                     .withColumn("RACE", $"RACE" cast "string")
                                     .withColumn("DCAPS", $"DCAPS" cast "string")
                                     .withColumn("DPROS", $"DPROS" cast "string")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available GLM arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGLM

            val predictors = Array("AGE", "RACE", "VOL", "GLEASON")
            val response = "CAPSULE"

            val estimator = new H2OGLM()
              .setFamily("binomial")
              .setFeaturesCols(predictors)
              .setLabelCol(response)
              .setLambdaValue(Array(0))
              .setComputePValues(true)

            val model = estimator.fit(trainingDF)

        Note: When family is not set, by default, by default, the ``H2OGLM`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2OGLMClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2OGLMRegressor`` instead.

        Print the coefficients table

        .. code:: scala

            model.getCoefficients().show(truncate = false)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(truncate = false)

        You can also get model details via calling methods listed in :ref:`model_details_H2OGLMMOJOModel`.


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
            sparkDF = sparkDF.withColumn("RACE", sparkDF.RACE.cast("string"))
            sparkDF = sparkDF.withColumn("DCAPS", sparkDF.DCAPS.cast("string"))
            sparkDF = sparkDF.withColumn("DPROS", sparkDF.DPROS.cast("string"))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available GLM arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OGLM

            predictors = ["AGE", "RACE", "VOL", "GLEASON"]
            response = "CAPSULE"

            estimator = H2OGLM(family="binomial",
                 featuresCols=predictors,
                 labelCol=response,
                 computePValues=True,
                 lambdaValue=[0])

            model = estimator.fit(trainingDF)

        Note: When family is not set, by default, the ``H2OGLM`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2OGLMClassifier`` or ``H2OGLMRegressor`` instead.

        Print the coefficients table

        .. code:: python

            model.getCoefficients().show(truncate = False)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2OGLMMOJOModel`.
