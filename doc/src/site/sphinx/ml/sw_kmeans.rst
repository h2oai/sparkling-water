Train KMeans Model in Sparkling Water
--------------------------------------

Introduction
~~~~~~~~~~~~
K-Means falls in the general category of clustering algorithms. For more more comprehensive description see `H2O-3 Deep learning documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/k-means.html>`__.

Example
~~~~~~~

The following section describes how to train the KMeans model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2OKMeans`
and :ref:`model_details_H2OKMeansMOJOModel`.

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
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/iris/iris_wheader.csv")
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("iris_wheader.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Set the predictors

        .. code:: scala

            val predictors = Array("sepal_len", "sepal_wid", "petal_len", "petal_wid")

        Build and train the model. You can configure all the available KMeans arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OKMeans
            val estimator = new H2OKMeans()
               .setEstimateK(true)
               .setK(10)
               .setSeed(1234)
               .setFeaturesCols(predictors)
            val model = estimator.fit(trainingDF)

        Eval performance

        .. code:: scala

            val metrics = model.getTrainingMetrics()
            println(metrics)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        You can also get model details via calling methods listed in :ref:`model_details_H2OKmeansMOJOModel`.


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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/iris/iris_wheader.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Set the predictors

        .. code:: python

            predictors = ["sepal_len", "sepal_wid", "petal_len", "petal_wid"]

        Build and train the model. You can configure all the available KMeans arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OKMeans
            estimator = H2OKMeans(
                           estimateK = True,
                           k = 10,
                           seed = 1234,
                           featuresCols = predictors)
            model = estimator.fit(trainingDF)

        Eval performance

        .. code:: python

            metrics = model.getTrainingMetrics()
            print(metrics)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2OKmeansMOJOModel`.

