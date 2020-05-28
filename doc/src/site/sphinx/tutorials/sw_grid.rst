Train Sparkling Water Algorithms with Grid Search
-------------------------------------------------

Grid Search serves for finding optimal values for hyper parameters of a given H2O/SW algorithm. Grid Search in Sparkling Water
is able to traverse hyper space for H2OGBM, H2OXGBoost, H2ODRF, H2OGLM, H2ODeepLearning. For more details about hyper parameters
for a specific algorithm (see `H2O-3 documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/grid-search.html#supported-grid-search-hyperparameters>`__).


Sparkling Water provides API in Scala and Python for Grid Search. The following sections describe how to Apply Grid Search on
H2ODRF in both languages.

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

            val frame = H2OFrame(new URI("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv"))
            val sparkDF = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available DRF arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2ODRF
            val algorithm = new H2ODRF().setLabelCol("CAPSULE")


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
            algorithm = H2ODRF(labelCol = "CAPSULE")

