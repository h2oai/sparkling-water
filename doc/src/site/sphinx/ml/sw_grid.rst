Train Sparkling Water Algorithms with Grid Search
-------------------------------------------------

Grid Search serves for finding optimal values for hyper-parameters of a given H2O/SW algorithm. Grid Search in Sparkling Water
is able to traverse hyper-space for H2OGBM, H2OXGBoost, H2ODRF, H2OGLM, H2OGAM, H2ODeepLearning, H2OKMeans, H2OCoxPH,
and H2OIsolationForest.
For more details about hyper-parameters for a specific algorithm
(see `H2O-3 documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/grid-search.html#supported-grid-search-hyperparameters>`__).


Sparkling Water provides API in Scala and Python for Grid Search. The following sections describe how to Apply Grid Search on
H2ODRF in both languages. See also :ref:`parameters_H2OGridSearch`.

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

        Define the algorithm, which will be a subject of hyper-parameter tuning

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2ODRF
            val algo = new H2ODRF().setLabelCol("CAPSULE")

        By default, the ``H2ODRF`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2ODRFClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2ODRFRegressor`` instead.


        Define a hyper-space which will be traversed

        .. code:: scala

            import scala.collection.mutable.HashMap
            val hyperSpace: HashMap[String, Array[AnyRef]] = HashMap()
            hyperSpace += "ntrees" -> Array(1, 10, 30).map(_.asInstanceOf[AnyRef])
            hyperSpace += "mtries" -> Array(-1, 5, 10).map(_.asInstanceOf[AnyRef])

        Pass the algorithm and hyper-space to the grid search and set properties defining the way how the hyper-space will be traversed.

        Sparkling Water supports two strategies for traversing hyperspace:

        - **Cartesian** - (Default) This strategy tries out every possible combination of hyper-parameter values and
          finishes after the whole space is traversed.
        - **RandomDiscrete** - In each iteration, the strategy randomly selects the combination of values from the hyper-space and
          can be terminated before the whole space is traversed. The termination depends on various criteria
          (consider parameters: ``maxRuntimeSecs``, ``maxModels``, ``stoppingRounds``, ``stoppingTolerance``, ``stoppingMetric``).
          For details see `H2O-3 documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/grid-search.html>`_

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGridSearch
            val grid = new H2OGridSearch()
                .setHyperParameters(hyperSpace)
                .setAlgo(algo)
                .setStrategy("Cartesian")

        Fit the grid search to get the best DRF model.

        .. code:: scala

            val model = grid.fit(trainingDF)

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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available DRF arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2ODRF
            algo = H2ODRF(labelCol = "CAPSULE")

        By default, the ``H2ODRF`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2ODRFClassifier`` or ``H2ODRFRegressor`` instead.

        Define a hyper-space which will be traversed

        .. code:: python

            hyperSpace = {"ntrees": [1, 10, 30], "mtries": [-1, 5, 10]}

        Pass the algorithm and hyper-space to the grid search and set properties defining the way how the hyper-space will be traversed.

        Sparkling Water supports two strategies for traversing hyperspace:

        - **Cartesian** - (Default) This strategy tries out every possible combination of hyper-parameter values and
          finishes after the whole space is traversed.
        - **RandomDiscrete** - In each iteration, the strategy randomly selects the combination of values from the hyper-space and
          can be terminated before the whole space is traversed. The termination depends on various criteria
          (consider parameters: ``maxRuntimeSecs``, ``maxModels``, ``stoppingRounds``, ``stoppingTolerance``, ``stoppingMetric``).
          For details see `H2O-3 documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/grid-search.html>`_

        .. code:: python

            from pysparkling.ml import H2OGridSearch
            grid = H2OGridSearch(hyperParameters=hyperSpace, algo=algo, strategy="Cartesian")

        Fit the grid search to get the best DRF model.

        .. code:: python

            model = grid.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            model.getModelDetails()

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)
