.. _extended_isolation_forest:

Train Extended Isolation Forest Model in Sparkling Water
--------------------------------------------------------

Introduction
~~~~~~~~~~~~
The Extended Isolation Forest algorithm generalizes its predecessor algorithm, Isolation Forest. The original Isolation Forest algorithm brings a brand new form of detection, although the algorithm suffers from bias due to tree branching. Extension of the algorithm mitigates the bias by adjusting the branching, and the original algorithm becomes just a special case.
For more comprehensive description see `H2O-3 Extended Isolation Forest documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/eif.html>`__.

Example
~~~~~~~

The following section describes how to train the Extended Isolation Forest model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2OExtendedIsolationForest`
and :ref:`model_details_H2OExtendedIsolationForestMOJOModel`.

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
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available Extended Isolation Forest arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OExtendedIsolationForest

            val predictors = Array("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")

            val algo = new H2OExtendedIsolationForest()
               .setSampleSize(256)
               .setNtrees(100)
               .setExtensionLevel(predictors.length - 1)
               .setSeed(1234)
               .setFeaturesCols(predictors)

            val model = algo.fit(trainingDF)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(truncate = false)

        View model summary containing info about trained trees etc.

        .. code:: scala

            model.getModelSummary()

        You can also get other model details by calling methods listed in :ref:`model_details_H2OExtendedIsolationForestMOJOModel`.


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
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available ExtendedIsolationForest arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OExtendedIsolationForest

            predictors = ["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]

            algo = H2OExtendedIsolationForest(featuresCols=predictors,
                                              sampleSize=256,
                                              ntrees=100,
                                              seed=1234,
                                              extensionLevel=len(predictors) - 1)

            model = algo.fit(trainingDF)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        View model summary containing info about trained trees etc.

        .. code:: python

            model.getModelSummary()

        You can also get other model details by calling methods listed in :ref:`model_details_H2OExtendedIsolationForestMOJOModel`.
