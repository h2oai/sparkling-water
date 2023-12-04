.. _extended_isolation_forest:

Train Distributed Uplift Random Forest (Uplift DRF) Model in Sparkling Water
--------------------------------------------------------

Introduction
~~~~~~~~~~~~
Distributed Uplift Random Forest (Uplift DRF) is a classification tool for modeling uplift - the incremental impact of a treatment. Only binomial classification (distribution="bernoulli") is currently supported.
Uplift DRF can be applied in fields where we operate with two groups of subjects. First group, let’s call it treatment, receive some kind of treatment (e.g. marketing campaign, medicine,…), and a second group, let’s call it control, is separated from the treatment. We also gather information about their response, whether they bought a product, recover from disease, or similar. Then, Uplift DRF trains so-called uplift trees.
For more comprehensive description see `H2O-3 Distributed Uplift Random Forest (Uplift DRF) documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/upliftdrf.html>`__.

Example
~~~~~~~

The following section describes how to train the Distributed Uplift Random Forest (Uplift DRF) model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2OUpliftDRF`
and :ref:`model_details_H2OUpliftDRFMOJOModel`.

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
            val datasetUrl = "https://s3.amazonaws.com/h2o-public-test-data/smalldata/uplift/criteo_uplift_13k.csv"
            spark.sparkContext.addFile(datasetUrl) //for example purposes, on a real cluster it's better to load directly from distributed storage
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available Distributed Uplift Random Forest (Uplift DRF) arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OUpliftDRF

            val predictorColumns = Array("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8")
            val responseColumn = "conversion"
            val treatmentColumn = "treatment"

            val algo = new H2OUpliftDRF()
               .setNtrees(10)
               .setMaxDepth(5)
               .setTreatmentCol(treatmentColumn)
               .setUpliftMetric("KL")
               .setMinRows(10)
               .setSeed(1234)
               .setAuucType("qini")
               .setLabelCol(responseColumn)
               .setFeaturesCols(predictorColumns :+ treatmentColumn :+ responseColumn)

            val model = algo.fit(trainingDF)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(truncate = false)

        View model summary containing info about trained trees etc.

        .. code:: scala

            model.getModelSummary()

        You can also get other model details by calling methods listed in :ref:`model_details_H2OUpliftDRFMOJOModel`.


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
            frame = h2o.import_file("https://s3.amazonaws.com/h2o-public-test-data/smalldata/uplift/criteo_uplift_13k.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available UpliftDRF arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OUpliftDRF

            predictors = ["f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "treatment", "conversion"]

            algo = H2OUpliftDRF(featuresCols=predictors,
                                ntrees = 10,
                                maxDepth = 5,
                                treatmentCol = treatmentColumn,
                                upliftMetric = "KL",
                                minRows = 10,
                                seed = 1234,
                                auucType = "qini",
                                labelCol = responseColumn)

            model = algo.fit(trainingDF)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        View model summary containing info about trained trees etc.

        .. code:: python

            model.getModelSummary()

        You can also get other model details by calling methods listed in :ref:`model_details_H2OUpliftDRFMOJOModel`.
