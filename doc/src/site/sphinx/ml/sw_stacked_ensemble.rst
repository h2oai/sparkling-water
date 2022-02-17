Train Stacked Ensemble Model in Sparkling Water
-----------------------------------------------

Stacked Ensemble is a supervised machine learning algorithm that finds an optimal combination of a collection
of prediction algorithms (base models). For further details about the algorithm and its parameters see `H2O-3 documentation
<https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/stacked-ensembles.html>`__.


Sparkling Water provides API in Scala and Python for Stacked Ensemble. The following sections describe how to
utilize Stacked Ensemble in both languages. See also :ref:`parameters_H2OStackedEnsemble`.

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
            val dataset = rawSparkDF.withColumn("CAPSULE", $"CAPSULE" cast "string")

        Train the base models the Stack Ensemble will operate on. It's important to keep the same folding across
        the base models. Furthermore, setKeepCrossValidationPredictions has to set to *true*, as the cross-validated
        predicted values are used internally by Stacked Ensemble (for metalearning). Moreover, as the Stacked Ensemble
        combines the base models inside an H2O cluster, the base models have to be available there and therefore
        setKeepBinaryModels has to be set to *true* as well.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.{H2ODRF, H2OGBM, H2OStackedEnsemble}
            val drf = new H2ODRF()
                .setLabelCol("CAPSULE")
                .setNfolds(5)
                .setFoldAssignment("Modulo")
                .setKeepBinaryModels(true)
                .setKeepCrossValidationPredictions(true)
            val drfModel = drf.fit(dataset)

            val gbm = new H2OGBM()
              .setLabelCol("CAPSULE")
              .setNfolds(5)
              .setFoldAssignment("Modulo")
              .setKeepBinaryModels(true)
              .setKeepCrossValidationPredictions(true)
            val gbmModel = gbm.fit(dataset)

        Then, train a stacked ensemble using the (base) models available.

        .. code:: scala

            val ensemble = new H2OStackedEnsemble()
                .setBaseModels(Seq(drfModel, gbmModel))
                .setLabelCol("CAPSULE")

            val ensembleModel = ensemble.fit(dataset)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: scala

            ensembleModel.getModelDetails()

        Run Predictions

        .. code:: scala

            ensembleModel.transform(testingDF).show(false)


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
            dataset = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))

        Train the base models the Stack Ensemble will operate on. It's important to keep the same folding across
        the base models. Furthermore, setKeepCrossValidationPredictions has to set to *true*, as the cross-validated
        predicted values are used internally by Stacked Ensemble (for metalearning). Moreover, as the Stacked Ensemble
        combines the base models inside an H2O cluster, the base models have to be available there and therefore
        setKeepBinaryModels has to be set to *true* as well.

        .. code:: python

            from pysparkling.ml import H2ODRF, H2OGBM, H2OStackedEnsemble
            drf = H2ODRF()
            drf.setLabelCol("CAPSULE")
            drf.setNfolds(5)
            drf.setFoldAssignment("Modulo")
            drf.setKeepBinaryModels(True)
            drf.setKeepCrossValidationPredictions(True)
            drf_model = drf.fit(dataset)

            gbm = H2OGBM()
            gbm.setLabelCol("CAPSULE")
            gbm.setNfolds(5)
            gbm.setFoldAssignment("Modulo")
            gbm.setKeepBinaryModels(True)
            gbm.setKeepCrossValidationPredictions(True)
            gbm_model = gbm.fit(dataset)

        Then, train a stacked ensemble using the (base) models available.

        .. code:: python

            ensemble = H2OStackedEnsemble()
            ensemble.setBaseModels([drf_model, gbm_model])
            ensemble.setLabelCol("CAPSULE")

            ensemble_model = ensemble.fit(dataset)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            ensemble_model.getModelDetails()

        Run Predictions

        .. code:: python

            ensemble_model.transform(testingDF).show(truncate = False)
