Train Stacked Ensemble Model in Sparkling Water
-----------------------------------------------

Stacked Ensemble is a supervised machine learning algorithm that finds an optimal combination of a collection
of prediction algorithms (base models). For further details about the algorithm and its parameters see `H2O-3 documentation
<https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/stacked-ensembles.html>`__.


Sparkling Water provides API in Scala and Python for Stacked Ensemble. The following sections describe how to
utilize Stacked Ensemble in both languages. See also :ref:`parameters_H2OStackedEnsemble`.

.. |start cluster| replace:: Start H2O cluster inside the Spark environment

.. |get data| replace:: Parse the data using H2O and convert them to Spark Frame

.. |setup base algorithms| replace:: Setup the algorithms the StackedEnsemble will operate with. StackedEnsemble will
    automatically train the corresponding (base) models and pass them to H2O backend when needed. There are currently
    two options how a meta-learner in StackedEnsemble combines the base models. It either utilizes cross validated
    predictions or uses a blending frame. In the former case, it's important to keep the same folding across
    the base models and set *setKeepCrossValidationPredictions* to *true* as the cross-validated predicted values
    will be used by meta-learner. Furthermore, as the Stacked Ensemble combines the base models inside an H2O backend
    the base models have to be available there as well and therefore *setKeepBinaryModels* has to be set to *true* too.

.. |setup algorithm and train| replace:: Then, specify the algorithms when setting up the StackedEnsemble and train it.

.. |get details| replace:: You can also get raw model details by calling the *getModelDetails()* method
    available on the model as:

.. |run predictions| replace:: Run Predictions

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        |start cluster|

        .. code:: scala

            import ai.h2o.sparkling._
            import java.net.URI
            val hc = H2OContext.getOrCreate()

        |get data|

        .. code:: scala

            import org.apache.spark.SparkFiles
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            val rawSparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val dataset = rawSparkDF.withColumn("CAPSULE", $"CAPSULE" cast "string")

        |setup base algorithms|

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.{H2ODRF, H2OGBM, H2OStackedEnsemble}
            val drf = new H2ODRF()
                .setLabelCol("CAPSULE")
                .setNfolds(5)
                .setFoldAssignment("Modulo")
                .setKeepBinaryModels(true)
                .setKeepCrossValidationPredictions(true)

            val gbm = new H2OGBM()
              .setLabelCol("CAPSULE")
              .setNfolds(5)
              .setFoldAssignment("Modulo")
              .setKeepBinaryModels(true)
              .setKeepCrossValidationPredictions(true)

        |setup algorithm and train|

        .. code:: scala

            val ensemble = new H2OStackedEnsemble()
                .setBaseAlgorithms(Array(drf, gbm))
                .setLabelCol("CAPSULE")

            ensemble.fit(dataset)

        |get details|

        .. code:: scala

            ensembleModel.getModelDetails()

        |run predictions|

        .. code:: scala

            ensembleModel.transform(testingDF).show(false)


    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell as

        .. code:: shell

            ./bin/pysparkling

        |start cluster|

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        |get data|

        .. code:: python

            import h2o
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            dataset = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))

        |setup base algorithms|

        .. code:: python

            from pysparkling.ml import H2ODRF, H2OGBM, H2OStackedEnsemble
            drf = H2ODRF()
            drf.setLabelCol("CAPSULE")
            drf.setNfolds(5)
            drf.setFoldAssignment("Modulo")
            drf.setKeepBinaryModels(True)
            drf.setKeepCrossValidationPredictions(True)

            gbm = H2OGBM()
            gbm.setLabelCol("CAPSULE")
            gbm.setNfolds(5)
            gbm.setFoldAssignment("Modulo")
            gbm.setKeepBinaryModels(True)
            gbm.setKeepCrossValidationPredictions(True)

        |setup algorithm and train|

        .. code:: python

            ensemble = H2OStackedEnsemble()
            ensemble.setBaseAlgorithms([drf, gbm])
            ensemble.setLabelCol("CAPSULE")

            ensemble_model = ensemble.fit(dataset)

        |get details|

        .. code:: python

            ensemble_model.getModelDetails()

        |run predictions|

        .. code:: python

            ensemble_model.transform(testingDF).show(truncate = False)
