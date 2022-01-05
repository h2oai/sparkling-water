Autoencoder in Sparkling Water
==============================
Autoencoder in Sparkling Water is based on H2O-3's Deep Learning algorithm and can be used for encoding an arbitrary
list of features to the vector numerical values and for anomaly detection. Sparkling Water provides API for
Autoencoder in Scala and Python. The following sections describe how to train and use
the Autoencoder model in Sparkling Water in both languages. See also :ref:`parameters_H2OAutoEncoder`
and :ref:`model_details_H2OAutoEncoderMOJOModel`.

Prepare Sparkling Water Environment
-----------------------------------

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

Use Case 1: Feature Encoding
----------------------------
``H2OAutoEncoder`` is implemented as a Spark feature estimator, and thus can be used as a stage in a Spark pipeline.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Create `H2OAutoEncoder`` and set input columns, the neural network architecture and other parameters
        (see :ref:`parameters_H2OAutoEncoder`).

        .. code:: scala

            import ai.h2o.sparkling.ml.features.H2OAutoEncoder
            val autoEncoder = new H2OAutoEncoder()
            autoEncoder.setInputCols(Array("AGE", "RACE", "DPROS", "DCAPS"))
            autoEncoder.setHidden(Array(100))

        Define other pipeline stages.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGBM
            val gbm = new H2OGBM()
            gbm.setFeaturesCol(autoEncoder.getOutputCol())
            gbm.setLabelCol("CAPSULE")

        Construct and fit the pipeline.

        .. code:: scala

            import org.apache.spark.ml.Pipeline
            val pipeline = new Pipeline().setStages(Array(autoEncoder, gbm))
            val model = pipeline.fit(trainingDF)

        Now, you can score with the pipeline model.

        .. code:: scala

            val resultDF = model.transform(testingDF)
            resultDF.show(truncate=false)

    .. tab-container:: Python
        :title: Python

        Create `H2OAutoEncoder`` and set input columns, the neural network architecture and other parameters
        (see :ref:`parameters_H2OAutoEncoder`).

        .. code:: python

            from pysparkling.ml import H2OAutoEncoder
            autoEncoder = H2OAutoEncoder()
            autoEncoder.setInputCols(["AGE", "RACE", "DPROS", "DCAPS"])
            autoEncoder.setHidden([100,])

        Define other pipeline stages.

        .. code:: python

            from pysparkling.ml import H2OGBM
            gbm = H2OGBM()
            gbm.setFeaturesCols([autoEncoder.getOutputCol()])
            gbm.setLabelCol("CAPSULE")

        Construct and fit the pipeline.

        .. code:: python

            from pyspark.ml import Pipeline
            pipeline = Pipeline(stages = [autoEncoder, gbm])
            model = pipeline.fit(trainingDF)

        Now, you can score with the pipeline model.

        .. code:: python

            resultDF = model.transform(testingDF)
            resultDF.show(truncate=False)

Use Case 2: Anomaly Detection
-----------------------------

To use ``H2OAutoEncoder`` for the anomaly detection problem, ``H2OAutoEncoder`` or its MOJO model must be configured to
produce a column with mean square errors ("MSE"). The errors are calculated from the output column and the original column,
which represents a numerical input to the neural network of ``H2OAutoEncoder``.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Create ``H2OAutoEncoder``, enable MSE column and optionally the original column

        .. code:: scala

            import ai.h2o.sparkling.ml.features.H2OAutoEncoder
            val autoEncoder = new H2OAutoEncoder()
            autoEncoder.setInputCols(Array("RACE", "DPROS", "DCAPS"))
            autoEncoder.setOutputCol("Output")
            autoEncoder.setWithOriginalCol(true)
            autoEncoder.setOriginalCol("Original")
            autoEncoder.setWithMSECol(true)
            autoEncoder.setMSECol("MSE")
            autoEncoder.setHidden(Array(3))
            autoEncoder.setSplitRatio(0.8)

        Train the auto encoder model.

        .. code:: scala

            val model = autoEncoder.fit(trainingDF)

        Specify MSE threshold, score with the trained model and identify outliers

        .. code:: scala

            val threshold = 0.1
            val scoredDF = model.transform(testingDF)
            import org.apache.spark.sql.functions.col
            val outliersDF = scoredDF.filter(col("MSE") > threshold)
            outliersDF.show(truncate=false)

        The overall performance of the auto encoder model can be checked by seeing training and validation metrics (MSE, RMSE).
        The validation metrics are available only if a validation data frame or split ration is set.

        .. code:: scala

            println(model.getTrainingMetrics())
            println(model.getValidationMetrics())

        The same thing can be achieved with an auto encoder MOJO model loaded from a file, but the MSE column
        (and the original column) needs to be explicitly enabled.

        .. code:: scala

            import ai.h2o.sparkling.ml.models.H2OAutoEncoderMOJOModel
            val model = H2OAutoEncoderMOJOModel.createFromMojo("path/to/auto_encoder_model.mojo")
            model.setOutputCol("Output")
            model.setWithOriginalCol(true)
            model.setOriginalCol("Original")
            model.setWithMSECol(true)
            model.setMSECol("MSE")

    .. tab-container:: Python
        :title: Python

        Create ``H2OAutoEncoder``, enable MSE column and optionally the original column

        .. code:: python

            from pysparkling.ml import H2OAutoEncoder
            autoEncoder = H2OAutoEncoder()
            autoEncoder.setInputCols(["RACE", "DPROS", "DCAPS"])
            autoEncoder.setOutputCol("Output")
            autoEncoder.setWithOriginalCol(True)
            autoEncoder.setOriginalCol("Original")
            autoEncoder.setWithMSECol(True)
            autoEncoder.setMSECol("MSE")
            autoEncoder.setHidden([3,])
            autoEncoder.setSplitRatio(0.8)

        Train the auto encoder model.

        .. code:: python

            model = autoEncoder.fit(trainingDF)

        Specify MSE threshold, score with the trained model and identify outliers.

        .. code:: python

            threshold = 0.1
            scoredDF = model.transform(testingDF)
            from pyspark.sql.functions import col
            outliersDF = scoredDF.filter(col("MSE") > threshold)
            outliersDF.show(truncate=False)

        The overall performance of the auto encoder model can be checked by seeing training and validation metrics (MSE, RMSE).
        The validation metrics are available only if a validation data frame or split ration is set.

        .. code:: python

            print(model.getTrainingMetrics())
            print(model.getValidationMetrics())

        The same thing can be achieved with an auto encoder MOJO model loaded from a file, but the MSE column
        (and the original column) needs to be explicitly enabled.

        .. code:: python

            from pysparkling.ml import H2OAutoEncoderMOJOModel
            model = H2OAutoEncoderMOJOModel.createFromMojo("path/to/auto_encoder_model.mojo")
            model.setOutputCol("Output")
            model.setWithOriginalCol(True)
            model.setOriginalCol("Original")
            model.setWithMSECol(True)
            model.setMSECol("MSE")
