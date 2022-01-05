Generalized Low Rank Models (GLRM) in Sparkling Water
-----------------------------------------------------

Generalized Low Rank Models (GLRM) in Sparkling Water is an feature estimator, which serves to reduce number of
features in Spark pipeline. Sparkling Water provides API for GLRM in Scala and Python. The following sections describe
how to train and use the Sparkling Water GLRM in both languages. See also :ref:`parameters_H2OGLRM`
and :ref:`model_details_H2OGLRMMOJOModel`.

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

        Create ``H2OGLRM`` and set input columns, ``k`` representing a number of output features and other parameters
        (see :ref:`parameters_H2OGLRM`). An input column could be of any simple type or represent multiple features
        in form of the Spark vector type (``org.apache.spark.ml.linalg.VectorUDT``).

        .. code:: scala

            import ai.h2o.sparkling.ml.features.H2OGLRM
            val glrm = new H2OGLRM()
            glrm.setInputCols(Array("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"))
            glrm.setK(4)

        Define other pipeline stages.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGBM
            val gbm = new H2OGBM()
            gbm.setFeaturesCol(glrm.getOutputCol())
            gbm.setLabelCol("CAPSULE")

        Construct and fit the pipeline.

        .. code:: scala

            import org.apache.spark.ml.Pipeline
            val pipeline = new Pipeline().setStages(Array(glrm, gbm))
            val model = pipeline.fit(trainingDF)

        Now, you can score with the pipeline model.

        .. code:: scala

            val resultDF = model.transform(testingDF)
            resultDF.show(truncate=false)

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

        Create ``H2OGLRM`` and set input columns, ``k`` representing a number of output features and other parameters
        (see :ref:`parameters_H2OGLRM`). An input column could be of any simple type or represent multiple features
        in form of the Spark vector type (``pyspark.ml.linalg.VectorUDT``).

        .. code:: python

            from pysparkling.ml import H2OGLRM
            glrm = H2OGLRM()
            glrm.setInputCols(["RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"])
            glrm.setK(4)

        Define other pipeline stages.

        .. code:: python

            from pysparkling.ml import H2OGBM
            gbm = H2OGBM()
            gbm.setFeaturesCols([glrm.getOutputCol()])
            gbm.setLabelCol("CAPSULE")

        Construct and fit the pipeline.

        .. code:: python

            from pyspark.ml import Pipeline
            pipeline = Pipeline(stages = [glrm, gbm])
            model = pipeline.fit(trainingDF)

        Now, you can score with the pipeline model.

        .. code:: python

            resultDF = model.transform(testingDF)
            resultDF.show(truncate=False)
