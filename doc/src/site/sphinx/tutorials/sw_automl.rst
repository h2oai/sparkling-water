Train AutoML Model in Sparkling Water
-------------------------------------

Sparkling Water provides API in Scala and Python for H2O AutoML.
The following sections describe how to train an AutoML model in Sparkling Water in both languages.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        Start H2O cluster inside the Spark environment

        .. code:: scala

            import org.apache.spark.h2o._
            import java.net.URI
            val hc = H2OContext.getOrCreate(spark)

        Parse the data using H2O and convert them to Spark Frame

        .. code:: scala

            val frame = new H2OFrame(new URI("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv"))
            val sparkDF = hc.asDataFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Create a H2OAutoML instance and configure it according your use case via provided setters. If feature columns are not specified explicitly,
        all columns excluding label, fold, weight and ignored columns are considered as features.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OAutoML
            val automl = new H2OAutoML()
            automl.setLabelCol("CAPSULE")
            automl.setIgnoredCols(Array("ID"))

        By default, AutoML goes through a huge space of H2O algorithms and their hyper-parameters which requires some time. If you wish to speed up
        the training phase, you can exclude some H2O algorithms and limit the number of trained models.

        .. code:: scala

            automl.setExcludeAlgos(Array("GLM"))
            automl.setMaxModels(10)

        Train the AutoML model. The training phase returns the best model according to the sortMetric. If it's not specified, the sortMetric is chosen automatically.

        .. code:: scala

            automl.setSortMetric("AUC")
            val model = automl.fit(trainingDF)

        You can also get raw model details by calling the ``getModelDetails()`` method available on the model as:

        .. code:: scala

            model.getModelDetails()

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        If you are curious to see information about other models created during the AutoML training process, you can get
        a model leaderboard represented by Spark DataFrame.

        .. code:: scala

            val leaderboard = automl.getLeaderboard()
            leaderboard.show(false)

        By default, the leaderboard contains the model name (*model_id*) and various performance metrics like AUC, RMSE, etc.
        If you want to see more information about models, you can add extra columns to the leaderboard by passing column names
        to the ``getLeaderboard()`` method.

        .. code:: scala

            val leaderboard = automl.getLeaderboard("training_time_ms", "predict_time_per_row_ms")
            leaderboard.show(false)

        Extra columns don't have to be specified explicitly. You can specify addition of all possible extra columns as:

        .. code:: scala

            val leaderboard = automl.getLeaderboard("ALL")
            leaderboard.show(false)


    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell as

        .. code:: shell

            ./bin/pysparkling

        Start H2O cluster inside the Spark environment

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate(spark)

        Parse the data using H2O and convert them to Spark Frame

        .. code:: python

            import h2o
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Create a H2OAutoML instance and configure it according your use case via provided setters or named constructor parameters.
        If feature columns are not specified explicitly, all columns excluding label, fold, weight and ignored columns are considered as features.

        .. code:: python

            from pysparkling.ml import H2OAutoML
            automl = H2OAutoML(labelCol="CAPSULE", ignoredCols=["ID"])

        By default, AutoML goes through a huge space of H2O algorithms and their hyper-parameters which requires some time. If you wish to speed up
        the training phase, you can exclude some H2O algorithms and limit the number of trained models.

        .. code:: python

            automl.setExcludeAlgos(["GLM"])
            automl.setMaxModels(10)

        Train the AutoML model. The training phase returns the best model according to the sortMetric. If it's not specified, the sortMetric is chosen automatically.

        .. code:: python

            automl.setSortMetric("AUC")
            model = automl.fit(trainingDF)

        You can also get raw model details by calling the ``getModelDetails()`` method available on the model as:

        .. code:: python

            model.getModelDetails()

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        If you are curious to see information about other models created during the AutoML training process, you can get
        a model leaderboard represented by Spark DataFrame.

        .. code:: python

            leaderboard = automl.getLeaderboard()
            leaderboard.show(truncate = False)

        By default, the leaderboard contains the model name (*model_id*) and various performance metrics like AUC, RMSE, etc.
        If you want to see more information about models, you can add extra columns to the leaderboard by passing column names
        to the ``getLeaderboard()`` method.

        .. code:: scala

            leaderboard = automl.getLeaderboard("training_time_ms", "predict_time_per_row_ms")
            leaderboard.show(truncate = False)

        Extra columns don't have to be specified explicitly. You can specify addition of all possible extra columns as:

        .. code:: scala

            leaderboard = automl.getLeaderboard("ALL")
            leaderboard.show(truncate = False)


Enabling XGBoost Models when Running Sparkling Water in a Distributed Environment (YARN)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The multi-node XGBoost algorithm is considered as an experimental feature of AutoML. Thus the XGBoost algorithm is disabled for AutoML by default when running
Sparkling Water in a distributed environment (e.g. on YARN). When Sparkling Water is run in the ``local`` mode, XGBoost is enabled.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        To enable the algorithm on YARN, ``sparkling-shell`` has to be executed with the extra driver option as:

        .. code:: shell

            ./bin/sparkling-shell --conf spark.driver.extraJavaOptions=-Dsys.ai.h2o.automl.xgboost.multinode.enabled=true


    .. tab-container:: Python
        :title: Python

        To enable the algorithm on YARN, ``pysparkling`` has to be executed with the extra driver option as:

        .. code:: shell

            ./bin/pysparkling --conf spark.driver.extraJavaOptions=-Dsys.ai.h2o.automl.xgboost.multinode.enabled=true


The statement above also holds for executing Sparkling Water with the external backend and connecting Sparkling Water to an existing H2O cluster.
Other configuration steps are not necessary for enabling XGBoost in AutoML.