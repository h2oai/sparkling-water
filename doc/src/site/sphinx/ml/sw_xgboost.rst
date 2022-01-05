Train XGBoost Model in Sparkling Water
--------------------------------------

Sparkling Water provides API for H2O XGBoost in Scala and Python. The following sections describe how to train
the XGBoost model in Sparkling Water in both languages. See also :ref:`parameters_H2OXGBoost`
and :ref:`model_details_H2OXGBoostMOJOModel`.

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

        Train the model. You can configure all the available XGBoost arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OXGBoost
            val estimator = new H2OXGBoost().setLabelCol("CAPSULE")
            val model = estimator.fit(trainingDF)

        By default, the ``H2OXGBoost`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2OXGBoostClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2OXGBoostRegressor`` instead.

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        You can also get model details via calling methods listed in :ref:`model_details_H2OXGBoostMOJOModel`.


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

        Train the model. You can configure all the available XGBoost arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2OXGBoost
            estimator = H2OXGBoost(labelCol = "CAPSULE")
            model = estimator.fit(trainingDF)

        By default, the ``H2OXGBoost`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2OXGBoostClassifier`` or ``H2OXGBoostRegressor`` instead.

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2OXGBoostMOJOModel`.


XGBoost Memory Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

H2O XGBoost uses additionally to Java memory, off-heap memory. This means that it requires some additional memory
available on the system.

When running on YARN or Kubernetes, please make sure to set the ``SUBST_EXECUTOR_MEMORY_OVERHEAD`` so XGBoost has enough
native memory on executors. It's recommended to set the property to 12O% of the value set in ``spark.executor.memory``.

Note: ``SUBST_EXECUTOR_MEMORY_OVERHEAD`` must be set in MiB.

Example
```````
If you set ``spark.executor.memory`` to ``10g``, ``SUBST_EXECUTOR_MEMORY_OVERHEAD`` should be set to ``12288``.
The size of the corresponding YARN or Kubernetes container will be at least 22 GiB.

Note: In case of Pysparkling, the YARN container will be bigger about the memory required by the Python process.


Memory Overhead on Spark driver
```````````````````````````````
If you enabled a H2O client (a special H2O node representing an entry point for the communication with the H2O cluster)
to run on the Spark driver, you should also set the following properties in the same way as ``SUBST_EXECUTOR_MEMORY_OVERHEAD``.

- ``spark.yarn.am.memoryOverhead`` - in case of deploying to YARN in the client mode
- ``SUBST_DRIVER_MEMORY_OVERHEAD`` - in case of deploying to YARN in the cluster mode and other deployments (Kubernetes, Mesos)

Note: A H2O client can run on the Spark driver only with Sparkling Water in Scala/Java API and the property
``spark.ext.h2o.rest.api.based.client`` set to ``false``. The default value of the property is ``true``.