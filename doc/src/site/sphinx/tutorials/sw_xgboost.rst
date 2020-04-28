Train XGBoost Model in Sparkling Water
--------------------------------------

Sparkling Water provides API for H2O XGBoost in Scala and Python.
The following sections describe how to train XGBoost model in Sparkling Water in both languages.

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

            val frame = new H2OFrame(new URI("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv"))
            val sparkDF = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available XGBoost arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OXGBoost
            val estimator = new H2OXGBoost().setLabelCol("CAPSULE")
            val model = estimator.fit(trainingDF)

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

        Train the model. You can configure all the available XGBoost arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2OXGBoost
            estimator = H2OXGBoost(labelCol = "CAPSULE")
            model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            model.getModelDetails()

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)


XGBoost Memory Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

H2O XGBoost uses additionally to Java memory, off-heap memory. This means that it requires some additionally memory
available on the system.

When running on YARN, please make sure to set the ``memoryOverhead`` so XGBoost has enough memory. On Spark, the following
properties might be set

- ``spark.yarn.am.memoryOverhead`` - in case of YARN Cluster deployment
- ``spark.yarn.driver.memoryOverhead`` - in case of YARN client and other deployments
- ``spark.yarn.executor.memoryOverhead`` - in all deployment scenarios

On YARN, the container size is determined by ``application_memory * memory_overhead``. Therefore, by specifying the
overhead, we are also allocating some additional off-heap memory which XGBoost can use.

In Spark Standalone Mode or IBM Conductor environment, please make sure to configure the following configurations:


- ``spark.memory.offHeap.enabled=true``
- ``spark.memory.offHeap.size=4G`` - example of setting this property to 4G of off-heap memory