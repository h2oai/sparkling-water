Train XGBoost Model in Sparkling Water
--------------------------------------

Sparkling Water provides API for H2O XGBoost in both Scala and Python.
The following sections describe how to train XGBoost model in Sparkling Water in both languages.

Running XGBoost in Scala
~~~~~~~~~~~~~~~~~~~~~~~~

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
    val sparkFrame = hc.asDataFrame(frame)

Train the model. You can configure all the available XGBoost arguments using provided setters, such as the predictions column.

.. code:: scala

    import org.apache.spark.ml.h2o.algos.H2OXGBoost
    val estimator = new H2OXGBoost().setLabelCol("AGE")
    val model = estimator.fit(sparkFrame)


Run Predictions

.. code:: scala

    model.transform(sparkFrame)

Running XGBoost in Python
~~~~~~~~~~~~~~~~~~~~~~~~~


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
    spark_frame = hc.as_spark_frame(frame)

Train the model. You can configure all the available XGBoost arguments using provided setters, such as the predictions column.

.. code:: python

    from pysparkling.ml import H2OXGBoost
    estimator = H2OXGBoost(labelCol="AGE")
    model = estimator.fit(spark_frame)


Run Predictions

.. code:: python

    model.transform(spark_frame)


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