Using H2O Binary Model in Sparkling Water
-----------------------------------------

Sparkling Water can generate binary models and can also load already existing
models trained for example in H2O-3.

Train Model in Sparkling Water and Obtain Binary model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We first train a model using Sparkling Water API from which we can extract the binary model class.
The binary model class contains methods used to work with binary models.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame

        .. code:: scala

            import org.apache.spark.SparkFiles
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))

        Select algorithm, set the parameter ``keepBinaryModels`` to ``true``, train the model.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OXGBoostClassifier
            val estimator = new H2OXGBoostClassifier().setLabelCol("CAPSULE").setKeepBinaryModels(true)
            val mojoModel = estimator.fit(sparkDF)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame

        .. code:: python

            import h2o
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)

        Select algorithm, set the parameter ``keepBinaryModels`` to ``True``, train the model.

        .. code:: python

            from pysparkling.ml import H2OXGBoostClassifier
            estimator = H2OXGBoostClassifier(labelCol = "CAPSULE", keepBinaryModels = True)
            mojoModel = estimator.fit(sparkDF)

To obtain the binary model once the model training has finished, run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val binaryModel = estimator.getBinaryModel()

    .. tab-container:: Python
        :title: Python

        .. code:: python

            binaryModel = estimator.getBinaryModel()

Utilization of Binary Model in H2O-3 API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following scoring example demonstrates how a binary model trained with the SW API can be utilized with the H2O-3 API:

.. content-tabs::

    .. tab-container:: Python
        :title: Python

        .. code:: python

            h2oBinaryModel = h2o.get_model(binaryModel.modelId)
            h2oBinaryModel.predict(test_data=frame)

Save Binary Model to File System
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following example demonstrates how a binary model can be stored on a file system:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val binaryModel = estimator.getBinaryModel()
            binaryModel.write("/tmp/binary.model")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            binaryModel = estimator.getBinaryModel()
            binaryModel.write("/tmp/binary.model")

In case of a Hadoop-enabled system, the command by default uses HDFS. To reference a path on the local file system of
the Spark driver, the path must be prefixed with ``file://`` when HDFS is enabled.

Load Existing Binary Model
~~~~~~~~~~~~~~~~~~~~~~~~~~

Before you start, please make sure that your ``H2OContext`` is running as we need H2O to be running.
Also please make sure that Sparkling Water is of the same version as the H2O version in which
the binary model was trained. If this condition is not met, Sparkling Water throws an exception.

To load binary model, run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import ai.h2o.sparkling._
            import ai.h2o.sparkling.ml.models.H2OBinaryModel
            val hc = H2OContext.getOrCreate()
            val model = H2OBinaryModel.read(path)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            from pysparkling.ml import H2OBinaryModel
            hc = H2OContext.getOrCreate()
            model = H2OBinaryModel.read(path)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            hc <- H2OContext.getOrCreate()
            model <- H2OBinaryModel.read(path)
