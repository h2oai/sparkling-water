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

        Train the model.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OXGBoostClassifier
            val estimator = new H2OXGBoostClassifier().setLabelCol("CAPSULE")
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

        Train the model.

        .. code:: python

            from pysparkling.ml import H2OXGBoostClassifier
            estimator = H2OXGBoost(labelCol = "CAPSULE")
            mojoModel = estimator.fit(sparkDF)

To obtain the binary model once the model training has finished, run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            estimator.getBinaryModel()

    .. tab-container:: Python
        :title: Python

        .. code:: python

            estimator.getBinaryModel()

You can store the model on disk as:

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

The loaded end exported models are always equal to each other.

Load existing binary Model
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
