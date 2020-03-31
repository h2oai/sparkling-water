Importing H2O MOJOs
-------------------

H2O MOJOs can be imported to Sparkling Water from all data sources supported by Apache Spark such as local file, S3 or HDFS and the
semantics of the import is the same as in the Spark API.


When creating a MOJO specified by a relative path and HDFS is enabled, the method attempts to load
the MOJO from the HDFS home directory of the current user. In case we are not running on HDFS-enabled system, we create
the mojo from a current working directory.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling.ml.models._
            val model = H2OMOJOModel.createFromMojo("prostate_mojo.zip")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling.ml import *
            model = H2OMOJOModel.createFromMojo("prostate_mojo.zip")

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            model <- H2OMOJOModel.createFromMojo("prostate_mojo.zip")


Absolute local path can also be used. To create a MOJO model from a locally available MOJO, call:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling.ml.models._
            val model = H2OMOJOModel.createFromMojo("/Users/peter/prostate_mojo.zip")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling.ml import *
            model = H2OMOJOModel.createFromMojo("/Users/peter/prostate_mojo.zip")

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            model <- H2OMOJOModel.createFromMojo("/Users/peter/prostate_mojo.zip")



Absolute paths on Hadoop can also be used. To create a MOJO model from a MOJO stored on HDFS, call:


.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling.ml.models._
            val model = H2OMOJOModel.createFromMojo("/user/peter/prostate_mojo.zip")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling.ml import *
            model = H2OMOJOModel.createFromMojo("/user/peter/prostate_mojo.zip")

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            model <- H2OMOJOModel.createFromMojo("/user/peter/prostate_mojo.zip")



The call loads the mojo file from the following location ``hdfs://{server}:{port}/user/peter/prostate_mojo.zip``, where ``{server}`` and ``{port}`` is automatically filled in by Spark.


You can also manually specify the type of data source you need to use, in that case, you need to provide the schema:


.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling.ml.models._
            // HDFS
            val modelHDFS = H2OMOJOModel.createFromMojo("hdfs:///user/peter/prostate_mojo.zip")
            // Local file
            val modelLocal = H2OMOJOModel.createFromMojo("file:///Users/peter/prostate_mojo.zip")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling.ml import *
            # HDFS
            modelHDFS = H2OMOJOModel.createFromMojo("hdfs:///user/peter/prostate_mojo.zip")
            # Local file
            modelLocal = H2OMOJOModel.createFromMojo("file:///Users/peter/prostate_mojo.zip")


    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
             # HDFS
            modelHDFS <- H2OMOJOModel.createFromMojo("hdfs:///user/peter/prostate_mojo.zip")
            # Local file
            modelLocal <- H2OMOJOModel.createFromMojo("file:///Users/peter/prostate_mojo.zip")


The loaded model is an immutable instance, so it's not possible to change the configuration of the model during its existence.
On the other hand, the model can be configured during its creation via ``H2OMOJOSettings``, in Scala:


.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling.ml.models._
            val settings = H2OMOJOSettings(convertUnknownCategoricalLevelsToNa = true, convertInvalidNumbersToNa = true)
            val model = H2OMOJOModel.createFromMojo("prostate_mojo.zip", settings)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling.ml import *
            settings = H2OMOJOSettings(convertUnknownCategoricalLevelsToNa = True, convertInvalidNumbersToNa = True)
            model = H2OMOJOModel.createFromMojo("prostate_mojo.zip", settings)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            settings <- H2OMOJOSettings(convertUnknownCategoricalLevelsToNa = TRUE, convertInvalidNumbersToNa = TRUE)
            model <- H2OMOJOModel.createFromMojo("prostate_mojo.zip", settings)


To score the dataset using the loaded mojo, call:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            model.transform(dataset)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            model.transform(dataset)

    .. tab-container:: R
        :title: R

        .. code:: r

            model$transform(dataset)

In Scala, the ``createFromMojo`` method returns a mojo model instance casted as a base class ``H2OMOJOModel``. This class holds
only properties that are shared accross all MOJO model types from the following type hierarchy:

- ``H2OMOJOModel``
    - ``H2OUnsupervisedMOJOModel``
    - ``H2OSupervisedMOJOModel``
        - ``H2OTreeBasedSupervisedMOJOModel``


If a Scala user wants to get a property specific for a given MOJO model type, he/she must utilize casting or
call the ``createFromMojo`` method on the specific MOJO model type.

.. code:: scala

    import ai.h2o.sparkling.ml.models._
    val specificModel = H2OTreeBasedSupervisedMOJOModel.createFromMojo("prostate_mojo.zip")
    println(s"Ntrees: ${specificModel.getNTrees()}") // Relevant only to GBM, DRF and XGBoost

Exporting the trained MOJO model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To export the trained MOJO model, call ``model.write.save(path)``. In case of Hadoop enabled system, the command by default
uses HDFS.

Importing the trained MOJO model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To import the trained MOJO model, call ``H2OMOJOModel.read.load(path)``. In case of Hadoop enabled system, the command by default
uses HDFS.

Methods available on MOJO Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Obtaining Domain Values
^^^^^^^^^^^^^^^^^^^^^^^

To obtain domain values of the trained model, you can run ``getDomainValues()`` on the model. This call
returns a mapping from a column name to it's domain in a form of array.

Obtaining Model Category
^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getModelCategory`` can be used to get the model category (such as ``binomial``, ``multinomial`` etc).

Obtaining Training Params
^^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getTrainingParams`` can be used to get map containing all training parameters used in the H2O. It is a map
from parameter name to the value. The parameters name use the H2O's naming structure.


Obtaining Metrics
^^^^^^^^^^^^^^^^^

There are several methods to obtain metrics from the MOJO model. All return a map from metric name to its double value.

- ``getTrainingMetrics`` - obtain training metrics
- ``getValidationMetrics`` - obtain validation metrics
- ``getCrossValidationMetrics`` - obtain cross validation metrics

We also have method ``getCurrentMetrics`` which gets one of the metrics above based on the following algorithm:

If cross validation was used, ie, ``setNfolds`` was called and value was higher than zero, this method returns cross validation
metrics. If cross validation was not used, but validation frame was used, the method returns validation metrics. Validation
frame is used if ``setSplitRatio`` was called with value lower than one. If neither cross validation or validation frame
was used, this method returns the training metrics.
