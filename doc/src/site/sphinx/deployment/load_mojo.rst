Importing H2O MOJOs from H2O-3
------------------------------

When training algorithm using Sparkling Water API, Sparkling Water always produces ``H2OMOJOModel``. It is however also possible
to import existing MOJO into Sparkling Water ecosystem from H2O-3. After importing the H2O-3 MOJO the API is unified for the
loaded MOJO and the one created in Sparkling Water, for example, using ``H2OXGBoost``.

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


We can also manually specify the type of data source we need to use, in that case, we need to provide the schema:


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
On the other hand, the model can be configured during its creation via ``H2OMOJOSettings``:


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

Exporting the loaded MOJO model using Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To export the MOJO model, call ``model.write.save(path)``. In case of Hadoop enabled system, the command by default
uses HDFS.

Importing the previously exported MOJO model from Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To import the MOJO model, call ``H2OMOJOModel.read.load(path)``. In case of Hadoop enabled system, the command by default
uses HDFS.

Accessing additional prediction details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After computing predictions, the ``prediction`` column contains in case of classification problem the predicted label
and in case regression problem the predicted number. If we need to access more details for each prediction, set
``withDetailedPredictionCol`` to true on ``H2OMOJOSettings`` before running the predictions. This will ensure that
additional column will be created during predictions, by default named ``detailed_prediction`` which contains, for example,
predicted probabilities for each predicted label in case of classification problem, shapley values and other information.

Customizing the MOJO Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can configure the output and format of predictions via the H2OMOJOSettings. The available options are

- ``predictionCol`` - Specifies the name of the generated prediction column. Default value is `prediction`.
- ``detailedPredictionCol`` - Specifies the name of the generated detailed prediction column. The detailed prediction column,
  if enabled, contains additional details, such as probabilities, shapley values etc. The default value is `detailed_prediction`.
- ``withDetailedPredictionCol`` - Enables or disables the generation of the detailed prediction column. By default, it is disabled.
- ``convertUnknownCategoricalLevelsToNa`` - Enables or disables conversion of unseen categoricals to NAs. By default, it is disabled.
- ``convertInvalidNumbersToNa`` - Enables or disables conversion of invalid numbers to NAs. By default, it is disabled.
- ``withContributions`` - Enables or disables computing Shapley values. Shapley values are generated as a sub-column for the
  detailed prediction column. Therefore, to compute Shapley values, both this option and ``withDetailedPredictionCol`` needs to be
  enabled. By default, it is disabled.

Methods available on MOJO Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Obtaining Domain Values
^^^^^^^^^^^^^^^^^^^^^^^

To obtain domain values of the trained model, we can run ``getDomainValues()`` on the model. This call
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
