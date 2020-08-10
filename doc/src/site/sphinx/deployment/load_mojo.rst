Importing H2O MOJOs from H2O-3
------------------------------

When training algorithm using Sparkling Water API, Sparkling Water always produces ``H2OMOJOModel``. It is however also possible
to import existing MOJO into the Sparkling Water ecosystem from H2O-3. After importing the H2O-3 MOJO the API is unified for the
loaded MOJO and the one created in Sparkling Water, for example, using ``H2OXGBoost``.

H2O MOJOs can be imported to Sparkling Water from all data sources supported by Apache Spark such as a local file, S3 or HDFS and the
semantics of the import is the same as in the Spark API.


When creating a MOJO specified by a relative path and HDFS is enabled, the method attempts to load
the MOJO from the HDFS home directory of the current user. In case we are not running on a HDFS-enabled system, we create
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


An absolute local path can also be used. To create a MOJO model from a locally available MOJO, call:

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

In Scala, the ``createFromMojo`` method returns a mojo model instance cast as a base class ``H2OMOJOModel``. This class holds
only properties that are shared across all MOJO model types from the following type hierarchy:

- ``H2OMOJOModel``
    - ``H2OUnsupervisedMOJOModel``
        - ``H2OTreeBasedUnsupervisedMOJOModel``
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
and in case regression problem the predicted number. If we need to access more details for each prediction, see the content
of a detailed prediction column. By default, the column is named named ``detailed_prediction``. It could contain, for example,
predicted probabilities for each predicted label in case of classification problem, Shapley values, and other information.

Customizing the MOJO Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can configure the output and format of predictions via the H2OMOJOSettings. The available options are

- ``predictionCol`` - Specifies the name of the generated prediction column. The default value is `prediction`.
- ``detailedPredictionCol`` - Specifies the name of the generated detailed prediction column. The detailed prediction column,
  if enabled, contains additional details, such as probabilities, Shapley values etc. The default value is `detailed_prediction`.
- ``convertUnknownCategoricalLevelsToNa`` - Enables or disables conversion of unseen categoricals to NAs. By default, it is disabled.
- ``convertInvalidNumbersToNa`` - Enables or disables conversion of invalid numbers to NAs. By default, it is disabled.
- ``withContributions`` - Enables or disables computing Shapley values. Shapley values are generated as a sub-column for the
  detailed prediction column.
- ``withLeafNodeAssignments`` - When enabled, a user can obtain the leaf node assignments after the model training
  has finished. By default, it is disabled.
- ``withStageResults`` - When enabled, a user can obtain the stage results for tree-based models. By default,
  it is disabled and also it's not supported by XGBoost although it's a tree-based algorithm.
- ``withReconstructedData`` - When enabled, a user can obtain reconstructed data for dimensional reduction models.
  This option is only supported by the GLRM algorithm and is disabled by default.

Methods available on MOJO Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Obtaining Domain Values
^^^^^^^^^^^^^^^^^^^^^^^

To obtain domain values of the trained model, we can run ``getDomainValues()`` on the model. This call
returns a mapping from a column name to its domain in a form of an array.

Obtaining Model Category
^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getModelCategory`` can be used to get the model category (such as ``binomial``, ``multinomial`` etc).

Obtaining Training Params
^^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getTrainingParams`` can be used to get a map containing all training parameters used in the H2O. It is a map
from the parameter name to the value. The parameters name use the H2O's naming structure.


Obtaining Metrics
^^^^^^^^^^^^^^^^^

There are several methods to obtain metrics from the MOJO model. All return a map from the metric name to its double value.

- ``getTrainingMetrics`` - obtain training metrics
- ``getValidationMetrics`` - obtain validation metrics
- ``getCrossValidationMetrics`` - obtain cross validation metrics

We also have method ``getCurrentMetrics`` which gets one of the metrics above based on the following algorithm:

If cross-validation was used, ie, ``setNfolds`` was called and the value was higher than zero, this method returns cross-validation
metrics. If cross-validation was not used, but the validation frame was used, the method returns validation metrics. The validation
frame is used if ``setSplitRatio`` was called with the value lower than one. If neither cross-validation nor validation frame
was used, this method returns the training metrics.

Obtaining Leaf Node Assignments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To obtain the leaf node assignments, please first make sure to set ``withLeafNodeAssignments``
to true on your MOJO settings object. The leaf node assignments are now stored
in the ``${detailedPredictionCol}.leafNodeAssignments`` column on the dataset obtained from the prediction.
Please replace ``${detailedPredictionCol}`` with the actual value of your detailed prediction col. By default,
it is ``detailed_prediction``.

Obtaining Stage Probabilities
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To obtain the stage results, please first make sure to set ``withStageResults`` to true on your MOJO settings object.
The stage results for regression and anomaly detection problems are stored in the ``${detailedPredictionCol}.stageResults``
on the dataset obtained from the prediction. The stage results for classification (binomial, multinomial) problems
are stored under ``${detailedPredictionCol}.stageProbabilities`` Please replace ``${detailedPredictionCol}``
with the actual value of your detailed prediction col. By default, it is ``detailed_prediction``.

The stage results are an array of values, where a value at the position *t* is the prediction/probability combined from contributions of trees *T1, T2, ..., Tt*.
For *t* equal to a number of model trees, the value is the same as the final prediction/probability. The stage results (probabilities) for the classification problem
are represented by a list of columns, where one column contains stage probabilities for a given prediction class.

Obtaining Reconstructed Data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To obtain reconstructed data for dimensional reduction models, please first make sure to set ``withReconstructedData``
to true on your MOJO settings object. Reconstructed columns will be located under the
``${detailedPredictionCol}.reconstructed`` column on the dataset obtained from the prediction.
Please replace ``${detailedPredictionCol}`` with the actual value of your detailed prediction col. By default,
it is ``detailed_prediction``.
