Importing H2O MOJOs from H2O-3
------------------------------

When training algorithm using Sparkling Water API, Sparkling Water always produces ``H2OMOJOModel``. It is also possible
to import existing MOJO models into the Sparkling Water ecosystem from H2O-3. Such MOJO models then have the same scoring
capabilities as MOJO models trained via Sparkling Water API.

**Note**: Sparkling Water is backward compatible with MOJO versions produced by different H2O-3 versions.

One advantage of scoring the MOJO artifacts is that ``H2OContext`` does not have to be created if we only want to
run predictions on MOJOs using Spark. It is important to mention that the format of prediction on MOJOs from
Driverless AI differs from predictions on H2O-3 MOJOs. The format of H2O-3 predictions is explained bellow.

Starting a Scoring Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, we need to start a scoring environment for the desired language. There are two variants.
We can use Sparkling Water prepared scripts which put required dependencies on the Spark classpath or we can use Spark
directly and add the dependencies manually.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: bash

            ./bin/spark-shell --jars jars/sparkling-water-assembly-scoring_SUBST_SCALA_BASE_VERSION-SUBST_SW_VERSION-all.jar

        If there is a need to train H2O-3/SW models at the same time when we score with existing MOJO models, use
        ``jars/sparkling-water-assembly_SUBST_SCALA_BASE_VERSION-SUBST_SW_VERSION-all.jar`` instead.

        .. code:: bash

            ./bin/sparkling-shell


    .. tab-container:: Python
        :title: Python

        .. code:: bash

            SUBST_PYTHON_PATH_WORKAROUND./bin/pyspark --py-files py/h2o_pysparkling_scoring_SUBST_SPARK_MAJOR_VERSION-SUBST_SW_VERSION.zip

        If there is a need to train H2O-3/SW models at the same time when we score with existing MOJO models, use
        ``py/h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION-SUBST_SW_VERSION.zip`` instead.

        .. code:: bash

            ./bin/pysparkling


At this point, we have a Spark interactive terminal where we can carry out predictions. If we don't require an interactive environment,
we can deploy our scoring logic with ``./bin/spark-submit``. The parameters will be the same as in the example above.


Loading and Usage of H2O-3 MOJO Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

H2O MOJOs can be imported to Sparkling Water from all data sources supported by Apache Spark such as a local file, S3 or
HDFS and the semantics of the import is the same as in the Spark API.

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
only properties common for all mojo models across different Sparkling Water algorithms.

If a Scala user wants to get a property specific for a given MOJO model type, he/she must utilize casting or
call the ``createFromMojo`` method on the specific MOJO model type.

.. code:: scala

    import ai.h2o.sparkling.ml.models._
    val specificModel = H2OGBMMOJOModel.createFromMojo("prostate_mojo.zip")
    println(s"Ntrees: ${specificModel.getNTrees()}")

The list of specific MOJO models:

- ``H2OXGBoostMOJOModel``
- ``H2OGBMMOJOModel``
- ``H2ODRFMOJOModel``
- ``H2OGLMMOJOModel``
- ``H2OGAMMOJOModel``
- ``H2ODeepLearningMOJOModel``
- ``H2OKMeansMOJOModel``
- ``H2OIsolationForestMOJOModel``
- ``H2OCoxPHMOJOModel``
- ``H2OTargetEncoderMOJOModel``
- ``H2OAutoEncoderMOJOModel``

Exporting the loaded MOJO model using Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To export the MOJO model, call ``model.write.save("/some/path")``. In case of a Hadoop-enabled system, the command by default
uses HDFS. To reference a path on the local file system of the Spark driver, the path must be prefixed with ``file://`` when HDFS is enabled.

Importing the previously exported MOJO model from Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To import the MOJO model, call ``H2OMOJOModel.read.load("/some/path")``. In case of a Hadoop-enabled system, the command by default
uses HDFS. To reference a path on the local file system of the Spark driver, the path must be prefixed with ``file://`` when HDFS is enabled.

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
  detailed prediction column. Shapley values are supported only by tree-based binomial and regression models.
- ``withLeafNodeAssignments`` - When enabled, a user can obtain the leaf node assignments after the model training
  has finished. By default, it is disabled.
- ``withStageResults`` - When enabled, a user can obtain the stage results for tree-based models. By default,
  it is disabled and also it's not supported by XGBoost although it's a tree-based algorithm.
- ``dataFrameSerializer`` - A full name of a serializer used for serialization and deserialization of Spark DataFrames
  to a JSON value within ``NullableDataFrameParam``.

Methods available on MOJO Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- See :ref:`model_details` for methods available on particular model types.

Obtaining Domain Values
^^^^^^^^^^^^^^^^^^^^^^^

To obtain domain values of the trained model, we can run ``getDomainValues()`` on the model. This call
returns a mapping from a column name to its domain in a form of an array.

Obtaining Model Category
^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getModelCategory`` can be used to get the model category (such as ``binomial``, ``multinomial`` etc).

Obtaining Feature Types
^^^^^^^^^^^^^^^^^^^^^^^

The method ``getFeatureTypes`` returns a map/dictionary from a feature name to a corresponding feature type
[``enum`` (categorical), ``numeric``, ``string``, etc.]. These pieces helps to understand how individual columns of
the training dataset were treated during the model training.

Obtaining Feature Importances
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getFeatureImportances`` returns a data frame describing importance of each feature. The importance is expressed
by several numbers (Relative Importance, Scaled Importance and Percentage). `H2O-3 documentation
<https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/docs-website/h2o-docs/variable-importance.html>`__
describes how the numbers are calculated.


Obtaining Scoring History
^^^^^^^^^^^^^^^^^^^^^^^^^

The method ``getScoringHistory`` returns a data frame describing how the model evolved during the training process according to
a certain training and validation metrics.

Obtaining Metrics
^^^^^^^^^^^^^^^^^

There are two sets of methods to obtain metrics from the MOJO model.

1. The first set of methods return a map from the metric name to its double value.

- ``getTrainingMetrics()`` - to obtain training metrics
- ``getValidationMetrics()`` - to obtain validation metrics
- ``getCrossValidationMetrics()`` - to obtain metrics combined from cross-validation holdouts

There is also the method ``getCurrentMetrics()`` which gets one of the metrics above based on the following algorithm:

If cross-validation was used, ie, ``setNfolds`` was called and the value was higher than zero, this method returns cross-validation
metrics. If cross-validation was not used, but the validation frame was used, the method returns validation metrics. The validation
frame is used if ``setSplitRatio`` was called with the value lower than one. If neither cross-validation nor validation frame
was used, this method returns the training metrics.

2. The second set of methods returns typed instances. The instances make individual metrics available via getter methods and
the metrics could be also of a complex type. (see :ref:`metrics` for details)

- ``getTrainingMetricsObject()`` - to obtain training metrics
- ``getValidationMetricsObject()`` - to obtain validation metrics
- ``getCrossValidationMetricsObject()`` - to obtain metrics combined from cross-validation holdouts

There is also the  method ``getCurrentMetricsObject()`` working a similar way as ``getCurrentMetrics()``.

Obtaining Cross Validation Metrics Summary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The ``getCrossValidationMetricsSummary`` method returns data frame with information about performance of individual folds
according to various model metrics.

Obtaining Cross Validation Models
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If the model was trained with SW API (i.e. the model wasn't loaded with the method ``H2OMOJOModel.createFromMojo()``),
the algorithm parameter ``keepCrossValidationModels`` was set to ``true`` and cross-validation was enabled during
the training phase, a user can access the sequence cross-validation models by calling the method ``getCrossValidationModels()``.
The returned models are regular Sparkling Water MOJO models with model metrics and other important information.
*[This feature is not available in SW R API.]*

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
