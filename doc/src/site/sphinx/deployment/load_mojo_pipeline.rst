Importing MOJO Pipelines from Driverless AI
-------------------------------------------

MOJO scoring pipeline artifacts, created in Driverless AI, can be used in Spark to carry out predictions in parallel
using the Sparkling Water API. This section shows how to load and run predictions on the MOJO scoring pipeline in
Sparkling Water.

**Note**: Sparkling Water is backward compatible with MOJO versions produced by different Driverless AI versions.

One advantage of scoring the MOJO artifacts is that ``H2OContext`` does not have to be created if we only want to
run predictions on MOJOs using Spark. This is because the scoring is independent of the H2O run-time. It is also
important to mention that the format of prediction on MOJOs from Driverless AI differs from predictions on H2O-3 MOJOs.
The format of Driverless AI prediction is explained bellow.

Requirements
~~~~~~~~~~~~

To use the MOJO scoring pipeline, a Driverless AI license has to be passed to Spark.
This can be achieved via ``--jars`` argument of the Spark launcher scripts.

**Note**: In Local Spark mode, please use ``--driver-class-path`` to specify the path to the license file.

We also need Sparkling Water distribution which can be obtained from `H2O Download page <https://www.h2o.ai/download/>`__.
After we downloaded the Sparkling Water distribution, extract it, and go to the extracted directory.

The MOJO scoring pipeline is included in the ``mojo.zip`` archive downloaded from Driverless AI. The archive also contains
a mojo runtime library, examples and other files. The only file which is important for scoring with Sparkling Water is
``pipeline.mojo``. Thus before running Sparkling Water, extract the ``mojo.zip`` archive and copy ``pipeline.mojo`` to a
location of your choice.

.. code:: bash

    unzip /path/to/mojo.zip -d /tmp/mojo.zip.extracted
    cp /tmp/mojo.zip.extracted/mojo-pipeline/pipeline.mojo /path/to/pipeline.mojo

Loading and Score the MOJO
~~~~~~~~~~~~~~~~~~~~~~~~~~

First, start the environment for the desired language with a Driverless AI license. There are two variants. We can use
Sparkling Water prepared scripts which put required dependencies on the Spark classpath or we can use Spark directly
and add the dependencies manually.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: bash

            ./bin/spark-shell --jars license.sig,jars/sparkling-water-assembly_SUBST_SCALA_BASE_VERSION-SUBST_SW_VERSION-all.jar

        .. code:: bash

            ./bin/sparkling-shell --jars license.sig


    .. tab-container:: Python
        :title: Python

        .. code:: bash

            ./bin/pyspark --jars license.sig --py-files py/build/dist/h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION-SUBST_SW_VERSION.zip

        .. code:: bash

            ./bin/pysparkling --jars license.sig


At this point, we should have a Spark interactive terminal where we can carry out predictions.
For productionalizing the scoring process, we can use the same configuration,
except instead of using Spark shell, we would submit the application using ``./bin/spark-submit``.

Now Load the MOJO as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel
            val settings = H2OMOJOSettings(predictionCol = "fruit_type", convertUnknownCategoricalLevelsToNa = true)
            val mojo = H2OMOJOPipelineModel.createFromMojo("file:///path/to/pipeline.mojo", settings)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling.ml import H2OMOJOPipelineModel
            settings = H2OMOJOSettings(predictionCol = "fruit_type", convertUnknownCategoricalLevelsToNa = True)
            mojo = H2OMOJOPipelineModel.createFromMojo("file:///path/to/pipeline.mojo", settings)

In the examples above ``settings`` is an optional argument. If it's not specified, the default values are used.

Prepare the dataset to score on:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val dataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///path/to/data.csv")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            dataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///path/to/data.csv")

And finally, score the mojo on the loaded dataset:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val predictions = mojo.transform(dataFrame)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            predictions = mojo.transform(dataFrame)

We can select the predictions as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            predictions.select("prediction")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            predictions.select("prediction")

The output data frame contains all the original columns plus the prediction column which is by default named
``prediction``. The prediction column contains all the prediction detail. Its name can be modified via the ``H2OMOJOSettings``
object.

Predictions Format
~~~~~~~~~~~~~~~~~~

When the option ``namedMojoOutputColumns`` is enabled on ``H2OMOJOSettings``, the ``predictionCol`` contains sub-columns with
names corresponding to the columns Driverless AI identified as output columns. For example, if Driverless API MOJO
pipeline contains one output column `AGE` ( for example regression problem), the prediction column contains another sub-column
named `AGE`. If The MOJO pipeline contains multiple output columns, such as `VALID.0` and `VALID.1` (for example classification problems),
the prediction column contains two sub-columns with the aforementioned names.

If this option is disabled, the ``predictionCol`` contains the array of predictions without
the column names. For example, if the Driverless API MOJO pipeline contains one output column `AGE` ( for example regression problem),
the prediction column contains an array of size 1 with the predicted value.
If The MOJO pipeline contains multiple output columns, such as `VALID.0` and `VALID.1` (for example classification problems),
the prediction column contains an array of size 2 containing predicted probabilities for each class.

By default, this option is enabled.

Customizing the MOJO Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We can configure the output and format of predictions via the H2OMOJOSettings. The available options are

- ``predictionCol`` - Specifies the name of the generated prediction column. The default value is `prediction`.
- ``convertUnknownCategoricalLevelsToNa`` - Enables or disables conversion of unseen categoricals to NAs. By default, it is disabled.
- ``convertInvalidNumbersToNa`` - Enables or disables conversion of invalid numbers to NAs. By default, it is disabled.
- ``namedMojoOutputColumns`` - Enables or disables named output columns. By default, it is enabled.

Troubleshooting
~~~~~~~~~~~~~~~

If you see the following exception during loading the MOJO pipeline:

- ``java.io.IOException: MOJO doesn't contain resource mojo/pipeline.pb``, then it means you are adding
  incompatible mojo-runtime.jar on your classpath. It is not required and also not suggested
  to put the JAR on the classpath as Sparkling Water already bundles the correct dependencies.

- ``java.io.IOException: None of 2 available pipeline factories [pbuf, toml] can read this mojo.``, then you most-likely
  passed the whole ``mojo.zip`` archive to the createFromMojo method instead of the ``pipeline.mojo`` file, which is contained
  in the archive.
