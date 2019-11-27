Migration Guide
===============

Migration guide between Sparkling Water versions.

From 3.26 To 3.28
-----------------

String instead of enums in Sparkling Water Algo API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- In scala, setters of the pipeline wrappers for H2O algorithms now accepts strings in places where they accepted
  enum values before. Before, we called, for example:

.. code-block:: scala

    import hex.genmodel.utils.DistributionFamily
    val gbm = H2OGBM()
    gbm.setDistribution(DistributionFamily.multinomial)


Now, the correct code is:

.. code-block:: scala

    val gbm = H2OGBM()
    gbm.setDistribution("multinomial")

which makes the Python and Scala APIs consistent. Both upper case and lower case values are valid and if a wrong
input is entered, warning is printed out with correct possible values.

Switch to Java 1.8 on Spark 2.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water for Spark 2.1 now requires Java 1.8 and higher.

DRF exposed into Sparkling Water Algorithm API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DRF is now exposed in the Sparkling Water. Please see our documentation to learn how to use it :ref:`drf`.

Also we can run our Grid Search API on DRF.

Change Default Name of Prediction Column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default name of the prediction column has been changed from ``prediction_output`` to ``prediction``.

Single value in prediction column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The prediction column contains directly the predicted value. For example, before this change, the prediction column contained
another struct field called ``value`` (in case of regression issue), which contained the value. From now on, the predicted value
is always stored directly in the prediction column. In case of regression issue, the predicted numeric value
and in case of classification, the predicted label. If you are interested in more details created during the prediction,
please make sure to set ``withDetailedPredictionCol`` to ``true`` via the setters on both PySparkling and Sparkling Water.
When enabled, additional column named ``detailed_prediction`` is created which contains additional prediction details, such as
probabilities, contributions and so on.

In manual mode of external backend always require a specification of cluster location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In previous versions, H2O client was able to discover nodes using the multicast search.
That is now removed and IP:Port of any node of external cluster to which we need
to connect is required. This requirement may be removed in the future architecture of Sparkling Water.

Removal of Deprecated Methods and Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``getColsampleBytree`` and ``setColsampleBytree`` methods are removed from the XGBoost API. Please use
  the new ``getColSampleByTree`` and ``setColSampleByTree``.

- Removal of deprecated option ``spark.ext.h2o.external.cluster.num.h2o.nodes`` and corresponding setters.
  Please use ``spark.ext.h2o.external.cluster.size`` or the corresponding setter ``setClusterSize``.

- Removal of deprecated algorithm classes in package ``org.apache.spark.h2o.ml.algos``. Please
  use the classes from the package ``ai.h2o.sparkling.ml.algos``. Their API remains the same as before. This is the
  beginning of moving Sparkling Water classes to our distinct package ``ai.h2o.sparkling``

- Removal of deprecated option ``spark.ext.h2o.external.read.confirmation.timeout`` and related setters.
  This option is removed without a replacement as it is no longer needed.

- Removal of deprecated parameter ``SelectBestModelDecreasing`` on the Grid Search API. Related getters and setters
  have been also removed. This method is removed without replacement as we now internally sort
  the models with the ordering meaningful to the specified sort metric.

- TargetEncoder transformer now accepts the ``outputCols`` parameter which can be used to override the default output
  column names.

- On PySparkling ``H2OGLM`` API, we removed deprecated parameter ``alpha`` in favor of ``alphaValue`` and ``lambda_`` in favor of
  ``lambdaValue``. On Both PySparkling and Sparkling Water ``H2OGLM`` API, we removed methods ``getAlpha`` in favor of
  ``getAlphaValue``, ``getLambda`` in favor of ``getLambdaValue``, ``setAlpha`` in favor of ``setAlphaValue`` and
  ``setLambda`` in favor of ``setLambdaValue``. These changes ensure the consistency across Python and Scala APIs.

Change of Versioning Scheme
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Version of Sparkling Water is changed to the following pattern: ``H2OVersion-SWPatchVersion-SparkVersion``, where:
``H2OVersion`` is full H2O Version which is integrated to Sparkling Water. ``SWPatchVersion`` is used to specify
a patch version and ``SparkVersion`` is a Spark version. This change of scheme allows us to do releases of Sparkling Water
without the need of releasing H2O if there is only change on the Sparkling Water side. In that case, we just increment the
``SWPatchVersion``. The new version therefore looks, for example, like ``3.26.0.9-2-2.4``. This version tells us this
Sparkling Water is integrating H2O ``3.26.0.9``, it is the second release with ``3.26.0.9`` version and is for Spark ``2.4``.

Renamed Property for Passing Extra HTTP Headers for Flow UI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The configuration property ``spark.ext.h2o.client.flow.extra.http.headers`` was renamed to
to ``spark.ext.h2o.flow.extra.http.headers`` since Flow UI can also run on H2O nodes and the value of the property is
also propagated to H2O nodes since the major version ``3.28.0.1-1``.

Automatic mode of External Backend now keeps H2O Flow accessible on worker nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The option ``spark.ext.h2o.node.enable.web`` does not have any effect anymore for automatic mode of external
backend as we required H2O Flow to be accessible on the worker nodes. The associated getters and setters do also
not have any effect in this case.

From any previous version to 3.26.11
------------------------------------

- Users of Sparkling Water external cluster in manual mode on Hadoop need to update the command the external cluster is launched with.
  A new parameter ``-sw_ext_backend`` needs to be added to the h2odriver invocation.

