Migration Guide
===============

Migration guide between major Sparkling Water versions.

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
