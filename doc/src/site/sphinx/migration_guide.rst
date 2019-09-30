Migration Guide
===============

Migration guide between major Sparkling Water versions

From 3.26 To 3.28
-----------------

String instead of enums in Sparkling Water Algo API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- In scala, Setters on the pipeline wrappers for H2O algorithms now accepts strings in places where they accepted
  enum values before. Before, we called, for example:

.. code-block:: scala

    import hex.genmodel.utils.DistributionFamily
    val gbm = H2OGBM()
    gbm.setDistribution(DistributionFamily.multinomial)


Now, the correct code is:

.. code-block:: scala

    val gbm = H2OGBM()
    gbm.setDistribution("multinomial")

which makes the Python and Scala consistent. Both upper case and lower case values are valid and if a wrong
input is entered, user is warned with correct possible values.

Switch to Java 1.8 on Spark 2.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water for Spark 2.1 now requires Java 1.8 and higher.

DRF exposed into Sparkling Water Algorithm API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DRF is now exposed in the Sparkling Water. Please see our documentation to learn how to use it :ref:`drf`.

Single value in prediction column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default name of the prediction column is changed to ``prediction`` from ``prediction_output``. Also, the
prediction output now contains always directly the predicted value. In case of regression issue the predicted numeric value
and in case of classification the predicted label. If you are interested in more details created during the prediction,
please make sure to set ``withDetailedPredictionCol`` to ``true`` via the setters on both PySparkling Scala side.
When enabled, additional column ``detailed_prediction`` is created which contains additional prediction details, such as
probabilities, contributions and so on.

Removal of Deprecated Methods and Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``getColsampleBytree and setColsampleBytree`` methods are removed on our algorithm API. Please use
  the new ``getColSampleByTree`` and ``setColSampleByTree``

- Remove deprecated option ``spark.ext.h2o.external.cluster.num.h2o.nodes`` and corresponding setters.
  Please use ``spark.ext.h2o.external.cluster.size`` or corresponding setter ``setClusterSize``

- Deprecated Algorithm classes in package ``org.apache.spark.h2o.ml.algos`` have been removed. Please
  use the classes from the package ``ai.h2o.sparkling.ml.algos``. Their API remains the same. This is the
  beginning of moving Sparkling Water classes to our distinct package.

- Remove deprecated option ``spark.ext.h2o.external.read.confirmation.timeout`` and related setters.
  This option is removed without replacement as it is no longer needed.

- Remove deprecated parameter ``SelectBestModelDecreasing`` on our Grid Search API. Related getters and setters
  have been also removed. This method is removed without replacement as we now internally correctly sort
  the models according to what is best for the specified metric.

- TargetEncoder transformer now accepts outputCols parameter which you can use to override the default output
  column names.
