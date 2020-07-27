Provided Primitives
-------------------

The Sparkling Water provides the following primitives, which are the basic
classes used by Spark components:

+-------------------+--------------------------------------+--------------------------------------+
| Concept           | Implementation class                 | Description                          |
+===================+======================================+======================================+
| H2O context       | ``ai.h2o.sparkling.H2OContext``      | H2O Context that holds state and     |
|                   |                                      | provides primitives to transfer      |
|                   |                                      | RDD/DataFrames/Datasets into         |
|                   |                                      | H2OFrame and vice versa. It follows  |
|                   |                                      | design principles of Spark           |
|                   |                                      | primitives such as ``SparkSession``, |
|                   |                                      | ``SparkContext`` and ``SQLContext``. |
+-------------------+--------------------------------------+--------------------------------------+
