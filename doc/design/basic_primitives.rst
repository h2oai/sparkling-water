Provided Primitives
-------------------

The Sparkling Water provides following primitives, which are the basic
classes used by Spark components:

+-------------------+--------------------------------------+----------------+
| Concept           | Implementation class                 | Description    |
+===================+======================================+================+
| H2O context       | ``org.apache.spark.h2o.H2OContext``  | H2O context    |
|                   |                                      | that holds H2O |
|                   |                                      | state and      |
|                   |                                      | provides       |
|                   |                                      | primitives to  |
|                   |                                      | transfer RDD   |
|                   |                                      | into H2OFrame  |
|                   |                                      | and vice       |
|                   |                                      | versa. It      |
|                   |                                      | follows design |
|                   |                                      | principles of  |
|                   |                                      | Spark          |
|                   |                                      | primitives     |
|                   |                                      | such as        |
|                   |                                      | ``SparkContext |
|                   |                                      | ``             |
|                   |                                      | or             |
|                   |                                      | ``SQLContext`` |
+-------------------+--------------------------------------+----------------+
| H2O entry point   | ``water.H2O``                        | Represents the |
|                   |                                      | entry point    |
|                   |                                      | for accessing  |
|                   |                                      | H2O services.  |
|                   |                                      | It holds       |
|                   |                                      | information    |
|                   |                                      | about the      |
|                   |                                      | actual H2O     |
|                   |                                      | cluster,       |
|                   |                                      | including a    |
|                   |                                      | list of nodes  |
|                   |                                      | and the status |
|                   |                                      | of distributed |
|                   |                                      | K/V datastore. |
+-------------------+--------------------------------------+----------------+
| H2O H2OFrame      | ``water.fvec.H2OFrame``              | H2OFrame is    |
|                   |                                      | the H2O data   |
|                   |                                      | structure that |
|                   |                                      | represents a   |
|                   |                                      | table of       |
|                   |                                      | values. The    |
|                   |                                      | table is       |
|                   |                                      | column-based   |
|                   |                                      | and provides   |
|                   |                                      | column and row |
|                   |                                      | accessors.     |
+-------------------+--------------------------------------+----------------+
| H2O Algorithms    | package ``hex``                      | Represents the |
|                   |                                      | H2O machine    |
|                   |                                      | learning       |
|                   |                                      | algorithms     |
|                   |                                      | library,       |
|                   |                                      | including      |
|                   |                                      | DeepLearning,  |
|                   |                                      | GBM,           |
|                   |                                      | RandomForest.  |
+-------------------+--------------------------------------+----------------+
