Spark Frame - H2O Frame Mapping
-------------------------------

Type mapping between H2O H2OFrame types and Spark DataFrame types
-----------------------------------------------------------------

For all primitive Scala types or Spark SQL (see
``org.apache.spark.sql.types``) types which can be part of Spark
RDD/DataFrame we provide mapping into H2O vector types (numeric,
categorical, string, time, UUID - see ``water.fvec.Vec``):

+----------------------+-----------------+------------+
| Scala type           | SQL type        | H2O type   |
+======================+=================+============+
| *NA*                 | BinaryType      | Numeric    |
+----------------------+-----------------+------------+
| Byte                 | ByteType        | Numeric    |
+----------------------+-----------------+------------+
| Short                | ShortType       | Numeric    |
+----------------------+-----------------+------------+
| Integer              | IntegerType     | Numeric    |
+----------------------+-----------------+------------+
| Long                 | LongType        | Numeric    |
+----------------------+-----------------+------------+
| Float                | FloatType       | Numeric    |
+----------------------+-----------------+------------+
| Double               | DoubleType      | Numeric    |
+----------------------+-----------------+------------+
| String               | StringType      | String     |
+----------------------+-----------------+------------+
| Boolean              | BooleanType     | Numeric    |
+----------------------+-----------------+------------+
| java.sql.Timestamp   | TimestampType   | Time       |
+----------------------+-----------------+------------+

--------------

Type mapping between H2O H2OFrame types and RDD[T] types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As type ``T`` we support following types:

+--------------------------------------------------+
| T                                                |
+==================================================+
| *NA*                                             |
+--------------------------------------------------+
| Byte                                             |
+--------------------------------------------------+
| Short                                            |
+--------------------------------------------------+
| Integer                                          |
+--------------------------------------------------+
| Long                                             |
+--------------------------------------------------+
| Float                                            |
+--------------------------------------------------+
| Double                                           |
+--------------------------------------------------+
| String                                           |
+--------------------------------------------------+
| Boolean                                          |
+--------------------------------------------------+
| java.sql.Timestamp                               |
+--------------------------------------------------+
| Any scala class extending scala ``Product``      |
+--------------------------------------------------+
| org.apache.spark.mllib.regression.LabeledPoint   |
+--------------------------------------------------+

As is specified in the table, Sparkling Water provides support for
transforming arbitrary scala class extending ``Product``, which are, for
example, all case classes.
