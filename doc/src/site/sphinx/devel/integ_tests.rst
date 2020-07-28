Integration Tests
-----------------

This section describes integration tests that are part of the Sparkling Water project.

The tests are performed for both Sparkling Water backend types (internal and external). Please see :ref:`backend` for more information about the backends.

Testing Environments
~~~~~~~~~~~~~~~~~~~~

-  Local - corresponds to setting Spark ``MASTER`` variable to one of
   ``local``, or ``local[*]``, or ``local-cluster[_,_,_]`` (``local-cluster`` mode is just for testing purposes) values
-  Standalone cluster - the ``MASTER`` variable points to existing
   standalone Spark cluster ``spark://...``
-  YARN cluster - the ``MASTER`` variable contains ``yarn-client`` or
   ``yarn-cluster`` values

--------------

Testing Scenarios
~~~~~~~~~~~~~~~~~

1. Initialize H2O on top of Spark by running ``H2OContext.getOrCreate()`` and verifying that H2O was properly initialized.
2. Load data with help from the H2O API from various data sources:

 -  local disk
 -  HDFS
 -  S3N

3. Convert from ``RDD[T]`` to ``H2OFrame``.
4. Convert from ``DataFrame`` to ``H2OFrame``.
5. Convert from ``H2OFrame`` to ``RDD``.
6. Convert from ``H2OFrame`` to ``DataFrame``.
7. Integrate with H2O Algorithms using RDD as algorithm input.
8. Integrate with MLlib Algorithms using H2OFrame as algorithm input (KMeans).
9. Integrate with MLlib pipelines.

--------------

Integration Tests Example
~~~~~~~~~~~~~~~~~~~~~~~~~

The following code reflects the use cases listed above. The code is executed in all testing environments (if applicable). Spark 2.0+ is required:

 - local
 - standalone cluster
 - YARN

1. Initialize H2O:

 .. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()
    import h2oContext.implicits._

2. Load data:

 - From the local disk:

    .. code:: scala

        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().getOrCreate()
        import ai.h2o.sparkling._
        val h2oContext = H2OContext.getOrCreate()

        import java.io.File
        val df: H2OFrame = H2OFrame(new File("examples/smalldata/airlines/allyears2k_headers.csv"))

    **Note**: The file must be present on all nodes. Specifically, in the case of the Sparkling Water internal backend, this must be present on all nodes with Spark. In the case of the Sparkling Water external backend, this must be present on all nodes with H2O.
     

 -  From HDFS:

    .. code:: scala

        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().getOrCreate()
        import ai.h2o.sparkling._
        val h2oContext = H2OContext.getOrCreate()

        val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
        val uri = new java.net.URI(path)
        val airlinesHF = H2OFrame(uri)

 - From S3N:

    .. code:: scala

        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().getOrCreate()
        import ai.h2o.sparkling._
        val h2oContext = H2OContext.getOrCreate()

        val path = "s3n://h2o-airlines-unpacked/allyears2k.csv"
        val uri = new java.net.URI(path)
        val airlinesHF = H2OFrame(uri)

    **Note**: Spark/H2O needs to know the AWS credentials specified in ``core-site.xml``. The credentials are passed via ``HADOOP_CONF_DIR``, which points to a configuration directory with ``core-site.xml``.

3. Convert from ``RDD[T]`` to ``H2OFrame``:

 .. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()

    val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(Some(v)))
    val hf: H2OFrame = h2oContext.asH2OFrame(rdd)

4. Convert from ``DataFrame`` to ``H2OFrame``:

 .. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v))).toDF
    val hf = h2oContext.asH2OFrame(df)

5. Convert from ``H2OFrame`` to ``RDD[T]``:

 .. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()

    val rdd = spark.sparkContext.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v)))
    val hf: H2OFrame = h2oContext.asH2OFrame(rdd)
    val newRdd = h2oContext.asRDD[IntHolder](hf)

6. Convert from ``H2OFrame`` to ``DataFrame``:

 .. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v))).toDF
    val hf = h2oContext.asH2OFrame(df)
    val newRdd = h2oContext.asSparkFrame(hf)
