Integration Tests
-----------------

This documentation paper describes what is tested in terms of
integration tests as part of the Sparkling Water project.

The tests are perform for both Sparkling Water backend types. Please see `Sparkling Water Backends <../tutorials/backends.rst>`__
for more information about the backends.

Quick links:

- `Testing Environments`_
- `Testing Scenarios`_
- `Integration Tests Example`_

Testing Environments
--------------------

-  Local - corresponds to setting Spark ``MASTER`` variable to one of
   ``local``, or ``local[*]``, or ``local-cluster[_,_,_]`` values
-  Standalone cluster - the ``MASTER`` variable points to existing
   standalone Spark cluster ``spark://...``
-  YARN cluster - the ``MASTER`` variable contains ``yarn-client`` or
   ``yarn-cluster`` values

--------------

Testing Scenarios
-----------------

1. Initialize H2O on top of Spark by running
   ``H2OContext.getOrCreate(spark)`` and verifying that H2O was properly
   initialized.
2. Load data with help from the H2O API from various data sources:

-  local disk
-  HDFS
-  S3N

3. Convert from ``RDD[T]`` to ``H2OFrame``
4. Convert from ``DataFrame`` to ``H2OFrame``
5. Convert from ``H2OFrame`` to ``RDD``
6. Convert from ``H2OFrame`` to ``DataFrame``
7. Integrate with H2O Algorithms using RDD as algorithm input
8. Integrate with MLlib Algorithms using H2OFrame as algorithm input
   (KMeans)
9. Integrate with MLlib pipelines

--------------

Integration Tests Example
-------------------------

The following code reflects the use cases listed above. The code is
executed in all testing environments (if applicable). Spark 2.0+ required:

- local
- standalone cluster
- YARN

1. Initialize H2O:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)
    import h2oContext.implicits._

2. Load data:

- From the local disk:

    .. code:: scala

        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().getOrCreate()
        import org.apache.spark.h2o._
        val h2oContext = H2OContext.getOrCreate(spark)

        import java.io.File
        val df: H2OFrame = new H2OFrame(new File("examples/smalldata/allyears2k_headers.csv.gz"))

    Note: The file must be present on all nodes. In case of Sparkling Water internal backend, on all nodes with Spark. In case
    of Sparkling Water external backend, on all nodes with H2O.
     

-  From HDFS:

    .. code:: scala

        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().getOrCreate()
        import org.apache.spark.h2o._
        val h2oContext = H2OContext.getOrCreate(spark)

        val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
        val uri = new java.net.URI(path)
        val airlinesHF = new H2OFrame(uri)

- From S3N:

    .. code:: scala

        import org.apache.spark.sql.SparkSession
        val spark = SparkSession.builder().getOrCreate()
        import org.apache.spark.h2o._
        val h2oContext = H2OContext.getOrCreate(spark)

        val path = "s3n://h2o-airlines-unpacked/allyears2k.csv"
        val uri = new java.net.URI(path)
        val airlinesHF = new H2OFrame(uri)

    Note: Spark/H2O needs to know the AWS credentials specified in ``core-site.xml``. The credentials are passed via ``HADOOP_CONF_DIR``
    that points to a configuration directory with ``core-site.xml``.

3. Convert from ``RDD[T]`` to ``H2OFrame``:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)

    val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(Some(v)))
    val hf: H2OFrame = h2oContext.asH2OFrame(rdd)

4. Convert from ``DataFrame`` to ``H2OFrame``:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)

    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v))).toDF
    val hf = h2oContext.asH2OFrame(df)

5. Convert from ``H2OFrame`` to ``RDD[T]``:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)

    val rdd = spark.sparkContext.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v)))
    val hf: H2OFrame = h2oContext.asH2OFrame(rdd)
    val newRdd = h2oContext.asRDD[IntHolder](hf)

6. Convert from ``H2OFrame`` to ``DataFrame``:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)

    import spark.implicits._
    val df = spark.sparkContext.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v))).toDF
    val hf = h2oContext.asH2OFrame(df)
    val newRdd = h2oContext.asDataFrame(hf)(spark.sqlContext)

7. Integrate with H2O Algorithms using RDD as algorithm input:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)
    import h2oContext.implicits._
    import org.apache.spark.examples.h2o._

    val path = "examples/smalldata/prostate.csv"
    val prostateText = spark.sparkContext.textFile(path)
    val prostateRDD = prostateText.map(_.split(",")).map(row => ProstateParse(row))
    import _root_.hex.tree.gbm.GBM
    import _root_.hex.tree.gbm.GBMModel.GBMParameters
    val train: H2OFrame = prostateRDD
    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._response_column = "CAPSULE"
    gbmParams._ntrees = 10
    val gbmModel = new GBM(gbmParams).trainModel.get

8. Integrate with MLlib algorithms:

.. code:: scala

    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder().getOrCreate()
    import org.apache.spark.h2o._
    val h2oContext = H2OContext.getOrCreate(spark)
    import org.apache.spark.examples.h2o._

    import java.io.File
    val path = "examples/smalldata/prostate.csv"
    val prostateHF = new H2OFrame(new File(path))
    val prostateRDD = h2oContext.asRDD[Prostate](prostateHF)
    import org.apache.spark.mllib.clustering.KMeans
    import org.apache.spark.mllib.linalg.Vectors
    val train = prostateRDD.map( v => Vectors.dense(v.CAPSULE.get*1.0, v.AGE.get*1.0, v.DPROS.get*1.0,v.DCAPS.get*1.0, v.GLEASON.get*1.0))
    val clusters = KMeans.train(train, 5, 20)