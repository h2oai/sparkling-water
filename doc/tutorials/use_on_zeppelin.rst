Sparkling Water and Zeppelin
----------------------------

Since Sparkling Water exposes Scala API, it is possible to access it
directly from the Zeppelin's notebook cell marked by ``%spark`` tag.

Launch Zeppelin with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using Sparkling Water from Zeppelin is easy since Sparkling Water is
distributed as a Spark package. In this case, before launching Zeppelin
addition shell variable is needed:

.. code:: bash

    export SPARK_HOME=...# Spark 2.1 home
    export SPARK_SUBMIT_OPTIONS="--packages ai.h2o:sparkling-water-examples_2.11:2.1.0"
    bin/zeppelin.sh -Pspark-2.1

The command is using Spark 2.1 version and corresponding Sparkling Water
package.

Using Zeppelin
~~~~~~~~~~~~~~

The use of Sparkling Water package is directly driven by Sparkling Water
API. For example, getting ``H2OContext`` is straightforward:

.. code:: scala

    %spark
    import org.apache.spark.h2o._
    val hc = H2OContext.getOrCreate(spark)

Creating ``H2OFrame`` from Spark ``DataFrame``:

.. code:: scala

    %spark
    val df = sc.parallelize(1 to 1000).toDF
    val hf = hc.asH2OFrame(df)

Creating Spark ``DataFrame`` from ``H2OFrame``:

.. code:: scala

    %spark
    val df = hc.asDataFrame(hf)
