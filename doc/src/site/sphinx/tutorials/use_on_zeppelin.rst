Sparkling Water and Zeppelin
----------------------------

Because Sparkling Water exposes the Scala API, it is possible to access it directly from the Zeppelin's notebook cell marked by the ``%spark`` tag.

Launch Zeppelin with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using Sparkling Water from Zeppelin is easy because Sparkling Water is distributed as a Spark package. In this case, before launching Zeppelin, an addition shell variable is needed:

.. code:: bash

    export SPARK_HOME=...# Spark 2.2 home
    export SPARK_SUBMIT_OPTIONS="--packages ai.h2o:sparkling-water-examples_2.11:SUBST_SW_VERSION"
    bin/zeppelin.sh -Pspark-2.2

The above command uses Spark 2.2 and the corresponding Sparkling Water package.

Using Zeppelin
~~~~~~~~~~~~~~

The use of the Sparkling Water package is directly driven by the Sparkling Water API. For example, getting ``H2OContext`` is straightforward:

.. code:: scala

    %spark
    import org.apache.spark.h2o._
    val hc = H2OContext.getOrCreate(spark)

Creating an ``H2OFrame`` from a Spark ``DataFrame``:

.. code:: scala

    %spark
    val df = sc.parallelize(1 to 1000).toDF
    val hf = hc.asH2OFrame(df)

Creating a Spark ``DataFrame`` from an ``H2OFrame``:

.. code:: scala

    %spark
    val df = hc.asDataFrame(hf)
