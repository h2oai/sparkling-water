Sparkling Water and Zeppelin
----------------------------

Because Sparkling Water exposes the Scala API, it is possible to access it directly from the Zeppelin's notebook cell marked by the ``%spark`` tag.

Launch Zeppelin with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using Sparkling Water from Zeppelin is easy because Sparkling Water is distributed as a Spark package. In this case, before launching Zeppelin, an addition shell variable is needed:

.. code:: bash

    export SPARK_HOME=...# Spark SUBST_SPARK_MAJOR_VERSION home
    export SPARK_SUBMIT_OPTIONS="--packages ai.h2o:sparkling-water-package_SUBST_SCALA_BASE_VERSION:SUBST_SW_VERSION"
    bin/zeppelin.sh -Pspark-SUBST_SPARK_MAJOR_VERSION

The above command uses Spark SUBST_SPARK_MAJOR_VERSION and the corresponding Sparkling Water package.

Using Zeppelin
~~~~~~~~~~~~~~

The use of the Sparkling Water package is directly driven by the Sparkling Water API. For example, getting ``H2OContext`` is straightforward:

.. code:: scala

    %spark
    import ai.h2o.sparkling._
    val hc = H2OContext.getOrCreate()

Creating an ``H2OFrame`` from a Spark ``DataFrame``:

.. code:: scala

    %spark
    val df = sc.parallelize(1 to 1000).toDF
    val hf = hc.asH2OFrame(df)

Creating a Spark ``DataFrame`` from an ``H2OFrame``:

.. code:: scala

    %spark
    val df = hc.asSparkFrame(hf)
