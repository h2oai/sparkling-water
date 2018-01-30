Spark Frame <--> H2O Frame Conversions
--------------------------------------

Quick links:

- `Converting an H2OFrame into an RDD[T]`_
- `Converting an H2OFrame into a DataFrame`_
- `Converting an RDD[T] into an H2OFrame`_
- `Converting a DataFrame into an H2OFrame`_

Converting an H2OFrame into an RDD[T]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` class provides the explicit conversion ``asRDD``, which creates an RDD-like wrapper around the provided H2O's H2OFrame:

.. code:: scala

    def asRDD[A <: Product: TypeTag: ClassTag](fr : H2OFrame) : RDD[A]

The call expects the type ``A`` to create a correctly typed RDD. The conversion requires type ``A`` to be bound by the ``Product`` interface. The relationship between the columns of the H2OFrame and the attributes of class ``A`` is based on name matching.

Example
^^^^^^^

.. code:: scala

    val df: H2OFrame = ...
    val rdd = asRDD[Weather](df)

--------------

Converting an H2OFrame into a DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` class provides the explicit conversion, ``asDataFrame``, which creates a DataFrame-like wrapper around the provided H2O H2OFrame. Technically, it provides the ``RDD[sql.Row]`` RDD API:

.. code:: scala

    def asDataFrame(fr : H2OFrame)(implicit sqlContext: SQLContext) : DataFrame

This call does not require any type of parameters, but because it creates ``DataFrame`` instances, it requires access to an instance of ``SQLContext``. In this case, the instance is provided as an implicit parameter of the call. The parameter can be passed in two ways: as an explicit parameter or by introducing an implicit variable into the current context.

The schema of the created instance of the ``DataFrame`` is derived from the column name and the type of ``H2OFrame`` specified.

Example
^^^^^^^

Using an explicit parameter in the call to pass sqlContext:

.. code:: scala

    val sqlContext = new SQLContext(sc)
    val schemaRDD = asDataFrame(h2oFrame)(sqlContext)

or as an implicit variable provided by the actual environment:

.. code:: scala

    implicit val sqlContext = new SQLContext(sc)
    val schemaRDD = asDataFrame(h2oFrame)

--------------

Converting an RDD[T] into an H2OFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` provides **implicit** conversion from the specified ``RDD[A]`` to ``H2OFrame``. As with conversion in the opposite direction, the type ``A`` has to satisfy the upper bound expressed by the type ``Product``. The conversion will create a new ``H2OFrame``, transfer data from the specified RDD, and save it to the H2O K/V data store.

.. code:: scala

    implicit def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A]) : H2OFrame

The API also provides a explicit version, which allows for specifying the name for the resulting H2OFrame.

.. code:: scala

    def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A], frameName: Option[String]) : H2OFrame

Example
^^^^^^^

.. code:: scala

    val rdd: RDD[Weather] = ...
    import h2oContext.implicits._
    // implicit call of H2OContext.asH2OFrame[Weather](rdd) is used 
    val hf: H2OFrame = rdd
    // Explicit call of of H2OContext API with name for resulting H2O frame
    val hfNamed: H2OFrame = h2oContext.asH2OFrame(rdd, Some("h2oframe"))

--------------

Converting a DataFrame into an H2OFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` provides **implicit** conversion from the specified ``DataFrame`` to ``H2OFrame``. The conversion will create a new ``H2OFrame``, transfer data from the specified ``DataFrame``, and save it to the H2O K/V data store.

.. code:: scala

    implicit def asH2OFrame(rdd : DataFrame) : H2OFrame

The API also provides an explicit version, which allows for specifying the name for the resulting H2OFrame.

.. code:: scala

    def asH2OFrame(rdd : DataFrame, frameName: Option[String]) : H2OFrame

Example
^^^^^^^

.. code:: scala

    val df: DataFrame = ...
    import h2oContext.implicits._
    // Implicit call of H2OContext.asH2OFrame(srdd) is used 
    val hf: H2OFrame = df 
    // Explicit call of H2Context API with name for resulting H2O frame
    val hfNamed: H2OFrame = h2oContext.asH2OFrame(df, Some("h2oframe"))
