Spark Frame <--> H2O Frame Conversions
--------------------------------------

Quick links:

- `Converting an H2OFrame into an RDD[T]`_
- `Converting an H2OFrame into a DataFrame`_
- `Converting an RDD[T] into an H2OFrame`_
- `Converting a DataFrame into an H2OFrame`_

Converting an H2OFrame into an RDD[T]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` class provides the method ``asRDD``, which creates an RDD-like wrapper around the provided H2O's H2OFrame:

.. code:: scala

    def asRDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A]

The call expects the type ``A`` to create a correctly typed RDD. The conversion requires type ``A`` to be bound by the ``Product`` interface.
The relationship between the columns of the H2OFrame and the attributes of class ``A`` is based on name matching.

Example
^^^^^^^

.. code:: scala

    case class Person(name: String, age: Int)
    val rdd = asRDD[Person](h2oFrame)

Converting an H2OFrame into a DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` class provides the method ``asDataFrame``, which creates a DataFrame-like wrapper around the provided H2OFrame:

.. code:: scala

    def asDataFrame(fr: H2OFrame): DataFrame

The schema of the created instance of the ``DataFrame`` is derived from the column names and types of the specified ``H2OFrame``.

Example
^^^^^^^

.. code:: scala

    val dataFrame = asDataFrame(h2oFrame)

Converting an RDD[T] into an H2OFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` provides conversion method from the specified ``RDD[A]`` to ``H2OFrame``. As with conversion
in the opposite direction, the type ``A`` has to satisfy the upper bound expressed by the type ``Product``. The conversion
creates a new ``H2OFrame``, transfers data from the specified RDD, and saves it to to the H2O backend.

.. code:: scala

    def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A]): H2OFrame

The API also provides a version, which allows for specifying the name for the resulting H2OFrame.

.. code:: scala

    def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A], frameName: String): H2OFrame

Example
^^^^^^^

.. code:: scala

    val h2oFrame = h2oContext.asH2OFrame(rdd)

Converting a DataFrame into an H2OFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``H2OContext`` provides conversion method from the specified ``DataFrame`` to ``H2OFrame``.
The conversion creates a new ``H2OFrame``, transfer data from the specified ``DataFrame``, and saves it to the H2O
backend.

.. code:: scala

    def asH2OFrame(df: DataFrame): H2OFrame

The API also provides a version, which allows for specifying the name for the resulting H2OFrame.

.. code:: scala

    def asH2OFrame(rdd : DataFrame, frameName: String): H2OFrame

Example
^^^^^^^

.. code:: scala

    val h2oFrame = h2oContext.asH2OFrame(df)
