Spark Frame <--> H2O Frame Conversions
--------------------------------------

Quick links:

- `Converting an H2OFrame into an RDD[T]`_
- `Converting an H2OFrame into a DataFrame`_
- `Converting an RDD[T] into an H2OFrame`_
- `Converting a DataFrame into an H2OFrame`_

Converting an H2OFrame into an RDD[T]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        The ``H2OContext`` class provides the method ``asRDD``, which creates an RDD-like wrapper around the provided H2O's H2OFrame:

        .. code:: scala

            def asRDD[A <: Product: TypeTag: ClassTag](fr: H2OFrame): RDD[A]

        The call expects the type ``A`` to create a correctly typed RDD. The conversion requires type ``A`` to be bound by the ``Product`` interface.
        The relationship between the columns of the H2OFrame and the attributes of class ``A`` is based on name matching.

        **Example**

        .. code:: scala

            case class Person(name: String, age: Int)
            val rdd = asRDD[Person](h2oFrame)

Converting an H2OFrame into a DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        The ``H2OContext`` class provides the method ``asSparkFrame``, which creates a DataFrame-like wrapper around the provided H2OFrame:

        .. code:: scala

            def asSparkFrame(fr: H2OFrame): DataFrame

        The schema of the created instance of the ``DataFrame`` is derived from the column names and types of the specified ``H2OFrame``.

        **Example**

        .. code:: scala

            val dataFrame = h2oContext.asSparkFrame(h2oFrame)

    .. tab-container:: Python
        :title: Python

        The ``H2OContext`` class provides the method ``asSparkFrame``, which creates a DataFrame-like wrapper around the provided H2OFrame:

        .. code:: python

            def asSparkFrame(self, h2oFrame)

        The schema of the created instance of the ``DataFrame`` is derived from the column names and types of the specified ``H2OFrame``.

        **Example**

        .. code:: python

            dataFrame = h2oContext.asSparkFrame(h2oFrame)

    .. tab-container:: R
        :title: R

        The ``H2OContext`` class provides the method ``asSparkFrame``, which creates a DataFrame-like wrapper around the provided H2OFrame:

        .. code:: R

            asSparkFrame = function(h2oFrame)

        The schema of the created instance of the ``DataFrame`` is derived from the column names and types of the specified ``H2OFrame``.

        **Example**

        .. code:: R

            dataFrame <- h2oContext$asSparkFrame(h2oFrame)

Converting an RDD[T] into an H2OFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        The ``H2OContext`` provides a conversion method from the specified ``RDD[A]`` to ``H2OFrame``. As with conversion
        in the opposite direction, the type ``A`` has to satisfy the upper bound expressed by the type ``Product``. The conversion
        creates a new ``H2OFrame``, transfers data from the specified RDD, and saves it to the DKV store on the H2O backend.

        .. code:: scala

            def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A]): H2OFrame

        The API also provides a version, which allows for specifying the name for the resulting H2OFrame.

        .. code:: scala

            def asH2OFrame[A <: Product : TypeTag](rdd : RDD[A], frameName: String): H2OFrame

        **Example**

        .. code:: scala

            val h2oFrame = h2oContext.asH2OFrame(rdd)

    .. tab-container:: Python
        :title: Python

        The ``H2OContext`` provides a conversion method from the specified PySpark ``RDD`` to ``H2OFrame``. The conversion
        creates a new ``H2OFrame``, transfers data from the specified RDD, and saves it to the DKV store on the H2O backend.

        .. code:: python

            def asH2OFrame(self, rdd, h2oFrameName=None, fullCols=-1)

        **Parameters**

        - ``rdd`` : PySpark RDD
        - ``h2oFrameName`` : Optional name for resulting H2OFrame
        - ``fullCols`` : A number of first n columns which are considered for conversion. -1 represents 'no limit'.

        **Example**

        .. code:: python

            h2oFrame = h2oContext.asH2OFrame(df)


Converting a DataFrame into an H2OFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        The ``H2OContext`` provides conversion method from the specified ``DataFrame`` to ``H2OFrame``.
        The conversion creates a new ``H2OFrame``, transfers data from the specified ``DataFrame``, and saves it
        to the DKV store on the H2O backend.

        .. code:: scala

            def asH2OFrame(df: DataFrame): H2OFrame

        The API also provides a version, which allows for specifying the name for the resulting H2OFrame.

        .. code:: scala

            def asH2OFrame(rdd : DataFrame, frameName: String): H2OFrame

        **Example**

        .. code:: scala

            val h2oFrame = h2oContext.asH2OFrame(df)

    .. tab-container:: Python
        :title: Python

        The ``H2OContext`` provides conversion method from the specified ``DataFrame`` to ``H2OFrame``.
        The conversion creates a new ``H2OFrame``, transfers data from the specified ``DataFrame``, and saves it
        to the DKV store on the H2O backend.

        .. code:: python

            def asH2OFrame(self, sparkFrame, h2oFrameName=None, fullCols=-1)

        **Parameters**

        - ``sparkFrame`` : PySpark data frame
        - ``h2oFrameName`` : Optional name for resulting H2OFrame
        - ``fullCols`` : A number of first n columns which are considered for conversion. -1 represents 'no limit'.

        **Example**

        .. code:: python

            h2oFrame = h2oContext.asH2OFrame(df)

    .. tab-container:: R
        :title: R

        The ``H2OContext`` provides conversion method from the specified ``DataFrame`` to ``H2OFrame``.
        The conversion creates a new ``H2OFrame``, transfers data from the specified ``DataFrame``, and saves it
        to the DKV store on the H2O backend.

        .. code:: R

            asH2OFrame = function(sparkFrame, h2oFrameName = NULL)

        **Parameters**

        - ``sparkFrame`` : Spark data frame
        - ``h2oFrameName`` : Optional name for resulting H2OFrame

        **Example**

        .. code:: R

            h2oFrame <- h2oContext$asH2OFrame(df)
