H2O Frame as Spark's Data Source
--------------------------------

H2O Frame can be used directly as a Spark data source.

Reading Spark Data Frame from H2O Frame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's suppose we have an H2OFrame with key ``testFrame``

There are multiple ways in which the Spark data frame can be loaded from H2OFrame:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val df = spark.read.format("h2o").option("key", "testFrame").load()

        .. code:: scala

            val df = spark.read.format("h2o").load("testFrame")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            df = spark.read.format("h2o").option("key", "testFrame").load()

        .. code:: python

            df = spark.read.format("h2o").load("testFrame")


Saving H2O Frame as Spark Data Frame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's suppose we have Spark Data Frame ``df``.

There are multiple ways in which the Spark Data Frame can be saved as H2OFrame:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            df.write.format("h2o").option("key", "new_key").save()

        .. code:: scala

            df.write.format("h2o").save("new_key")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            df.write.format("h2o").option("key", "new_key").save()

        .. code:: python

            df.write.format("h2o").save("new_key")


All variants save the data frame as an H2OFrame with the key ``new_key``. They will not
succeed if the H2OFrame with the same key already exists.

Loading & Saving Options
~~~~~~~~~~~~~~~~~~~~~~~~

If the key is specified with the ``key`` option and also in the ``load/save`` method, then
the ``key`` option is preferred

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val df = spark.read.from("h2o").option("key", "key_one").load("key_two")

        .. code:: scala

            val df = spark.read.from("h2o").option("key", "key_one").save("key_two")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            df = spark.read.from("h2o").option("key", "key_one").load("key_two")

        .. code:: python

            df = spark.read.from("h2o").option("key", "key_one").save("key_two")



In all examples, ``key_one`` is used.

Specifying Saving Mode
~~~~~~~~~~~~~~~~~~~~~~

There are four save modes available when saving data using the Data Source API: ``append``, ``overwrite``, ``error`` and ``ignore``. The full description is available in the Spark documentation for `Spark Save Modes <http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes>`__.

- If ``append`` is used, an existing H2OFrame with the same key is deleted, and a new one containing the union of all rows from the original H2O Frame and from the appended Data Frame is created with the same key.

- If ``overwrite`` is used, an existing H2OFrame with the same key is deleted, and a new one with the new rows is created with the same key.

- If ``error`` is used and an H2OFrame with the specified key already exists, then an exception is thrown.

- If ``ignore`` is used and an H2OFrame with the specified key already exists, then no data is changed.
