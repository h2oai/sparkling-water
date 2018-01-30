H2O Frame as Spark's Data Source
--------------------------------

The way that an H2O Frame can be used as Spark's Data Source differs between Python and Scala.

Quick links:

- `Usage in Python - PySparkling`_
- `Usage in Scala`_
- `Specifying Saving Mode`_

Usage in Python - PySparkling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reading from H2O Frame
^^^^^^^^^^^^^^^^^^^^^^

Let's suppose we have an H2OFrame ``frame``.

There are two ways in which the dataframe can be loaded from H2OFrame in PySparkling:

.. code:: python

    df = spark.read.format("h2o").option("key", frame.frame_id).load()

or

.. code:: python

    df = spark.read.format("h2o").load(frame.frame_id)

Saving to H2O Frame
^^^^^^^^^^^^^^^^^^^

Let's suppose we have DataFrame ``df``.

There are two ways in which the dataframe can be saved as H2OFrame in PySparkling:

.. code:: python

    df.write.format("h2o").option("key", "new_key").save()

or

.. code:: python

    df.write.format("h2o").save("new_key")

Both variants save the dataframe as an H2OFrame with the key ``new_key``. They will not succeed if the H2OFrame with the same key already exists.

Loading & Saving Options
^^^^^^^^^^^^^^^^^^^^^^^^

If the key is specified with the ``key`` option and also in the ``load/save`` method, then the ``key`` option is preferred

.. code:: python

    df = spark.read.from("h2o").option("key", "key_one").load("key_two")

or

.. code:: python

    df = spark.read.from("h2o").option("key", "key_one").save("key_two")

In both examples, ``key_one`` is used.

Usage in Scala
~~~~~~~~~~~~~~

Reading from H2O Frame
^^^^^^^^^^^^^^^^^^^^^^

Let's suppose we have H2OFrame ``frame``.

The shortest way in which the dataframe can be loaded from the H2OFrame with default settings is:

.. code:: scala

    val df = spark.read.h2o(frame.key)

There are two more ways in which the dataframe can be loaded from H2OFrame. These calls allow us to specify additional options:

.. code:: scala

    val df = spark.read.format("h2o").option("key", frame.key.toString).load()

or

.. code:: scala

    val df = spark.read.format("h2o").load(frame.key.toString)

Saving to H2O Frame
^^^^^^^^^^^^^^^^^^^

Let's suppose we have DataFrame ``df``.

The shortest way in which a dataframe can be saved as an H2O Frame with default settings is:

.. code:: scala

    df.write.h2o("new_key")

There are two additional methods for saving a dataframe as an H2OFrame. These calls allow us to specify additional options:

.. code:: scala

    df.write.format("h2o").option("key", "new_key").save()

or

.. code:: scala

    df.write.format("h2o").save("new_key")

All three variants save the dataframe as an H2OFrame with key ``new_key``. They will not succeed if the H2OFrame with the same key already exists.

Loading & Saving Options
^^^^^^^^^^^^^^^^^^^^^^^^

If the key is specified with the ``key`` option and also in the ``load/save`` method, then the ``key`` option is preferred.

.. code:: scala

    val df = spark.read.from("h2o").option("key", "key_one").load("key_two")

or

.. code:: scala

    val df = spark.read.from("h2o").option("key", "key_one").save("key_two")

In both examples, ``key_one`` is used.

Specifying Saving Mode
~~~~~~~~~~~~~~~~~~~~~~

There are four save modes available when saving data using the Data Source API: ``append``, ``overwrite``, ``error`` and ``ignore``. The full description is available in the Spark documentation for `Spark Save Modes <http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes>`__.

- If ``append`` is used, an existing H2OFrame with the same key is deleted, and a new one containing the union of all rows from the original H2O Frame and from the appended Data Frame is created with the same key.

- If ``overwrite`` is used, an existing H2OFrame with the same key is deleted, and new one with the new rows is created with the same key.

- If ``error`` is used and an H2OFrame with the specified key already exists, then an exception is thrown.

- If ``ignore`` is used and an H2OFrame with the specified key already exists, then no data is changed.
