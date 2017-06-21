H2O Frame as Spark's Data Source
--------------------------------

The way how H2O Frame can be used as Spark's Data Source differs a
little bit in Python and Scala.

Quick links:

- `Usage in Python - PySparkling`_
- `Usage in Scala`_
- `Specifying Saving Mode`_

Usage in Python - PySparkling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reading from H2O Frame
^^^^^^^^^^^^^^^^^^^^^^

Let's suppose we have H2OFrame ``frame``.

There are two ways how dataframe can be loaded from H2OFrame in
PySparkling:

.. code:: python

    df = spark.read.format("h2o").option("key", frame.frame_id).load()

or

.. code:: python

    df = spark.read.format("h2o").load(frame.frame_id)

Saving to H2O Frame
^^^^^^^^^^^^^^^^^^^

Let's suppose we have DataFrame ``df``.

There are two ways how dataframe can be saved as H2OFrame in
PySparkling:

.. code:: python

    df.write.format("h2o").option("key", "new_key").save()

or

.. code:: python

    df.write.format("h2o").save("new_key")

Both variants save dataframe as H2OFrame with key ``new_key``. They
don't succeed if the H2OFrame with the same key already exists.

Loading & Saving Options
^^^^^^^^^^^^^^^^^^^^^^^^

If the key is specified as ``key`` option and also in the ``load/save``
method, the option ``key`` is preferred

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

The shortest way how dataframe can be loaded from H2OFrame with default
settings is:

.. code:: scala

    val df = spark.read.h2o(frame.key)

There are two more ways how dataframe can be loaded from H2OFrame. These calls allow
us to specify additional options:

.. code:: scala

    val df = spark.read.format("h2o").option("key", frame.key.toString).load()

or

.. code:: scala

    val df = spark.read.format("h2o").load(frame.key.toString)

Saving to H2O Frame
^^^^^^^^^^^^^^^^^^^

Let's suppose we have DataFrame ``df``.

The shortest way how dataframe can be saved as H2O Frame with default
settings is:

.. code:: scala

    df.write.h2o("new_key")

There are two more ways how dataframe can be saved as H2OFrame. These calls allow
us to specify additional options:

.. code:: scala

    df.write.format("h2o").option("key", "new_key").save()

or

.. code:: scala

    df.write.format("h2o").save("new_key")

All three variants save dataframe as H2OFrame with key ``new_key``. They
don't succeed if the H2OFrame with the same key already exists.

Loading & Saving Options
^^^^^^^^^^^^^^^^^^^^^^^^

If the key is specified as ``key`` option and also in the ``load/save``
method, the option ``key`` is preferred

.. code:: scala

    val df = spark.read.from("h2o").option("key", "key_one").load("key_two")

or

.. code:: scala

    val df = spark.read.from("h2o").option("key", "key_one").save("key_two")

In both examples, ``key_one`` is used.

Specifying Saving Mode
~~~~~~~~~~~~~~~~~~~~~~

There are 4 save modes available when saving data using Data Source
API - ``append``, ``overwrite``, ``error`` and ``ignore``. The full description is available at `Spark Save Modes <http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes>`__.

- If ``append`` mode is used, an existing H2OFrame with the same key is
  deleted and new one containing union of all rows from original H2O Frame
  and appended Data Frame is created with the same key.

- If ``overwrite`` mode is used, an existing H2OFrame with the same key is
  deleted and new one with the new rows is created with the same key.

- If ``error`` mode is used and a H2OFrame with the specified key already
  exists, exception is thrown.

- If ``ignore`` mode is used and a H2OFrame with the specified key already
  exists, no data is changed.
