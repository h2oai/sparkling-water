Build Sparkling Water
---------------------

Download Spark installation and point environment variable
``SPARK_HOME`` to it.

Then use the provided ``gradlew`` to build project:

In order to build the whole project including PySparkling, one of the
following properties needs to be set:

- ``H2O_HOME`` - should point to location of the local H2O project directory.
- ``H2O_PYTHON_WHEEL`` - should point to H2O Python Wheel.

If you are not sure which property to set, just run

.. code:: bash

    ./gradlew build

and the commands which sets the ``H2O_PYTHON_WHEEL`` will be shown on
your console and can be copy-pasted into your terminal. After setting
the property, the build needs to be rerun.

    To avoid running tests, use the ``-x test -x integTest`` or
    ``-x check`` option.

    To build only a specific module, use, for example,
    ``./gradlew :sparkling-water-examples:build``.

    To build and test a specific module, use, for example,
    ``./gradlew :sparkling-water-examples:check``.

Sparkling can be be build with Scala 2.10 or Scala 2.11. To build
Sparkling Water with non-default Scala version, use, for example,
``./gradlew build -x check -PscalaBaseVersion=2.10``.
