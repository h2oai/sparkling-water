Build Sparkling Water
---------------------

Download and install Spark, and point the environment variable ``SPARK_HOME`` to the installation path. Then use the provided ``gradlew`` to build project.

In order to build the whole project, including PySparkling, run:

.. code:: bash

    ./gradlew build

- To avoid running tests, use the ``-x test -x integTest`` or the ``-x check`` option.

- To build only a specific module, use, for example, ``./gradlew :sparkling-water-examples:build``.

- To build and test a specific module, use, for example, ``./gradlew :sparkling-water-examples:check``.

Sparkling Water SUBST_SW_VERSION is built with Scala 2.11.

Note: If you would like to build against custom H2O Python package, specify ``H2O_HOME`` environment variable. The variable
should point to the root directory of H2O-3 repository. This is mainly used for integration testing with H2O-3.