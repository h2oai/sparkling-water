.. _build:

Build Sparkling Water
---------------------

Download and install Spark, and point the environment variable ``SPARK_HOME`` to the installation path. Then use the provided ``gradlew`` to build project.

In order to build the full distribution, run:

.. code:: bash

    ./gradlew dist

After this command finishes, the full distribution of Sparkling Water is available at `./dist/build/dist` directory.

- To avoid running tests,  use the ``-x check`` option.

If you don't want to build executable distribution, but just want to build or test specific modules:

- To build only a specific module, use, for example, ``./gradlew :sparkling-water-examples:build``.

- To build and test a specific module, use, for example, ``./gradlew :sparkling-water-examples:check``.


Note: If you would like to build against custom H2O Python package, specify ``H2O_HOME`` environment variable. The variable
should point to the root directory of H2O-3 repository. This is mainly used for integration testing with H2O-3.