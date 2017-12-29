Running Unit Tests
------------------

To invoke tests for examples from IntelliJ Idea, the following JVM option is required:

- ``-Dspark.testing=true``

To invoke unit tests from gradle, run:

.. code:: shell

    ./gradlew build -x integTest -x scriptTest