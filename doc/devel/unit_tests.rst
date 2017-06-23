Running Unit Tests
------------------

To invoke tests for example from IntelliJ Idea, the following JVM
options are required:

- ``-Dspark.testing=true``

To invoke unit tests from gradle, run:

.. code:: shell

    ./gradlew build -x integTest -x scriptTest