Running Sparkling Water
-----------------------

In order to run Sparkling Water, the environment must contain the property ``SPARK_HOME`` that points to the Spark distribution.

H2O on Spark can be started in the Spark Shell or in the Spark application as:

.. code:: bash

    ./bin/sparkling-shell

Sparkling Water (H2O on Spark) can be initiated using the following call:

.. code:: scala

    val hc = H2OContext.getOrCreate()

The semantic of the call depends on the configured Sparkling Water backend. For more information about the backends, please see :ref:`backend`.

In internal backend mode, the call will:

1. Collect the number and hostnames of the executors (worker nodes) in the Spark cluster
2. Launch H2O services on each detected executor
3. Create a cloud for H2O services based on the list of executors
4. Verify the H2O cloud status

In external backend mode, the call will:

1. Start H2O in client mode on the Spark driver
2. Start the separated H2O cluster on the configured YARN queue
3. Connect to the external cluster from the H2O client

To see how to run Sparkling Water on Windows, please visit :ref:`run_on_windows`.
