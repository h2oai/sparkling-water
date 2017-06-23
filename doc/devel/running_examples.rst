Running Sparkling Water Examples
--------------------------------

The Sparkling Water distribution includes also a set of examples. You
can find their implementation in `examples directory <../../examples/>`__. You
can build and run them in the following way:

1. Build a package that can be submitted to Spark cluster:

   .. code:: bash

        ./gradlew build -x check

2. Set the configuration of the demo Spark cluster (for example, ``local[*]`` or ``local-cluster[3,2,1024]``)

   .. code:: bash

        export SPARK_HOME="/path/to/spark/installation"
        export MASTER="local[*]"

   In this example, the description ``local[*]`` causes creation of a single node local cluster.


3. And run the example:

- On Local Cluster

    The cluster is defined by MASTER address local-cluster[3,2,3072] which means that cluster
    contains 3 worker nodes, each having 2 CPU cores and 3GB of memory:

   .. code:: bash

        bin/run-example.sh <name of example>

- On a Spark Cluster:

    - Run the Spark cluster, for example via:

    .. code:: bash

        bin/launch-spark-cloud.sh

    - Verify that Spark is running: The Spark UI on http://localhost:8080/ should show 3 worker nodes
    - Export ``MASTER`` address of Spark master using:

    .. code:: bash

        export MASTER="spark://localhost:7077"

    - Run example:

    .. code:: bash

        bin/run-example.sh <name of example>

    - Observe status of the application via Spark UI on http://localhost:8080/


For more details about examples, please see the
`README <../../examples/README.md>`__ file in the `examples directory <../../examples/>`__.

Additional Examples
~~~~~~~~~~~~~~~~~~~
Additional examples are available at `examples folder <../../examples/>`__.
