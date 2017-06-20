Running Sparkling Water examples
--------------------------------

The Sparkling Water distribution includes also a set of examples. You
can find their implementation in `examples directory <example/>`__. You
can run them in the following way:

1. Build a package that can be submitted to Spark cluster:

   .. code:: bash

        ./gradlew build -x check

2. Set the configuration of the demo Spark cluster (for example,
   ``local[*]`` or ``local-cluster[3,2,1024]``)

   .. code:: bash

        export SPARK_HOME="/path/to/spark/installation"
        export MASTER="local[*]"

   In this example, the description ``local[*]`` causes creation of a single node local cluster.

3. And run the example:

   .. code:: bash

        bin/run-example.sh


For more details about examples, please see the
`README <examples/README.md>`__ file in the `examples directory <examples/>`__.

Additional Examples
~~~~~~~~~~~~~~~~~~~
Additional examples are available at `examples folder <examples/>`__.
