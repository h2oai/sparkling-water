Running Sparkling Water Examples
--------------------------------

The Sparkling Water distribution includes also a set of examples. You can find their implementation in `examples directory <https://github.com/h2oai/sparkling-water/tree/master/examples>`__. You can build and run them in the following way:

1. Obtain Sparkling Water distribution

Download the official release from `https://www.h2o.ai/download/ <https://www.h2o.ai/download/>`_.  or build it yourself.
To see how to build Sparkling Water, please see :ref:`build`. The scripts to start interactive Sparkling Water/PySparkling
shells are available in the `bin` directory of the extracted distribution.

2. Set the configuration of the demo Spark cluster (for example, ``local[*]``)

   .. code:: bash

        export SPARK_HOME="/path/to/spark/installation"
        export MASTER="local[*]"

   In this example, the description ``local[*]`` causes the creation of a single-node local cluster.

3. And run the example:

 - On Local Cluster:

    The local cluster is defined by ``MASTER`` address ``local``, ``local[*]`` or additional variants
    available at `Spark Master URLs <https://spark.apache.org/docs/latest/submitting-applications.html#master-urls>`__.

    .. code:: bash

        bin/run-example.sh <name of example>

 - On a Spark Standalone Cluster:

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


    - Observe the status of the application via Spark UI on http://localhost:8080/


Additional Details and Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more information about examples or to view additional examples, review the README file in the `examples folder <https://github.com/h2oai/sparkling-water/blob/master/examples/README.rst>`__.
