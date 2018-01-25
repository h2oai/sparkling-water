Change Sparkling Shell Logs Location
------------------------------------

We can configure the location of the Sparkling Water logs, but we need to distinguish between the client/driver node and
the H2O worker nodes.

Client
~~~~~~
The logs location for the client node is driven by the ``spark.ext.h2o.client.log.dir`` spark configuration property.

We can either start the spark application with this configuration being passed on the command line such as:

.. code:: shell

    $SPARK_HOME/bin/spark-submit --conf "spark.ext.h2o.client.log.dir=/client/log/location"

or we can set it at runtime, but before you create the ``H2OContext``, as the in the following examples:

In Scala:

.. code:: scala

    val conf = new H2OConf(spark).setH2OClientLogDir("log_location")
    val hc = H2OContext.getOrCreate(spark, conf

In Python:

.. code:: python

    conf = H2OConf(spark).set_h2o_client_log_dir("log_location")
    hc = H2OContext.getOrCreate(spark, conf)

Worker Nodes
~~~~~~~~~~~~

For the worker nodes, we first check if the ``spark.yarn.app.container.log.dir`` environmental property is defined. If
it is available, we store the logs there.


If this environmental property is missing we try to read ``spark.ext.h2o.node.log.dir`` spark configuration property
and store the logs there. If this property is missing, we store the logs from the worker nodes into the default
directory which is specified as:

.. code:: scala

    System.getProperty("user.dir")/h2ologs/${sparkAppId}


So to change the logs location for the worker nodes we can either set the environment variable ``spark.yarn.app.container.log.dir``,
or specify the spark configuration property ``spark.ext.h2o.node.log.dir``.

We can start the spark application with this configuration being passed on the command line such as:

.. code:: shell

    $SPARK_HOME/bin/spark-submit --conf "spark.ext.h2o.node.log.dir=/worker/node/log/location"

or we can set it at runtime, but before you create the ``H2OContext``, as the in the following examples:

In Scala:

.. code:: scala

    val conf = new H2OConf(spark).setH2ONodeLogDir("log_location")
    val hc = H2OContext.getOrCreate(spark, conf

In Python:

.. code:: python

    conf = H2OConf(spark).set_h2o_node_log_dir("log_location")
    hc = H2OContext.getOrCreate(spark, conf)
