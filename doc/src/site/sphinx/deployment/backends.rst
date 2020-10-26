.. _backend:

Sparkling Water Backends
------------------------

Internal Backend
~~~~~~~~~~~~~~~~

In the internal backend, an H2O cluster is created automatically during the ``H2OContext.getOrCreate`` call.
Because it's not technically possible to get the number of executors in Spark, we try to discover all executors
at the initiation of ``H2OContext``, and we start the H2O instance inside of each discovered executor. This
solution is easiest to deploy; however, when Spark or YARN kills the executor the whole H2O cluster goes down
since H2O does not support high availability.

The internal backend is the default behavior for Sparkling Water. The alternative is the external backend. The behavior
can be changed via the Spark configuration property ``spark.ext.h2o.backend.cluster.mode`` by specifying either
``external`` or ``internal``. It is also possible to change the type of the backend on our configuration object
, ``H2OConf``. ``H2OConf`` is a simple wrapper around ``SparkConf`` and inherits all properties in the Spark configuration.

Here we show a few examples of how ``H2OContext`` can be started with the internal backend.

Explicitly specify internal backend on ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setInternalClusterMode()
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf().setInternalClusterMode()
            hc = H2OContext.getOrCreate(conf)


If ``spark.ext.h2o.backend.cluster.mode`` property was set to ``internal`` either on the command
line or on the ``SparkConf`` class,  we can call just the following as ``H2OContext`` internally creates
``H2OConf`` which is picking up the value of the property:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()


We can however also explicitly pass the ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf()
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf()
            hc = H2OContext.getOrCreate(conf)


.. _external-backend:

External Backend
~~~~~~~~~~~~~~~~

In the external cluster, we use the H2O cluster running separately from the rest of the Spark application. This separation
gives us more stability because we are no longer affected by Spark executors being killed, which can
lead (as in the previous mode) to h2o cluster being killed as well.

There are two deployment strategies of the external cluster: manual and automatic. In manual mode, we need to start
the H2O cluster, and in the automatic mode, the cluster is started for us automatically based on our configuration.
In Hadoop environments, the creation of the cluster is performed by a simple process called H2O driver.
When the cluster is fully formed, the H2O driver terminates. In both modes, we have to store a path of H2O driver jar
to the environment variable ``H2O_DRIVER_JAR``.

.. code:: bash

    H2O_DRIVER_JAR=$(./bin/get-h2o-driver.sh some_hadoop_distribution)

Automatic Mode of External Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In automatic mode, the H2O cluster is started automatically. The cluster can be started automatically only in YARN
environment at the moment. We recommend this approach, as it is easier to deploy external clusters in this mode
and it is also more suitable for production environments. When the H2O cluster is started on YARN, it is started
as a map-reduce job, and it always uses the flat-file approach for nodes to cloud up.

Get H2O driver, for example, for cdh 5.8:

.. code:: bash

    H2O_DRIVER_JAR=$(./bin/get-h2o-driver.sh cdh5.8)

To start an H2O cluster and connect to it, run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf()
                        .setExternalClusterMode()
                        .useAutoClusterStart()
                        .setH2ODriverPath("path_to_h2o_driver")
                        .setClusterSize(1) // Number of H2O worker nodes to start
                        .setExternalMemory("2G") // Memory per single H2O worker node
                        .setYARNQueue("abc")
            val hc = H2OContext.getOrCreate(conf)

        In case we stored the path of the driver H2O jar to environmental variable ``H2O_DRIVER_JAR``, we don't
        have to specify ``setH2ODriverPath`` as Sparkling Water will read the path from the environmental variable.

    .. tab-container:: Python
        :title: Python

         .. code:: python

            from pysparkling import *
            conf = H2OConf()
                    .setExternalClusterMode()
                    .useAutoClusterStart()
                    .setH2ODriverPath("path_to_h2o_driver")
                    .setClusterSize(1) # Number of H2O worker nodes to start
                    .setExternalMemory("2G") # Memory per single H2O worker node
                    .setYARNQueue("abc")
            hc = H2OContext.getOrCreate(conf)

        In case we stored the path of the driver H2O jar to environmental variable ``H2O_DRIVER_JAR``, we don't
        have to specify ``setH2ODriverPath`` as Sparkling Water will read the path from the environmental variable.

When specifying the queue, we recommend that this queue has YARN preemption off in order to have stable an H2O cluster.

In the case of Scala, it can also happen that we might need to explicitly set the client's IP or network. To see how this can be configured, please
see `Specifying the Client Network in Scala`_.

Manual Mode of External Backend on Hadoop
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In manual mode, we need to start the H2O cluster before connecting to it manually. At this section, we will start the cluster
on Hadoop.

Get H2O driver, for example, for cdh 5.8:

.. code:: bash

    H2O_DRIVER_JAR=$(./bin/get-h2o-driver.sh cdh5.8)

Set path to sparkling-water-assembly-extensions-SUBST_SW_VERSION-all.jar which is bundled in Sparkling Water archive.

.. code:: bash

    SW_EXTENSIONS_ASSEMBLY=/path/to/sparkling-water-SUBST_SW_VERSION/jars/sparkling-water-assembly-extensions-SUBST_SW_VERSION-all.jar

Start H2O cluster on Hadoop:

.. code:: bash

    hadoop -jar $H2O_DRIVER_JAR -libjars $SW_EXTENSIONS_ASSEMBLY -sw_ext_backend -jobname test -nodes 3 -mapperXmx 6g

The ``-sw_ext_backend`` is required as without it, the cluster won't allow Sparkling Water client to connect to it.

After this step, we should have an H2O cluster with 3 nodes running on Hadoop.

To connect to this external cluster, run the following commands:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf()
                        .setExternalClusterMode()
                        .useManualClusterStart()
                        .setH2OCluster("representant_ip", representant_port)
                        .setCloudName("test")
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf()
                    .setExternalClusterMode()
                    .useManualClusterStart()
                    .setH2OCluster("representant_ip", representant_port)
                    .setCloudName("test")
            hc = H2OContext.getOrCreate(conf)

The ``representant_ip`` and ``representant_port`` are IP and port of any node in the external cluster to which Sparkling
Water should connect.

.. _external-backend-manual-standalone:

Manual Mode of External Backend without Hadoop (standalone)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In manual mode, we need to start the H2O cluster before connecting to it manually. At this section, we will start the cluster
as a standalone application (without Hadoop).

Get assembly H2O jar:

.. code:: bash

    H2O_JAR=$(./bin/get-h2o-driver.sh standalone)

Set path to sparkling-water-assembly-extensions-SUBST_SW_VERSION-all.jar which is bundled in Sparkling Water archive.

.. code:: bash

    SW_EXTENSIONS_ASSEMBLY=/path/to/sparkling-water-SUBST_SW_VERSION/jars/sparkling-water-assembly-extensions_SUBST_SCALA_BASE_VERSION-SUBST_SW_VERSION-all.jar

To start an external H2O cluster, run:

.. code:: bash

    java -cp "$H2O_JAR:$SW_EXTENSIONS_ASSEMBLY" water.H2OApp -allow_clients -name test -flatfile path_to_flatfile

where the flat-file's content are lines in the format of IP:port of the nodes where H2O is supposed to run. To
read more about flat-file and its format, please
see `H2O's flat-file configuration property <https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/H2O-DevCmdLine.md#flatfile>`__.


To connect to this external cluster, run the following commands:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf()
                        .setExternalClusterMode()
                        .useManualClusterStart()
                        .setH2OCluster("representant_ip", representant_port)
                        .setClusterSize(3)
                        .setCloudName("test")
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf()
                    .setExternalClusterMode()
                    .useManualClusterStart()
                    .setH2OCluster("representant_ip", representant_port)
                    .setClusterSize(3)
                    .setCloudName("test")
            hc = H2OContext.getOrCreate(conf)

The ``representant_ip`` and ``representant_port`` are ip and port of any node in the external cluster to which Sparkling
Water should connect.

Specifying the Client Network in Scala
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible that Spark driver, in which we are running H2O client which is connecting to the external H2O cluster, is
connected to multiple networks.

In this case, it can happen that external H2O cluster decides to use addresses from network A while Spark decides to use
addresses for its executors and driver from network B. When we start ``H2OContext``, the H2O
client running inside of the Spark Driver can get the same IP address as the Spark driver, and, thus, the rest
of the H2O cluster can't see it. This shouldn't happen in environments where the nodes are connected to only one
network; however, we provide a configuration for how to deal with this case as well. This problem exists only in
Sparkling Water Scala client.

Let's assume we have two H2O nodes on addresses 192.168.0.1 and 192.168.0.2. Let's also assume that the Spark driver
is available on 172.16.1.1, and the only executor is available on 172.16.1.2. The node with the Spark driver
is also connected to the 192.168.0.x network with address 192.168.0.3.

In this case, there is a chance that the H2O client will use the address from 172.16.x.x network instead
of the 192.168.0.x one, which can lead to the problem that the H2O cluster and H2O client can't see each other.

We can force the client to use the correct network or address using the following configuration:

.. code:: scala

    import ai.h2o.sparkling._
    val conf = new H2OConf()
                .setExternalClusterMode()
                .useManualClusterStart()
                .setH2OCluster("ip", port)
                .setClientNetworkMask("192.168.0.0/24")
                .setClusterSize(2)
                .setCloudName("test")
    val hc = H2OContext.getOrCreate(conf)

Instead of ``setClientNetworkMask``, we can also use more strict variant and specify the IP address directly using
``setClientIp("192.168.0.3")``. This IP address needs to be one of the IP addresses of the Spark driver and in
the same network as the rest of the H2O worker nodes.
