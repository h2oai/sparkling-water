.. _backend:

Sparkling Water Backends
------------------------

Internal Backend
~~~~~~~~~~~~~~~~

In the internal backend, an H2O cluster is created automatically during the ``H2OContext.getOrCreate`` call.
Because it's not technically possible to get the number of executors in Spark, we try to discover all executors
at the initiation of ``H2OContext``, and we start the H2O instance inside of each discovered executor. This
solution is easiest to deploy; however when Spark or YARN kills the executor the whole H2O cluster goes down
since H2O does not support high availability.

The internal backend is the default behavior for Sparkling Water. The alternative is external backend. The behaviour
can be changed via the Spark configuration property ``spark.ext.h2o.backend.cluster.mode`` by specifying either
``external`` or ``internal``. It is also possible to change the type of the backend on our configuration object
, ``H2OConf``. ``H2OConf`` is a simple wrapper around ``SparkConf`` and inherits all properties in the Spark configuration.

Here we show a few examples on how ``H2OContext`` can be started with the internal backend.

Explicitly specify internal backend on ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark).setInternalClusterMode()
            val hc = H2OContext.getOrCreate(spark, conf

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf(spark).set_internal_cluster_mode()
            hc = H2OContext.getOrCreate(spark, conf)


If ``spark.ext.h2o.backend.cluster.mode`` property was set to ``internal`` either on the command
line or on the ``SparkConf`` class,  we can call just the following as ``H2OContext`` internally creates
``H2OConf`` which is picking up the value of the property:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val hc = H2OContext.getOrCreate(spark)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate(spark)


We can however also explicitly pass the ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark)
            val hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf(spark)
            hc = H2OContext.getOrCreate(spark)



External Backend
~~~~~~~~~~~~~~~~

In the external cluster, we use the H2O cluster running separately from the rest of the Spark application. This separation
gives us more stability because we are no longer affected by Spark executors being killed, which can
lead (as in the previous mode) to h2o cluster being killed as well.

There are two deployment strategies of the external cluster: manual and automatic. In manual mode, we need to start
the H2O cluster, and in automatic mode, the cluster is started for us automatically based on our configuration.
In both modes, we can not use the regular H2O driver jar as the deployment artifact for the external H2O cluster.

Obtaining Extended H2O Jar
^^^^^^^^^^^^^^^^^^^^^^^^^^

The extended H2O jar can be downloaded using our helper script available in the official Sparkling Water distribution which
can be downloaded from `http://www.h2o.ai <http://www.h2o.ai/>`_.
After you download and unpack the Sparkling Water distribution package, you can use the ``./bin/get-extendend-h2o.sh``
script to download the extended H2O jar. This script expects a single argument that specifies the Hadoop version
for which you obtained the jar.

The following code downloads H2O extended JAR for the cdh5.8:

 .. code:: bash

    ./bin/get-extended-h2o.sh cdh5.8

If you don't want to run on Hadoop and instead want to run H2O in standalone mode, you can get the corresponding
extended H2O standalone jar as:

 .. code:: bash

    ./bin/get-extended-h2o.sh standalone

If you want to see a list of supported Hadoop versions, just run the shell script without any arguments as:

 .. code:: bash

    ./bin/get-extended-h2o.sh

The script downloads the jar to the current directory and prints the absolute path to the downloaded jar.

**Note**: If you want to get an extended H2O jar for Sparkling Water and H2O versions that have not yet been
released, you need to extend the JAR manually. This is explained in the following tutorial: :ref:`extend_jar_manually`.

Automatic Mode of External Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In automatic mode, the H2O cluster is started automatically. The cluster can be started automatically only in YARN
environment at the moment. We recommend this approach, as it is easier to deploy external clusters in this mode
and it is also more suitable for production environments. When the H2O cluster is started on YARN, it is started
as a map reduce job, and it always uses the flatfile approach for nodes to cloud up.

Get extended H2O driver, for example, for cdh 5.8:

.. code:: bash

    H2O_EXTENDED_JAR=$(./bin/get-extended-h2o.sh cdh5.8)

To start an H2O cluster and connect to it, run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark)
                        .setExternalClusterMode()
                        .useAutoClusterStart()
                        .setH2ODriverPath("path_to_extended_driver")
                        .setClusterSize(1) // Number of H2O worker nodes to start
                        .setMapperXmx("2G") // Memory per single H2O worker node
                        .setYARNQueue("abc")
            val hc = H2OContext.getOrCreate(spark, conf)

        In case we stored the path of the extended H2O jar to environmental variable ``H2O_EXTENDED_JAR``, we don't
        have to specify ``setH2ODriverPath`` as Sparkling Water will read the path from the environmental variable.

    .. tab-container:: Python
        :title: Python

         .. code:: python

            from pysparkling import *
            conf = H2OConf(spark)
                    .set_external_cluster_mode()
                    .use_auto_cluster_start()
                    .setH2ODriverPath("path_to_extended_driver")
                    .setClusterSize(1) # Number of H2O worker nodes to start
                    .setMapperXmx("2G") # Memory per single H2O worker node
                    .setYARNQueue("abc")
            hc = H2OContext.getOrCreate(spark, conf)

        In case we stored the path of the extended H2O jar to environmental variable ``H2O_EXTENDED_JAR``, we don't
        have to specify ``setH2ODriverPath`` as Sparkling Water will read the path from the environmental variable.

When specifying the queue, we recommend that this queue has YARN preemption off in order to have stable a H2O cluster.

It can also happen that we might need to explicitly set the client's IP or network. To see how this can be configured, please
see `Specifying the Client Network`_.

Manual Mode of External Backend on Hadoop
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In manual mode, we need to start the H2O cluster before connecting to it manually. At this section, we will start the cluster
on Hadoop.

Get extended H2O driver, for example, for cdh 5.8:

.. code:: bash

    H2O_EXTENDED_JAR=$(./bin/get-extended-h2o.sh cdh5.8)


Start H2O cluster on Hadoop:

.. code:: bash

    hadoop -jar $H2O_EXTENDED -sw_ext_backend -jobname test -nodes 3 -mapperXmx 6g

The ``-sw_ext_backend`` is required as without it, the cluster won't allow Sparkling Water client to connect to it.

After this step, we should have an H2O cluster with 3 nodes running on Hadoop.

To connect to this external cluster, run the following commands:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark)
                        .setExternalClusterMode()
                        .useManualClusterStart()
                        .setH2OCluster("representant_ip", representant_port)
                        .setClusterSize(3)
                        .setCloudName("test")
            val hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf(spark)
                    .set_external_cluster_mode()
                    .use_manual_cluster_start()
                    .setH2OCluster("representant_ip", representant_port)
                    .setClusterSize(3)
                    .set_cloud_name("test")
            hc = H2OContext.getOrCreate(spark, conf)

The ``representant_ip`` and ``representant_port`` are ip and port of any node in the external cluster to which Sparkling
Water should connect.

.. _external-backend-manual-standalone:

Manual Mode of External Backend without Hadoop (standalone)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In manual mode, we need to start the H2O cluster before connecting to it manually. At this section, we will start the cluster
as a standalone application (without Hadoop).

Get extended H2O driver:

.. code:: bash

    H2O_EXTENDED_JAR=$(./bin/get-extended-h2o.sh standalone)


To start an external H2O cluster, run:

.. code:: bash

    java -jar $H2O_EXTENDED_JAR -allow_clients -name test -flatfile path_to_flatfile

where the flatfile content are lines in the format of ip:port of the nodes where H2O is supposed to run. To
read more about flatfile and its format, please
see `H2O's flatfile configuration property <https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/H2O-DevCmdLine.md#flatfile>`__.


To connect to this external cluster, run the following commands:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark)
                        .setExternalClusterMode()
                        .useManualClusterStart()
                        .setH2OCluster("representant_ip", representant_port)
                        .setClusterSize(3)
                        .setCloudName("test")
            val hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf(spark)
                    .set_external_cluster_mode()
                    .use_manual_cluster_start()
                    .setH2OCluster("representant_ip", representant_port)
                    .setClusterSize(3)
                    .set_cloud_name("test")
            hc = H2OContext.getOrCreate(spark, conf)

The ``representant_ip`` and ``representant_port`` are ip and port of any node in the external cluster to which Sparkling
Water should connect.

Specifying the Client Network
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible that Spark driver, in which we are running H2O client which is connecting to the external H2O cluster, is
connected to multiple networks.

In this case, it can happen that external H2O cluster decides to use addresses from network A while Spark decides to use
addresses for its executors and driver from network B. When we start ``H2OContext``, the H2O
client running inside of the Spark Driver can get the same IP address as the Spark driver, and, thus, the rest
of the H2O cluster can't see it. This shouldn't happen in environments where the nodes are connected to only one
network; however we provide a configuration for how to deal with this case as well.

Let's assume we have two H2O nodes on addresses 192.168.0.1 and 192.168.0.2. Let's also assume that the Spark driver
is available on 172.16.1.1, and the only executor is available on 172.16.1.2. The node with the Spark driver
is also connected to the 192.168.0.x network with address 192.168.0.3.

In this case there is a chance that the H2O client will use the address from 172.16.x.x network instead
of the 192.168.0.x one, which can lead to the problem that the H2O cluster and H2O client can't see each other.

We can force the client to use the correct network or address using the following configuration:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

         .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark)
                        .setExternalClusterMode()
                        .useManualClusterStart()
                        .setH2OCluster("representant_ip", representant_port)
                        .setClientNetworkMask("192.168.0.0/24")
                        .setClusterSize(2)
                        .setCloudName("test")
            val hc = H2OContext.getOrCreate(spark, conf)

        Instead of ``setClientNetworkMask``, we can also use more strict variant and specify the IP address directly using
        ``setClientIp("192.168.0.3")``. This IP address needs to be one of the IP address of the Spark driver and in
        the same network as the rest of the H2O worker nodes.

    .. tab-container:: Python
        :title: Python


         .. code:: python

            from pysparkling import *
            conf = H2OConf(spark)
                    .set_external_cluster_mode()
                    .use_manual_cluster_start()
                    .setH2OCluster("representant_ip", representant_port)
                    .set_client_network_mask("192.168.0.0/24")
                    .setClusterSize(2)
                    .set_cloud_name("test")
            hc = H2OContext.getOrCreate(spark, conf)

        Instead of ``set_client_network_mask``, we can also use more strict variant and specify the IP address directly using
        ``set_client_ip("192.168.0.3")``. This IP address needs to be one of the IP address of the Spark driver and in
        the same network as the rest of the H2O worker nodes.

The same configuration can be applied when the H2O cluster has been started via multicast discovery.