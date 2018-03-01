.. _backend:

Sparkling Water Backends
------------------------

Internal Backend
~~~~~~~~~~~~~~~~

In the internal backend, an H2O cloud is created automatically during the ``H2OContext.getOrCreate`` call. Because it's not technically possible to get the number of executors in Spark, we try to discover all executors at the initiation of ``H2OContext``, and we start the H2O instance inside of each discovered executor. This solution is easiest to deploy; however when Spark or YARN kills the executor - which is not an unusual case - the entire H2O cluster goes down because H2O does not support high availability.

The internal backend is the default for behavior for Sparkling Water. It can be changed via the Spark configuration property ``spark.ext.h2o.backend.cluster.mode`` and specifying either ``external`` or ``internal``. Another way to change type of backend is by calling the ``setExternalClusterMode()`` or ``setInternalClusterMode()`` method on the ``H2OConf`` class. ``H2OConf`` is simple wrapper around ``SparkConf`` and inherits all properties in the Spark configuration.

Here we show a few examples showing how H2OContext can be started with the internal backend.

Explicitly specify internal backend on ``H2OConf``:

 .. code:: scala

    val conf = new H2OConf(spark).setInternalClusterMode()
    val h2oContext = H2OContext.getOrCreate(spark, conf)

If ``spark.ext.h2o.backend.cluster.mode`` property was set to ``internal`` either on the command line or on the ``SparkConf`` class, then we can call:

 .. code:: scala

    val h2oContext = H2OContext.getOrCreate(spark) 

or

 .. code:: scala

    val conf = new H2OConf(spark)
    val h2oContext = H2OContext.getOrCreate(spark, conf)

External Backend
~~~~~~~~~~~~~~~~

In the external cluster, we use the H2O cluster running separately from the rest of the Spark application. This separation gives us more stability because we are no longer affected by Spark executors being killed, which can lead (as in the previous mode) to h2o cloud kill as well.

There are two deployment strategies of the external cluster: manual and automatic. In manual mode, we need to start the H2O cluster, and in automatic mode, the cluster is started for us automatically based on our configuration. In both modes, we can't use the regular H2O/H2O driver jar as the main artifact for the external H2O cluster. We need to extend it by classes required by Sparkling Water. Users are expected to extend the H2O/H2O driver jar and build the artifacts on their own using a few simple steps mentioned below.

Obtaining Extended H2O Jar
~~~~~~~~~~~~~~~~~~~~~~~~~~

For the released Sparkling Water versions, the extended H2O jar can be downloaded using our helper script. After you download and unpack the Sparkling Water distribution package, you can use the ``./bin/get-extendend-h2o.sh`` script to download the extended H2O jar. This script expects a single argument that specifies the Hadoop version for which you obtained the jar.

The following code downloads H2O extended JAR for the cdh5.8:

 .. code:: bash

    ./bin/get-extended-h2o.sh cdh5.8

If you don't want to run on Hadoop and instead want to run H2O in standalone mode, you can get the corresponding extended H2O standalone jar as:

 .. code:: bash

    ./bin/get-extended-h2o.sh standalone

If you want to see a list of supported Hadoop versions, just run the shell script without any arguments as:

 .. code:: bash

    ./bin/get-extended-h2o.sh

The script downloads the jar to the current directory and prints the absolute path to the downloaded jar.

The sections that follow explain how to use the external cluster in both modes. Let's assume for later sections that the path to the extended H2O/H2O driver jar file is available in the ``H2O_EXTENDED_JAR`` environmental variable.

**Note**: If you want to get an extended H2O jar for Sparkling Water and H2O versions that have not yet been released, you need to extend the JAR manually. This is explained in the following tutorial: :ref:`extend_jar_manually`.

Manual Mode of External Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We need to start the H2O cluster before connecting to it manually. An H2O cluster can be started on Hadoop or in standalone H2O mode.

Running External Backend on Hadoop
##################################

This assumes that ``$H2O_EXTENDED_JAR`` contains the extended h2o driver (with support for particular Hadoop version).

To start H2O cluster on Hadoop, please run:

.. code:: bash

    hadoop -jar $H2O_EXTENDED -jobname test -nodes 3 -mapperXmx 6g

After this step, we should have an H2O cluster with three nodes running on Hadoop. (Internally, the nodes discovered each other using the multicast discovery.)

To connect to this external cluster, run the following commands in the corresponding shell (Sparkling in case of Scala; PySparkling in case of Python):

Scala:

 .. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setExternalClusterMode().useManualClusterStart().setH2OCluster("representant_ip", representant_port).setCloudName("test”)
    val hc = H2OContext.getOrCreate(spark, conf)

Python:

 .. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_external_cluster_mode().use_manual_cluster_start().set_h2o_cluster("representant_ip", representant_port).set_cloud_name("test”)
    hc = H2OContext.getOrCreate(spark, conf)


Running External Backend in Standalone H2O Mode
###############################################

This assumes that ``$H2O_EXTENDED_JAR`` contains the standalone h2o extended jar (no Hadoop support):

In general, an H2O cluster can be started in standalone mode in two ways: using the multicast discovery of the other nodes, or using the flatfile where we manually specify the future locations of H2O nodes.
We recommend using the flatfile to specify the location of nodes for production usage of Sparkling Water, but in simple environments where multicast is supported, the multicast discovery should work as well.

Let's have a look on how to start the H2O cluster and connect to it from Sparkling Water in a multicast environment. To start an H2O cluster with 3 nodes, run the following line three times:

.. code:: bash

    java -jar $H2O_EXTENDED_JAR  -name test

After this step, we should have an H2O cluster with three nodes running, and the nodes should have discovered each other using the multicast discovery.

Now, let's start Sparkling shell first as ``./bin/sparkling-shell`` and connect to the cluster:

.. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setExternalClusterMode().useManualClusterStart().setCloudName("test”)
    val hc = H2OContext.getOrCreate(spark, conf)

To connect to an existing H2O cluster from Python, start PySparkling shell as ``./bin/pysparkling`` and run:

.. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_external_cluster_mode().use_manual_cluster_start().set_cloud_name("test")
    hc = H2OContext.getOrCreate(spark, conf)

To start an external H2O cluster where the nodes are discovered using the flatfile, you can run:

.. code:: bash

    java -jar $H2O_EXTENDED_JAR -name test -flatfile path_to_flatfile

where the flatfile should contain lines in the format of ip:port of the nodes where H2O is supposed to run. To read more about flatfile and its format, please see `H2O's flatfile configuration property <https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/H2O-DevCmdLine.md#flatfile>`__.

To connect to this external cluster, run the following commands in the corresponding shell (Sparkling in case of Scala; PySparkling in case of Python):

Scala:

 .. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setExternalClusterMode().useManualClusterStart().setH2OCluster("representant_ip", representant_port).setCloudName("test”)
    val hc = H2OContext.getOrCreate(spark, conf)

Python:

 .. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_external_cluster_mode().use_manual_cluster_start().set_h2o_cluster("representant_ip", representant_port).set_cloud_name("test”)
    hc = H2OContext.getOrCreate(spark, conf)


Specifying the Client IP
########################

In case we are running H2O on Hadoop or using standalone H2O with a flatfile, we need to use an extra call ``setH2OCluster`` in Scala and ``set_h2o_cluster`` in Python. When the external cluster is started via the flatfile approach, we need to give Sparkling Water the IP address and port of an arbitrary node inside the H2O cloud in order to connect to the cluster. The IP and port of this node are passed as arguments to the ``setH2OCluster/set_h2o_cluster`` method.

It's possible in both cases that the node on which want to start Sparkling shell is connected to more networks. In this case, it can happen that the H2O cloud decides to use addresses from network A while Spark decides to use addresses for its executors and driver from network B. Later, when we start ``H2OContext``, the special H2O client running inside of the Spark Driver can get the same IP address as the Spark driver, and, thus, the rest of the H2O cloud can't see it. This shouldn't happen in environments where the nodes are connected to only one network; however we provide a configuration for how to deal with this case as well.

We can use the ``setClientIp`` method in Scala and the ``set_client_ip`` function in Python, available on ``H2OConf``, which expects an IP address and sets this IP address for the H2O client running inside the Spark driver. The IP address passed to this method should be the address of the node where the Spark driver is about to run and should be from the same network as the rest of H2O cloud.

Let's assume we have two H2O nodes on addresses 192.168.0.1 and 192.168.0.2. Let's also assume that the Spark driver is available on 172.16.1.1, and the only executor is available on 172.16.1.2. The node with the Spark driver is also connected to the 192.168.0.x network with address 192.168.0.3.

In this case there is a chance that the H2O client will use the address from 172.16.x.x network instead of the 192.168.0.x one, which can lead to the problem that the H2O cloud and H2O client can't see each other.

We can force the client to use the correct address using the following configuration:

Scala:

 .. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setExternalClusterMode().useManualClusterStart().setH2OCluster("representant_ip", representant_port).setClientIp("192.168.0.3").setCloudName("test”)
    val hc = H2OContext.getOrCreate(spark, conf)

Python:

 .. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_external_cluster_mode().use_manual_cluster_start().set_h2o_cluster("representant_ip", representant_port).set_client_ip("192.168.0.3").set_cloud_name("test”)
    hc = H2OContext.getOrCreate(spark, conf)

There is also a less strict configuration ``setClientNetworkMask`` in Scala and ``set_client_network_mask`` in Python. Instead of its IP address equivalent, using this method, we can force the H2O client to use just a specific network, and then the client determines which IP address from this network to use.

The same configuration can be applied when the H2O cluster has been started via multicast discovery.

Automatic Mode of External Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In automatic mode, the H2O cluster is started automatically. The cluster can be started automatically only in a YARN environment at the moment. We recommend this approach, as it is easier to deploy external clusters in this mode and it is also more suitable for production environments. When the H2O cluster is started on YARN, it is started as a map reduce job, and it always uses the flatfile approach for nodes to cloud up.

For this case to work, we need to extend the H2O driver for the desired Hadoop version as mentioned above. Let's assume the path to this extended H2O driver is stored in the ``H2O_EXTENDED_JAR`` environmental property.

To start an H2O cluster and connect to it from Spark application in Scala:

 .. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setExternalClusterMode().useAutoClusterStart().setH2ODriverPath("path_to_extended_driver").setNumOfExternalH2ONodes(1).setMapperXmx("2G").setYARNQueue("abc")
    val hc = H2OContext.getOrCreate(spark, conf)

and in Python:

 .. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_external_cluster_mode().use_auto_cluster_start().set_h2o_driver_path("path_to_extended_driver").set_num_of_external_h2o_nodes(1).set_mapper_xmx("2G”).set_yarn_queue(“abc”)`
    hc = H2OContext.getOrCreate(spark, conf)

In both cases, we can see various configuration methods. We explain only the Scala ones because the Python equivalents are doing exactly the same.

-  ``setH2ODriverPath``: Tells Sparkling Water where it can find the extended H2O driver jar. This jar is passed to Hadoop and is used to start H2O cluster on YARN.
-  ``setNumOfExternalH2ONodes``: Specifies the number of H2O nodes we want to start.
-  ``setMapperXmx``: Specifies the amount of memory each H2O node should have available.
-  ``setYarnQueue``: Specifies the YARN queue on which the H2O cluster will be started. We highly recommend that this queue has YARN preemption off in order to have stable a H2O cluster.

When using ``useAutoClusterStart``, we do not need to call ``setH2ODriverPath`` explicitly when the ``H2O_EXTENDED_JAR`` environmental property is set and pointing to that file. In this case Sparkling Water will fetch the path from this variable automatically. Also when ``setCloudName`` is not called, the name is set automatically, and the H2O cluster with that name is started.

It can also happen that we might need to use the ``setClientIp/set_client_ip`` method as mentioned in the section above for the same reasons. The usage of this method in automatic mode is exactly the same as in the manual mode.
