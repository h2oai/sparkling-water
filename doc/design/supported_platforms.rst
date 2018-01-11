Supported platforms
-------------------

Sparkling Water can run on top of Spark in the various ways, however
starting Sparkling Water requires different configuration on different
environments:

Local
~~~~~

In this case Sparkling Water runs as a local cluster (Spark master
variable points to one of the values ``local``, ``local[*]`` or additional local modes available at
`Spark Master URLs <https://spark.apache.org/docs/latest/submitting-applications.html#master-urls>`__).

Standalone Spark Cluster
~~~~~~~~~~~~~~~~~~~~~~~~

`Spark documentation - running Standalone
cluster <http://spark.apache.org/docs/latest/spark-standalone.html>`__

YARN
~~~~

`Spark documentation - running Spark Application on
YARN <http://spark.apache.org/docs/latest/running-on-yarn.html>`__

When submitting Sparkling Water application to CHD or Apache Hadoop
cluster, the command to submit may look like:

.. code:: bash

    ./spark-submit --master=yarn-client --class water.SparklingWaterDriver --conf "spark.yarn.am.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=current"
    --driver-memory=8G --num-executors=3 --executor-memory=3G --conf "spark.executor.extraClassPath=-XX:MaxPermSize=384m -Dhdp.version=current"
    sparkling-water-assembly-2.1.9-all.jar

When submitting sparkling water application to HDP Cluster, the command
to submit may look like:

.. code:: bash

    ./spark-submit --master=yarn-client --class water.SparklingWaterDriver --conf "spark.yarn.am.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=current"
    --driver-memory=8G --num-executors=3 --executor-memory=3G --conf "spark.executor.extraClassPath=-XX:MaxPermSize=384m -Dhdp.version=current"
    sparkling-water-assembly-2.1.9-all.jar

Apart from the typical spark configuration it is necessary to add
``-XX:MaxPermSize=384m`` (or higher, but 384m is minimum) to both
``spark.executor.extraClassPath`` and ``spark.yarn.am.extraJavaOptions``
(or for client mode, ``spark.driver.extraJavaOptions`` for cluster mode)
configuration properties in order to run Sparkling Water correctly.

The only difference between HDP cluster and both CDH and Apache hadoop
clusters is that we need to add ``-Dhdp.version=current`` to both
``spark.executor.extraClassPath`` and ``spark.yarn.am.extraJavaOptions``
(resp., ``spark.driver.extraJavaOptions``) configuration properties in
the HDP case.

Mesos
~~~~~

`Spark documentation - running Spark Application on
Mesos <http://spark.apache.org/docs/latest/running-on-mesos.html>`__
