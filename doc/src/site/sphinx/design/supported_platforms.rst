.. _supported_platforms:

Supported Platforms
-------------------

Sparkling Water can run on top of Spark in various ways; however, starting Sparkling Water requires different configurations on different environments:

Local
~~~~~

In this case, Sparkling Water runs as a local cluster (Spark master
variable points to one of the values ``local``, ``local[*]`` or additional local modes available at
`Spark Master URLs <https://spark.apache.org/docs/latest/submitting-applications.html#master-urls>`__).

Standalone Spark Cluster
~~~~~~~~~~~~~~~~~~~~~~~~

Spark documentation: `Spark Standalone Mode <http://spark.apache.org/docs/latest/spark-standalone.html>`__

YARN
~~~~

Spark documentation: `Running Spark on YARN <http://spark.apache.org/docs/latest/running-on-yarn.html>`__

When submitting a Sparkling Water application to a CHD or Apache Hadoop cluster, the command to submit may look like:

.. code:: bash

    ./spark-submit --master=yarn --deploy-mode=client --class ai.h2o.sparkling.SparklingWaterDriver
    --driver-memory=8G --num-executors=3 --executor-memory=3G --conf "spark.executor.extraClassPath=-Dhdp.version=current"
    sparkling-water-assembly-SUBST_SW_VERSION-all.jar

When submitting a Sparkling Water application to an HDP Cluster, the command to submit may look like:

.. code:: bash

    ./spark-submit --master=yarn --deploy-mode=client --class ai.h2o.sparkling.SparklingWaterDriver --conf "spark.yarn.am.extraJavaOptions=-Dhdp.version=current"
    --driver-memory=8G --num-executors=3 --executor-memory=3G --conf "spark.executor.extraClassPath=-Dhdp.version=current"
    sparkling-water-assembly-SUBST_SW_VERSION-all.jar

The only difference between the HDP cluster and the CDH and Apache Hadoop clusters is that we need to add ``-Dhdp.version=current`` to both the ``spark.executor.extraClassPath`` and ``spark.yarn.am.extraJavaOptions`` (resp., ``spark.driver.extraJavaOptions``) configuration properties in the HDP case.

Mesos
~~~~~
Spark documentation: `Running Spark on Mesos <http://spark.apache.org/docs/latest/running-on-mesos.html>`__
