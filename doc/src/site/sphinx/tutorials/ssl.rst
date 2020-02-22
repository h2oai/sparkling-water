Enabling SSL
------------

Both Spark and H2O support basic node authentication and data encryption. In H2O's case, we encrypt all the data
sent between server nodes and between client and server nodes.

Currently only encryption based on Java's key pair is supported (more in-depth explanation can be found in H2O's documentation linked below).

To enable security for Spark methods, please review their `Spark Security documentation <http://spark.apache.org/docs/latest/security.html>`__.

Security for data exchanged between H2O instances can be enabled by generating all necessary files and distributing
them to all worker nodes (as described in the `H2O-3 documentation <http://docs.h2o.ai/h2o/latest-stable/h2o-docs/security.html#ssl-internode-security>`__).
Sparkling Water allows the user to use the manually created security files or it can generate it automatically.

Using Automatically Generated Security Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To automatically generate and apply the security configuration. please set ``spark.ext.h2o.internal_secure_connections=true`` option to the Spark submit:

.. code:: shell

    bin/sparkling-shell --conf "spark.ext.h2o.internal_secure_connections=true"

This can be also achieved in programmatic way on the ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark).setInternalSecureConnectionsEnabled()
            val hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf(spark).setInternalSecureConnectionsEnabled()
            hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf = H2OConf(spark)$setInternalSecureConnectionsEnabled()
            hc = H2OContext.getOrCreate(spark, conf)

This method generates all files and distributes them via YARN or Spark methods to all worker nodes. This
communication is secure in the case of configured YARN/Spark security.


Using Manually Generated Security Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use manually generated security files, please pass the following configuration to your Spark Submit:

.. code:: shell

    bin/sparkling-shell --conf "spark.ext.h2o.internal_security_conf=ssl.properties"

This can be also achieved in programmatic way on the ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark).setSslConf("/path/to/ssl/configuration")
            val hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf(spark).setSslConf("/path/to/ssl/configuration")
            hc = H2OContext.getOrCreate(spark, conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf = H2OConf(spark)$setSslConf("/path/to/ssl/configuration")
            hc = H2OContext.getOrCreate(spark, conf)

Format of the security configuration is explained at
`H2O-3 documentation <http://docs.h2o.ai/h2o/latest-stable/h2o-docs/security.html#ssl-internode-security>`__.