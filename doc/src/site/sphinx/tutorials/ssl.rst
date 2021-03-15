Enabling SSL
------------

This section describes how to secure communication and data exchange among nodes of H2O-3 cluster deployed with Sparkling water.

To secure communication between Spark instances (executors + driver) and H2O nodes, enable SSL on H2O FLOW UI (see :ref:`tutorials_secured_flow`).

To configure security for logic writen in Spark API, please see `Spark Security documentation <http://spark.apache.org/docs/latest/security.html>`__.

Security of communication and data exchange among H2O-3 nodes via files containing secrets
(see the `H2O-3 documentation <http://docs.h2o.ai/h2o/latest-stable/h2o-docs/security.html#ssl-internode-security>`__ for more details).

Sparkling Water allows the user to use the manually created security files or it can generate it automatically.

Using Automatically Generated Security Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To automatically generate and apply the security configuration, please set ``spark.ext.h2o.internal_secure_connections=true`` option to the Spark submit:

.. code:: shell

    bin/sparkling-shell --conf "spark.ext.h2o.internal_secure_connections=true"

This can be also achieved using a programmatic way on the ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setInternalSecureConnectionsEnabled()
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setInternalSecureConnectionsEnabled()
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf = H2OConf()$setInternalSecureConnectionsEnabled()
            hc = H2OContext.getOrCreate(conf)

This method generates all files and distributes them via YARN or Spark methods to all worker nodes. This
communication is secure in the case of configured YARN/Spark security.


Using Manually Generated Security Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use manually generated security files, please pass the following configuration to your Spark Submit:

.. code:: shell

    bin/sparkling-shell --conf "spark.ext.h2o.internal_security_conf=ssl.properties"

This can be also achieved using a programmatic way on the ``H2OConf``:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setSslConf("/path/to/ssl/configuration")
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setSslConf("/path/to/ssl/configuration")
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf = H2OConf()$setSslConf("/path/to/ssl/configuration")
            hc = H2OContext.getOrCreate(conf)

Format of the security configuration is explained at
`H2O-3 documentation <http://docs.h2o.ai/h2o/latest-stable/h2o-docs/security.html#ssl-internode-security>`__.
