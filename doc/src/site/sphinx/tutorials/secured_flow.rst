Using SSL to secure H2O Flow
============================

Sparkling Water supports security of H2O Flow user interface. There are two ways how to secure the Flow.

- Provide the existing Java key store and password.
- Let Sparkling Water automatically create the necessary files. This solution has several limitations
  which are described bellow.

Using existing Java keystore
----------------------------

In order to use https correctly, the following two options need to be specified:

- ``spark.ext.h2o.jks`` - path to Java keystore file
- ``spark.ext.h2o.jks.pass`` - keystore file password


.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        To enable https in Sparkling Water, you can start Sparkling Water as:

        .. code:: shell

            bin/sparkling-shell --conf "spark.ext.h2o.jks=/path/to/keystore" --conf "spark.ext.h2o.jks.pass=password"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: scala

            import org.apache.spark.h2o._
            val hc = H2OContext.getOrCreate(spark)

        You can also start Sparkling shell without the configuration and specify it using the setters on ``H2OConf`` as:

        .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark).setJks("/path/to/keystore").setJksPass("password)
            val hc = H2OContext.getOrCreate(spark, conf)


    .. tab-container:: Python
        :title: Python

        To enable https in Pysparkling, you can start PySparkling as:

        .. code:: shell

            bin/pysparkling --conf "spark.ext.h2o.jks=/path/to/keystore" --conf "spark.ext.h2o.jks.pass=password"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate(spark)

        You can also start PySparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: python

            from pysparkling import *
            conf = H2OConf(spark).set_jks("/path/to/keystore").set_jks_pass("password)
            hc = H2OContext.getOrCreate(spark, conf)

        In case your certificates are self-signed, the connection of the
        Python client to the backend cluster will fail due to the security
        limitations. In this case, you can skip the certificates verification
        using ``verify_ssl_certificates=False`` and connect as:

        .. code:: python

            from pysparkling import *
            conf = H2OConf(spark).set_jks("/path/to/keystore").set_jks_pass("password)
            hc = H2OContext.getOrCreate(spark, conf, verify_ssl_certificates=False)



Generate the files automatically
--------------------------------

Sparkling Water can generate the necessary key store and password automatically. To enable the automatic
generation, the ``spark.ext.h2o.auto.flow.ssl`` option needs to be set to ``true``. In this mode only self-signed
certificates are created.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala


        To enable the security using this mode in Sparkling Water, start Sparkling Shell as:

        .. code:: shell

            bin/sparkling-shell --conf "spark.ext.h2o.auto.flow.ssl=true"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: scala

            import org.apache.spark.h2o._
            val hc = H2OContext.getOrCreate(spark)

        You can also start Sparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: scala

            import org.apache.spark.h2o._
            val conf = new H2OConf(spark).setAutoFlowSslEnabled()
            val hc = H2OContext.getOrCreate(spark, conf)


    .. tab-container:: Python
        :title: Python

        To enable https in Pysparkling using this mode, you can start PySparkling as:

        .. code:: shell

            bin/pysparkling --conf "spark.ext.h2o.auto.flow.ssl=true"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate(spark, verify_ssl_certificates=False)

        You can also start PySparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: python

            from pysparkling import *
            conf = H2OConf(spark).set_auto_flow_ssl_enabled()
            hc = H2OContext.getOrCreate(spark, conf, verify_ssl_certificates=False)
