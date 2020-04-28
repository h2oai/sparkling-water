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

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        You can also start Sparkling shell without the configuration and specify it using the setters on ``H2OConf`` as:

        .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setJks("/path/to/keystore").setJksPass("password")
            val hc = H2OContext.getOrCreate(conf)


    .. tab-container:: Python
        :title: Python

        To enable https in PySparkling, you can start PySparkling as:

        .. code:: shell

            bin/pysparkling --conf "spark.ext.h2o.jks=/path/to/keystore" --conf "spark.ext.h2o.jks.pass=password"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        You can also start PySparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setJks("/path/to/keystore").setJksPass("password)
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        To enable https in RSparkling, run in RStudio:

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf <- H2OConf()$setJks("/path/to/keystore")$setJksPass("password")
            hc <- H2OContext.getOrCreate(conf)


In case your certificates are self-signed, the connection to the H2O cluster will fail due to the security
limitations. In this case, you can skip the certificates verification
by calling ``setVerifySslCertificates`` on ``H2OConf`` as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val conf = new H2OConf().setVerifySslCertificates(false)
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            conf = H2OConf().setVerifySslCertificates(False)
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            conf <- H2OConf()$setVerifySslCertificates(FALSE)
            hc <- H2OContext.getOrCreate(conf)

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

            bin/sparkling-shell --conf "spark.ext.h2o.auto.flow.ssl=true" --conf "spark.ext.h2o.verify_ssl_certificates=false"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        You can also start Sparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setAutoFlowSslEnabled().setVerifySslCertificates(false)
            val hc = H2OContext.getOrCreate(conf)


    .. tab-container:: Python
        :title: Python

        To enable https in PySparkling using this mode, you can start PySparkling as:

        .. code:: shell

            bin/pysparkling --conf "spark.ext.h2o.auto.flow.ssl=true"  --conf "spark.ext.h2o.verify_ssl_certificates=false"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        You can also start PySparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setAutoFlowSslEnabled().setVerifySslCertificates(False)
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        To enable https in RSparkling using this mode, run in your RStudio:

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf <- H2OConf()$setAutoFlowSslEnabled()$setVerifySslCertificates(FALSE)
            hc <- H2OContext.getOrCreate(conf)
