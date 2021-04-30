.. _tutorials_secured_flow:

Using SSL to secure H2O Flow UI
===============================

Sparkling Water allows user to set https for communication with H2O Flow user interface. The security settings for FLOW UI
are also applied to communication and data exchange between Spark instances (driver + executors) and H2O nodes.

**There are two ways how to secure Flow UI**

- Provide an existing SSL certificate in Java key store to Sparkling Water
- Let Sparkling Water automatically generate SSL certificate. This solution has several limitations
  which are described below.

Using existing Java Keystore
----------------------------

In order to use https correctly, the following two options need to be specified:

- ``spark.ext.h2o.jks`` - A Path to the Java keystore file containing a SSL certificate
- ``spark.ext.h2o.jks.pass`` - A password to the Java keystore file
- ``spark.ext.h2o.jks.alias`` - (Optional) Alias of the SSL certificate if the Java keystore file contains more than one
  certificate.

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

        You can also start the Sparkling shell without the configuration and specify it using the setters on ``H2OConf`` as:

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


In case your certificate is self-signed or signed by an untrusted CA, the connection to the H2O cluster will fail due to
the security limitations. In this case, you can skip the certificates verification as follows:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val conf = new H2OConf().setSslCertificateVerificationInInternalRestConnectionsDisabled()
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            conf = H2OConf()
            conf.setSslCertificateVerificationInInternalRestConnectionsDisabled()
            conf.setVerifySslCertificates(False)
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            conf <- H2OConf()
            conf$setSslCertificateVerificationInInternalRestConnectionsDisabled()
            conf$setVerifySslCertificates(FALSE)
            hc <- H2OContext.getOrCreate(conf)


However, the verification of certificates signed by a custom CA is supported. To achieve that, add the CA certificate
into the Java trust store on all servers of the cluster. If running  PySparkling or Rsparkling,
also set the CA certificate path to the SW property ``spark.ext.h2o.ssl.ca.cert``. The certificate itself must cover
all hostnames for servers where H2O nodes are running (wild card certificate, the list of all servers). If the certificate
covers only the server with Spark driver where H2O Flow UI is running, set the property
``spark.ext.h2o.internal.rest.verify_ssl_hostnames`` to ``false``.


.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val conf = new H2OConf().setSslHostnameVerificationInInternalRestConnectionsDisabled()
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            conf = H2OConf()
            conf.setSslCACert("/path/to/cacert.pem")
            conf.setSslHostnameVerificationInInternalRestConnectionsDisabled()
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            conf <- H2OConf()
            conf$setSslCACert("/path/to/cacert.pem")
            conf$setSslHostnameVerificationInInternalRestConnectionsDisabled()
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

            bin/sparkling-shell --conf "spark.ext.h2o.auto.flow.ssl=true"

        and when you have the shell running, start ``H2OContext`` as:

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        You can also start Sparkling shell without the configuration
        and specify it using the setters on ``H2OConf`` as:

        .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setAutoFlowSslEnabled()
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
