.. _proxy_only_auth:

Proxy Only Authentication
-------------------------

In various deployment scenarios - for example when using Sparkling Water on IBM Spectrum Conductor,
it might be required to make the H2O-3 interface and Flow UI accessible only to the cluster owner, without specifying the password in the configuration beforehand.
For those cases we expose a special parameter: ``spark.ext.h2o.proxy.login.only`` that should be used together with the "standard" authentication method property like ``spark.ext.h2o.ldap.login``.
With "Proxy only" mode Sparkling Water will communicate with H2O-3 cluster using internally generated hash-based credentials, and use the requested authentication method only for authenticating the user trying to access the proxy port (Flow UI).

For the examples below, we'll use LDAP authentication method, described in more detail here: :ref:`ldap`

Configuring "Proxy Only" mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable LDAP with "Proxy only authentication", the following properties need to be set:

 - ``spark.ext.h2o.ldap.login=true``
 - ``spark.ext.h2o.proxy.login.only=true``
 - ``spark.ext.h2o.login.conf=ldap.conf``
 - ``spark.ext.h2o.user.name=username``

where ``ldap.conf`` is the configuration file for the LDAP connection and `username` is a username of the LDAP account
that will be used for authentication to the H2O-3 cluster.

For example those required properties can be set directly as Spark properties, such as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: shell

            ./bin/sparkling-shell \
            --conf spark.ext.h2o.ldap.login=true \
            --conf spark.ext.h2o.proxy.login.only=true \
            --conf spark.ext.h2o.login.conf=ldap.conf \
            --conf spark.ext.h2o.user.name=username

    .. tab-container:: Python
        :title: Python

        .. code:: shell

            ./bin/pysparkling \
            --conf spark.ext.h2o.ldap.login=true \
            --conf spark.ext.h2o.proxy.login.only=true \
            --conf spark.ext.h2o.login.conf=ldap.conf \
            --conf spark.ext.h2o.user.name=username

There are also ``H2OConf`` setter methods available:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setLoginConf("ldap.conf").setLdapLoginEnabled().setProxyLoginOnlyEnabled().setUserName("username")
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setLoginConf("ldap.conf").setLdapLoginEnabled().setProxyLoginOnlyEnabled().setUserName("username").setPassword("password")
            hc = H2OContext.getOrCreate(conf)


Later when accessing Flow, the user will be asked for the username and password of the user specified in the configuration
property `spark.ext.h2o.user.name`.
