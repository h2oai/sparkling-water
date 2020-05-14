Enabling LDAP
-------------

Sparkling Water can use LDAP for user authentication. You need to have ``login.conf`` with the content similar to the one below:

.. code:: xml

    ldaploginmodule {
        ai.h2o.org.eclipse.jetty.plus.jaas.spi.LdapLoginModule required
        debug="true"
        useLdaps="false"
        contextFactory="com.sun.jndi.ldap.LdapCtxFactory"
        hostname="ldap.h2o.ai"
        port="389"
        bindDn="cn=admin,dc=h2o,dc=ai"
        bindPassword="h2o"
        authenticationMethod="simple"
        forceBindingLogin="true"
        userBaseDn="ou=users,dc=h2o,dc=ai";
    };

This configuration file needs to be modified for your specific LDAP configuration.

Generally, to enable LDAP you need to set the following environmental properties:

 - ``spark.ext.h2o.ldap.login=true``
 - ``spark.ext.h2o.login.conf=ldap.conf``
 - ``spark.ext.h2o.user.name=username``

where ``ldap.conf`` is the configuration file for the LDAP connection and `username` is a username of your LDAP account
that will be used for authentication to the H2O-3 cluster.

Configuring LDAP in Scala
~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/sparkling-shell \
    --conf spark.ext.h2o.ldap.login=true \
    --conf spark.ext.h2o.login.conf=ldap.conf \
    --conf spark.ext.h2o.user.name=username

And later, you can create ``H2OContext`` as:

.. code:: scala

    import ai.h2o.sparkling._
    conf = new H2OConf().setUserName("username").setPassword("password")
    val hc = H2OContext.getOrCreate(conf)

Or, you can also use setters available on ``H2OConf`` as:

.. code:: scala

    import ai.h2o.sparkling._
    val conf = new H2OConf().setLoginConf("ldap.conf").setLdapLoginEnabled().setUserName("username").setPassword("password")
    val hc = H2OContext.getOrCreate(conf)

Later when accessing Flow, you will be asked for the username and password of the user you specified in the configuration
property `spark.ext.h2o.user.name` or via the method `setUserName`.

Configuring LDAP in Python (PySparkling)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/pysparkling \
    --conf spark.ext.h2o.ldap.login=true \
    --conf spark.ext.h2o.login.conf=ldap.conf \
    --conf spark.ext.h2o.user.name=username

And later, you can create ``H2OContext`` as:

.. code:: python

    from pysparkling import *
    conf = H2OConf().setUserName("username").setPassword("password")
    hc = H2OContext.getOrCreate(conf)


Or, you can also use setters available on ``H2OConf`` as:

.. code:: python

    from pysparkling import *
    conf = H2OConf().setLoginConf("ldap.conf").setLdapLoginEnabled().setUserName("username").setPassword("password")
    hc = H2OContext.getOrCreate(conf)

You can see that in the case of PySparkling, you need to also specify the username and password as part of the ``H2OContext`` call. This is required because you want to have the Python client authenticated as well.

Later when accessing Flow, you will be asked for the username and password of the user you specified in the configuration
property `spark.ext.h2o.user.name` or via the method `setUserName`.
