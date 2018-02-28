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

where ``ldap.conf`` is the configuration file for the LDAP connection.

Configuring LDAP in Scala
~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/sparkling-shell --conf spark.ext.h2o.ldap.login=true --conf spark.ext.h2o.login.conf=ldap.conf

And later, you can create ``H2OContext`` without the configuration object as:

.. code:: scala

    import org.apache.spark.h2o._
    val hc = H2OContext.getOrCreate(spark)


Or, you can also use setters available on ``H2OConf`` as:

.. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setLoginConf("ldap.conf").setLdapLoginEnabled()
    val hc = H2OContext.getOrCreate(spark, conf)

Later when accessing Flow, you will be asked for the username and password of a user available in your LDAP domain.

Configuring LDAP in Python (PySparkling)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/pysparkling --conf spark.ext.h2o.ldap.login=true --conf spark.ext.h2o.login.conf=ldap.conf

And later, you can create ``H2OContext`` without the configuration object as:

.. code:: python

    from pysparkling import *
    hc = H2OContext.getOrCreate(spark, auth=("username", "password"))


Or, you can also use setters available on ``H2OConf`` as:

.. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_login_conf("ldap.conf").set_ldap_login_enabled()
    hc = H2OContext.getOrCreate(spark, conf, auth=("username", "password"))

You can see that in the case of PySparkling, you need to also specify the username and password as part of the ``H2OContext`` call. This is required because you want to have the Python client authenticated as well.

Later when accessing Flow, you will be asked for the username and password of a user available in your LDAP domain.

