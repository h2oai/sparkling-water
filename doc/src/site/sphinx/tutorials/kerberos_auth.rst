.. _kerberos_auth:

Enabling Kerberos Authentication
--------------------------------

Sparkling Water can use Kerberos for user authentication. You need to have ``login.conf`` with the content similar to the one below:

.. code:: xml

    krb5loginmodule {
         com.sun.security.auth.module.Krb5LoginModule required
         java.security.krb5.realm="0XDATA.LOC"
         java.security.krb5.kdc="kerberos.0xdata.loc";
    };

This configuration file needs to be modified for your specific Kerberos configuration.

Generally, to enable Kerberos authentication you need to set the following environmental properties:

 - ``spark.ext.h2o.kerberos.login=true``
 - ``spark.ext.h2o.login.conf=kerberos.conf``
 - ``spark.ext.h2o.user.name=username``

where ``kerberos.conf`` is the configuration file for the Kerberos connection and `username` is a username of your Kerberos account
that will be used for authentication to the H2O-3 cluster.

Configuring Kerberos Auth in Scala
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/sparkling-shell \
    --conf spark.ext.h2o.kerberos.login=true \
    --conf spark.ext.h2o.login.conf=kerberos.conf \
    --conf spark.ext.h2o.user.name=username

And later, you can create ``H2OContext`` as:

.. code:: scala

    import ai.h2o.sparkling._
    val conf = new H2OConf().setUserName("username").setPassword("password")
    val hc = H2OContext.getOrCreate(conf)


Or, you can also use setters available on ``H2OConf`` as:

.. code:: scala

    import ai.h2o.sparkling._
    val conf = new H2OConf().setLoginConf("kerberos.conf").setKerberosLoginEnabled().setUserName("username").setPassword("password")
    val hc = H2OContext.getOrCreate(conf)

Later when accessing Flow, you will be asked for the username and password of the user you specified in the configuration
property `spark.ext.h2o.user.name` or via the method `setUserName`.

Configuring Kerberos Auth in Python (PySparkling)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/pysparkling \
    --conf spark.ext.h2o.kerberos.login=true \
    --conf spark.ext.h2o.login.conf=kerberos.conf \
    --conf spark.ext.h2o.user.name=username

And later, you can create ``H2OContext`` as:

.. code:: python

    from pysparkling import *
    conf = H2OConf().setUserName("username").setPassword("password")
    hc = H2OContext.getOrCreate(conf)


Or, you can also use setters available on ``H2OConf`` as:

.. code:: python

    from pysparkling import *
    conf = H2OConf().setLoginConf("kerberos.conf").setKerberosLoginEnabled().setUserName("username").setPassword("password")
    hc = H2OContext.getOrCreate(conf)

You can see that in the case of PySparkling, you need to also specify the username and password as part of the ``H2OContext`` call.
This is required because you want to have the Python client authenticated as well.

Later when accessing Flow, you will be asked for the username and password of the user you specified in the configuration
property `spark.ext.h2o.user.name` or via the method `setUserName`.
