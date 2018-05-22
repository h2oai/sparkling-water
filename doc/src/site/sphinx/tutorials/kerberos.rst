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

where ``kerberos.conf`` is the configuration file for the Kerberos connection.

The last option is only needed in cases when your user name differs from your Kerberos user name. The user name in
Sparkling Water needs to be the same as is in the Kerberos to enable the authentication.

Configuring Kerberos Auth in Scala
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/sparkling-shell --conf spark.ext.h2o.kerberos.login=true --conf spark.ext.h2o.login.conf=kerberos.conf

And later, you can create ``H2OContext`` without the configuration object as:

.. code:: scala

    import org.apache.spark.h2o._
    val hc = H2OContext.getOrCreate(spark)


Or, you can also use setters available on ``H2OConf`` as:

.. code:: scala

    import org.apache.spark.h2o._
    val conf = new H2OConf(spark).setLoginConf("kerberos.conf").setKerberosLoginEnabled()
    val hc = H2OContext.getOrCreate(spark, conf)

The method ``setUserName`` can be also used to specify the user name on ``H2OConf`` object.

Later when accessing Flow, you will be asked for the username and password of a user available in your Kerberos database.

Configuring Kerberos Auth in Python (PySparkling)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass the required properties directly as Spark properties, such as:

.. code:: shell

    ./bin/pysparkling --conf spark.ext.h2o.kerberos.login=true --conf spark.ext.h2o.login.conf=kerberos.conf

And later, you can create ``H2OContext`` without the configuration object as:

.. code:: python

    from pysparkling import *
    hc = H2OContext.getOrCreate(spark, auth=("username", "password"))


Or, you can also use setters available on ``H2OConf`` as:

.. code:: python

    from pysparkling import *
    conf = H2OConf(spark).set_login_conf("kerberos.conf").set_kerberos_login_enabled()
    hc = H2OContext.getOrCreate(spark, conf, auth=("username", "password"))

The method ``set_user_name`` can be also used to specify the user name on ``H2OConf`` object.

You can see that in the case of PySparkling, you need to also specify the username and password as part of the ``H2OContext`` call. This is required because you want to have the Python client authenticated as well.

Later when accessing Flow, you will be asked for the username and password of a user available in your Kerberos database.

