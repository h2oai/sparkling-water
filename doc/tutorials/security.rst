Enabling Security
-----------------

Both Spark and H2O support basic node authentication and data
encryption. In H2O's case we encrypt all the data sent between server
nodes and between client and server nodes. This feature does not support
H2O's UDP feature, only data sent via TCP is encrypted.

Currently only encryption based on Java's key pair is supported (more
in-depth explanation can be found in H2O's documentation linked below).

To enable security for Spark methods please check `their
documentation <http://spark.apache.org/docs/latest/security.html>`__.

Security for data exchanged between H2O instances can be enabled
manually by generating all necessary files and distributing them to all
worker nodes as described in `H2O's
documentation <https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/security.rst#ssl-internode-security>`__
and passing the ``spark.ext.h2o.internal_security_conf`` to Spark
submit:

.. code:: shell

    bin/sparkling-shell --conf "spark.ext.h2o.internal_security_conf=ssl.properties"

We also provide utility methods which automatically generate all
necessary files and enable security on all H2O nodes by passing the 
option ``spark.ext.h2o.internal_secure_connections=true`` to Spark job submit:

.. code:: shell

    bin/sparkling-shell --conf "spark.ext.h2o.internal_secure_connections=true"

This can be also achieved in programmatic way in Scala using the utility class
``org.apache.spark.network.Security``:

.. code:: scala

    import org.apache.spark.network.Security
    import org.apache.spark.h2o._
    Security.enableSSL(spark) // generate properties file, key pairs and set appropriate H2O parameters
    val hc = H2OContext.getOrCreate(spark) // start the H2O cloud

Or if you plan on passing your own H2OConf then please use:

.. code:: scala

    import org.apache.spark.network.Security
    import org.apache.spark.h2o._
    val conf: H2OConf = // generate H2OConf file
    Security.enableSSL(spark, conf) // generate properties file, key pairs and set appropriate H2O parameters
    val hc = H2OContext.getOrCreate(spark, conf) // start the H2O cloud

This method generates all files and distributes them via YARN or
Spark methods to all worker nodes. This communication is secure in case of configured YARN/Spark security.
