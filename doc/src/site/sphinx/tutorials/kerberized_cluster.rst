Running Sparkling Water on Kerberized Hadoop Cluster
----------------------------------------------------

Sparkling Water can run on Kerberized Hadoop cluster and also supports Kerberos authentification for clients and Flow access.
This tutorial shows how to configure Sparkling Water to run on kerberized Hadoop cluster.
If you are also interested in using Kerberos authentification, please read :ref:`kerberos_auth`.

Sparkling Water supports the Kerberized cluster in both internal and external backend.

Internal Backend
~~~~~~~~~~~~~~~~

To make Sparkling Water aware of the Kerberized cluster, you can call:

.. code:: shell

    bin/sparkling-shell --conf "spark.yarn.principal=PRINCIPAL" --conf "spark.yarn.keytab=/path/to/keytab"

or you can create the Kerberos ticket in beforehand using ``kinit`` and call just

.. code:: shell

    ./bin/sparkling-shell

In this case, Sparking Water will use the created ticket and we don't need to pass the configuration details.

External Backend
~~~~~~~~~~~~~~~~

In External Backend, we are also starting the H2O cluster on YARN and we need to make sure it is secured as well.

You can start Sparkling Water as:

.. code:: shell

    bin/sparkling-shell --conf "spark.yarn.principal=PRINCIPAL" --conf "spark.yarn.keytab=/path/to/keytab"


In this case, the value of ``spark.yarn.principal`` and ``spark.yarn.keytab`` properties will be also used to set
``spark.ext.h2o.external.kerberos.principal`` and ``spark.ext.h2o.external.kerberos.keytab`` correspondingly. These options
are used to set up Kerberos on H2O external cluster via Sparkling Water.

You can also set the ``spark.ext.h2o.external.kerberos.principal`` and ``spark.ext.h2o.external.kerberos.keytab``
options directly.


The simplest option you can also start Sparkling Water is:

.. code:: shell

    ./bin/sparkling-shell

In this case, we assume that the ticket has been created using ``kinit`` and it will be used for both Spark and external
H2O cluster.


The same configuration is valid also for PySparkling and RSparkling.