Hive Support in Sparkling Water
===============================

Spark supports reading data natively from Hive and H2O supports that in a Hadoop environment as well.
In Sparkling Water you can decide which tool you want to use for this task. This tutorial explains what is needed
to use H2O to read data from Hive in the Sparkling Water environment.

Import Data from Hive via Hive Metastore
----------------------------------------

- Make sure ``$SPARK_HOME/conf`` contains the hive-site.xml with your Hive configuration.
- In YARN client mode or any local mode, please copy the required connector jars for your Metastore to ``$SPARK_HOME/jars``.
  You can find these jars in ``$HIVE_HOME/lib directory``. For example, if you are using MySQL as a Metastore for Hive,
  copy MySQL metastore JDBC connector. This is not required in the YARN cluster mode.

This is all preparation we need to do. The following code shows how to import the table.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        To read data from Hive in Sparkling Water, you can use the method:

        .. code:: Scala

            val airlinesTable = h2oContext.importHiveTable("default", "airlines")

    .. tab-container:: Python
        :title: Python

        To read data from Hive in PySparkling, you can use the method:

        .. code:: python

            airlines_frame = h2oContext.importHiveTable("default", "airlines")

    .. tab-container:: R
        :title: R

        To read data from Hive in RSparkling, you can use the method:

        .. code:: R

            airlines_frame <- h2oContext$importHiveTable("default", "airlines")

This call reads the airlines table from the default database.

Import Data from Hive via JDBC connection
-----------------------------------------
This feature reads data from Hive via a standard JDBC connection.

Obtain the Hive JDBC Client JAR
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To be able to connect to Hive, Sparkling Water will need Hive JDBC Client JAR on the class-path. The jar can be obtained
from in the following ways.

- For Hortonworks, Hive JDBC client jars can be found at
  ``/usr/hdp/current/hive-client/lib/hive-jdbc-<version>-standalone.jar`` on one of the edge nodes after you have
  installed HDP. For more information, see Hortenworks documentation
  https://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.6.4/bk_data-access/content/hive-jdbc-odbc-drivers.html.
- For Cloudera, install the JDBC package for your operating system according to instructions stated on Cloudera documentation
  https://www.cloudera.com/documentation/enterprise/5-3-x/topics/cdh_ig_hive_jdbc_install.html.
  The path to the jar then will be ``/usr/lib/hive/lib/hive-jdbc-<version>-standalone.jar``
- You can also retrieve this from Maven for the desired version using
  ``mvn dependency:get -Dartifact=groupId:artifactId:version``.

Import Data from a non-Kerberized Hive
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water can import data from the non-Kerberized hive. This also applies for the case when
your Hadoop cluster is Kerberized but Hive is not.

To import data from non-Kerberized Hive, run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, start Sparkling Shell with the  Hive JDBC client JAR on the class-path

        .. code:: bash

            ./bin/sparkling-shell --jars /path/to/hive-jdbc-<version>-standalone.jar

        Create ``H2OContext`` with properties ensuring connectivity to Hive

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        Import data table from Hive

        .. code:: scala

            val frame = hc.importHiveTable("jdbc:hive2://hostname:10000/default", "airlines")

    .. tab-container:: Python
        :title: Python

        First, start PySparkling Shell with the  Hive JDBC client JAR on the class-path

        .. code:: bash

            ./bin/pysparkling --jars /path/to/hive-jdbc-<version>-standalone.jar

        Create ``H2OContext`` with properties ensuring connectivity to Hive

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        Import data table from Hive

        .. code:: python

            frame = hc.importHiveTable("jdbc:hive2://hostname:10000/default", "airlines")

    .. tab-container:: R
        :title: R

        Run your R environment and install required libraries according to :ref:`rsparkling` tutorial and then create
        Spark context with the Hive JDBC client JAR on the class-path.

        .. code:: R

            library(sparklyr)
            library(rsparkling)
            conf <- spark_config()
            conf$sparklyr.jars.default <- "/path/to/hive-jdbc-<version>-standalone.jar"
            sc <- spark_connect(master = "yarn-client", config = conf)
            hc <- H2OContext.getOrCreate()

        Import data table from Hive

        .. code:: R

            frame <- hc$importHiveTable("jdbc:hive2://hostname:10000/default", "airlines")


Import Data from Kerberized Hive in a Kerberized Hadoop Cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before a given connection to Hive is made, a user has to be authenticated with the Hive instance
via a delegation token and pass the delegation token to Sparkling Water.
Sparkling Water ensures that the delegation token is being automatically refreshed, thus delegation token never expires
in long-running Sparkling Water applications.

First, we need to generate the initial token, which can be generated with the following steps.

Authenticate your user against Kerberos.

.. code:: bash

    kinit <your_user_name>

Put Hive JDBC client JAR on the Hadoop class-path.

.. code:: bash

    export HADOOP_CLASSPATH=/path/to/hive-jdbc-<version>-standalone.jar

Set path to sparkling-water-assembly-SUBST_SW_VERSION-all.jar which is bundled in Sparkling Water archive.

.. code:: bash

    SW_ASSEMBLY=/path/to/sparkling-water-SUBST_SW_VERSION/jars/sparkling-water-assembly_SUBST_SCALA_BASE_VERSION-SUBST_SW_VERSION-all.jar

Get the delegation token generated with arguments:
    - ``hiveHost`` - The full address of HiveServer2, for example ``hostname:10000``
    - ``hivePrincipal`` - Hiveserver2 Kerberos principal, for example ``hive/hostname@DOMAIN.COM``
    - ``tokenFile`` - The output file which the delegation token will be generated to

.. code:: bash

    hadoop jar $SW_ASSEMBLY water.hive.GenerateHiveToken -hiveHost <your_hive_host> -hivePrincipal <your_hive_principal> -tokenFile hive.token

With the token generated, we can run Sparkling Water with Hive support for the Kerberized
Hadoop cluster as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, start Sparkling Shell with the  Hive JDBC client JAR on the class-path

        .. code:: bash

            ./bin/sparkling-shell --jars /path/to/hive-jdbc-<version>-standalone.jar

        Create ``H2OContext`` with properties ensuring connectivity to Hive

        .. code:: scala

            import ai.h2o.sparkling._
            val conf = new H2OConf()
            conf.setKerberizedHiveEnabled()
            conf.setHiveHost("hostname:10000") // The full address of HiveServer2
            conf.setHivePrincipal("hive/hostname@DOMAIN.COM") // Hiveserver2 Kerberos principal
            conf.setHiveJdbcUrlPattern("jdbc:hive2://{{host}}/;{{auth}}") // Doesn't have to be specified if host is set
            val source = scala.io.Source.fromFile("hive.token")
            try {
                conf.setHiveToken(source.mkString())
            } finally {
                source.close()
            }
            val hc = H2OContext.getOrCreate(conf)

        Import data table from Hive

        .. code:: scala

            val frame = hc.importHiveTable("jdbc:hive2://hostname:10000/default;auth=delegationToken", "airlines")

    .. tab-container:: Python
        :title: Python

        First, start PySparkling Shell with the  Hive JDBC client JAR on the class-path

        .. code:: bash

            ./bin/pysparkling --jars /path/to/hive-jdbc-<version>-standalone.jar

        Create ``H2OContext`` with properties ensuring connectivity to Hive

        .. code:: python

            from pysparkling import *
            conf = H2OConf()
            conf.setKerberizedHiveEnabled()
            conf.setHiveHost("hostname:10000") # The full address of HiveServer2
            conf.setHivePrincipal("hive/hostname@DOMAIN.COM") # Hiveserver2 Kerberos principal
            conf.setHiveJdbcUrlPattern("jdbc:hive2://{{host}}/;{{auth}}") # Doesn't have to be specified if host is set
            with open('hive.token', 'r') as tokenFile:
                token = tokenFile.read()
                conf.setHiveToken(token)
            hc = H2OContext.getOrCreate(conf)

        Import data table from Hive

        .. code:: python

            frame = hc.importHiveTable("jdbc:hive2://hostname:10000/default;auth=delegationToken", "airlines")

    .. tab-container:: R
        :title: R

        Run your R environment and install required libraries according to :ref:`rsparkling` tutorial and then create
        Spark context with the Hive JDBC client JAR on the class-path.

        .. code:: R

            library(sparklyr)
            library(rsparkling)
            conf <- spark_config()
            conf$sparklyr.jars.default <- "/path/to/hive-jdbc-<version>-standalone.jar"
            sc <- spark_connect(master = "yarn-client", config = conf)

        Create ``H2OContext`` with properties ensuring connectivity to Hive

        .. code:: R

            h2oConf <- H2OConf()
            conf.setKerberizedHiveEnabled()
            h2oConf$setHiveHost("hostname:10000")
            h2oConf$setHivePrincipal("hive/hostname@DOMAIN.COM")
            tokenFile <- 'hive.token'
            token <- readChar(tokenFile, file.info(tokenFile)$size)
            h2oConf$setHiveToken(token)
            hc <- H2OContext.getOrCreate(h2oConf)

        Import data table from Hive

        .. code:: R

            frame <- hc$importHiveTable("jdbc:hive2://hostname:10000/default;auth=delegationToken", "airlines")
