Frequently Asked Questions
--------------------------

-  Where do I find the Spark logs?

    **Standalone mode**: Spark executor logs are located in the
    directory ``$SPARK_HOME/work/app-<AppName>`` (where ``<AppName>`` is
    the name of your application). The location contains also
    stdout/stderr from H2O.

    **YARN mode**: The executors logs are available via
    ``yarn logs -applicationId <appId>`` command. Driver logs are by
    default printed to console, however, H2O also writes logs into
    ``current_dir/h2ologs``.

    The location of H2O driver logs can be controlled via Spark property
    ``spark.ext.h2o.client.log.dir`` (pass via ``--conf``) option.

-  How to display Sparkling Water information in the Spark History
   Server?

    Sparkling Water reports the information already, you just
    need to add the sparkling-water classes on the classpath of the Spark
    history server. > To see how to configure the spark application for
    logging into the History Server, please see `Spark Monitoring
    Configuration <http://spark.apache.org/docs/latest/monitoring.html>`__

-  Spark is very slow during initialization or H2O does not form a
   cluster. What should I do?

    Configure the Spark variable ``SPARK_LOCAL_IP``. For example:

    .. code:: bash

        export SPARK_LOCAL_IP='127.0.0.1'


-  How do I increase the amount of memory assigned to the Spark
   executors in Sparkling Shell?

    Sparkling Shell accepts common Spark Shell arguments. For example,
    to increase the amount of memory allocated by each executor, use the
    ``spark.executor.memory`` parameter:
    ``bin/sparkling-shell --conf "spark.executor.memory=4g"``

-  How do I change the base port H2O uses to find available ports?

    The H2O accepts ``spark.ext.h2o.port.base`` parameter via Spark
    configuration properties:
    ``bin/sparkling-shell --conf "spark.ext.h2o.port.base=13431"``. For
    a complete list of configuration options, refer to `Devel
    Documentation <https://github.com/h2oai/sparkling-water/blob/master/DEVEL.md#sparkling-water-configuration-properties>`__.

-  How do I use Sparkling Shell to launch a Scala ``test.script`` that I
   created?

    Sparkling Shell accepts common Spark Shell arguments. To pass your
    script, please use ``-i`` option of Spark Shell:
    ``bin/sparkling-shell -i test.script``

-  How do I increase PermGen size for Spark driver?

    Specify
    ``--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m"``

-  How do I add Apache Spark classes to Python path?

    Configure the Python path variable ``PYTHONPATH``:

    .. code:: bash

        export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
        export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

-  Trying to import a class from the ``hex`` package in Sparkling Shell
   but getting weird error:
   ``missing arguments for method hex in object functions;   follow this method with '_' if you want to treat it as a partially applied``

    In this case you are probably using Spark 1.5+ which is importing SQL
    functions into Spark Shell environment. Please use the following
    syntax to import a class from the ``hex`` package:

    .. code:: scala

        import _root_.hex.tree.gbm.GBM


-  Trying to run Sparkling Water on HDP Yarn cluster, but getting error: ``java.lang.NoClassDefFoundError: com/sun/jersey/api/client/config/ClientConfig``

    The Yarn time service is not compatible with libraries provided by Spark. Please disable time service via setting
    ``spark.hadoop.yarn.timeline-service.enabled=false``. For more details, please visit
    https://issues.apache.org/jira/browse/SPARK-15343

-  Getting non-deterministic H2O Frames after the Spark Data Frame to
   H2O Frame conversion.

    This is caused by what we think is a bug in Apache Spark. On
    specific kinds of data combined with higher number of partitions we
    can see non-determinism in BroadCastHashJoins. This leads to to
    jumbled rows and columns in the output H2O frame. We recommend to
    disable broadcast based joins which seem to be non-deterministic as:

    .. code:: scala

        sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")

    The issue can be tracked as
    `PUBDEV-3808 <https://0xdata.atlassian.net/browse/PUBDEV-3808>`__.
    On the Spark side, the following issues are related to the problem:
    `Spark-17806 <https://issues.apache.org/jira/browse/SPARK-17806>`__

- How to configure Hive metastore location ?

    Spark SQL context (in fact Hive) requires the use of metastore which stores metadata about Hive tables.
    In order to ensure this works correctly, the ``${SPARK_HOME}/conf/hive-site.xml`` needs to contain the following
    configuration. We provide two examples, how to use MySQL and Derby as the metastore.

    For MySQL, the following configuration needs to be located in the ``${SPARK_HOME}/conf/hive-site.xml`` configuration file:

    .. code:: xml

        <property>
          <name>javax.jdo.option.ConnectionURL</name>
          <value>jdbc:mysql://{mysql_host}:${mysql-port}/{metastore_db}?createDatabaseIfNotExist=true</value>
          <description>JDBC connect string for a JDBC metastore</description>
        </property>

        <property>
          <name>javax.jdo.option.ConnectionDriverName</name>
          <value>com.mysql.jdbc.Driver</value>
          <description>Driver class name for a JDBC metastore</description>
        </property>

        <property>
          <name>javax.jdo.option.ConnectionUserName</name>
          <value>{username}</value>
          <description>username to use against metastore database</description>
        </property>

        <property>
          <name>javax.jdo.option.ConnectionPassword</name>
          <value>{password}</value>
          <description>password to use against metastore database</description>
        </property>


    where:
        - ``{mysql_host}`` and ``{mysql_port}`` are the host and port of the MySQL database.
        - ``{metastore_db}`` is the name of the MySQL database holding all the metastore tables.
        - ``{username}`` and ``{password}`` are the username and password to MySQL database with read and write access to the ``{metastore_db}`` database.

    For Derby, the following configuration needs to be location in the the ``${SPARK_HOME}/conf/hive-site.xml`` configuration file:

    .. code:: xml

        <property>
          <name>javax.jdo.option.ConnectionURL</name>
          <value>jdbc:derby://{file_location}/metastore_db;create=true</value>
          <description>JDBC connect string for a JDBC metastore</description>
        </property>

        <property>
          <name>javax.jdo.option.ConnectionDriverName</name>
          <value>org.apache.derby.jdbc.ClientDriver</value>
          <description>Driver class name for a JDBC metastore</description>
        </property>

    where:
        - ``{file_location}`` is the location to the metastore_db database file.