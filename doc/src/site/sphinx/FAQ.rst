Frequently Asked Questions
--------------------------

-  Where do I find the Spark logs?

    **Standalone mode**: Spark executor logs are located in the ``$SPARK_HOME/work/app-<AppName>`` directory (where ``<AppName>`` is the name of your application). The location also contains stdout/stderr from H2O.

    **YARN mode**: The executors logs are available via the ``yarn logs -applicationId <appId>`` command. Driver logs are by default printed to console; however, H2O also writes logs into ``current_dir/h2ologs``.

    The location of H2O driver logs can be controlled via the Spark property ``spark.ext.h2o.log.dir`` (passed via ``--conf``).

-  How can I display Sparkling Water information in the Spark History Server?

    Sparkling Water reports the information already. You just need to add the sparkling-water classes on the classpath of the Spark history server. To see how to configure the Spark application for logging into the History Server, please see the Spark documentation for `Monitoring and Instrumentation <http://spark.apache.org/docs/latest/monitoring.html>`__

-  Spark is very slow during initialization or H2O does not form a cluster. What should I do?

    Configure the Spark variable ``SPARK_LOCAL_IP``. For example:

    .. code:: bash

        export SPARK_LOCAL_IP='127.0.0.1'


-  How do I increase the amount of memory assigned to the Spark executors in Sparkling Shell?

    Sparkling Shell accepts common Spark Shell arguments. For example, to increase the amount of memory allocated by each executor, use the ``spark.executor.memory`` parameter: 

    .. code::

       bin/sparkling-shell --conf "spark.executor.memory=4g"

-  How do I change the base port that H2O uses to find available ports?

    H2O accepts the ``spark.ext.h2o.port.base`` parameter via Spark configuration properties: ``bin/sparkling-shell --conf "spark.ext.h2o.port.base=13431"``. For a complete list of configuration options, refer to :ref:`sw_config_properties`.

-  How do I use Sparkling Shell to launch a Scala ``test.script`` that I created?

    Sparkling Shell accepts common Spark Shell arguments. To pass your script, use ``-i`` option of the Spark Shell: 

    .. code::

       bin/sparkling-shell -i test.script

-  How do I increase PermGen size for Spark driver?

    Specify ``--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m"``

-  How do I add Apache Spark classes to the Python path?

    Configure the Python path variable ``PYTHONPATH``:

    .. code:: bash

        export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
        export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

-  While importing a class from the ``hex`` package in Sparkling Shell, I'm getting the following message:
   ``missing arguments for method hex in object functions; follow this method with '_'`` ``if you want to treat it as a partially applied``

    In this case you are probably using Spark 1.5+, which is importing SQL functions into the Spark Shell environment. Please use the following syntax to import a class from the ``hex`` package:

    .. code:: scala

        import _root_.hex.tree.gbm.GBM


-  While attempting to run Sparkling Water on HDP YARN cluster, I'm getting an error: ``java.lang.NoClassDefFoundError: com/sun/jersey/api/client/config/ClientConfig``

    The YARN time service is not compatible with libraries provided by Spark. Please disable time service by setting ``spark.hadoop.yarn.timeline-service.enabled=false``. For more details, please visit  https://issues.apache.org/jira/browse/SPARK-15343.

- How can I configure the Hive metastore location?

    Spark SQL context (in fact Hive) requires the use of metastore, which stores metadata about Hive tables. In order to ensure this works correctly, the ``${SPARK_HOME}/conf/hive-site.xml`` needs to contain the following configuration. We provide two examples, how to use MySQL and Derby as the metastore.

    For MySQL, the following configuration needs to be located in the ``${SPARK_HOME}/conf/hive-site.xml`` configuration file: 

    .. code:: xml

        <property>
          <name>javax.jdo.option.ConnectionURL</name>
          <value>jdbc:mysql://{mysql_host}:${mysql_port}/{metastore_db}?createDatabaseIfNotExist=true</value>
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

    For Derby, the following configuration needs to be in the the ``${SPARK_HOME}/conf/hive-site.xml`` configuration file:

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

- After conversion of Spark Data Frame to H2O Frame, I see only 100 columns on the console.

    If your Spark Data Frame has more than 100 columns, we don't treat it any different. We always fully convert
    the Spark Data Frame to H2O Frame. We just limit the number of columns we send to the client as it's hard to read that
    many columns in the console plus it optimizes the amount of data we transfer betweeen the client and backend.
    If you wish to configure how many columns are sent to the client, you can specify it as part of the conversion method as:

    .. code:: python

        h2o_context.asH2OFrame(dataframe, "Frame_Name", 200):

    The last parameter specifies the number of columns to sent for the preview.

- I'm getting the following exception when trying to start PySparkling installed via ``pip``:
  ``No matching distribution found for pyspark<=SUBST_SPARK_VERSION,>=UBST_SPARK_MAJOR_VERSION.0`` ``(from h2o-pysparkling-UBST_SPARK_MAJOR_VERSION==SUBST_SW_VERSION)``

    h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION package now correctly requires PySpark package. If you want to use PySparkling from
    installation from ``pip``, please also make sure to install `PySpark package <https://pypi.org/project/pyspark/>`__.

- I'm getting ``java.lang.reflect.InvocationTargetException`` via ``java.lang.IllegalArgumentException`` saying that
  "*YOUR_SPARK_ML_STAGE* parameter locale given invalid value *YOUR_LOCALE*." when using a Spark stage in my ML pipeline.

    Set the default locale for JVM of Spark driver to a valid combination of a language and country:

    .. code:: bash

        --conf spark.driver.extraJavaOptions="-Duser.language=en -Duser.country=US"
