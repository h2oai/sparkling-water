Use Sparkling Water in Windows environments
-------------------------------------------

The Windows environments require several additional steps to make Spark
and Sparkling Water working. Great summary of configuration steps is
`here <https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-tips-and-tricks-running-spark-windows.html>`__.

On Windows it is required:

1. Download Spark distribution

2. Setup variable ``SPARK_HOME``:

   .. code:: bat

      SET SPARK_HOME=<location of your downloaded Spark distribution>


3. From https://github.com/steveloughran/winutils, download ``winutils.exe`` for Hadoop version which is referenced by your Spark
   distribution (for example, for ``spark-2.1.0-bin-hadoop2.6.tgz`` you need ``wintutils.exe`` for `hadoop2.6 <https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.4/bin/winutils.exe?raw=true>`__).

4. Put ``winutils.exe`` into a new directory ``%SPARK_HOME%\hadoop\bin`` and set:

   .. code:: bat

      SET HADOOP_HOME=%SPARK_HOME%\hadoop


5. Create a new file ``%SPARK_HOME%\hadoop\conf\hive-site.xml`` which setup default Hive scratch dir. The best location
   is a writable temporary directory, for example ``%TEMP%\hive``:

   .. code:: xml

      <configuration>
        <property>
          <name>hive.exec.scratchdir</name>
          <value>PUT HERE LOCATION OF TEMP FOLDER</value>
          <description>Scratch space for Hive jobs</description>
        </property>
      </configuration>

   Note: you can also use Hive default scratch directory which is ``c:\tmp\hive``. In this case, you need to create
   directory manually and call ``winutils.exe chmod -R 777 c:\tmp\hive`` to setup right permissions.

6. Set ``HADOOP_CONF_DIR`` property:

   .. code:: bat

      SET HADOOP_CONF_DIR=%SPARK_HOME%\hadoop\conf


7. Go to Sparkling Water directory and run Sparkling Water shell:

   .. code:: bat

      bin/sparkling-shell.cmd