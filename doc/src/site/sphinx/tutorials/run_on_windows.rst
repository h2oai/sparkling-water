.. _run_on_windows:

Use Sparkling Water in Windows Environments
-------------------------------------------

Windows environments require several additional steps to run Spark and Sparkling Water. A great summary of the configuration steps is available `here <https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-tips-and-tricks-running-spark-windows.html>`__.

To use Sparkling Water in Windows environments:

1. Download the appropriate Spark distribution from the `Spark Downloads page <https://spark.apache.org/downloads.html>`__.

2. Point the ``SPARK_HOME`` variable to the location of your Spark distribution:

   .. code:: bat

      SET SPARK_HOME=<location of your downloaded Spark distribution>


3. From https://github.com/steveloughran/winutils, download ``winutils.exe`` for the Hadoop version that is referenced by your Spark distribution (For example, for ``spark-SUBST_SPARK_VERSION-bin-hadoop2.7.tgz``, you need ``wintutils.exe`` for Hadoop 2.7.)

4. Move ``winutils.exe`` into a new directory ``%SPARK_HOME%\hadoop\bin`` and set:

   .. code:: bat

      SET HADOOP_HOME=%SPARK_HOME%\hadoop


5. Create a new file ``%SPARK_HOME%\hadoop\conf\hive-site.xml``, which sets up a default Hive scratch directory. The best location is a writable temporary directory, for example ``%TEMP%\hive``:

   .. code:: xml

      <configuration>
        <property>
          <name>hive.exec.scratchdir</name>
          <value>PUT HERE LOCATION OF TEMP FOLDER</value>
          <description>Scratch space for Hive jobs</description>
        </property>
      </configuration>

   **Note**: You can also use the Hive default scratch directory, which is ``c:\tmp\hive``. In this case, you need to create the directory manually and call ``winutils.exe chmod -R 777 c:\tmp\hive`` to set up the correct permissions.

6. Set the ``HADOOP_CONF_DIR`` property:

   .. code:: bat

      SET HADOOP_CONF_DIR=%SPARK_HOME%\hadoop\conf


7. Go to the Sparkling Water directory and run the Sparkling Water shell:

   .. code:: bat

      bin/sparkling-shell.cmd
