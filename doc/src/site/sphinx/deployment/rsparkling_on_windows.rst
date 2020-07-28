.. _rsparkling_on_windows:

Use RSparkling in Windows Environments
--------------------------------------

Prepare Spark Environment
~~~~~~~~~~~~~~~~~~~~~~~~~
Initially, please follow the tutorial of running :ref:`run_on_windows`. The configurations applies to RSparkling
as well.

Prepare R Environment
~~~~~~~~~~~~~~~~~~~~~

Please follow the :ref:`rsparkling` Documentation to properly set up R packages and environment.

Test the Functionality
~~~~~~~~~~~~~~~~~~~~~~
Use the following script below to test if you have any RSparkling issues.

The script will check that you can:

1. Connect to Spark
2. Start H2O
3. Copy a R dataframe from R to a Spark DataFrame.

.. code:: r

    library(sparklyr)
    library(rsparkling)

    # Set spark connection
    sc <- spark_connect(master = "local", version = "SUBST_SPARK_VERSION")

    # Create H2O Context
    h2o_context(sc)

    # Copy R dataset to Spark
    library(dplyr)
    mtcars_tbl <- copy_to(sc, mtcars, overwrite = TRUE)
    mtcars_tbl


Troubleshooting
~~~~~~~~~~~~~~~

-  Error from running ``h2o_context``

    .. code:: java

         Error: org.apache.spark.SparkException: Job aborted due to stage failure: Task 3 in stage 2.0 failed 1 times, most recent failure: Lost task 3.0 in stage 2.0 (TID 13, localhost): java.lang.NullPointerException
                at java.lang.ProcessBuilder.start(ProcessBuilder.java:1012)
                at org.apache.hadoop.util.Shell.runCommand(Shell.java:483)
                at org.apache.hadoop.util.Shell.run(Shell.java:456)
                at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:722)
                at org.apache.hadoop.fs.FileUtil.chmod(FileUtil.java:873)
                at org.apache.hadoop.fs.FileUtil.chmod(FileUtil.java:853)
                at org.apache.spark.util.Utils$.fetchFile(Utils.scala:471)

    This is caused because ``HADOOP_HOME`` environment variable is not explicitly set. Set the
    `HADOOP_HOME` environment to ``%SPARK_HOME%/tmp/hadoop`` or location where ``bin\winutils.exe`` is located.

    Download winutils.exe binary from https://github.com/steveloughran/winutils repository.

    **NOTE**: You need to select the correct Hadoop version which is compatible with your Spark distribution.
    Hadoop version is often encoded in spark download name, for example, ``spark-SUBST_SPARK_VERSION-bin-hadoop2.7.tgz``.

-  Error from running ``copy_to``

    .. code:: java

        Error: java.lang.reflect.InvocationTargetException
                at sun.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method)
                at sun.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:62)
                at sun.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45)
                at java.lang.reflect.Constructor.newInstance(Constructor.java:423)
                at org.apache.spark.sql.hive.client.IsolatedClientLoader.createClient(IsolatedClientLoader.scala:258)
                at org.apache.spark.sql.hive.HiveUtils$.newClientForMetadata(HiveUtils.scala:359)
                at org.apache.spark.sql.hive.HiveUtils$.newClientForMetadata(HiveUtils.scala:263)
                at org.apache.spark.sql.hive.HiveSharedState.metadataHive$lzycompute(HiveSharedState.scala:39)


    This is caused because there are no permissions to the folder: ``file:///tmp/hive``.
    You can run a command in the command prompt which will change the permissions of the ``/tmp/hive`` directory.
    It will change the permissions of the ``/tmp/hive`` directory so that all three users (Owner, Group, and Public)
    can Read, Write, and Execute.

    To change the permissions, go to the command prompt and write: ``\path\to\winutils\Winutils.exe chmod 777 \tmp\hive``

    You can also create a file ``hive-site.xml`` in ``%HADOOP_HOME%\conf`` and modify the location of default Hive scratch dir
    (which is ``/tmp/hive``):

    .. code:: xml

        <configuration>
          <property>
            <name>hive.exec.scratchdir</name>
            <value>/Users/michal/hive/</value>
            <description>Scratch space for Hive jobs</description>
          </property>
        </configuration>


    In this case, do not forget to set the variable ``HADOOP_CONF_DIR``:

    .. code:: bash

        SET HADOOP_CONF_DIR=%HADOOP_HOME%\conf


    If the previous does not work, you can delete the ``metastore_db`` folder in your R working directory.

References
~~~~~~~~~~

- :ref:`rsparkling`
- `H2O.ai website <http://h2o.ai>`__
- `Running Spark Applications on Windows <https://jaceklaskowski.gitbooks.io/mastering-apache-spark/content/spark-tips-and-tricks-running-spark-windows.html>`__
