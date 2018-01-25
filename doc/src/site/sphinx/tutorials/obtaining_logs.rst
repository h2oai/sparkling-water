Obtain Sparkling Water Logs
---------------------------

Depending on how you launched H2O, there are a couple of ways to obtain the logs.

Logs for Sparkling Water on YARN
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When launching Sparkling Water on YARN, you can find the application ID for the YARN job on the resource manager (where you can also find the application master, which is also the Spark master). The following command prints the YARN logs to the console:

.. code:: shell

    yarn logs -applicationId <application id>


Logs for Standalone Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the Spark property ``SPARK_LOG_DIR`` is set to ``$SPARK_HOME/work/``. To also log the configuration with which the Spark was started, start Sparkling Water with the following configuration:

.. code:: shell

    bin/sparkling-shell.sh --conf spark.logConf=true

The logs for the particular application are located at ``$SPARK_HOME/work/<application id>``. The directory contains also stdout and stderr for each node in the cluster.
