Sparkling Water Logging
-----------------------

Changing Log Directory
~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water log directory can be specified using the Spark option ``spark.ext.h2o.log.dir`` or at run-time
using our setters as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setLogDir("dir")
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setLogDir("dir")
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf = H2OConf()$setLogDir("dir")
            hc = H2OContext.getOrCreate(conf)


Logging Directory Selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water uses the following steps to determine the final logging directory:

- First, we check if the ``spark.yarn.app.container.log.dir`` environmental property is defined. If
  it is available, we use it as a logging directory.

- If ``spark.yarn.app.container.log.dir`` is missing, we check whether ``spark.ext.h2o.log.dir`` is defined and use it
  if it is not empty.

- At last, if both options are missing, we store the logs into the default directory ``${user.dir}/h2ologs/${sparkAppId}``

We can see that ``spark.yarn.app.container.log.dir`` has precedence over our logging option. This is to ensure that
when running on YARN, the logs are collected by YARN tooling.

Obtaining Logs for Sparkling Water on YARN
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When launching Sparkling Water on YARN, you can find the application ID for the YARN job on the resource
manager (where you can also find the application master, which is also the Spark master). The following
command prints the YARN logs to the console:

.. code:: shell

    yarn logs -applicationId <application id>


Obtaining Logs for Standalone Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the Spark property ``SPARK_LOG_DIR`` is set to ``$SPARK_HOME/work/``. To also log the configuration with which
the Spark was started, start Sparkling Water with the following configuration:

.. code:: shell

    bin/sparkling-shell.sh --conf spark.logConf=true

The logs for the particular application are located at ``$SPARK_HOME/work/<application id>``. The directory contains
also stdout and stderr for each node in the cluster.

Change Sparkling Water Logging Level
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To change the log level for H2O running inside the Sparkling Water,
you can use the option ``spark.ext.h2o.log.level`` or set it as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: Scala

            import ai.h2o.sparkling._
            val conf = new H2OConf().setLogLevel("DEBUG")
            val hc = H2OContext.getOrCreate(conf)

    .. tab-container:: Python
        :title: Python

        .. code:: python

            from pysparkling import *
            conf = H2OConf().setLogLevel("DEBUG")
            hc = H2OContext.getOrCreate(conf)

    .. tab-container:: R
        :title: R

        .. code:: r

            library(rsparkling)
            sc <- spark_connect(master = "local")
            conf = H2OConf()$setLogLevel("DEBUG")
            hc = H2OContext.getOrCreate(conf)

We can also change the logging level used by Spark by modifying the log4j.properties file passed to Spark as:

.. code:: shell

    cd $SPARK_HOME/conf
    cp log4j.properties.template log4j.properties

Then either in a text editor or vim, change the contents of the log4j.properties file from:

.. code:: shell

    #Set everything to be logged to the console
    log4j.rootCategory=INFO, console
    ...

to:

.. code:: shell

    #Set everything to be logged to the console
    log4j.rootCategory=WARN, console
    ...
