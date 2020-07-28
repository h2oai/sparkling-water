Installing and Starting
=======================

This section describes how to download and run Sparkling Water in different environments. Refer to the :ref:`pysparkling` and :ref:`rsparkling` sections for instructions on installing and running PySparkling and RSparkling. 

Download and Run Locally
------------------------

This section describes how to quickly get started with Sparkling Water on your personal computer (in Spark's ``local`` cluster mode).

1. Download and install Spark (if not already installed) from the `Spark Downloads page <https://spark.apache.org/downloads.html>`__.


    - Choose Spark release: SUBST_SPARK_VERSION
    - Choose a package type: Pre-built for Hadoop 2.7 and later

2. Point SPARK_HOME to the existing installation of Spark and export variable MASTER.

.. code:: bash

    export SPARK_HOME="/path/to/spark/installation"
    # To launch a local Spark cluster.
    export MASTER="local[*]"

3. From your terminal, run:

.. code:: bash

    cd ~/Downloads
    unzip sparkling-water-SUBST_SW_VERSION.zip
    cd sparkling-water-SUBST_SW_VERSION
    bin/sparkling-shell

4. Create an H2O cloud inside the Spark cluster:

.. code:: scala

    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()
    import h2oContext._

5. Begin using Sparkling Water by following `this demo <https://github.com/h2oai/sparkling-water/tree/master/examples#step-by-step-weather-data-example>`__, which imports airlines and weather data and runs predictions on delays.

|NOTE_PASTE_MODE|

Run on Hadoop
-------------

This section describes how to launch Sparkling Water on Hadoop using YARN.

1. Download Spark (if not already installed) from the `Spark Downloads page <https://spark.apache.org/downloads.html>`__.

.. code:: bash

    - Choose Spark release: SUBST_SPARK_VERSION
    - Choose a package type: Pre-built for Hadoop 2.7 and later

2. Point SPARK_HOME to the existing installation of Spark.

.. code:: bash

    export SPARK_HOME='/path/to/spark/installation'

3. Set the HADOOP_CONF_DIR and Spark MASTER environmental variables.

.. code:: bash

    export HADOOP_CONF_DIR=/etc/hadoop/conf
    export MASTER="yarn"

4. Download Spark and use ``sparkling-shell`` to launch Sparkling Shell on YARN.

.. code:: bash

    wget http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/sparkling-water-SUBST_SW_VERSION.zip
    unzip sparkling-water-SUBST_SW_VERSION.zip 
    cd sparkling-water-SUBST_SW_VERSION/
    bin/sparkling-shell --num-executors 3 --executor-memory 2g --master yarn --deploy-mode client

5. Create an H2O cluster inside the Spark cluster:

.. code:: scala

    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()
    import h2oContext._ 

|NOTE_PASTE_MODE|

Run on a Standalone Spark Cluster
---------------------------------

This section describes how to launch H2O on a standalone Spark cluster.

1. Download Spark (if not already installed) from the `Spark Downloads page <https://spark.apache.org/downloads.html>`__.

.. code:: bash

    - Choose Spark release: SUBST_SPARK_VERSION
    - Choose a package type: Pre-built for Hadoop 2.7 and later

2. Point SPARK_HOME to the existing installation of Spark and export variable MASTER.

.. code:: bash

    export SPARK_HOME='/path/to/spark/installation'

3. From your terminal, run:

.. code:: bash

    cd ~/Downloads
    unzip sparkling-water-SUBST_SW_VERSION.zip
    cd sparkling-water-SUBST_SW_VERSION
    bin/launch-spark-cloud.sh
    export MASTER="spark://localhost:7077"
    bin/sparkling-shell

4. Create an H2O cloud inside the Spark cluster:

.. code:: scala

    import ai.h2o.sparkling._
    val h2oContext = H2OContext.getOrCreate()
    import h2oContext._ 

|NOTE_PASTE_MODE|

External Backend
----------------

Sparkling Water Kluster mode supports a connection to external H2O clusters (standalone/Hadoop).
The H2O cluster needs to be started with a corresponding H2O, which can be downloaded as below.

1. Download and unpack the Sparkling Water distribution.

2. Download the corresponding H2O driver for your Hadoop distribution (e.g., hdp2.2, cdh5.4) or standalone one:

.. code:: bash

    export H2O_DRIVER_JAR=$(/path/to/sparkling-water-SUBST_SW_VERSION/bin/get-h2o-driver.sh hdp2.2)

3. Set path to sparkling-water-assembly-extensions-SUBST_SW_VERSION-all.jar which is bundled in Sparkling Water archive:

.. code:: bash

    SW_EXTENSIONS_ASSEMBLY=/path/to/sparkling-water-SUBST_SW_VERSION/jars/sparkling-water-assembly-extensions-SUBST_SW_VERSION-all.jar

4. Start an H2O cluster on Hadoop

.. code:: bash

    hadoop -jar $H2O_DRIVER_JAR -libjars $SW_EXTENSIONS_ASSEMBLY -sw_ext_backend -jobname test -nodes 3 -mapperXmx 6g

5. In your Sparkling Water application, create H2OContext:

**Scala**

.. code:: scala

    import ai.h2o.sparkling._
    val conf = new H2OConf().setExternalClusterMode().useManualClusterStart().setCloudName("test")
    val hc = H2OContext.getOrCreate(conf)

**Python**

.. code:: python

    from pysparkling import *
    conf = H2OConf().setExternalClusterMode().useManualClusterStart().setCloudName("test")
    hc = H2OContext.getOrCreate(conf)

**Note**: The following is a list of supported Hadoop distributions: SUBST_H2O_DRIVERS_LIST

For more information, please follow the :ref:`backend`.

|NOTE_PASTE_MODE|

Use from Maven
--------------

This section provides a Gradle-style specification for Maven artifacts.

See the `h2o-droplets GitHub repository <https://github.com/h2oai/h2o-droplets>`__ for a working example.

.. code:: bash

  repositories {
    mavenCentral()
  }

  dependencies {
    compile "ai.h2o:sparkling-water-package_SUBST_SCALA_BASE_VERSION:SUBST_SW_VERSION"
  }

See Maven Central for `artifact details <http://search.maven.org/#artifactdetails|ai.h2o|sparkling-water-package_SUBST_SCALA_BASE_VERSION|SUBST_SW_VERSION|jar>`__.

|NOTE_PASTE_MODE|

Sparkling Water as a Spark Package
----------------------------------

This section describes how to start Spark with Sparkling Water enabled via Spark package.

1. Ensure that Spark is installed, and ``MASTER`` and ``SPARK_HOME`` environmental variables are properly set.
2. Start Spark and point to maven coordinates of Sparkling Water:

.. code:: bash

   $SPARK_HOME/bin/spark-shell --packages ai.h2o:sparkling-water-package_SUBST_SCALA_BASE_VERSION:SUBST_SW_VERSION

3. Create an H2O cloud inside the Spark cluster:

.. code:: scala

   import ai.h2o.sparkling._
   val h2oContext = H2OContext.getOrCreate()
   import h2oContext._ 

|NOTE_PASTE_MODE|

.. |NOTE_PASTE_MODE| replace:: Please note that when copying code into the Scala Sparkling shell, make sure to use the ``:paste`` mode feature of the Scala REPL. Otherwise, you might hit a compiler error.
