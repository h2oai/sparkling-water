Sparkling Water
================

|Join the chat at https://gitter.im/h2oai/sparkling-water| |image1|
|image2| |image3|

What is H2O?
------------

H2O is an open source, in-memory, distributed, fast, and scalable machine learning and predictive analytics platform that provides capability to build machine learning models on big data and allow easy productionalization of them in an enterprise environment. 

H2O core code is in JAVA. Inside H2O, a Distributed Key/Value store is used to access and reference data, models, objects, etc., across all nodes/machines, has a non blocking hashmap and a memory manager. The algoritms are implemented in a map reduce style and utilize the JAVA Fork/Join framework.
The data is read in parallel and is distributed across the cluster, stored in memory in a columnar format in a compressed way. H2O's data parser has a  built-in intelligence to guess the schema of the incoming dataset and supports data ingest from multiple sources in various formats.

H2O's REST API allows access to all the capabilities of H2O from an external program or script, via JSON over HTTP. The Rest API is used by H2O's web interface(Flow UI), R binding(H2O-R) and Python binding(H2O-Python).

The speed, quality and ease of use and model-deployment, for the various cutting edge Supervised and Unsupervised algorithms like Deeplearning, Tree Ensembles and GLRM, makes H2O a highly sought after API for big data  data science.

What is Spark?
--------------

Spark is an open source, in-memory, distributed cluster computing framework that provides a comprehensive capability of building efficient big data pipelines.

Spark core implements a distributed memory abstraction, called Resilient Distributed Datasets (RDDs) and manages distributed task dispatching and scheduling.An RDD is a logical collection of data. The actual data sits on disk. RDDs can be cashed for interactive data analysis. Operations on an RDD are lazy and are only executed when a user calls an action on an RDD. 

Spark provides APIs in Java, Python, Scala, and R for building and manipulating RDDs. It also supports SQL queries, Streaming data, MLlib and graph data processing.

The fast and unified framework to manage data processing, makes Spark a preferred solution for big data analysis.

What is Sparkling Water?
------------------------

Sparkling Water is an integration of H2O into the Spark ecosystem. It facilitates the use of H2O algorithms in Spark workflows. It is designed as a regular Spark application and provides a way to start H2O services on each node of a Spark cluster and access data stored in data structures of Spark and H2O.

A Spark cluster is composed of one Driver JVM and one or many Executor JVMs. Spark Context is a connection to a spark cluster. Each Spark application creates a Spark Context.
The machine where the Spark application process, that creates a SparkContext (sc), is running, is the Driver node. The SparkContext connects to the cluster manager (either Spark standalone cluster manager, Mesos or YARN), that allocates executors to spark cluster for the application. Then, Spark sends the application code (defined by JAR or Python files ) to the executors. Finally, SparkContext sends tasks to the executors to run.

The driver program in Sparkling water, creates a SparkContext (sc) which in turn is used to create an H2O Context (hc) that is used to start H2O services on the spark executors. H2O Context is a connection to H2O cluster and  also facilitates communication between H2O and Spark. When an H2O cluster starts, it has the same topology as the Spark cluster and H2O nodes shares the same JVMs as the Spark Executors.

To leverage H2O's algorithms, data in Spark cluster, stored as an RDD, needs to be converted to an H2Odataframe.This requires a data copy because of the difference in data layout in Spark(blocks/rows) and H2O(columns). But as data is stored in H2O in a highly compressed format, the overhead of making a data copy is low. When converting an H2Odataframe to RDD, Sparkling water creates a wrapper around the H2Odataframe to provide an RDD-like API. In this case, no data is duplicated and data is served directly from the underlying H2Odataframe.As H2O runs in the same JVMs as the Spark Executors, moving data from Spark to H2o or vise versa requires a simple in memory, in process call.


Getting Started
---------------

Select the right version
~~~~~~~~~~~~~~~~~~~~~~~~

The Sparkling Water is developed in multiple parallel branches. Each
branch corresponds to a Spark major release (e.g., branch **rel-1.5**
provides implementation of Sparkling Water for Spark **1.5**).

Switch to the correct branch: 

- For Spark 1.6, use branch `rel-1.6 <https://github.com/h2oai/sparkling-water/tree/rel-1.6>`__ 
- For Spark 1.5, use branch `rel-1.5 <https://github.com/h2oai/sparkling-water/tree/rel-1.5>`__ 
- For Spark 1.4 use, branch `rel-1.4 <https://github.com/h2oai/sparkling-water/tree/rel-1.4>`__ 
- For Spark 1.3 use, branch `rel-1.3 <https://github.com/h2oai/sparkling-water/tree/rel-1.3>`__

**Note**: The `master <https://github.com/h2oai/sparkling-water/tree/master>`__ branch includes the latest changes for the latest Spark version. The changes are back-ported into older Sparkling Water versions.

Requirements
~~~~~~~~~~~~

-  Linux/OS X/Windows
-  Java 7+
-  `Spark 1.3+ <https://spark.apache.org/downloads.html>`__
-  ``SPARK_HOME`` shell variable must point to your local Spark installation

Download
~~~~~~~~ 
 
Binaries for each Sparkling Water version are available here:

- `Latest version <http://h2o-release.s3.amazonaws.com/sparkling-water/master/latest.html>`__
- `Latest 1.6 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.6/latest.html>`__
- `Latest 1.5 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.5/latest.html>`__
- `Latest 1.4 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.4/latest.html>`__
- `Latest 1.3 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.3/latest.html>`__

Additional Resources
--------------------

Additional resources are available to help you get up and running with Sparkling Water.

Sparkling Water Tutorials
~~~~~~~~~~~~~~~~~~~~~~~~~

-  `Building Machine Learning Applications with Sparkling Water <http://docs.h2o.ai/h2o-tutorials/latest-stable/tutorials/sparkling-water/index.html>`_: This short tutorial describes project building and demonstrates the capabilities of Sparkling Water using Spark Shell to build a Deep Learning model.

-  `Connecting RStudio to Sparkling Water <https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/Connecting_RStudio_to_Sparkling_Water.md>`_: This illustrated tutorial describes how to use RStudio to connect to Sparkling Water.

- `Sparkling Water on YARN <http://blog.h2o.ai/2014/11/sparkling-water-on-yarn-example/>`_: Follow these instructions to run Sparkling Water on a YARN cluster.


Sparkling Water Blog Posts
~~~~~~~~~~~~~~~~~~~~~~~~~~

- `How Sparkling Water Brings H2O to Spark <http://blog.h2o.ai/2014/09/how-sparkling-water-brings-h2o-to-spark/>`_

- `H2O - The Killer App on Spark <http://blog.h2o.ai/2014/06/h2o-killer-application-spark/>`_

- `In-memory Big Data: Spark + H2O <http://blog.h2o.ai/2014/03/spark-h2o/>`_


Sparkling Water Meetup Slide Decks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  `Sparkling Water Meetups <http://www.slideshare.net/0xdata/spa-43755759>`_

-  `Interactive Session on Sparkling Water <http://www.slideshare.net/0xdata/2014-12-17meetup>`_

-  `Sparkling Water Hands-On <http://www.slideshare.net/0xdata/2014-09-30sparklingwaterhandson>`_

-  `Additional Sparkling Water Meetup meeting notes <https://github.com/h2oai/sparkling-water/tree/master/examples/meetups>`_

Using Sparkling Water
---------------------

Sparkling Water is distributed as a Spark application library which can
be used by any Spark application. Furthermore, we provide also zip
distribution which bundles the library and shell scripts.

There are several ways of using Sparkling Water: 

- Sparkling Shell 
- Sparkling Water driver 
- Spark Shell including the Sparkling Water library via the ``--jars`` or ``--packages`` option 
- Spark Submit including the Sparkling Water library via the ``--jars`` or ``--packages`` option 
- PySpark with PySparkling

An H2O cloud is created automatically when ``H2OContext.getOrCreate`` is called. Because it's not technically possible to get number the of executors in Spark, we try to discover all executors at the initiation of ``H2OContext``, and we start H2O instance inside of each discovered executor. This solution is easiest to deploy; however when Spark or YARN kills the executor - which is not an unusual case - the whole H2O cluster goes down because H2O doesn't support high availability.

Here we show a few examples of how H2OContext can be started.

 Explicitly specify the internal backend on ``H2OConf``

 ::

    val conf = new H2OConf(sc).setInternalClusterMode()
    val h2oContext = H2OContext.getOrCreate(sc, conf)

 If the ``spark.ext.h2o.backend.cluster.mode`` property was set to ``internal`` on the command line or on the ``SparkConf`` class, we can call:

 ::

    val h2oContext = H2OContext.getOrCreate(sc) 

 or

 ::

    val conf = new H2OConf(sc)
    val h2oContext = H2OContext.getOrCreate(sc, conf)

Run Sparkling Shell
~~~~~~~~~~~~~~~~~~~

The Sparkling Shell encapsulates a regular Spark shell and appends the Sparkling Water library on the classpath via ``--jars`` option. The Sparkling Shell supports the creation of an H2O cloud and the execution of H2O
algorithms.

1. First, build a package containing Sparkling Water using ``./gradlew assemble``

2. Configure the location of the Spark cluster. For example:

 ::
 
	export SPARK_HOME="/path/to/spark/installation"   export MASTER="local-cluster[3,2,2048]"

 In this case, ``local-cluster[3,2,2048]`` points to an embedded cluster of 3 worker nodes, each with 2 cores and 2G of memory.

3. Run Sparkling Shell using ``bin/sparkling-shell``

    Sparkling Shell accepts common Spark shell arguments. For example,
    to increase memory allocated by each executor, use the
    ``spark.executor.memory`` parameter:
    ``bin/sparkling-shell --conf "spark.executor.memory=4g"``

4. Initialize H2OContext.
   ``scala import org.apache.spark.h2o._ val hc = H2OContext.getOrCreate(sc)``

    H2OContext starts the H2O services on top of Spark cluster and provides
    primitives for transformations between H2O and Spark datastructures.

Run Examples
''''''''''''
 
The Sparkling Water distribution includes a set of examples. You can find their implementation in the `example <example/>`__ folder, and you can run them in the following way:

1. Build a package that can be submitted to Spark cluster:
   ``./gradlew assemble``

2. Set the configuration of the demo Spark cluster (for example,
   ``local-cluster[3,2,1024]``)
   ``export SPARK_HOME="/path/to/spark/installation"   export MASTER="local-cluster[3,2,1024]"``

 In this example, the description ``local-cluster[3,2,1024]`` creates a local cluster consisting of 3 workers.

3. And run the example: ``bin/run-example.sh``

For more details about examples or for more examples, please see the
`README.md <examples/README.md>`__ file in the `examples
directory <examples/>`__.

--------------

Use Sparkling Water via Spark Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water is also published as a Spark package. You can use it directly from your Spark distribution. For example, if you have Spark version 1.5 and would like to use Sparkling Water version 1.5.2 and launch example ``CraigslistJobTitlesStreamingApp``, then you can use the following command:

.. code:: bash

    $SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 --class org.apache.spark.examples.h2o.CraigslistJobTitlesStreamingApp /dev/null

The Spark ``--packages`` option points to published Sparkling Water
packages in Maven repository.

The following command works similarly for ``spark-shell``:

.. code:: bash

    $SPARK_HOME/bin/spark-shell --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 

The same command works for Python programs:

.. code:: bash

    $SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 example.py

**Note**: When you are using Spark packages, you do not need to download Sparkling Water distribution. The Spark installation is sufficient.

--------------

Develop with Sparkling Water
----------------------------

Setup Sparkling Water in IntelliJ IDEA
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. In IDEA, install the Scala plugin for IDEA
2. Open a terminal window and enter the following command:

::

    git clone https://github.com/h2oai/sparkling-water.git
    cd sparkling-water
    ./gradlew idea
    open sparkling-water.ipr

3. In IDEA, open the file: sparkling-water/core/src/main/scala/water/SparklingWaterDriver.scala
 
 **Note**: Wait for IDEA indexing to complete so the Run and Debug choices are available.

4. In IDEA, *Run* or *Debug* SparklingWaterDriver (via right-click)

Develop Applications with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An application using Sparkling Water is regular Spark application which
bundling Sparkling Water library. See Sparkling Water Droplet providing
an example application
`here <https://github.com/h2oai/h2o-droplets/tree/master/sparkling-water-droplet>`__.

Reporting Issues
----------------

To report issues, please use our JIRA page at `http://jira.h2o.ai/ <https://0xdata.atlassian.net/projects/SW/issues>`__.

FAQ
---

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

-  Spark is very slow during initialization or H2O does not form a
   cluster. What should I do?

    Configure the Spark variable ``SPARK_LOCAL_IP``. For example:

    ::

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

    ::

        export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
        export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH

-  Trying to import a class from the ``hex`` package in Sparkling Shell
   but getting weird error:
   ``error: missing arguments for method hex in object functions;   follow this method with '_' if you want to treat it as a partially applied``

    In this case you are probably using Spark 1.5, which is importing SQL
    functions into Spark Shell environment. Please use the following
    syntax to import a class from the ``hex`` package:

    ::

        import _root_.hex.tree.gbm.GBM

.. |Join the chat at https://gitter.im/h2oai/sparkling-water| image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/h2oai/sparkling-water?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

.. |image1| image:: https://travis-ci.org/h2oai/sparkling-water.svg?branch=master
   :target: https://travis-ci.org/h2oai/sparkling-water

.. |image2| image:: https://maven-badges.herokuapp.com/maven-central/ai.h2o/sparkling-water-core_2.10/badge.svg
   :target: http://search.maven.org/#search%7Cgav%7C1%7Cg:%22ai.h2o%22%20AND%20a:%22sparkling-water-core_2.10%22

.. |image3| image:: https://img.shields.io/badge/License-Apache%202-blue.svg
   :target: https://github.com/h2oai/sparkling-water/blob/master/LICENSE
