PySparkling
===========

This section provides an introduction to PySparkling. To understand more about PySparkling, it's better to understand H2O, Spark, and Sparkling Water first.

What is H2O?
------------

H2O is an open-source, in-memory, distributed, fast and scalable machine learning and predictive analytics platform that provides capability to build machine learning models on big data and enable easy productionalization of them in an enterprise environment.

H2O core code is written in Java. Inside H2O, a Distributed Key/Value (DKV) store is used to access and reference data, models, objects, etc., across all nodes/machines, has a non blocking hashmap and a memory manager. The algoritms are implemented in the map-reduce style and utilize the Java Fork/Join framework.

The data is read in parallel and distributed across the cluster, stored in memory in a columnar format in a compressed way. H2O's data parser has built-in intelligence to guess the schema of the incoming dataset and supports data ingestion from multiple sources in various formats.

H2O's REST API enables to access all the capabilities of H2O from an external program or script, via JSON over HTTP. The REST API is used by H2O's web interface (Flow UI), the R binding (H2O-R) and the Python binding (H2O-Python).

The speed, quality, ease of use and model deployment, for the various cutting-edge supervised and unsupervised algorithms like Deep Learning, Tree Ensembles and Generalized Low Rank Models, makes H2O a highly sought after API for big data analytics.

What is Spark?
--------------

Apache Spark is an open-source, in-memory, distributed cluster computing framework that provides a comprehensive capability of building efficient big data pipelines.

Spark core implements a distributed memory abstraction, called Resilient Distributed Datasets (RDDs) and manages distributed task dispatching and scheduling. RDDs represents the first API for transforming data with Apache Spark and became essential building blocks for higher abstractions like Datasets and Dataframes (Datasets of Row objects).

Datasets/Dataframes in contrast to RDDs track the schema of transformed data and offer faster execution in most cases due to the Tungsten memory representation and utilization of the Catalyst optimizer. Furthermore, Dataset/Dataframe API is very similar SQL of standard database engines, thus data engineers and scientists can learn the API very fast.

RDDs and Datasets/Dataframes represents distributed data that sits on disk. Any transformation applied on RDDs and Datasets/Dataframes is lazy and nothing is executed until a user calls an action on a given abstraction. RDDs and Datasets/Dataframes can be cashed in order to avoid multiple execution of the same sequence of transformations.

Spark provides APIs in Java, Python, Scala, and R for building and manipulating RDDs and Datasets/Dataframes. Spark also comes with libraries having capabilities to process streamed data, graph data and to apply basic ML algorithms.

The fast and unified framework to manage data processing, makes Spark a preferred solution for big data analysis.

What is Sparkling Water?
------------------------

Sparkling Water is an integration of H2O into the Spark ecosystem. It facilitates the use of H2O algorithms in Spark workflows. It is designed as a regular Spark application and provides a way to start H2O services on each node of a Spark cluster and access data stored in data structures of Spark and H2O.

A Spark cluster is composed of one Driver JVM and one or many Executor JVMs. A Spark Context is a connection to a Spark cluster. Each Spark application creates a ``SparkSession``. The machine where the Spark application process, that creates a ``SparkSession`` (spark), is running, is the Driver node. The Spark Context connects to the cluster manager (either Spark standalone cluster manager, Mesos or YARN), that allocates executors to spark cluster for the application. Then, Spark sends the application code (defined by JAR or Python files) to the executors. Finally, the Spark Context sends tasks to the executors to run.

The driver program in Sparkling Water, creates a ``SparkSession`` (spark) which in turn is used to create an ``H2OContext`` (hc) that is used to start H2O services on the spark executors. An H2O Context is a connection to H2O cluster and  also facilitates communication between H2O and Spark. When an H2O cluster starts, it has the same topology as the Spark cluster and H2O nodes shares the same JVMs as the Spark Executors.

To leverage H2O's algorithms, data in Spark cluster, stored as an RDD or Dataset/Dataframe, needs to be converted to an H2OFrame (H2O's distributed data frame). This requires a data copy because of the difference in data layout in Spark (blocks/rows) and H2O (columns). But as data is stored in H2O in a highly compressed format, the overhead of making a data copy is low. When converting an H2OFrame to a RDD or Dataframe, Sparkling Water creates a wrapper around the H2OFrame to provide a given Spark API. In this case, no data is duplicated and data is served directly from the underlying H2OFrame. As H2O runs in the same JVMs as the Spark Executors, moving data from Spark to H2O or vice-versa requires a simple in memory, in process call.


What is PySparkling?
--------------------

PySparkling is an integration of Python with Sparkling Water. It allows user to start H2O services on a Spark cluster from Python API.
  
In the PySparkling driver program, the Spark Context, which uses Py4J to start the driver JVM and the Java Spark Context, is used to create the H2O Context (hc).  That in turn starts an H2O cloud (cluster) in the Spark ecosystem. Once the H2O cluster is up, the H2O Python package is used to interact with it and run H2O algorithms. All pure H2O calls are executed via H2O's REST API interface. Users can easily integrate their regular PySpark workflow with H2O algorithms using PySparkling.
  
PySparkling programs can be launched as an application or in an interactive shell or notebook environment. 
  

PySparkling and Spark Version
-----------------------------

There are multiple PySparkling packages, each is intended to be used with different Spark version.

 - h2o_pysparkling_2.1 - for Spark 2.1.x
 - h2o_pysparkling_2.2 - for Spark 2.2.x
 - h2o_pysparkling_2.3 - for Spark 2.3.x
 - h2o_pysparkling_2.4 - for Spark 2.4.x

So for example, to install PySparkling for Spark 2.1, the command would look like:

.. code-block:: bash

    pip install h2o_pysparkling_2.1

Dependencies
------------

Prerequisites:
    
  - Python 2.7 or 3+
  - Numpy 1.9.2

For Windows users, please grab a .whl from http://www.lfd.uci.edu/~gohlke/pythonlibs/#numpy.

In order to use PySparkling, it requires the following runtime python dependencies to be available on the system, all of which are available on PyPI:

.. code-block:: bash

  $ pip install requests
  $ pip install tabulate
  $ pip install future
  $ pip install colorama>=0.3.8

The required packages are installed automatically in case when PySparkling is installed from PyPI.


The Sparkling Water Python Module
---------------------------------

Prepare the environment
~~~~~~~~~~~~~~~~~~~~~~~

1. Either clone and build Sparkling Water project

  .. code-block:: bash

    git clone http://github.com/h2oai/sparkling-water
    cd sparkling-water
    ./gradlew build -x check


 or download and unpack sparkling water release from  `here <https://www.h2o.ai/download/>`_.

2. Configure the location of Spark distribution and cluster:

  .. code-block:: bash

    export SPARK_HOME="/path/to/spark/installation"
    export MASTER='local[*]'


Run PySparkling interactive shell
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Ensure you are in the Sparkling Water project directory and run PySparkling shell:

 .. code-block:: bash

    bin/pysparkling


The *pysparkling* shell accepts common *pyspark* arguments.


For running on YARN and other supported platforms please see `Running Sparkling Water on supported platforms
<https://github.com/h2oai/sparkling-water/blob/master/DEVEL.md#TargetPlatforms>`_.


2. Initialize H2OContext

 .. code:: python

      from pysparkling import *
      import h2o
      hc = H2OContext.getOrCreate(spark)


Run IPython Notebook with PySparkling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    PYSPARK_DRIVER_PYTHON="ipython" PYSPARK_DRIVER_PYTHON_OPTS="notebook" bin/pysparkling

For running on Windows, the syntax would be: 

.. code-block:: bash

    SET PYSPARK_DRIVER_PYTHON=ipython 
    SET PYSPARK_DRIVER_PYTHON_OPTS=notebook 
    bin/pysparkling


Run IPython with PySparkling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    PYSPARK_DRIVER_PYTHON="ipython" bin/pysparkling

Use PySparkling in Databricks Cloud
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to use PySparkling in Databricks cloud, PySparkling module has to be added as a library to the current cluster.
PySparkling can be added as library in two ways. You can either upload the PySparkling source zip file or add the
PySparkling module from PyPI.

If you choose to upload PySparkling zip file, don't forget to add PySparkling `Dependencies`_.
The PySparkling zip file is available in *py/dist* directory in both built Sparkling Water project and downloaded Sparkling Water release.

	
