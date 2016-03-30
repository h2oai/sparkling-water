Setup and Installation
======================

Prerequisites:
    
  - Python 2.7
  - Numpy 1.9.2

For windows users, please grab a .whl from http://www.lfd.uci.edu/~gohlke/pythonlibs/#numpy

In order to use PySparkling, it requires the following runtime python dependencies to be available on the system: *requests*, *tabulate*, *six* and *future* modules, all of which are available on PyPi:

.. code-block:: bash

  $ pip install requests
  $ pip install tabulate
  $ pip install six
  $ pip install future
  
The required packages are installed automatically in case PySparkling is installed from PyPi (this option will be available soon).



The Sparkling-Water Python Module
=================================

Prepare the environment
-----------------------
1. Either clone and build Sparkling Water project

.. code-block:: bash

    git clone http://github.com/h2oai/sparkling-water
    cd sparkling-water
    ./gradlew build -x check


or download and unpack sparkling water release from  `here
<http://www.h2o.ai/download/sparkling-water/choose>`_.

2. Configure the location of Spark distribution and cluster:

.. code-block:: bash

    export SPARK_HOME="/path/to/spark/installation"
    export MASTER='local-cluster[3,2,2048]'


Run PySparkling interactive shell
---------------------------------

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
      hc = H2OContext(sc).start()


Run IPython Notebook with PySparkling
-------------------------------------
.. code-block:: bash

    IPYTHON_OPTS="notebook" bin/pysparkling


Run IPython with PySparkling
----------------------------
.. code-block:: bash

    PYSPARK_PYTHON="ipython" bin/pysparkling


Use PySparkling as Spark Package
--------------------------------
.. code-block:: bash

	$SPARK_HOME/bin/spark-submit
	--packages ai.h2o:sparkling-water-core_2.10:1.4.10  
	--py-files $SPARKLING_HOME/py/dist/pySparkling-1.4.10-py2.7.egg  ./py/examples/scripts/ChicagoCrimeDemo.py

Use PySparkling in Databricks Cloud
-----------------------------------

To use PySparkling in Databricks cloud, add PySparkling egg file as a library and attach this library to an existing cluster. After this step you can use PySparkling in a notebook
attached to the cluster in exactly same way as from pySparkling shell.

The PySparkling egg file is available in *py/dist* directory in both built Sparkling Water project and downloaded Sparkling Water release.

	
An introduction to PySparkling
==============================

What is H2O?
------------

H2O is an opensource, in-memory, distributed, fast and scalable machine learning and predictive analytics platform that provides capability to build machine learning models on big data and allow easy productionalization of them in an enterprise environment. 

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

What is Sparkling water?
------------------------

Sparkling water is an integration of H2O into the Spark ecosystem. It facilitates the use of H2O algorithms in Spark workflows. It is designed as a regular Spark application and provides a way to start H2O services on each node of a Spark cluster and access data stored in data structures of Spark and H2O.

A Spark cluster is composed of one Driver JVM and one or many Executor JVMs. Spark Context is a connection to a spark cluster. Each Spark application creates a Spark Context.
The machine where the Spark application process, that creates a SparkContext (sc), is running, is the Driver node. The SparkContext connects to the cluster manager (either Spark standalone cluster manager, Mesos or YARN), that allocates executors to spark cluster for the application. Then, Spark sends the application code (defined by JAR or Python files ) to the executors. Finally, SparkContext sends tasks to the executors to run.

The driver program in Sparkling water, creates a Spark context(sc) which in turn is used to create an H2O Context(hc) that is used to start H2O services on the spark executors. H2O Context is a connection to H2O cluster and  also facilitates communication between H2O and Spark. When an H2O cluster starts, it has the same topology as the Spark cluster and H2O nodes shares the same JVMs as the Spark Executors.

To leverage H2O's algorithms, data in Spark cluster, stored as an RDD, needs to be converted to an H2Odataframe.This requires a data copy because of the difference in data layout in Spark(blocks/rows) and H2O(columns). But as data is stored in H2O in a highly compressed format, the overhead of making a data copy is low. When converting an H2Odataframe to RDD, Sparkling water creates a wrapper around the H2Odataframe to provide an RDD-like API. In this case, no data is duplicated and data is served directly from the underlying H2Odataframe.As H2O runs in the same JVMs as the Spark Executors, moving data from Spark to H2o or vise versa requires a simple in memory, in process call.


What is PySparkling Water?
--------------------------

PySparkling Water is an integration of Python with Sparkling water. It allows user to start H2O services on a spark cluster from Python API.
	
In the PySparkling Water driver program, Spark context(sc), that uses Py4J to start the driver JVM and the JAVA spark Context, is used to create H2O context(hc), that in turn starts H2O cloud in the Spark ecosystem. Once the H2O cluster is up, H2O-Python package is used to interact with it and run H2O algorithms. All pure H2O calls are executed via H2O's rest api interface. Users can easily integrate their regular PySpark workflow with H2O algorithms using PySparkling Water.
	
PySparkling Water programs can be launched as an application or in an interactive shell or notebook environment.
