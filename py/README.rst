PySparkling
===========

What is PySparkling Water?
--------------------------

PySparkling Water is an integration of Python with Sparkling water. It allows user to start H2O services on a spark cluster from Python API.
	
In the PySparkling Water driver program, Spark context(sc), that uses Py4J to start the driver JVM and the JAVA spark Context, is used to create H2O context(hc), that in turn starts H2O cloud in the Spark ecosystem. Once the H2O cluster is up, H2O-Python package is used to interact with it and run H2O algorithms. All pure H2O calls are executed via H2O's rest api interface. Users can easily integrate their regular PySpark workflow with H2O algorithms using PySparkling Water.
	
PySparkling Water programs can be launched as an application or in an interactive shell or notebook environment. 
	

PySparkling Requirements
------------------------

**Prerequisites**
    
  - Python 2.7
  - Numpy 1.9.2

For windows users, please grab a .whl from http://www.lfd.uci.edu/~gohlke/pythonlibs/#numpy

**Dependencies**

In order to use PySparkling, the following runtime python dependencies must be available on the system: *requests*, *tabulate*, *six* and *future* modules. Each of these dependencies is available on PyPi:

.. code-block:: bash

  $ pip install requests
  $ pip install tabulate
  $ pip install six
  $ pip install future
  
Note that the required packages are installed automatically when PySparkling is installed from PyPi.

PySparkling Installation
------------------------

Multiple PySparkling packages are available, and each is intended to be used with different Spark version.

 - h2o_pysparkling_1.6 - for Spark 1.6.x
 - h2o_pysparkling_1.5 - for Spark 1.5.x
 - h2o_pysparkling_1.4 - for Spark 1.4.x

To install PySparkling for Spark 1.6, for example, the command would look like:

.. code-block:: bash

    pip install h2o_pysparkling_1.6


The Sparkling Water Python Module
---------------------------------

Preparing the Environment
~~~~~~~~~~~~~~~~~~~~~~~~~

1. Either clone and build Sparkling Water project:

 .. code-block:: bash

    git clone http://github.com/h2oai/sparkling-water
    cd sparkling-water
    ./gradlew build -x check

 or download and unpack sparkling water release from  `here <http://www.h2o.ai/download/sparkling-water/choose>`_.

2. Configure the location of the Spark distribution and cluster:

 .. code-block:: bash

    export SPARK_HOME="/path/to/spark/installation"
    export MASTER='local[*]'


Running the PySparkling Interactive Shell
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Ensure you are in the Sparkling Water project directory, and then run the PySparkling shell:

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


Running IPython Notebook with PySparkling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    IPYTHON_OPTS="notebook" bin/pysparkling


Running IPython with PySparkling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    PYSPARK_PYTHON="ipython" bin/pysparkling


Using PySparkling as a Spark Package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

	$SPARK_HOME/bin/spark-submit
	--packages ai.h2o:sparkling-water-core_2.10:1.6.1
	--py-files $SPARKLING_HOME/py/dist/pySparkling-1.6.1-py2.7.egg  ./py/examples/scripts/ChicagoCrimeDemo.py


Using PySparkling in a Databricks Cloud
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In order to use PySparkling in Databricks cloud, the PySparkling module has to be added as a library to the current cluster. PySparkling can be added as library in two ways:

- Upload the PySparkling egg file
- Add the PySparkling module from PyPi

If you choose to upload the PySparkling egg file, don't forget to add libraries for following python modules: request, tabulate and future. The PySparkling egg file is available in the *py/dist* directory in both the built Sparkling Water project and the downloaded Sparkling Water release.

	
