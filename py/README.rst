Setup and Installation
======================

Prerequisites:
    
    - Python 2.7
    - Numpy 1.9.2

  For windows users, please grab a .whl from http://www.lfd.uci.edu/~gohlke/pythonlibs/#numpy

This module depends on *requests* and *tabulate* modules, both of which are available on pypi:

    $ pip install requests

    $ pip install tabulate

The Sparkling-Water Python Module
=====================
To run a pySparkling interactive shell:
    
    export SPARK_HOME="/path/to/spark/installation"
    
    export MASTER='local-cluster[3,2,2040]'
    
    export SPARKLING_HOME="/path/to/SparklingWater/installation"
    
    $SPARKLING_HOME/bin/pysparkling

On a notebook
    
    IPYTHON_OPTS="notebook" $SPARKLING_HOME/bin/pysparkling
On YARN
    
    export SPARK_HOME="/path/to/spark/installation"
    
    export HADOOP_CONF_DIR=/etc/hadoop/conf
    
    export SPARKLING_HOME="/path/to/SparklingWater/installation"
    
    $SPARKLING_HOME/bin/pysparkling --num-executors 3 --executor-memory 20g --executor-cores 10 --driver-memory 20g --master yarn-client
    
To initialize H2O context and import H2O-Python library-
    
    from pysparkling import *
    
    hc= H2OContext(sc).start()
    
    import h2o

To run as a Spark Package-
	
	$SPARK_HOME/bin/spark-submit 
	--packages ai.h2o:sparkling-water-core_2.10:1.5.10  
	--py-files $SPARKLING_HOME/py/dist/pySparkling-1.5.10-py2.7.egg  $SPARKLING_HOME/py/examples/scripts/H2OContextDemo.py 

    
