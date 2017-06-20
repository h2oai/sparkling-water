# Sparkling Water Development Documentation

## Table of Contents
- [Typical Use Case](#UseCase)
- [Requirements](#Req)
- [Design](#Design)
- [Features](#Features)
  - [Supported Data Sources](#DataSource)
  - [Supported Data Formats](#DataFormat)
  - [Data Sharing](#DataShare)
  - [Provided Primitives](#ProvPrim)
- [Running on Select Target Platforms](#TargetPlatforms)
  - [Local](#Local)
  - [Standalone](#Standalone)
  - [YARN](#YARN)
  - [Mesos](#Mesos)
- [H2O Initialization Sequence](#H2OInit)
  - [Configuration](#Config)
    - [Build Environment](#BuildEnv)
    - [Run Environment](#RunEnv)
    - [Sparkling Water Configuration Properties](doc/configuration_properties.rst)
- [Running Sparkling Water](#RunSW)
  - [Starting H2O Services](#StartH2O)
  - [Memory Allocation](#MemorySetup)
  - [Security](doc/security.rst)
  - [Spark Frame - H2O Frame Conversions](doc/spark_h2o_conversions.rst)
  - [Creating H2OFrame from an existing key](doc/h2o_frame_from_key.rst)
  - [Calling H2O Algorithms](doc/calling_h2o_algos.rst)
  - [Spark Frame - H2O Frame Mapping](doc/spark_h2o_mapping.rst)
- Testing
    - [Running Unit Tests](doc/unit_tests.rst)
    - [Integration Tests](doc/integ_tests.rst)
- [Sparkling Water Log Locations](doc/log_location.rst)
- [Change Sparkling Shell Logging Level](doc/change_log_level.rst)
- [H2O Frame as Spark's Data Source](doc/datasource.rst)
- [Sparkling Water Tuning](doc/internal_backend_tuning.rst)
- [Sparkling Water and Zeppelin](doc/zeppelin.rst)

--- 
 
 <a name="UseCase"></a>
## Typical Use-Case
Sparkling Water excels in leveraging existing Spark-based workflows that need to call advanced machine learning algorithms. A typical example involves data munging with help of Spark API, where a prepared table is passed to the H2O DeepLearning algorithm. The constructed DeepLearning model estimates different metrics based on the testing data, which can be used in the rest of the Spark workflow.

---

<a name="Req"></a>
## Requirements
 - Linux or Mac OSX platform
 - Java 1.7+
 - [Spark 1.6.0+](http://spark.apache.org/downloads.html)

---

<a name="Design"></a>
## Design

Sparkling Water is designed to be executed as a regular Spark application.
It provides a way to initialize H2O services on each node in the Spark cluster and access
data stored in data structures of Spark and H2O.

Since Sparkling Water is designed as Spark application, it is launched 
inside a Spark executor, which is created after application submission. 
At this point, H2O starts services, including distributed KV store and memory manager,
and orchestrates them into a cloud. The topology of the created cloud matches the topology of the underlying Spark cluster exactly.

 ![Topology](design-doc/images/Sparkling Water cluster.png)

When H2O services are running, it is possible to create H2O data structures, call H2O algorithms, and transfer values from/to RDD.

---

<a name="Features"></a>
## Features

Sparkling Water provides transparent integration for the H2O engine and its machine learning 
algorithms into the Spark platform, enabling:
 * use of H2O algorithms in Spark workflow
 * transformation between H2O and Spark data structures
 * use of Spark RDDs as input for H2O algorithms
 * transparent execution of Sparkling Water applications on top of Spark


<a name="DataSource"></a> 
### Supported Data Sources
Currently, Sparkling Water can use the following data source types:
 - standard RDD API to load data and transform them into `H2OFrame`
 - H2O API to load data directly into `H2OFrame` from:
   - local file(s)
   - HDFS file(s)
   - S3 file(s)

---
<a name="DataFormat"></a>   
### Supported Data Formats
Sparkling Water can read data stored in the following formats:

 - CSV
 - SVMLight
 - ARFF


---
<a name="DataShare"></a>
### Data Sharing
Sparkling Water enables transformation between different types of Spark `RDD` and H2O's `H2OFrame`, and vice versa.

 ![Data Sharing](design-doc/images/DataShare.png)

When converting from `H2OFrame` to `RDD`, a wrapper is created around the `H2OFrame` to provide an RDD-like API. In this case, no data is duplicated; instead, the data is served directly from then underlying `H2OFrame`.

Converting in the opposite direction (i.e, from Spark `RDD`/`DataFrame` to `H2OFrame`) needs evaluation of data stored in Spark `RDD` and transfer them from RDD storage into `H2OFrame`. However, data stored in `H2OFrame` is heavily compressed. 

<!--TODO: estimation of overhead -->
----
<a name="ProvPrim"></a>
### Provided Primitives
The Sparkling Water provides following primitives, which are the basic classes used by Spark components:


| Concept        | Implementation class              | Description |
|----------------|-----------------------------------|-------------|
| H2O context    | `org.apache.spark.h2o.H2OContext` | H2O context that holds H2O state and provides primitives to transfer RDD into H2OFrame and vice versa. It follows design principles of Spark primitives such as `SparkContext` or `SQLContext` |
| H2O entry point| `water.H2O`                       | Represents the entry point for accessing H2O services. It holds information about the actual H2O cluster, including a list of nodes and the status of distributed K/V datastore. |
| H2O H2OFrame  | `water.fvec.H2OFrame`            | H2OFrame is the H2O data structure that represents a table of values. The table is column-based and provides column and row accessors. |
| H2O Algorithms | package `hex`                     | Represents the H2O machine learning algorithms library, including DeepLearning, GBM, RandomForest. |
 

---

<a name="TargetPlatforms"></a>
# Running on Select Target Platforms

Sparkling Water can run on top of Spark in the various ways described in the following sections.

<a name="Local"></a>
## Local
In this case Sparkling Water runs as a local cluster (Spark master variable points to one of values `local`, `local[*]`, or `local-cluster[...]`

<a name="Standalone"></a>
## Standalone
[Spark documentation - running Standalone cluster](http://spark.apache.org/docs/latest/spark-standalone.html)

<a name="YARN"></a>
## YARN
[Spark documentation - running Spark Application on YARN](http://spark.apache.org/docs/latest/running-on-yarn.html)

When submitting Sparkling Water application to CHD or Apache Hadoop cluster, the command to submit may look like:
```
./spark-submit --master=yarn-client --class water.SparklingWaterDriver --conf "spark.yarn.am.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=current"
--driver-memory=8G --num-executors=3 --executor-memory=3G --conf "spark.executor.extraClassPath=-XX:MaxPermSize=384m -Dhdp.version=current"
sparkling-water-assembly-1.5.11-all.jar
```

When submitting sparkling water application to HDP Cluster, the command to submit may look like:
```
./spark-submit --master=yarn-client --class water.SparklingWaterDriver --conf "spark.yarn.am.extraJavaOptions=-XX:MaxPermSize=384m -Dhdp.version=current"
--driver-memory=8G --num-executors=3 --executor-memory=3G --conf "spark.executor.extraClassPath=-XX:MaxPermSize=384m -Dhdp.version=current"
sparkling-water-assembly-1.5.11-all.jar
```
Apart from the typical spark configuration it is necessary to add `-XX:MaxPermSize=384m` (or higher, but 384m is minimum) to both `spark.executor.extraClassPath` and `spark.yarn.am.extraJavaOptions` (or for client mode, `spark.driver.extraJavaOptions` for cluster mode) configuration properties in order to run Sparkling Water correctly.

The only difference between HDP cluster and both CDH and Apache hadoop clusters is that we need to add `-Dhdp.version=current` to both `spark.executor.extraClassPath` and `spark.yarn.am.extraJavaOptions` (resp., `spark.driver.extraJavaOptions`) configuration properties in the HDP case.

<a name="Mesos"></a>
## Mesos
[Spark documentation - running Spark Application on Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html)


---
<a name="H2OInit"></a>
# H2O Initialization Sequence
If `SparkContext` is available, initialize and start H2O context: 
```scala
val sc:SparkContext = ...
val hc = H2OContext.getOrCreate(sc)
```

The call will:
 1. Collect the number and host names of the executors (worker nodes) in the Spark cluster
 2. Launch H2O services on each detected executor
 3. Create a cloud for H2O services based on the list of executors
 4. Verify the H2O cloud status

The former variant is preferred, because it initiates and starts H2O Context in one call and also can be used to obtain already existing H2OContext, but it does semantically the same as the latter variant.

---
<a name="Config"></a>
## Configuration

<a name="BuildEnv"></a>
### Build Environment
The environment must contain the property `SPARK_HOME` that points to the Spark distribution.

---

<a name="RunEnv"></a>
### Run Environment
The environment must contain the property `SPARK_HOME` that points to the Spark distribution.

---

<a name="RunSW"></a>
# Running Sparkling Water

---

<a name="StartH2O"></a>
### Starting H2O Services
```scala
val sc:SparkContext = ...
val hc = H2OContext.getOrCreate(sc)
```

---
<a name="MemorySetup"></a>
### Memory Allocation 

H2O resides in the same executor JVM as Spark. The memory provided for H2O is configured via Spark; refer to [Spark configuration](http://spark.apache.org/docs/1.4.0/configuration.html) for more details.

#### Generic configuration
 * Configure the Executor memory (i.e., memory available for H2O) via the Spark configuration property `spark.executor.memory` .
     > For example, `bin/sparkling-shell --conf spark.executor.memory=5g` or configure the property in `$SPARK_HOME/conf/spark-defaults.conf`
     
 * Configure the Driver memory (i.e., memory available for H2O client running inside Spark driver) via the Spark configuration property `spark.driver.memory`
     > For example, `bin/sparkling-shell --conf spark.driver.memory=4g` or configure the property in `$SPARK_HOME/conf/spark-defaults.conf`
      
#### Yarn specific configuration
* Refer to the [Spark documentation](http://spark.apache.org/docs/1.4.0/running-on-yarn.html)

* For JVMs that require a large amount of memory, we strongly recommend configuring the maximum amount of memory available for individual mappers. For information on how to do this using Yarn, refer to http://docs.h2o.ai/deployment/hadoop_yarn.html

