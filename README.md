# Sparkling Water

[![Join the chat at https://gitter.im/h2oai/sparkling-water](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/h2oai/sparkling-water?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![][travis img]][travis]
[![][maven img]][maven]
[![][license img]][license]
[![Powered by H2O.ai](https://img.shields.io/badge/powered%20by-h2oai-yellow.svg)](https://github.com/h2oai/)

[travis]:https://travis-ci.org/h2oai/sparkling-water
[travis img]:https://travis-ci.org/h2oai/sparkling-water.svg?branch=master

[maven]:http://search.maven.org/#search|gav|1|g:"ai.h2o"%20AND%20a:"sparkling-water-core_2.11"
[maven img]:https://maven-badges.herokuapp.com/maven-central/ai.h2o/sparkling-water-core_2.11/badge.svg

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg


Sparkling Water integrates H<sub>2</sub>O's fast scalable machine learning engine with Spark. It provides: 
 - Utilities to publish Spark data structures (RDDs, DataFrames) as H2O's frames and vice versa. 
 - DSL to use Spark data structures as input for H2O's algorithms
 - Basic building blocks to create ML applications utilizing Spark and H2O APIs
 - Python interface enabling use of Sparkling Water directly from pySpark

## Getting Started

### Select right version
The Sparkling Water is developed in multiple parallel branches.
Each branch corresponds to a Spark major release (e.g., branch **rel-1.5** provides implementation of Sparkling Water for Spark **1.5**).

Please, switch to the right branch:
 - For Spark 2.1 use branch [rel-2.1](https://github.com/h2oai/sparkling-water/tree/rel-2.1)
 - For Spark 2.0 use branch [rel-2.0](https://github.com/h2oai/sparkling-water/tree/rel-2.0)
 - For Spark 1.6 use branch [rel-1.6](https://github.com/h2oai/sparkling-water/tree/rel-1.6)

> **Note** Older releases are available here:
>  - For Spark 1.5 use branch [rel-1.5](https://github.com/h2oai/sparkling-water/tree/rel-1.5)
>  - For Spark 1.4 use branch [rel-1.4](https://github.com/h2oai/sparkling-water/tree/rel-1.4)
>  - For Spark 1.3 use branch [rel-1.3](https://github.com/h2oai/sparkling-water/tree/rel-1.3)

> **Note** The [master](https://github.com/h2oai/sparkling-water/tree/master) branch includes the latest changes
for the latest Spark version. They are back-ported into older Sparkling Water versions.

<a name="Req"></a>
### Requirements

  * Linux/OS X/Windows
  * Java 7+
  * [Spark 1.6+](https://spark.apache.org/downloads.html)
    * `SPARK_HOME` shell variable must point to your local Spark installation

---
<a name="MakeBuild"></a>
### Build

Download Spark installation and point environment variable `SPARK_HOME` to it.
> Before building this project, you may want to build Spark in case you are using Spark source distribution: go to Spark folder and do `sbt assembly`.

Then use the provided `gradlew` to build project:

In order to build the whole project inlucding Python module, one of the following properties needs to be set: 
`H2O_HOME`, which should point to location of local h2o project directory, or `H2O_PYTHON_WHEEL`, which should point to H2O Python Wheel.

If you are not sure which property to set, just run
```
./gradlew build
```
and the commands which sets the `H2O_PYTHON_WHEEL` will be shown on your console and can be copy-pasted into your terminal. After setting the property, the build needs to be rerun.

Now, to have everything you need for Python, you may need to install Python `future` library, via `pip install future`.
First, you may need to install pip. See http://stackoverflow.com/questions/17271319/installing-pip-on-mac-os-x
Use brew package manager or
```
curl https://bootstrap.pypa.io/ez_setup.py -o - | sudo python
sudo easy_install pip
pip install future
```

> To avoid running tests, use the `-x test -x integTest` or `-x check` option. 

> To build only a specific module, use, for example, `./gradlew :sparkling-water-examples:build`.
> To build and test a specific module, use, for example,  `./gradlew :sparkling-water-examples:check`.

---
<a name="Binary"></a>
### Download Binaries
For each Sparkling Water you can download binaries here:
   * [Sparkling Water - Latest version](http://h2o-release.s3.amazonaws.com/sparkling-water/master/latest.html)
   * [Sparkling Water - Latest 2.1 version](http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/latest.html)
   * [Sparkling Water - Latest 2.0 version](http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.0/latest.html)
   * [Sparkling Water - Latest 1.6 version](http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.6/latest.html)
   * [Sparkling Water - Latest 1.5 version](http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.5/latest.html)
   * [Sparkling Water - Latest 1.4 version](http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.4/latest.html)
   * [Sparkling Water - Latest 1.3 version](http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.3/latest.html)

---
### Maven
Each Sparkling Water release is published into Maven central. Right now we publish artifacts only for Scala 2.10 and Scala 2.11.

The artifacts coordinates are:
 - `ai.h2o:sparkling-water-core_{{scala_version}}:{{version}}` - includes core of Sparkling Water.
 - `ai.h2o:sparkling-water-examples_{{scala_version}}:{{version}}` - includes example applications.

> Note: The `{{version}}` reference to a release version of Sparkling Water, the `{{scala_version}}` references to Scala base version (`2.10` or `2.11`).
> For example: `ai.h2o:sparkling-water-examples_2.11:2.0.0`

The full list of published packages is available [here](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ai.h2o%22%20AND%20a%3Asparkling-water*).

---
## Use Sparkling Water

Sparkling Water is distributed as a Spark application library which can be used by any Spark application. 
Furthermore, we provide also zip distribution which bundles the library and shell scripts.

There are several ways of using Sparkling Water:
  - Sparkling Shell
  - Sparkling Water driver
  - Spark Shell and include Sparkling Water library via `--jars` or `--packages` option
  - Spark Submit and include Sparkling Water library via `--jars` or `--packages` option
  - pySpark with pySparkling

---

## Sparkling Water cluster backends

Sparkling water supports two backend/deployment modes. We call them internal and external back-ends.
Sparkling Water applications are independent on selected backend, the before `H2OContext` is created
we need to tell it which backend used.

### Internal backend
In internal backend, H2O cloud is created automatically during the call of `H2OContext.getOrCreate`. Since it's not
technically possible to get number of executors in Spark, we try to discover all executors at the initiation of `H2OContext`
and we start H2O instance inside of each discovered executor. This solution is easiest to deploy; however when Spark
or YARN kills the executor - which is not an unusual case - the whole H2O cluster goes down since h2o doesn't support high 
availability. 


Internal backend is default for behaviour for Sparkling Water. It can be changed via spark configuration property
`spark.ext.h2o.backend.cluster.mode` to `external` or `internal`. Another way how to change type of backend is by calling
 `setExternalClusterMode()` or `setInternalClusterMode()` method on `H2OConf` class. `H2OConf` is simple wrapper 
around `SparkConf` and inherits all properties in spark configuration.

Here we show a few examples how H2OContext can be started with internal backend.

Explicitly specify internal backend on `H2OConf`
```scala
val conf = new H2OConf(sc).setInternalClusterMode()
val h2oContext = H2OContext.getOrCreate(sc, conf)
```

If `spark.ext.h2o.backend.cluster.mode` property was set to `internal` either on command line or on the `SparkConf` class
 we can call:
```scala
val h2oContext = H2OContext.getOrCreate(sc) 
```

or

```scala
val conf = new H2OConf(sc)
val h2oContext = H2OContext.getOrCreate(sc, conf)
```


 
### External backend
In external cluster we use H2O cluster running separately from the rest of Spark application. This separation gives us more stability since we are no longer
affected by Spark executors being killed, which can lead as in previous mode to h2o cloud kill as well.

There are 2 deployment strategies of external cluster: manual and automatic. In manual mode, we need to start H2O cluster and in automatic mode, 
the cluster isstarted for us automatically based on our configuration. 
In both modes, we can't use regular H2O/H2O driver jar as main artifact for external 
H2O cluster, but we need to extend it by classes required by Sparkling Water.
Users are expected to extend the H2O/H2O driver jar and build the artifacts on their own using a few simple steps mentioned bellow.

Before you start, please build and clone Sparkling Water. Sparkling Water can be built using Gradle as `./gradlew build -x check`.

#### Extending H2O jar
In order to extend H2O/H2O driver jar file, Sparkling Water build process has Gradle task `extendJar` which can be configured in various ways. 
 
The recommended way for the user is to call `./gradlew extendJar -PdownloadH2O`. The `downloadH2O` Gradle command line argument tels the task to
download correct h2o version for current sparkling water from our repository automatically. The downloaded jar is cached for future calls.
If `downloadH2O` is run without any argument, then basic H2O jar is downloaded, but `downloadH2O` command takes also argument specifying hadoop version,
such as cdh5.4 or hdp2.2. In case the argument is specified, for example as `downloadH2O=cdh5.4`, instead of downloading H2O jar, H2O driver jar for specified hadoop version will be downloaded.

The location of H2O/H2O driver jar to be extended can also be set manually via `H2O_ORIGINAL_JAR` environmental variable. No work is done if the file is not available
on this location or if it's not correct H2O or H2O driver jar.
  
Gradle property has higher priority if both `H2O_ORIGINAL_JAR` and `-PdownloadH2O` are set
 
Here is a few few examples how H2O/H2O driver jar can be extended:

1)
In this case the jar to be extended is located using the provided environment variable.

```
export H2O_ORIGINAL_JAR = ...
./gradlew extendJar
```


2)
In this case the jar to be extended is H2O jar and the correct version is downloaded from our repository first.

```
./gradlew extendJar -PdownloadH2O
```

3)
In this case the jar to be extended is H2O driver jar for provided hadoop version and is downloaded from our repository first

```
./gradlew extendJar -PdownloadH2O=cdh5.4
```

4)
This case will throw an exception since such hadoop version is not supported.

```
./gradlew extendJar -PdownloadH2O=abc
```

5)
This version will ignore environment variable and jar to be extended will be downloaded from our repository. The same
holds for version with the argument.

```
export H2O_ORIGINAL_JAR = ...
./gradlew extendJar -PdownloadH2O
```



The `extendJar` tasks also prints a path to the extended jar. It can be saved to environmental variable as
```
export H2O_EXTENDED_JAR=`./gradlew -q extendJar -PdownloadH2O`
```


Now we have the the jar file for either regular H2O or H2O driver ready!
 
The following sections explain how to use external cluster in both modes.
Let's assume for later sections that the location of extended H2O/H2O driver jar file is available in `H2O_EXTENDED_JAR` environmental variable.

#### Manual mode of External backend
We need to start H2O cluster before connecting to it manually.
In general, H2O cluster can be started in 2 ways - using the multicast discovery of the other nodes and using the flatfile,
where we manually specify the future locations
of H2O nodes. We recommend to use flatfile to specify the location of nodes for production usage of Sparkling Water, but in simple environments where
multicast is supported the multicast discovery should work as well.

Let's have a look on how to start H2O cluster and connect to it from Sparkling Water in multicast environment.
To start H2O cluster of 3 nodes, run the following line 3 times: 
```
java -jar $H2O_EXTENDED_JAR -md5skip -name test
```

Don't forget the `-md5skip` argument, it's additional argument required for external cluster to work.

After this step, we should have H2O cluster of 3 nodes running and the nodes should have discovered each other using the multicast discovery.

Now, let's start Sparkling shell first as `./bin/sparkling-shell` and connect to the cluster:
```scala
import org.apache.spark.h2o._
val conf = new H2OConf(sc).setExternalClusterMode().useManualClusterStart().setCloudName("test”)
val hc = H2OContext.getOrCreate(sc, conf)
```

To connect to existing H2O cluster from Python, start PySparkling shell as `./bin/pysparkling` and do:
```python
from pysparkling import *
conf = H2OConf(sc).set_external_cluster_mode().use_manual_cluster_start().set_cloud_name("test”)
hc = H2OContext.getOrCreate(sc, conf)
```

To start external H2O cluster where the nodes are discovered using the flatfile, you can run
```
java -jar $H2O_EXTENDED_JAR -md5skip -name test -flatfile path_to_flatfile
```
, where the flatfile should contain lines in format ip:port of nodes where H2O is supposed to run. To read more about flatfile and it's format, please see [H2O's flatfile configuration property](https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/H2O-DevCmdLine.md#flatfile).


To connect to this external cluster, run the following commands in the corresponding shell ( Sparkling in case of Scala, PySparkling in case of Python):

Scala:
```scala
import org.apache.spark.h2o._
val conf = new H2OConf(sc).setExternalClusterMode().useManualClusterStart().setH2OCluster("representant_ip", representant_port).setCloudName("test”)
val hc = H2OContext.getOrCreate(sc, conf)
```

Python:
```python
from pysparkling import *
conf = H2OConf(sc).set_external_cluster_mode().use_manual_cluster_start().set_h2o_cluster("representant_ip", representant_port).set_cloud_name("test”)
hc = H2OContext.getOrCreate(sc, conf)
```
We can see that in this case we are using extra call `setH2OCluster` in Scala and `set_h2o_cluster` in Python. When the external cluster is started via the flatfile approach, we need to give Sparkling Water ip address
and port of arbitrary node inside the H2O cloud in order to connect to the cluster. The ip and port of this node are passed as arguments to `setH2OCluster/set_h2o_cluster` method.

It's possible in both cases that node on which want to start Sparkling-Shell is connected to more networks. In this case it can happen that H2O
cloud decides to use addresses from network A, whilst Spark decides to use addresses for its executors and driver from network B. Later, when we start `H2OContext`,
the special H2O client, running inside of the Spark Driver, can get the same IP address as the Spark driver and thus the rest of the H2O cloud can't see it. This shouldn't happen in 
environments where the nodes are connected to only one network, however we provide configuration how to deal with this case as well.

We can use method `setClientIp` in Scala and `set_client_ip` in python available on `H2OConf` which expects IP address and sets this IP address for the H2O client running inside the Spark driver. The IP address
passed to this method should be address of the node where Spark driver is about to run and should be from the same network as the rest of H2O cloud. 

Let's say we have two H2O nodes on addresses 192.168.0.1 and 192.168.0.2 and also assume that Spark driver is available on 172.16.1.1 
and the only executor is available on 172.16.1.2. The node with Spark driver is also connected to 192.168.0.x network with address 192.168.0.3.

In this case there is a chance that H2O client will use the address from 172.168.x.x network instead of the 192.168.0.x one, which can lead
to the problem that H2O cloud and H2O client can't see each other.

We can force the client to use the correct address using the following configuration:

Scala:
```scala
import org.apache.spark.h2o._
val conf = new H2OConf(sc).setExternalClusterMode().useManualClusterStart().setH2OCluster("representant_ip", representant_port).setClientIp("192.168.0.3").setCloudName("test”)
val hc = H2OContext.getOrCreate(sc, conf)
```

Python:
```python
from pysparkling import *
conf = H2OConf(sc).set_external_cluster_mode().use_manual_cluster_start().set_h2o_cluster("representant_ip", representant_port).set_client_ip("192.168.0.3").set_cloud_name("test”)
hc = H2OContext.getOrCreate(sc, conf)
```

There is also less strict configuration `setClientNetworkMask` in Scala and `set_client_network_mask` in Python. Instead of it's IP address equivalent, using this
method we can force H2O client to use just specific network and let up to the client which IP address from this network to use.

The same configuration can be applied when the H2O cluster has been started via multicast discovery.

#### Automatic mode of External backend
In automatic mode, H2O cluster is started automatically. The cluster can be started automatically only in YARN environment at the moment. We recommend
this approach as it is easier to deploy external cluster in this mode ans it is also more suitable for production environments.
When H2O cluster is start on YARN, it is started as map reduce job and it always use the flatfile approach for nodes to cloud
up.

For this case to work, we need to extend H2O driver for the desired hadoop version as mentioned above. Let's assume the path to this extended H2O driver is stored in
`H2O_EXTENDED_DRIVER` environmental property.

To start H2O cluster and connect to it from Spark application in Scala:

```scala
import org.apache.spark.h2o._
val conf = new H2OConf(sc).setExternalClusterMode().useAutoClusterStart().setH2ODriverPath("path_to_extended_driver").setNumOfExternalH2ONodes(1).setMapperXmx("2G").setYARNQueue("abc")
val hc = H2OContext.getOrCreate(sc, conf)
```

and in Python:
```python
from pysparkling import *
conf = H2OConf(sc).set_external_cluster_mode().use_auto_cluster_start().set_h2o_driver_path("path_to_extended_driver").set_num_of_external_h2o_nodes(1).set_mapper_xmx("2G”).set_yarn_queue(“abc”)`
hc = H2OContext.getOrCreate(sc, conf)
```

In both cases we can see various configuration methods. We explain only the Scala ones since the python equivalents are doing exactly the same.

* `setH2ODriverPath` method is used to tell Sparkling Water where it can find the extended H2O driver jar. This jar is passed to hadoop and used to start H2O cluster on YARN.
* `setNumOfExternalH2ONodes` method specifies how many H2O nodes we want to start.
* `setMapperXmx` method specifies how much memory each H2O node should have available.
* `setYarnQueue` method specifies YARN queue on which H2O cluster will be started. We highly recommend that this queue should have YARN preemption off in order to have stable H2O cluster.

When using `useAutoClusterStart` we do not need to call `setH2ODriverPath` explicitly in case when `H2O_EXTENDED_DRIVER` environmental property is set and pointing to that file.
In this case Sparkling Water will fetch the path from this variable automatically. Also when `setCloudName` is not called, the name is set automatically and H2O cluster with that name is started.

It can also happen that we might need to use `setClientIp/set_client_ip` method as mentioned in the chapter above for the same reasons. The usage of this method in automatic mode is exactly the
as in the manual mode.

<a name="SparkShell"></a>
### Run Sparkling shell

The Sparkling shell encapsulates a regular Spark shell and append Sparkling Water library on the classpath via `--jars` option. 
The Sparkling Shell supports creation of an H<sub>2</sub>O cloud and execution of H<sub>2</sub>O algorithms.

1. First, build a package containing Sparkling water:
  ```
  ./gradlew assemble
  ```

2. Configure the location of Spark cluster:
  ```
  export SPARK_HOME="/path/to/spark/installation"
  export MASTER="local[*]"
  ```

  > In this case, `local[*]` points to an embedded single node cluster.

3. Run Sparkling Shell:
  ```
  bin/sparkling-shell
  ```

  > Sparkling Shell accepts common Spark Shell arguments. For example, to increase memory allocated by each executor, use the `spark.executor.memory` parameter: `bin/sparkling-shell --conf "spark.executor.memory=4g"`

4. Initialize H2OContext 
  ```scala
  import org.apache.spark.h2o._
  val hc = H2OContext.getOrCreate(sc)
  ```

  > H2OContext start H2O services on top of Spark cluster and provides primitives for transformations between H2O and Spark datastructures.

---

<a name="RunExample"></a>
### Run examples
The Sparkling Water distribution includes also a set of examples. You can find there implementation in [example folder](example/). 
You can run them in the following way:

1. Build a package that can be submitted to Spark cluster:
  ```
  ./gradlew assemble
  ```

2. Set the configuration of the demo Spark cluster (for example, `local[*]` or `local-cluster[3,2,1024]`)
  ```
  export SPARK_HOME="/path/to/spark/installation"
  export MASTER="local[*]"
  ```
  > In this example, the description `local[*]` causes creation of a single node local cluster.

3. And run the example:
  ```
  bin/run-example.sh
  ```

For more details about examples, please see the [README.md](examples/README.md) file in the [examples directory](examples/).

<a name="MoreExamples"></a>
#### Additional Examples
You can find more examples in the [examples folder](examples/).

---
<a name="PySparkling"></a>
### Run PySparkling
Sparkling Water can be also used directly from PySpark.
 
See [py/README.md](py/README.rst) to learn about PySparkling.

---
<a name="Packages"></a>
### Use Sparkling Water via Spark Packages
Sparkling Water is also published as a Spark package. 
You can use it directly from your Spark distribution.

For example, if you have Spark version 2.0 and would like to use Sparkling Water version 2.0.0 and launch example `CraigslistJobTitlesStreamingApp`, then you can use the following command:

```bash
$SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.11:2.0.0,ai.h2o:sparkling-water-examples_2.11:2.0.0 --class org.apache.spark.examples.h2o.CraigslistJobTitlesStreamingApp /dev/null
```
The Spark option `--packages` points to published Sparkling Water packages in Maven repository.


The similar command works for `spark-shell`:
```bash
$SPARK_HOME/bin/spark-shell --packages ai.h2o:sparkling-water-core_2.11:2.0.0,ai.h2o:sparkling-water-examples_2.11:2.0.0
```


The same command works for Python programs:
```bash
$SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.11:2.0.0,ai.h2o:sparkling-water-examples_2.11:2.0.0 example.py
```

> Note: When you are using Spark packages you do not need to download Sparkling Water distribution! Spark installation is sufficient!

---  

<a name="Docker"></a>
### Docker Support

See [docker/README.md](docker/README.md) to learn about Docker support.

---

## Develop with Sparkling Water

### Setup Sparkling Water in IntelliJ IDEA

*  In IDEA, install the Scala plugin for IDEA
*  In a Terminal:

```
git clone https://github.com/h2oai/sparkling-water.git
cd sparkling-water
./gradlew idea
open sparkling-water.ipr
```

*  In IDEA, open the file sparkling-water/core/src/main/scala/water/SparklingWaterDriver.scala
*  [ Wait for IDEA indexing to complete so the Run and Debug choices are available ]
*  In IDEA, _Run_ or _Debug_ SparklingWaterDriver (via right-click)

### Develop applications with Sparkling Water

An application using Sparkling Water is regular Spark application which bundling Sparkling Water library.
See Sparkling Water Droplet providing an example application [here](https://github.com/h2oai/h2o-droplets/tree/master/sparkling-water-droplet).


---
<a name="Contrib"></a>
## Contributing

Look at our [list of JIRA tasks](https://0xdata.atlassian.net/issues/?filter=13600) for new contributors or send your idea to [support@h2o.ai](mailto:support@h2o.ai).

---
<a name="Issues"></a>
## Issues 
To report issues, please use our JIRA page at [http://jira.h2o.ai/](https://0xdata.atlassian.net/projects/SW/issues).

---
<a name="MailList"></a>
## Mailing list

Follow our [H2O Stream](https://groups.google.com/forum/#!forum/h2ostream).

---

<a name="FAQ"></a>
## FAQ

* Where do I find the Spark logs?
  
 > **Standalone mode**: Spark executor logs are located in the directory `$SPARK_HOME/work/app-<AppName>` (where `<AppName>` is the name of your application). The location contains also stdout/stderr from H2O.
 
 > **YARN mode**: The executors logs are available via `yarn logs -applicationId <appId>` command. Driver logs are by default printed to console, however, H2O also writes logs into `current_dir/h2ologs`.
 
 > The location of H2O driver logs can be controlled via Spark property `spark.ext.h2o.client.log.dir` (pass via `--conf`) option.
 
* Spark is very slow during initialization or H2O does not form a cluster. What should I do?
  
 > Configure the Spark variable `SPARK_LOCAL_IP`. For example: 
  ```
  export SPARK_LOCAL_IP='127.0.0.1'
  ```  

* How do I increase the amount of memory assigned to the Spark executors in Sparkling Shell?
 
 > Sparkling Shell accepts common Spark Shell arguments. For example, to increase
 > the amount of memory allocated by each executor, use the `spark.executor.memory`
 > parameter: `bin/sparkling-shell --conf "spark.executor.memory=4g"`

* How do I change the base port H2O uses to find available ports?
  
  > The H2O accepts `spark.ext.h2o.port.base` parameter via Spark configuration properties: `bin/sparkling-shell --conf "spark.ext.h2o.port.base=13431"`. For a complete list of configuration options, refer to [Devel Documentation](https://github.com/h2oai/sparkling-water/blob/master/DEVEL.md#sparkling-water-configuration-properties).

* How do I use Sparkling Shell to launch a Scala `test.script` that I created?

 > Sparkling Shell accepts common Spark Shell arguments. To pass your script, please use `-i` option of Spark Shell:
 > `bin/sparkling-shell -i test.script`

* How do I increase PermGen size for Spark driver?

 > Specify `--conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m"`
 
* How do I add Apache Spark classes to Python path?
 
 > Configure the Python path variable `PYTHONPATH`:
  ```
  export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH
  export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.9-src.zip:$PYTHONPATH
  ```

* Trying to import a class from the `hex` package in Sparkling Shell but getting weird error:
  ```
  error: missing arguments for method hex in object functions;
  follow this method with '_' if you want to treat it as a partially applied
  ```

  > In this case you are probably using Spark 1.5 which is importing SQL functions
    into Spark Shell environment. Please use the following syntax to import a class 	from the `hex` package:
    ```
    import _root_.hex.tree.gbm.GBM
    ```
  
