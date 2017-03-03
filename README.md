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
In external cluster mode we expected that H2O cluster is already running and we connect to it from Spark driver (actually
H2O node in special client mode running in spark driver). 

Few examples how to Sparkling Water in external backend mode.

This piece of code tries to connect to existing h2o cloud with name "h2o-cloud". The H2O cloud is located using multicast
and is assumed that all h2o nodes were started without `-flatfile` option and with `-md5skip` option.
For example like:
```
java -jar h2o.jar -name h2o-cloud -md5skip
```

The following code will not work since specifying flatfile on h2o nodes and not in the H2O configuration ends up with spark not being 
able to connect to the rest of the cloud.
```
java -jar h2o.jar -name h2o-cloud -md5skip -flatfile path_to_flat_file
```


Since multicast communication is often limited in the network, this code connects to H2O cluster "h2o-cluster" using 
direct communication with arbitrary h2o node. Method `setH2OCluster` automatically sets external backend mode. It is internally 
using [H2O's flatfile configuration property](https://github.com/h2oai/h2o-3/blob/master/h2o-docs/src/product/howto/H2O-DevCmdLine.md#flatfile).

This is also why all H2O nodes has to be started with `-flatfile` option and at least one node has to have ip and port of h2o
client in it's flatfile. We can also specify the IP address of h2o client using method `setClientIP` and then use the provided
ip address in the flatfile mentioned earlier.

```
val conf = new H2OConf(sc).setH2OCluster(host, port).setClientIp(ip).setCloudName("h2o-cloud")
val h2oContext = H2OContext.getOrCreate(sc, conf)
```


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
  
