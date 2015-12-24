# Sparkling Water

[![Join the chat at https://gitter.im/h2oai/sparkling-water](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/h2oai/sparkling-water?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![][travis img]][travis]
[![][maven img]][maven]
[![][license img]][license]

[travis]:https://travis-ci.org/h2oai/sparkling-water
[travis img]:https://travis-ci.org/h2oai/sparkling-water.svg?branch=master

[maven]:http://search.maven.org/#search|gav|1|g:"ai.h2o"%20AND%20a:"sparkling-water-core_2.10"
[maven img]:https://maven-badges.herokuapp.com/maven-central/ai.h2o/sparkling-water-core_2.10/badge.svg

[license]:LICENSE
[license img]:https://img.shields.io/badge/License-Apache%202-blue.svg


Sparkling Water integrates H<sub>2</sub>O's fast scalable machine learning engine with Spark. It provides: 
 - utilities to publish Spark data structures (RDDs, DataFrames) as H2O's frames and vice versa. 
 - DSL to use Spark data structures as input for H2O's algorithms
 - basic building blocks to create ML applications utilizing Spark and H2O APIs
 - Python interface enabling use of Sparkling Water directly from pyspark

## Getting Started

<a name="Req"></a>
### Requirements

  * Linux/OS X/Windows
  * Java 7+
  * [Spark 1.3+](https://spark.apache.org/downloads.html)
    * `SPARK_HOME` shell variable must point to your local Spark installation

---
<a name="MakeBuild"></a>
### Build

Download Spark installation and point environment variable `SPARK_HOME` to it.
Then use the provided `gradlew` to build project:

```
./gradlew build
```

> To avoid running tests, use the `-x test -x integTest` option. 

---
<a name="Binary"></a>
### Download Binaries
   * [Sparkling Water - Latest version](http://h2o-release.s3.amazonaws.com/sparkling-water/master/latest.html)
   
  > Note: For each version of Spark there is a corresponding Sparkling Water version (i.e., for Spark 1.5 we provide Sparkling water and Maven artifacts with version 1.5.X) 

---
<a name="SparkShell"></a>
### Run Sparkling shell

The Sparkling shell provides a regular Spark shell that supports creation of an H<sub>2</sub>O cloud and execution of H<sub>2</sub>O algorithms.

First, build a package containing Sparkling water:
```
./gradlew assemble
```

Configure the location of Spark cluster:
```
export SPARK_HOME="/path/to/spark/installation"
export MASTER="local-cluster[3,2,1024]"
```

> In this case, `local-cluster[3,2,1024]` points to embedded cluster of 3 worker nodes, each with 2 cores and 1G of memory.

And run Sparkling Shell:
```
bin/sparkling-shell
```

> Sparkling Shell accepts common Spark Shell arguments. For example, to increase memory allocated by each executor, use the `spark.executor.memory` parameter: `bin/sparkling-shell --conf "spark.executor.memory=4g"`

And initialize H2OContext 
```scala
import org.apache.spark.h2o._
val hc = H2OContext.getOrCreate(sc)
```

> H2OContext start H2O services on top of Spark cluster and provides primitives for transformations between H2O and Spark datastructures.

---

<a name="RunExample"></a>
### Run examples

Build a package that can be submitted to Spark cluster:
```
./gradlew assemble
```

Set the configuration of the demo Spark cluster (for example, `local-cluster[3,2,1024]`)

```
export SPARK_HOME="/path/to/spark/installation"
export MASTER="local-cluster[3,2,1024]"
```
> In this example, the description `local-cluster[3,2,1024]` causes creation of a local cluster consisting of 3 workers.

And run the example:
```
bin/run-example.sh
```

For more details about the demo, please see the [README.md](examples/README.md) file in the [examples directory](examples/).

---

<a name="MoreExamples"></a>
#### Additional Examples
You can find more examples in the [examples folder](examples/).

## Running (Scala) Sparkling Water in IntelliJ IDEA

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
*  In IDEA, Run or Debug SparklingWaterDriver (via right-click)

---
<a name="PySparkling"></a>
## PySparkling
Sparkling Water can be also used directly from pySpark

First, build a package:
```
./gradlew build -x check
```

Configure the location of Spark distribution and cluster:
```
export SPARK_HOME="/path/to/spark/installation"
export MASTER="local-cluster[3,2,1024]"
```

And run pySparkling shell:
```
bin/pysparkling
```

> The `pysparkling` shell accepts common `pyspark` arguments. 

And initialize H2OContext 
```python
import pysparkling
hc = H2OContext(sc).start()
```

> To run `pysparkling` on top of Spark cluster, H2O Python package is required. You can install it via `pip` or point to it via `PYTHONPATH` shell variable: `export PYTHONPATH=$PYTHONPATH:$H2O_HOME/h2o-py`

> To use Python notebook with `pysparkling` you need to specify `IPYTHON_OPTS` shell variable: `IPYTHON_OPTS="notebook" bin/pysparkling`

> To use iPython with `pysparkling` you need to specify `PYSPARK_PYTHON` shell variable: `PYSPARK_PYTHON="ipython" bin/pysparkling`

---
<a name="Packages"></a>
## Sparkling Water as Spark Package
Sparkling Water is also published as a Spark package. 
You can use it directly from your Spark distribution.

For example, if you have Spark version 1.5 and would like to use Sparkling Water version 1.5.2 and launch example `CraigslistJobTitlesStreamingApp`, then you can use the following command:

```bash
$SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 --class org.apache.spark.examples.h2o.CraigslistJobTitlesStreamingApp /dev/null
```
The Spark option `--packages` points to published Sparkling Water packages in Maven repository.


The similar command works for `spark-shell`:
```bash
$SPARK_HOME/bin/spark-shell --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 
```


The same command works for Python programs:
```bash
$SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.10:1.5.2,ai.h2o:sparkling-water-examples_2.10:1.5.2 example.py
```

> Note: When you are using Spark packages you do not need to download Sparkling Water distribution! Spark installation is sufficient!


---  
<a name="Docker"></a>
### Docker Support

See [docker/README.md](docker/README.md) to learn about Docker support.
 
---
<a name="Contrib"></a>
## Contributing

Look at our [list of JIRA tasks](https://0xdata.atlassian.net/issues/?filter=13600) for new contributors or send your idea to [support@h2o.ai](mailto:support@h2o.ai).

---
<a name="Issues"></a>
## Issues 
To report issues, please use our JIRA page at [http://jira.h2o.ai/](http://jira.h2o.ai/).

---
<a name="MailList"></a>
## Mailing list

Follow our [H2O Stream](https://groups.google.com/forum/#!forum/h2ostream).

---

<a name="FAQ"></a>
## FAQ

* Where do I find the Spark logs?
  
 > Spark logs are located in the directory `$SPARK_HOME/work/app-<AppName>` (where `<AppName>` is the name of your application. 
 
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
  export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
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
  
<a name="Diagram"></a>
#Diagram of Sparkling Water on YARN

The following illustration depicts the topology of a Sparkling Water cluster of three nodes running on YARN: 
 ![Diagram](design-doc/images/Sparkling Water cluster.png)
