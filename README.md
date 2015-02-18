# Sparkling Water

- [Requirements](#Req)
- [Contributing](#Contrib)
- [Issues](#Issues)
- [Mailing List](#MailList)
- [Binary downloads](#Binary)
- [Making a build](#MakeBuild)
- [Sparkling Shell](#SparkShell)
- [Running Examples](#RunExample)
- [Additional Examples](#MoreExamples)
- [Docker Support](#Docker)
- [FAQ](#FAQ)



Sparkling Water integrates H<sub>2</sub>O's fast scalable machine learning engine with Spark.

<a name="Req"></a>
## Requirements

  * Linux or OS X (Windows support is pending)
  * Java 7
  * [Spark 1.2.0](https://spark.apache.org/downloads.html)
    * `SPARK_HOME` shell variable must point to your local Spark installation
 
---
<a name="Contrib"></a>
## Contributing


Look at our [list of JIRA tasks](https://0xdata.atlassian.net/issues/?filter=13600) for new contributors or send  
your idea to [support@h2o.ai](mailto:support@h2o.ai).

---
<a name="Issues"></a>
## Issues 
For issues reporting please use JIRA at [http://jira.h2o.ai/](http://jira.h2o.ai/).

---
<a name="MailList"></a>
## Mailing list

Follow our [H2O Stream](https://groups.google.com/forum/#!forum/h2ostream).
---
<a name="Binary"></a>
## Downloads of binaries
   * [Sparkling Water - Latest version](http://h2o-release.s3.amazonaws.com/sparkling-water/master/latest.html)

---
<a name="MakeBuild"></a>
## Making a build

Use the provided `gradlew` to build project:

```
./gradlew build
```

> To avoid running tests, please, use `-x test` option

---
<a name="SparkShell"></a>
## Sparkling shell

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

> In this case `local-cluster[3,2,1024]` points to embedded cluster of 3 worker nodes, each with 2 cores and 1G of memory.

And run Sparkling Shell:
```
bin/sparkling-shell
```

> Sparkling Shell accepts common Spark Shell arguments. For example, to increase memory allocated by each executor use the `spark.executor.memory` parameter: `bin/sparkling-shell --conf "spark.executor.memory=4g"`
---

<a name="RunExample"></a>
## Running examples

Build a package that can be submitted to Spark cluster:
```
./gradlew assemble
```

Set the configuration of the demo Spark cluster, for example; `local-cluster[3,2,1024]`

```
export SPARK_HOME="/path/to/spark/installation"
export MASTER="local-cluster[3,2,1024]"
```
> In this example, the description `local-cluster[3,2,1024]` causes the creation of an embedded cluster consisting of 3 workers.

And run the example:
```
bin/run-example.sh
```

For more details about the demo, please see the [README.md](examples/README.md) file in the [examples directory](examples/).
---
<a name="MoreExamples"></a>
### Additional Examples
You can find more examples in [examples folder](examples/).
---  
<a name="Docker"></a>
## Docker Support

See [docker/README.md](docker/README.md) to learn about Docker support.
---
<a name="FAQ"></a>
## FAQ

* Where to find Spark logs?
  > Look for `$SPARK_HOME/work/app-XXX`. The last part of address is name of your application.
 
* Spark is too slow during start or H2O is not able to cluster.
  > Configure Spark variable `SPARK_LOCAL_IP`. For example: 
  ```
  export SPARK_LOCAL_IP='127.0.0.1'
  ```  
* How to increase memory assigned to Spark executors in Sparkling Shell?
 > Sparkling Shell accepts common Spark Shell arguments. For example, to increase
 > memory allocated by each executor it > is possible to pass `spark.executor.memory`
 > parameter: `bin/sparkling-shell --conf "spark.executor.memory=4g"`

