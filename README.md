# Sparkling Water

Sparkling Water integrates H<sub>2</sub>O fast scalable machine learning engine with Spark.

## Requirements

  * Linux or OS X (Windows support is coming)
  * Java 7
  * Spark 1.1.0 
    * `SPARK_HOME` shell variable should point to your local Spark installation
 
## Downloads of binaries
   * [Sparkling Water - Latest version](http://h2o-release.s3.amazonaws.com/sparkling-water/master/latest.html)

## Contributing

### Where to start

Look at our [list of JIRA tasks](https://0xdata.atlassian.net/issues/?filter=13600) for new contributors or send us 
your idea via [support@h2o.ai](mailto:support@h2o.ai).

## Issues 
For issues reporting please use JIRA at [http://jira.h2o.ai/](http://jira.h2o.ai/).

## Mailing list

Follow our [H2O Stream](https://groups.google.com/forum/#!forum/h2ostream).

## Building

Use provided `gradlew` to build project:

```
./gradlew build
```

> To avoid running tests, please, use `-x test` option

## Running examples

Build a package which can be submitted to Spark cluster:
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


## Sparkling shell

The Sparkling shell provides a regular Spark shell with support to create a H<sub>2</sub>O cloud and execute H<sub>2</sub>O algorithms.

First, build a package containing Sparkling water
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

> Sparkling Shell accepts common Spark Shell arguments. For example, to increase memory allocated by each executor it > is possible to pass `spark.executor.memory` parameter:
> `bin/sparkling-shell --conf "spark.executor.memory=4g"`

### Examples
You can find more examples in [examples folder](examples/).
  
## Docker Support

See [docker/README.md](docker/README.md) to learn about Docker support.
