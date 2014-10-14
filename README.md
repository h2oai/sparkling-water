# Sparkling Water

Sparkling Water integrates H<sub>2</sub>O fast scalable machine learning engine with Spark.

## Requirements

  * Java 7
  * Spark 1.1.0 
    * `SPARK_HOME` shell variable should point to your local Spark installation
  
## Building

Use provided `gradlew` to build project:

```
./gradlew build
```

> To avoid running tests, please, use `-x test` option

## Running examples

Build a package which can be submitted to Spark cluster:
```
./gradlew shadowJar
```

Configure location of your Spark cluster or use default `local-cluster[3,2,104]`
```
export MASTER="spark://localhost:7077"
```

And run example:
```
bin/run-example.sh
```

> Default address of Spark cluster is `local-cluster[3,2,104]` causing creation of an embedded cluster consisting of 3 workers.

## Sparkling shell

It provides regular Spark shell with support to create H<sub>2</sub>O cloud and execute H<sub>2</sub> algorithms.

First, build a package containing Sparkling water
```
./gradlew shadowJar
```

Configure location of your Spark cluster if necessary:
```
export MASTER="spark://localhost:7077"
```

And run Sparkling Shell
```
bin/sparkling-shell
```




