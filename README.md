# Sparkling Water

Sparkling Water integrates H<sub>2</sub>O fast scalable machine learning engine with Spark.

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

Configure location of your Spark cluster, for example:
```
export MASTER="spark://localhost:7077"
```

And run example:
```
bin/run-example.sh
```

> Default address of Spark cluster is `local-cluster[3,2,104]` pointing to embedded cluster of 3 workers.

## Sparkling shell

It provides regular Spark shell with support to create H<sub>2</sub>O cloud and execute H<sub>2</sub> algorithms.

First, build a package containing Sparkling water
```
./gradlew shadowJar
```

Configure location of your Spark cluster, for example:
```
export MASTER="spark://localhost:7077"
```

And run Sparkling Shell
```
bin/sparkling-shell
```




