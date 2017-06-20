# Running Sparkling Water

In order to run Sparkling Water, the environment must contain the property `SPARK_HOME` that
points to the Spark distribution.

## Starting H2OContext

H2O on Spark can be started in the Spark Shell or in the Spark application as:

```scala
val hc = H2OContext.getOrCreate(spark)
```

The semantic of the call depends on the configured Sparkling Water backend. For more information
about the backends, please see [Sparkling Water Backends](backends.rst). 

In internal backend mode, the call will:
 1. Collect the number and host names of the executors (worker nodes) in the Spark cluster
 2. Launch H2O services on each detected executor
 3. Create a cloud for H2O services based on the list of executors
 4. Verify the H2O cloud status
 
In external backend mode, the call will:
 1. Start H2O in client mode on the Spark driver
 2. Start separated H2O cluster on the configured YARN queue
 3. Connects to the external cluster from the H2O client


Sparkling Water can run on top of Spark in the various ways, however starting
Sparkling Water requires different configuration on different environments:
- Local
    
    In this case Sparkling Water runs as a local cluster (Spark master variable points to one of values `local`, `local[*]`, or `local-cluster[...]`

- Standalone Spark Cluster
    
    [Spark documentation - running Standalone cluster](http://spark.apache.org/docs/latest/spark-standalone.html)

- YARN

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

- Mesos
    
    [Spark documentation - running Spark Application on Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html)
