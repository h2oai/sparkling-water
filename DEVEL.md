# Sparkling Water Development Documentation

## Features

Sparkling Water provides transparent integration of H2O engine and its machine learning 
algorithms
into the Spark platform. Hence, it enables:
 * using H2O algorithms in Spark workflow
 * transformation between H2O and Spark data structures
 * using Spark RDDs as input for H2O algorithms
 * transparent execution of Sparkling Water applications on top of Spark
 
### Supported data sources
Currently Sparkling Water can use:
 - standard RDD API to load data and transferm them into DataFrame
 - H2O API to load data directly into DataFrame from
   - local file(s)
   - HDFS file(s)
   - S3 files(s)
   
### Supported data formats
Sparkling Water can read data stored in the following formats:
 - CSV
 - SVMLight
 - ARFF
 
### Supported execution environments
Sparkling Water can run on the top of Spark:
 - running as local cluster (master points to one of values `local`, `local[*]`, or `local-cluster[...]`
 - running as a standalone cluster
 - running in YARN environment

## Design

The Sparkling Water is designed to be executed as a regular Spark application.
It provides a way to initialize H2O services on each node in the Spark cluster and access
data stored in Spark and H2O's data structures.

Since Sparkling Water is designed as Spark application, it is launched 
inside Spark executor which are created after application submission. 
At this point H2O services including distributed KV store, memory manager are started
and orchestrated into a cloud. The topology of the created cloud exactly matches the topology of the underlying Spark cluster.

[Image from Slides showing the topology]

When H2O services are running, it is possible to create H2O data structures, call H2O algorithms, and transfer values from/to RDD.


### Provided Primitives
The Sparkling Water provides following primitives:


| Concept        | Implementation class              | Description |
|----------------|-----------------------------------|-------------|
| H2O context    | `org.apache.spark.h2o.H2OContext` | H2O context holds H2O state and provides primitives to transfer RDD into DataFrame and vice versa. It follows design principles of Spark primitives such as `SparkContext` or `SQLContext` |
| H2O entry point| `water.H2O`                       | The class represents entry point for accessing H2O services. It holds information about actual H2O cluster including list of nodes and state of distributed K/V datastore. |
| H2O DataFrame  | `water.fvec.DataFrame`            | DataFrame is H2O data structure representing a table of values. The table is column based providing column and row accessors. |
| H2O Algorithms | package `hex`                     | The package represents H2O machine learning algorithms library including DeepLearning, GBM, RandomForest. |
 

### H2O Initialization Sequence
When `SparkContext` is available, it is possible to initialize and start H2O context
```scala
val sc:SparkContext = ...
val hc = new H2OContext(sc).start()
```

The call will:
 1. collect actual number and host names of executors (worker nodes) in Spark cluster;
 2. launch H2O services on each detected executor;
 3. cloud up H2O services based on the list of executors;
 4. verify H2O cloud status

### Data sharing
Sparkling Water provides transformation between different types of RDD and H2O's DataFrame and vice versa.

[pic from slides]

The direction from DataFrame to RDD is based on creating a cheap wrapper around H2O DataFrame which provides RDD-like API. In this case, no data is duplicated and is served directly from underling DataFrame.

The opposite direction, from RDD to DataFrame, introduces data duplication since it transfers data from RDD storage into DataFrame. However, data stored in DataFrame is heavily compressed. 

TODO: estimation of overhead


## Typical Use-Case
Sparkling Water excels in existing Spark-based workflows which need to call advanced machine learning algorithms. Typical example involves data munging with help of Spark API, prepared table is passed to H2O DeepLearning algorithm. The constructed DeepLearning model estimates different metrics based on testing data, which can be used in the rest of Spark workflow.

## Requirements
 - Linux or Mac OSX platform
 - Java 1.7+
 - [Spark 1.2.0](http://spark.apache.org/downloads.html)

## Configuration

### Build Environment
The environment should contain a property `SPARK_HOME` pointing to Spark distribution.

### Run Environment
The environment should contain a property `SPARK_HOME`.

### Sparkling Water Configuration Properties

These configuration properties can be passed to Spark to configure Sparking Water.

| Property name | Default value | Description |
|---------------|---------------|-------------|
|`spark.ext.h2o.flatfile` | `true`| Use flatfile approach for H2O clouding instead of multicast|
|`spark.ext.h2o.cluster.size` | `-1` |Expected number of workers of H2O cloud. Value -1 means automatic detection of cluster size. Should be equal to number of Spark workers|
|`spark.ext.h2o.port.base`| `54321`| Base port used for individual H2O nodes configuration.|
|`spark.ext.h2o.port.incr`| `2` | Increment added to base port to find available port.|
|`spark.ext.h2o.cloud.timeout`| `60*1000` | Timeout for cloud up in msec |
|`spark.ext.h2o.spreadrdd.retries` | `10` | Number of retries to create an RDD covering all existing Spark executors. |
|`spark.ext.h2o.cloud.name`| `sparkling-water-` | Name of H2O cloud. |
|`spark.ext.h2o.log.level`| `INFO`| H2O internal log level. |
|`spark.ext.h2o.network.mask`|--|Subnet selector for h2o if IP guess fail - useful if 'spark.ext.h2o.flatfile' is false and we are trying to guess right IP on the machine.* |

### Pass property to Sparkling Shell
TODO: example of arg passing from sparkling shell.

### Pass property to Spark submit

## Running

### Creating H2O Services
```scala
val sc:SparkContext = ...
val hc = new H2OContext(sc).start()
```

When number of Spark nodes is known in advance, it can be specified in `start` call:
```scala
val hc = new H2OContext(sc).start(3)
```

### Transforming DataFrame into RDD[T]
The `H2OContext` class provides explicit conversion `asRDD` which creates a RDD-like wrapper around provided H2O DataFrame:
```scala
def asRDD[A <: Product: TypeTag: ClassTag](fr : DataFrame) : RDD[A]
```

The call expects the type `A` to create correctly typed RDD. 
The transformation requires type `A` to be bound by `Product` interface.
The relation between columns of DataFrame and attributes of class `A` is based on name matching.

#### Example
```scala
val df: DataFrame = ...
val rdd = asRDD[Weather](df)

```

### Transforming DataFrame into SchemaRDD
The `H2OContext` class provides explicit conversion `asSchemaRDD` which creates a SchemaRDD-like wrapper
around provided H2O DataFrame (technically it provides `RDD[sql.Row]` RDD API):
```scala
def asSchemaRDD(fr : DataFrame)(implicit sqlContext: SQLContext) : SchemaRDD
```

This call does not require any type parameters, but since it creates `SchemaRDD` instances it requires access to
an instance of `SQLContext`. In this case, the instance is provided as an implicit parameter of the call. The parameter can be passed in two ways as explicit parameter or introducing implicit variable into current context.

The schema of created instance of SchemaRDD is derived from column name and types of given DataFrame.


#### Example

Using explicit parameter of call to pass sqlContext:
```scala
val sqlContext = new SQLContext(sc)
val schemaRDD = asSchemaRDD(dataFrame)(sqlContext)
```
or as implicit variable provided by actual environment:
```scala
implicit val sqlContext = new SQLContext(sc)
val schemaRDD = asSchemaRDD(dataFrame)
```


### Transforming RDD[T] into DataFrame
The `H2OContext` provides **implicit** conversion from given `RDD[A]` to DataFrame. As in opposite direction, the type `A` has to satisfy upper bound expressed by the type `Product`. The conversion will create a new DataFrame, transfer data from given RDD, and save it to H2O K/V data store.

```scala
implicit def createDataFrame[A <: Product : TypeTag](rdd : RDD[A]) : DataFrame
```

#### Example
```scala
val rdd: RDD[Weather] = ...
import h2oContext._
val df: DataFrame = rdd // implicit call of H2OContext.createDataFrame[Weather](rdd) is used 
```

### Transforming SchemaRDD into DataFrame
The `H2OContext` provides **implicit** conversion from given `SchemaRDD` to DataFrame. The conversion will create a new DataFrame, transfer data from given RDD, and save it to H2O K/V data store.

```scala
implicit def createDataFrame(rdd : SchemaRDD) : DataFrame
```

#### Example
```scala
val srdd: SchemaRDD = ...
import h2oContext._
val df: DataFrame = srdd // implicit call of H2OContext.createDataFrame(srdd) is used 
```


### Creating DataFrame from existing Key

If H2O cluster already contains a loaded DataFrame referenced by key `train.hex`, it is possible
to reference it from Sparkling Water via creating a proxy `DataFrame` instance using key as the input:
```scala
val trainDF = new DataFrame("train.hex")
```

### Type mapping between H2O DataTypes and Spark SchemaRDD types
TBD


### Calling H2O Algorithm

Calling of H2O algorithm is composed of three steps:
 1. Creating parameters object which hold references to input data and parameters specific for the algorithm:
 ```scala
 val train: RDD = ...
 val valid: DataFrame = ...
 
 val gbmParams = new GBMParameters()
 gbmParams._train = train
 gbmParams._valid = valid
 gbmParams._response_column = 'bikes
 gbmParams._ntrees = 500
 gbmParams._max_depth = 6
 ```
 2. Creating model builder:
 ```scala
 val gbm = new GBM(gbmParams)
 ```
 3. Invoking model build job and blocking till the end of computation (`trainModel` is asynchronous call by default):
 ```scala
 val gbmModel = gbm.trainModel.get
 ```
 

## Running unit tests
JVM options -Dspark.testing=true -Dspark.test.home=/Users/michal/Tmp/spark/spark-1.1.0-bin-cdh4/

## Overhead Estimation
TBD

## Application Development

## Sparkling Water configuration

TODO: used datasources, how data is moved to spark
TODO: platform testing - mesos, SIMR

# Running on select target platforms
## Standalone
[Spark documentation - running Standalone cluster](http://spark.apache.org/docs/latest/spark-standalone.html)

## YARN
[Spark documentation - running Spark Application on YARN](http://spark.apache.org/docs/latest/running-on-yarn.html)

## Mesos
[Spark documentation - running Spark Application on Mesos](http://spark.apache.org/docs/latest/running-on-mesos.html)

# Integration Tests

## Testing environments
 * Local - corresponds to setting Spark `MASTER` variable to one of `local`, or `local[*]`, or `local-cluster[_,_,_]` values
 * Standalone cluster - the `MASTER` variable points to existing standalone Spark cluster `spark://...` 
   * ad-hoc build cluster
   * CDH5.3 provided cluster
 * YARN cluster - the `MASTER variable contains `yarn-client` or `yarn-cluster` values

## Testing scenarios
 1. Initializing H2O on the top of Spark
 It includes running of `new H2OContext(sc).start()` and verifying that H2O was properly initialized on all Spark nodes
 2. Load data with help of H2O API from various data sources
   1. local disk
   2. HDFS
   3. S3N
 3. Transformation from `RDD[T]` to `DataFrame`
 4. Transformation from `SchemaRDD` to `DataFrame`
 5. Transformation from `DataFrame` to `RDD` 
 6. Transformation from `DataFrame` to `SchemaRDD`
 7. Integration with H2O Algorithms
   - using RDD as algorithm input
 8. Integration with MLlib Algorithms
   - using DataFrame as algorithm input
     - KMeans
 9. Integration with MLlib pipelines (TBD)

## Integration tests examples

The following code reflects use-cases listed above. The code is executed in all testing environments (if applicable): local, standalone cluster, yarn


 1. Initialize H2O
  ```scala
  import org.apache.spark.h2o._
  val sc = new SparkContext(conf)
  val h2oContext = new H2OContext(sc).start()
  import h2oContext._
  ```
  
 2. Data load
 	1. Local disk
  	
  	```scala
    val sc = new SparkContext(conf)
 	import org.apache.spark.h2o._
    val h2oContext = new H2OContext(sc).start()
 	import java.io.File
 	val df: DataFrame = new DataFrame(new File("/datasets/allyears2k_headers.csv.gz"))
 	```
 	> Note: The file has to exist on all nodes.
 	
 	2. HDFS
 	
 	```scala
    val sc = new SparkContext(conf)
 	import org.apache.spark.h2o._
    val h2oContext = new H2OContext(sc).start()
  	val path = "hdfs://mr-0xd6.0xdata.loc/datasets/airlines_all.csv"
 	val uri = new java.net.URI(path)
    val airlinesData = new DataFrame(uri)
 	```
 	
	3. S3N
	
 	```scala
    val sc = new SparkContext(conf)
    import org.apache.spark.h2o._
	val h2oContext = new H2OContext(sc).start()
 	val path = "s3n://h2o-airlines-unpacked/allyears2k.csv"
 	val uri = new java.net.URI(path)
    val airlinesData = new DataFrame(uri)
 	```
 	
	> Spark/H2O needs to know AWS credentials specified in `core-site.xml`. The credentials are passed via `HADOOP_CONF_DIR` pointing to a configuration directory with `core-site.xml`.
	
 3. Transformation from `RDD[T]` to `DataFrame`
   ```scala
   val sc = new SparkContext(conf)
   import org.apache.spark.h2o._
   val h2oContext = new H2OContext(sc).start()
   val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(Some(v)))
   val dataFrame:DataFrame = h2oContext.createDataFrame(rdd)
   ```
   
 4. Transformation from `SchemaRDD` to `DataFrame`
   ```scala
   val sc = new SparkContext(conf)
   import org.apache.spark.h2o._
   val h2oContext = new H2OContext(sc).start()
   import org.apache.spark.sql._
   val sqlContext = new SQLContext(sc)
   val srdd:SchemaRDD = sc.parallelize(values).map(v => LongField(v))
   val dataFrame = h2oContext.toDataFrame(srdd)
   ``` 
