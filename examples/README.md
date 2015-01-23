# Sparkling Water Examples

## Available Examples
  * `ProstateDemo` - running K-means on prostate dataset (see
    _smalldata/prostate.csv_)
  * `DeepLearningDemo` - running DeepLearning on a subset of airlines dataset (see
    _smalldata/allyears2k\_headers.csv.gz_)
  * `AirlinesWithWeatherDemo` - joining flights data with weather data and running
    Deep Learning
  * `AirlinesWithWeatherDemo2` - new iteration of `AirlinesWithWeatherDemo`

## Compile Example
Please use top-level `gradlew`:
```
./gradlew assemble
```
## Run Example

### Simple Local Cluster
 
 Run a given example on local cluster. The cluster is defined by `MASTER` address `local-cluster[3,2,3072]` which means that cluster contains 3 worker nodes, each having 2CPUs and 3GB of memory:
 
   * Run `bin/run-example.sh <name of demo>`

### Run on Spark Cluster
   * Run Spark cluster, for example via `bin/launch-spark-cloud.sh`
     * Verify that Spark is running - Spark UI on `http://localhost:8080/` should show 3 worker nodes 
   * Export `MASTER` address of Spark master, i.e., `export MASTER="spark://localhost:7077"`
   * Run `bin/run-example.sh <name of demo>`
   * Observe status of the application via Spark UI on `http://localhost:8080/`

## Sparkling Water Variables

You can tune Sparkling Water via the following variables:
  * `spark.h2o.cloud.timeout` - number of msec to wait for cloud formation
  * `spark.h2o.workers` - number of expected H<sub>2</sub>O workers - it should be same as number of Spark workers
  * `spark.h2o.preserve.executors` - do not kill executors via calling `sc.stop()` call


## Step-by-Step through Weather Data Example

1. Run Sparkling shell with an embedded cluster:
  ```
  export SPARK_HOME="/path/to/spark/installation"
  export MASTER="local-cluster[3,2,1024]"
  bin/sparkling-shell
  ```

2. You can go to [http://localhost:4040/](http://localhost:4040/) to see the Sparkling shell (i.e., Spark driver) status.

3. Create H<sub>2</sub>O cloud using all 3 Spark workers
  ```scala
  import org.apache.spark.h2o._
  val h2oContext = new H2OContext(sc).start()
  import h2oContext._
  ```

4. Load weather data for Chicago international airport (ORD) with help of RDD API.
  ```scala
  import org.apache.spark.examples.h2o._
  val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
  val wrawdata = sc.textFile(weatherDataFile,3).cache()
  val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())
  ```

5. Load airlines data using H<sub>2</sub>O parser
  ```scala
  import java.io.File
  import water.fvec.DataFrame
  val dataFile = "examples/smalldata/allyears2k_headers.csv.gz"
  val airlinesData = DataFrame(new File(dataFile))
  ```

6. Select flights with destination in Chicago (ORD)
  ```scala
  val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
  val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))
  ```
  
7. Compute number of these flights
  ```scala
  flightsToORD.count
  ```

8. Use Spark SQL to join flight data with weather data
  ```scala
  import org.apache.spark.sql.SQLContext
  implicit val sqlContext = new SQLContext(sc)
  import sqlContext._
  flightsToORD.registerTempTable("FlightsToORD")
  weatherTable.registerTempTable("WeatherORD")
  ```

9. Perform SQL JOIN on both tables
  ```scala
  val bigTable = sql(
          """SELECT
            |f.Year,f.Month,f.DayofMonth,
            |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
            |f.UniqueCarrier,f.FlightNum,f.TailNum,
            |f.Origin,f.Distance,
            |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
            |f.ArrDelay
            |FROM FlightsToORD f
            |JOIN WeatherORD w
            |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day""".stripMargin)
  ```
  
10. Transform the first 3 columns holding date information into enum columns
  ```scala
  val bigDataFrame: DataFrame = bigTable // implicit conversion from RDD to DataFrame
  for( i <- 0 to 2) bigDataFrame.replace(i, bigDataFrame.vec(i).toEnum)
  ```

11. Run deep learning to produce model estimating arrival delay:
  ```scala
  import hex.deeplearning.DeepLearning
  import hex.deeplearning.DeepLearningModel.DeepLearningParameters
  import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
  val dlParams = new DeepLearningParameters()
  dlParams._train = bigDataFrame
  dlParams._response_column = 'ArrDelay
  dlParams._epochs = 5
  dlParams._activation = Activation.RectifierWithDropout
  dlParams._hidden = Array[Int](100, 100)
  
  // Create a job  
  val dl = new DeepLearning(dlParams)
  val dlModel = dl.trainModel.get
  ```

12. Use model to estimate delay on training data
  ```scala
  val predictionH2OFrame = dlModel.score(bigTable)('predict)
  val predictionsFromModel = asSchemaRDD(predictionH2OFrame).collect.map(row => if (row.isNullAt(0)) Double.NaN else row(0))
  ```

13. Generate R-code producing residual plot
  ```scala
  import org.apache.spark.examples.h2o.DemoUtils.residualPlotRCode
  residualPlotRCode(predictionH2OFrame, 'predict, bigTable, 'ArrDelay)  
  ```
  
  Execute generated R-code in RStudio:
  ```R
  #
  # R script for residual plot
  #
  # Import H2O library
  library(h2o)
  # Initialize H2O R-client
  h = h2o.init()
  # Fetch prediction and actual data, use remembered keys
  pred = h2o.getFrame(h, "dframe_b5f449d0c04ee75fda1b9bc865b14a69")
  act = h2o.getFrame (h, "frame_rdd_14_b429e8b43d2d8c02899ccb61b72c4e57")
  # Select right columns
  predDelay = pred$predict
  actDelay = act$ArrDelay
  # Make sure that number of rows is same  
  nrow(actDelay) == nrow(predDelay)
  # Compute residuals  
  residuals = predDelay - actDelay
  # Plot residuals   
  compare = cbind (as.data.frame(actDelay$ArrDelay), as.data.frame(residuals$predict))
  nrow(compare)
  plot( compare[,1:2] )
  ```

# Sparkling Water on Hadoop

Compatiable Hadoop Distribution: CDH4, ~~CDH5~~, and HDP2.1

## Install on Hadoop

- To install on your Hadoop Cluster clone the git repository and make a build:

```
git clone https://github.com/0xdata/sparkling-water.git 
cd sparkling-water
./gradlew build
```

- Then set MASTER to the IP address of where your Spark Master Node is launched and set SPARK_HOME to the location of your Spark installation. In the example below the path for SPARK_HOME is the default location of Spark preinstalled on a CDH5 cluster. Please change MASTER below:

```
export MASTER="spark://mr-0xd9-precise1.0xdata.loc:7077"
export SPARK_HOME="/opt/cloudera/parcels/CDH-5.2.0-1.cdh5.2.0.p0.11/lib/spark"
```

- Launch Sparkling Shell:

```
./bin/sparkling-shell
```

## Import Data from HDFS

The initialization of H2O remains the same with the exception of importing data from a HDFS path. Please change path variable below to one suitable for your data.

```scala
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
// Create H2O context
val h2oContext = new H2OContext(sc).start()
// Export H2O context to the 
import h2oContext._

// URI to access HDFS file
val path = "hdfs://mr-0xd6-precise1.0xdata.loc:8020/datasets/airlines_all.05p.csv"
val d = new java.net.URI(path)
val f = new DataFrame(d)
```
