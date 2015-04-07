# Sparkling Water Meetup (01/20/2015)

## Requirements
 
### For Sparkling Water part
 - Oracle Java 7+
 - Spark 1.1.0
 - Sparkling Water 0.2.4-62
 
### For R part
 - R 3.1.2+
 - RStudio
 
### For Sparkling Water droplet part
 - Git
 - Idea/Eclipse IDE with Scala support
 
## Download

Please download [Sparkling Water
0.2.4-62](http://h2o-release.s3.amazonaws.com/sparkling-water/master/62/index.html) and unzip the file:
```
unzip sparkling-water-0.2.4-62.zip
cd sparkling-water-0.2.4-62
```

> All materials will be also available on provided USBs.

## Slides
Hands-On slides are available at [H2O.ai SlideShare account](http://www.slideshare.net/0xdata/spa-43755759)

## Use Sparkling Shell for ML 

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
  import org.apache.spark.examples.h2o._
  val h2oContext = new H2OContext(sc).start()
  import h2oContext._
  ```

4. Load weather data for Chicago international airport (ORD) with help of RDD API.
  ```scala
  val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
  val wrawdata = sc.textFile(weatherDataFile,3).cache()
  val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())
  ```

5. Load airlines data using H<sub>2</sub>O parser
  ```scala
  import java.io.File
  val dataFile = "examples/smalldata/year2005.csv.gz"
  val airlinesData = new DataFrame(new File(dataFile))
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
  // Make sqlContext implicit to allow its use from H2OContext
  implicit val sqlContext = new SQLContext(sc)
  import sqlContext._
  flightsToORD.registerTempTable("FlightsToORD")
  weatherTable.registerTempTable("WeatherORD")
  ```

9. Perform SQL JOIN on both tables
  ```scala
  val joinedTable = sql(
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
  
10. Split data into train/validation/test datasets
  ```scala
  import hex.splitframe.SplitFrame
  import hex.splitframe.SplitFrameModel.SplitFrameParameters

  val sfParams = new SplitFrameParameters()
  sfParams._train = joinedTable
  sfParams._ratios = Array(0.7, 0.2)
  val sf = new SplitFrame(sfParams)

  val splits = sf.trainModel().get._output._splits
  val trainTable = splits(0)
  val validTable = splits(1)
  val testTable  = splits(2)
  ```
  
11. Inspect results in H2O Flow - go to (http://localhost:54321/)[http://localhost:54321/]
  
12. Run deep learning to produce model estimating arrival delay:
  ```scala
  import hex.deeplearning.DeepLearning
  import hex.deeplearning.DeepLearningModel.DeepLearningParameters
  val dlParams = new DeepLearningParameters()
  dlParams._train = trainTable
  dlParams._response_column = 'ArrDelay
  dlParams._valid = validTable
  dlParams._epochs = 100
  dlParams._reproducible = false
  dlParams._force_load_balance = false

  // Invoke model training
  val dl = new DeepLearning(dlParams)
  val dlModel = dl.trainModel.get
  ```

13. Use model to estimate delay on training data
  ```scala
  // Produce single-vector table with prediction
  val dlPredictTable = dlModel.score(testTable)('predict)
  // Convert vector to SchemaRDD and collect results
  val predictionsFromDlModel = asSchemaRDD(dlPredictTable).collect.map(row => if (row.isNullAt(0)) Double.NaN else row(0))
  ```

## Use Sparkling Water from R
  
1. In Sparkling Shell generate R code producing residuals plot
  ```scala
  import org.apache.spark.examples.h2o.DemoUtils.residualPlotRCode
  residualPlotRCode(dlPredictTable, 'predict, testTable, 'ArrDelay)
  ```
  
2.  Use resulting R code inside RStudio:
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
  
3. Try to generate better model with GBM
  ```scala
  import hex.tree.gbm.GBM
  import hex.tree.gbm.GBMModel.GBMParameters

  val gbmParams = new GBMParameters()
  gbmParams._train = trainTable
  gbmParams._response_column = 'ArrDelay
  gbmParams._valid = validTable
  gbmParams._ntrees = 100

  val gbm = new GBM(gbmParams)
  val gbmModel = gbm.trainModel.get
  ```
  
4. Make prediction and print R code for residual plot
  ```scala
  val gbmPredictTable = gbmModel.score(testTable)('predict)
  residualPlotRCode(gbmPredictTable, 'predict, testTable, 'ArrDelay)
  ```
  
## Develop a Standalone Sparkling Water Application

1. Clone H2O Droplets repository
  ```
  git clone https://github.com/h2oai/h2o-droplets.git
  ```

2. Go to Sparkling Water droplet directory
  ```
  cd h2o-droplets/sparkling-water-droplet/
  ```

3. Generate Idea/Eclipse project files
  For Idea
  ```
  ./gradlew idea
  ```

  For Eclipse
  ```
  ./gradlew eclipse
  ```

4. Open generated project in you favorite IDE

5. Try to run simple application `water.droplets.SparklingWaterDroplet`

6. Add a new Scala object `water.droplets.AirlinesWeatherAnalysis` with main method
  ```scala
  package water.droplets
  object AirlineWeatherAnalysis {
    def main(args: Array[String]) {
    }
  }
  ```

7. Create Spark configuration and context 
  ```scala
  package water.droplets
  object AirlineWeatherAnalysis {
    def main(args: Array[String]) {
      import org.apache.spark._
      
	  val conf = new SparkConf().setAppName("Flight analysis")
	  conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
	  val sc = new SparkContext(conf)
    }
  }
  ```
 
8. Use code above to create `H2OContext`, load data and perform analysis. At the end of code, do not forget to release Spark context (H2O context will be released automatically)

  ```scala
  package water.droplets
  import org.apache.spark.h2o._
  import org.apache.spark.examples.h2o._
 
  import org.apache.spark._

  object AirlineWeatherAnalysis {
    def main(args: Array[String]) {
	  val conf = new SparkConf().setAppName("Flights analysis")
	  conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
	  val sc = new SparkContext(conf)
	  
      import org.apache.spark.h2o._
	  import org.apache.spark.examples.h2o._
      val h2oContext = new H2OContext(sc).start()
      import h2oContext._
    
	  // Do not forget to modify location of your data  
	  val weatherDataFile = "smalldata/Chicago_Ohare_International_Airport.csv"
      val wrawdata = sc.textFile(weatherDataFile,3).cache()
      val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())
      
	  import java.io.File
      val dataFile = "smalldata/allyears2k_headers.csv.gz"
      val airlinesData = new DataFrame(new File(dataFile))
      
      val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
      val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))
      
	  import org.apache.spark.sql.SQLContext
      // Make sqlContext implicit to allow its use from H2OContext
      implicit val sqlContext = new SQLContext(sc)
      import sqlContext._
      flightsToORD.registerTempTable("FlightsToORD")
      weatherTable.registerTempTable("WeatherORD")
	
	  val joinedTable = sql(
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
          
	  import hex.splitframe.SplitFrame
      import hex.splitframe.SplitFrameModel.SplitFrameParameters

      val sfParams = new SplitFrameParameters()
      sfParams._train = joinedTable
      sfParams._ratios = Array(0.7, 0.2)
      val sf = new SplitFrame(sfParams)

      val splits = sf.trainModel().get._output._splits
      val trainTable = splits(0)
      val validTable = splits(1)
      val testTable  = splits(2)
      
      import hex.deeplearning.DeepLearning
      import hex.deeplearning.DeepLearningModel.DeepLearningParameters
      val dlParams = new DeepLearningParameters()
      dlParams._train = trainTable
      dlParams._response_column = 'ArrDelay
      dlParams._valid = validTable
      dlParams._epochs = 100
      dlParams._reproducible = false
      dlParams._force_load_balance = false

      // Invoke model training
      val dl = new DeepLearning(dlParams)
      val dlModel = dl.trainModel.get
      
      // Produce single-vector table with prediction
      val dlPredictTable = dlModel.score(testTable)('predict)
      // Convert vector to SchemaRDD and collect results
      val predictionsFromDlModel = asSchemaRDD(dlPredictTable).collect.map(row => if (row.isNullAt(0)) Double.NaN else row(0))
      
	  println(predictionsFromDlModel.mkString("\n"))
	  
      // Shutdown application
      sc.stop()
    }
  }
  ```

9. Try to run application with your IDE

10. Build application from command line

  ```
  ./gradlew build shadowJar
  ```
  
11. Submit application to Spark cluster
  ```
  export MASTER='local-cluster[3,2,1024]'
  $SPARK_HOME/bin/spark-submit --class water.droplets.AirlineWeatherAnalysis build/libs/sparkling-water-droplet-app.jar
  ```   
