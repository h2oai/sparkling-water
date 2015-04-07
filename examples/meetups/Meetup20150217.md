# Sparkling Water Meetup (02/17/2015)

## Requirements
 
### For Sparkling Water part
 - Oracle Java 7+
 - [Spark 1.2.0](http://spark.apache.org/downloads.html)
 - [Sparkling Water 0.2.11-80](http://h2o-release.s3.amazonaws.com/sparkling-water/master/80/index.html)
 
### For Sparkling Water droplet part
 - Git
 - Idea/Eclipse IDE with Scala support (Idea is recommended)
 - [Sparkling Water Droplet](https://github.com/h2oai/h2o-droplets)
 
## Download

Please download [Sparkling Water
0.2.11-80](http://h2o-release.s3.amazonaws.com/sparkling-water/master/80/index.html) and unzip the file:
```
unzip sparkling-water-0.2.11-80.zip
cd sparkling-water-0.2.11-80
```

> All materials will be also available on provided USBs.

## Slides
Hands-On slides are available at [H2O.ai SlideShare account](http://www.slideshare.net/0xdata/spa-43755759)

## Script
The script is available at GitHub - [https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/scripts/Meetup20150217.script](https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/scripts/Meetup20150217.script).

```bash
curl https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/scripts/Meetup20150217.script > meetup.script
```

## Data
Data are available on provided USB or in S3:

### CitiBike data

```bash
for f in 2013-07.csv 2013-08.csv 2013-09.csv 2013-10.csv 2013-11.csv 2013-12.csv 2014-01.csv 2014-02.csv 2014-03.csv 2014-04.csv 2014-05.csv 2014-06.csv 2014-07.csv 2014-08.csv 
do
wget "https://s3.amazonaws.com/h2o-public-test-data/bigdata/laptop/citibike-nyc/$f"
done
```
### Weather data 

```bash
wget https://s3.amazonaws.com/h2o-public-test-data/bigdata/laptop/citibike-nyc/31081_New_York_City__Hourly_2013.csv
```

## ML Workflow

> Create a model which will predict number of bikes on stations.

### Prepare data and build GBM model

1. Run Sparkling shell with an embedded Spark cluster:
  ```
  export SPARK_HOME="/path/to/spark/installation"
  export MASTER="local-cluster[3,2,2048]"
  bin/sparkling-shell
  ```

2. Open Spark UI: You can go to [http://localhost:4040/](http://localhost:4040/) to see the Sparkling shell (i.e., Spark driver) status.


3. Initialize H2O: Create H<sub>2</sub>O cloud using all 3 Spark workers
  ```scala
  import org.apache.spark.h2o._
  import org.apache.spark.examples.h2o._
  import org.apache.spark.sql.{SQLContext, SchemaRDD}
  import org.joda.time.MutableDateTime
  import water.fvec._
  import hex.tree.gbm.GBMModel

  implicit val h2oContext = new H2OContext(sc).start()
  import h2oContext._
  
  // Initialize SQLConcept
  implicit val sqlContext = new SQLContext(sc)
  import sqlContext._
  ```

4. Open H2O UI: 
  ```scala
  openFlow
  ```
  > At this point, you can go use H2O UI and see status of H2O cloud by typing `getCloud`.
  
  > You can also open Spark UI by typing `openSparkUI`.
  
5. Load data into H2O:
  ```scala
  val DIR_PREFIX = "/Users/michal/Devel/projects/h2o/repos/h2o2/bigdata/laptop/citibike-nyc/"
  val dataFiles = Array[String](
      "2013-07.csv", "2013-08.csv", "2013-09.csv", "2013-10.csv",
      "2013-11.csv", "2013-12.csv",
      "2014-01.csv", "2014-02.csv", "2014-03.csv", "2014-04.csv",
      "2014-05.csv", "2014-06.csv", "2014-07.csv", "2014-08.csv").map(f => new java.io.File(DIR_PREFIX, f))
  // Load and parse data
  val bikesDF = new DataFrame(dataFiles:_*)
  // Rename columns and remove all spaces in header
  val colNames = bikesDF.names().map( n => n.replace(' ', '_'))
  bikesDF._names = colNames
  bikesDF.update(null)
  ```
  
  Data contains columns: _tripduration,starttime,stoptime,start station id,start station name,start station latitude,start station longitude,end station id,end station name,end station latitude,end station longitude,bikeid,usertype,birth year,gender_

6. Transform column `starttime` to number of days from Epoch: 
  ```scala
  // Select column
  val startTimeCol = bikesDF('starttime)
  // Transform column and add it to original table
  bikesDF.add(new TimeSplit().doIt(startTimeCol))
  // Do not forget to update frame in K/V store
  bikesDF.update(null)
  ```
  
  > What is `TimeSplit`?
  ```scala
  class TimeSplit extends MRTask[TimeSplit] {
    def doIt(time: DataFrame):DataFrame =
        DataFrame(doAll(1, time).outputFrame(Array[String]("Days"), null))
    override def map(msec: Chunk, day: NewChunk):Unit = {
      for (i <- 0 until msec.len) {
        day.addNum(msec.at8(i) / (1000 * 60 * 60 * 24)); // Days since the Epoch
      }
    }
  }
  ```
  
7. Transform DataFrame `bikesDF` into RDD:
  ```scala
  val bikesRdd = asSchemaRDD(bikesDF)
  ``` 
8. Do grouping via Spark SQL: make a new table by grouping by `start_station_id` and `Days`
  ```scala
  // Register table and SQL table
  sqlContext.registerRDDAsTable(bikesRdd, "bikesRdd")
  val bikesPerDayRdd = sql(
    """SELECT Days, start_station_id, count(*) bikes
      |FROM bikesRdd
      |GROUP BY Days, start_station_id """.stripMargin)
  ```
  
9. Convert RDD to DataFrame type
  ```scala
  val bikesPerDayDF:DataFrame = bikesPerDayRdd // Implicit conversion h2oContext.createDataFrame(
  ```

10. Perform another column transformation - refine `Days` column to `Month` and `DayOfWeek`
  ```scala
  // Select "Days" column
  val daysVec = bikesPerDayDF('Days)
  // Run transformation TimeTransform
  val finalBikeDF = bikesPerDayDF.add(new TimeTransform().doIt(daysVec))
  ```
  
  What is `TimeTransform`?

11. Build a GBM model: 
  
  1. define a helper function to make a model
    ```scala
    def r2(model: GBMModel, fr: Frame) =  hex.ModelMetrics.getFromDKV(model,fr).asInstanceOf[hex.ModelMetricsSupervised].r2()
    
    def buildModel(df: DataFrame)(implicit h2oContext: H2OContext) = {
        import hex.splitframe.ShuffleSplitFrame
        import h2oContext._
        import water.Key
        //
        // Split into train and test parts
        //
        val keys = Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make(_))
        val ratios = Array[Double](0.6, 0.3, 0.1)
        val frs = ShuffleSplitFrame.shuffleSplitFrame(df, keys, ratios, 1234567689L)
        val train = frs(0)
        val test = frs(1)
        val hold = frs(2)

        //
        // Launch GBM prediction
        //
        import hex.tree.gbm.GBM
        import hex.tree.gbm.GBMModel.GBMParameters

        val gbmParams = new GBMParameters()
        gbmParams._train = train
        gbmParams._valid = test
        gbmParams._response_column = 'bikes
        gbmParams._ntrees = 500
        gbmParams._max_depth = 6

        val gbm = new GBM(gbmParams)
        val gbmModel = gbm.trainModel.get

        gbmModel.score(train).remove()
        gbmModel.score(test).remove()
        gbmModel.score(hold).remove()

        println(
          s"""
            |r2 on train: ${r2(gbmModel, train)}
            |r2 on test:  ${r2(gbmModel, test)}
            |r2 on hold:  ${r2(gbmModel, hold)}"""".stripMargin)

        // Perform clean-up
        train.delete()
        test.delete()
        hold.delete()

        gbmModel
    }
    ``` 

  2. Build a model
    ```scala
    buildModel(finalBikeDF)
    ``` 
    
### Can weather improve our model?


1. Load weather data via Spark API, transform and filter them:
  ```scala
  // Load weather data in NY 2013
  val weatherData = sc.textFile(DIR_PREFIX + "31081_New_York_City__Hourly_2013.csv")
  // Parse data and filter them
  val weatherRdd = weatherData.map(_.split(",")).
    map(row => NYWeatherParse(row)).
    filter(!_.isWrongRow()).
    filter(_.HourLocal == Some(12)).cache()
  ```
  > `NYWeatherParse` will in

2. Join weather data with `finalBikeDF` - use information about `Days` to make a join:
  ```scala
  sqlContext.registerRDDAsTable(weatherRdd, "weatherRdd")
  sqlContext.registerRDDAsTable(asSchemaRDD(finalBikeDF), "bikesRdd")

  val bikesWeatherRdd = sql(
    """SELECT b.Days, b.start_station_id, b.bikes, b.Month, b.DayOfWeek,
      |w.DewPoint, w.HumidityFraction, w.Prcp1Hour, w.Temperature, w.WeatherCode1
      | FROM bikesRdd b
      | JOIN weatherRdd w
      | ON b.Days = w.Days
      |
    """.stripMargin)
  ```

3. Build a model again:
  ```scala
  // And make prediction again but now on RDD
  buildModel(bikesWeatherRdd)
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

6. Add a new Scala object `water.droplets.CitiBikeAnalysis` with main method
  ```scala
  package water.droplets
  object CitiBikeAnalysis {
    def main(args: Array[String]) {
    }
  }
  ```

7. Create Spark configuration and context 
  ```scala
  package water.droplets
  object CitiBikeAnalysis {
    def main(args: Array[String]) {
      import org.apache.spark._
      
	  val conf = new SparkConf().setAppName("Flight analysis")
	  conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
	  val sc = new SparkContext(conf)
	  
	  /* Put here your code */
	  
	  // Shutdown the application
	  sc.stop()
    }
  }
  ```
 
8. Try to run application with your IDE

9. Build application from command line

  ```
  ./gradlew build shadowJar
  ```
  
10. Submit application to Spark cluster
  ```
  export MASTER='local-cluster[3,2,1024]'
  $SPARK_HOME/bin/spark-submit --class water.droplets.CitiBikeAnalysis build/libs/sparkling-water-droplet-app.jar
  ```   
