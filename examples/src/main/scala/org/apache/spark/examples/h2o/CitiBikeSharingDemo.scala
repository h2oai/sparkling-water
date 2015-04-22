package org.apache.spark.examples.h2o

import java.io.File

import hex.splitframe.ShuffleSplitFrame
import hex.tree.gbm.GBMModel
import hex.{ModelMetrics, ModelMetricsSupervised}
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.h2o.{DataFrame, H2OContext}
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.MutableDateTime
import water.fvec.{NewChunk, Chunk, Frame}
import water.util.Timer
import water.{Key, MRTask}
import scala.collection.mutable

/**
 * Citibike NYC Demo.
 *
 *
 */
object CitiBikeSharingDemo {

  val DIR_PREFIX = "/Users/michal/Devel/projects/h2o/repos/h2o2/bigdata/laptop/citibike-nyc/"
  val TREES = 1

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = configure("Sparkling Water Meetup: Predict occupation of citi bike station in NYC")

    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    implicit val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext._

    // Make a shared timer
    implicit val gTimer = new GTimer

    //
    // Load data into H2O by using H2O parser
    //
    gTimer.start()
    val dataFiles = Array[String](
      "2013-07.csv", "2013-08.csv", "2013-09.csv", "2013-10.csv",
      "2013-11.csv", "2013-12.csv",
      "2014-01.csv", "2014-02.csv", "2014-03.csv", "2014-04.csv",
      "2014-05.csv", "2014-06.csv", "2014-07.csv", "2014-08.csv").map(f => new File(DIR_PREFIX, f).toURI)
    // Load and parse data
    val dataf = new DataFrame(dataFiles:_*)
    // Rename columns and remove all spaces in header
    val colNames = dataf.names().map( n => n.replace(' ', '_'))
    dataf._names = colNames
    dataf.update(null)
    gTimer.stop("H2O: parse")

    //
    // Transform start time to days from Epoch
    //
    gTimer.start()
    val startTimeF = dataf('starttime)
    //
    // Add a new column
    //
    dataf.add(new TimeSplit().doIt(startTimeF))
    // Do not forget to update frame in K/V store
    dataf.update(null)
    println(dataf)
    gTimer.stop("H2O: split start time column")

    //
    // Transform DataFrame into SchemaRDD
    //
    val brdd = asSchemaRDD(dataf)

    // Register table and SQL table
    sqlContext.registerRDDAsTable(brdd, "brdd")

    //
    // Do grouping with help of Spark SQL
    //
    val bph = sql(
      """SELECT Days, start_station_id, count(*) bikes
        |FROM brdd
        |GROUP BY Days, start_station_id """.stripMargin)
    println(bph.take(10).mkString("\n"))


    gTimer.start()
    // Convert RDD to DataFrame
    val bphf:DataFrame = bph
    gTimer.stop("Spark: do SQL query").start()
    //
    // Perform time transformation
    //
    val daysVec = bphf('Days)
    val finalTable = bphf.add(new TimeTransform().doIt(daysVec))
    gTimer.stop("H2O: time transformation")
    println(finalTable)

    // Build GBM model
    buildModel(finalTable)

    //
    // Try to join with weather data and repeat experiment again
    //

    // Load weather data via Spark API and parse them
    val weatherData = sc.textFile(DIR_PREFIX + "31081_New_York_City__Hourly_2013.csv")
    // Parse data and filter them
    val weatherRdd = weatherData.map(_.split(","))
      .map(row => NYWeatherParse(row))
      .filter(!_.isWrongRow())
      .filter(_.HourLocal == Some(12))

    // Join with bike table
    sqlContext.registerRDDAsTable(weatherRdd, "weatherRdd")
    sqlContext.registerRDDAsTable(asSchemaRDD(finalTable), "bikesRdd")
    //sql("SET spark.sql.shuffle.partitions=20")
    val bikesWeatherRdd = sql(
      """SELECT b.Days, b.start_station_id, b.bikes, b.Month, b.DayOfWeek,
        |w.DewPoint, w.HumidityFraction, w.Prcp1Hour, w.Temperature, w.WeatherCode1
        | FROM bikesRdd b
        | JOIN weatherRdd w
        | ON b.Days = w.Days
        |
      """.stripMargin)


    // And make prediction again but now on RDD
    buildModel(bikesWeatherRdd)

    // Print timing results
    println(gTimer)

    sc.stop()
  }

  def r2(model: GBMModel, fr: Frame) =  ModelMetrics.getFromDKV(model, fr).asInstanceOf[ModelMetricsSupervised].r2()

  def buildModel(df: DataFrame)(implicit gTimer: GTimer, h2oContext: H2OContext) = {
    import h2oContext._
    //
    // Split into train and test parts
    //
    gTimer.start()
    val keys = Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make(_))
    val ratios = Array[Double](0.6, 0.3, 0.1)
    val frs = ShuffleSplitFrame.shuffleSplitFrame(df, keys, ratios, 1234567689L)
    val train = frs(0)
    val test = frs(1)
    val hold = frs(2)
    gTimer.stop("H2O: split frame")

    //
    // Launch GBM prediction
    //
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = test
    gbmParams._response_column = 'bikes
    gbmParams._ntrees = TREES
    gbmParams._max_depth = 6

    gTimer.start()
    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get
    gTimer.stop("H2O: gbm model training")

    gbmModel.score(train).remove()
    gbmModel.score(test).remove()
    gbmModel.score(hold).remove()

    println(
      s"""
         |r2 on train: ${r2(gbmModel, train)}
          |r2 on test:  ${r2(gbmModel, test)}
          |r2 on hold:  ${r2(gbmModel, hold)}"""".stripMargin)
    train.delete()
    test.delete()
    hold.delete()

    gbmModel
  }

  def basicStats(brdd: SchemaRDD)(implicit sqlContext:SQLContext): Unit = {
    import sqlContext._

    // check Sri's first case
    brdd.first
    brdd.count

    // Register table and SQL table
    sqlContext.registerRDDAsTable(brdd, "brdd")

    val tGBduration = sql("SELECT bikeid, SUM(tripduration) FROM brdd GROUP BY bikeid")
    // Sort based on duration
    val bottom10 = tGBduration.sortBy( r => r.getLong(1)).take(10)

    // Get min
    import org.apache.spark.sql.Row
    val minDurationBikeId = tGBduration.min()(Ordering.by[Row, Long](r => r.getLong(1)))

    assert (bottom10(0) == minDurationBikeId)

    val maxDurationBikeId = tGBduration.min()(Ordering.by[Row, Long](r => -r.getLong(1)))
  }

  def withTimer(timer:GTimer, round: String)(b : => Unit): Unit = {
    timer.start()
    try {
      b
    } finally {
      timer.stop(round)
    }
  }
}

class TimeSplit extends MRTask[TimeSplit] {
  def doIt(time: DataFrame):DataFrame =
      new DataFrame(doAll(1, time).outputFrame(Array[String]("Days"), null))

  override def map(msec: Chunk, day: NewChunk):Unit = {
    for (i <- 0 until msec.len) {
      day.addNum(msec.at8(i) / (1000 * 60 * 60 * 24)); // Days since the Epoch
    }
  }
}

class TimeTransform extends MRTask[TimeSplit] {
  def doIt(days: DataFrame):DataFrame =
    new DataFrame(doAll(2, days).outputFrame(Array[String]("Month", "DayOfWeek"), null))

  override def map(in: Array[Chunk], out: Array[NewChunk]):Unit = {
    val days = in(0)
    val month = out(0)
    val dayOfWeek = out(1)
    val mdt = new MutableDateTime()
    for (i <- 0 until days.len) {
      val msec = days.at8(i) * (1000L*60*60*24)
      mdt.setMillis(msec)
      month.addNum(mdt.getMonthOfYear - 1)
      dayOfWeek.addNum(mdt.getDayOfWeek -1)
    }
  }
}

class GTimer {
  type T = (String, String)
  val timeList = new mutable.Queue[T]()
  var t:Timer = _

  def start():GTimer = {
    t = new Timer
    this
  }
  def stop(roundName: String):GTimer = {
    val item = roundName -> t.toString
    timeList += item
    t = null
    this
  }

  override def toString: String = {
    timeList.map(p=> s"   * ${p._1} : takes ${p._2}").mkString("------\nTiming\n------\n","\n", "\n------")
  }
}


