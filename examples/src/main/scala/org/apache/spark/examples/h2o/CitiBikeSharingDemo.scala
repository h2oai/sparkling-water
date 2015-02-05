package org.apache.spark.examples.h2o

import java.io.File

import hex.tree.gbm.GBMModel
import hex.{ModelMetricsSupervised, Model, ModelMetrics, SupervisedModel}
import hex.splitframe.{ShuffleSplitFrame, SplitFrame}
import hex.splitframe.SplitFrameModel.SplitFrameParameters
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.examples.h2o.DemoUtils._
import org.joda.time.MutableDateTime
import water.{Key, MRTask}
import water.fvec._

/**
 * Citibike NYC Demo.
 *
 *
 */
object CitiBikeSharingDemo {

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = configure("Sparkling Water Meetup: Predict occupation of citi bike station in NYC")

    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    implicit val sqlContext = new SQLContext(sc)
    import sqlContext._

    //
    // Load data into H2O by using H2O parser
    //
    val dataf = new DataFrame(new File("/Users/michal/Devel/projects/h2o/repos/h2o2/bigdata/laptop/citibike-nyc/2013-09.csv"))

    //
    // Transform start time to days from Epoch
    //
    val startTimeF = dataf('starttime)
    // Add a new column
    dataf.add(new TimeSplit().doIt(startTimeF))
    println(dataf)

    //
    // Transform DataFrame into SchemaRDD
    //
    val brdd = asSchemaRDD(dataf)

    // Register table and SQL table
    sqlContext.registerRDDAsTable(brdd, "brdd")

    //
    // Do grouping
    //
    val bph = sql(
      """SELECT Days, start_station_id, count(*) bikes
        |FROM brdd
        |GROUP BY Days, start_station_id """.stripMargin)
    println(bph.take(10).mkString("\n"))

    val bphf:DataFrame = bph
    val daysVec = bphf('Days)
    val finalTable = bphf.add(new TimeTransform().doIt(daysVec))
    println(finalTable)

    //
    // Split into train and test parts
    //
    val keys = Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make(_))
    val ratios = Array[Double](0.6, 0.3, 0.1)
    val frs = ShuffleSplitFrame.shuffleSplitFrame(finalTable, keys, ratios, 1234567689L)
    val train = frs(0)
    val test = frs(1)
    val hold = frs(2)

    // Cleanup
    dataf.delete()

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

    sc.stop()
  }

  def r2(model: GBMModel, fr: Frame) =  ModelMetrics.getFromDKV(model, fr).asInstanceOf[ModelMetricsSupervised].r2()

  def basicStats(brdd: SchemaRDD)(implicit sqlContext:SQLContext): Unit = {
    import sqlContext._

    // check Sri's first case
    brdd.first
    brdd.count

    // Register table and SQL table
    sqlContext.registerRDDAsTable(brdd, "brdd")

    val tGBduration = sql("select bikeid, sum(tripduration) from brdd group by bikeid")
    // Sort based on duration
    val bottom10 = tGBduration.sortBy( r => r.getLong(1)).take(10)

    // Get min
    import org.apache.spark.sql.Row
    val minDurationBikeId = tGBduration.min()(Ordering.by[Row, Long](r => r.getLong(1)))

    assert (bottom10(0) == minDurationBikeId)

    val maxDurationBikeId = tGBduration.min()(Ordering.by[Row, Long](r => -r.getLong(1)))

  }

}

class TimeSplit extends MRTask[TimeSplit] {
  def doIt(time: DataFrame):DataFrame =
      DataFrame(doAll(1, time).outputFrame(Array[String]("Days"), null))

  override def map(msec: Chunk, day: NewChunk):Unit = {
    for (i <- 0 until msec.len) {
      day.addNum(msec.at8(i) / (1000 * 60 * 60 * 24)); // Days since the Epoch
    }
  }
}

class TimeTransform extends MRTask[TimeSplit] {
  def doIt(days: DataFrame):DataFrame =
    DataFrame(doAll(2, days).outputFrame(Array[String]("Month", "DayOfWeek"), null))

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


