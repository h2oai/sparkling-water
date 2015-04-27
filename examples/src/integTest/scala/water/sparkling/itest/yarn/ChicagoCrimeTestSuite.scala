package water.sparkling.itest.yarn

import hex.Model
import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.Activation
import hex.tree.gbm.GBMModel.GBMParameters.Family
import org.apache.spark.SparkContext
import org.apache.spark.examples.h2o.DemoUtils.configure
import org.apache.spark.h2o._
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.joda.time.DateTimeConstants._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, MutableDateTime}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.MRTask
import water.fvec.{Chunk, NewChunk, Vec}
import water.parser.ValueString
import water.sparkling.itest.SparkITest

/**
 * Test following Alex's chicago crime demo.
 */
@RunWith(classOf[JUnitRunner])
class ChicagoCrimeTestSuite extends FunSuite with SparkITest {

  test("Chicago Crime Demo") {
    launch( "water.sparkling.itest.yarn.ChicagoCrimeTest",
      env {
        sparkMaster("yarn-client")
        // Configure YARN environment
        conf("spark.yarn.max.executor.failures", 1) // In fail of executor, fail the test
        conf("spark.executor.instances", 3)
        conf("spark.executor.memory", "8g")
        conf("spark.ext.h2o.port.base", 63331)
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object ChicagoCrimeTest {

  val WEATHER_FILE = "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoAllWeather.csv"
  val CENSUS_FILE = "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCensus.csv"
  val CRIME_FILE = "hdfs://mr-0xd6-precise1.0xdata.loc/datasets/chicagoCrimes.csv"

  val DATE_PATTERN = "MM/dd/yyyy hh:mm:ss a"
  val DATETIME_ZONE = "Etc/UTC"

  def loadData(datafile: String): DataFrame = new DataFrame(new java.net.URI(datafile))

  def createWeatherTable(datafile: String): DataFrame = {
    val table = loadData(datafile)
    // Remove first column since we do not need it
    table.remove(0).remove()
    table.update(null)
    table
  }

  def createCensusTable(datafile: String): DataFrame = {
    val table = loadData(datafile)
    // Rename columns: replace ' ' by '_'
    val colNames = table.names().map( n => n.trim.replace(' ', '_').replace('+','_'))
    table._names = colNames
    table.update(null)
    table
  }

  def createCrimeTable(datafile: String): DataFrame = {
    val table = loadData(datafile)
    // Refine date into multiple columns
    val dateCol = table.vec(2)
    table.add(new RefineDateColumn().doIt(dateCol))
    // Update names, replace all ' ' by '_'
    val colNames = table.names().map( n => n.trim.replace(' ', '_'))
    table._names = colNames
    // Remove Date column
    table.remove(2).remove()
    // Update in DKV
    table.update(null)
    table
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(configure("ChicagoCrimeTest"))
    // SQL support
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext._
    // Start H2O services
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    // Weather data
    val weatherTable = asSchemaRDD(createWeatherTable(WEATHER_FILE))
    registerRDDAsTable(weatherTable, "chicagoWeather")
    // Census data
    val censusTable = asSchemaRDD(createCensusTable(CENSUS_FILE))
    registerRDDAsTable(censusTable, "chicagoCensus")
    // Crime data
    val crimeTable  = asSchemaRDD(createCrimeTable(CRIME_FILE))
    registerRDDAsTable(crimeTable, "chicagoCrime")

    weatherTable.printSchema()
    censusTable.printSchema()
    crimeTable.printSchema()

    //
    // Join crime data with weather and census tables
    //
    val crimeWeather = sql(
      """SELECT
        |a.Year, a.Month, a.Day, a.WeekNum, a.HourOfDay, a.Weekend, a.Season, a.WeekDay,
        |a.IUCR, a.Primary_Type, a.Location_Description, a.Community_Area, a.District,
        |a.Arrest, a.Domestic, a.Beat, a.Ward, a.FBI_Code,
        |b.minTemp, b.maxTemp, b.meanTemp,
        |c.PERCENT_AGED_UNDER_18_OR_OVER_64, c.PER_CAPITA_INCOME, c.HARDSHIP_INDEX,
        |c.PERCENT_OF_HOUSING_CROWDED, c.PERCENT_HOUSEHOLDS_BELOW_POVERTY,
        |c.PERCENT_AGED_16__UNEMPLOYED, c.PERCENT_AGED_25__WITHOUT_HIGH_SCHOOL_DIPLOMA
        |FROM chicagoCrime a
        |JOIN chicagoWeather b
        |ON a.Year = b.year AND a.Month = b.month AND a.Day = b.day
        |JOIN chicagoCensus c
        |ON a.Community_Area = c.Community_Area_Number""".stripMargin)

    crimeWeather.printSchema()
    val crimeWeatherDF:DataFrame = crimeWeather

    //
    // Split final data table
    //
    import org.apache.spark.examples.h2o.DemoUtils._

    val keys = Array[String]("train.hex", "test.hex")
    val ratios = Array[Double](0.8, 0.2)
    val frs = splitFrame(crimeWeatherDF, keys, ratios)
    val (train, test) = (frs(0), frs(1))

    //
    // Build GBM model
    //
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = train
    gbmParams._valid = test
    gbmParams._response_column = 'Arrest
    gbmParams._ntrees = 10
    gbmParams._max_depth = 6
    gbmParams._distribution = Family.bernoulli

    val gbm = new GBM(gbmParams)
    val gbmModel = gbm.trainModel.get
    // Score on train/test data
    gbmModel.score(train).delete()
    gbmModel.score(test).delete()
    val trainMetricsGBM = binomialMM(gbmModel, train)
    val testMetricsGBM = binomialMM(gbmModel, test)

    //
    // Build Deep Learning model
    //
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._valid = test
    dlParams._response_column = 'Arrest
    dlParams._epochs = 10
    dlParams._l1 = 0.0001
    dlParams._l2 = 0.0001
    dlParams._activation = Activation.RectifierWithDropout
    dlParams._hidden = Array[Int](200, 200)

    // Create a job
    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get
    // Score
    dlModel.score(train).delete()
    dlModel.score(test).delete()
    val trainMetricsDL = binomialMM(dlModel, train)
    val testMetricsDL = binomialMM(dlModel, test)

    //
    // Print Scores of GBM & Deep Learning
    //
    println(
      s"""
        |GBM:
        |  train AUC = ${trainMetricsGBM.auc._auc}
        |  test  AUC = ${testMetricsGBM.auc._auc}
        |DL:
        |  train AUC = ${trainMetricsDL.auc._auc}
        |  test  AUC = ${testMetricsDL.auc._auc}
      """.stripMargin)

    val crimes = Seq( Crime("02/08/2015 11:43:58 PM", 1811, "NARCOTICS", Some("STREET"),false, 422, 4, 7, 46, 18),
                      Crime("02/08/2015 11:00:39 PM", 1150, "DECEPTIVE PRACTICE", Some("RESIDENCE"),false, 923, 9, 14, 63, 11))
    for (crime <- crimes) {
      val arrestProbGBM = 100*scoreEvent(crime, gbmModel, censusTable)(sqlContext, h2oContext)
      val arrestProbDL = 100*scoreEvent(crime, dlModel, censusTable)(sqlContext, h2oContext)
      println(
        s"""
           |Crime: $crime
           |  Probability of arrest best on DeepLearning: ${arrestProbDL} %
           |  Probability of arrest best on GBM: ${arrestProbGBM} %
        """.stripMargin)
    }

    // Shutdown Spark
    sc.stop()
    // Shutdown H2O explicitly (at least the driver)
    water.H2O.shutdown()
  }

  /**
   * For given crime and model returns probability of crime.
   *
   * @param crime
   * @param model
   * @param censusTable
   * @param sqlContext
   * @param h2oContext
   * @return
   */
  def scoreEvent(crime: Crime, model: Model[_,_,_], censusTable: SchemaRDD)
                (implicit sqlContext: SQLContext, h2oContext: H2OContext): Float = {
    import sqlContext._
    import h2oContext._
    // Create a single row table
    val srdd:SchemaRDD = sqlContext.sparkContext.parallelize(Seq(crime))
    // Join table with census data
    val row: DataFrame = censusTable.join(srdd, on = Option('Community_Area === 'Community_Area_Number)) //.printSchema
    val predictTable = model.score(row)
    val probOfArrest = predictTable.vec("true").at(0)

    probOfArrest.toFloat
  }

  case class Crime(Year: Short, Month: Byte, Day: Byte, WeekNum: Byte, HourOfDay: Byte,
                   Weekend:Byte, Season: String, WeekDay: Byte,
                   IUCR: Short,
                   Primary_Type: String,
                   Location_Description: Option[String],
                   Domestic: String,
                   Beat: Short,
                   District: Byte,
                   Ward: Byte,
                   Community_Area: Byte,
                   FBI_Code: Byte,
                   minTemp: Option[Byte],
                   maxTemp: Option[Byte],
                   meanTemp: Option[Byte])

  object Crime {
     def apply(date:String,
              iucr: Short,
              primaryType: String,
              locationDescr: Option[String],
              domestic: Boolean,
              beat: Short,
              district: Byte,
              ward: Byte,
              communityArea: Byte,
              fbiCode: Byte,
              minTemp: Option[Byte] = None,
              maxTemp: Option[Byte] = None,
              meanTemp: Option[Byte] = None,
              datePattern: String = "MM/dd/yyyy hh:mm:ss a",
              dateTimeZone: String = "Etc/UTC"):Crime = {
      val dtFmt = DateTimeFormat.forPattern(datePattern).withZone(DateTimeZone.forID(dateTimeZone))
      val mDateTime = new MutableDateTime()
      dtFmt.parseInto(mDateTime, date, 0)
      val month = mDateTime.getMonthOfYear.toByte
      val dayOfWeek = mDateTime.getDayOfWeek

      Crime(mDateTime.getYear.toShort,
            month,
            mDateTime.getDayOfMonth.toByte,
            mDateTime.getWeekOfWeekyear.toByte,
            mDateTime.getHourOfDay.toByte,
            if (dayOfWeek == SUNDAY || dayOfWeek == SATURDAY) 1 else 0,
            SEASONS(getSeason(month)),
            mDateTime.getDayOfWeek.toByte,
            iucr, primaryType, locationDescr,
            if (domestic) "true" else "false" ,
            beat, district, ward, communityArea, fbiCode,
            minTemp, maxTemp, meanTemp)
    }
  }


  /**
   * Adhoc date column refinement.
   *
   * It takes column in format 'MM/dd/yyyy hh:mm:ss a' and refines
   * it into 8 columns: "Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend", "Season", "HourOfDay"
   */
  private class RefineDateColumn(val datePattern: String = DATE_PATTERN,
                                 val dateTimeZone: String = DATETIME_ZONE) extends MRTask[RefineDateColumn] {
    // Entry point
    def doIt(col: Vec): DataFrame = new DataFrame(
                    doAll(8, col).outputFrame(
                      Array[String]("Day", "Month", "Year", "WeekNum", "WeekDay", "Weekend", "Season", "HourOfDay"),
                      Array[Array[String]](null, null, null, null, null, null,
                                           SEASONS, null)))

    override def map(cs: Array[Chunk], ncs: Array[NewChunk]): Unit = {
      // Initialize DataTime convertor (cannot be done in setupLocal since it is not H2O serializable :-/
      val dtFmt = DateTimeFormat.forPattern(datePattern).withZone(DateTimeZone.forID(dateTimeZone))
      // Get input and output chunks
      val dateChunk = cs(0)
      val (dayNC, monthNC, yearNC, weekNC, weekdayNC, weekendNC, seasonNC, hourNC)
          = (ncs(0), ncs(1), ncs(2), ncs(3), ncs(4), ncs(5), ncs(6), ncs(7))
      val valStr = new ValueString()
      val mDateTime = new MutableDateTime()
      for(row <- 0 until dateChunk.len()) {
        if (dateChunk.isNA(row)) {
          addNAs(ncs)
        } else {
          // Extract data
          val ds = dateChunk.atStr(valStr, row).toString
          if (dtFmt.parseInto(mDateTime, ds, 0) > 0) {
            val month = mDateTime.getMonthOfYear
            dayNC.addNum(mDateTime.getDayOfMonth, 0)
            monthNC.addNum(month, 0)
            yearNC.addNum(mDateTime.getYear, 0)
            weekNC.addNum(mDateTime.getWeekOfWeekyear)
            val dayOfWeek = mDateTime.getDayOfWeek
            weekdayNC.addNum(dayOfWeek)
            weekendNC.addNum(if (dayOfWeek == SUNDAY || dayOfWeek == SATURDAY) 1 else 0, 0)
            seasonNC.addNum(getSeason(month), 0)
            hourNC.addNum(mDateTime.getHourOfDay)
          } else {
            addNAs(ncs)
          }
        }
      }
    }

    private def addNAs(ncs: Array[NewChunk]): Unit = ncs.foreach(nc => nc.addNA())
  }

  val SEASONS = Array[String]("Spring", "Summer", "Autumn", "Winter")
  private def getSeason(month: Int) =
    if (month >= MARCH && month <= MAY) 0 // Spring
    else if (month >= JUNE && month <= AUGUST) 1 // Summer
    else if (month >= SEPTEMBER && month <= OCTOBER) 2 // Autumn
    else 3 // Winter

}
