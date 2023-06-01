package ai.h2o.sparkling.examples

import java.io.File

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.algos.H2ODeepLearning
import org.apache.spark.sql.SparkSession

object DeepLearningDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Deep Learning Demo on Airlines Data")
      .getOrCreate()
    import spark.implicits._

    val airlinesDataPath = "./examples/smalldata/airlines/allyears2k_headers.csv"
    val airlinesDataFile = s"file://${new File(airlinesDataPath).getAbsolutePath}"
    val airlinesTable = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .csv(airlinesDataFile)

    println(s"\n===> Number of all flights: ${airlinesTable.count()}\n")

    val airlinesSFO = airlinesTable.filter('Dest === "SFO")
    println(s"\n===> Number of flights with destination in SFO: ${airlinesSFO.count()}\n")

    println("\n====> Running DeepLearning on the prepared data frame\n")

    val train = airlinesSFO.select('Year, 'Month, 'DayofMonth, 'DayOfWeek, 'CRSDepTime, 'CRSArrTime, 'UniqueCarrier,
      'FlightNum, 'TailNum, 'CRSElapsedTime, 'Origin, 'Dest, 'Distance, 'IsDepDelayed)

    H2OContext.getOrCreate()
    val dl = new H2ODeepLearning()
      .setLabelCol("IsDepDelayed")
      .setConvertUnknownCategoricalLevelsToNa(true)
    val count = train.count()
    val model = dl.fit(train.repartition(count.toInt + 10)) // verify also fitting on empty partitions

    val predictions = model.transform(airlinesSFO).select("prediction").collect()
    println(predictions.mkString("\n===> Model predictions from DL: ", ", ", ", ...\n"))
  }
}
