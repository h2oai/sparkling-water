package ai.h2o.sparkling.examples

import java.io.File

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.algos.H2OKMeans
import org.apache.spark.sql.SparkSession

object ProstateDemo {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Prostate Demo")
      .getOrCreate()
    import spark.implicits._
    val prostateDataPath = "./examples/smalldata/prostate/prostate.csv"
    val prostateDataFile = s"file://${new File(prostateDataPath).getAbsolutePath}"

    val prostateTable = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(prostateDataFile)

    // Select a subsample
    val train = prostateTable.filter('CAPSULE === 1)

    H2OContext.getOrCreate()
    val kmeans = new H2OKMeans().setK(3)
    val model = kmeans.fit(train)
    val prediction = model.transform(train).select("prediction").collect()
    println(prediction.mkString("\n===> Model predictions: ", ", ", ", ...\n"))
  }
}
