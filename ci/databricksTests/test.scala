import java.net.URI
import ai.h2o.sparkling._
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("App name").getOrCreate()
import spark.implicits._

val hc = H2OContext.getOrCreate()
val expected = 3
val actual = hc.getH2ONodes().length
if (actual != expected) {
  throw new RuntimeException(s"H2O cluster should be of size $expected but is $actual")
}
spark.sparkContext.addFile(
  "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
val frame = H2OFrame(new URI("file://" + SparkFiles.get("prostate.csv")))
val sparkDF = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string")
val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

import ai.h2o.sparkling.ml.algos.H2OXGBoost
val estimator = new H2OXGBoost().setLabelCol("CAPSULE")
val model = estimator.fit(trainingDF)

model.transform(testingDF).collect()
