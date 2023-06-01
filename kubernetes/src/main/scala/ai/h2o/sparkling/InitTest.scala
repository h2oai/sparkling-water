package ai.h2o.sparkling

import java.net.URI

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object InitTest {

  /**
    * Tests Kubernetes clustering and ability of the cluster to run XGBoost
    * @param spark A valid instance of SparkSession
    * @param hc A valid instance of H2OContext
    */
  def run(spark: SparkSession, hc: H2OContext): Unit = {
    import spark.implicits._
    spark.sparkContext.addFile(
      "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
    val frame = H2OFrame(new URI("file://" + SparkFiles.get("prostate.csv")))
    val sparkDF = hc
      .asSparkFrame(frame)
      .repartition(10)
      .withColumn("CAPSULE", $"CAPSULE" cast "string")
    val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

    import ai.h2o.sparkling.ml.algos.H2OXGBoost
    val estimator = new H2OXGBoost().setLabelCol("CAPSULE")
    val model = estimator.fit(trainingDF)
    model.transform(testingDF).collect()
  }
}
