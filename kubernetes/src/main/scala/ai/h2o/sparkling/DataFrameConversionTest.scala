package ai.h2o.sparkling

import java.net.URI

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

/**
  * When running on Kubernetes, H2O-3 disables servlets on non-leader nodes to avoid using a misconfigured cluster.
  * Yet there are servlets required to be accessible on each node by Sparkling water - those servlets are used by SW
  * to upload chunks of data to each node.
  *
  * This test verifies the data upload servlets are active while running on Kubernetes.
  */
object DataFrameConversionTest {

  def run(spark: SparkSession, hc: H2OContext): Unit = {
    import spark.implicits._
    spark.sparkContext.addFile(
      "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
    val frame = H2OFrame(new URI("file://" + SparkFiles.get("prostate.csv")))
    val sparkDF = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string").repartition(10)
    val h2oFrame = hc.asH2OFrame(sparkDF)

    assert(h2oFrame.columnNames.toSeq == sparkDF.columns.toSeq)
    assert(h2oFrame.numberOfRows == sparkDF.count())
  }

}
