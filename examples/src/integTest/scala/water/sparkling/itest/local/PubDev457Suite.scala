package water.sparkling.itest.local

import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.app.SparkContextSupport
import water.sparkling.itest.{LocalTest, IntegTestHelper}

/**
  * PUBDEV-457 test suite.
  */
@RunWith(classOf[JUnitRunner])
class PubDev457Suite extends FunSuite with IntegTestHelper {

  test("Launch simple ML pipeline using H2O", LocalTest) {
    launch("water.sparkling.itest.local.PubDev457Test",
      env {
        sparkMaster("local-cluster[3,2,2048]")
        conf("spark.executor.memory", "2g")
        conf("spark.driver.memory", "2g")
      }
    )
  }
}

object PubDev457Test extends SparkContextSupport {

  case class LabeledDocument(id: Long, text: String, label: Double)

  case class Document(id: Long, text: String)

  def main(args: Array[String]): Unit = {
    val conf = configure("PUBDEV-457")
    val sc = new SparkContext(conf)
    val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext.implicits._
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val training = sc.parallelize(Seq(
      LabeledDocument(0L, "a b c d e spark", 1.0),
      LabeledDocument(1L, "b d", 0.0),
      LabeledDocument(2L, "spark f g h", 1.0),
      LabeledDocument(3L, "hadoop mapreduce", 0.0)))

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF))
    val model = pipeline.fit(training.toDF)
    val transformed = model.transform(training.toDF)

    val transformedDF: H2OFrame = transformed
    assert(transformedDF.numRows == 4)
    assert(transformedDF.numCols == 1009)

    val transformedFeaturesDF: H2OFrame = transformed.select("features")
    assert(transformedFeaturesDF.numRows == 4)
    assert(transformedFeaturesDF.numCols == 1000)

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }
}