package water.sparkling.itest.local

import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.DataFrame
import water.sparkling.itest.{LocalTest, SparkITest}

/**
 * PUBDEV-457 test suite.
 */
@RunWith(classOf[JUnitRunner])
class PubDev457TestSuite extends FunSuite with SparkITest {

  test("Launch simple ML pipepile using H2O", LocalTest) {
    launch( "org.apache.spark.examples.h2o.AirlinesWithWeatherDemo",
      env {
        sparkMaster("local-cluster[3,2,1024]")
      }
    )
  }
}

object PubDev457Test {

  case class LabeledDocument(id: Long, text: String, label: Double)
  case class Document(id: Long, text: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PubDev457").setMaster("local")
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._
    val sqlContext = new SQLContext(sc)
    import sqlContext._

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
    val model = pipeline.fit(training)
    val transformed = model.transform(training)

    val transformedDF: DataFrame = transformed
    assert (transformedDF.numRows == 4)
  }
}