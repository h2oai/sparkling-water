package org.apache.spark.ml.h2o.features

import org.apache.spark.SparkContext
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ColumnVectorizerTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext(
    "local[*]", "test-local",
    conf = defaultSparkConf
  )

  test("Should vectorize features") {
    val h2oContext = H2OContext.getOrCreate(sc)
    import spark.implicits._
    val inp = sc.parallelize(Array(
      // TODO test for strings
      Seq(1,2,3)
    )).toDF()

    val cv = new ColumnVectorizer()
      .setKeep(true)
      .setColumns(Array("test", "test2"))
      .setOutputCol("features")
//    cv.transform(inp).collect().foreach(println)

  }

}
