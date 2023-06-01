package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class BinaryModelTestSuiteWithoutH2ORuntime extends FunSuite with Matchers with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test("Using Binary model when H2O Runtime is not available throws exception") {
    val thrown = intercept[IllegalArgumentException] {
      H2OBinaryModel.read("/path/not/required")
    }
    assert(thrown.getMessage == "To use features available on a binary model, H2O Context has to be running!")
  }

}
