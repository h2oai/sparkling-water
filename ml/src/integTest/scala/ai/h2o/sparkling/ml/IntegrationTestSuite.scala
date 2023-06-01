package ai.h2o.sparkling.ml

import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class IntegrationTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local-cluster[2,1,2560]")

  test("SchemaUtils: flattenDataFrame should process a complex data frame with more than 180k columns after flattening") {
    val expectedNumberOfColumns = 180000
    val settings =
      TestUtils.GenerateDataFrameSettings(numberOfRows = 200, rowsPerPartition = 50, maxCollectionSize = 90)
    testFlatteningOnComplexType(settings, expectedNumberOfColumns)
  }

  test("SchemaUtils: flattenDataFrame should process a complex data frame with 100k rows and 2k columns") {
    val expectedNumberOfColumns = 2000
    val settings =
      TestUtils.GenerateDataFrameSettings(numberOfRows = 100000, rowsPerPartition = 10000, maxCollectionSize = 10)
    testFlatteningOnComplexType(settings, expectedNumberOfColumns)
  }

  private def testFlatteningOnComplexType(
      settings: TestUtils.GenerateDataFrameSettings,
      expectedNumberOfColumns: Int): Unit = {
    trackTime {
      val complexDF = TestUtils.generateDataFrame(spark, ComplexSchema, settings)
      val flattened = SchemaUtils.flattenDataFrame(complexDF)

      val fieldTypeNames = flattened.schema.fields.map(_.dataType.typeName)
      val numberOfFields = fieldTypeNames.length
      println(s"Number of columns: $numberOfFields")
      assert(numberOfFields > expectedNumberOfColumns)
      assert(fieldTypeNames.intersect(Array("struct", "array", "map")).isEmpty)
      flattened.foreach { _: Row =>
        {}
      }
    }
  }

  private def trackTime[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // evaluate block
    val t1 = System.nanoTime()
    val diff = Duration.fromNanos(t1 - t0)
    println(s"Elapsed time: ${diff.toSeconds} seconds")
    result
  }
}
