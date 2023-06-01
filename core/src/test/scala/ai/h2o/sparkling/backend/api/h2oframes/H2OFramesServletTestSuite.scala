package ai.h2o.sparkling.backend.api.h2oframes

import java.io.File

import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OFramesServletTestSuite extends FunSuite with SharedH2OTestContext with H2OFramesRestApi {

  override def createSparkSession(): SparkSession =
    sparkSession("local[*]", defaultSparkConf.set("spark.ext.h2o.context.path", "context"))

  test("H2OFramesHandler.toDataFrame() method") {
    val h2oFrame = H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv")))
    val result = convertToDataFrame(h2oFrame.frameId, "requested_name")
    val df = spark.table(result.dataframe_id)
    assert(
      spark.sqlContext.tableNames().contains("requested_name"),
      "DataFrame should be stored in table named \"requested_name\"")
    assert(df.columns.length == h2oFrame.numberOfColumns, "Number of columns should match")
    assert(df.columns.sameElements(h2oFrame.columnNames), "Column names should match")
    assert(df.count() == h2oFrame.numberOfRows, "Number of rows should match")
    assert(spark.sqlContext.tableNames().length == 1, "Number of stored DataFrames should be 1")
  }

  test("H2OFramesHandler.toDataFrame() method, trying to convert H2OFrame which does not exist") {
    intercept[RestApiCommunicationException] {
      convertToDataFrame("does_not_exist", null)
    }
  }
}
