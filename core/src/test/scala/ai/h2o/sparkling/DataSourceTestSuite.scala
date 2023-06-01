package ai.h2o.sparkling

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Test using H2O Frame as Spark SQL data source
  */
@RunWith(classOf[JUnitRunner])
class DataSourceTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  test("Reading H2OFrame using key option") {
    val rdd = sc.parallelize(1 to 1000)
    val h2oFrame = hc.asH2OFrame(rdd)
    val df = spark.read.format("h2o").option("key", h2oFrame.frameId).load()

    assert(df.columns.length == h2oFrame.numberOfColumns, "Number of columns should match")
    assert(df.columns.sameElements(h2oFrame.columnNames), "Column names should match")
    assert(df.count() == h2oFrame.numberOfRows, "Number of rows should match")
  }

  test("Reading H2OFrame using key in load method ") {
    val rdd = sc.parallelize(1 to 1000)
    val h2oFrame = hc.asH2OFrame(rdd)
    val df = spark.read.format("h2o").load(h2oFrame.frameId)

    assert(df.columns.length == h2oFrame.numberOfColumns, "Number of columns should match")
    assert(df.columns.sameElements(h2oFrame.columnNames), "Column names should match")
    assert(df.count() == h2oFrame.numberOfRows, "Number of rows should match")
  }

  test("Writing DataFrame to new H2O Frame ") {
    val df = sc.parallelize(1 to 1000).toDF()
    df.write.format("h2o").save("new_key")

    val h2oFrame = H2OFrame("new_key")
    assert(df.columns.length == h2oFrame.numberOfColumns, "Number of columns should match")
    assert(df.columns.sameElements(h2oFrame.columnNames), "Column names should match")
    assert(df.count() == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  test("Writing DataFrame to existing H2O Frame ") {
    val df = sc.parallelize(1 to 1000).toDF()
    df.write.format("h2o").save("new_key")

    val dfNew = sc.parallelize(1 to 1000).map(v => v.toString).toDF()

    val h2oFrame = H2OFrame("new_key")
    val thrown = intercept[RuntimeException] {
      dfNew.write.format("h2o").mode(SaveMode.ErrorIfExists).save("new_key")
    }

    assert(
      thrown.getMessage == "Frame with key 'new_key' already exists, if you want to override it set the save mode to override.")
    h2oFrame.delete()
  }

  test("Overwriting existing H2O Frame ") {
    val df = sc.parallelize(1 to 1000).toDF()
    df.write.format("h2o").save("new_key")

    val dfNew = sc.parallelize(1 to 100).map(v => v.toString).toDF()

    dfNew.write.format("h2o").mode(SaveMode.Overwrite).save("new_key")
    val h2oFrame = H2OFrame("new_key")

    assert(dfNew.columns.length == h2oFrame.numberOfColumns, "Number of columns should match")
    assert(dfNew.columns.sameElements(h2oFrame.columnNames), "Column names should match")
    assert(dfNew.count() == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  test("Writing to existing H2O Frame with ignore mode") {
    val df = sc.parallelize(1 to 1000).toDF()
    df.write.format("h2o").save("new_key")

    val dfNew = sc.parallelize(1 to 100).map(v => v.toString).toDF()
    dfNew.write.format("h2o").mode(SaveMode.Ignore).save("new_key")

    val h2oFrame = H2OFrame("new_key")

    assert(df.columns.length == h2oFrame.numberOfColumns, "Number of columns should match")
    assert(df.columns.sameElements(h2oFrame.columnNames), "Column names should match")
    assert(df.count() == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  test("Appending to existing H2O Frame ") {
    val df = sc.parallelize(1 to 1000).toDF()
    df.write.format("h2o").save("new_key")
    val dfNew = sc.parallelize(1 to 100).toDF()

    val thrown = intercept[RuntimeException] {
      dfNew.write.format("h2o").mode(SaveMode.Append).save("new_key")
    }
    assert(thrown.getMessage == "Appending to H2O Frame is not supported.")
  }
}
