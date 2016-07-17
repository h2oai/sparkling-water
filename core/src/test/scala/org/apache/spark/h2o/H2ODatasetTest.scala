package org.apache.spark.h2o

import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec

/**
  * Testing schema for h2o schema spark dataset transformation.
  * Created by vpatryshev on 7/15/16.
  */

case class SamplePerson(name: String, age: Int, email: String)

@RunWith(classOf[JUnitRunner])
class H2ODatasetTest  extends FunSuite with SharedSparkTestContext {
  val people:List[SamplePerson] = SamplePerson("Hermione Granger", 15, "hgranger@griffindor.edu.uk")::
               SamplePerson("Ron Weasley", 14, "rweasley@griffindor.edu.uk")::
               SamplePerson("Harry Potter", 14, "hpotter@griffindor.edu.uk")::Nil
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Dataset[SamplePerson] to H2OFrame and back") {
    val sqlContext = hsc.sqlContext
//    val rdd:RDD[Product] = sqlContext.sparkContext.parallelize(people)
//    val df = rdd.toDF()
//    df.registerTempTable("TestDataSamplePerson")

    import sqlContext.implicits._

    val ds = sqlContext.createDataset(people)//(encoderFor[SamplePerson])

    val h2oFrame:H2OFrame = hsc.asH2OFrame(ds)

    assertBasicInvariants(ds, h2oFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at8(row) // value stored at row-th
      assert (row1 == value, "The H2OFrame values should match row numbers+1")
    })
//    val backToDs = hsc.asDataset

    val asrdd = hc.asRDD[SamplePerson](h2oFrame)
    assert(ds.count == h2oFrame.numRows(), "Number of rows should match")
    assert(asrdd.count == h2oFrame.numRows(), "Number of rows should match")
    // Clean up
    h2oFrame.delete()
    ds.unpersist()
  }

  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T<:Product](ds: Dataset[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertHolderProperties(df)
    assert (ds.count == df.numRows(), "Number of rows in H2OFrame and Dataset should match")
    // Check numbering
    val vec = df.vec(0)
    var row = 0
    while(row < df.numRows()) {
      assert (!vec.isNA(row), "The H2OFrame should not contain any NA values")
      rowAssert (row, vec)
      row += 1
    }
  }

  private def assertHolderProperties(df: H2OFrame): Unit = {

    assert (df.numCols() == 1, s"H2OFrame should contain single column, have ${df.numCols()}")
    assert (df.names().length == 1, s"H2OFrame column names should have single value, have ${df.names().length}")
    assert (df.names()(0).equals("result"),
      "H2OFrame column name should be 'result' since Holder object was used to define Dataset")
  }
}
