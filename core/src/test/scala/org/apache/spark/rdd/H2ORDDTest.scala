package org.apache.spark.rdd

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{StringHolder, DoubleHolder, IntHolder, H2OContext}
import org.apache.spark.h2o.util.{SharedSparkTestContext, SparkTestContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite
import water.fvec.{Vec, DataFrame}
import water.parser.ValueString

/**
 * Testing schema for h2o schema rdd transformation.
 */
// FIXME this should be only trait but used in different SparkContext
class H2ORDDTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local")

  test("RDD[IntHolder] to DataFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(Some(v)))
    val dataFrame:DataFrame = hc.createDataFrame(rdd)

    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at8(row) // value stored at row-th
      assert (row1 == value, "The DataFrame values should match row numbers+1")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }

  test("RDD[DoubleHolder] to DataFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => DoubleHolder(Some(v)))
    val dataFrame:DataFrame = hc.createDataFrame(rdd)

    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at(row) // value stored at row-th
      // Using == since int should be mapped strictly to doubles
      assert (row1 == value, "The DataFrame values should match row numbers+1")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }

  test("RDD[StringHolder] to DataFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => StringHolder(Some(v.toString)))
    val dataFrame:DataFrame = hc.createDataFrame(rdd)

    val valueString = new ValueString()
    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val row1 = (row + 1).toString
      val value = vec.atStr(valueString, row) // value stored at row-th
      // Using == since int should be mapped strictly to doubles
      assert (row1.equals(value.toString), "The DataFrame values should match row numbers+1")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }


  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T<:Product](rdd: RDD[T], df: DataFrame, rowAssert: RowValueAssert): Unit = {
    assertHolderProperties(df)
    assert (rdd.count == df.numRows(), "Number of rows in DataFrame and RDD should match")
    // Check numbering
    val vec = df.vec(0)
    var row = 0
    while(row < df.numRows()) {
      assert (!vec.isNA(row), "The DataFrame should not contain any NA values")
      rowAssert (row, vec)
      row += 1
    }
  }

  private def assertHolderProperties(df: DataFrame): Unit = {
    assert (df.numCols() == 1, "DataFrame should contain single column")
    assert (df.names().length == 1, "DataFrame column names should have single value")
    assert (df.names()(0).equals("result"),
      "DataFrame column name should be 'result' since Holder object was used to define RDD")
  }
}
