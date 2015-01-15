package org.apache.spark.rdd

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{IntHolder, H2OContext}
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite
import water.fvec.DataFrame

/**
 * Testing schema for h2o schema rdd transformation.
 */
// FIXME this should be only trait but used in different SparkContext
class H2ORDDTest extends FunSuite with SparkTestContext {

  sc = new SparkContext("local[*]", "test-local")
  hc = new H2OContext(sc).start()

  test("RDD[IntHolder] to DataFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(Some(v)))
    val dataFrame:DataFrame = hc.createDataFrame(rdd)
    assertHolderProperties (dataFrame)
    assert (rdd.count == dataFrame.numRows(), "Number of rows in DataFrame and RDD should match")
    // Check numbering
    dataFrame.foreach( x => {
      val row1 = 1+x._1 // make row 1-based
      val value = x._2(0).getOrElse( assert(false, "Data should not contain NaN") ) // there is single column
      assert (row1 == value, "Row numbers should match values stored in original RDD")
    })
  }

  private def assertHolderProperties(df: DataFrame): Unit = {
    assert (df.numCols() == 1, "DataFrame should contain single column")
    assert (df.names().length == 1, "DataFrame column names should have single value")
    assert (df.names()(0).equals("result"),
      "DataFrame column name should be 'result' since Holder object was used to define RDD")
  }

}
