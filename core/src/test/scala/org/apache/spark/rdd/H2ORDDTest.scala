/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.apache.spark.h2o._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec
import water.parser.{Categorical, BufferedString}

/**
 * Testing schema for h2o schema rdd transformation.
 */
// FIXME this should be only trait but used in different SparkContext
@RunWith(classOf[JUnitRunner])
class H2ORDDTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("RDD[IntHolder] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(Some(v)))
    val dataFrame:H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at8(row) // value stored at row-th
      assert (row1 == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }

  test("RDD[DoubleHolder] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => DoubleHolder(Some(v)))
    val dataFrame:H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at(row) // value stored at row-th
      // Using == since int should be mapped strictly to doubles
      assert (row1 == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }

  test("RDD[StringHolder] to H2OFrame[Enum] and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map( v => StringHolder(Some(v.toString)))
    val dataFrame : H2OFrame = hc.asH2OFrame(rdd)

    assert (dataFrame.vec(0).isString, "The vector type should be of string type")
    assert (dataFrame.vec(0).domain() == null, "The vector domain should be <null>")

    // Transform string vector to categorical
    dataFrame.replace(0, dataFrame.vec(0).toCategoricalVec).remove()

    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val dom = vec.domain()
      val value = dom(vec.at8(row).asInstanceOf[Int]) // value stored at row-th
      // Using == since int should be mapped strictly to doubles
      assert (row + 1 == value.toInt, "The H2OFrame values should match row numbers")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }

  test("RDD[StringHolder] to H2OFrame[String] and back") {

    val rdd = sc.parallelize(1 to (Categorical.MAX_CATEGORICAL_COUNT + 1), 100).map( v => StringHolder(Some(v.toString)))
    val dataFrame : H2OFrame = hc.asH2OFrame(rdd)

    assert (dataFrame.vec(0).isString, "The vector type should be string")
    assert (dataFrame.vec(0).domain() == null, "The vector should have null domain")
    val valueString = new BufferedString()
    assertBasicInvariants(rdd, dataFrame, (row, vec) => {
      val row1 = (row + 1).toString
      val value = vec.atStr(valueString, row) // value stored at row-th
      // Using == since int should be mapped strictly to doubles
      assert (row1.equals(value.toString), "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    dataFrame.delete()
    rdd.unpersist()
  }

  test("PUBDEV-458 - from Rdd[IntHolder] to H2OFrame and back") {
    val h2oContext = hc
    import h2oContext._

    val rdd = sc.parallelize(1 to 100, 10).map(i => IntHolder(Some(i)))
    val dataFrame:H2OFrame = rdd

    val back2rdd = asRDD[PUBDEV458Type](rdd)
    assert(rdd.count == dataFrame.numRows(), "Number of rows should match")
    assert(back2rdd.count == dataFrame.numRows(), "Number of rows should match")
  }

  // PUBDEV-1173
  test("RDD[Int] to H2OFrame[Numeric]") {
    // Create RDD with 100 Int values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10)
    // PUBDEV-1173
    val dataFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == dataFrame.numRows(), "Number of rows should match")
    dataFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[Float] to H2OFrame[Numeric]") {
    // Create RDD with 100 Float values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toFloat)
    // PUBDEV-1173
    val dataFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == dataFrame.numRows(), "Number of rows should match")
    dataFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[Double] to H2OFrame[Numeric]") {
    // Create RDD with 100 Double values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toDouble)
    // PUBDEV-1173
    val dataFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == dataFrame.numRows(), "Number of rows should match")
    dataFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[String] to H2OFrame[String]") {
    // Create RDD with 100 String values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toString)
    // PUBDEV-1173
    val dataFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == dataFrame.numRows(), "Number of rows should match")
    dataFrame.delete()
  }


  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T<:Product](rdd: RDD[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertHolderProperties(df)
    assert (rdd.count == df.numRows(), "Number of rows in H2OFrame and RDD should match")
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
    assert (df.numCols() == 1, "H2OFrame should contain single column")
    assert (df.names().length == 1, "H2OFrame column names should have single value")
    assert (df.names()(0).equals("result"),
      "H2OFrame column name should be 'result' since Holder object was used to define RDD")
  }
}

class PUBDEV458Type(val result: Option[Int]) extends Product with Serializable {
  //def this() = this(None)
  override def canEqual(that: Any):Boolean = that.isInstanceOf[PUBDEV458Type]
  override def productArity: Int = 1
  override def productElement(n: Int) =
    n match {
      case 0 => result
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }
}
