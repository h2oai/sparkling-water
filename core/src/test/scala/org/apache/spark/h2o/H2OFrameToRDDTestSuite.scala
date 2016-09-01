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
package org.apache.spark.h2o

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.TestData._
import org.apache.spark.h2o.utils._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec
import water.parser.{BufferedString, Categorical}

/**
  * Testing schema for rdd  to h2o frame transformations.
  */
@RunWith(classOf[JUnitRunner])
class H2ORDDTest extends TestBase with SharedSparkTestContext {
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("int iterator does not get stuck") {
    val h2oContext = hc
    import h2oContext.implicits._
    val rdd = sc.parallelize(1 to 10, 10).map(i => IntHolder(Some(i)))
    val h2oFrame: H2OFrame = rdd

    import StaticStorage._

    def dupChecker(iter: Iterator[IntHolder]): Unit = {
      iter foreach intIteratorTestMemory.put
    }
    try {
      sc.runJob(rdd, dupChecker _)
    } catch {
      case x: Exception => abort(x)
    }

    assert(intIteratorTestMemory.size == 10)
  }

  test("product iterator does not get stuck") {
    val h2oContext = hc
    import h2oContext.implicits._
    val rdd = sc.parallelize(1 to 10, 10).map(i => IntHolder(Some(i)))
    val h2oFrame: H2OFrame = rdd
    val numRows = h2oFrame.numRows()

    import StaticStorage._

    val back2rdd = hc.asRDD[PUBDEV458Type](h2oFrame)

    def dupChecker(iterator: Iterator[PUBDEV458Type]) = {
      iterator foreach pubdev458TestMemory.put
    }

    val c1 = rdd.count
    assert(c1 == numRows, "Number of rows should match")

    sc.runJob(back2rdd, dupChecker _)

    val c2 = back2rdd.count

    assert(c2 == numRows, "Number of rows should match")
  }

  test("RDD[IntHolder] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => IntHolder(Some(v)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      val value = vec.at8(rowIdx) // value stored at rowIdx-th
      assert (nextRowIdx == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[IntHolder] with nulls, does it hold nulls?") {

    def almostDefined(i: Long) = Some(i.toInt) filter (_ % 31 != 0)

    val rdd = sc.parallelize(1 to 1000, 100).map( v => IntHolder(almostDefined(v)))
    val h2oFrame:H2OFrame = hc.asH2OFrame(rdd)

    assertInvariantsWithNulls(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      almostDefined(nextRowIdx) match {
        case Some(r) =>
          val value = vec.at8(rowIdx) // value stored at rowIdx-th
          assert (r == value, s"The H2OFrame values should match row numbers+1 = $r")
        case None =>
          assert(vec.isNA(rowIdx), s"Row at $rowIdx must be NA")
      }
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[DoubleHolder] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => DoubleHolder(Some(v)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      val value = vec.at(rowIdx) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert (nextRowIdx == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[ByteHolder] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => ByteHolder(Some(v.toByte)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at8(row) // value stored at row-th
      // Using == since int should be mapped strictly to bytes
      assert(row1.toByte == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[ShortHolder] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => ShortHolder(Some(v.toShort)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at8(row) // value stored at row-th
      // Using == since int should be mapped strictly to shorts
      assert(row1 == value, "The H2OFrame values should match row numbers+1")
    })
    // TODO(vlad): this makes no sense, cleanup only happen on success; move it.
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[StringHolder] to H2OFrame[Enum] and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => StringHolder(Some(v.toString)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assert(h2oFrame.vec(0).isString, "The vector type should be of string type")
    assert(h2oFrame.vec(0).domain() == null, "The vector domain should be <null>")

    // Transform string vector to categorical
    h2oFrame.replace(0, h2oFrame.vec(0).toCategoricalVec).remove()

    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val dom = vec.domain()
      val value = dom(vec.at8(rowIdx).asInstanceOf[Int]) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert (rowIdx + 1 == value.toInt, "The H2OFrame values should match row numbers")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[StringHolder] to H2OFrame[String] and back") {

    val rdd = sc.parallelize(1 to (Categorical.MAX_CATEGORICAL_COUNT + 1), 100).map(v => StringHolder(Some(v.toString)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assert(h2oFrame.vec(0).isString, "The vector type should be string")
    assert(h2oFrame.vec(0).domain() == null, "The vector should have null domain")
    val valueString = new BufferedString()
    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = (rowIdx + 1).toString
      val value = vec.atStr(valueString, rowIdx) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert (nextRowIdx.equals(value.toString), "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("PUBDEV-458 - from Rdd[IntHolder] to H2OFrame and back") {
    val h2oContext = hc
    import h2oContext.implicits._
    val rdd = sc.parallelize(1 to 1000000, 10).map(i => IntHolder(Some(i)))
    val h2oFrame: H2OFrame = rdd
    val back2rdd = hc.asRDD[PUBDEV458Type](h2oFrame)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    assert(back2rdd.count == h2oFrame.numRows(), "Number of rows should match")
  }

  test("Options handled correctly (no null -> 0)") {
    val h2oContext = hc
    import h2oContext.implicits._

    def almostDefined(i: Long) = Some(i.toInt) filter (_ % 31 != 0)

    val rdd = sc.parallelize(1 to 1000000, 10).map(i => OptionAndNot(Some(i), almostDefined(i)))
    val h2oFrame:H2OFrame = rdd
    val back2rdd = hc.asRDD[OptionAndNot](h2oFrame)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    assert(back2rdd.count == h2oFrame.numRows(), "Number of rows should match")

    back2rdd.foreach {
      case OptionAndNot(x, xOpt) => if(xOpt != x.flatMap(i => almostDefined(i))) throw new IllegalStateException(s"Failed at $x/$xOpt")
    }
  }

  // PUBDEV-1173
  test("RDD[Int] to H2OFrame[Numeric]") {
    // Create RDD with 100 Int values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10)
    // PUBDEV-1173
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[Float] to H2OFrame[Numeric]") {
    // Create RDD with 100 Float values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toFloat)
    // PUBDEV-1173
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[Double] to H2OFrame[Numeric]") {
    // Create RDD with 100 Double values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toDouble)
    // PUBDEV-1173
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[String] to H2OFrame[String]") {
    // Create RDD with 100 String values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toString)
    // PUBDEV-1173
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[Byte] to H2OFrame[Numeric]") {
    // Create RDD with 100 Byte values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toByte)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[Short] to H2OFrame[Numeric]") {
    // Create RDD with 100 Short values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toShort)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[java.sql.Timestamp] to H2OFrame[Time]") {
    // Create RDD with 100 Timestamp values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => new Timestamp(v.toLong))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[LabeledPoint] ( Dense Vector, same size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.dense(1, 2, 3, 4))
    val p2 = LabeledPoint(1, Vectors.dense(5, 6, 7, 8))
    val rdd = sc.parallelize(Seq(p1, p2), 10)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  test("RDD[LabeledPoint] ( Dense Vector, different size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.dense(1, 2, 3, 4, 5, 6))
    val p2 = LabeledPoint(1, Vectors.dense(7, 8))
    val rdd = sc.parallelize(Seq(p1, p2))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  test("RDD[LabeledPoint] ( Sparse Vector, same size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.sparse(4, Array(1, 3), Array(3.0, 8.1)))
    val p2 = LabeledPoint(1, Vectors.sparse(4, Array(0), Array(3.14)))
    val rdd = sc.parallelize(Seq(p1, p2), 10)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  test("RDD[LabeledPoint] ( Sparse Vector, different size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.sparse(4, Array(1, 3), Array(3.0, 8.1)))
    val p2 = LabeledPoint(1, Vectors.sparse(7, Array(0, 5, 6), Array(1.0, 11, 8)))
    val rdd = sc.parallelize(Seq(p1, p2))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T <: Product](rdd: RDD[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertHolderProperties(df)
    assert(rdd.count == df.numRows(), "Number of rows in H2OFrame and RDD should match")
    // Check numbering
    val vec = df.vec(0)
    var rowIdx = 0
    while(rowIdx < df.numRows()) {
      assert (!vec.isNA(rowIdx), "The H2OFrame should not contain any NA values")
      rowAssert (rowIdx, vec)
      rowIdx += 1
    }
  }

  private def assertInvariantsWithNulls[T<:Product](rdd: RDD[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertHolderProperties(df)
    assert(rdd.count == df.numRows(), "Number of rows in H2OFrame and RDD should match")
    // Check numbering

    val vec = df.vec(0)
    var rowIdx = 0
    while (rowIdx < df.numRows()) {
      rowAssert(rowIdx, vec)
      rowIdx += 1
    }
  }

  private def assertHolderProperties(df: H2OFrame): Unit = {
    assert(df.numCols() == 1, "H2OFrame should contain single column")
    assert(df.names().length == 1, "H2OFrame column names should have single value")
    assert(df.names()(0).equals("result"),
      "H2OFrame column name should be 'result' since Holder object was used to define RDD")
  }
}

object StaticStorage {
  val intIteratorTestMemory = new TestMemory[Int]
  val pubdev458TestMemory = new TestMemory[PUBDEV458Type]
}

class PUBDEV458Type(val result: Option[Int]) extends Product with Serializable {
  override def canEqual(that: Any):Boolean = that.isInstanceOf[PUBDEV458Type]
  override def productArity: Int = 1
  override def productElement(n: Int) =
    n match {
      case 0 => result
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }
}

case class OptionAndNot(val x: Option[Int], val xOpt: Option[Int]) extends Serializable {
  override def canEqual(that: Any):Boolean = that.isInstanceOf[OptionAndNot]
  override def productArity: Int = 2
  override def productElement(n: Int) =
    n match {
      case 0 => x
      case 1 => xOpt
      case _ => throw new IndexOutOfBoundsException(n.toString)
    }
}
