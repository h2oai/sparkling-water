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
package ai.h2o.sparkling.backend.converters

import java.io.File
import java.sql.Timestamp

import ai.h2o.sparkling.TestUtils._
import ai.h2o.sparkling.backend.utils.H2OClientUtils
import ai.h2o.sparkling.{SharedH2OTestContext, SparkTimeZone, TestUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.H2OFrame
import water.parser.{BufferedString, Categorical}

@RunWith(classOf[JUnitRunner])
class SupportedRDDConverterTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test("Empty RDD to H2O frame, Byte type") {
    val rdd = sc.parallelize(Array.empty[Byte])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Short type") {
    val rdd = sc.parallelize(Array.empty[Short])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Integer type") {
    val rdd = sc.parallelize(Array.empty[Integer])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Long type") {
    val rdd = sc.parallelize(Array.empty[Long])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Float type") {
    val rdd = sc.parallelize(Array.empty[Float])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Double type") {
    val rdd = sc.parallelize(Array.empty[Double])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, String type") {
    val rdd = sc.parallelize(Array.empty[String])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Boolean type") {
    val rdd = sc.parallelize(Array.empty[Boolean])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Timestamp type") {
    val rdd = sc.parallelize(Array.empty[Timestamp])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Product type") {
    val rdd = sc.parallelize(Array.empty[ByteField])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("Empty RDD to H2O frame, Labeled point") {
    val rdd = sc.parallelize(Array.empty[LabeledPoint])
    val fr = hc.asH2OFrame(rdd)

    assert(fr.numCols() == 1)
    assert(fr.numRows() == 0)
  }

  test("H2OFrame[T_NUM] to RDD[Prostate]") {
    val h2oFrame: H2OFrame = new H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv")))
    assert(
      h2oFrame.vec(0).isNumeric & h2oFrame.vec(1).isNumeric & h2oFrame.vec(2).isNumeric &
        h2oFrame.vec(3).isNumeric & h2oFrame.vec(4).isNumeric & h2oFrame.vec(5).isNumeric & h2oFrame.vec(6).isNumeric
        & h2oFrame.vec(7).isNumeric & h2oFrame.vec(8).isNumeric)
    val rdd = hc.asRDD[Prostate](h2oFrame)

    def @@(i: Int) = rdd.take(i + 1)(i)

    assert(rdd.count == h2oFrame.numRows())
    assert(@@(4).productArity == 9)
    val sample7 = @@(7)
    assert(sample7.AGE.get == 61)

    h2oFrame.delete()
  }

  test("RDD[Option[Int]] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => Some(v))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      val value = vec.at8(rowIdx) // value stored at rowIdx-th
      assert(nextRowIdx == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Int]] with nulls, does it hold nulls?") {

    def almostDefined(i: Long) = Some(i.toInt) filter (_ % 31 != 0)

    val rdd = sc.parallelize(1 to 1000, 100).map(v => almostDefined(v))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertInvariantsWithNulls(
      rdd,
      h2oFrame,
      (rowIdx, vec) => {
        val nextRowIdx = rowIdx + 1
        almostDefined(nextRowIdx) match {
          case Some(r) =>
            val value = vec.at8(rowIdx) // value stored at rowIdx-th
            assert(r == value, s"The H2OFrame values should match row numbers+1 = $r")
          case None =>
            assert(vec.isNA(rowIdx), s"Row at $rowIdx must be NA")
        }
      })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Double]] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => Some(v))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      val value = vec.at(rowIdx) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert(nextRowIdx == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Byte]] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => Some(v.toByte))
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

  test("RDD[Option[Short]] to H2OFrame and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => Some(v.toShort))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assertBasicInvariants(rdd, h2oFrame, (row, vec) => {
      val row1 = row + 1
      val value = vec.at8(row) // value stored at row-th
      // Using == since int should be mapped strictly to shorts
      assert(row1 == value, "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[String]] to H2OFrame[Enum] and back") {

    val rdd = sc.parallelize(1 to 1000, 100).map(v => Some(v.toString))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assert(h2oFrame.vec(0).isString, "The vector type should be of string type")
    assert(h2oFrame.vec(0).domain() == null, "The vector domain should be <null>")

    // Transform string vector to categorical
    h2oFrame.replace(0, h2oFrame.vec(0).toCategoricalVec).remove()

    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val dom = vec.domain()
      val value = dom(vec.at8(rowIdx).asInstanceOf[Int]) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert(rowIdx + 1 == value.toInt, "The H2OFrame values should match row numbers")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[String]] to H2OFrame[String] and back") {

    val rdd = sc.parallelize(1 to (Categorical.MAX_CATEGORICAL_COUNT + 1), 100).map(v => Some(v.toString))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)

    assert(h2oFrame.vec(0).isString, "The vector type should be string")
    assert(h2oFrame.vec(0).domain() == null, "The vector should have null domain")
    val valueString = new BufferedString()
    assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = (rowIdx + 1).toString
      val value = vec.atStr(valueString, rowIdx) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert(nextRowIdx.equals(value.toString), "The H2OFrame values should match row numbers+1")
    })
    // Clean up
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("PUBDEV-458 - from Rdd[Option[Int]] to H2OFrame and back") {
    val h2oContext = hc
    import h2oContext.implicits._
    val rdd = sc.parallelize(1 to 1000000, 10).map(i => Some(i))
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
    val h2oFrame: H2OFrame = rdd
    val back2rdd = hc.asRDD[OptionAndNot](h2oFrame)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    assert(back2rdd.count == h2oFrame.numRows(), "Number of rows should match")

    back2rdd.foreach {
      case OptionAndNot(x, xOpt) =>
        if (xOpt != x.flatMap(i => almostDefined(i))) throw new IllegalStateException(s"Failed at $x/$xOpt")
    }
  }

  test("RDD[ByteField] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte]))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
  }

  test("RDD[ShortField] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short]))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
  }

  test("RDD[IntField] to H2OFrame[Numeric]") {
    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val rdd = sc.parallelize(values).map(v => IntField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
  }

  test("RDD[LongField] to H2OFrame[Numeric]") {
    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val rdd = sc.parallelize(values).map(v => LongField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
  }

  test("RDD[FloatField] to H2OFrame[Numeric]") {
    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val rdd = sc.parallelize(values).map(v => FloatField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
  }

  test("RDD[DoubleField] to H2OFrame[Numeric]") {
    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val rdd = sc.parallelize(values).map(v => DoubleField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
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

  test("RDD[java.sql.Timestamp] to H2OFrame[Time] and back") {
    // Create RDD with timestamp value in default timezone
    val rdd = sc.parallelize(1 to 100, 10).map(v => new Timestamp(v.toLong))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count == h2oFrame.numRows(), "Number of rows should match")
    val timestamp = DateTimeUtils.toUTCTime(h2oFrame.vec(0).at8(0) * 1000, SparkTimeZone.current().getID) / 1000
    assert(rdd.first().getTime == timestamp)
    // the rdd back should have spark time zone set up because the h2o frame -> rdd respects the Spark timezone
    val rddBack = hc.asDataFrame(h2oFrame)
    val rddBackData = rddBack.collect().map(_.getTimestamp(0))
    assert(rdd.collect.sameElements(rddBackData))
    h2oFrame.delete()
  }

  test("RDD[LabeledPoint] ( Dense Vector, same size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.dense(1, 2, 3, 4))
    val p2 = LabeledPoint(1, Vectors.dense(5, 6, 7, 8))
    val rdd = sc.parallelize(Seq(p1, p2), 10)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(1, 5))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(2, 6))
    assertVectorDoubleValues(h2oFrame.vec(3), Seq(3, 7))
    assertVectorDoubleValues(h2oFrame.vec(4), Seq(4, 8))
  }

  test("RDD[LabeledPoint] ( Dense Vector, different size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.dense(1, 2, 3))
    val p2 = LabeledPoint(1, Vectors.dense(4, 5))
    val rdd = sc.parallelize(Seq(p1, p2))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(1, 4))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(2, 5))
    assertVectorDoubleValues(h2oFrame.vec(3), Seq(3, 0))
  }

  test("RDD[LabeledPoint] ( Sparse Vector, same size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.sparse(4, Array(1, 3), Array(3.0, 8.1)))
    val p2 = LabeledPoint(1, Vectors.sparse(4, Array(0), Array(3.14)))
    val rdd = sc.parallelize(Seq(p1, p2), 10)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0, 3.14))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(3.0, 0))
    assertVectorDoubleValues(h2oFrame.vec(3), Seq(0, 0))
    assertVectorDoubleValues(h2oFrame.vec(4), Seq(8.1, 0))

  }

  test("RDD[LabeledPoint] ( Sparse Vector, different size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.sparse(1, Array(0), Array(3.0)))
    val p2 = LabeledPoint(1, Vectors.sparse(3, Array(0, 1), Array(1.0, 8)))
    val rdd = sc.parallelize(Seq(p1, p2))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assert(rdd.count() == h2oFrame.numRows(), "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(3.0, 1.0))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(0, 8))
    assertVectorDoubleValues(h2oFrame.vec(3), Seq(0, 0))
  }

  test("RDD[ml.linalg.Vector](dense - same length) to H2OFrame") {
    val dataDef = 1 to 10
    val data = dataDef.map(v => org.apache.spark.ml.linalg.Vectors.dense(v, 0, 0))
    val rdd = sc.parallelize(data)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assertVectorDoubleValues(h2oFrame.vec(0), dataDef.map(_.toDouble))
    assertVectorDoubleValues(h2oFrame.vec(1), dataDef.map(_ => 0.0))
    assertVectorDoubleValues(h2oFrame.vec(2), dataDef.map(_ => 0.0))
  }

  test("RDD[mllib.linalg.Vector](dense - same length) to H2OFrame") {
    val dataDef = 1 to 10
    val data = dataDef.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v, 0, 0))
    val rdd = sc.parallelize(data)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assertVectorDoubleValues(h2oFrame.vec(0), dataDef.map(_.toDouble))
    assertVectorDoubleValues(h2oFrame.vec(1), dataDef.map(_ => 0.0))
    assertVectorDoubleValues(h2oFrame.vec(2), dataDef.map(_ => 0.0))
  }

  test("RDD[mllib.linalg.Vector](dense - different length) to H2OFrame") {
    val data = Seq(
      org.apache.spark.mllib.linalg.Vectors.dense(1),
      org.apache.spark.mllib.linalg.Vectors.dense(1, 2),
      org.apache.spark.mllib.linalg.Vectors.dense(1, 2, 3))
    val rdd = sc.parallelize(data)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(1, 1, 1))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0, 2, 2))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(0, 0, 3))
  }

  test("RDD[mllib.linalg.Vector](Sparse - same length, one vector empty) to H2OFrame") {
    val data = Seq(Vectors.sparse(2, Array(0), Array(1)), Vectors.sparse(2, Array(1), Array(0)))
    val rdd = sc.parallelize(data, 1)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assertRDDH2OFrameInvariants(rdd, h2oFrame)

    assertVectorDoubleValues(h2oFrame.vec(0), Seq(1, 0))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0, 0))
  }

  test("RDD[mllib.linalg.Vector](Sparse - same length) to H2OFrame") {
    val data = Seq(Vectors.sparse(2, Array(0), Array(1)), Vectors.sparse(2, Array(1), Array(2)))
    val rdd = sc.parallelize(data, 1)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assertRDDH2OFrameInvariants(rdd, h2oFrame)

    assertVectorDoubleValues(h2oFrame.vec(0), Seq(1, 0))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0, 2))
  }

  test("RDD[mllib.linalg.Vector](Sparse - different length) to H2OFrame") {
    val dataDef = 1 to 3
    val data = dataDef.map(v => Vectors.sparse(v, Array(v - 1), Array(42)))

    val rdd = sc.parallelize(data)
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    assertRDDH2OFrameInvariants(rdd, h2oFrame)

    assertVectorDoubleValues(h2oFrame.vec(0), Seq(42, 0, 0))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0, 42, 0))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(0, 0, 42))
  }

  test("H2OFrame with categorical column into RDD") {
    val hf = hc.asH2OFrame(sc.parallelize(1 to 100).map(_.toString))
    H2OClientUtils.withLockAndUpdate(hf) {
      _.replace(0, hf.vec(0).toCategoricalVec).remove()
    }
    val rdd = hc.asRDD[StringHolder](hf)
    assert(rdd.count() == hf.numRows(), "Number of row should match")
  }

  private def assertRDDH2OFrameInvariants[T](inputRDD: RDD[T], df: H2OFrame): Unit = {
    assert(inputRDD.count == df.numRows(), "Number of rows has to match")
    inputRDD match {
      case x if x.take(1)(0).isInstanceOf[ByteField] =>
        assert(df.numCols() == inputRDD.take(1)(0).asInstanceOf[ByteField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[ShortField] =>
        assert(df.numCols() == inputRDD.take(1)(0).asInstanceOf[ShortField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[LongField] =>
        assert(df.numCols() == inputRDD.take(1)(0).asInstanceOf[LongField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[IntField] =>
        assert(df.numCols() == inputRDD.take(1)(0).asInstanceOf[IntField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[FloatField] =>
        assert(df.numCols() == inputRDD.take(1)(0).asInstanceOf[FloatField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[DoubleField] =>
        assert(
          df.numCols() == inputRDD.take(1)(0).asInstanceOf[DoubleField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[org.apache.spark.ml.linalg.Vector] =>
        assert(
          df.numCols() == inputRDD.asInstanceOf[RDD[org.apache.spark.ml.linalg.Vector]].map(v => v.size).max(),
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[org.apache.spark.mllib.linalg.Vector] =>
        assert(
          df.numCols() == inputRDD.asInstanceOf[RDD[org.apache.spark.mllib.linalg.Vector]].map(v => v.size).max(),
          "Number columns should match")
      case x => fail(s"Bad data $x")
    }
  }

}
