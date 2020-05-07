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
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, SparkTimeZone, TestUtils}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.parser.Categorical

@RunWith(classOf[JUnitRunner])
class SupportedRDDConverterTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test("Empty RDD to H2O frame, Byte type") {
    val rdd = sc.parallelize(Array.empty[Byte])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Short type") {
    val rdd = sc.parallelize(Array.empty[Short])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Integer type") {
    val rdd = sc.parallelize(Array.empty[Integer])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Long type") {
    val rdd = sc.parallelize(Array.empty[Long])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Float type") {
    val rdd = sc.parallelize(Array.empty[Float])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Double type") {
    val rdd = sc.parallelize(Array.empty[Double])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, String type") {
    val rdd = sc.parallelize(Array.empty[String])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Boolean type") {
    val rdd = sc.parallelize(Array.empty[Boolean])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Timestamp type") {
    val rdd = sc.parallelize(Array.empty[Timestamp])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Product type") {
    val rdd = sc.parallelize(Array.empty[ByteField])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("Empty RDD to H2O frame, Labeled point") {
    val rdd = sc.parallelize(Array.empty[LabeledPoint])
    val fr = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assert(fr.numberOfColumns == 1)
    assert(fr.numberOfRows == 0)
  }

  test("H2OFrame[T_NUM] to RDD[Prostate]") {
    val h2oFrame: H2OFrame = H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv")))
    assert(
      h2oFrame.columns(0).isNumeric & h2oFrame.columns(1).isNumeric & h2oFrame.columns(2).isNumeric &
        h2oFrame.columns(3).isNumeric & h2oFrame.columns(4).isNumeric & h2oFrame.columns(5).isNumeric & h2oFrame
        .columns(6)
        .isNumeric
        & h2oFrame.columns(7).isNumeric & h2oFrame.columns(8).isNumeric)
    val rdd = hc.asRDD[Prostate](h2oFrame)

    def @@(i: Int) = rdd.take(i + 1)(i)

    assert(rdd.count == h2oFrame.numberOfRows)
    assert(@@(4).productArity == 9)
    val sample7 = @@(7)
    assert(sample7.AGE.get == 61)

    h2oFrame.delete()
  }

  test("RDD[Option[Int]] to H2OFrame and back") {
    val rdd = sc.parallelize(1 to 3, 100).map(v => Some(v))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 2, 3))
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Int]] with nulls, does it hold nulls?") {
    val rdd = sc.parallelize(Seq(Some(1), None, Some(2)))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 0, 2))
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Double]] to H2OFrame and back") {
    val rdd = sc.parallelize(1 to 3, 100).map(v => Some(v.toDouble))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(1.0, 2.0, 3.0))
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Byte]] to H2OFrame and back") {
    val rdd = sc.parallelize(1 to 3, 100).map(v => Some(v.toByte))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 2, 3))
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[Short]] to H2OFrame and back") {
    val rdd = sc.parallelize(1 to 3, 100).map(v => Some(v.toShort))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 2, 3))
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[String]] to H2OFrame[Enum] and back") {
    val rdd = sc.parallelize(Seq(Some("1"), Some("2"), Some("1")))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(0, 1, 0))
    assert(h2oFrame.columns.head.isCategorical())
    assert(h2oFrame.columns.head.domain.sameElements(Array("1", "2")))
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("RDD[Option[String]] to H2OFrame[String] and back") {
    val data = (1 to (Categorical.MAX_CATEGORICAL_COUNT + 1)).map(_.toString)
    val rdd = sc.parallelize(data, 100)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(h2oFrame.columns.head.isString(), "The vector type should be of string type")
    assert(h2oFrame.columns.head.domain == null, "The vector domain should be <null>")
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorStringValues(h2oFrame.collectStrings(0), data)
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("PUBDEV-458 - from Rdd[Option[Int]] to H2OFrame and back") {
    val rdd = sc.parallelize(1 to 1000000, 10).map(i => Some(i))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    val back2rdd = hc.asRDD[PUBDEV458Type](h2oFrame)
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    assert(back2rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
  }

  test("Options handled correctly (no null -> 0)") {
    def almostDefined(i: Long) = Some(i.toInt) filter (_ % 31 != 0)

    val rdd = sc.parallelize(1 to 1000000, 10).map(i => OptionAndNot(Some(i), almostDefined(i)))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    val back2rdd = hc.asRDD[OptionAndNot](h2oFrame)
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    assert(back2rdd.count == h2oFrame.numberOfRows, "Number of rows should match")

    back2rdd.foreach {
      case OptionAndNot(x, xOpt) =>
        if (xOpt != x.flatMap(i => almostDefined(i))) throw new IllegalStateException(s"Failed at $x/$xOpt")
    }
  }

  test("RDD[ByteField] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte]))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.columns.head.isNumeric())
  }

  test("RDD[ShortField] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short]))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.columns.head.isNumeric())
  }

  test("RDD[IntField] to H2OFrame[Numeric]") {
    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val rdd = sc.parallelize(values).map(v => IntField(v))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.columns.head.isNumeric())
  }

  test("RDD[LongField] to H2OFrame[Numeric]") {
    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val rdd = sc.parallelize(values).map(v => LongField(v))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.columns.head.isNumeric())
  }

  test("RDD[FloatField] to H2OFrame[Numeric]") {
    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val rdd = sc.parallelize(values).map(v => FloatField(v))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.columns.head.isNumeric())
  }

  test("RDD[DoubleField] to H2OFrame[Numeric]") {
    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val rdd = sc.parallelize(values).map(v => DoubleField(v))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert(h2oFrame.columns.head.isNumeric())
  }

  // PUBDEV-1173
  test("RDD[Int] to H2OFrame[Numeric]") {
    // Create RDD with 100 Int values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10)
    // PUBDEV-1173
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[Float] to H2OFrame[Numeric]") {
    // Create RDD with 100 Float values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toFloat)
    // PUBDEV-1173
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[Double] to H2OFrame[Numeric]") {
    // Create RDD with 100 Double values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toDouble)
    // PUBDEV-1173
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  // PUBDEV-1173
  test("RDD[String] to H2OFrame[String]") {
    // Create RDD with 100 String values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toString)
    // PUBDEV-1173
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[Byte] to H2OFrame[Numeric]") {
    // Create RDD with 100 Byte values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toByte)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[Short] to H2OFrame[Numeric]") {
    // Create RDD with 100 Short values, 10 values per 1 Spark partition
    val rdd = sc.parallelize(1 to 100, 10).map(v => v.toShort)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    h2oFrame.delete()
  }

  test("RDD[java.sql.Timestamp] to H2OFrame[Time] and back") {
    // Create RDD with timestamp value in default timezone
    val rdd = sc.parallelize(1 to 100, 10).map(v => new Timestamp(v.toLong))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count == h2oFrame.numberOfRows, "Number of rows should match")
    val timestamp = DateTimeUtils.toUTCTime(h2oFrame.collectLongs(0)(0) * 1000, SparkTimeZone.current().getID) / 1000
    assert(rdd.first().getTime == timestamp)
    // the rdd back should have spark time zone set up because the h2o frame -> rdd respects the Spark timezone
    val rddBack = hc.asSparkFrame(h2oFrame)
    val rddBackData = rddBack.collect().map(_.getTimestamp(0))
    assert(rdd.collect.sameElements(rddBackData))
    h2oFrame.delete()
  }

  test("RDD[LabeledPoint] ( Dense Vector, same size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.dense(1, 2, 3, 4))
    val p2 = LabeledPoint(1, Vectors.dense(5, 6, 7, 8))
    val rdd = sc.parallelize(Seq(p1, p2), 10)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count() == h2oFrame.numberOfRows, "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(1, 5))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(2, 6))
    assertVectorDoubleValues(h2oFrame.collectDoubles(3), Seq(3, 7))
    assertVectorDoubleValues(h2oFrame.collectDoubles(4), Seq(4, 8))
  }

  test("RDD[LabeledPoint] ( Dense Vector, different size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.dense(1, 2, 3))
    val p2 = LabeledPoint(1, Vectors.dense(4, 5))
    val rdd = sc.parallelize(Seq(p1, p2))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count() == h2oFrame.numberOfRows, "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(1, 4))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(2, 5))
    assertVectorDoubleValues(h2oFrame.collectDoubles(3), Seq(3, 0))
  }

  test("RDD[LabeledPoint] ( Sparse Vector, same size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.sparse(4, Array(1, 3), Array(3.0, 8.1)))
    val p2 = LabeledPoint(1, Vectors.sparse(4, Array(0), Array(3.14)))
    val rdd = sc.parallelize(Seq(p1, p2), 10)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count() == h2oFrame.numberOfRows, "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0, 3.14))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(3.0, 0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(3), Seq(0, 0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(4), Seq(8.1, 0))

  }

  test("RDD[LabeledPoint] ( Sparse Vector, different size ) to H2OFrame[LabeledPoint]") {
    val p1 = LabeledPoint(0, Vectors.sparse(1, Array(0), Array(3.0)))
    val p2 = LabeledPoint(1, Vectors.sparse(3, Array(0, 1), Array(1.0, 8)))
    val rdd = sc.parallelize(Seq(p1, p2))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assert(rdd.count() == h2oFrame.numberOfRows, "Number of rows should match")
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(0, 1))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(3.0, 1.0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(0, 8))
    assertVectorDoubleValues(h2oFrame.collectDoubles(3), Seq(0, 0))
  }

  test("RDD[ml.linalg.Vector](dense - same length) to H2OFrame") {
    val dataDef = 1 to 10
    val data = dataDef.map(v => org.apache.spark.ml.linalg.Vectors.dense(v, 0, 0))
    val rdd = sc.parallelize(data)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), dataDef.map(_.toDouble))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), dataDef.map(_ => 0.0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), dataDef.map(_ => 0.0))
  }

  test("RDD[mllib.linalg.Vector](dense - same length) to H2OFrame") {
    val dataDef = 1 to 10
    val data = dataDef.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v, 0, 0))
    val rdd = sc.parallelize(data)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), dataDef.map(_.toDouble))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), dataDef.map(_ => 0.0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), dataDef.map(_ => 0.0))
  }

  test("RDD[mllib.linalg.Vector](dense - different length) to H2OFrame") {
    val data = Seq(
      org.apache.spark.mllib.linalg.Vectors.dense(1),
      org.apache.spark.mllib.linalg.Vectors.dense(1, 2),
      org.apache.spark.mllib.linalg.Vectors.dense(1, 2, 3))
    val rdd = sc.parallelize(data)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(1, 1, 1))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0, 2, 2))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(0, 0, 3))
  }

  test("RDD[mllib.linalg.Vector](Sparse - same length, one vector empty) to H2OFrame") {
    val data = Seq(Vectors.sparse(2, Array(0), Array(1)), Vectors.sparse(2, Array(1), Array(0)))
    val rdd = sc.parallelize(data, 1)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDH2OFrameInvariants(rdd, h2oFrame)

    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(1, 0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0, 0))
  }

  test("RDD[mllib.linalg.Vector](Sparse - same length) to H2OFrame") {
    val data = Seq(Vectors.sparse(2, Array(0), Array(1)), Vectors.sparse(2, Array(1), Array(2)))
    val rdd = sc.parallelize(data, 1)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDH2OFrameInvariants(rdd, h2oFrame)

    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(1, 0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0, 2))
  }

  test("RDD[mllib.linalg.Vector](Sparse - different length) to H2OFrame") {
    val dataDef = 1 to 3
    val data = dataDef.map(v => Vectors.sparse(v, Array(v - 1), Array(42)))

    val rdd = sc.parallelize(data)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(rdd))
    assertRDDH2OFrameInvariants(rdd, h2oFrame)

    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(42, 0, 0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0, 42, 0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(0, 0, 42))
  }

  test("H2OFrame with categorical column into RDD") {
    val hf = H2OFrame(hc.asH2OFrameKeyString(sc.parallelize(1 to 100).map(_.toString)))
    hf.convertColumnsToCategorical(Array(0))
    val rdd = hc.asRDD[StringHolder](hf)
    assert(rdd.count() == hf.numberOfRows, "Number of row should match")
  }

  private def assertRDDH2OFrameInvariants[T](inputRDD: RDD[T], df: H2OFrame): Unit = {
    assert(inputRDD.count == df.numberOfRows, "Number of rows has to match")
    inputRDD match {
      case x if x.take(1)(0).isInstanceOf[ByteField] =>
        assert(
          df.numberOfColumns == inputRDD.take(1)(0).asInstanceOf[ByteField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[ShortField] =>
        assert(
          df.numberOfColumns == inputRDD.take(1)(0).asInstanceOf[ShortField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[LongField] =>
        assert(
          df.numberOfColumns == inputRDD.take(1)(0).asInstanceOf[LongField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[IntField] =>
        assert(
          df.numberOfColumns == inputRDD.take(1)(0).asInstanceOf[IntField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[FloatField] =>
        assert(
          df.numberOfColumns == inputRDD.take(1)(0).asInstanceOf[FloatField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[DoubleField] =>
        assert(
          df.numberOfColumns == inputRDD.take(1)(0).asInstanceOf[DoubleField].productArity,
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[org.apache.spark.ml.linalg.Vector] =>
        assert(
          df.numberOfColumns == inputRDD.asInstanceOf[RDD[org.apache.spark.ml.linalg.Vector]].map(v => v.size).max(),
          "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[org.apache.spark.mllib.linalg.Vector] =>
        assert(
          df.numberOfColumns == inputRDD.asInstanceOf[RDD[org.apache.spark.mllib.linalg.Vector]].map(v => v.size).max(),
          "Number columns should match")
      case x => fail(s"Bad data $x")
    }
  }

}
