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
package org.apache.spark.h2o.converters

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.testdata._
import org.apache.spark.h2o.utils.H2OSchemaUtils.flatSchema
import org.apache.spark.h2o.utils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec._
import water.parser.BufferedString
import water.{DKV, Key}

import scala.reflect.ClassTag

/**
 * Testing schema for h2o schema rdd transformation.
 */
@RunWith(classOf[JUnitRunner])
class ConvertersTest extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

/* this test is under construction right now
  test("PUBDEV-766 H2OFrame[T_ENUM] to DataFrame[StringType]") {
    val fname: String = "Test1"
    SparkDataFrameConverter.initFrame(fname, Array("ZERO", "ONE"))
    val writer = new InternalWriteConverterContext
    writer.createChunks(fname, Array(Vec.T_NUM, Vec.T_NUM), 1)
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Integer]] = Array(Array(1, 0), Array(0, 1))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_CAT, colDomains = Array(Array("ZERO", "ONE")))
println("--------1-------")
    assert (h2oFrame.vec(0).chunkForChunkIdx(0).at8(0) == 1)
    println("--------2-------")
    assert (h2oFrame.vec(0).isCategorical)
    println("--------3-------")

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    println("--------4-------")
    assert (dataFrame.take(4)(3)(0) == "ONE")
    println("--------5-------")
    assert (dataFrame.schema.fields(0) match{
      case StructField("C0",StringType,false, _) => true
      case _ => false
    })
    println("--------6-------")

    h2oFrame.delete()
  }
*/

  test("H2OFrame[T_STR] to DataFrame[StringType]") {
    val fname: String = "testString.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 3L, 2L)
    val data: Array[Array[String]] = Array(Array("string1", "string2", "string3"),
                                           Array("string4", "string5", "string6"),
                                           Array("string7", "string8"))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_STR)

    assert (h2oFrame.vec(0).chunkForChunkIdx(2).atStr(new BufferedString(),1).toString.equals("string8"))
    assert (h2oFrame.vec(0).isString)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(8)(7)(0) == "string8")
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",StringType,false, _) => true
      case _ => false
    } )

    h2oFrame.delete()
  }

  test("PUBDEV-771 H2OFrame[T_UUID] to DataFrame[StringType]") {
    val fname: String = "testUUID.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 3L)
    val data: Array[Array[UUID]] = Array(
      Array(
        UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a"),
        UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3b"),
        UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3c")),
      Array(
        UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3d"),
        UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3e"),
        UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3f")))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_UUID)

    assert (UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getLeastSignificantBits ==
      h2oFrame.vec(0).chunkForChunkIdx(0).at16l(0)                                           &
      UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getMostSignificantBits        ==
        h2oFrame.vec(0).chunkForChunkIdx(0).at16h(0))
    assert (h2oFrame.vec(0).isUUID)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",StringType,false, _) => true
      case _ => false
    })
    val valuesInRdd = dataFrame.collect().map(row => row(0))
    for (idx <- valuesInRdd.indices)
      assert (valuesInRdd(idx) == "6870f256-e145-4d75-adb0-99ccb77d5d3" + ('a' + idx).asInstanceOf[Char])
    h2oFrame.delete()
  }

  test("test RDD to H2OFrame to DataFrame way") {
    val h2oContext = hc
    import h2oContext.implicits._

    val rdd = sc.parallelize(1 to 10000, 1000).map(i => IntHolder(Some(i)))
    val h2oFrame:H2OFrame = rdd

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (rdd.count == h2oFrame.numRows())
    assert (rdd.count == dataFrame.count)
  }

  test("RDD[Byte] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte]))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("RDD[Short] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short]))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("RDD[Int] to H2OFrame[Numeric]") {
    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val rdd = sc.parallelize(values).map(v => IntField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("RDD[Long] to H2OFrame[Numeric]") {
    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val rdd = sc.parallelize(values).map(v => LongField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("RDD[Float] to H2OFrame[Numeric]") {
    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val rdd = sc.parallelize(values).map(v => FloatField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("RDD[Double] to H2OFrame[Numeric]") {
    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val rdd = sc.parallelize(values).map(v => DoubleField(v))
    val h2oFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[Byte] to H2OFrame[Numeric]") {
    val sqlContext = sqlc
    import sqlContext.implicits._

    val srdd:DataFrame = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte])).toDF()
    val h2oFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[Short] to H2OFrame[Numeric]") {
    val sqlContext = sqlc
    import sqlContext.implicits._

    val srdd:DataFrame = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short])).toDF()
    val h2oFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[Int] to H2OFrame[Numeric]") {
    val sqlContext = sqlc
    import sqlContext.implicits._

    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val srdd:DataFrame = sc.parallelize(values).map(v => IntField(v)).toDF()
    val h2oFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[Long] to H2OFrame[Numeric]") {
    val sqlContext = sqlc
    import sqlContext.implicits._

    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val srdd:DataFrame = sc.parallelize(values).map(v => LongField(v)).toDF()
    val h2oFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[Float] to H2OFrame[Numeric]") {
    val sqlContext = sqlc
    import sqlContext.implicits._

    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val srdd:DataFrame = sc.parallelize(values).map(v => FloatField(v)).toDF
    val h2oFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  def makeH2OFrame[T: ClassTag](fname: String, colNames: Array[String], chunkLayout: Array[Long],
                                 data: Array[Array[T]], h2oType: Byte, colDomains: Array[Array[String]] = null): H2OFrame = {
    var f: Frame = new Frame(Key.make(fname))
    FrameUtils.preparePartialFrame(f,colNames)
    f.update()

    for( i <- chunkLayout.indices) { buildChunks(fname, data(i), i, Array(h2oType)) }

    f = DKV.get(fname).get()

    FrameUtils.finalizePartialFrame(f, chunkLayout, colDomains, Array(h2oType))

    new H2OFrame(f)
  }

  def fp(it:Iterator[Row]):Unit = {
    println(it.size)
  }

  def assertH2OFrameInvariants(inputRDD: DataFrame, df: H2OFrame): Unit = {
    assert( inputRDD.count == df.numRows(), "Number of rows has to match")
    assert( df.numCols() == flatSchema(inputRDD.schema).length , "Number columns should match")
  }

  def assertRDDH2OFrameInvariants[T](inputRDD: RDD[T], df: H2OFrame): Unit = {
    assert( inputRDD.count == df.numRows(), "Number of rows has to match")
    inputRDD match {
      case x if x.take(1)(0).isInstanceOf[ByteField] =>
        assert( df.numCols() == inputRDD.take(1)(0).asInstanceOf[ByteField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[ShortField] =>
        assert( df.numCols() == inputRDD.take(1)(0).asInstanceOf[ShortField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[LongField] =>
        assert( df.numCols() == inputRDD.take(1)(0).asInstanceOf[LongField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[IntField] =>
        assert( df.numCols() == inputRDD.take(1)(0).asInstanceOf[IntField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[FloatField] =>
        assert( df.numCols() == inputRDD.take(1)(0).asInstanceOf[FloatField].productArity, "Number columns should match")
      case x if x.take(1)(0).isInstanceOf[DoubleField] =>
        assert( df.numCols() == inputRDD.take(1)(0).asInstanceOf[DoubleField].productArity, "Number columns should match")
      case x => fail(s"Bad data $x")
    }
  }

  def assertVectorIntValues(vec: water.fvec.Vec, values: Seq[Int]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) -1 == values(rIdx)
        else vec.at8(rIdx) == values(rIdx), "values stored has to match to values in rdd")
    }
  }

  def assertVectorDoubleValues(vec: water.fvec.Vec, values: Seq[Double]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) values(rIdx)==Double.NaN // this is Scala i can do NaN comparision
        else vec.at(rIdx) == values(rIdx), "values stored has to match to values in rdd")
    }
  }

  def assertVectorEnumValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val vecDom = vec.domain()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(vecDom(vec.at8(rIdx).asInstanceOf[Int]) == values(rIdx), "values stored has to match to values in rdd")
    }
  }

  def assertVectorStringValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val valString = new BufferedString()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(
        vec.isNA(rIdx) || {
          vec.atStr(valString, rIdx)
          valString.bytesToString() == values(rIdx)
        }, "values stored has to match to values in rdd")
    }
  }
}

