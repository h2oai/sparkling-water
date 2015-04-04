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

import java.io.File
import java.sql.Timestamp
import java.util.UUID

import hex.splitframe.ShuffleSplitFrame
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{mllib, SparkContext}
import org.apache.spark.h2o.{H2OSchemaUtils, IntHolder, H2OContext}
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.{DKV, Key}
import water.fvec._
import org.apache.spark.sql._
import water.parser.Categorical
import org.apache.spark.h2o.H2OSchemaUtils.flatSchema

import scala.annotation.tailrec


/**
 * Testing schema for h2o schema rdd transformation.
 */
@RunWith(classOf[JUnitRunner])
class H2OSchemaRDDTest extends FunSuite with SparkTestContext {

  sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
  hc = new H2OContext(sc).start()
  // Shared sqlContext
  val h2oContext = hc
  val sqlContext = new SQLContext(sc)

  test("test creation of H2OSchemaRDD") {
    import h2oContext._

    // FIXME: create different shapes of frame
    val dataFrame = new DataFrame(new File("../examples/smalldata/prostate.csv"))
    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert(dataFrame.numRows() == schemaRdd.count(), "Number of lines in dataframe and in schema has to be same")
    dataFrame.delete()
  }

  ignore("DataFrame[UUID] to SchemaRDD[StringType]") {
    import h2oContext._

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

    var f: Frame = new Frame(Key.make(fname))
    FrameUtils.preparePartialFrame(f,colNames)
    f.update(null)

    for( i <- 0 to chunkLayout.length-1) { createNUUIDC(fname, data(i), i, chunkLayout(i).toInt) }

    f = DKV.get(fname).get()
    FrameUtils.finalizePartialFrame(f, chunkLayout, null, Array(Vec.T_UUID))

    val dataFrame = new DataFrame(f)

    assert (UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getLeastSignificantBits() ==
      dataFrame.vec(0).chunkForChunkIdx(0).at16l(0)                                           &
      UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getMostSignificantBits()        ==
        dataFrame.vec(0).chunkForChunkIdx(0).at16h(0))

    assert (UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3b").getLeastSignificantBits() ==
      dataFrame.vec(0).chunkForChunkIdx(0).at16l(1)                                           &
      UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3b").getMostSignificantBits()        ==
        dataFrame.vec(0).chunkForChunkIdx(0).at16h(1))

    assert (UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3c").getLeastSignificantBits() ==
      dataFrame.vec(0).chunkForChunkIdx(0).at16l(2)                                           &
      UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3c").getMostSignificantBits()        ==
        dataFrame.vec(0).chunkForChunkIdx(0).at16h(2))

    implicit val sqlContext = new SQLContext(sc)
    // TODO: asSchemaRDD should convert UUID columns to String columns
    // TODO: test that UUID's between dataFrame and schemaRDD match
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    dataFrame.delete()
  }

  test("test RDD to DataFrame to SchemaRDD way") {
    import h2oContext._

    val rdd = sc.parallelize(1 to 10000, 1000).map(i => IntHolder(Some(i)))
    val dataFrame:DataFrame = rdd

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (rdd.count == dataFrame.numRows())
    assert (rdd.count == schemaRdd.count)
  }

  test("SchemaRDD[Byte] to DataFrame[Numeric]") {
    import sqlContext._

    val srdd:SchemaRDD = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte]))
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("SchemaRDD[Short] to DataFrame[Numeric]") {
    import sqlContext._

    val srdd:SchemaRDD = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short]))
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("SchemaRDD[Int] to DataFrame[Numeric]") {
    import sqlContext._

    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val srdd:SchemaRDD = sc.parallelize(values).map(v => IntField(v))
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("SchemaRDD[Long] to DataFrame[Numeric]") {
    import sqlContext._

    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val srdd:SchemaRDD = sc.parallelize(values).map(v => LongField(v))
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("SchemaRDD[String] to DataFrame[Enum]") {
    import sqlContext._

    val num = 3000
    val values = (1 to num).map( v => StringField(v + "-value"))
    val srdd:SchemaRDD = sc.parallelize(values)
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isEnum())
    assert (dataFrame.domains()(0).length == num)
  }

  test("SchemaRDD[String] to DataFrame[String]") {
    import sqlContext._

    val num = Categorical.MAX_ENUM_SIZE + 1
    val values = (1 to num).map( v => StringField(v + "-value"))
    val srdd:SchemaRDD = sc.parallelize(values)
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isString())
  }

  test("SchemaRDD[TimeStamp] to DataFrame[Time]") {
    import sqlContext._

    val num = 20
    val values = (1 to num).map(v => new Timestamp(v))
    val srdd:SchemaRDD = sc.parallelize(values).map(v => TimestampField(v))
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isTime())
  }

  ignore("DataFrame[Time] to SchemaRDD[TimeStamp]") {

  }

  test("DataFrame[Simple StructType] to SchemaRDD[flattened StructType]") {
    import sqlContext._
    val num = 20
    val values = (1 to num).map(x => PrimitiveA(x, "name="+x))
    val srdd:SchemaRDD = sc.parallelize(values)
    // Convert to DataFrame
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
  }

  test("DataFrame[Composed StructType] to SchemaRDD[flattened StructType]") {
    import sqlContext._
    val num = 20
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name="+x), x*3.14))
    val srdd:SchemaRDD = sc.parallelize(values)
    // Convert to DataFrame
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
  }

  test("SchemaRDD[Int] to DataFrame with empty partitions (error detected in calling ShuffleSplitFrame)") {
    import sqlContext._

    val values = 1 to 100
    val srdd:SchemaRDD = sc.parallelize(values, 2000).map(v => IntField(v))

    val dataFrame = hc.toDataFrame(srdd)

    ShuffleSplitFrame.shuffleSplitFrame(dataFrame,
        Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make(_)),
        Array[Double](0.5, 0.3, 0.2), 1234567689L)
  }

  test("Expand composed schema of RDD") {
    import sqlContext._
    val num = 2
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name=" + x), x * 1.0))
    val srdd: SchemaRDD = sc.parallelize(values)

    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)
    assert(expandedSchema === Vector(
      (List(0, 0), StructField("a.n", IntegerType, true), 0),
      (List(0, 1), StructField("a.name", StringType, true), 0),
      (List(1), StructField("weight", DoubleType, false), 0)))

    // Verify transformation into dataframe
    val dataFrame = hc.toDataFrame(srdd)
    assertDataFrameInvariants(srdd, dataFrame)

    // Verify data stored in dataFrame after transformation
    assertVectorIntValues(dataFrame.vec(0), Seq(1,2))
    assertVectorEnumValues(dataFrame.vec(1), Seq("name=1", "name=2"))
    assertVectorDoubleValues(dataFrame.vec(2), Seq(1.0, 2.0))
  }

  test("Expand schema with array") {
    import sqlContext._
    val num = 5
    val values = (1 to num).map(x => PrimitiveB(1 to x))
    val srdd: SchemaRDD = sc.parallelize(values)
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)

    assert(expandedSchema === Vector(
      (List(0), StructField("f0", IntegerType, false), 1),
      (List(0), StructField("f1", IntegerType, false), 1),
      (List(0), StructField("f2", IntegerType, false), 1),
      (List(0), StructField("f3", IntegerType, false), 1),
      (List(0), StructField("f4", IntegerType, false), 1)))

    // Verify transformation into dataframe
    val dataFrame = hc.toDataFrame(srdd)
    // Basic invariants
    assert(srdd.count == dataFrame.numRows(), "Number of rows has to match")
    assert(5 == dataFrame.numCols(), "Number columns should match")

    // Verify data stored in dataFrame after transformation
    assertVectorIntValues(dataFrame.vec(0), Seq(1, 1, 1, 1, 1))
    assertVectorIntValues(dataFrame.vec(1), Seq(-1, 2, 2, 2, 2))
    assertVectorIntValues(dataFrame.vec(2), Seq(-1, -1, 3, 3, 3))
    assertVectorIntValues(dataFrame.vec(3), Seq(-1, -1, -1, 4, 4))
    assertVectorIntValues(dataFrame.vec(4), Seq(-1, -1, -1, -1, 5))
  }

  test("Expand schema with dense vectors") {
    import sqlContext._
    val num = 2
    val values = (1 to num).map(x => PrimitiveC(Vectors.dense((1 to x).map(1.0*_).toArray)))
    val srdd:SchemaRDD = sc.parallelize(values)
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)

    assert (expandedSchema === Vector(
      (List(0),StructField("f0",DoubleType,true),2),
      (List(0),StructField("f1",DoubleType,true),2)))

    // Verify transformation into dataframe
    val dataFrame = hc.toDataFrame(srdd)
    // Basic invariants
    assert( srdd.count == dataFrame.numRows(), "Number of rows has to match")
    assert( 2 == dataFrame.numCols(), "Number columns should match")

    // Verify data stored in dataFrame after transformation
    assertVectorDoubleValues(dataFrame.vec(0), Seq(1.0,1.0))
    // For vectors missing values are replaced by zeros
    assertVectorDoubleValues(dataFrame.vec(1), Seq(0.0, 2.0))
  }

  test("Expand schema with sparse vectors") {
    import sqlContext._
    val num = 3
    val values = (0 until num).map(x =>
      PrimitiveC(
        Vectors.sparse(num, (0 until num).map(i => if (i==x) (i, 1.0) else (i, 0.0)).toSeq )
      ))
    val srdd:SchemaRDD = sc.parallelize(values)
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)

    assert (expandedSchema === Vector(
      (List(0),StructField("f0",DoubleType,true),2),
      (List(0),StructField("f1",DoubleType,true),2),
      (List(0),StructField("f2",DoubleType,true),2)))

    // Verify transformation into dataframe
    val dataFrame = hc.toDataFrame(srdd)
    // Basic invariants
    assert( srdd.count == dataFrame.numRows(), "Number of rows has to match")
    assert( 3 == dataFrame.numCols(), "Number columns should match")

    // Verify data stored in dataFrame after transformation
    assertVectorDoubleValues(dataFrame.vec(0), Seq(1.0,0.0,0.0))
    assertVectorDoubleValues(dataFrame.vec(1), Seq(0.0,1.0,0.0))
    assertVectorDoubleValues(dataFrame.vec(2), Seq(0.0,0.0,1.0))
  }

  def createNUUIDC(fname: String, data: Array[UUID], cidx: Integer, len: Integer): NewChunk = {
    var nchunks: Array[NewChunk] = FrameUtils.createNewChunks(fname, cidx)
    for (i <- 0 to len-1) { nchunks(0).addUUID(data(i).getLeastSignificantBits(), data(i).getMostSignificantBits()) }
    FrameUtils.closeNewChunks(nchunks)
    return nchunks(0)
  }

  def fp(it:Iterator[Row]):Unit = {
    println(it.size)
  }

  def assertDataFrameInvariants(inputRDD: SchemaRDD, df: DataFrame): Unit = {
    assert( inputRDD.count == df.numRows(), "Number of rows has to match")
    assert( df.numCols() == flatSchema(inputRDD.schema).length , "Number columns should match")
  }

  def assertVectorIntValues(vec: water.fvec.Vec, values: Seq[Int]): Unit = {
    (0 until vec.length().asInstanceOf[Int]).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) -1 == values(rIdx)
        else vec.at8(rIdx) == values(rIdx), "values stored has to match to values in rdd")
    }
  }

  def assertVectorDoubleValues(vec: water.fvec.Vec, values: Seq[Double]): Unit = {
    (0 until vec.length().asInstanceOf[Int]).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) values(rIdx)==Double.NaN // this is Scala i can do NaN comparision
        else vec.at(rIdx) == values(rIdx), "values stored has to match to values in rdd")
    }
  }

  def assertVectorEnumValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val vecDom = vec.domain()
    (0 until vec.length().asInstanceOf[Int]).foreach { rIdx =>
      assert(vecDom(vec.at8(rIdx).asInstanceOf[Int]) == values(rIdx), "values stored has to match to values in rdd")
    }
  }
}

object H2OSchemaRDDTest {

}

// Helper classes for conversion from RDD to SchemaRDD
// which expects T <: Product
case class ByteField  (v: Byte)
case class ShortField (v: Short)
case class IntField   (v: Int)
case class LongField  (v: Long)
case class StringField(v: String)
case class TimestampField(v: Timestamp)

case class PrimitiveA(n: Int, name: String)
case class ComposedA(a: PrimitiveA, weight: Double)

case class PrimitiveB(f: Seq[Int])

case class PrimitiveC(f: mllib.linalg.Vector)
