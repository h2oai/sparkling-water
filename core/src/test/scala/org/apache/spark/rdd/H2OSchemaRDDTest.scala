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
import water.parser.{ValueString, Categorical}
import org.apache.spark.h2o.H2OSchemaUtils.flatSchema


import scala.reflect.ClassTag


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

  // DataFrame to RDD[T] JUnits
  test("DataFrame[T_NUM] to RDD[Prostate]") {
    import h2oContext._
    val dataFrame: DataFrame = new DataFrame(new File("../examples/smalldata/prostate.csv"))
    assert (dataFrame.vec(0).isNumeric & dataFrame.vec(1).isNumeric & dataFrame.vec(2).isNumeric &
      dataFrame.vec(3).isNumeric & dataFrame.vec(4).isNumeric & dataFrame.vec(5).isNumeric & dataFrame.vec(6).isNumeric
      & dataFrame.vec(7).isNumeric & dataFrame.vec(8).isNumeric)

    implicit val sqlContext = new SQLContext(sc)
    val rdd = asRDD[Prostate](dataFrame)

    assert (rdd.count == dataFrame.numRows())
    assert (rdd.take(5)(4).productArity == 9)
    assert (rdd.take(8)(7).AGE.get == 61)

    dataFrame.delete()
  }

  // DataFrame to SchemaRDD[T] JUnits
  ignore("PUBDEV-766 DataFrame[T_ENUM] to SchemaRDD[StringType]") {
    import h2oContext._
    val fname: String = "testEnum.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Integer]] = Array(Array(1, 0), Array(0, 1))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_ENUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(0).at8(0) == 1)
    assert (dataFrame.vec(0).isEnum())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0) == "1")
    assert (schemaRdd.schema.fields(0) == StructField("C0",StringType,false))

    dataFrame.delete()
  }

  test("DataFrame[T_TIME] to SchemaRDD[TimestampType]") {
    import h2oContext._
    val fname: String = "testTime.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Long]] = Array(Array(1428517563L, 1428517564L), Array(1428517565L, 1428517566L))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_TIME)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(1) == 1428517566L)
    assert (dataFrame.vec(0).isTime())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0).asInstanceOf[Timestamp].getTime() == 1428517566L)
    assert (schemaRdd.schema.fields(0) == StructField("C0",TimestampType,false))

    dataFrame.delete()
  }

  test("DataFrame[T_NUM(Byte)] to SchemaRDD[ByteType]") {
    import h2oContext._
    val fname: String = "testByte.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Byte]] = Array(Array(-1.toByte, 2.toByte, -3.toByte),
      Array(4.toByte, -5.toByte, 6.toByte, -7.toByte, 8.toByte))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(4) == 8)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == 8)
    assert (schemaRdd.schema.fields(0) == StructField("C0",ByteType,false))

    dataFrame.delete()
  }

  test("DataFrame[T_NUM(Short)] to SchemaRDD[ShortType]") {
    import h2oContext._
    val fname: String = "testShort.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Short]] = Array(Array(-200.toShort, 201.toShort, -202.toShort),
      Array(204.toShort, -205.toShort, 206.toShort, -207.toShort, 208.toShort))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(4) == 208)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == 208)
    assert (schemaRdd.schema.fields(0) == StructField("C0",ShortType,false))

    dataFrame.delete()
  }

  test("DataFrame[T_NUM(Integer)] to SchemaRDD[IntegerType]") {
    import h2oContext._
    val fname: String = "testInteger.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Integer]] = Array(Array(-100000, 100001, -100002),
      Array(100004, -100005, 100006, -100007, 100008))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(4) == 100008)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == 100008)
    assert (schemaRdd.schema.fields(0) == StructField("C0",IntegerType,false))

    dataFrame.delete()
  }

  ignore("PUBDEV-767 DataFrame[T_NUM(Long)] to SchemaRDD[LongType]") {
    import h2oContext._
    val fname: String = "testLong.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Long]] = Array(Array(-8589934592L, 8589934593L), Array(8589934594L, -8589934595L))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(1) == -8589934595L)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0) == -8589934595L)
    assert (schemaRdd.schema.fields(0) == StructField("C0",LongType,false))

    dataFrame.delete()
  }

  test("DataFrame[T_NUM(Double)] to SchemaRDD[DoubleType]") {
    import h2oContext._
    val fname: String = "testDouble.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Double]] = Array(Array(-1.7, 23.456), Array(-99.9, 100.00012))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).atd(1) == 100.00012)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0) == 100.00012)
    assert (schemaRdd.schema.fields(0) == StructField("C0",DoubleType,false))

    dataFrame.delete()
  }

  test("DataFrame[T_STR] to SchemaRDD[StringType]") {
    import h2oContext._
    val fname: String = "testString.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 3L, 2L)
    val data: Array[Array[String]] = Array(Array("string1", "string2", "string3"),
                                           Array("string4", "string5", "string6"),
                                           Array("string7", "string8"))
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_STR)

    assert (dataFrame.vec(0).chunkForChunkIdx(2).atStr(new ValueString(),1) == "string8")
    assert (dataFrame.vec(0).isString())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == "string8")
    assert (schemaRdd.schema.fields(0) == StructField("C0",StringType,false))

    dataFrame.delete()
  }

  ignore("PUBDEV-771 DataFrame[T_UUID] to SchemaRDD[StringType]") {
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
    val dataFrame = makeDataFrame(fname, colNames, chunkLayout, data, Vec.T_UUID)

    assert (UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getLeastSignificantBits() ==
      dataFrame.vec(0).chunkForChunkIdx(0).at16l(0)                                           &
      UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getMostSignificantBits()        ==
        dataFrame.vec(0).chunkForChunkIdx(0).at16h(0))
    assert (dataFrame.vec(0).isUUID())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asSchemaRDD(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.schema.fields(0) == StructField("C0",StringType,false))
    assert (schemaRdd.take(6)(5)(0) == "6870f256-e145-4d75-adb0-99ccb77d5d3a")
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

  test("SchemaRDD[Float] to DataFrame[Numeric]") {
    import sqlContext._

    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val srdd:SchemaRDD = sc.parallelize(values).map(v => FloatField(v))
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("SchemaRDD[Double] to DataFrame[Numeric]") {
    import sqlContext._

    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val srdd:SchemaRDD = sc.parallelize(values).map(v => DoubleField(v))
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

  test("SchemaRDD[flattened StructType] to DataFrame[Simple StructType]") {
    import sqlContext._
    val num = 20
    val values = (1 to num).map(x => PrimitiveA(x, "name="+x))
    val srdd:SchemaRDD = sc.parallelize(values)
    // Convert to DataFrame
    val dataFrame = hc.toDataFrame(srdd)

    assertDataFrameInvariants(srdd, dataFrame)
  }

  test("SchemaRDD[flattened StructType] to DataFrame[Composed StructType]") {
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

  def makeDataFrame[T: ClassTag](fname: String, colNames: Array[String], chunkLayout: Array[Long],
                                 data: Array[Array[T]], h2oType: Byte): DataFrame = {
    var f: Frame = new Frame(Key.make(fname))
    FrameUtils.preparePartialFrame(f,colNames)
    f.update(null)

    for( i <- 0 to chunkLayout.length-1) { createNC(fname, data(i), i, chunkLayout(i).toInt, h2oType) }

    f = DKV.get(fname).get()

    FrameUtils.finalizePartialFrame(f, chunkLayout, null, Array(h2oType))

    return new DataFrame(f)
  }

  def createNC[T: ClassTag](fname: String, data: Array[T], cidx: Integer, len: Integer, h2oType: Byte): NewChunk = {
    val nchunks: Array[NewChunk] = FrameUtils.createNewChunks(fname, cidx)

    data.foreach { el =>
      el match {
        case x if x.isInstanceOf[UUID]               => nchunks(0).addUUID(
          x.asInstanceOf[UUID].getLeastSignificantBits(),
          x.asInstanceOf[UUID].getMostSignificantBits())
        case x if x.isInstanceOf[String]             => nchunks(0).addStr(new ValueString(x.asInstanceOf[String]))
        case x if x.isInstanceOf[Byte]               => nchunks(0).addNum(x.asInstanceOf[Byte])
        case x if x.isInstanceOf[Short]              => nchunks(0).addNum(x.asInstanceOf[Short])
        case x if x.isInstanceOf[Integer] &
          h2oType == Vec.T_ENUM                      => nchunks(0).addEnum(x.asInstanceOf[Integer])
        case x if x.isInstanceOf[Integer] &
          h2oType != Vec.T_ENUM                      => nchunks(0).addNum(x.asInstanceOf[Integer].toDouble)
        case x if x.isInstanceOf[Long]               => nchunks(0).addNum(x.asInstanceOf[Long].toDouble)
        case x if x.isInstanceOf[Double]             => nchunks(0).addNum(x.asInstanceOf[Double])
        case _ => ???
      }
    }
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
case class ByteField   (v: Byte)
case class ShortField  (v: Short)
case class IntField    (v: Int)
case class LongField   (v: Long)
case class FloatField  (v: Float)
case class DoubleField (v: Double)
case class StringField(v: String)
case class TimestampField(v: Timestamp)

case class PrimitiveA(n: Int, name: String)
case class ComposedA(a: PrimitiveA, weight: Double)

case class PrimitiveB(f: Seq[Int])

case class PrimitiveC(f: mllib.linalg.Vector)

case class Prostate(ID      :Option[Long]  ,
                    CAPSULE :Option[Int]  ,
                    AGE     :Option[Int]  ,
                    RACE    :Option[Int]  ,
                    DPROS   :Option[Int]  ,
                    DCAPS   :Option[Int]  ,
                    PSA     :Option[Float],
                    VOL     :Option[Float],
                    GLEASON :Option[Int]  ) {
  def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
}