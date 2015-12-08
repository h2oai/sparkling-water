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
import org.apache.spark.h2o.H2OFrame
import org.apache.spark.h2o.H2OSchemaUtils.flatSchema
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.h2o._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, mllib}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Frame
import water.fvec._
import water.parser.{Categorical, BufferedString}
import water.{DKV, Key}

import scala.reflect.ClassTag


/**
 * Testing schema for h2o schema rdd transformation.
 */
@RunWith(classOf[JUnitRunner])
class H2OSchemaRDDTest extends FunSuite with SparkTestContext {

  sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
  hc = H2OContext.getOrCreate(sc)
  // Shared sqlContext
  val h2oContext = hc
  val sqlContext = new SQLContext(sc)

  test("test creation of H2OSchemaRDD") {
    import h2oContext._

    // FIXME: create different shapes of frame
    val dataFrame = new H2OFrame(new File("examples/smalldata/prostate.csv"))
    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert(dataFrame.numRows() == schemaRdd.count(), "Number of lines in dataframe and in schema has to be same")
    dataFrame.delete()
  }

  // H2OFrame to RDD[T] JUnits
  test("H2OFrame[T_NUM] to RDD[Prostate]") {
    import h2oContext._
    val dataFrame: H2OFrame = new H2OFrame(new File("examples/smalldata/prostate.csv"))
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

  // H2OFrame to DataFrame[T] JUnits
  test("PUBDEV-766 H2OFrame[T_ENUM] to DataFrame[StringType]") {
    import h2oContext._
    val fname: String = "testEnum.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Integer]] = Array(Array(1, 0), Array(0, 1))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_CAT, colDomains = Array(Array("ZERO", "ONE")))

    assert (dataFrame.vec(0).chunkForChunkIdx(0).at8(0) == 1)
    assert (dataFrame.vec(0).isCategorical())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0) == "ONE")
    assert (schemaRdd.schema.fields(0) match{
      case StructField("C0",StringType,false, _) => true
      case _ => false
    })

    dataFrame.delete()
  }

  test("H2OFrame[T_TIME] to DataFrame[TimestampType]") {
    import h2oContext._
    val fname: String = "testTime.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Long]] = Array(Array(1428517563L, 1428517564L), Array(1428517565L, 1428517566L))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_TIME)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(1) == 1428517566L)
    assert (dataFrame.vec(0).isTime())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0).asInstanceOf[Timestamp].getTime() == 1428517566L)
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",TimestampType,false,_)=> true
      case _ => false
    })

    dataFrame.delete()
  }

  test("H2OFrame[T_NUM(Byte)] to DataFrame[ByteType]") {
    import h2oContext._
    val fname: String = "testByte.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Byte]] = Array(Array(-1.toByte, 2.toByte, -3.toByte),
      Array(4.toByte, -5.toByte, 6.toByte, -7.toByte, 8.toByte))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(4) == 8)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == 8)
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",ByteType,false, _) => true
      case _ => false
    })

    dataFrame.delete()
  }

  test("H2OFrame[T_NUM(Short)] to DataFrame[ShortType]") {
    import h2oContext._
    val fname: String = "testShort.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Short]] = Array(Array(-200.toShort, 201.toShort, -202.toShort),
      Array(204.toShort, -205.toShort, 206.toShort, -207.toShort, 208.toShort))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(4) == 208)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == 208)
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",ShortType,false, _) => true
      case _ => false
    })

    dataFrame.delete()
  }

  test("H2OFrame[T_NUM(Integer)] to DataFrame[IntegerType]") {
    import h2oContext._
    val fname: String = "testInteger.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Integer]] = Array(Array(-100000, 100001, -100002),
      Array(100004, -100005, 100006, -100007, 100008))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(4) == 100008)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == 100008)
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",IntegerType,false, _) => true
      case _ => false
    })

    dataFrame.delete()
  }

  test("PUBDEV-767 H2OFrame[T_NUM(Long)] to DataFrame[LongType]") {
    import h2oContext._
    val fname: String = "testLong.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Long]] = Array(Array(-8589934592L, 8589934593L), Array(8589934594L, -8589934595L))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).at8(1) == -8589934595L)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0) == -8589934595L)
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",LongType,false, _) => true
      case _ => false
    })

    dataFrame.delete()
  }

  test("H2OFrame[T_NUM(Double)] to DataFrame[DoubleType]") {
    import h2oContext._
    val fname: String = "testDouble.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Double]] = Array(Array(-1.7, 23.456), Array(-99.9, 100.00012))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (dataFrame.vec(0).chunkForChunkIdx(1).atd(1) == 100.00012)
    assert (dataFrame.vec(0).isNumeric())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(4)(3)(0) == 100.00012)
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",DoubleType,false, _) => true
      case _ => false
    })

    dataFrame.delete()
  }

  test("H2OFrame[T_STR] to DataFrame[StringType]") {
    import h2oContext._
    val fname: String = "testString.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 3L, 2L)
    val data: Array[Array[String]] = Array(Array("string1", "string2", "string3"),
                                           Array("string4", "string5", "string6"),
                                           Array("string7", "string8"))
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_STR)

    assert (dataFrame.vec(0).chunkForChunkIdx(2).atStr(new BufferedString(),1).toString.equals("string8"))
    assert (dataFrame.vec(0).isString())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.take(8)(7)(0) == "string8")
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",StringType,false, _) => true
      case _ => false
    } )

    dataFrame.delete()
  }

  test("PUBDEV-771 H2OFrame[T_UUID] to DataFrame[StringType]") {
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
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_UUID)

    assert (UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getLeastSignificantBits() ==
      dataFrame.vec(0).chunkForChunkIdx(0).at16l(0)                                           &
      UUID.fromString("6870f256-e145-4d75-adb0-99ccb77d5d3a").getMostSignificantBits()        ==
        dataFrame.vec(0).chunkForChunkIdx(0).at16h(0))
    assert (dataFrame.vec(0).isUUID())

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (schemaRdd.count == dataFrame.numRows())
    assert (schemaRdd.schema.fields(0) match {
      case StructField("C0",StringType,false, _) => true
      case _ => false
    })
    val valuesInRdd = schemaRdd.collect().map(row => row(0))
    for (idx <- valuesInRdd.indices)
      assert (valuesInRdd(idx) == "6870f256-e145-4d75-adb0-99ccb77d5d3" + ('a' + idx).asInstanceOf[Char])
    dataFrame.delete()
  }

  test("test RDD to H2OFrame to DataFrame way") {
    import h2oContext._

    val rdd = sc.parallelize(1 to 10000, 1000).map(i => IntHolder(Some(i)))
    val dataFrame:H2OFrame = rdd

    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert (rdd.count == dataFrame.numRows())
    assert (rdd.count == schemaRdd.count)
  }

  test("RDD[Byte] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte]))
    val dataFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("RDD[Short] to H2OFrame[Numeric]") {
    val rdd = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short]))
    val dataFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("RDD[Int] to H2OFrame[Numeric]") {
    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val rdd = sc.parallelize(values).map(v => IntField(v))
    val dataFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("RDD[Long] to H2OFrame[Numeric]") {
    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val rdd = sc.parallelize(values).map(v => LongField(v))
    val dataFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("RDD[Float] to H2OFrame[Numeric]") {
    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val rdd = sc.parallelize(values).map(v => FloatField(v))
    val dataFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("RDD[Double] to H2OFrame[Numeric]") {
    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val rdd = sc.parallelize(values).map(v => DoubleField(v))
    val dataFrame = hc.asH2OFrame(rdd)

    assertRDDH2OFrameInvariants(rdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[Byte] to H2OFrame[Numeric]") {
    import sqlContext.implicits._

    val srdd:DataFrame = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte])).toDF()
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[Short] to H2OFrame[Numeric]") {
    import sqlContext.implicits._

    val srdd:DataFrame = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short])).toDF()
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[Int] to H2OFrame[Numeric]") {
    import sqlContext.implicits._

    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val srdd:DataFrame = sc.parallelize(values).map(v => IntField(v)).toDF()
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[Long] to H2OFrame[Numeric]") {
    import sqlContext.implicits._

    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val srdd:DataFrame = sc.parallelize(values).map(v => LongField(v)).toDF()
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[Float] to H2OFrame[Numeric]") {
    import sqlContext.implicits._

    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val srdd:DataFrame = sc.parallelize(values).map(v => FloatField(v)).toDF
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[Double] to H2OFrame[Numeric]") {
    import sqlContext.implicits._

    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val srdd:DataFrame = sc.parallelize(values).map(v => DoubleField(v)).toDF
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isNumeric())
  }

  test("DataFrame[String] to H2OFrame[String]") {
    import sqlContext.implicits._

    val domSize = 3000
    val values = (1 to domSize).map( v => StringField(v + "-value"))
    val srdd:DataFrame = sc.parallelize(values).toDF()
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isString())
    assert (dataFrame.domains()(0) == null)
    val catVec = dataFrame.vec(0).toCategoricalVec
    assert (catVec.isCategorical)
    assert (catVec.domain() != null)
    assert (catVec.domain().length == domSize)
  }

  test("DataFrame[TimeStamp] to H2OFrame[Time]") {
    import sqlContext.implicits._

    val num = 20
    val values = (1 to num).map(v => new Timestamp(v))
    val srdd:DataFrame = sc.parallelize(values).map(v => TimestampField(v)).toDF
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
    assert (dataFrame.vec(0).isTime())
  }

  ignore("H2OFrame[Time] to DataFrame[TimeStamp]") {

  }

  test("H2OFrame[Simple StructType] to DataFrame[flattened StructType]") {
    import sqlContext.implicits._
    val num = 20
    val values = (1 to num).map(x => PrimitiveA(x, "name=" + x))
    val srdd:DataFrame = sc.parallelize(values).toDF
    // Convert to H2OFrame
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
  }

  test("DataFrame[flattened StructType] to H2OFrame[Composed StructType]") {
    import sqlContext.implicits._
    val num = 20
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name=" + x), x * 3.14))
    val srdd:DataFrame = sc.parallelize(values).toDF
    // Convert to H2OFrame
    val dataFrame = hc.asH2OFrame(srdd)

    assertH2OFrameInvariants(srdd, dataFrame)
  }

  test("DataFrame[Int] to H2OFrame with empty partitions (error detected in calling ShuffleSplitFrame)") {
    import sqlContext.implicits._

    val values = 1 to 100
    val srdd:DataFrame = sc.parallelize(values, 2000).map(v => IntField(v)).toDF

    val dataFrame = hc.asH2OFrame(srdd)

    ShuffleSplitFrame.shuffleSplitFrame(dataFrame,
        Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make[Frame](_)),
        Array[Double](0.5, 0.3, 0.2), 1234567689L)
  }

  test("Expand composed schema of RDD") {
    import sqlContext.implicits._
    val num = 2
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name=" + x), x * 1.0))
    val srdd: DataFrame = sc.parallelize(values).toDF

    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)
    assert(expandedSchema === Vector(
      (List(0, 0), StructField("a.n", IntegerType, true), 0),
      (List(0, 1), StructField("a.name", StringType, true), 0),
      (List(1), StructField("weight", DoubleType, false), 0)))

    // Verify transformation into dataframe
    val dataFrame = hc.asH2OFrame(srdd)
    assertH2OFrameInvariants(srdd, dataFrame)

    // Verify data stored in dataFrame after transformation
    assertVectorIntValues(dataFrame.vec(0), Seq(1,2))
    assertVectorStringValues(dataFrame.vec(1), Seq("name=1", "name=2"))
    assertVectorDoubleValues(dataFrame.vec(2), Seq(1.0, 2.0))
  }

  test("Expand schema with array") {
    import sqlContext.implicits._
    val num = 5
    val values = (1 to num).map(x => PrimitiveB(1 to x))
    val srdd: DataFrame = sc.parallelize(values).toDF
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)
    val metadatas = expandedSchema.map(f =>f._2.metadata)

    assert(expandedSchema === Vector(
      (List(0), StructField("f0", IntegerType, false, metadatas(0)), 1),
      (List(0), StructField("f1", IntegerType, false, metadatas(1)), 1),
      (List(0), StructField("f2", IntegerType, false, metadatas(2)), 1),
      (List(0), StructField("f3", IntegerType, false, metadatas(3)), 1),
      (List(0), StructField("f4", IntegerType, false, metadatas(4)), 1)))

    // Verify transformation into dataframe
    val dataFrame = hc.asH2OFrame(srdd)
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
    import sqlContext.implicits._
    val num = 2
    val values = (1 to num).map(x => PrimitiveC(Vectors.dense((1 to x).map(1.0*_).toArray)))
    val srdd:DataFrame = sc.parallelize(values).toDF
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)

    assert (expandedSchema === Vector(
      (List(0),StructField("f0",DoubleType,true),2),
      (List(0),StructField("f1",DoubleType,true),2)))

    // Verify transformation into dataframe
    val dataFrame = hc.asH2OFrame(srdd)
    // Basic invariants
    assert( srdd.count == dataFrame.numRows(), "Number of rows has to match")
    assert( 2 == dataFrame.numCols(), "Number columns should match")

    // Verify data stored in dataFrame after transformation
    assertVectorDoubleValues(dataFrame.vec(0), Seq(1.0,1.0))
    // For vectors missing values are replaced by zeros
    assertVectorDoubleValues(dataFrame.vec(1), Seq(0.0, 2.0))
  }

  test("Expand schema with sparse vectors") {
    import sqlContext.implicits._
    val num = 3
    val values = (0 until num).map(x =>
      PrimitiveC(
        Vectors.sparse(num, (0 until num).map(i => if (i==x) (i, 1.0) else (i, 0.0)).toSeq )
      ))
    val srdd:DataFrame = sc.parallelize(values).toDF()
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, srdd)

    assert (expandedSchema === Vector(
      (List(0),StructField("f0",DoubleType,true),2),
      (List(0),StructField("f1",DoubleType,true),2),
      (List(0),StructField("f2",DoubleType,true),2)))

    // Verify transformation into dataframe
    val dataFrame = hc.asH2OFrame(srdd)
    // Basic invariants
    assert( srdd.count == dataFrame.numRows(), "Number of rows has to match")
    assert( 3 == dataFrame.numCols(), "Number columns should match")

    // Verify data stored in dataFrame after transformation
    assertVectorDoubleValues(dataFrame.vec(0), Seq(1.0,0.0,0.0))
    assertVectorDoubleValues(dataFrame.vec(1), Seq(0.0,1.0,0.0))
    assertVectorDoubleValues(dataFrame.vec(2), Seq(0.0,0.0,1.0))
  }

  test("Add metadata to Dataframe") {
    import h2oContext._
    val fname: String = "testMetadata.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(50L, 50L)
    val data: Array[Array[Long]] = Array((1L to 50L).toArray, (51L to 100L).toArray)
    val dataFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)
    println(dataFrame.vec(0).pctiles())
    implicit val sqlContext = new SQLContext(sc)
    val schemaRdd = asDataFrame(dataFrame)

    assert(schemaRdd.schema("C0").metadata.getDouble("min") == 1L)
    assert(schemaRdd.schema("C0").metadata.getLong("count") == 100L)

    dataFrame.delete()

    val fnameEnum: String = "testEnum.hex"
    val colNamesEnum: Array[String] = Array("C0")
    val chunkLayoutEnum: Array[Long] = Array(2L, 2L)
    val dataEnum: Array[Array[Integer]] = Array(Array(1, 0), Array(0, 1))
    val dataFrameEnum = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_CAT, colDomains = Array(Array("ZERO", "ONE")))
    val schemaRddEnum = asDataFrame(dataFrameEnum)
    assert(schemaRddEnum.schema("C0").metadata.getLong("cardinality") == 2L)
    dataFrameEnum.delete()
  }

  def makeH2OFrame[T: ClassTag](fname: String, colNames: Array[String], chunkLayout: Array[Long],
                                 data: Array[Array[T]], h2oType: Byte, colDomains: Array[Array[String]] = null): H2OFrame = {
    var f: Frame = new Frame(Key.make(fname))
    FrameUtils.preparePartialFrame(f,colNames)
    f.update(null)

    for( i <- 0 to chunkLayout.length-1) { createNC(fname, data(i), i, chunkLayout(i).toInt, Array(h2oType)) }

    f = DKV.get(fname).get()

    FrameUtils.finalizePartialFrame(f, chunkLayout, colDomains, Array(h2oType))

    return new H2OFrame(f)
  }

  def createNC[T: ClassTag](fname: String, data: Array[T], cidx: Integer, len: Integer, h2oType: Array[Byte]): NewChunk = {
    val nchunks: Array[NewChunk] = FrameUtils.createNewChunks(fname, h2oType, cidx)

    data.foreach { el =>
      el match {
        case x if x.isInstanceOf[UUID]               => nchunks(0).addUUID(
          x.asInstanceOf[UUID].getLeastSignificantBits(),
          x.asInstanceOf[UUID].getMostSignificantBits())
        case x if x.isInstanceOf[String]             => nchunks(0).addStr(new BufferedString(x.asInstanceOf[String]))
        case x if x.isInstanceOf[Byte]               => nchunks(0).addNum(x.asInstanceOf[Byte])
        case x if x.isInstanceOf[Short]              => nchunks(0).addNum(x.asInstanceOf[Short])
        case x if x.isInstanceOf[Integer] &
          h2oType(0) == Vec.T_CAT                      => nchunks(0).addCategorical(x.asInstanceOf[Integer])
        case x if x.isInstanceOf[Integer] &
          h2oType(0) != Vec.T_CAT                      => nchunks(0).addNum(x.asInstanceOf[Integer].toDouble)
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
      case _ => ???
    }
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

  def assertVectorStringValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val valString = new BufferedString()
    (0 until vec.length().asInstanceOf[Int]).foreach { rIdx =>
      assert(
        if (vec.isNA(rIdx)) values(rIdx)==Double.NaN // this is Scala i can do NaN comparision
        else {
          vec.atStr(valString, rIdx)
          valString.bytesToString() == values(rIdx)
        }, "values stored has to match to values in rdd")
    }
  }
}

object H2OSchemaRDDTest {

}

// Helper classes for conversion from RDD to DataFrame
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
