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

import java.io.File
import java.sql.Timestamp
import java.util.UUID

import hex.splitframe.ShuffleSplitFrame
import org.apache.spark.SparkContext
import org.apache.spark.h2o.testdata._
import org.apache.spark.h2o.utils.H2OSchemaUtils.flatSchema
import org.apache.spark.h2o.utils.{H2OSchemaUtils, SharedSparkTestContext}
import org.apache.spark.h2o.{Frame => _, H2OFrame => _}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Assertions, FunSuite}
import water.Key
import water.fvec._
import water.parser.BufferedString

/**
 * Testing Conversions between H2OFrame and Spark DataFrame
 */
@RunWith(classOf[JUnitRunner])
class DataFrameConverterTest extends FunSuite with SharedSparkTestContext {
  
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Creation of H2ODataFrame") {
    // FIXME: create different shapes of frame
    val h2oFrame = new H2OFrame(new File("examples/smalldata/prostate.csv"))

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert(h2oFrame.numRows() == dataFrame.count(), "Number of lines in H2O Frame and in Spark DataFrame has to be same")
    h2oFrame.delete()
  }

  // H2OFrame to DataFrame[T] JUnits
  test("PUBDEV-766 H2OFrame[T_ENUM] to DataFrame[StringType]") {
    val fname: String = "testEnum.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Integer]] = Array(Array(1, 0), Array(0, 1))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_CAT, colDomains = Array(Array("ZERO", "ONE")))

    assert (h2oFrame.vec(0).chunkForChunkIdx(0).at8(0) == 1)
    assert (h2oFrame.vec(0).isCategorical)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(4)(3)(0) == "ONE")
    assert (dataFrame.schema.fields(0) match{
      case StructField("C0",StringType,false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_TIME] to DataFrame[TimestampType]") {
    val fname: String = "testTime.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Long]] = Array(Array(1428517563L, 1428517564L), Array(1428517565L, 1428517566L))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_TIME)

    assert (h2oFrame.vec(0).chunkForChunkIdx(1).at8(1) == 1428517566L)
    assert (h2oFrame.vec(0).isTime)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(4)(3)(0).asInstanceOf[Timestamp].getTime == 1428517566L)
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",TimestampType,false,_)=> true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Byte)] to DataFrame[ByteType]") {
    val fname: String = "testByte.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Byte]] = Array(Array(-1, 2, -3) map (_.toByte),
      Array(4, -5, 6, -7, 8) map (_.toByte))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (h2oFrame.vec(0).chunkForChunkIdx(1).at8(4) == 8)
    assert (h2oFrame.vec(0).isNumeric)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(8)(7)(0) == 8)
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",ByteType,false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Short)] to DataFrame[ShortType]") {
    val fname: String = "testShort.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Short]] = Array(Array(-200, 201, -202) map (_.toShort),
      Array(204, -205, 206, -207, 208) map (_.toShort))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (h2oFrame.vec(0).chunkForChunkIdx(1).at8(4) == 208)
    assert (h2oFrame.vec(0).isNumeric)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(8)(7)(0) == 208)
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",ShortType,false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Integer)] to DataFrame[IntegerType]") {
    val fname: String = "testInteger.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(3L, 5L)
    val data: Array[Array[Integer]] = Array(Array(-100000, 100001, -100002),
      Array(100004, -100005, 100006, -100007, 100008))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (h2oFrame.vec(0).chunkForChunkIdx(1).at8(4) == 100008)
    assert (h2oFrame.vec(0).isNumeric)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(8)(7)(0) == 100008)
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",IntegerType,false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("PUBDEV-767 H2OFrame[T_NUM(Long)] to DataFrame[LongType]") {
    val fname: String = "testLong.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Long]] = Array(Array(-8589934592L, 8589934593L), Array(8589934594L, -8589934595L))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (h2oFrame.vec(0).chunkForChunkIdx(1).at8(1) == -8589934595L)
    assert (h2oFrame.vec(0).isNumeric)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(4)(3)(0) == -8589934595L)
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",LongType,false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Double)] to DataFrame[DoubleType]") {
    val fname: String = "testDouble.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(2L, 2L)
    val data: Array[Array[Double]] = Array(Array(-1.7, 23.456), Array(-99.9, 100.00012))
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)

    assert (h2oFrame.vec(0).chunkForChunkIdx(1).atd(1) == 100.00012)
    assert (h2oFrame.vec(0).isNumeric)

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (dataFrame.count == h2oFrame.numRows())
    assert (dataFrame.take(4)(3)(0) == 100.00012)
    assert (dataFrame.schema.fields(0) match {
      case StructField("C0",DoubleType,false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

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

  test("DataFrame[ByteField] to H2OFrame[Numeric]") {
    import spark.implicits._

    val df = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte])).toDF()
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[ShortField] to H2OFrame[Numeric]") {
    import spark.implicits._

    val df = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short])).toDF()
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[IntField] to H2OFrame[Numeric]") {
    import spark.implicits._

    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val df = sc.parallelize(values).map(v => IntField(v)).toDF()
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[LongField] to H2OFrame[Numeric]") {
    import spark.implicits._

    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val df = sc.parallelize(values).map(v => LongField(v)).toDF()
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[FloatField] to H2OFrame[Numeric]") {
    import spark.implicits._

    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val df = sc.parallelize(values).map(v => FloatField(v)).toDF
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[DoubleField] to H2OFrame[Numeric]") {
    import spark.implicits._

    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val df = sc.parallelize(values).map(v => DoubleField(v)).toDF
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isNumeric)
  }

  test("DataFrame[StringField] to H2OFrame[String]") {
    import spark.implicits._

    val domSize = 3000
    val values = (1 to domSize).map( v => StringField(v + "-value"))
    val df = sc.parallelize(values).toDF()
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isString)
    assert (h2oFrame.domains()(0) == null)
    val catVec = h2oFrame.vec(0).toCategoricalVec
    assert (catVec.isCategorical)
    assert (catVec.domain() != null)
    assert (catVec.domain().length == domSize)
  }

  test("DataFrame[TimeStampField] to H2OFrame[Time]") {
    import spark.implicits._

    val num = 20
    val values = (1 to num).map(v => new Timestamp(v))
    val df = sc.parallelize(values).map(v => TimestampField(v)).toDF
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert (h2oFrame.vec(0).isTime)
  }

  test("DataFrame[Struct(TimeStampField)] to H2OFrame[Time]") {
    import spark.implicits._

    val num = 20
    val values = (1 to num).map(v =>
                                  ComposedWithTimestamp(
                                    PrimitiveA(v, v.toString),
                                    TimestampField(new Timestamp(v))
                                  ))
    val df = sc.parallelize(values).toDF
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.vec(0).isNumeric)
    assert(h2oFrame.vec(1).isString)
    assert(h2oFrame.vec(2).isTime)
  }

  ignore("H2OFrame[Time] to DataFrame[TimeStamp]") {

  }

  test("H2OFrame[Simple StructType] to DataFrame[flattened StructType]") {
    import spark.implicits._
    val num = 20
    val values = (1 to num).map(x => PrimitiveA(x, "name=" + x))
    val df = sc.parallelize(values).toDF
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
  }

  test("DataFrame[flattened StructType] to H2OFrame[Composed StructType]") {
    import spark.implicits._

    val num = 20
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name=" + x), x * 3.14))
    val df = sc.parallelize(values).toDF
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
  }

  test("DataFrame[IntField] to H2OFrame with empty partitions (error detected in calling ShuffleSplitFrame)") {
    import spark.implicits._

    val values = 1 to 100
    val df = sc.parallelize(values, 2000).map(v => IntField(v)).toDF

    val h2oFrame = hc.asH2OFrame(df)

    ShuffleSplitFrame.shuffleSplitFrame(h2oFrame,
        Array[String]("train.hex", "test.hex", "hold.hex") map Key.make[Frame],
        Array[Double](0.5, 0.3, 0.2), 1234567689L)
  }

  test("Expand composed schema of DataFrame") {
    import spark.implicits._
    val num = 2
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name=" + x), x * 1.0))
    val rdd: RDD[ComposedA] = sc.parallelize(values)
    val df = rdd.toDF
    
    val expectObjectsNullableByDefault = true

    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, df)
    val expected: Vector[(List[Int], StructField, Int)] = Vector(
      (List(0, 0), StructField("a.n", IntegerType), 0),
      (List(0, 1), StructField("a.name", StringType), 0),
      (List(1), StructField("weight", DoubleType, nullable = expectObjectsNullableByDefault), 0))
    Assertions.assertResult(expected.length)(expandedSchema.length)

    assertResult(expectObjectsNullableByDefault, "Nullability in component#2")(expandedSchema(2)._2.nullable)
    for {i <- expected.indices} {
      assertResult(expected(i), s"@$i")(expandedSchema(i))
    }
    
    assert(expandedSchema === expected)

    // Verify transformation into dataframe
    val h2oFrame = hc.asH2OFrame(df)
    assertH2OFrameInvariants(df, h2oFrame)

    // Verify data stored in h2oFrame after transformation
    assertVectorIntValues(h2oFrame.vec(0), Seq(1,2))
    assertVectorStringValues(h2oFrame.vec(1), Seq("name=1", "name=2"))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(1.0, 2.0))
  }

  test("Expand schema with array") {
    import spark.implicits._
    val num = 5
    val values = (1 to num).map(x => PrimitiveB(1 to x))
    val df = sc.parallelize(values).toDF
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, df)
    val metadatas = expandedSchema.map(f =>f._2.metadata)

    assert(expandedSchema === Vector(
      (List(0), StructField("f0", IntegerType, nullable=false, metadatas.head), 1),
      (List(0), StructField("f1", IntegerType, nullable=false, metadatas(1)), 1),
      (List(0), StructField("f2", IntegerType, nullable=false, metadatas(2)), 1),
      (List(0), StructField("f3", IntegerType, nullable=false, metadatas(3)), 1),
      (List(0), StructField("f4", IntegerType, nullable=false, metadatas(4)), 1)))

    // Verify transformation into dataframe
    val h2oFrame = hc.asH2OFrame(df)
    // Basic invariants
    assert(df.count == h2oFrame.numRows(), "Number of rows has to match")
    assert(5 == h2oFrame.numCols(), "Number columns should match")

    // Verify data stored in h2oFrame after transformation
    assertVectorIntValues(h2oFrame.vec(0), Seq(1, 1, 1, 1, 1))
    assertVectorIntValues(h2oFrame.vec(1), Seq(-1, 2, 2, 2, 2))
    assertVectorIntValues(h2oFrame.vec(2), Seq(-1, -1, 3, 3, 3))
    assertVectorIntValues(h2oFrame.vec(3), Seq(-1, -1, -1, 4, 4))
    assertVectorIntValues(h2oFrame.vec(4), Seq(-1, -1, -1, -1, 5))
  }

  test("Expand schema with dense vectors") {
    import spark.implicits._

    val num = 2
    val values = (1 to num).map(x => PrimitiveC(Vectors.dense((1 to x).map(1.0*_).toArray)))
    val df = sc.parallelize(values).toDF
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, df)

    assert (expandedSchema === Vector(
      (List(0),StructField("f0",DoubleType),2),
      (List(0),StructField("f1",DoubleType),2)))

    // Verify transformation into DataFrame
    val h2oFrame = hc.asH2OFrame(df)
    // Basic invariants
    assert( df.count == h2oFrame.numRows(), "Number of rows has to match")
    assert( 2 == h2oFrame.numCols(), "Number columns should match")

    // Verify data stored in h2oFrame after transformation
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(1.0,1.0))
    // For vectors missing values are replaced by zeros
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0.0, 2.0))
  }

  test("Expand schema with sparse vectors") {
    import spark.implicits._
    val num = 3
    val values = (0 until num).map(x =>
      PrimitiveC(
        Vectors.sparse(num, (0 until num).map(i => if (i == x) (i, 1.0) else (i, 0.0)) )
      ))
    val df = sc.parallelize(values).toDF()
    val expandedSchema = H2OSchemaUtils.expandedSchema(sc, df)

    assert (expandedSchema === Vector(
      (List(0),StructField("f0",DoubleType),2),
      (List(0),StructField("f1",DoubleType),2),
      (List(0),StructField("f2",DoubleType),2)))

    // Verify transformation into DataFrame
    val h2oFrame = hc.asH2OFrame(df)
    // Basic invariants
    assert( df.count == h2oFrame.numRows(), "Number of rows has to match")
    assert( 3 == h2oFrame.numCols(), "Number columns should match")

    // Verify data stored in h2oFrame after transformation
    assertVectorDoubleValues(h2oFrame.vec(0), Seq(1.0,0.0,0.0))
    assertVectorDoubleValues(h2oFrame.vec(1), Seq(0.0,1.0,0.0))
    assertVectorDoubleValues(h2oFrame.vec(2), Seq(0.0,0.0,1.0))
  }

  test("Add metadata to Dataframe") {
    val fname: String = "testMetadata.hex"
    val colNames: Array[String] = Array("C0")
    val chunkLayout: Array[Long] = Array(50L, 50L)
    val data: Array[Array[Long]] = Array((1L to 50L).toArray, (51L to 100L).toArray)
    val h2oFrame = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_NUM)
    println(h2oFrame.vec(0).pctiles())
    val dataFrame = hc.asDataFrame(h2oFrame)

    assert(dataFrame.schema("C0").metadata.getDouble("min") == 1L)
    assert(dataFrame.schema("C0").metadata.getLong("count") == 100L)

    h2oFrame.delete()

    val h2oFrameEnum = makeH2OFrame(fname, colNames, chunkLayout, data, Vec.T_CAT, colDomains = Array(Array("ZERO", "ONE")))
    val dataFrameEnum = hc.asDataFrame(h2oFrameEnum)
    assert(dataFrameEnum.schema("C0").metadata.getLong("cardinality") == 2L)
    h2oFrameEnum.delete()
  }

  test("SW-303 Decimal column conversion failure") {
    import sqlContext.implicits._
    val df = sc.parallelize(Array("ok", "bad", "ok", "bad", "bad")).toDF("status")
    df.createOrReplaceTempView("responses")
    val dfDouble = spark.sqlContext.sql("SELECT IF(r.status = 'ok', 0.0, 1.0) AS cancelled FROM responses AS r")
    val frame = hc.asH2OFrame(dfDouble)
    assertVectorDoubleValues(frame.vec(0), Seq(0.0, 1.0, 0.0, 1.0, 1.0))
  }

  test("SW-304 DateType column conversion failure") {
    import java.sql.Date

    import sqlContext.implicits._
    val df = sc.parallelize(Seq(DateField(Date.valueOf("2016-12-24")))).toDF("created_date")
    val hf = hc.asH2OFrame(df)
    assert(hf.numRows() == 1)
    assert(hf.numCols() == 1)
    assert(hf.vec(0).at8(0) == Date.valueOf("2016-12-24").getTime)
  }

  test("SW-310 Decimal(2,1) not compatible in h2o frame") {
    import sqlContext.implicits._
    val dfInput = sc.parallelize(1 to 6).map(v => (v, v*v)).toDF("single", "double")
    dfInput.createOrReplaceTempView("dfInput")
    val df = spark.sqlContext.sql("SELECT *, IF(double < 5, 1.0, 0.0) AS label FROM dfInput")
    val hf = hc.asH2OFrame(df)
    assert(hf.numRows() == 6)
    assert(hf.numCols() == 3)
    assertVectorIntValues(hf.vec("single"), Seq(1, 2, 3, 4, 5, 6))
    assertVectorIntValues(hf.vec("double"), Seq(1, 4, 9, 16, 25, 36))
    assertVectorDoubleValues(hf.vec("label"), Seq(1.0, 1.0, 0.0, 0.0, 0.0, 0.0))
   }

  def fp(it:Iterator[Row]):Unit = {
    println(it.size)
  }

  def assertH2OFrameInvariants(inputDF: DataFrame, df: H2OFrame): Unit = {
    assert( inputDF.count == df.numRows(), "Number of rows has to match")
    assert( df.numCols() == flatSchema(inputDF.schema).length , "Number columns should match")
  }

  def assertVectorIntValues(vec: water.fvec.Vec, values: Seq[Int]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) -1 == values(rIdx)
        else vec.at8(rIdx) == values(rIdx), "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorDoubleValues(vec: water.fvec.Vec, values: Seq[Double]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(if (vec.isNA(rIdx)) values(rIdx)==Double.NaN // this is Scala i can do NaN comparision
        else vec.at(rIdx) == values(rIdx), "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorEnumValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val vecDom = vec.domain()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(vecDom(vec.at8(rIdx).asInstanceOf[Int]) == values(rIdx), "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorStringValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val valString = new BufferedString()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(
        vec.isNA(rIdx) || {
          vec.atStr(valString, rIdx)
          valString.bytesToString() == values(rIdx)
        }, "Values stored in H2OFrame has to match specified values")
    }
  }
}
