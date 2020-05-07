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
import java.util.TimeZone

import ai.h2o.sparkling.TestUtils._
import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Assertions, FunSuite}

@RunWith(classOf[JUnitRunner])
class DataFrameConverterTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  test("Creation of H2ODataFrame") {
    val h2oFrame = H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv")))

    val dataFrame = hc.asSparkFrame(h2oFrame)

    assert(
      h2oFrame.numberOfRows == dataFrame.count(),
      "Number of lines in H2O Frame and in Spark DataFrame has to be same")
    h2oFrame.delete()
  }

  test("Convert Empty dataframe, empty schema") {
    val schema = StructType(Seq())
    val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val fr = H2OFrame(hc.asH2OFrameKeyString(empty))

    assert(fr.numberOfColumns == 0)
    assert(fr.numberOfRows == 0)
  }

  test("Convert Empty dataframe, non-empty schema") {
    val schema = StructType(Seq(StructField("name", StringType), StructField("age", IntegerType)))
    val empty = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    val fr = H2OFrame(hc.asH2OFrameKeyString(empty))

    assert(fr.numberOfColumns == 2)
    assert(fr.numberOfRows == 0)
  }

  test("PUBDEV-766 H2OFrame[T_ENUM] to DataFrame[StringType]") {
    val df = spark.sparkContext.parallelize(Array("ONE", "ZERO", "ZERO", "ONE")).toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    h2oFrame.convertColumnsToCategorical(Array(0))
    assert(h2oFrame.columns(0).isCategorical())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.take(4)(3)(0) == "ONE")
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", StringType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_TIME] to DataFrame[TimestampType]") {
    val rdd = spark.sparkContext
      .parallelize(Array(1428517563L, 1428517564L, 1428517565L, 1428517566L).map(v => Row(new Timestamp(v))))
    val df = spark.createDataFrame(rdd, StructType(Seq(StructField("C0", TimestampType, nullable = true))))
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isTime())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.collect()(3)(0).asInstanceOf[Timestamp].getTime == 1428517566L)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", TimestampType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Byte)] to DataFrame[ByteType]") {
    val df = spark.sparkContext.parallelize(Array(-1, 2, -3, 4, -5, 6, -7, 8).map(_.toByte)).toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isNumeric())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.take(8)(7)(0) == 8)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", ByteType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Short)] to DataFrame[ShortType]") {
    val df = spark.sparkContext.parallelize(Array(-200, 201, -202, -207, 208).map(_.toShort)).toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isNumeric())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.collect()(4)(0) == 208)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", ShortType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Integer)] to DataFrame[IntegerType]") {
    val df = spark.sparkContext
      .parallelize(Array(-100000, 100001, -100002, 100004, -100005, 100006, -100007, 100008))
      .toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isNumeric())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.collect()(7)(0) == 100008)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", IntegerType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("PUBDEV-767 H2OFrame[T_NUM(Long)] to DataFrame[LongType]") {
    val df = spark.sparkContext
      .parallelize(Array(-8589934592L, 8589934593L, 8589934594L, -8589934595L))
      .toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isNumeric())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.take(4)(3)(0) == -8589934595L)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", LongType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_NUM(Double)] to DataFrame[DoubleType]") {
    val df = spark.sparkContext.parallelize(Array(-1.7, 23.456, -99.9, 100.00012)).toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isNumeric())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.take(4)(3)(0) == 100.00012)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", DoubleType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("H2OFrame[T_STR] to DataFrame[StringType]") {
    val df = spark.sparkContext
      .parallelize(Array("string1", "string2", "string3", "string4", "string5", "string6", "string7", "string8"))
      .toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isString())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.collect()(7)(0) == "string8")
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", StringType, false, _) => true
      case _ => false
    })

    h2oFrame.delete()
  }

  test("PUBDEV-771 H2OFrame[T_UUID] to DataFrame[StringType]") {
    val data = Array(
      "6870f256-e145-4d75-adb0-99ccb77d5d3a",
      "6870f256-e145-4d75-adb0-99ccb77d5d3b",
      "6870f256-e145-4d75-adb0-99ccb77d5d3c",
      "6870f256-e145-4d75-adb0-99ccb77d5d3d",
      "6870f256-e145-4d75-adb0-99ccb77d5d3e",
      "6870f256-e145-4d75-adb0-99ccb77d5d3f")

    val df = spark.sparkContext.parallelize(data).toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(h2oFrame.columns(0).isString())

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.count == h2oFrame.numberOfRows)
    assert(dataFrame.schema.fields(0) match {
      case StructField("C0", StringType, false, _) => true
      case _ => false
    })
    val valuesInRdd = dataFrame.collect().map(row => row(0))
    for (idx <- valuesInRdd.indices)
      assert(valuesInRdd(idx) == "6870f256-e145-4d75-adb0-99ccb77d5d3" + ('a' + idx).asInstanceOf[Char])
    h2oFrame.delete()
  }

  test("DataFrame[ByteField] to H2OFrame[Numeric]") {
    val df = sc.parallelize(-127 to 127).map(v => ByteField(v.asInstanceOf[Byte])).toDF()
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
  }

  test("DataFrame[ShortField] to H2OFrame[Numeric]") {
    val df = sc.parallelize(-2048 to 4096).map(v => ShortField(v.asInstanceOf[Short])).toDF()
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
  }

  test("DataFrame[IntField] to H2OFrame[Numeric]") {
    val values = Seq(Int.MinValue, Int.MaxValue, 0, -100, 200, -5000, 568901)
    val df = sc.parallelize(values).map(v => IntField(v)).toDF()
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
  }

  test("DataFrame[LongField] to H2OFrame[Numeric]") {
    val values = Seq(Long.MinValue, Long.MaxValue, 0L, -100L, 200L, -5000L, 5689323201L, -432432433335L)
    val df = sc.parallelize(values).map(v => LongField(v)).toDF()
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
  }

  test("DataFrame[FloatField] to H2OFrame[Numeric]") {
    val values = Seq(Float.MinValue, Float.MaxValue, -33.33.toFloat, 200.001.toFloat, -5000.34.toFloat)
    val df = sc.parallelize(values).map(v => FloatField(v)).toDF
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
  }

  test("DataFrame[BooleanField] to H2OFrame[Numeric]") {
    val values = Seq(true, false, true, false)
    val df = sc.parallelize(values).toDF()
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 0, 1, 0))
  }

  test("DataFrame[DoubleField] to H2OFrame[Numeric]") {
    val values = Seq(Double.MinValue, Double.MaxValue, -33.33, 200.001, -5000.34)
    val df = sc.parallelize(values).map(v => DoubleField(v)).toDF
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
  }

  test("DataFrame[StringField] to H2OFrame[String]") {
    val domSize = 3000
    val values = (1 to domSize).map(v => StringField(v + "-value"))
    val df = sc.parallelize(values).toDF()
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isString())
    assert(h2oFrame.columns(0).domain == null)
    val h2oFrameWithCat = h2oFrame.convertColumnsToCategorical(Array(0))
    assert(h2oFrameWithCat.columns(0).isCategorical())
    assert(h2oFrameWithCat.columns(0).domain != null)
    assert(h2oFrameWithCat.columns(0).domain.length == domSize)
  }

  test("DataFrame[String] to H2OFrame[T_STRING] and back") {
    val df = Seq("one", "two", "three", "four", "five", "six", "seven").toDF("Strings").repartition(3)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isString())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
  }

  test("DataFrame[String] to H2OFrame[T_CAT] and back") {
    val df = Seq("one", "two", "three", "one", "two", "three", "one").toDF("Strings").repartition(3)
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isCategorical())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
  }

  test("DataFrame[DateType] to H2OFrame[Time]") {
    val dates = Seq("2020-12-12", "2020-01-01", "2020-02-02", "2019-05-10")
    val df = dates.toDF("strings").select('strings.cast("date").alias("dates"))

    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isTime())

    def toUTC(time: Long): Long = DateTimeUtils.toUTCTime(time * 1000, TimeZone.getDefault.getID) / 1000

    val values = dates.indices.map(i => new java.sql.Date(toUTC(h2oFrame.collectLongs(0)(i))).toString)
    assert(values.sorted == dates.sorted)
  }

  test("DataFrame[TimeStampField] to H2OFrame[Time]") {
    val num = 20
    val values = (1 to num).map(v => new Timestamp(v))
    val df = sc.parallelize(values).map(v => TimestampField(v)).toDF
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isTime())
  }

  test("DataFrame[Struct(TimeStampField)] to H2OFrame[Time]") {
    val num = 20
    val values = (1 to num).map(v => ComposedWithTimestamp(PrimitiveA(v, v.toString), TimestampField(new Timestamp(v))))
    val df = sc.parallelize(values).toDF
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isNumeric())
    assert(h2oFrame.columns(1).isString())
    assert(h2oFrame.columns(2).isTime())
  }

  test("H2OFrame[Simple StructType] to DataFrame[flattened StructType]") {
    import spark.implicits._
    val num = 20
    val values = (1 to num).map(x => PrimitiveA(x, "name=" + x))
    val df = sc.parallelize(values).toDF
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
  }

  test("DataFrame[flattened StructType] to H2OFrame[Composed StructType]") {
    val num = 20
    val values = (1 to num).map(x => ComposedA(PrimitiveA(x, "name=" + x), x * 3.14))
    val df = sc.parallelize(values).toDF
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    assertH2OFrameInvariants(df, h2oFrame)
  }

  test("DataFrame[IntField] to H2OFrame with empty partitions (error detected in calling ShuffleSplitFrame)") {
    val values = 1 to 100
    val df = sc.parallelize(values, 2000).map(v => IntField(v)).toDF

    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    h2oFrame.split(0.5, 0.3, 0.1)
  }

  test("Expand composed schema of DataFrame") {
    val num = 2
    val rdd = sc.parallelize(1 to num).map(x => Row(Row(x, "name=" + x), x * 1.0))
    val schema = StructType(
      Seq(
        StructField(
          "a",
          StructType(
            Seq(StructField("n", IntegerType, nullable = false), StructField("name", StringType, nullable = true))),
          nullable = false),
        StructField("weight", DoubleType, nullable = false)))
    val df = spark.createDataFrame(rdd, schema)

    val flattenDF = SchemaUtils.flattenDataFrame(df)
    val maxElementSizes = SchemaUtils.collectMaxElementSizes(flattenDF)
    val expandedSchema = SchemaUtils.expandedSchema(SchemaUtils.flattenSchema(df), maxElementSizes)
    val expected: Vector[StructField] = Vector(
      StructField("a.n", IntegerType, nullable = false),
      StructField("a.name", StringType, nullable = true),
      StructField("weight", DoubleType, nullable = false))
    Assertions.assertResult(expected.length)(expandedSchema.length)

    for { i <- expected.indices } {
      assertResult(expected(i), s"@$i")(expandedSchema(i))
    }

    assert(expandedSchema === expected)

    // Verify transformation into dataframe
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    assertH2OFrameInvariants(df, h2oFrame)

    // Verify data stored in h2oFrame after transformation
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 2))
    assertVectorStringValues(h2oFrame.collectStrings(1), Seq("name=1", "name=2"))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(1.0, 2.0))
  }

  test("Expand schema with array") {
    val num = 5
    val rdd = sc.parallelize(1 to num).map(x => Row(1 to x))
    val schema = StructType(StructField("f", ArrayType(IntegerType, containsNull = false), nullable = false) :: Nil)
    val df = spark.createDataFrame(rdd, schema)

    val (_, _, expandedSchema) = getSchemaInfo(df)

    val metadatas = expandedSchema.map(f => f.metadata)

    assert(
      expandedSchema === Array(
        StructField("f.0", IntegerType, nullable = false, metadatas.head),
        StructField("f.1", IntegerType, nullable = true, metadatas(1)),
        StructField("f.2", IntegerType, nullable = true, metadatas(2)),
        StructField("f.3", IntegerType, nullable = true, metadatas(3)),
        StructField("f.4", IntegerType, nullable = true, metadatas(4))))

    // Verify transformation into dataframe
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(5 == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    assertVectorIntValues(h2oFrame.collectInts(0), Seq(1, 1, 1, 1, 1))
    assertVectorIntValues(h2oFrame.collectInts(1), Seq(0, 2, 2, 2, 2))
    assertVectorIntValues(h2oFrame.collectInts(2), Seq(0, 0, 3, 3, 3))
    assertVectorIntValues(h2oFrame.collectInts(3), Seq(0, 0, 0, 4, 4))
    assertVectorIntValues(h2oFrame.collectInts(4), Seq(0, 0, 0, 0, 5))
  }

  test("Expand schema with MLLIB dense vectors") {
    val num = 3
    val values = (1 to num).map(x => PrimitiveMllibFixture(Vectors.dense((1 to x).map(1.0 * _).toArray)))
    val df = sc.parallelize(values).toDF

    val (_, _, expandedSchema) = getSchemaInfo(df)

    assert(
      expandedSchema === Vector(
        StructField("f0", DoubleType),
        StructField("f1", DoubleType),
        StructField("f2", DoubleType)))

    // Verify transformation into DataFrame
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(expandedSchema.length == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(1.0, 1.0, 1.0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0, 2.0, 2.0))
    assertVectorDoubleValues(h2oFrame.collectDoubles(2), Seq(0, 0, 3.0))
  }

  test("Expand schema with MLLIB sparse vectors") {
    import spark.implicits._
    val num = 3
    val values = (0 until num).map(x => PrimitiveMllibFixture(Vectors.sparse(num, Seq((x, 1.0)))))
    val df = sc.parallelize(values).toDF()

    val (_, _, expandedSchema) = getSchemaInfo(df)

    assert(
      expandedSchema === Vector(
        StructField("f0", DoubleType),
        StructField("f1", DoubleType),
        StructField("f2", DoubleType)))

    // Verify transformation into DataFrame
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(expandedSchema.length == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    assertDoubleFrameValues(h2oFrame, values.map(_.f.toArray))
  }

  test("Expand schema with MLLIB empty sparse vectors") {
    import spark.implicits._
    val num = 3
    val values = (0 until num).map(_ => PrimitiveMllibFixture(Vectors.sparse(num, Seq())))
    val df = sc.parallelize(values).toDF()

    val (_, _, expandedSchema) = getSchemaInfo(df)

    assert(
      expandedSchema === Vector(
        StructField("f0", DoubleType),
        StructField("f1", DoubleType),
        StructField("f2", DoubleType)))

    // Verify transformation into DataFrame
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(expandedSchema.length == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    assertDoubleFrameValues(h2oFrame, values.map(_.f.toArray))
  }

  test("Expand schema with ML dense vectors") {
    val num = 2
    val values =
      (0 to num).map(x => PrimitiveMlFixture(org.apache.spark.ml.linalg.Vectors.dense((1 to x).map(1.0 * _).toArray)))
    val df = sc.parallelize(values).toDF

    val (_, _, expandedSchema) = getSchemaInfo(df)

    assert(expandedSchema === Vector(StructField("f0", DoubleType), StructField("f1", DoubleType)))

    // Verify transformation into DataFrame
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(expandedSchema.length == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    // Empty dense vector represents the 1st line
    assertVectorDoubleValues(h2oFrame.collectDoubles(0), Seq(0.0, 1.0, 1.0))
    // For vectors missing values are replaced by zeros
    assertVectorDoubleValues(h2oFrame.collectDoubles(1), Seq(0.0, 0.0, 2.0))
  }

  test("Expand schema with ML sparse vectors") {
    import spark.implicits._
    val num = 3
    val values =
      (0 until num).map(x => PrimitiveMlFixture(org.apache.spark.ml.linalg.Vectors.sparse(num, Seq((x, 1.0)))))
    val df = sc.parallelize(values, num).toDF()

    val (_, _, expandedSchema) = getSchemaInfo(df)

    assert(
      expandedSchema === Vector(
        StructField("f0", DoubleType),
        StructField("f1", DoubleType),
        StructField("f2", DoubleType)))

    // Verify transformation into DataFrame
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(expandedSchema.length == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    assertDoubleFrameValues(h2oFrame, values.map(_.f.toArray))
  }

  test("Expand complex schema with sparse and dense vectors") {
    val num = 3
    val rdd = sc.parallelize(0 until num, num).map { x =>
      Row(
        org.apache.spark.ml.linalg.Vectors.sparse(num, Seq((x, 1.0))),
        x,
        org.apache.spark.ml.linalg.Vectors.dense((1 to x).map(1.0 * _).toArray))
    }
    val schema = StructType(
      Seq(
        StructField("f1", VectorType),
        StructField("idx", IntegerType, nullable = false),
        StructField("f2", VectorType)))
    val df = spark.createDataFrame(rdd, schema)

    val (_, _, expandedSchema) = getSchemaInfo(df)

    assert(
      expandedSchema === Array(
        StructField("f10", DoubleType, nullable = true),
        StructField("f11", DoubleType, nullable = true),
        StructField("f12", DoubleType, nullable = true),
        StructField("idx", IntegerType, nullable = false),
        StructField("f20", DoubleType, nullable = true),
        StructField("f21", DoubleType, nullable = true)))

    // Verify transformation into DataFrame
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))

    // Basic invariants
    assert(df.count == h2oFrame.numberOfRows, "Number of rows has to match")
    assert(expandedSchema.length == h2oFrame.numberOfColumns, "Number columns should match")
    assert(h2oFrame.columnNames === expandedSchema.map(_.name))

    // Verify data stored in h2oFrame after transformation
    assertDoubleFrameValues(
      h2oFrame,
      Seq(
        Array(1.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        Array(0.0, 1.0, 0.0, 1.0, 1.0, 0.0),
        Array(0.0, 0.0, 1.0, 2.0, 1.0, 2.0)))
  }

  test("Add metadata to Dataframe numeric column") {
    val data = (1L to 100L).toArray
    val df = spark.sparkContext.parallelize(data).toDF("C0")
    val h2oFrame = hc.asH2OFrame(df)

    val dataFrame = hc.asSparkFrame(h2oFrame)
    assert(dataFrame.schema("C0").metadata.getDouble("min") == 1L)
    assert(dataFrame.schema("C0").metadata.getLong("count") == 100L)

    h2oFrame.delete()
  }

  test("Add metadata to Dataframe categorical column") {
    val df = spark.sparkContext.parallelize(Array("ZERO", "ONE")).toDF("C0")
    val h2oFrame = H2OFrame(hc.asH2OFrameKeyString(df))
    val h2oFrameWithCat = h2oFrame.convertColumnsToCategorical(Array(0))
    assert(h2oFrameWithCat.columns(0).isCategorical())
    val dataFrameEnum = hc.asSparkFrame(h2oFrameWithCat)
    assert(dataFrameEnum.schema("C0").metadata.getLong("cardinality") == 2L)
    h2oFrame.delete()
  }

  test("SW-303 Decimal column conversion failure") {
    val df = sc.parallelize(Array("ok", "bad", "ok", "bad", "bad")).toDF("status")
    df.createOrReplaceTempView("responses")
    val dfDouble = spark.sqlContext.sql("SELECT IF(r.status = 'ok', 0.0, 1.0) AS cancelled FROM responses AS r")
    val frame = H2OFrame(hc.asH2OFrameKeyString(dfDouble))
    assertVectorDoubleValues(frame.collectDoubles(0), Seq(0.0, 1.0, 0.0, 1.0, 1.0))
  }

  test("SW-304 DateType column conversion failure") {
    import java.sql.Date
    val df = sc.parallelize(Seq(DateField(Date.valueOf("2016-12-24")))).toDF("created_date")
    val hf = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(hf.numberOfRows == 1)
    assert(hf.numberOfColumns == 1)
    val expectedValue = DateTimeUtils.fromUTCTime(Date.valueOf("2016-12-24").getTime * 1000, TimeZone.getDefault.getID) / 1000
    assert(hf.collectLongs(0)(0) == expectedValue)
  }

  test("SW-310 Decimal(2,1) not compatible in h2o frame") {
    val dfInput = sc.parallelize(1 to 6).map(v => (v, v * v)).toDF("single", "double")
    dfInput.createOrReplaceTempView("dfInput")
    val df = spark.sqlContext.sql("SELECT *, IF(double < 5, 1.0, 0.0) AS label FROM dfInput")
    val hf = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(hf.numberOfRows == 6)
    assert(hf.numberOfColumns == 3)
    assertVectorIntValues(hf.collectInts("single"), Seq(1, 2, 3, 4, 5, 6))
    assertVectorIntValues(hf.collectInts("double"), Seq(1, 4, 9, 16, 25, 36))
    assertVectorDoubleValues(hf.collectDoubles("label"), Seq(1.0, 1.0, 0.0, 0.0, 0.0, 0.0))
  }

  test("SparkDataFrame with BinaryType to H2O Frame") {
    val df = sc
      .parallelize(1 to 3)
      .map { v =>
        (0 until v).map(_.toByte).toArray[Byte]
      }
      .toDF()
    // just verify that we are really testing the binary type case
    assert(df.schema.fields(0).dataType == BinaryType)
    val hf = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(hf.numberOfRows == 3)
    assert(hf.numberOfColumns == 3) // max size of the array is 3

    assertVectorIntValues(hf.collectInts(0), Seq(0, 0, 0))
    assertVectorIntValues(hf.collectInts(1), Seq(0, 1, 1))
    assertVectorIntValues(hf.collectInts(2), Seq(0, 0, 2))

  }

  test("Convert DataFrame to H2OFrame with dot in column name") {
    import spark.implicits._
    val df = sc.parallelize(1 to 10).toDF("with.dot")
    val hf = H2OFrame(hc.asH2OFrameKeyString(df))
    assert(hf.columnNames.head == "with.dot")
  }

  test("Convert nested DataFrame to H2OFrame with dots in column names") {
    import org.apache.spark.sql.types._
    val nameSchema = StructType(
      Seq[StructField](
        StructField("given.name", StringType, nullable = true),
        StructField("family", StringType, nullable = true)))
    val personSchema = StructType(
      Seq(StructField("name", nameSchema, nullable = true), StructField("person.age", IntegerType, nullable = false)))

    import spark.implicits._
    val df = sc.parallelize(Seq(Person(Name("Charles", "Dickens"), 58), Person(Name("Terry", "Prachett"), 66))).toDF()
    val renamedDF = spark.sqlContext.createDataFrame(df.rdd, personSchema)
    val hf = H2OFrame(hc.asH2OFrameKeyString(renamedDF))

    assert(hf.columnNames.sameElements(Array("name.given.name", "name.family", "person.age")))
  }

  test("Test conversion of DataFrame to H2OFrame and back with a high number of columns") {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val numCols = 20000
    val cols = (1 to numCols).map(n => $"_tmp".getItem(n).as("col" + n))

    val df = sc.parallelize(Seq((1 to numCols).mkString(","))).toDF
    val wideDF = df.withColumn("_tmp", split($"value", ",")).select(cols: _*).drop("_tmp")

    val hf = H2OFrame(hc.asH2OFrameKeyString(wideDF))
    val resultDF = hc.asSparkFrame(hf)

    assert(hf.numberOfColumns == numCols)
    resultDF.foreach(_ => {})
  }

  private def assertH2OFrameInvariants(inputDF: DataFrame, df: H2OFrame): Unit = {
    assert(inputDF.count == df.numberOfRows, "Number of rows has to match")
    assert(df.numberOfColumns == SchemaUtils.flattenSchema(inputDF).length, "Number columns should match")
  }

  private def getSchemaInfo(df: DataFrame): (DataFrame, Array[Int], Seq[StructField]) = {
    val flattenDF = SchemaUtils.flattenDataFrame(df)
    val maxElementSizes = SchemaUtils.collectMaxElementSizes(flattenDF)
    val expandedSchema = SchemaUtils.expandedSchema(SchemaUtils.flattenSchema(df), maxElementSizes)
    (flattenDF, maxElementSizes, expandedSchema)
  }
}
