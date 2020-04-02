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
package ai.h2o.sparkling.ml.utils

import ai.h2o.sparkling.{SparkTestContext, TestUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class SchemaUtilsTestSuite extends FlatSpec with Matchers with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  "flattenStructsInSchema" should "flatten a simple schema" in {
    val expSchema = StructType(
      StructField("a", IntegerType, nullable = true) ::
        StructField("b", IntegerType, nullable = false)
        :: Nil)
    val flatSchema = SchemaUtils.flattenStructsInSchema(expSchema)
    val expected =
      Seq((StructField("a", IntegerType, nullable = true), "a"), (StructField("b", IntegerType, nullable = false), "b"))
    assert(flatSchema === expected)
  }

  "flattenStructsInSchema" should "flatten a composed schema" in {
    val expSchema = StructType(
      StructField(
        "a",
        StructType(StructField("a1", DoubleType, nullable = false) ::
          StructField("a2", StringType, nullable = true) :: Nil),
        nullable = true) ::
        StructField(
          "b",
          StructType(StructField("b1", DoubleType, nullable = false) ::
            StructField("b2", StringType, nullable = true) :: Nil),
          nullable = false)
        :: Nil)
    val flatSchema = SchemaUtils.flattenStructsInSchema(expSchema)
    val expected = Seq(
      (StructField("a.a1", DoubleType, nullable = true), "a.a1"),
      (StructField("a.a2", StringType, nullable = true), "a.a2"),
      (StructField("b.b1", DoubleType, nullable = false), "b.b1"),
      (StructField("b.b2", StringType, nullable = true), "b.b2"))
    assert(flatSchema === expected)
  }

  "flattenDataFrame" should "flatten an array of structs" in {

    val input = Seq(Seq((1, 2), (3, 4)), Seq((1, 2), (3, 4), (5, 6))).toDF("arr")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer)](
      (1, 2, 3, 4, null, null),
      (1, 2, 3, 4, 5, 6)).toDF("arr.0._1", "arr.0._2", "arr.1._1", "arr.1._2", "arr.2._1", "arr.2._2")

    val result = SchemaUtils.flattenDataFrame(input)

    TestUtils.assertFieldNamesAreEqual(expected, result)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten a struct of arrays" in {
    val input = Seq[(Seq[Integer], Seq[Integer], Seq[Integer])](
      (Seq[Integer](1, null), Seq[Integer](3, 4), Seq[Integer](5)),
      (Seq[Integer](1, 2), null, Seq[Integer](5, 6)))
      .toDF("arr1", "arr2", "arr3")
      .select(struct('arr1, 'arr2, 'arr3) as "struct")
    val expected =
      Seq[(Integer, Integer, Integer, Integer, Integer, Integer)]((1, null, 3, 4, 5, null), (1, 2, null, null, 5, 6))
        .toDF("struct.arr1.0", "struct.arr1.1", "struct.arr2.0", "struct.arr2.1", "struct.arr3.0", "struct.arr3.1")

    val result = SchemaUtils.flattenDataFrame(input)

    TestUtils.assertFieldNamesAreEqual(expected, result)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten an array of arrays" in {

    val input = Seq((Seq(Seq(1), null, Seq(3, 4, 5)), "extra"), (Seq(Seq(1, 2), Seq(3, 4), Seq(5, 6)), "extra"))
      .toDF("arr", "extra")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer, Integer, String)](
      (1, null, null, null, 3, 4, 5, "extra"),
      (1, 2, 3, 4, 5, 6, null, "extra")).toDF(
      "arr.0.0",
      "arr.0.1",
      "arr.1.0",
      "arr.1.1",
      "arr.2.0",
      "arr.2.1",
      "arr.2.2",
      "extra")

    val result = SchemaUtils.flattenDataFrame(input)

    TestUtils.assertFieldNamesAreEqual(expected, result)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten a struct of structs" in {

    val input = Seq[((Integer, Integer), (Integer, Integer))](((1, null), (3, 4)), ((1, 2), (null, 4)))
      .toDF("struct1", "struct2")
      .select(struct('struct1, 'struct2) as "struct")
    val expected = Seq[(Integer, Integer, Integer, Integer)]((1, null, 3, 4), (1, 2, null, 4))
      .toDF("struct.struct1._1", "struct.struct1._2", "struct.struct2._1", "struct.struct2._2")

    val result = SchemaUtils.flattenDataFrame(input)

    TestUtils.assertFieldNamesAreEqual(expected, result)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten an array of maps" in {

    val input = Seq((Seq(Map("a" -> 1, "b" -> 2)), "extra"), (Seq(Map("b" -> 2, "c" -> 3), Map("a" -> 4)), "extra"))
      .toDF("arr", "extra")
    val expected = Seq[(Integer, Integer, Integer, Integer, String)](
      (1, 2, null, null, "extra"),
      (null, 2, 3, 4, "extra")).toDF("arr.0.a", "arr.0.b", "arr.0.c", "arr.1.a", "extra")

    val result = SchemaUtils.flattenDataFrame(input)

    TestUtils.assertFieldNamesAreEqual(expected, result)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten a map of arrays" in {

    val input = Seq(
      (Map("a" -> Seq[Integer](1, 2), "b" -> Seq[Integer](3)), "extra"),
      (Map("b" -> Seq[Integer](null, 4), "c" -> Seq[Integer](5, 6)), "extra")).toDF("map", "extra")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer, String)](
      (1, 2, 3, null, null, null, "extra"),
      (null, null, null, 4, 5, 6, "extra")).toDF(
      "map.a.0",
      "map.a.1",
      "map.b.0",
      "map.b.1",
      "map.c.0",
      "map.c.1",
      "extra")

    val result = SchemaUtils.flattenDataFrame(input)

    TestUtils.assertFieldNamesAreEqual(expected, result)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenSchema" should "flatten a schema with an array of structs" in {
    val rdd = sc.parallelize {
      Seq(Row(Seq(Row(1, null), Row(3, 4))), Row(Seq(Row(1, 2), Row(3, 4), Row(5, 6))))
    }
    val structType = StructType(
      StructField("a", IntegerType, nullable = false) ::
        StructField("b", IntegerType, nullable = true) ::
        Nil)
    val schema = StructType(
      StructField("arr", ArrayType(structType, containsNull = false), nullable = false) ::
        Nil)
    val df = spark.createDataFrame(rdd, schema)

    val expectedSchema = StructType(
      StructField("arr.0.a", IntegerType, nullable = false) ::
        StructField("arr.0.b", IntegerType, nullable = true) ::
        StructField("arr.1.a", IntegerType, nullable = false) ::
        StructField("arr.1.b", IntegerType, nullable = true) ::
        StructField("arr.2.a", IntegerType, nullable = true) ::
        StructField("arr.2.b", IntegerType, nullable = true) ::
        Nil)

    val result = SchemaUtils.flattenSchema(df)

    result shouldEqual expectedSchema
  }

  "flattenSchema" should "flatten a schema with a struct of arrays" in {
    val rdd = sc.parallelize {
      Seq(
        Row(Row(Seq(1, null), Seq(3, 4), Seq(5), Seq.empty[Integer])),
        Row(Row(Seq(1, 2), null, Seq(5, 6), Seq(7, 8))))
    }
    val structType = StructType(
      StructField("a", ArrayType(IntegerType, containsNull = true), nullable = false) ::
        StructField("b", ArrayType(IntegerType, containsNull = false), nullable = true) ::
        StructField("c", ArrayType(IntegerType, containsNull = false), nullable = false) ::
        StructField("d", ArrayType(IntegerType, containsNull = false), nullable = false) ::
        Nil)
    val schema = StructType(StructField("struct", structType, nullable = false) :: Nil)
    val df = spark.createDataFrame(rdd, schema)

    val expectedSchema = StructType(
      StructField("struct.a.0", IntegerType, nullable = true) ::
        StructField("struct.a.1", IntegerType, nullable = true) ::
        StructField("struct.b.0", IntegerType, nullable = true) ::
        StructField("struct.b.1", IntegerType, nullable = true) ::
        StructField("struct.c.0", IntegerType, nullable = false) ::
        StructField("struct.c.1", IntegerType, nullable = true) ::
        StructField("struct.d.0", IntegerType, nullable = true) ::
        StructField("struct.d.1", IntegerType, nullable = true) ::
        Nil)

    val result = SchemaUtils.flattenSchema(df)

    result shouldEqual expectedSchema
  }

  "flattenSchema" should "flatten a schema with a map of structs" in {
    val rdd = sc.parallelize {
      Seq(
        Row(Map("a" -> Row(1, null), "b" -> Row(3, 4), "d" -> Row(7, 8))),
        Row(Map("a" -> Row(1, 2), "b" -> Row(3, 4), "c" -> Row(5, 6))))
    }
    val structType = StructType(
      StructField("a", IntegerType, nullable = false) ::
        StructField("b", IntegerType, nullable = true) ::
        Nil)
    val schema = StructType(
      StructField("map", MapType(StringType, structType, valueContainsNull = false), nullable = false) ::
        Nil)
    val df = spark.createDataFrame(rdd, schema)

    val expectedSchema = StructType(
      StructField("map.a.a", IntegerType, nullable = false) ::
        StructField("map.a.b", IntegerType, nullable = true) ::
        StructField("map.b.a", IntegerType, nullable = false) ::
        StructField("map.b.b", IntegerType, nullable = true) ::
        StructField("map.c.a", IntegerType, nullable = true) ::
        StructField("map.c.b", IntegerType, nullable = true) ::
        StructField("map.d.a", IntegerType, nullable = true) ::
        StructField("map.d.b", IntegerType, nullable = true) ::
        Nil)

    val result = SchemaUtils.flattenSchema(df)

    result shouldEqual expectedSchema
  }

  "flattenSchema" should "flatten a schema with a struct of maps" in {
    val rdd = sc.parallelize {
      Seq(
        Row(
          Row(
            Map("a" -> 1, "b" -> null, "c" -> 3),
            Map("d" -> 4, "e" -> 5),
            Map("f" -> 6),
            Map.empty[String, Integer])),
        Row(Row(Map("b" -> 1, "c" -> 2), null, Map("f" -> 6, "g" -> 7), Map("h" -> 8, "i" -> 9))))
    }
    val structType = StructType(
      StructField("a", MapType(StringType, IntegerType, valueContainsNull = true), nullable = false) ::
        StructField("b", MapType(StringType, IntegerType, valueContainsNull = false), nullable = true) ::
        StructField("c", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false) ::
        StructField("d", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false) ::
        Nil)
    val schema = StructType(StructField("struct", structType, nullable = false) :: Nil)
    val df = spark.createDataFrame(rdd, schema)

    val expectedSchema = StructType(
      StructField("struct.a.a", IntegerType, nullable = true) ::
        StructField("struct.a.b", IntegerType, nullable = true) ::
        StructField("struct.a.c", IntegerType, nullable = true) ::
        StructField("struct.b.d", IntegerType, nullable = true) ::
        StructField("struct.b.e", IntegerType, nullable = true) ::
        StructField("struct.c.f", IntegerType, nullable = false) ::
        StructField("struct.c.g", IntegerType, nullable = true) ::
        StructField("struct.d.h", IntegerType, nullable = true) ::
        StructField("struct.d.i", IntegerType, nullable = true) ::
        Nil)

    val result = SchemaUtils.flattenSchema(df)

    result shouldEqual expectedSchema
  }
}
