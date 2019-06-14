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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.{H2OSchemaUtils, SparkTestContext, TestFrameUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

/**
 * Set of test for various DataFrame's schema-related methods.
 */
@RunWith(classOf[JUnitRunner])
class H2OSchemaUtilsTestSuite extends FlatSpec with Matchers with SparkTestContext {

  sc = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  "flattenStructsInSchema" should "flatten a simple schema" in {
    val expSchema = StructType(
        StructField("a", IntegerType, true) ::
        StructField("b", IntegerType, false)
        :: Nil
    )
    val flatSchema = H2OSchemaUtils.flattenStructsInSchema(expSchema)
    assert (flatSchema.fields === Seq(StructField("a", IntegerType, true), StructField("b", IntegerType, false)))
  }

  "flattenStructsInSchema" should "flatten a composed schema" in {
    val expSchema = StructType(
        StructField("a", StructType(
          StructField("a1", DoubleType, false)::
          StructField("a2", StringType, true):: Nil
        ), true) ::
        StructField("b", StructType(
          StructField("b1", DoubleType, false)::
          StructField("b2", StringType, true):: Nil
        ), false)
        :: Nil
    )
    val flatSchema = H2OSchemaUtils.flattenStructsInSchema(expSchema)
    assert (flatSchema.fields === Seq(StructField("a.a1", DoubleType, true),
                                StructField("a.a2", StringType, true),
                                StructField("b.b1", DoubleType, false),
                                StructField("b.b2", StringType, true)))
  }

  "flattenStructsInSchema" should "be compatible with collectStringIndices" in {
    val expSchema = StructType(
      StructField("a", StructType(
        StructField("a1", DoubleType, false)::
        StructField("a2", StringType, true):: Nil
      ), true) ::
        StructField("b", StructType(
          StructField("b1", StringType, false)::
          StructField("b2", StringType, true):: Nil
        ), false) ::
        StructField("c", StringType, false)
        :: Nil
    )

    val flattenSchema = H2OSchemaUtils.flattenStructsInSchema(expSchema)
    val stringIndices = H2OSchemaUtils.collectStringIndices(flattenSchema)
    val arrayIndices = H2OSchemaUtils.collectArrayLikeTypes(flattenSchema)

    assert (stringIndices === List (1, 2, 3, 4))
    assert (arrayIndices === Nil)
  }

  "flattenDataFrame" should "flatten an array of structs" in {
    import spark.implicits._

    val input = Seq(
      Seq((1, 2), (3, 4)),
      Seq((1, 2), (3, 4), (5, 6))
    ).toDF("arr")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer)](
      (1 ,2, 3, 4, null, null),
      (1, 2, 3, 4, 5, 6)
    ).toDF("arr_0__1", "arr_0__2", "arr_1__1", "arr_1__2", "arr_2__1", "arr_2__2")

    val result = H2OSchemaUtils.flattenDataFrame(input)

    TestFrameUtils.assertFieldNamesAreEqual(expected, result)
    TestFrameUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten a struct of arrays" in {
    import spark.implicits._

    val input = Seq[(Seq[Integer], Seq[Integer], Seq[Integer])](
      (Seq[Integer](1, null), Seq[Integer](3, 4), Seq[Integer](5)),
      (Seq[Integer](1, 2), null, Seq[Integer](5, 6))
    ).toDF("arr1", "arr2", "arr3").select(struct('arr1, 'arr2, 'arr3) as "str")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer)](
      (1, null, 3, 4, 5, null),
      (1, 2, null, null, 5, 6)
    ).toDF("str_arr1_0", "str_arr1_1", "str_arr2_0", "str_arr2_1", "str_arr3_0", "str_arr3_1")

    val result = H2OSchemaUtils.flattenDataFrame(input)

    TestFrameUtils.assertFieldNamesAreEqual(expected, result)
    TestFrameUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten an array of arrays" in {
    import spark.implicits._

    val input = Seq(
      Seq(Seq(1), null, Seq(3, 4, 5)),
      Seq(Seq(1, 2), Seq(3, 4), Seq(5, 6))
    ).toDF("arr")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer, Integer)](
      (1 , null, null, null, 3, 4, 5),
      (1, 2, 3, 4, 5, 6, null)
    ).toDF("arr_0_0", "arr_0_1", "arr_1_0", "arr_1_1", "arr_2_0", "arr_2_1", "arr_2_2")

    val result = H2OSchemaUtils.flattenDataFrame(input)

    TestFrameUtils.assertFieldNamesAreEqual(expected, result)
    TestFrameUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten a struct of structs" in {
    import spark.implicits._

    val input = Seq[((Integer, Integer),(Integer, Integer))](
      ((1, null), (3, 4)),
      ((1, 2), (null, 4))
    ).toDF("str1", "str2").select(struct('str1, 'str2) as "str")
    val expected = Seq[(Integer, Integer, Integer, Integer)](
      (1, null, 3, 4),
      (1, 2, null, 4)
    ).toDF("str_str1__1", "str_str1__2", "str_str2__1", "str_str2__2")

    val result = H2OSchemaUtils.flattenDataFrame(input)

    TestFrameUtils.assertFieldNamesAreEqual(expected, result)
    TestFrameUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten an array of maps" in {
    import spark.implicits._

    val input = Seq(
      Seq(Map("a" -> 1, "b" -> 2)),
      Seq(Map("b" -> 2, "c" -> 3), Map("a" -> 4))
    ).toDF("arr")
    val expected = Seq[(Integer, Integer, Integer, Integer)](
      (1 ,2, null, null),
      (null, 2, 3, 4)
    ).toDF("arr_0_a", "arr_0_b", "arr_0_c", "arr_1_a")

    val result = H2OSchemaUtils.flattenDataFrame(input)

    TestFrameUtils.assertFieldNamesAreEqual(expected, result)
    TestFrameUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenDataFrame" should "flatten a map of arrays" in {
    import spark.implicits._

    val input = Seq(
      Map("a" -> Seq[Integer](1, 2), "b" -> Seq[Integer](3)),
      Map("b" -> Seq[Integer](null, 4), "c" -> Seq[Integer](5, 6))
    ).toDF("map")
    val expected = Seq[(Integer, Integer, Integer, Integer, Integer, Integer)](
      (1, 2, 3, null, null, null),
      (null, null, null, 4, 5, 6)
    ).toDF("map_a_0", "map_a_1", "map_b_0", "map_b_1", "map_c_0", "map_c_1")

    val result = H2OSchemaUtils.flattenDataFrame(input)

    TestFrameUtils.assertFieldNamesAreEqual(expected, result)
    TestFrameUtils.assertDataFramesAreIdentical(expected, result)
  }

  "flattenSchema" should "flatten a schema with an array of structs" in {
    val rdd = sc.parallelize{
      Seq(
        Row(Seq(Row(1, null), Row(3, 4))),
        Row(Seq(Row(1, 2), Row(3, 4), Row(5, 6)))
      )
    }
    val structType = StructType(
      StructField("a", IntegerType, false) ::
      StructField("b", IntegerType, true) ::
      Nil)
    val schema = StructType(
      StructField("arr", ArrayType(structType, false), false) ::
      Nil)
    val df = spark.createDataFrame(rdd, schema)

    val expectedSchema = StructType(
      StructField("arr_0_a", IntegerType, false) ::
      StructField("arr_0_b", IntegerType, true) ::
      StructField("arr_1_a", IntegerType, false) ::
      StructField("arr_1_b", IntegerType, true) ::
      StructField("arr_2_a", IntegerType, true) ::
      StructField("arr_2_b", IntegerType, true) ::
      Nil)

    val result = H2OSchemaUtils.flattenSchema(df)

    result shouldEqual expectedSchema
  }

  "flattenSchema" should "flatten a schema with a struct of arrays" in {
    val rdd = sc.parallelize{
      Seq(
        Row(Row(Seq(1, null), Seq(3, 4), Seq(5), Seq.empty[Integer])),
        Row(Row(Seq(1, 2), null, Seq(5, 6), Seq(7, 8)))
      )
    }
    val structType = StructType(
      StructField("a", ArrayType(IntegerType, true), false) ::
      StructField("b", ArrayType(IntegerType, false), true) ::
      StructField("c", ArrayType(IntegerType, false), false) ::
      StructField("d", ArrayType(IntegerType, false), false) ::
      Nil)
    val schema = StructType(StructField("str", structType, false) :: Nil)
    val df = spark.createDataFrame(rdd, schema)

    val expectedSchema = StructType(
      StructField("str_a_0", IntegerType, true) ::
      StructField("str_a_1", IntegerType, true) ::
      StructField("str_b_0", IntegerType, true) ::
      StructField("str_b_1", IntegerType, true) ::
      StructField("str_c_0", IntegerType, false) ::
      StructField("str_c_1", IntegerType, true) ::
      StructField("str_d_0", IntegerType, true) ::
      StructField("str_d_1", IntegerType, true) ::
      Nil)

    val result = H2OSchemaUtils.flattenSchema(df)

    result shouldEqual expectedSchema
  }
}