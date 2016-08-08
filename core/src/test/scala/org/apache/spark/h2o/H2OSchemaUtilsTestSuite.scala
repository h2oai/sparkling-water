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

import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Set of test for various DataFrame's schema-related methods.
 */
@RunWith(classOf[JUnitRunner])
class H2OSchemaUtilsTestSuite extends FunSuite {

  test("Test flatSchema on simple schema") {
    val expSchema = StructType(
        StructField("a", IntegerType, true) ::
        StructField("b", IntegerType, false)
        :: Nil
    )
    val flatSchema = H2OSchemaUtils.flatSchema(expSchema)
    assert (flatSchema === Seq(StructField("a", IntegerType, true), StructField("b", IntegerType, false)))
  }

  test("Test flatSchema on composed schema") {
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
    val flatSchema = H2OSchemaUtils.flatSchema(expSchema)
    assert (flatSchema === Seq(StructField("a.a1", DoubleType, true),
                                StructField("a.a2", StringType, true),
                                StructField("b.b1", DoubleType, false),
                                StructField("b.b2", StringType, true)))
  }

  test("Test collect string types") {
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

    val stringIndices = H2OSchemaUtils.collectStringTypesIndx(expSchema.fields)
    val arrayIndices = H2OSchemaUtils.collectArrayLikeTypes(expSchema.fields)

    assert (stringIndices === List ( List(0,1), List(1,0), List(1,1), List(2)))
    assert (arrayIndices === Nil)
  }

}
