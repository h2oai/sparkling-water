package org.apache.spark.rdd

import org.apache.spark.h2o.H2OSchemaUtils
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.sql.api.java.DataType
import org.apache.spark.sql.catalyst.types.{IntegerType, DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Set of test for SchemaRDD utilities.
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
