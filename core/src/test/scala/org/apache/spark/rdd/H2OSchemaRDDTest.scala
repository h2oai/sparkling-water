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

import hex.splitframe.ShuffleSplitFrame
import org.apache.spark.SparkContext
import org.apache.spark.h2o.{IntHolder, H2OContext}
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.Key
import water.fvec.DataFrame
import org.apache.spark.sql._
import water.parser.{Categorical, Parser}


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

  test("SchemaRDD[Int] to DataFrame with empty partitions (error detected in calling ShuffleSplitFrame)") {
    import sqlContext._

    val values = 1 to 100
    val srdd:SchemaRDD = sc.parallelize(values, 2000).map(v => IntField(v))

    val dataFrame = hc.toDataFrame(srdd)

    ShuffleSplitFrame.shuffleSplitFrame(dataFrame,
        Array[String]("train.hex", "test.hex", "hold.hex").map(Key.make(_)),
        Array[Double](0.5, 0.3, 0.2), 1234567689L)
  }

  def fp(it:Iterator[Row]):Unit = {
    println(it.size)
  }

  def assertDataFrameInvariants(inputRDD: SchemaRDD, df: DataFrame): Unit = {
    assert( inputRDD.count == df.numRows(), "Number of rows has to match")
    assert( df.numCols() == 1 , "Only single column")
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
