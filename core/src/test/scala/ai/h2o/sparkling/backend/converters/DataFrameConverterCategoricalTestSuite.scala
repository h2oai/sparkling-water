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

import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import water.parser.Categorical

@RunWith(classOf[JUnitRunner])
class DataFrameConverterCategoricalTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  test("PUBDEV-766 H2OFrame[T_ENUM] to DataFrame[StringType]") {
    val df = spark.sparkContext.parallelize(Array("ONE", "ZERO", "ZERO", "ONE")).toDF("C0")
    val h2oFrame = hc.asH2OFrame(df)
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

  test("DataFrame[String] to H2OFrame[T_STRING] and back") {
    val df = Seq("one", "two", "three", "four", "five", "six", "seven").toDF("Strings").repartition(3)
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isString())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
    h2oFrame.delete()
  }

  test("DataFrame[String] to H2OFrame[T_CAT] and back") {
    val df = Seq("one", "two", "three", "one", "two", "three", "one").toDF("Strings").repartition(3)
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isCategorical())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
    h2oFrame.delete()
  }

  test("DataFrame[String] with more than 10M unique values in one partition to H2OFrame[T_STR] and back") {
    testDataFrameConversionWithHighNumberOfCategoricalLevels(1)
  }

  test("DataFrame[String] with more than 10M unique values in 100 partitions to H2OFrame[T_STR] and back") {
    testDataFrameConversionWithHighNumberOfCategoricalLevels(100)
  }

  def testDataFrameConversionWithHighNumberOfCategoricalLevels(numPartitions: Int) {
    val uniqueValues = 1 to (Categorical.MAX_CATEGORICAL_COUNT * 1.1).toInt
    val values = uniqueValues.map(i => (i % (Categorical.MAX_CATEGORICAL_COUNT + 1)).toHexString)
    val rdd = sc.parallelize(values, numPartitions)

    val df = rdd.toDF("strings")
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isString())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
    h2oFrame.delete()
  }

  private def assertH2OFrameInvariants(inputDF: DataFrame, df: H2OFrame): Unit = {
    assert(inputDF.count == df.numberOfRows, "Number of rows has to match")
    assert(df.numberOfColumns == SchemaUtils.flattenSchema(inputDF).length, "Number columns should match")
  }
}
