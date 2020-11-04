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

import ai.h2o.sparkling.extensions.internals.CategoricalConstants
import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFrameConverterLimitedCategoricalTestSuite extends FunSuite with SharedH2OTestContext {

  val maximumCategoricalLevels = 100000

  val sparkConf = defaultSparkConf

  if (sys.props.getOrElse("spark.ext.h2o.backend.cluster.mode", "internal") == "internal") {
    sys.props
      .put(CategoricalConstants.TESTING_MAXIMUM_CATEGORICAL_LEVELS_PROPERTY_NAME, maximumCategoricalLevels.toString)
  } else {
    sparkConf.set(
      "spark.ext.h2o.extra.properties",
      s"-JJ -D${CategoricalConstants.TESTING_MAXIMUM_CATEGORICAL_LEVELS_PROPERTY_NAME}=$maximumCategoricalLevels")
  }

  override def createSparkSession(): SparkSession = sparkSession("local[*]", sparkConf)
  import spark.implicits._

  test("DataFrame[String, Categorical, ...] in one partition to H2OFrame[T_STR] and back") {
    testDataFrameConversionMixOfCategoricalAndStringColumns(1)
  }

  test("DataFrame[String, Categorical, ...] in 100 partitions to H2OFrame[T_STR] and back") {
    testDataFrameConversionMixOfCategoricalAndStringColumns(100)
  }

  def testDataFrameConversionMixOfCategoricalAndStringColumns(numPartitions: Int) {

    val uniqueValues = 1 to (maximumCategoricalLevels * 2)
    val values = uniqueValues.map { value =>
      (
        (value % 10).toString,
        (value % 200).toString,
        (value % (maximumCategoricalLevels + 10)).toHexString,
        (value % (maximumCategoricalLevels + 100)).toHexString,
        (value % 100).toString,
        (value % (maximumCategoricalLevels + 1000)).toHexString,
        (value % 5).toString)
    }
    val rdd = sc.parallelize(values, numPartitions)

    val df = rdd.toDF("cat10", "cat200", "strings1", "strings2", "cat100", "strings3", "cat5")
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isCategorical())
    assert(h2oFrame.columns(1).isCategorical())
    assert(h2oFrame.columns(2).isString())
    assert(h2oFrame.columns(3).isString())
    assert(h2oFrame.columns(4).isCategorical())
    assert(h2oFrame.columns(5).isString())
    assert(h2oFrame.columns(6).isCategorical())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
    h2oFrame.delete()
  }

  test("DataFrame[String, Numeric, ...] in one partition to H2OFrame[T_STR] and back") {
    testDataFrameConversionMixOfNumericAndStringColumns(1)
  }

  test("DataFrame[String, Numeric, ...] in 100 partitions to H2OFrame[T_STR] and back") {
    testDataFrameConversionMixOfNumericAndStringColumns(100)
  }

  def testDataFrameConversionMixOfNumericAndStringColumns(numPartitions: Int) {

    val uniqueValues = 1 to (maximumCategoricalLevels * 2)
    val values = uniqueValues.map { value =>
      (
        (value % (maximumCategoricalLevels + 200)).toHexString,
        value % 100000,
        value % 200,
        (value % (maximumCategoricalLevels + 10)).toHexString,
        (value % (maximumCategoricalLevels + 100)).toHexString,
        value,
        (value % (maximumCategoricalLevels + 1000)).toHexString,
        (value % 5) + ((value % 5) / 10),
        (value % (maximumCategoricalLevels + 1000)).toHexString)
    }
    val rdd = sc.parallelize(values, numPartitions)

    val df = rdd.toDF("str1", "num100k", "num200", "str2", "str3", "num", "str4", "num5", "str5")
    val h2oFrame = hc.asH2OFrame(df)

    assertH2OFrameInvariants(df, h2oFrame)
    assert(h2oFrame.columns(0).isString())
    assert(h2oFrame.columns(1).isNumeric())
    assert(h2oFrame.columns(2).isNumeric())
    assert(h2oFrame.columns(3).isString())
    assert(h2oFrame.columns(4).isString())
    assert(h2oFrame.columns(5).isNumeric())
    assert(h2oFrame.columns(6).isString())
    assert(h2oFrame.columns(7).isNumeric())
    assert(h2oFrame.columns(8).isString())

    val resultDF = hc.asSparkFrame(h2oFrame)
    TestUtils.assertDataFramesAreIdentical(df, resultDF)
    h2oFrame.delete()
  }

  private def assertH2OFrameInvariants(inputDF: DataFrame, df: H2OFrame): Unit = {
    assert(inputDF.count == df.numberOfRows, "Number of rows has to match")
    assert(df.numberOfColumns == SchemaUtils.flattenSchema(inputDF).length, "Number columns should match")
  }
}
