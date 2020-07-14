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

package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.functions.bround
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class DimReductionPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))
    .drop("class")

  def getPreconfiguredAlgorithm() = {
    new H2OGLRM()
      .setK(3)
      .setLoss("Quadratic")
      .setGammaX(0.5)
      .setGammaY(0.5)
      .setSeed(42)
      .setTransform("standardize")
  }

  test("predictionCol content") {
    val algo = getPreconfiguredAlgorithm()

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)

    // the 'dimensions' from the dimension reduction prediction are directly in predictionCol
    assert(transformed.select("prediction").head().getList[Double](0).size() == 3)
    assert(!transformed.columns.contains("detailed_prediction"))
  }

  test("detailedPredictionCol content") {
    import spark.implicits._
    val algo = getPreconfiguredAlgorithm().setWithDetailedPredictionCol(true)

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset).cache()
    transformed.show(false)

    val expectedCols = Seq("dimensions")
    assert(transformed.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))

    def roundResult(dataFrame: DataFrame, precision: Int): DataFrame = {
      dataFrame.select(
        bround($"prediction".getItem(0), precision) as "prediction0",
        bround($"prediction".getItem(1), precision) as "prediction1",
        bround($"prediction".getItem(2), precision) as "prediction2")
    }

    val expected = roundResult(transformed.select("prediction"), 3)
    val result = roundResult(transformed.select($"detailed_prediction.dimensions" as "prediction"), 3)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  test("transformSchema with detailed prediction col") {
    val algo = getPreconfiguredAlgorithm().setWithDetailedPredictionCol(true)

    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", ArrayType(DoubleType, containsNull = false), nullable = true)

    val dimensionsField = StructField("dimensions", ArrayType(DoubleType, containsNull = false), nullable = true)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(dimensionsField :: Nil), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  test("transformSchema without detailed prediction col") {
    val algo = getPreconfiguredAlgorithm()
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", ArrayType(DoubleType, containsNull = false), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }
}
