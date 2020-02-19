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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class OrdinalPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  import spark.implicits._
  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/insurance.csv"))

  private def createAlgorithm(): H2OGLM =  {
    new H2OGLM()
      .setSplitRatio(0.8)
      .setFeaturesCols("District", "Group", "Claims")
      .setLabelCol("Age")
      .setSeed(1)
      .setFamily("ordinal")
  }

  private def assertExistenceOfColumns(df: DataFrame, path: String, expectedColumns: Seq[String]) = {
    assert(df.select(path).columns.sameElements(expectedColumns))
  }

  test("Correct content with details disabled") {
    val algorithm = createAlgorithm()
    val model = algorithm.fit(dataset)

    val predictions = model.transform(dataset)

    assert(model.getModelDetails().contains(""""model_category": "Ordinal""""))
    assertExistenceOfColumns(predictions, "*", dataset.columns ++ Seq("prediction"))
  }

  test("Correct content with details enabled") {
    val algorithm = createAlgorithm().setWithDetailedPredictionCol(true)
    val model = algorithm.fit(dataset)

    val predictions = model.transform(dataset)

    assert(model.getModelDetails().contains(""""model_category": "Ordinal""""))
    assertExistenceOfColumns(predictions, "*", dataset.columns ++ Seq("detailed_prediction", "prediction"))
    assertExistenceOfColumns(predictions, "detailed_prediction.*", Seq("label", "probabilities"))
    val probabilities = predictions.select("detailed_prediction.probabilities").head().getMap[String, Double](0)
    assert(probabilities.keys.toList.sorted == Seq("25-29", "30-35", "<25", ">35").sorted)
  }

  test("transformSchema without details returns expected result") {
    val algorithm = createAlgorithm()
    val model = algorithm.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", StringType, nullable = true)

    val expectedSchema = StructType(datasetFields ++ (predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(model.getModelDetails().contains(""""model_category": "Ordinal""""))
    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  test("transformSchema with details returns expected result") {
    val algorithm = createAlgorithm().setWithDetailedPredictionCol(true)
    val model = algorithm.fit(dataset)

    val datasetFields = dataset.schema.fields
    val labelField = StructField("label", StringType, nullable = true)
    val predictionColField = StructField("prediction", StringType, nullable = true)
    val probabilitiesField = StructField("probabilities", MapType(StringType, DoubleType, valueContainsNull = false), nullable = true)
    val detailedPredictionColField = StructField("detailed_prediction", StructType(Seq(labelField, probabilitiesField)), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(model.getModelDetails().contains(""""model_category": "Ordinal""""))
    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }
}
