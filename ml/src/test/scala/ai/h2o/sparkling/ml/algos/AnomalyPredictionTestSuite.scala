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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class AnomalyPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset: DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
  }

  test("predictionCol content") {
    val algo = new H2OIsolationForest()

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)

    val result = transformed.select("prediction").take(2).map(_.getDouble(0))
    assert(result(0) != result(1))
  }

  test("detailedPredictionCol content") {
    val algo = new H2OIsolationForest()
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset).limit(2).cache()

    val expectedCols = Seq("score", "normalizedScore", "leafNodeAssignments", "stageResults")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))

    val scores = predictions.select("detailed_prediction.score").take(2).map(_.getDouble(0))
    assert(scores(0) != scores(1))

    val normalizedScore = predictions.select("detailed_prediction.normalizedScore").take(2).map(_.getDouble(0))
    assert(normalizedScore(0) != normalizedScore(1))

    val leafNodeAssignments = predictions.select("detailed_prediction.leafNodeAssignments").head().getSeq[String](0)
    assert(leafNodeAssignments != null)
    assert(leafNodeAssignments.length == algo.getNtrees())

    val stageResults = predictions.select("detailed_prediction.stageResults").head().getSeq[Double](0)
    assert(stageResults != null)
    assert(stageResults.size == algo.getNtrees())
  }

  test("transformSchema with leafNodeAssignments and stageResults") {
    val algo = new H2OIsolationForest()
      .setWithStageResults(true)
      .setWithLeafNodeAssignments(true)

    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", DoubleType, nullable = true)

    val scoreField = StructField("score", DoubleType, nullable = false)
    val normalizedScoreField = StructField("normalizedScore", DoubleType, nullable = false)
    val leafNodeAssignmentField =
      StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
    val stageResultsField =
      StructField("stageResults", ArrayType(DoubleType, containsNull = false), nullable = false)
    val detailedPredictionColField = StructField(
      "detailed_prediction",
      StructType(scoreField :: normalizedScoreField :: leafNodeAssignmentField :: stageResultsField :: Nil),
      nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  test("transformSchema without leafNodeAssignments and stageResults") {
    val algo = new H2OIsolationForest()

    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", DoubleType, nullable = true)

    val scoreField = StructField("score", DoubleType, nullable = false)
    val normalizedScoreField = StructField("normalizedScore", DoubleType, nullable = false)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(scoreField :: normalizedScoreField :: Nil), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }
}
