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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class BinomialPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))
    // iris dataset has 3 classes, filter out one class
    // to do binomial classification
    .filter('class =!= "Iris-virginica")

  test("predictionCol content") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val expectedCols = Seq("prediction")
    val predictions = model.transform(dataset)
    assert(predictions.select("prediction").schema.fields.map(_.name).sameElements(expectedCols))
  }

  test("detailedPredictionCol content with contributions, assignments and stage probabilities") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("sepal_len", "sepal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset).limit(2).cache()

    val expectedCols = Seq("label", "probabilities", "contributions", "leafNodeAssignments", "stageProbabilities")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    val probabilities = predictions.select("detailed_prediction.probabilities.*")
    val setosaProbabilities = probabilities.select("Iris-setosa").collect().map(_.getDouble(0))
    assert(setosaProbabilities(0) != setosaProbabilities(1))
    val versicolorProbabilities = probabilities.select("Iris-versicolor").collect().map(_.getDouble(0))
    assert(versicolorProbabilities(0) != versicolorProbabilities(1))

    val contributions = predictions.select("detailed_prediction.contributions").head().getStruct(0)
    assert(contributions != null)
    assert(contributions.size == 3)

    val leafNodeAssignments = predictions.select("detailed_prediction.leafNodeAssignments").head().getSeq[String](0)
    assert(leafNodeAssignments != null)
    assert(leafNodeAssignments.length == algo.getNtrees())

    val stageProbabilities = predictions.select("detailed_prediction.stageProbabilities").head().getStruct(0)
    assert(stageProbabilities != null)
    assert(stageProbabilities.size == 2)
    assert(stageProbabilities.getList(0).size() == algo.getNtrees())
    assert(stageProbabilities.getList(1).size() == algo.getNtrees())
  }

  test("detailedPredictionCol content without contributions") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(false)
      .setFeaturesCols("sepal_len", "sepal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)

    val expectedCols = Seq("label", "probabilities")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    val probabilities = predictions.select("detailed_prediction.probabilities.*").limit(2).cache()
    val setosaProbabilities = probabilities.select("Iris-setosa").collect().map(_.getDouble(0))
    assert(setosaProbabilities(0) != setosaProbabilities(1))
    val versicolorProbabilities = probabilities.select("Iris-versicolor").collect().map(_.getDouble(0))
    assert(versicolorProbabilities(0) != versicolorProbabilities(1))
  }

  test("transformSchema with contributions, assignments and stage probabilities") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("sepal_len", "sepal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val labelField = StructField("label", StringType, nullable = true)
    val clazzFields = Seq("Iris-setosa", "Iris-versicolor").map(clazz => StructField(clazz, DoubleType, false))
    val probabilitiesField = StructField("probabilities", StructType(clazzFields), nullable = false)
    val predictionColField = StructField("prediction", StringType, nullable = true)
    val individualContributions = Seq("sepal_len", "sepal_wid", "BiasTerm")
      .map(StructField(_, FloatType, nullable = false))
    val contributionsType = StructType(individualContributions)
    val contributionsField = StructField("contributions", contributionsType, nullable = false)
    val leafNodeAssignmentField =
      StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
    val stageProbabilitiesType = StructType(
      Seq(
        StructField("Iris-setosa", ArrayType(DoubleType, containsNull = false), nullable = false),
        StructField("Iris-versicolor", ArrayType(DoubleType, containsNull = false), nullable = false)))
    val stageProbabilitiesField = StructField("stageProbabilities", stageProbabilitiesType, nullable = false)
    val detailedPredictionColField = StructField(
      "detailed_prediction",
      StructType(
        labelField :: probabilitiesField :: contributionsField :: leafNodeAssignmentField :: stageProbabilitiesField :: Nil),
      nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  test("transformSchema without contributions") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(false)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val labelField = StructField("label", StringType, nullable = true)
    val clazzFields = Seq("Iris-setosa", "Iris-versicolor").map(clazz => StructField(clazz, DoubleType, false))
    val probabilitiesField = StructField("probabilities", StructType(clazzFields), nullable = false)
    val predictionColField = StructField("prediction", StringType, nullable = true)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(labelField :: probabilitiesField :: Nil), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }
}
