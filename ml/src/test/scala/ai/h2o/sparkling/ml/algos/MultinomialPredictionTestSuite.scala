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
class MultinomialPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))

  test("predictionCol content") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val expectedCols = Seq("prediction")
    val predictions = model.transform(dataset)
    assert(predictions.select("prediction").schema.fields.map(_.name).sameElements(expectedCols))
  }

  test("detailedPredictionCol content") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("sepal_len", "sepal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)

    val expectedCols = Seq("label", "probabilities", "leafNodeAssignments", "stageProbabilities")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    val probabilities = predictions.select("detailed_prediction.probabilities.*").limit(2).cache()
    val virginicaProbabilities = probabilities.select("Iris-virginica").collect().map(_.getDouble(0))
    assert(virginicaProbabilities(0) != virginicaProbabilities(1))
    val setosaProbabilities = probabilities.select("Iris-setosa").collect().map(_.getDouble(0))
    assert(setosaProbabilities(0) != setosaProbabilities(1))
    val versicolorProbabilities = probabilities.select("Iris-versicolor").collect().map(_.getDouble(0))
    assert(versicolorProbabilities(0) != versicolorProbabilities(1))
    val leafNodeAssignments = predictions.select("detailed_prediction.leafNodeAssignments").head().getSeq[String](0)
    assert(leafNodeAssignments != null)
    assert(leafNodeAssignments.length == algo.getNtrees() * 3) // 3 is number of classes
    val stageProbabilities = predictions.select("detailed_prediction.stageProbabilities").head().getStruct(0)
    assert(stageProbabilities != null)
    assert(stageProbabilities.size == 3)
    assert(stageProbabilities.getList(0).size() == algo.getNtrees())
    assert(stageProbabilities.getList(1).size() == algo.getNtrees())
    assert(stageProbabilities.getList(2).size() == algo.getNtrees())
  }

  test("transformSchema") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithLeafNodeAssignments(true)
      .setWithStageResults(true)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val labelField = StructField("label", StringType, nullable = true)
    val classFields = Seq("Iris-setosa", "Iris-versicolor", "Iris-virginica")
      .map(StructField(_, DoubleType, nullable = false))
    val probabilitiesField = StructField("probabilities", StructType(classFields), nullable = false)
    val predictionColField = StructField("prediction", StringType, nullable = true)
    val leafNodeAssignmentField =
      StructField("leafNodeAssignments", ArrayType(StringType, containsNull = false), nullable = false)
    val stageProbabilitiesType = StructType(
      Seq(
        StructField("Iris-setosa", ArrayType(DoubleType, containsNull = false), nullable = false),
        StructField("Iris-versicolor", ArrayType(DoubleType, containsNull = false), nullable = false),
        StructField("Iris-virginica", ArrayType(DoubleType, containsNull = false), nullable = false)))
    val stageProbabilitiesField = StructField("stageProbabilities", stageProbabilitiesType, nullable = false)
    val detailedPredictionColField =
      StructField(
        "detailed_prediction",
        StructType(labelField :: probabilitiesField :: leafNodeAssignmentField :: stageProbabilitiesField :: Nil),
        nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)
    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }

  test("getDomainValues") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)

    val domainValues = model.getDomainValues()
    assert(domainValues("class").toList.sorted == Seq("Iris-virginica", "Iris-setosa", "Iris-versicolor").sorted)
    assert(domainValues("sepal_len") == null)
    assert(domainValues("sepal_wid") == null)
    assert(domainValues("petal_len") == null)
    assert(domainValues("petal_wid") == null)
  }
}
