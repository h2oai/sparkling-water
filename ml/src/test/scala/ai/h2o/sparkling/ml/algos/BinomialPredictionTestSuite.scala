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
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils
@RunWith(classOf[JUnitRunner])
class BinomialPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

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

      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val expectedCols = Seq("p0", "p1")
    val predictions = model.transform(dataset)
    assert(predictions.select("prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    assert(!predictions.columns.contains("detailed_prediction"))
  }

  test("detailedPredictionCol content") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithDetailedPredictionCol(true)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)

    val expectedCols = Seq("p0", "p1", "contributions")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    val contributions = predictions.select("detailed_prediction.contributions").head().getAs[Seq[Double]](0)
    assert(contributions != null)
    assert(contributions.size == 5)
  }

  test("transformSchema with detailed prediction col") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithDetailedPredictionCol(true)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val propFields = Seq("p0", "p1").map(StructField(_, DoubleType, nullable = false))
    val predictionColField = StructField("prediction", StructType(propFields), nullable = false)
    val contributionsField = StructField("contributions", ArrayType(FloatType))
    val detailedPredictionColField = StructField("detailed_prediction", StructType(propFields ++ Seq(contributionsField)), nullable = false)

    val expectedSchema = StructType(datasetFields ++ (predictionColField :: detailedPredictionColField :: Nil))
    val schema = model.transformSchema(dataset.schema)
    assert(schema == expectedSchema)
  }

  test("transformSchema without detailed prediction col") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")
      .setColumnsToCategorical("class")
      .setLabelCol("class")
    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val propFields = Seq("p0", "p1").map(StructField(_, DoubleType, nullable = false))
    val predictionColField = StructField("prediction", StructType(propFields), nullable = false)

    val expectedSchema = StructType(datasetFields ++ (predictionColField :: Nil))
    val schema = model.transformSchema(dataset.schema)
    assert(schema == expectedSchema)
  }
}
