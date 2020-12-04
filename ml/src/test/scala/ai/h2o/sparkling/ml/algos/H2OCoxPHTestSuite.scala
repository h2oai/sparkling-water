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

import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.Key

@RunWith(classOf[JUnitRunner])
class H2OCoxPHTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/coxph_test/heart.csv"))

  private lazy val datasetTest = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/coxph_test/heart_test.csv"))

  test("Test H2OCoxPH Pipeline") {
    val algo = new H2OCoxPH()
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/cox_ph_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/cox_ph_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/cox_ph_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/cox_ph_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  test("H2OCoxPH with set modelId is trained mutliple times") {
    val modelId = "testingH2OCoxPHModel"

    val key1 = Key.make(modelId).toString
    val key2 = Key.make(modelId + "_1").toString
    val key3 = Key.make(modelId + "_2").toString

    // Create  model
    val algo = new H2OCoxPH()
      .setModelId(modelId)
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")

    H2OModel.modelExists(key1) shouldBe false

    algo.fit(dataset)
    print(H2OModel.listAllModels().mkString("Array(", ", ", ")"))
    H2OModel.modelExists(key1) shouldBe true
    H2OModel.modelExists(key2) shouldBe false
    H2OModel.modelExists(key3) shouldBe false

    algo.fit(dataset)
    H2OModel.modelExists(key1) shouldBe true
    H2OModel.modelExists(key2) shouldBe true
    H2OModel.modelExists(key3) shouldBe false

    algo.fit(dataset)
    H2OModel.modelExists(key1) shouldBe true
    H2OModel.modelExists(key2) shouldBe true
    H2OModel.modelExists(key3) shouldBe true
  }

  test("predictions have reasonable values") {
    // Create  model
    val algo = new H2OCoxPH()
      .setStartCol("start")
      .setStopCol("stop")
      .setLabelCol("event")
      .setIgnoredCols(Array("id"))

    val model = algo.fit(dataset)
    val predictions = model.transform(dataset) .select("prediction")

    predictions.show()

    predictions.count() shouldBe dataset.count()
    predictions.filter("prediction is not null").count() shouldBe dataset.count()
    predictions.first().get(0) shouldBe 0.20032351116082292

  }
}
