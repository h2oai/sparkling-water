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
class H2OGLMTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("Propagation of predictionCol settings") {

    val predictionCol = "my_prediction_col_name"
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setPredictionCol(predictionCol)

    val model = algo.fit(dataset)

    assert(model.transform(dataset).columns.contains(predictionCol))
  }

  test("Test H2OGLM Pipeline") {

    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/glm_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/glm_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/glm_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/glm_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  test("H2OGLM with set modelId is trained mutliple times") {
    val modelId = "testingH2OGLMModel"

    val key1 = Key.make(modelId).toString
    val key2 = Key.make(modelId + "_1").toString
    val key3 = Key.make(modelId + "_2").toString

    val dataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

    // Create GLM model
    val algo = new H2OGLM()
      .setModelId(modelId)
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    H2OModel.modelExists(key1) shouldBe false

    algo.fit(dataset)
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

  test("H2OGLM converts labelCol to categorical when the binomial family is set") {
    val algorithm = new H2OGLM()
      .setFamily("binomial")
      .setLabelCol("CAPSULE")

    // If the labelCol wasn't converted from numeric(IntegerType) to categorical, the fit method would fail.
    val model = algorithm.fit(dataset)
    val probabilities = model.transform(dataset).select("detailed_prediction.probabilities")
    val Array(first, second) = probabilities.take(2)

    dataset.schema.fields.find(_.name == "CAPSULE").get.dataType shouldEqual IntegerType
    model.getFamily() shouldEqual "binomial"
    first should not equal second
  }
}
