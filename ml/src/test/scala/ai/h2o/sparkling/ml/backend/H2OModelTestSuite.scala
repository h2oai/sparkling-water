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

package ai.h2o.sparkling.ml.backend

import ai.h2o.sparkling.ml.algos.H2OGLM
import ai.h2o.sparkling.ml.internals.H2OModel
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OModelTestSuite extends FunSuite with Matchers with SharedH2OTestContext {
  override def createSparkContext: SparkContext =
    new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("getCurrentMetrics when trained with just training frame") {
    val modelId = "gbm_model_1"
    val glmModel = new H2OGLM()
      .setModelId(modelId)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    glmModel.fit(dataset)

    val model = H2OModel(modelId)
    assert(model.metrics.trainingMetrics.nonEmpty)
    assert(model.metrics.validationMetrics.isEmpty)
    assert(model.metrics.crossValidationMetrics.isEmpty)
    assert(model.currentMetrics.nonEmpty)
    assert(model.metrics.trainingMetrics == model.currentMetrics)
  }

  test("getCurrentMetrics when trained with validation frame") {
    val modelId = "gbm_model_2"
    val glmModel = new H2OGLM()
      .setModelId(modelId)
      .setSeed(1)
      .setSplitRatio(0.8)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    glmModel.fit(dataset)

    val model = H2OModel(modelId)
    assert(model.metrics.trainingMetrics.nonEmpty)
    assert(model.metrics.validationMetrics.nonEmpty)
    assert(model.metrics.crossValidationMetrics.isEmpty)
    assert(model.currentMetrics.nonEmpty)
    assert(model.metrics.validationMetrics == model.currentMetrics)
  }

  test("getCurrentMetrics when trained with cross-validation") {
    val modelId = "gbm_model_3"
    val glmModel = new H2OGLM()
      .setModelId(modelId)
      .setSeed(1)
      .setNfolds(3)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    glmModel.fit(dataset)

    val model = H2OModel(modelId)
    assert(model.metrics.trainingMetrics.nonEmpty)
    assert(model.metrics.validationMetrics.isEmpty)
    assert(model.metrics.crossValidationMetrics.nonEmpty)
    assert(model.currentMetrics.nonEmpty)
    assert(model.metrics.crossValidationMetrics == model.currentMetrics)
  }

  test("getCurrentMetrics when trained with validation frame and cross-validation") {
    val modelId = "gbm_model_4"
    val glmModel = new H2OGLM()
      .setModelId(modelId)
      .setSeed(1)
      .setNfolds(3)
      .setSplitRatio(0.8)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
    glmModel.fit(dataset)

    val model = H2OModel(modelId)
    assert(model.metrics.trainingMetrics.nonEmpty)
    assert(model.metrics.validationMetrics.nonEmpty)
    assert(model.metrics.crossValidationMetrics.nonEmpty)
    assert(model.currentMetrics.nonEmpty)
    assert(model.metrics.crossValidationMetrics == model.currentMetrics)
  }
}
