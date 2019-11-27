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

package org.apache.spark.ml.spark.models

import ai.h2o.sparkling.ml.algos.{H2ODeepLearning, H2OGBM, H2OGLM, H2OSupervisedAlgorithm}
import ai.h2o.sparkling.ml.models.H2OSupervisedMOJOModel
import ai.h2o.sparkling.ml.params.NullableStringParam
import hex.Model
import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OBaseModel, H2OBaseModelBuilder}
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OSupervisedMOJOModelTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", getClass.getSimpleName, conf = defaultSparkConf)

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .cache()

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.8, 0.2), 1234L).map(_.cache())

  def testOffsetColumnGetsPropagatedToMOJOModel(
      algo: H2OSupervisedAlgorithm[_ <: H2OBaseModelBuilder, _ <: H2OBaseModel, _ <: Model.Parameters]): Unit = {
    val offsetColumn = "PSA"
    algo
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
      .setOffsetCol(offsetColumn)

    val pipeline = new Pipeline().setStages(Array(algo))

    val model = pipeline.fit(dataset)
    val path = s"ml/build/offset_propagation_to_mojo_${algo.getClass.getSimpleName}"
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)

    def getModelOffset(model: PipelineModel): String = {
      model.stages(0).asInstanceOf[H2OSupervisedMOJOModel].getOffsetCol()
    }
    val modelOffset = getModelOffset(model)
    val deserializedModelOffset = getModelOffset(loadedModel)

    modelOffset shouldEqual offsetColumn
    deserializedModelOffset shouldEqual offsetColumn
  }

  test("Offset column gets propagated to MOJO model - GBM") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2OGBM())
  }

  test("Offset column gets propagated to MOJO model - GLM") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2OGLM())
  }

  test("Offset column gets propagated to MOJO model - DeepLearning") {
    testOffsetColumnGetsPropagatedToMOJOModel(new H2ODeepLearning())
  }

  def testDeserializedMOJOAndOriginalMOJOReturnSameResult(
      algo: H2OSupervisedAlgorithm[_ <: H2OBaseModelBuilder, _ <: H2OBaseModel, _ <: Model.Parameters]): Unit = {
    val offsetColumn = "PSA"
    algo
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
      .setOffsetCol(offsetColumn)

    val pipeline = new Pipeline().setStages(Array(algo))

    val model = pipeline.fit(trainingDataset)
    val path = s"ml/build/deserialized_mojo_produce_same_result_${algo.getClass.getSimpleName}"
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)

    val originalResult = model.transform(testingDataset)
    val deserializedResult = model.transform(testingDataset)

    TestFrameUtils.assertDataFramesAreIdentical(originalResult, deserializedResult)
  }

  test("The original MOJO and deserialized MOJO return the same result - GBM") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2OGBM())
  }

  // TODO: Enable test once PUBDEV-7067 is resolved.
  // GLMMojoModel doesn't support offsets, and throws an UnsupportedOperationException if a non-zero offset value is set.
  ignore("The original MOJO and deserialized MOJO return the same result - GLM") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2OGLM())
  }

  test("The original MOJO and deserialized MOJO return the same result - DeepLearning") {
    testDeserializedMOJOAndOriginalMOJOReturnSameResult(new H2ODeepLearning())
  }

  def testMOJOWithSetOffsetColumnReturnsDifferentResult(
      algo: H2OSupervisedAlgorithm[_ <: H2OBaseModelBuilder, _ <: H2OBaseModel, _ <: Model.Parameters]): Unit = {
    val offsetColumn = "PSA"
    algo
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setOffsetCol(offsetColumn)

    val model = algo.fit(trainingDataset)
    def extractResult(model: H2OSupervisedMOJOModel): Array[Any] = {
      model
        .transform(testingDataset)
        .sort("ID")
        .select("prediction")
        .collect()
        .map((r: Row) => r.get(0))
    }

    val resultWithOffset = extractResult(model)

    val offsetColField = model.getClass.getDeclaredField("offsetCol")
    offsetColField.setAccessible(true)
    val offsetColParam = offsetColField.get(model).asInstanceOf[NullableStringParam]
    model.set(offsetColParam, null)

    val resultWithoutOffset = extractResult(model)

    resultWithOffset should not equal resultWithoutOffset
  }

  test("The MOJO model with set offsetColumn returns a different result - GBM") {
    testMOJOWithSetOffsetColumnReturnsDifferentResult(new H2OGBM())
  }

  // TODO: Enable test once PUBDEV-7067 is resolved.
  // GLMMojoModel doesn't support offsets, and throws an UnsupportedOperationException if a non-zero offset value is set.
  ignore("The MOJO model with set offsetColumn returns a different result - GLM") {
    testMOJOWithSetOffsetColumnReturnsDifferentResult(new H2OGLM())
  }

  // TODO: Enable test once PUBDEV-7067 is resolved.
  // Setting offset on DeepLearningMojoModel doesn't take effect.
  ignore("The MOJO model with set offsetColumn returns a different result - DeepLearning") {
    testMOJOWithSetOffsetColumnReturnsDifferentResult(new H2ODeepLearning())
  }
}
