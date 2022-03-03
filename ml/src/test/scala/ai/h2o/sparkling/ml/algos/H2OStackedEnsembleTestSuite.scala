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
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OStackedEnsembleTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", col("CAPSULE").cast("string"))

  private val outputPath = "ml/build/binary.model"
  private val foldsNo = 5

  test("Create Stacked Ensemble model using cross validations") {

    val glm = getGlm()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(glm, gbm))
      .setMetalearnerAlgorithm("GLM")
      .setLabelCol("CAPSULE")
      .setSeed(42)
      .setScoreTrainingSamples(0)

    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2), seed = 42)
    val ensembleModel = ensemble.fit(train)

    ensembleModel.getMetalearnerAlgorithm() shouldBe "glm"

    val predictions = ensembleModel.transform(test)
    predictions.distinct().count() shouldBe 56
  }

  test("Create Stacked Ensemble using blending frame") {

    val Array(trainingDF, blendingDF) = dataset.randomSplit(Array(0.6, 0.4), seed = 42)

    val drf = new H2ODRF()
      .setLabelCol("CAPSULE")
      .setKeepBinaryModels(true)

    val gbm = new H2OGBM()
      .setLabelCol("CAPSULE")
      .setKeepBinaryModels(true)

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setBlendingDataFrame(blendingDF)
      .setLabelCol("CAPSULE")
      .setSeed(42)

    val ensembleModel = ensemble.fit(trainingDF)

    val predictions = ensembleModel.transform(dataset)
    predictions.distinct().count() shouldBe 380
  }

  test("H2O StackedEnsemble deletes base models by default") {

    val drf = getDrf()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setLabelCol("CAPSULE")
      .setSeed(42)

    H2OModel.listAllModels() should have length 0

    ensemble.fit(dataset)

    ensemble.getBaseModels() should have length 0
    H2OModel.listAllModels() should have length 0
  }

  test("H2O StackedEnsemble doesn't remove base models if requested") {

    val drf = getDrf()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setLabelCol("CAPSULE")
      .setKeepBaseModels(true)
      .setSeed(42)

    H2OModel.listAllModels() should have length 0

    ensemble.fit(dataset)

    ensemble.getBaseModels() should have length 2

    // 2 algos * ( 1 model + 5 cross validation models )
    val modelsNo = 2 * (1 + foldsNo)
    H2OModel.listAllModels() should have length modelsNo

    // cleanup
    ensemble.deleteBaseModels()
    H2OModel.listAllModels() should have length 0
  }

  test("H2O Stacked Ensemble pipeline serialization and deserialization") {

    val drf = getDrf()
    val gbm = getGbm()

    val ensemble = new H2OStackedEnsemble()
      .setBaseAlgorithms(Array(drf, gbm))
      .setLabelCol("CAPSULE")
      .setSeed(42)

    val pipeline = new Pipeline().setStages(Array(ensemble))
    pipeline.write.overwrite().save("ml/build/stacked_ensemble_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/stacked_ensemble_pipeline")

    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/stacked_ensemble_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/stacked_ensemble_pipeline_model")

    loadedModel.transform(dataset).count() shouldBe 380
  }

  private def getGbm() = {
    new H2OGBM()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setSeed(42)
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }

  private def getGlm() = {
    new H2OGLM()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setSeed(42)
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }

  private def getDrf() = {
    new H2ODRF()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setSeed(42)
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }
}
