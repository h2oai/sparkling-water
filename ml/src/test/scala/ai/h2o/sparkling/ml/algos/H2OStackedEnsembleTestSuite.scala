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
import ai.h2o.sparkling.ml.models.{H2OBinaryModel, H2OMOJOModel}
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

  test("H2O StackedEnsemble model") {

    val glm = getGlm()
    val glmModel = glm.fit(dataset)

    val gbm = getGbm()
    val gbmModel = gbm.fit(dataset)

    val ensemble = new H2OStackedEnsemble()
      .setBaseModels(Seq(glmModel, gbmModel))
      .setMetalearnerAlgorithm("GLM")
      .setLabelCol("CAPSULE")

    val ensembleModel = ensemble.fit(dataset)

    ensembleModel.getMetalearnerAlgorithm() shouldBe "glm"
  }

  test("H2O StackedEnsemble deletes base models by default") {

    val drf = getDrf()
    val drfModel = drf.fit(dataset)

    val gbm = getGbm()
    val gbmModel = gbm.fit(dataset)

    val ensemble = new H2OStackedEnsemble()
      .setBaseModels(Seq(drfModel, gbmModel))
      .setLabelCol("CAPSULE")

    // 2 algos * ( 1 model + 5 cross validation models)
    val modelsNo = 2 * (1 + foldsNo)
    H2OModel.listAllModels() should have length modelsNo

    ensemble.fit(dataset)

    H2OModel.listAllModels() should have length 0
  }

  test("H2O StackedEnsemble doesn't remove base models if requested") {

    val drf = getDrf()
    val drfModel = drf.fit(dataset)

    val gbm = getGbm()
    val gbmModel = gbm.fit(dataset)

    val ensemble = new H2OStackedEnsemble()
      .setBaseModels(Seq(drfModel, gbmModel))
      .setLabelCol("CAPSULE")
      .setDeleteBaseModels(false)

    val modelsNo = 2 * (1 + foldsNo)
    H2OModel.listAllModels() should have length modelsNo

    ensemble.fit(dataset)

    H2OModel.listAllModels() should have length modelsNo
  }

  ignore("H2O Stacked Ensemble pipeline serialization and deserialization") {

    val drf = getDrf()
    val drfModel = drf.fit(dataset)

    val gbm = getGbm()
    val gbmModel = gbm.fit(dataset)

    val ensemble = new H2OStackedEnsemble()
      .setBaseModels(Seq(drfModel, gbmModel))
      .setLabelCol("CAPSULE")

    val pipeline = new Pipeline().setStages(Array(ensemble))
    pipeline.write.overwrite().save("ml/build/stacked_ensemble_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/stacked_ensemble_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/stacked_ensemble_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/stacked_ensemble_pipeline_model")

    loadedModel.transform(dataset).count()
  }


  private def getGbm() = {
    new H2OGBM()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }

  private def getGlm() = {
    new H2OGLM()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }

  private def getDrf() = {
    new H2ODRF()
      .setLabelCol("CAPSULE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)
  }
}
