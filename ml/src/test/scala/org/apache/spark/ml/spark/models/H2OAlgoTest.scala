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

import hex.Model
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.h2o.algos._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class H2OAlgoTest extends FunSuite with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", "mojo-test-local", conf = defaultSparkConf)

  test("Test H2OGLM Pipeline") {

    val dataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
      // Create GBM model
     val algo = new H2OGLM()
        .setTrainRatio(0.8)
        .setSeed(1)
        .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA" , "VOL", "GLEASON")
        .setLabelCol("AGE")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/glm_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/glm_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/glm_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/glm_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  test("H2O Grid Search GLM Pipeline"){
    val glm = new H2OGLM()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(glm, hyperParams)
  }

  test("H2O Grid Search GBM Pipeline"){
    val gbm = new H2OGBM()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += ("_ntrees" -> Array(1, 10, 30).map(_.asInstanceOf[AnyRef]), "_seed" -> Array(1, 2).map(_.asInstanceOf[AnyRef]))

    testGridSearch(gbm, hyperParams)
  }

  test("H2O Grid Search DeepLearning Pipeline"){
    val deeplearning = new H2ODeepLearning()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(deeplearning, hyperParams)
  }


  private def testGridSearch(algo: H2OAlgorithm[_ <: Model.Parameters], hyperParams: mutable.HashMap[String, Array[AnyRef]]): Unit = {
      val dataset = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

      val stage = new H2OGridSearch()
        .setLabelCol("AGE")
        .setHyperParameters(hyperParams)
        .setAlgo(algo)

      val pipeline = new Pipeline().setStages(Array(stage))
      pipeline.write.overwrite().save("ml/build/grid_glm_pipeline")
      val loadedPipeline = Pipeline.load("ml/build/grid_glm_pipeline")
      val model = loadedPipeline.fit(dataset)

      model.write.overwrite().save("ml/build/grid_glm_pipeline_model")
      val loadedModel = PipelineModel.load("ml/build/grid_glm_pipeline_model")

      loadedModel.transform(dataset).count()
  }
}
