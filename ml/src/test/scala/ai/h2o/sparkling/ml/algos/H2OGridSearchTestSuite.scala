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

import hex.Model
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class H2OGridSearchTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("H2O Grid Search GLM Pipeline") {
    val glm = new H2OGLM()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(glm, hyperParams)
  }

  test("H2O Grid Search GBM Pipeline") {
    val gbm = new H2OGBM()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += ("_ntrees" -> Array(1, 10, 30).map(_.asInstanceOf[AnyRef]), "_seed" -> Array(1, 2).map(_.asInstanceOf[AnyRef]))

    testGridSearch(gbm, hyperParams)
  }

  test("H2O Grid Search DeepLearning Pipeline") {
    val deeplearning = new H2ODeepLearning()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(deeplearning, hyperParams)
  }

  test("H2O Grid Search XGBoost Pipeline") {
    val xgboost = new H2OXGBoost()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(xgboost, hyperParams)
  }


  private def testGridSearch(algo: H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters], hyperParams: mutable.HashMap[String, Array[AnyRef]]): Unit = {
    val stage = new H2OGridSearch()
      .setLabelCol("AGE")
      .setHyperParameters(hyperParams)
      .setAlgo(algo)

    val pipeline = new Pipeline().setStages(Array(stage))
    val algoName = algo.getClass.getSimpleName
    pipeline.write.overwrite().save(s"ml/build/grid_${algoName}_pipeline")
    val loadedPipeline = Pipeline.load(s"ml/build/grid_${algoName}_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save(s"ml/build/grid_${algoName}_pipeline_model")
    val loadedModel = PipelineModel.load(s"ml/build/grid_${algoName}_pipeline_model")

    loadedModel.transform(dataset).count()
  }

}
