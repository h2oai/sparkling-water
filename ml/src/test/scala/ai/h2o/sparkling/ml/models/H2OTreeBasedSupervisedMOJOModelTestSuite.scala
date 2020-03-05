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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.algos._
import hex.Model
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OTreeBasedSupervisedMOJOModelTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", getClass.getSimpleName, conf = defaultSparkConf)

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .cache()

  def testTrainedTreeBasedModelHasPositiveNumberOfTrees(
      algo: H2OTreeBasedSupervisedAlgorithm[_ <: Model.Parameters]): Unit = {
    algo
      .setNfolds(5)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
    val model = algo.fit(dataset)

    assert(algo.getNtrees() > 0)
    assert(model.getNtrees() > 0)
  }

  test("Positive number of trees after training - GBM") {
    testTrainedTreeBasedModelHasPositiveNumberOfTrees(new H2OGBM())
  }

  test("Positive number of trees after training - DRF") {
    testTrainedTreeBasedModelHasPositiveNumberOfTrees(new H2ODRF())
  }

  test("Positive number of trees after training - XGBoost") {
    testTrainedTreeBasedModelHasPositiveNumberOfTrees(new H2OXGBoost())
  }

  def testLoadedTreeBasedModelHasPositiveNumberOfTrees(
    algo: H2OTreeBasedSupervisedAlgorithm[_ <: Model.Parameters]): Unit = {
    algo
      .setNfolds(5)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
    val model = algo.fit(dataset)
    val path = s"ml/build/loaded_tree_based_has_positive_number_of_trees_${algo.getClass.getSimpleName}"
    model.write.overwrite().save(path)

    val loadedModel = H2OTreeBasedSupervisedMOJOModel.load(path)
    assert(loadedModel.getNtrees() > 0)
  }

  test("Positive number of trees after save & load - GBM") {
    testTrainedTreeBasedModelHasPositiveNumberOfTrees(new H2OGBM())
  }

  test("Positive number of trees after save & load - DRF") {
    testTrainedTreeBasedModelHasPositiveNumberOfTrees(new H2ODRF())
  }

  test("Positive number of trees after save & load - XGBoost") {
    testTrainedTreeBasedModelHasPositiveNumberOfTrees(new H2OXGBoost())
  }

  test("Load GLM as tree-based model fails") {
    val algo = new H2OGLM()
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")
    val model = algo.fit(dataset)
    val path = "ml/build/load_glm_as_tree_based_algorithm_fails"
    model.write.overwrite().save(path)

    intercept[RuntimeException] {
      H2OTreeBasedSupervisedMOJOModel.load(path)
    }
  }
}
