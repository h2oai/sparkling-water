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

import ai.h2o.sparkling.ml.models.H2ODeepLearningMOJOModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2ODeepLearningTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/insurance.csv"))

  test("Test H2ODeepLearning") {

    import spark.implicits._

    val algo = new H2ODeepLearning()
      .setDistribution("tweedie")
      .setHidden(Array(1))
      .setEpochs(1000)
      .setTrainSamplesPerIteration(-1)
      .setReproducible(true)
      .setActivation("Tanh")
      .setSingleNodeMode(false)
      .setBalanceClasses(false)
      .setForceLoadBalance(false)
      .setSeed(23123)
      .setTweediePower(1.5)
      .setScoreTrainingSamples(0)
      .setColumnsToCategorical("District")
      .setScoreValidationSamples(0)
      .setStoppingRounds(0)
      .setFeaturesCols("District", "Group", "Age")
      .setLabelCol("Claims")

    val model: H2ODeepLearningMOJOModel = algo.fit(dataset)

    val layers = model.getModelSummary()

    layers.select("Type").as[String].collect() shouldBe Seq("Input", "Tanh", "Linear")

    val result = model.transform(dataset)

    result.count() shouldBe dataset.count()
  }
}
