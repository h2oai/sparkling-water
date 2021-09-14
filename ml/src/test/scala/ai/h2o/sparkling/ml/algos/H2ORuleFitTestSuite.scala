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

import ai.h2o.sparkling.ml.algos.classification.H2ORuleFitClassifier
import ai.h2o.sparkling.ml.algos.regression.H2ORuleFitRegressor
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.functions.col

@RunWith(classOf[JUnitRunner])
class H2ORuleFitTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("H2ORuleFit Pipeline serialization and deserialization") {
    val algo = new H2ORuleFit()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/rulefit_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/rulefit_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/rulefit_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/rulefit_pipeline_model")

    val predictions = loadedModel.transform(dataset)
    predictions.where(col("prediction") > 0).count() shouldEqual predictions.count()
  }

  test("H2ORuleFitRegressor") {
    val algo = new H2ORuleFitRegressor()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)
    val predictions = model.transform(dataset)

    val expectedDetailsCols = Seq("value")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedDetailsCols))
    predictions.where(col("prediction") > 0).count() shouldEqual predictions.count()
  }

  test("H2ORuleFitClassifier") {
    val algo = new H2ORuleFitClassifier()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("CAPSULE")

    val model = algo.fit(dataset)
    val predictions = model.transform(dataset)
    val expectedDetailsCols = Seq("label", "probabilities")
    val predictionCounts = predictions.groupBy("prediction").count()

    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedDetailsCols))
    predictionCounts.where(col("count") > 0).count() shouldEqual predictionCounts.count()
  }
}
