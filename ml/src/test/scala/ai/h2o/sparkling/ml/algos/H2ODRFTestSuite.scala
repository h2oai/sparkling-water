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

import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2ODRFTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("H2ODRF Pipeline serialization and deserialization") {
    val algo = new H2ODRF()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/drf_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/drf_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/drf_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/drf_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  test("H2ODRF contributions") {
    val algo = new H2ODRF()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)
    val predictions = model.transform(dataset)

    val expectedCols = Seq("value", "contributions")
    assert(predictions.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))
    val contributions = predictions.select("detailed_prediction.contributions").head().getStruct(0)
    assert(contributions != null)
    assert(contributions.size == 8)
  }

  test("H2ODRF with nfolds") {
    val algo = new H2ODRF()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithContributions(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setNfolds(5)
      .setLabelCol("AGE")
    algo.fit(dataset).transform(dataset).collect()
  }
}
