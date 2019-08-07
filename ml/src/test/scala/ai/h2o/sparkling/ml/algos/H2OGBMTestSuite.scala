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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OGBMTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))


  test("H2OGBM with contributions") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithDetailedPredictionCol(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)
    val contributions = predictions.select("detailed_prediction.contributions").head().getAs[Seq[Double]](0)
    assert(contributions != null)
    assert(contributions.size == 8)
  }

  test("H2OGBM basic output in the prediction column, the old variant") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithDetailedPredictionCol(false)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)
    // if the column would not exist, Spark would throw exception
    predictions.select("prediction.value").head().getDouble(0)
  }

  // This test can be enabled after we deprecate the old behaviour captured in the test above. It was created now
  // to give context to the reader of this source code where the changes are heading
  ignore("H2OGBM basic output in the prediction column") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setWithDetailedPredictionCol(false)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")

    val model = algo.fit(dataset)

    val predictions = model.transform(dataset)
    predictions.select("prediction").head().getDouble(0)
  }
}
