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
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OAutoMLTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", 'CAPSULE cast "string")

  test("Setting sort metric") {
    val algo = new H2OAutoML()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setLabelCol("AGE")
    algo.setSortMetric("AUTo")
    assert(algo.getSortMetric() == "AUTO")
  }

  private def getAlgorithmForLeaderboardTesting(): H2OAutoML = {
    new H2OAutoML()
      .setLabelCol("CAPSULE")
      .setIgnoredCols(Array("ID"))
      .setExcludeAlgos(Array("GLM"))
      .setSortMetric("AUC")
      .setMaxModels(5)
  }

  test("getLeaderboard without arguments returns the same result as leaderboard") {
    val algo = getAlgorithmForLeaderboardTesting()
    algo.fit(dataset)

    TestFrameUtils.assertDataFramesAreIdentical(algo.leaderboard.get, algo.getLeaderboard())
  }

  test("Parameters of getLeaderboard add extra columns to the leaderboard") {
    val algo = getAlgorithmForLeaderboardTesting()
    algo.fit(dataset)

    val extraColumns = Seq("training_time_ms", "predict_time_per_row_ms")

    algo.getLeaderboard(extraColumns: _*).columns shouldEqual algo.getLeaderboard().columns ++ extraColumns
  }

  test("ALL as getLeaderboard adds extra columns to the leaderboard") {
    val algo = getAlgorithmForLeaderboardTesting()
    algo.fit(dataset)

    algo.getLeaderboard("ALL").columns.length should be > algo.getLeaderboard().columns.length
  }
}
