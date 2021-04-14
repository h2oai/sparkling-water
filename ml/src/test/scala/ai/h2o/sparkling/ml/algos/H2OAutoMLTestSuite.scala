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
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import org.apache.spark.sql.functions.col

@RunWith(classOf[JUnitRunner])
class H2OAutoMLTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

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

  private def getAlgorithmForLeaderboardTesting: H2OAutoML = {
    new H2OAutoML()
      .setLabelCol("CAPSULE")
      .setIgnoredCols(Array("ID"))
      .setExcludeAlgos(Array("GLM"))
      .setSortMetric("AUC")
      .setMaxModels(5)
  }

  test("Parameters of getLeaderboard add extra columns to the leaderboard") {
    val algo = getAlgorithmForLeaderboardTesting
    algo.fit(dataset)

    val extraColumns = Seq("training_time_ms", "predict_time_per_row_ms")
    val nullColumns = Seq("predict_time_per_row_ms")
    val leaderboardWithExtraColumns = algo.getLeaderboard(extraColumns: _*)
    val nonNullColumns = leaderboardWithExtraColumns.columns.diff(nullColumns)
    val nullValues = leaderboardWithExtraColumns.select(nullColumns.map(col): _*).first().toSeq
    val nonNullValues = leaderboardWithExtraColumns.select(nonNullColumns.map(col): _*).first().toSeq

    nullValues shouldEqual Seq(null) // TODO: This needs to be fixed in H2O-3 AutoML backend
    nonNullValues shouldNot contain(null)
    leaderboardWithExtraColumns.columns shouldEqual algo.getLeaderboard().columns ++ extraColumns
  }

  test("ALL as getLeaderboard adds extra columns to the leaderboard") {
    val algo = getAlgorithmForLeaderboardTesting
    algo.fit(dataset)

    algo.getLeaderboard("ALL").columns.length should be > algo.getLeaderboard().columns.length
  }

  test("AutoML with nfolds") {
    val automl = new H2OAutoML()
      .setLabelCol("CAPSULE")
      .setIgnoredCols(Array("ID"))
      .setExcludeAlgos(Array("GLM"))
      .setSortMetric("AUC")
      .setNfolds(5)
      .setMaxModels(5)

    val model = automl.fit(dataset)
    model.transform(dataset).collect()
  }

  test("H2OAutoML doesn't throw exception on GLM model and enabled contributions") {
    val automl = new H2OAutoML()
    automl.setIncludeAlgos(Array("GLM"))
    automl.setLabelCol("CAPSULE")
    automl.setMaxModels(5)
    automl.setSeed(42)
    automl.setWithContributions(true)

    automl.fit(dataset).transform(dataset).collect()
  }
}
