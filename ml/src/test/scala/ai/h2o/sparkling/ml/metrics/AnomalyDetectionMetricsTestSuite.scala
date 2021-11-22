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

package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.algos.H2OIsolationForest
import ai.h2o.sparkling.ml.features.H2OAutoEncoder
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class AnomalyDetectionMetricsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val trainingDataset = spark.read
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/anomaly/ecg_discord_train.csv"))

  private lazy val validationDataset = spark.read
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/anomaly/ecg_discord_test.csv"))

  test("test calculation of isolation forest metric objects on arbitrary dataset") {
    val algorithm = new H2OIsolationForest()
      .setSeed(42)
//      .setValidationDataFrame(validationDataset)

    val model = algorithm.fit(trainingDataset)

    MetricsAssertions.assertEssentialMetrics(
      model,
      trainingDataset,
      validationDataset,
      trainingMetricsTolerance = 0.00001,
      validationMetricsTolerance = 0.00001)
  }
}
