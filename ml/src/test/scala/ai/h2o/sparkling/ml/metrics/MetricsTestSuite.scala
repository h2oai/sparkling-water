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

import ai.h2o.sparkling.SparkTestContext
import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class MetricsTestSuite extends FunSuite with Matchers with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  var maybeSparkTesting:Option[String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    maybeSparkTesting = Option(System.getProperty("spark.testing"))
  }

  override def afterAll(): Unit = {
    maybeSparkTesting match {
      case Some(value) => System.setProperty("spark.testing", value)
      case None => System.clearProperty("spark.testing")
    }
    super.afterAll()
  }

  def jsonTemplate(confusionMatrix: String): String =
    s"""
      | {
      | "description": "description",
      | "scoring_time": 0,
      | "MSE": 1234.5,
      | "RMSE" : 1.5,
      | "nobs" : 1,
      |
      | "r2" : 1.0,
      | "logloss": 2.0,
      | "AUC": 3.0,
      | "pr_auc": 4.0,
      | "Gini": 5.0,
      | "mean_per_class_error": 6.0,
      |
      | ${confusionMatrix},
      | "thresholds_and_metric_scores": null,
      | "max_criteria_and_metric_scores": null,
      | "gains_lift_table": null
      | }
      |""".stripMargin

  def jsonWithoutConfusionMatrix: JsonObject = {
    val confusionMatrix =
      """
        | "cm": null
        |""".stripMargin

    JsonParser.parseString(jsonTemplate(confusionMatrix)).getAsJsonObject
  }

  def jsonContainingConfusionMatrix: JsonObject = {
    val confusionMatrix =
      """
        | "cm":{"__meta":{"schema_version":3,"schema_name":"ConfusionMatrixV3","schema_type":"ConfusionMatrix"},
        |        "table":{"__meta":{"schema_version":3,"schema_name":"TwoDimTableV3","schema_type":"TwoDimTable"},
        |        "name":"Confusion Matrix","description":"Row labels: Actual class; Column labels: Predicted class",
        |        "columns":[{"__meta":{"schema_version":-1,"schema_name":"ColumnSpecsBase","schema_type":"Iced"},
        |                    "name":"0",
        |                    "type":"long",
        |                    "format":"%d",
        |                    "description":"0"},
        |                   {"__meta":{"schema_version":-1,"schema_name":"ColumnSpecsBase","schema_type":"Iced"},
        |                    "name":"1",
        |                    "type":"long",
        |                    "format":"%d",
        |                    "description":"1"},
        |                   {"__meta":{"schema_version":-1,"schema_name":"ColumnSpecsBase","schema_type":"Iced"},
        |                    "name":"Error",
        |                    "type":"double",
        |                    "format":"%.4f",
        |                    "description":"Error"},
        |                   {"__meta":{"schema_version":-1,"schema_name":"ColumnSpecsBase","schema_type":"Iced"},
        |                    "name":"Rate",
        |                    "type":"string",
        |                    "format":"= %8s",
        |                    "description":"Rate"}],
        |        "rowcount":3,
        |        "data":[[190,34,224],
        |                [37,119,156],
        |                [0.16,0.22,0.18],
        |                ["37 / 227","34 / 153","71 / 380"]]}}
        |""".stripMargin

    JsonParser.parseString(jsonTemplate(confusionMatrix)).getAsJsonObject
  }

  test("Filling confusion matrix with a null value in production (spark.testing != true)") {
    val m = new H2OBinomialMetrics()

    System.setProperty("spark.testing", "false")
    m.setMetrics(jsonWithoutConfusionMatrix,"test")

    m.getConfusionMatrix() shouldBe null
  }

  test("Filling confusion matrix with a null value in test (spark.testing == true)") {
    val m = new H2OBinomialMetrics()

    System.setProperty("spark.testing", "true")
    m.setMetrics(jsonWithoutConfusionMatrix,"test")

    m.getConfusionMatrix() shouldBe null
  }

  test("Filling confusion matrix with real values in production (spark.testing != true)") {
    val m = new H2OBinomialMetrics()

    System.setProperty("spark.testing", "false")
    m.setMetrics(jsonContainingConfusionMatrix,"test")

    m.getConfusionMatrix() should not be null
    m.getConfusionMatrix().count() should be (3)
  }

  test("Filling confusion matrix with real values in test (spark.testing == true)") {
    val m = new H2OBinomialMetrics()

    System.setProperty("spark.testing", "true")
    m.setMetrics(jsonContainingConfusionMatrix,"test")

    m.getConfusionMatrix() should not be null
    m.getConfusionMatrix().count() should be (3)
  }
}
