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

import ai.h2o.sparkling.ml.models.H2OMOJOModel
import ai.h2o.sparkling.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class NoRuntimeMetricsTestSuite extends FunSuite with Matchers with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val irisDataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv("examples/smalldata/iris/iris_wheader.csv")
  }

  private lazy val prostateDataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("examples/smalldata/prostate/prostate.csv")
      .withColumnRenamed("CAPSULE", "capsule")
  }

  test("Test calculation of metrics on saved binomial model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")
    mojo.getMetrics(prostateDataFrame) shouldNot be(null)
    mojo.getMetricsObject(prostateDataFrame) shouldNot be(null)
  }

  test("Test calculation of metrics on saved regression model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("regre_model_prostate.mojo"),
      "regre_model_prostate.mojo")
    mojo.getMetrics(prostateDataFrame) shouldNot be(null)
    mojo.getMetricsObject(prostateDataFrame) shouldNot be(null)
  }

  test("Test calculation of metrics on saved multinomial model") {
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("multi_model_iris.mojo"),
      "multi_model_iris.mojo")
    mojo.getMetrics(irisDataFrame) shouldNot be(null)
    mojo.getMetricsObject(irisDataFrame) shouldNot be(null)
  }
}
