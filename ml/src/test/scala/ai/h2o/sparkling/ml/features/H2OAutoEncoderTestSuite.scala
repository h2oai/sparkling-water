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

package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.algos.H2ODeepLearning
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OAutoEncoderTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", 'CAPSULE.cast("string"))
    .withColumn("RACE", 'RACE.cast("string"))
    .withColumn("ID", 'ID.cast("string"))
    .withColumn("fake", lit(1))

  test("AutoEncoder test suite") {

    val algo = new H2OAutoEncoder()
      .setSeed(1)
      .setInputCols("ID", "CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setOutputCol("Output")
      .setHidden(Array(3))
      .setReproducible(true)

    val model = algo.fit(dataset)

    model.transform(dataset).show(truncate = false)
  }

  test("DeepLearnning test suite") {

    val algo = new H2ODeepLearning()
      .setSeed(1)
      .setFeaturesCols( "CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("fake")
      .setAutoencoder(true)
      .setHidden(Array(3))
      .setReproducible(true)
      .setSplitRatio(0.8)

    val model = algo.fit(dataset)
    val m =model.getValidationMetrics()
    model.transform(dataset).select("*","detailed_prediction.*").show(truncate = false)
  }
}
