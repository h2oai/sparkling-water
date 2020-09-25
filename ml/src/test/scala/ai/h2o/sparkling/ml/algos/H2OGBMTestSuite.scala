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
import org.apache.spark.sql.types.IntegerType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OGBMTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  import spark.implicits._
  test("H2OGBM with leafNodeAssignments") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setNtrees(2)
      .setWithLeafNodeAssignments(true)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setNfolds(5)
      .setLabelCol("AGE")
    val model = algo.fit(dataset)
    val prediction = model.transform(dataset)
    val leafNodeAssignments = prediction.select("detailed_prediction.leafNodeAssignments")
    assert(leafNodeAssignments.schema.length == 1)
    assert(leafNodeAssignments.schema.head.name == "leafNodeAssignments")
    assert(leafNodeAssignments.schema.head.dataType.simpleString == "array<string>")
    val withSize = leafNodeAssignments.withColumn("size", org.apache.spark.sql.functions.size($"leafNodeAssignments"))
    assert(withSize.select("size").head().getInt(0) == 2)
  }

  test("H2OGBM converts labelCol to categorical when the bernoulli distribution is set") {
    val algorithm = new H2OGBM()
      .setDistribution("bernoulli")
      .setLabelCol("CAPSULE")

    // If the labelCol wasn't converted from numeric(IntegerType) to categorical, the fit method would fail.
    val model = algorithm.fit(dataset)
    val probabilities = model.transform(dataset).select("detailed_prediction.probabilities")
    val Array(first, second) = probabilities.take(2)

    dataset.schema.fields.find(_.name == "CAPSULE").get.dataType shouldEqual IntegerType
    model.getDistribution() shouldEqual "bernoulli"
    first should not equal second
  }

  test("H2OGBM with monotone constraints") {
    val algo = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setLabelCol("CAPSULE")
      .setMonotoneConstraints(Map("AGE" -> 1, "PSA" -> -1))
    val model = algo.fit(dataset)
    val prediction = model.transform(dataset).select("prediction")
    val Array(first, second) = prediction.take(2)
    first should not equal second
  }
}
