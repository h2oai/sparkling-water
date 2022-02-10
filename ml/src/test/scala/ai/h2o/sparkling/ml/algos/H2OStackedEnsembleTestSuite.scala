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

@RunWith(classOf[JUnitRunner])
class H2OStackedEnsembleTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  private val outputPath = "ml/build/binary.model"

  test("H2O StackedEnsemble Pipeline") {

    val foldsNo = 5

    val glm = new H2OGLM()
      .setLabelCol("AGE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)

    val glmModel = glm.fit(dataset)

    val gbm = new H2OGBM()
      .setLabelCol("AGE")
      .setNfolds(foldsNo)
      .setFoldAssignment("Modulo")
      .setKeepBinaryModels(true)
      .setKeepCrossValidationPredictions(true)

    val gbmModel = gbm.fit(dataset)

    val ensemble = new H2OStackedEnsemble()
      .setBaseModels(Seq(glmModel, gbmModel))
      .setLabelCol("AGE")

    val ensembleModel = ensemble.fit(dataset)

    // TODO assert on ...
  }


}
