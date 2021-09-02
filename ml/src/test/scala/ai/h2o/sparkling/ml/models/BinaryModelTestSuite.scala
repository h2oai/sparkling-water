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

package ai.h2o.sparkling.ml.models

import java.io.File

import ai.h2o.sparkling.backend.BuildInfo
import ai.h2o.sparkling.ml.algos.{H2OAutoML, H2OGBM, H2OGridSearch}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class BinaryModelTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  private lazy val referenceAlgo = new H2OGBM()
    .setSeed(1)
    .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
    .setLabelCol("AGE")
    .setKeepBinaryModels(true)

  private val outputPath = "ml/build/binary.model"

  test("Exported binary model exists") {
    referenceAlgo.fit(dataset)
    referenceAlgo.getBinaryModel().write(outputPath)
    assert(new File(outputPath).exists())
  }

  test("Fields on H2OBinaryModel are set after loading") {
    referenceAlgo.fit(dataset)
    referenceAlgo.getBinaryModel().write(outputPath)
    val binaryModel = H2OBinaryModel.read(outputPath)
    assert(binaryModel.getBinaryModel().isDefined)
  }

  test("Binary model is available in H2O after training in SW") {
    referenceAlgo.fit(dataset)
    val binaryModel = referenceAlgo.getBinaryModel()
    assert(H2OBinaryModel.exists(binaryModel.modelId))
  }

  test("Binary model is uploaded to H2O cluster after loading") {
    referenceAlgo.fit(dataset)
    val binaryModel = referenceAlgo.getBinaryModel()
    binaryModel.write(outputPath)
    val model = H2OBinaryModel.read(outputPath)
    H2OBinaryModel.exists(model.modelId)
  }

  test("Load H2OBinaryModel trained in different H2O is not supported") {
    val path = this.getClass.getClassLoader.getResource("old_version_binary.model")
    val thrown = intercept[IllegalArgumentException] {
      H2OBinaryModel.read(path.getFile)
    }
    assert(thrown.getMessage == s"""
                              | The binary model has been trained in H2O of version
                              | 3.30.0.1 but you are currently running H2O version of ${BuildInfo.H2OVersion}.
                              | Please make sure that running Sparkling Water/H2O-3 cluster and the loaded binary
                              | model correspond to the same H2O-3 version.""".stripMargin)
  }

  test("Loading Binary Model when Algo hasn't been fit throw exception") {
    val gbm = new H2OGBM()
    val thrown = intercept[IllegalArgumentException] {
      gbm.getBinaryModel()
    }
    assert(thrown.getMessage == "Algorithm needs to be fit first with the `keepBinaryModels` parameter " +
      "set to true in order to access binary model.")
  }

  test("Binary model is available in H2O after training Grid in SW") {
    val grid = new H2OGridSearch()
      .setAlgo(
        new H2OGBM()
          .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
          .setLabelCol("AGE")
          .setKeepBinaryModels(true))

      .setHyperParameters(Map("ntrees" -> Array(10, 20).map(_.asInstanceOf[AnyRef])))
    grid.fit(dataset)
    val binaryModel = grid.getBinaryModel()
    assert(H2OBinaryModel.exists(binaryModel.modelId))
  }

  test("Acquiring model is available in H2O after training AutoML in SW") {
    val automl = new H2OAutoML()
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setMaxModels(3)
      .setKeepBinaryModels(true)
    automl.fit(dataset)
    val binaryModel = automl.getBinaryModel()
    assert(H2OBinaryModel.exists(binaryModel.modelId))
  }
}
