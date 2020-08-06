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
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OPCATestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/birds.csv"))

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.9, 0.1), 42)

  test("H2OPCA Pipeline serialization and deserialization") {

    val algo = new H2OPCA()
      .setK(3)
      .setMaxIterations(500)
      .setPcaImpl("MTJ_EVD_SYMMMATRIX")
      .setPcaMethod("GramSVD")
      .setSeed(42)
      .setConvertUnknownCategoricalLevelsToNa(true)
      .setTransform("standardize")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/pca_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/pca_pipeline")
    val model = loadedPipeline.fit(trainingDataset)
    val expected = model.transform(testingDataset)

    model.write.overwrite().save("ml/build/pca_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/pca_pipeline_model")
    val result = loadedModel.transform(testingDataset)

    TestUtils.assertDataFramesAreIdentical(expected, result)
  }
}
