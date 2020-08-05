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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.bround
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OGLRMTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))
    .drop("class")

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.9, 0.1), 42)

  test("H2OGLRM Pipeline serialization and deserialization") {
    import spark.implicits._

    val algo = new H2OGLRM()
      .setK(3)
      .setLoss("Quadratic")
      .setGammaX(0.5)
      .setGammaY(0.5)
      .setSeed(42)
      .setTransform("standardize")

    def roundResult(dataFrame: DataFrame): DataFrame = {
      dataFrame.select(
        'sepal_len,
        'sepal_wid,
        'petal_len,
        'petal_wid,
        bround($"prediction".getItem(0), 5) as "prediction0",
        bround($"prediction".getItem(1), 5) as "prediction1",
        bround($"prediction".getItem(2), 5) as "prediction2")
    }

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/glrm_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/glrm_pipeline")
    val model = loadedPipeline.fit(trainingDataset)
    val expected = roundResult(model.transform(testingDataset))

    model.write.overwrite().save("ml/build/glrm_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/glrm_pipeline_model")
    val result = roundResult(loadedModel.transform(testingDataset))

    TestUtils.assertDataFramesAreIdentical(expected, result)
  }
}
