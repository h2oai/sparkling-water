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
import org.apache.spark.sql.functions.col
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OUpliftDRFTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private val predictorColumns = Array("f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8")
  private val responseColumn = "conversion"
  private val treatmentColumn = "treatment"

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/uplift/criteo_uplift_1k.csv"))
    .withColumn(treatmentColumn, col(treatmentColumn).cast("string"))
    .withColumn(responseColumn, col(responseColumn).cast("string"))

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.8, 0.2), 1234)

  private lazy val algo = new H2OUpliftDRF()
    .setNtrees(10)
    .setMaxDepth(5)
    .setTreatmentCol(treatmentColumn)
    .setUpliftMetric("KL")
    .setMinRows(10)
    .setSeed(1234)
    .setAuucType("qini")
    .setLabelCol(responseColumn)
    .setFeaturesCols(predictorColumns :+ treatmentColumn :+ responseColumn)

  test("H2OUpliftDRF happy path") {
    val model = algo.fit(trainingDataset)
    val result = model.transform(testingDataset)
    result.columns should contain theSameElementsAs predictorColumns ++ Seq(
      responseColumn,
      treatmentColumn,
      "f0",
      "f9",
      "f10",
      "f11",
      "visit",
      "exposure",
      "detailed_prediction",
      "prediction")
    result.count() shouldEqual 205
    result.select("prediction").collect().head.getSeq[Double](0) shouldBe Seq(
      -0.04689146913588047,
      0.04408255908638239,
      0.09097402822226286)
  }

  test("H2OUpliftDRF Pipeline serialization and deserialization") {
    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/UpliftDRF_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/UpliftDRF_pipeline")
    val model = loadedPipeline.fit(trainingDataset)

    model.write.overwrite().save("ml/build/UpliftDRF_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/UpliftDRF_pipeline_model")

    loadedModel.transform(testingDataset).count()
  }

}
