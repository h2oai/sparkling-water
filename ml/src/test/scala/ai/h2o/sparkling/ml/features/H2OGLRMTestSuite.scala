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

import ai.h2o.sparkling.ml.algos.H2OGBM
import ai.h2o.sparkling.ml.metrics.{H2OGLRMMetrics, MetricsAssertions}
import ai.h2o.sparkling.ml.models.H2OGLRMMOJOModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OGLRMTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", 'CAPSULE.cast("string"))
    .withColumn("RACE", 'RACE.cast("string"))

  private lazy val trainingDataset = dataset.limit(300).cache()
  private lazy val testingDataset = dataset.except(trainingDataset).cache()

  private lazy val standaloneModel = {
    val algo = new H2OGLRM()
      .setSeed(1)
      .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setOutputCol("Output")
      .setWithReconstructedCol(true)
      .setReconstructedCol("Reconstructed")
      .setSplitRatio(0.5)
      .setK(3)

    algo.fit(trainingDataset)
  }

  test("The standalone GLRM model produces different results for various input rows.") {
    val scored = standaloneModel.transform(testingDataset)
    val rows = scored.take(2)

    val firstOutput = rows(0).getAs[DenseVector]("Output").values.toSeq
    val secondOutput = rows(1).getAs[DenseVector]("Output").values.toSeq

    val firstReconstructed = rows(0).getAs[DenseVector]("Reconstructed").values.toSeq
    val secondReconstructed = rows(1).getAs[DenseVector]("Reconstructed").values.toSeq

    firstOutput.length should be(3)
    secondOutput.length should be(3)

    firstReconstructed.length should be(6)
    secondReconstructed.length should be(6)

    firstOutput should not equal secondOutput
    firstReconstructed should not equal secondReconstructed
  }

  test("The standalone GLRM model can provide scoring history") {
    val expectedColumns = Array("Timestamp", "Duration", "Iterations", "Step Size", "Objective")

    val scoringHistoryDF = standaloneModel.getScoringHistory()

    scoringHistoryDF.count() shouldBe >(10L)
    scoringHistoryDF.columns shouldEqual expectedColumns
  }

  test(
    "A pipeline with a GLRM model sourcing data from multiple columns transforms testing dataset without an exception") {
    val glrm = new H2OGLRM()
      .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setK(4)

    val gbm = new H2OGBM()
      .setFeaturesCol(glrm.getOutputCol())
      .setLabelCol("CAPSULE")

    val pipeline = new Pipeline().setStages(Array(glrm, gbm))

    val model = pipeline.fit(trainingDataset)
    val numberOfPredictionsDF = model.transform(testingDataset).groupBy("prediction").count()
    val rows = numberOfPredictionsDF.collect()
    numberOfPredictionsDF.count() shouldBe >=(2L)
    rows.foreach { row =>
      assert(row.getAs[Long]("count") > 0, s"No predictions of class '${row.getAs[Int]("prediction")}'")
    }
  }

  test("A pipeline with a GLRM model sourcing data from vector column transforms testing dataset without an exception") {
    val autoEncoder = new H2OAutoEncoder()
      .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setHidden(Array(100))

    val glrm = new H2OGLRM()
      .setInputCols(autoEncoder.getOutputCol())
      .setK(3)

    val gbm = new H2OGBM()
      .setFeaturesCol(glrm.getOutputCol())
      .setLabelCol("CAPSULE")

    val pipeline = new Pipeline().setStages(Array(autoEncoder, glrm, gbm))

    val model = pipeline.fit(trainingDataset)
    val numberOfPredictionsDF = model.transform(testingDataset).groupBy("prediction").count()
    val rows = numberOfPredictionsDF.collect()
    numberOfPredictionsDF.count() shouldBe >=(2L)
    rows.foreach { row =>
      assert(row.getAs[Long]("count") > 0, s"No predictions of class '${row.getAs[Int]("prediction")}'")
    }
  }

  private def assertMetrics(model: H2OGLRMMOJOModel): Unit = {
    assertMetrics(model.getTrainingMetricsObject(), model.getTrainingMetrics())
    assertMetrics(model.getValidationMetricsObject(), model.getValidationMetrics())
    assert(model.getCrossValidationMetricsObject() == null)
    assert(model.getCrossValidationMetrics() == Map())
  }

  private def assertMetrics(metricsObject: H2OGLRMMetrics, metrics: Map[String, Double]): Unit = {
    MetricsAssertions.assertMetricsObjectAgainstMetricsMap(metricsObject, metrics)
  }

  test("test metric objects") {
    assertMetrics(standaloneModel)

    standaloneModel.write.overwrite().save("ml/build/glrm_model_metrics")
    val loadedModel = H2OGLRMMOJOModel.load("ml/build/glrm_model_metrics")
    assertMetrics(loadedModel)
  }
}
