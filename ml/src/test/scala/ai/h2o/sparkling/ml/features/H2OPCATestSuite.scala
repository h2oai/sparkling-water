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
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OPCATestSuite extends FunSuite with Matchers with SharedH2OTestContext {

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
    val algo = new H2OPCA()
      .setSeed(1)
      .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setOutputCol("Output")
      .setSplitRatio(0.8)
      .setImputeMissing(true)
      .setK(3)

    algo.fit(trainingDataset)
  }

  // Support for Spark 2.1 will be removed in SW 3.34. Tests are ignored due to a bug in Vector comparison in Spark 2.1:
  // https://issues.apache.org/jira/browse/SPARK-19425
  if (!createSparkSession().version.startsWith("2.1")) {

    test("The standalone PCA model produces different results for various input rows.") {
      val scored = standaloneModel.transform(testingDataset)
      val rows = scored.take(2)

      val first = rows(0).getAs[DenseVector]("Output").values.toSeq
      val second = rows(1).getAs[DenseVector]("Output").values.toSeq

      first.length should be(3)
      second.length should be(3)
      first should not equal second
    }

    test("The standalone PCA model can provide scoring history") {
      val expectedColumns = Array("Timestamp", "Duration", "Iterations")

      val scoringHistoryDF = standaloneModel.getScoringHistory()
      scoringHistoryDF.show(false) // TODO: Just one iteration
      scoringHistoryDF.count() shouldBe >(0L)
      scoringHistoryDF.columns shouldEqual expectedColumns
    }

    test("The standalone PCA can produce training and validation metrics") {
      val trainingMetrics = standaloneModel.getTrainingMetrics()
      println(trainingMetrics) // TODO: Why Map(MSE -> NaN, RMSE -> NaN)?

      val validationMetrics = standaloneModel.getValidationMetrics()
      println(validationMetrics)
    }

    test("The PCA model is able to transform dataset after it's saved and loaded") {
      val pca = new H2OPCA()
        .setSeed(1)
        .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
        .setK(5)
        .setImputeMissing(true)
        .setSplitRatio(0.8)

      val pipeline = new Pipeline().setStages(Array(pca))

      val model = pipeline.fit(trainingDataset)
      val expectedTestingDataset = model.transform(testingDataset)
      val path = "build/ml/pca_save_load"
      model.write.overwrite().save(path)
      val loadedModel = PipelineModel.load(path)
      val transformedTestingDataset = loadedModel.transform(testingDataset)

      TestUtils.assertDataFramesAreIdentical(expectedTestingDataset, transformedTestingDataset)
    }

    test(
      "A pipeline with a PCA model sourcing data from multiple columns transforms testing dataset without an exception") {
      val pca = new H2OPCA()
        .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
        .setK(4)
        .setImputeMissing(true)

      val gbm = new H2OGBM()
        .setFeaturesCol(pca.getOutputCol())
        .setLabelCol("CAPSULE")

      val pipeline = new Pipeline().setStages(Array(pca, gbm))

      val model = pipeline.fit(trainingDataset)
      val numberOfPredictionsDF = model.transform(testingDataset).groupBy("prediction").count()
      val rows = numberOfPredictionsDF.collect()
      numberOfPredictionsDF.count() shouldBe >=(2L)
      rows.foreach { row =>
        assert(row.getAs[Long]("count") > 0, s"No predictions of class '${row.getAs[Int]("prediction")}'")
      }
    }

    test("A pipeline with a PCA model sourcing data from vector column transforms testing dataset without an exception") {
      val autoEncoder = new H2OAutoEncoder()
        .setInputCols("RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
        .setHidden(Array(100))

      val pca = new H2OPCA()
        .setInputCols(autoEncoder.getOutputCol())
        .setK(3)
        .setImputeMissing(true)

      val gbm = new H2OGBM()
        .setFeaturesCol(pca.getOutputCol())
        .setLabelCol("CAPSULE")

      val pipeline = new Pipeline().setStages(Array(autoEncoder, pca, gbm))

      val model = pipeline.fit(trainingDataset)
      val numberOfPredictionsDF = model.transform(testingDataset).groupBy("prediction").count()
      val rows = numberOfPredictionsDF.collect()
      numberOfPredictionsDF.count() shouldBe >=(2L)
      rows.foreach { row =>
        assert(row.getAs[Long]("count") > 0, s"No predictions of class '${row.getAs[Int]("prediction")}'")
      }
    }
  }
}
