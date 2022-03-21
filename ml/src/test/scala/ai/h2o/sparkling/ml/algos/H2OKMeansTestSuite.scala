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
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OKMeansTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))

  test("H2OKMeans Pipeline serialization and deserialization") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/kmeans_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/kmeans_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save("ml/build/kmeans_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/kmeans_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  test("H2OKMeans user points") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setUserPoints(Array(Array(4.9, 3.0, 1.4, 0.2), Array(5.6, 2.5, 3.9, 1.1), Array(6.5, 3.0, 5.2, 2.0)))
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    assert(model.transform(dataset).select("prediction").head() == Row(0))
  }

  test("H2OKMeans input feature which is not in the dataset") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("not_exist_1", "not_exist_2")

    val thrown = intercept[IllegalArgumentException] {
      algo.fit(dataset)
    }
    assert(
      thrown.getMessage == "The following feature columns are not available on" +
        " the training dataset: 'not_exist_1, not_exist_2'")
  }
}
