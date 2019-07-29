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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils
import water.exceptions.H2OModelBuilderIllegalArgumentException

@RunWith(classOf[JUnitRunner])
class H2OKMeansTest extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

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

  test("H2OKMeans' result is directly in prediction column") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)
    assert(transformed.select("prediction").head() == Row(0))
    assert(transformed.select("prediction").distinct().count() == 3)
  }

  test("H2OKMeans' full result in prediction details column") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setWithDetailedPredictionCol(true)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)
    assert(transformed.select("detailed_prediction.cluster").head().getInt(0) == 0)
    assert(transformed.select("detailed_prediction.distances").head().getAs[Seq[Double]](0).length == 3)
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

  test("H2OKMeans single string column input without converting to categorical") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("class")
      .setAllStringColumnsToCategorical(false)

    val thrown = intercept[IllegalArgumentException] {
      algo.fit(dataset)
    }
    assert(thrown.getMessage.startsWith("Following columns are of type string: 'class'"))
  }

  test("H2OKMeans string column input with other non-string column") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("class", "sepal_wid")
      .setAllStringColumnsToCategorical(false)

    val thrown = intercept[IllegalArgumentException] {
      algo.fit(dataset)
    }
    assert(thrown.getMessage.startsWith("Following columns are of type string: 'class'"))
  }

  test("H2OKMeans input feature which is not in the dataset") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("not_exist_1", "not_exist_2")
      .setAllStringColumnsToCategorical(false)

    val thrown = intercept[IllegalArgumentException] {
      algo.fit(dataset)
    }
    assert(thrown.getMessage == "The following feature columns are not available on" +
      " the training dataset: 'not_exist_1, not_exist_2'")
  }

  test("H2OKMeans with constant column") {
    import org.apache.spark.sql.functions.lit
    val datasetWithConst = dataset.withColumn("constant", lit(1))
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("constant")
      .setAllStringColumnsToCategorical(false)

    val thrown = intercept[IllegalArgumentException] {
      algo.fit(datasetWithConst)
    }
    assert(thrown.getMessage.startsWith("H2O could not use any of the specified feature" +
      " columns: 'constant'. H2O ignores constant columns, are all the columns constants?"))
  }

}
