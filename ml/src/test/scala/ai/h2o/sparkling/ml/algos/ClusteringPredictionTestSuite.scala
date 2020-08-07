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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class ClusteringPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/iris/iris_wheader.csv"))

  test("predictionCol content") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)

    // the 'cluster' from clustering prediction is directly in predictionCol
    assert(transformed.select("prediction").head() == Row(0))
    assert(transformed.select("prediction").distinct().count() == 3)
  }

  test("detailedPredictionCol content") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)
    val transformed = model.transform(dataset)

    val expectedCols = Seq("cluster", "distances")
    assert(transformed.select("detailed_prediction.*").schema.fields.map(_.name).sameElements(expectedCols))

    assert(transformed.select("detailed_prediction.cluster").head().getInt(0) == 0)
    assert(transformed.select("detailed_prediction.distances").head().getAs[Seq[Double]](0).length == 3)
  }

  test("transformSchema") {
    val algo = new H2OKMeans()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setK(3)
      .setFeaturesCols("sepal_len", "sepal_wid", "petal_len", "petal_wid")

    val model = algo.fit(dataset)

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", IntegerType, nullable = true)

    val clusterField = StructField("cluster", IntegerType, nullable = true)
    val distancesField = StructField("distances", ArrayType(DoubleType, containsNull = false), nullable = true)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(clusterField :: distancesField :: Nil), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    assert(schema == expectedSchema)
    assert(schema == expectedSchemaByTransform)
  }
}
