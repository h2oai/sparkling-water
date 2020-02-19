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

import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, MapType, StringType, StructField, StructType}
import org.scalatest.{FunSuite, Matchers}
import water.api.TestUtils

class AnomalyPredictionTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  private def loadMojo(settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojoName = "isolation_forest_prostate.mojo"
    val mojo = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream(mojoName),
      mojoName,
      settings)
    mojo
  }

  test("transformSchema with detailed prediction col") {
    val model = loadMojo(H2OMOJOSettings(withDetailedPredictionCol = true))

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", DoubleType, nullable = true)
    val scoreField = StructField("score", DoubleType, nullable = false)
    val normalizedScoreField = StructField("normalizedScore", DoubleType, nullable = false)
    val detailedPredictionColField =
      StructField("detailed_prediction", StructType(scoreField :: normalizedScoreField :: Nil), nullable = true)

    val expectedSchema = StructType(datasetFields ++ (detailedPredictionColField :: predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    schema shouldEqual expectedSchema
    schema shouldEqual expectedSchemaByTransform
  }

  test("transformSchema without detailed prediction col") {
    val model = loadMojo(H2OMOJOSettings(withDetailedPredictionCol = false))

    val datasetFields = dataset.schema.fields
    val predictionColField = StructField("prediction", DoubleType, nullable = true)

    val expectedSchema = StructType(datasetFields ++ (predictionColField :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)

    schema shouldEqual expectedSchema
    schema shouldEqual expectedSchemaByTransform
  }
}
