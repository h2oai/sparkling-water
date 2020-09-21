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
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OWord2VecTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/craigslistJobTitles.csv"))

  private lazy val Array(trainingDataset, testingDataset) = dataset.randomSplit(Array(0.9, 0.1), 42)

  def getPipeline(): Pipeline = {
    val tokenizer = new RegexTokenizer()
      .setInputCol("jobtitle")
      .setMinTokenLength(2)

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)

    val w2v = new H2OWord2Vec()
      .setSentSampleRate(0)
      .setEpochs(10)
      .setInputCol(stopWordsRemover.getOutputCol)

    val gbm = new H2OGBM()
      .setLabelCol("category")
      .setFeaturesCols(w2v.getOutputCol)

    new Pipeline().setStages(Array(tokenizer, stopWordsRemover, w2v, gbm))
  }

  test("H2OWord2Vec Pipeline serialization and deserialization") {
    getPipeline().write.overwrite().save("ml/build/w2v_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/w2v_pipeline")
    val model = loadedPipeline.fit(trainingDataset)
    val expected = model.transform(testingDataset)

    model.write.overwrite().save("ml/build/w2v_pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/w2v_pipeline_model")
    val result = loadedModel.transform(testingDataset)
    TestUtils.assertDataFramesAreIdentical(expected, result)
  }

  test("Basic Word2Vec") {
    val model = getPipeline().fit(trainingDataset)
    val result = model.transform(testingDataset)
    import spark.implicits._
    result.select("prediction").map(row => row.getString(0)).take(3)
  }
}
