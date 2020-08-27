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

import java.io.File

import ai.h2o.sparkling.ml.features.{ColumnPruner, H2OWord2VecTokenizer}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OWord2VecTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private def loadTitlesTable(spark: SparkSession): DataFrame = {
    val titlesDataPath = "./examples/smalldata/craigslistJobTitles.csv"
    val titlesDataFile = s"file://${new File(titlesDataPath).getAbsolutePath}"
    spark.read.option("inferSchema", "true").option("header", "true").csv(titlesDataFile)
  }

  private def stopWords= Array("ax","i","you","edu","s","t","m","subject","can",
  "lines","re","what","there","all","we","one","the",
  "a","an","of","or","in","for","by","on","but","is",
  "in","a","not","with","as","was","if","they","are",
  "this","and","it","have","from","at","my","be","by",
  "not","that","to","from","com","org","like","likes",
  "so")
  private lazy val Array(trainingDataset, testingDataset) = loadTitlesTable(spark).randomSplit(Array(0.9, 0.1), 42)

  test("H2OWord2Vec Pipeline serialization and deserialization") {
    val tokenizedDataset = new H2OWord2VecTokenizer()
      .setInputCol("jobtitle")
      .setOutputCol("tokenized")
      .transform(trainingDataset)
      .select("tokenized")

    val algo = new H2OWord2Vec()
      .setSentSampleRate(0)
      .setEpochs(10)

    val pipeline = new Pipeline().setStages(Array(algo))
    pipeline.write.overwrite().save("ml/build/w2v_pipeline")
    val loadedPipeline = Pipeline.load("ml/build/w2v_pipeline")
    val model = loadedPipeline.fit(tokenizedDataset)
    val expected = model.transform(tokenizedDataset)
  }

  test("Basic Word2Vec") {
    val tokenizer = new H2OWord2VecTokenizer()
      .setStopWords(stopWords)
      .setMinTokenLength(2)
      .setPattern("\\w+")
      .setInputCol("jobtitle")
      .setOutputCol("tokenized")

    val w2v = new H2OWord2Vec()
      .setSentSampleRate(0)
      .setEpochs(10)
      .setFeaturesCol(tokenizer.getOutputCol)

    val columnPruner = new ColumnPruner()
      .setColumns(Array(tokenizer.getOutputCol))

    val gbm = new H2OGBM()
      .setFeaturesCols("prediction") // output column of w2v
      .setLabelCol("category")


    val tokenized = tokenizer.transform(trainingDataset)
    val afterw2v = w2v.fit(tokenized)
    val afterw2vData = afterw2v.transform(tokenized)
    val afterPruning = columnPruner.transform(afterw2vData)

    val pipeline = new Pipeline().setStages(Array(tokenizer, w2v, columnPruner, gbm))

    val model = pipeline.fit(trainingDataset)
    val predictions = model.transform(testingDataset)
    val a = 1
  }

  test("Word2Vec with pre-trained model") {}

  test("Multiple columns not allowed") {}
}
