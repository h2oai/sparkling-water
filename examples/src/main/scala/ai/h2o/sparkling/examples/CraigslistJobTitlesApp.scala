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

package ai.h2o.sparkling.examples

import java.io.File

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.algos.H2OGBM
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, Word2Vec}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CraigslistJobTitlesApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Craigslist Job Titles")
      .getOrCreate()

    val titlesTable = loadTitlesTable(spark)
    val model = fitModelPipeline(titlesTable)

    show(predictAndAssert(spark, "school teacher having holidays every month", model, "education"))
    show(predictAndAssert(spark, "Financial accountant CPA preferred", model, "accounting"))
  }

  def loadTitlesTable(spark: SparkSession): DataFrame = {
    val titlesDataPath = "./examples/smalldata/craigslistJobTitles.csv"
    val titlesDataFile = s"file://${new File(titlesDataPath).getAbsolutePath}"
    spark.read.option("inferSchema", "true").option("header", "true").csv(titlesDataFile)
  }

  def fitModelPipeline(train: DataFrame): PipelineModel = {
    val tokenizer = new RegexTokenizer()
      .setInputCol("jobtitle")
      .setOutputCol("tokenized")
      .setMinTokenLength(2)
      .setGaps(false)
      .setPattern("[a-zA-Z]+")

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("jobtitles_tokenized")
      .setCaseSensitive(false)

    val word2Vec = new Word2Vec()
      .setInputCol(stopWordsRemover.getOutputCol)
      .setOutputCol("word2vec")

    H2OContext.getOrCreate()
    val gbm = new H2OGBM()
      .setFeaturesCol(word2Vec.getOutputCol)
      .setNtrees(50)
      .setSplitRatio(0.8)
      .setMaxDepth(6)
      .setDistribution("AUTO")
      .setColumnsToCategorical("category")
      .setLabelCol("category")

    val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, word2Vec, gbm))
    pipeline.fit(train)
  }

  def predictAndAssert(
      spark: SparkSession,
      jobTitle: String,
      model: PipelineModel,
      expected: String): (String, Map[String, Double]) = {
    val prediction = predict(spark, jobTitle, model)
    assert(prediction._1 == expected, s"Expected category was: ${expected}, but predicted: ${prediction._1}")
    prediction
  }

  def predict(spark: SparkSession, jobTitle: String, model: PipelineModel): (String, Map[String, Double]) = {
    val titleSchema = StructType(Array(StructField("jobtitle", StringType, nullable = false)))
    val titleRDD = spark.sparkContext.parallelize(Seq(jobTitle)).map(Row(_))
    val titleDF = spark.createDataFrame(titleRDD, titleSchema)
    val prediction = model.transform(titleDF)
    val predictedCategory = prediction.select("prediction").head().getString(0)
    val probabilitiesDF = prediction.select("detailed_prediction.probabilities.*")
    val probabilityNames = probabilitiesDF.schema.fields.map(_.name)
    val probabilityValues = probabilitiesDF.head().toSeq.map(_.asInstanceOf[Double])
    val probabilities = probabilityNames.zip(probabilityValues).toMap

    (predictedCategory, probabilities)
  }

  def show(pred: (String, Map[String, Double])): Unit = {
    println(pred._1 + ": " + pred._2.mkString("\n[", "\n ", "]\n"))
  }
}
