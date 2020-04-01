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

package ai.h2o.sparkling.ml

import ai.h2o.sparkling.SharedH2OTestContext
import ai.h2o.sparkling.ml.algos.H2OGBM
import ai.h2o.sparkling.ml.features.ColumnPruner
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkContext, SparkFiles}
import org.scalatest.FunSuite

abstract class PipelinePredictionTestBase extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  // This method loads the data, perform some basic filtering and create Spark's dataframe
  def load(sc: SparkContext, dataFile: String): DataFrame = {
    val smsSchema = StructType(
      Array(StructField("label", StringType, nullable = false), StructField("text", StringType, nullable = false)))
    val rowRDD =
      sc.textFile(SparkFiles.get(dataFile)).map(_.split("\t", 2)).filter(r => !r(0).isEmpty).map(p => Row(p(0), p(1)))
    spark.createDataFrame(rowRDD, smsSchema)
  }

  def trainedPipelineModel(spark: SparkSession): PipelineModel = {

    /**
      * Define the pipeline stages
      */
    // Tokenize the messages
    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setMinTokenLength(3)
      .setGaps(false)
      .setPattern("[a-zA-Z]+")

    // Remove ignored words
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered")
      .setStopWords(Array("the", "a", "", "in", "on", "at", "as", "not", "for"))
      .setCaseSensitive(false)

    // Hash the words
    val hashingTF = new HashingTF()
      .setNumFeatures(1 << 10)
      .setInputCol(stopWordsRemover.getOutputCol)
      .setOutputCol("wordToIndex")

    // Create inverse document frequencies model
    val idf = new IDF()
      .setMinDocFreq(4)
      .setInputCol(hashingTF.getOutputCol)
      .setOutputCol("tf_idf")

    // Create GBM model
    val gbm = new H2OGBM()
      .setSplitRatio(0.8)
      .setSeed(42)
      .setFeaturesCols("tf_idf")
      .setLabelCol("label")

    // Remove all intermediate columns
    val colPruner = new ColumnPruner()
      .setColumns(
        Array[String](idf.getOutputCol, hashingTF.getOutputCol, stopWordsRemover.getOutputCol, tokenizer.getOutputCol))

    // Create the pipeline by defining all the stages
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, gbm, colPruner))

    // Train the pipeline model
    val data = load(spark.sparkContext, "smsData.txt")
    val model = pipeline.fit(data)
    model
  }
}
