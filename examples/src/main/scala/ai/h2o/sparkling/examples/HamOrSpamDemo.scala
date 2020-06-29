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

import ai.h2o.sparkling.ml.algos._
import ai.h2o.sparkling.ml.features.ColumnPruner
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.feature.{HashingTF, IDF, _}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object HamOrSpamDemo {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Ham or Spam Pipeline Demo")
      .getOrCreate()

    val smsDataPath = "./examples/smalldata/smsData.txt"
    val smsDataFile = s"file://${new File(smsDataPath).getAbsolutePath}"
    val data = load(spark, smsDataFile)
    H2OContext.getOrCreate()
    val tokenizer = createTokenizer()
    val stopWordsRemover = createStopWordsRemover(tokenizer)
    val hashingTF = createHashingTF(stopWordsRemover)
    val idf = createIDF(hashingTF)
    val columnPruner = createColumnPruner(idf, hashingTF, stopWordsRemover, tokenizer)
    val estimators = Array(gbm(), deepLearning(), autoML(), gridSearch(), xgboost())
    estimators.foreach { estimator =>
      val stages = Array(tokenizer, stopWordsRemover, hashingTF, idf, estimator, columnPruner)
      val pipeline = createPipeline(stages)
      val model = trainPipeline(spark, pipeline, data)
      assertPredictions(spark, model)
    }
  }

  def createPipeline(stages: Array[_ <: PipelineStage]): Pipeline = {
    val pipeline = new Pipeline().setStages(stages)
    // Test exporting and importing the pipeline
    pipeline.write.overwrite.save("examples/build/pipeline")
    Pipeline.load("examples/build/pipeline")
  }

  def assertPredictions(spark: SparkSession, model: PipelineModel): Unit = {
    assert(!isSpam(spark, "Michal, h2oworld party tonight in MV?", model))
    assert(
      isSpam(
        spark,
        "We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?",
        model))
  }

  def isSpam(spark: SparkSession, smsText: String, model: PipelineModel): Boolean = {
    val smsTextSchema = StructType(Array(StructField("text", StringType, nullable = false)))
    val smsTextRowRDD = spark.sparkContext.parallelize(Seq(smsText)).map(Row(_))
    val smsTextDF = spark.createDataFrame(smsTextRowRDD, smsTextSchema)
    val prediction = model.transform(smsTextDF)
    prediction.select("prediction").first.getString(0) == "spam"
  }

  def load(spark: SparkSession, dataFile: String): DataFrame = {
    val smsSchema = StructType(
      Array(StructField("label", StringType, nullable = false), StructField("text", StringType, nullable = false)))
    val rowRDD =
      spark.sparkContext.textFile(dataFile).map(_.split("\t", 2)).filter(r => !r(0).isEmpty).map(p => Row(p(0), p(1)))
    spark.createDataFrame(rowRDD, smsSchema)
  }

  def createTokenizer(): RegexTokenizer = {
    new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("words")
      .setMinTokenLength(3)
      .setGaps(false)
      .setPattern("[a-zA-Z]+")
  }

  def createStopWordsRemover(tokenizer: RegexTokenizer): StopWordsRemover = {
    new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered")
      .setStopWords(Array("the", "a", "", "in", "on", "at", "as", "not", "for"))
      .setCaseSensitive(false)
  }

  def createHashingTF(stopWordsRemover: StopWordsRemover): HashingTF = {
    new HashingTF()
      .setNumFeatures(1 << 10)
      .setInputCol(stopWordsRemover.getOutputCol)
      .setOutputCol("wordToIndex")
  }

  def createIDF(hashingTF: HashingTF): IDF = {
    new IDF().setMinDocFreq(4).setInputCol(hashingTF.getOutputCol).setOutputCol("tf_idf")
  }

  def createColumnPruner(
      idf: IDF,
      hashingTF: HashingTF,
      stopWordsRemover: StopWordsRemover,
      tokenizer: RegexTokenizer): ColumnPruner = {
    new ColumnPruner().setColumns(
      Array[String](idf.getOutputCol, hashingTF.getOutputCol, stopWordsRemover.getOutputCol, tokenizer.getOutputCol))
  }

  def trainPipeline(spark: SparkSession, pipeline: Pipeline, data: DataFrame): PipelineModel = {
    val model = pipeline.fit(data)
    // Test exporting and importing pipeline model
    model.write.overwrite.save("build/examples/model")
    PipelineModel.load("build/examples/model")
  }

  def gbm(): H2OGBM = {
    new H2OGBM().setSplitRatio(0.8).setSeed(1).setFeaturesCols("tf_idf").setLabelCol("label")
  }

  def deepLearning(): H2ODeepLearning = {
    new H2ODeepLearning()
      .setEpochs(10)
      .setL1(0.001)
      .setL2(0.0)
      .setSeed(1)
      .setHidden(Array[Int](200, 200))
      .setFeaturesCols("tf_idf")
      .setLabelCol("label")
  }

  def autoML(): H2OAutoML = {
    new H2OAutoML()
      .setLabelCol("label")
      .setSeed(1)
      .setMaxRuntimeSecs(60 * 100)
      .setMaxModels(10)
      .setConvertUnknownCategoricalLevelsToNa(true)
  }

  def gridSearch(): H2OGridSearch = {
    val hyperParams = Map("_ntrees" -> Array(1, 30).map(_.asInstanceOf[AnyRef]))
    val algo = new H2OGBM()
      .setMaxDepth(6)
      .setSeed(1)
      .setFeaturesCols("tf_idf")
      .setLabelCol("label")
      .setConvertUnknownCategoricalLevelsToNa(true)
    new H2OGridSearch()
      .setHyperParameters(hyperParams)
      .setAlgo(algo)
  }

  def xgboost(): H2OXGBoost = {
    new H2OXGBoost().setFeaturesCols("tf_idf").setLabelCol("label").setConvertUnknownCategoricalLevelsToNa(true)
  }
}
