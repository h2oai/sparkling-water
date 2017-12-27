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

package org.apache.spark.ml.spark.models

import java.io.{File, PrintWriter}

import org.apache.spark.h2o.utils.{H2OContextTestHelper, SparkTestContext}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.h2o.algos.H2OGBM
import org.apache.spark.ml.h2o.features.ColumnPruner
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkContext, SparkFiles}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils
import water.support.SparkContextSupport

object TestPipelineUtils {

  // This method loads the data, perform some basic filtering and create Spark's dataframe
  def load(sc: SparkContext, dataFile: String)(implicit sqlContext: SQLContext): DataFrame = {
    val smsSchema = StructType(Array(
      StructField("label", StringType, nullable = false),
      StructField("text", StringType, nullable = false)))
    val rowRDD = sc.textFile(SparkFiles.get(dataFile)).map(_.split("\t", 2)).filter(r => !r(0).isEmpty).map(p => Row(p(0), p(1)))
    sqlContext.createDataFrame(rowRDD, smsSchema)
  }

  def trainedPipelineModel(spark: SparkSession) = {
    implicit val hc = H2OContextTestHelper.createH2OContext(spark.sparkContext, 3)
    implicit val sqlContext = spark.sqlContext
    /**
      * Define the pipeline stages
      */
    // Tokenize the messages
    val tokenizer = new RegexTokenizer().
      setInputCol("text").
      setOutputCol("words").
      setMinTokenLength(3).
      setGaps(false).
      setPattern("[a-zA-Z]+")

    // Remove ignored words
    val stopWordsRemover = new StopWordsRemover().
      setInputCol(tokenizer.getOutputCol).
      setOutputCol("filtered").
      setStopWords(Array("the", "a", "", "in", "on", "at", "as", "not", "for")).
      setCaseSensitive(false)

    // Hash the words
    val hashingTF = new HashingTF().
      setNumFeatures(1 << 10).
      setInputCol(stopWordsRemover.getOutputCol).
      setOutputCol("wordToIndex")

    // Create inverse document frequencies model
    val idf = new IDF().
      setMinDocFreq(4).
      setInputCol(hashingTF.getOutputCol).
      setOutputCol("tf_idf")

    // Create GBM model
    val gbm = new H2OGBM().
      setTrainRatio(0.8).
      setSeed(42).
      setFeaturesCols("tf_idf").
      setPredictionsCol("label")

    // Remove all intermediate columns
    val colPruner = new ColumnPruner().
      setColumns(Array[String](idf.getOutputCol, hashingTF.getOutputCol, stopWordsRemover.getOutputCol, tokenizer.getOutputCol))

    // Create the pipeline by defining all the stages
    val pipeline = new Pipeline().
      setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, gbm, colPruner))

    // Train the pipeline model
    val data = load(spark.sparkContext, "smsData.txt")
    val model = pipeline.fit(data)

    H2OContextTestHelper.stopH2OContext(spark.sparkContext, hc)
    // return the trained model
    model
  }

}

@RunWith(classOf[JUnitRunner])
class PipelinePredictionTest extends FunSuite with SparkTestContext {

  override def beforeAll(): Unit = {
    sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
    super.beforeAll()
  }

  /**
    * This test is not using H2O runtime since we are testing deployment of the pipeline
    */
  test("Run predictions on Spark pipeline model containing H2O Mojo") {

    //
    // Load exported pipeline
    //
    val model_path = getClass.getResource("/sms_pipeline_deployment/sms_pipeline.model")
    val pipelineModel = PipelineModel.read.load(model_path.getFile)

    //
    // Define input stream
    //
    val smsDataFileName = "smsData.txt"
    val smsDataFilePath = TestUtils.locate(s"smalldata/$smsDataFileName")
    SparkContextSupport.addFiles(sc, smsDataFilePath)

    val inputDataStream = TestPipelineUtils.load(sc, "smsData.txt")

    //
    // Run predictions on the loaded model which was trained in PySparkling pipeline
    //
    val predictions1 = pipelineModel.transform(inputDataStream)

    //
    // UNTIL NOW, RUNTIME WAS NOT AVAILABLE
    //
    // Run predictions on the trained model right now in Scala
    val predictions2 = TestPipelineUtils.trainedPipelineModel(spark).transform(inputDataStream)

    TestUtils.assertEqual(predictions1, predictions2)
  }
}

@RunWith(classOf[JUnitRunner])
class StreamingPipelinePredictionTest extends FunSuite with SparkTestContext {

  override def beforeAll(): Unit = {
    sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
    super.beforeAll()
  }

  test("Test streaming pipeline with H2O MOJO") {
    //
    val model_path = getClass.getResource("/sms_pipeline_deployment/sms_pipeline.model")
    val pipelineModel = PipelineModel.read.load(model_path.getFile)

    //
    // Define input data
    //
    val smsDataFileName = "smsData.txt"
    val smsDataFilePath = TestUtils.locate(s"smalldata/$smsDataFileName")
    SparkContextSupport.addFiles(sc, smsDataFilePath)

    // This directory is automatically deleted when the JVM shuts down
    val streamingDataDir = Utils.createTempDir()

    val data = TestPipelineUtils.load(sc, "smsData.txt")
    // Create data for streaming input
    data.select("text").collect().zipWithIndex.foreach { case (r, idx) =>
      val printer = new PrintWriter(new File(streamingDataDir, s"$idx.txt"))
      printer.write(r.getString(0))
      printer.close()
    }
    val schema = StructType(Seq(StructField("text", StringType)))
    val inputDataStream = spark.readStream.schema(schema).text(streamingDataDir.getAbsolutePath)

    val outputDataStream = pipelineModel.transform(inputDataStream)
    outputDataStream.writeStream.format("memory").queryName("predictions").start()

    //
    // Run predictions on the loaded model which was trained in PySparkling pipeline
    //
    var predictions1 = spark.sql("select * from predictions")

    while (predictions1.count() != 1324) { // The file has 380 entries
      Thread.sleep(1000)
      predictions1 = spark.sql("select * from predictions")
    }

    //
    // UNTIL NOW, RUNTIME WAS NOT AVAILABLE
    //
    // Run predictions on the trained model right now in Scala
    val predictions2 = TestPipelineUtils.trainedPipelineModel(spark).transform(data)

    TestUtils.assertEqual(predictions1, predictions2)
  }

}


