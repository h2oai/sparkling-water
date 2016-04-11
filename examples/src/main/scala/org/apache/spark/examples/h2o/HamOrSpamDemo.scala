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

package org.apache.spark.examples.h2o

import hex.deeplearning.{DeepLearningModel, DeepLearning}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.h2o._
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext, mllib}
import water.app.{ModelMetricsSupport, SparkContextSupport}

/**
 * Demo for NYC meetup and MLConf 2015.
 *
 * It predicts spam text messages.
 * Training dataset is available in the file smalldata/smsData.txt.
 */
object HamOrSpamDemo extends SparkContextSupport with ModelMetricsSupport {

  val DATAFILE="smsData.txt"
  val TEST_MSGS = Seq(
    "Michal, beer tonight in MV?",
    "We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?")

  def main(args: Array[String]) {
    val conf: SparkConf = configure("Sparkling Water Meetup: Ham or Spam (spam text messages detector)")
    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    // Register input file as Spark file
    addFiles(sc, absPath("examples/smalldata/" + DATAFILE))
    // Initialize H2O context
    implicit val h2oContext = H2OContext.getOrCreate(sc)
    import h2oContext.implicits._
    // Initialize SQL context
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // Data load
    val data = load(sc, DATAFILE)
    // Extract response spam or ham
    val hamSpam = data.map( r => r(0))
    val message = data.map( r => r(1))
    // Tokenize message content
    val tokens = tokenize(message)

    // Build IDF model
    var (hashingTF, idfModel, tfidf) = buildIDFModel(tokens)

    // Merge response with extracted vectors
    val resultRDD: DataFrame = hamSpam.zip(tfidf).map(v => SMS(v._1, v._2)).toDF

    val table:H2OFrame = resultRDD
    // Transform target column into
    table.replace(table.find("target"), table.vec("target").toCategoricalVec).remove()

    // Split table
    val keys = Array[String]("train.hex", "valid.hex")
    val ratios = Array[Double](0.8)
    val frs = split(table, keys, ratios)
    val (train, valid) = (frs(0), frs(1))
    table.delete()

    // Build a model
    val dlModel = buildDLModel(train, valid)

    // Collect model metrics
    val trainMetrics = binomialMM(dlModel, train)
    val validMetrics = binomialMM(dlModel, valid)
    println(
      s"""
         |AUC on train data = ${trainMetrics.auc}
         |AUC on valid data = ${validMetrics.auc}
       """.stripMargin)

    // Detect spam messages
    TEST_MSGS.foreach(msg => {
      println(
        s"""
           |"$msg" is ${if (isSpam(msg,sc, dlModel, hashingTF, idfModel)) "SPAM" else "HAM"}
       """.stripMargin)
    })

    // Shutdown Spark cluster and H2O
    h2oContext.stop(stopSparkContext = true)
  }

  /** Data loader */
  def load(sc: SparkContext, dataFile: String): RDD[Array[String]] = {
    sc.textFile(enforceLocalSparkFile(dataFile)).map(l => l.split("\t")).filter(r => !r(0).isEmpty)
  }

  /** Text message tokenizer.
    *
    * Produce a bag of word representing given message.
    *
    * @param data RDD of text messages
    * @return RDD of bag of words
    */
  def tokenize(data: RDD[String]): RDD[Seq[String]] = {
    val ignoredWords = Seq("the", "a", "", "in", "on", "at", "as", "not", "for")
    val ignoredChars = Seq(',', ':', ';', '/', '<', '>', '"', '.', '(', ')', '?', '-', '\'','!','0', '1')

    val texts = data.map( r=> {
      var smsText = r.toLowerCase
      for( c <- ignoredChars) {
        smsText = smsText.replace(c, ' ')
      }

      val words =smsText.split(" ").filter(w => !ignoredWords.contains(w) && w.length>2).distinct

      words.toSeq
    })
    texts
  }

  /** Buil tf-idf model representing a text message. */
  def buildIDFModel(tokens: RDD[Seq[String]],
                    minDocFreq:Int = 4,
                    hashSpaceSize:Int = 1 << 10): (HashingTF, IDFModel, RDD[mllib.linalg.Vector]) = {
    // Hash strings into the given space
    val hashingTF = new HashingTF(hashSpaceSize)
    val tf = hashingTF.transform(tokens)
    // Build term frequency-inverse document frequency
    val idfModel = new IDF(minDocFreq = minDocFreq).fit(tf)
    val expandedText = idfModel.transform(tf)
    (hashingTF, idfModel, expandedText)
  }

  /** Builds DeepLearning model. */
  def buildDLModel(train: Frame, valid: Frame,
                   epochs: Int = 10, l1: Double = 0.001, l2: Double = 0.0,
                   hidden: Array[Int] = Array[Int](200, 200))
                  (implicit h2oContext: H2OContext): DeepLearningModel = {
    import h2oContext.implicits._
    // Build a model
    val dlParams = new DeepLearningParameters()
    dlParams._train = train
    dlParams._valid = valid
    dlParams._response_column = 'target
    dlParams._epochs = epochs
    dlParams._l1 = l1
    dlParams._hidden = hidden

    // Create a job
    val dl = new DeepLearning(dlParams, water.Key.make("dlModel.hex"))
    dl.trainModel.get
  }

  /** Spam detector */
  def isSpam(msg: String,
             sc: SparkContext,
             dlModel: DeepLearningModel,
             hashingTF: HashingTF,
             idfModel: IDFModel,
             hamThreshold: Double = 0.5)
            (implicit sqlContext: SQLContext, h2oContext: H2OContext):Boolean = {
    import h2oContext.implicits._
    import sqlContext.implicits._
    val msgRdd = sc.parallelize(Seq(msg))
    val msgVector: DataFrame = idfModel.transform(
      hashingTF.transform (
        tokenize (msgRdd))).map(v => SMS("?", v)).toDF
    val msgTable: H2OFrame = msgVector
    msgTable.remove(0) // remove first column
    val prediction = dlModel.score(msgTable)
    //println(prediction)
    prediction.vecs()(1).at(0) < hamThreshold
  }

}

/** Training message representation. */
case class SMS(target: String, fv: mllib.linalg.Vector)

