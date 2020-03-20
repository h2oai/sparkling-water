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

import ai.h2o.sparkling.ml.algos.H2OGBM
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.h2o._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkFiles, mllib}
import water.support._

/**
  * This application use word2vec to build a model
  * classifying job offers at Craigslist.
  */
class CraigslistJobTitlesApp(jobsFile: String = TestUtils.locate("smalldata/craigslistJobTitles.csv"))
                            (spark: SparkSession, h2oContext: H2OContext)
  extends ModelMetricsSupport with H2OFrameSupport with Serializable {

  // Import companion object methods
  import CraigslistJobTitlesApp._

  // Build models and predict a few job titles
  def run(): Unit = {
    // Get training frame and word to vec model for data
    val (gbmModel, w2vModel) = buildModels(jobsFile, "modelX")

    println(show(predict("school teacher having holidays every month", gbmModel, w2vModel)))
    println(show(predict("developer with 3+ Java experience, jumping", gbmModel, w2vModel)))
    println(show(predict("Financial accountant CPA preferred", gbmModel, w2vModel)))
  }

  def buildModels(datafile: String = jobsFile, modelName: String): (H2OMOJOModel, Word2VecModel) = {
    val (allDataFrame, w2vModel) = createH2OFrame(datafile)

    val gbm = new H2OGBM()
      .setModelId(modelName)
      .setNtrees(50)
      .setSplitRatio(0.8)
      .setMaxDepth(6)
      .setDistribution("AUTO")
      .setColumnsToCategorical("category")
      .setLabelCol("category")
      .setWithDetailedPredictionCol(true)
    val model = gbm.fit(allDataFrame)

    (model, w2vModel)
  }

  def predict(jobTitle: String, model: H2OMOJOModel, w2vModel: Word2VecModel): (String, Map[String, Double]) = {
    val tokens = tokenize(jobTitle, STOP_WORDS)
    val vec = wordsToVector(tokens, w2vModel)

    import spark.implicits._
    val frameToPredict = spark.sparkContext.parallelize(Seq(vec)).map(v => JobOffer(null, v)).toDF()
    val predictionDF = model.transform(frameToPredict.drop(frameToPredict.columns(0)))
    // Read predicted category
    val predictedCategory = predictionDF.select("prediction").head().getString(0)
    // Read probabilities for each category
    val probs = predictionDF.select("detailed_prediction.probabilities").head().getMap[String, Double](0).toMap
    (predictedCategory, probs)
  }

  def createH2OFrame(datafile: String): (DataFrame, Word2VecModel) = {
    // Load data from specified file
    val dataRdd = loadData(datafile)

    // Compute rare words
    val tokenizedRdd = dataRdd.map(d => (d(0), tokenize(d(1), STOP_WORDS)))
      .filter(s => s._2.length > 0)
    // Compute rare words
    val rareWords = computeRareWords(tokenizedRdd.map(r => r._2))
    // Filter all rare words
    val filteredTokenizedRdd = tokenizedRdd.map(row => {
      val tokens = row._2.filterNot(token => rareWords.contains(token))
      (row._1, tokens)
    }).filter(row => row._2.length > 0)

    // Extract words only to create Word2Vec model
    val words = filteredTokenizedRdd.map(v => v._2.toSeq)

    // Create a Word2Vec model
    val w2vModel = new Word2Vec().fit(words)

    // Sanity Check
    w2vModel.findSynonyms("teacher", 5).foreach(println)
    // Create vectors

    val finalRdd = filteredTokenizedRdd.map(row => {
      val label = row._1
      val tokens = row._2
      // Compute vector for given list of word tokens, unknown words are ignored
      val vec = wordsToVector(tokens, w2vModel)
      JobOffer(label, vec)
    })

    import spark.implicits._
    (finalRdd.toDF(), w2vModel)
  }

  def computeRareWords(dataRdd: RDD[Array[String]]): Set[String] = {
    // Compute frequencies of words
    val wordCounts = dataRdd.flatMap(s => s).map(w => (w, 1)).reduceByKey(_ + _)

    // Collect rare words
    val rareWords = wordCounts.filter { case (k, v) => v < 2 }
      .map { case (k, v) => k }
      .collect
      .toSet
    rareWords
  }

  // Load data via Spark API
  private def loadData(filename: String): RDD[Array[String]] = {
    val sc = spark.sparkContext
    sc.addFile(filename)
    val f = new File(filename).getName
    val data = sc.textFile("file://" + SparkFiles.get(f))
      .filter(line => !line.contains("category")).map(_.split(','))
    data
  }
}

/**
  * Representation of single job offer with its classification.
  *
  * @param category job category (education, labor, ...)
  * @param fv       feature vector describing job title
  */
case class JobOffer(category: String, fv: mllib.linalg.Vector)

object CraigslistJobTitlesApp{

  val EMPTY_PREDICTION = ("NA", Array[Double]())

  val STOP_WORDS = Set("ax", "i", "you", "edu", "s", "t", "m", "subject", "can", "lines", "re", "what"
    , "there", "all", "we", "one", "the", "a", "an", "of", "or", "in", "for", "by", "on"
    , "but", "is", "in", "a", "not", "with", "as", "was", "if", "they", "are", "this", "and", "it", "have"
    , "from", "at", "my", "be", "by", "not", "that", "to", "from", "com", "org", "like", "likes", "so")


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CraigslistJobTitlesApp").getOrCreate()
    val hc = H2OContext.getOrCreate()

    val app = new CraigslistJobTitlesApp(TestUtils.locate("smalldata/craigslistJobTitles.csv"))(spark, hc)
    try {
      app.run()
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    } finally {
      spark.stop()
      hc.stop()
    }
  }

  private[h2o] def tokenize(line: String, stopWords: Set[String]) = {
    //get rid of nonWords such as punctuation as opposed to splitting by just " "
    line.split("""\W+""")
      // Unify
      .map(_.toLowerCase)

      //remove mix of words+numbers
      .filter(word => !(word exists Character.isDigit))

      //remove stopwords defined above (you can add to this list if you want)
      .filterNot(word => stopWords.contains(word))

      //leave only words greater than 1 characters.
      //this deletes A LOT of words but useful to reduce our feature-set
      .filter(word => word.size >= 2)
  }

  // Make some helper functions
  private def sumArray(m: Array[Double], n: Array[Double]): Array[Double] = {
    for (i <- m.indices) {
      m(i) += n(i)
    }
    m
  }

  private def divArray(m: Array[Double], divisor: Double): Array[Double] = {
    for (i <- m.indices) {
      m(i) /= divisor
    }
    m
  }

  private[h2o] def wordsToVector(words: Array[String], model: Word2VecModel): Vector = {
    val vec = Vectors.dense(
      divArray(
        words.map(word => wordToVector(word, model).toArray).reduceLeft(sumArray),
        words.length))
    vec
  }

  private def wordToVector(word: String, model: Word2VecModel): Vector = {
    try {
      model.transform(word)
    } catch {
      case _: Exception => Vectors.zeros(100)
    }
  }

  def show(pred: (String, Map[String, Double])): String = {
    pred._1 + ": " + pred._2.mkString("[", ":", "]")
  }
}
