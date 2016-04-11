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

import hex.Model
import hex.Model.Output
import org.apache.spark.h2o._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, mllib}
import water.app.{GBMSupport, ModelMetricsSupport, SparkContextSupport, SparklingWaterApp}

/**
 * This application use word2vec to build a model
 * classifying job offers at Craigslist.
 */
class CraigslistJobTitlesApp(jobsFile: String = "examples/smalldata/craigslistJobTitles.csv")
                            (@transient override val sc: SparkContext,
                              @transient override val sqlContext: SQLContext,
                              @transient override val h2oContext: H2OContext) extends SparklingWaterApp
                            with SparkContextSupport with GBMSupport with ModelMetricsSupport with Serializable {

  // Import companion object methods
  import CraigslistJobTitlesApp._

  // Build models and predict a few job titles
  def run(): Unit = {
    // Get training frame and word to vec model for data
    val (gbmModel, w2vModel) = buildModels(jobsFile, "modelX")

    val classNames = gbmModel._output.asInstanceOf[Output].classNames()
    println(show(predict("school teacher having holidays every month", gbmModel, w2vModel), classNames))
    println(show(predict("developer with 3+ Java experience, jumping", gbmModel, w2vModel), classNames))
    println(show(predict("Financial accountant CPA preferred", gbmModel, w2vModel), classNames))
  }

  def buildModels(datafile: String = jobsFile, modelName: String): (Model[_,_,_], Word2VecModel) = {
    // Get training frame and word to vec model for data
    val (allDataFrame, w2vModel) = createH2OFrame(datafile)
    val frs = DemoUtils.splitFrame(allDataFrame, Array("train.hex", "valid.hex"), Array(0.8, 0.2))
    val (trainFrame, validFrame) = (h2oContext.asH2OFrame(frs(0)), h2oContext.asH2OFrame(frs(1)))

    val gbmModel = GBMModel(trainFrame, validFrame, "category", modelName, ntrees = 50)
    // Cleanup
    Seq(trainFrame, validFrame, allDataFrame).foreach(_.delete)

    (gbmModel, w2vModel)
  }


  def predict(jobTitle: String, modelId: String, w2vModel: Word2VecModel): (String, Array[Double]) = {
    predict(jobTitle, model = water.DKV.getGet(modelId), w2vModel)
  }

  def predict(jobTitle: String, model: Model[_,_,_], w2vModel: Word2VecModel): (String, Array[Double]) = {
    val tokens = tokenize(jobTitle, STOP_WORDS)
    val vec = wordsToVector(tokens, w2vModel)

    // FIXME should use Model#score(double[]) method but it is now wrong and need to be fixed
    import sqlContext.implicits._
    import h2oContext.implicits._
    val frameToPredict: H2OFrame = sc.parallelize(Seq(vec)).map(v => JobOffer(null, v)).toDF
    frameToPredict.remove(0).remove()
    val prediction = model.score(frameToPredict)
    // Read predicted category
    val predictedCategory = prediction.vec(0).factor(prediction.vec(0).at8(0))
    // Read probabilities for each category
    val probs = 1 to prediction.vec(0).cardinality() map (idx => prediction.vec(idx).at(0))
    // Cleanup
    Seq(frameToPredict, prediction).foreach(_.delete)
    (predictedCategory, probs.toArray)
  }

  def classify(jobTitle: String, modelId: String, w2vModel: Word2VecModel): (String, Array[Double]) = {
    classify(jobTitle, model = water.DKV.getGet(modelId), w2vModel)

  }

  def classify(jobTitle: String, model: Model[_,_,_], w2vModel: Word2VecModel): (String, Array[Double]) = {
    val tokens = tokenize(jobTitle, STOP_WORDS)
    if (tokens.length == 0) {
      EMPTY_PREDICTION
    } else {
      val vec = wordsToVector(tokens, w2vModel)

      hex.ModelUtils.classify(vec.toArray, model)
    }
  }

  def createH2OFrame(datafile: String): (H2OFrame, Word2VecModel) = {
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

    // Transform RDD into DF and then to HF
    import h2oContext.implicits._
    import sqlContext.implicits._
    val h2oFrame: H2OFrame = finalRdd.toDF
    h2oFrame.replace(h2oFrame.find("category"), h2oFrame.vec("category").toCategoricalVec).remove()

    (h2oFrame, w2vModel)
  }

  def computeRareWords(dataRdd : RDD[Array[String]]): Set[String] = {
    // Compute frequencies of words
    val wordCounts = dataRdd.flatMap(s => s).map(w => (w,1)).reduceByKey(_ + _)

    // Collect rare words
    val rareWords = wordCounts.filter { case (k, v) => v < 2 }
      .map {case (k, v) => k }
      .collect
      .toSet
    rareWords
  }

  // Load data via Spark API
  private def loadData(filename: String): RDD[Array[String]] = {
    val data = sc.textFile(filename)
      .filter(line => !line.contains("category")).map(_.split(','))
    data
  }
}

/**
 * Representation of single job offer with its classification.
 *
 * @param category  job category (education, labor, ...)
 * @param fv  feature vector describing job title
 */
case class JobOffer(category: String, fv: mllib.linalg.Vector)

object CraigslistJobTitlesApp extends SparkContextSupport {

  val EMPTY_PREDICTION = ("NA", Array[Double]())

  val STOP_WORDS = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
    ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
    ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
    , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")


  def main(args: Array[String]): Unit = {
    // Prepare environment
    val sc = new SparkContext(configure("CraigslistJobTitlesApp"))
    val sqlContext = new SQLContext(sc)
    // Start H2O services
    val h2oContext = H2OContext.getOrCreate(sc)

    val app = new CraigslistJobTitlesApp("examples/smalldata/craigslistJobTitles.csv")(sc, sqlContext, h2oContext)
    try {
      app.run()
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        throw e
    } finally {
      app.shutdown()
    }
  }

  private[h2o] def tokenize(line: String, stopWords: Set[String]) = {
    //get rid of nonWords such as punctuation as opposed to splitting by just " "
    line.split("""\W+""")
      // Unify
      .map(_.toLowerCase)

      //remove mix of words+numbers
      .filter(word => ! (word exists Character.isDigit) )

      //remove stopwords defined above (you can add to this list if you want)
      .filterNot(word => stopWords.contains(word))

      //leave only words greater than 1 characters.
      //this deletes A LOT of words but useful to reduce our feature-set
      .filter(word => word.size >= 2)
  }

  // Make some helper functions
  private def sumArray (m: Array[Double], n: Array[Double]): Array[Double] = {
    for (i <- 0 until m.length) {m(i) += n(i)}
    return m
  }

  private def divArray (m: Array[Double], divisor: Double) : Array[Double] = {
    for (i <- 0 until m.length) {m(i) /= divisor}
    return m
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
      return model.transform(word)
    } catch {
      case e: Exception => return Vectors.zeros(100)
    }
  }

  def show(pred: (String, Array[Double]), classNames: Array[String]): String = {
    val probs = classNames.zip(pred._2).map(v => f"${v._1}: ${v._2}%.3f")
    pred._1 + ": " + probs.mkString("[", ", ", "]")
  }
}
