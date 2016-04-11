/**
  * Craigslist example
  *
  * It predicts job category based on job description (called "job title").
  *
  * Launch following commands:
  *    export MASTER="local-cluster[3,2,4096]"
  *   bin/sparkling-shell -i examples/scripts/craigslistJobTitles.script.scala
  *
  * When running using spark shell or using scala rest API:
  *    SQLContext is available as sqlContext
  *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
  *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
  *    SparkContext is available as sc
  */
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.DataFrame


def isHeader(line: String) = line.contains("category")
// Load and split data based on ","
val data = sc.textFile("examples/smalldata/craigslistJobTitles.csv").filter(x => !isHeader(x)).map(d => d.split(','))

// Extract job category from job description
val jobCategories = data.map(l => l(0))
val jobTitles = data.map(l => l(1))
// Count of different job categories
val labelCounts = jobCategories.map(n => (n, 1)).reduceByKey(_+_).collect.mkString("\n")

/* Should return:
(education,2438)
(administrative,2500)
(labor,2500)
(accounting,1593)
(customerservice,2319)
(foodbeverage,2495)
*/

// All strings which are not useful for text-mining
val stopwords = Set("ax","i","you","edu","s","t","m","subject","can","lines","re","what"
  ,"there","all","we","one","the","a","an","of","or","in","for","by","on"
  ,"but", "is", "in","a","not","with", "as", "was", "if","they", "are", "this", "and", "it", "have"
  , "from", "at", "my","be","by","not", "that", "to","from","com","org","like","likes","so")

// Compute rare words
val rareWords = jobTitles.flatMap(t => t.split("""\W+""").map(_.toLowerCase)).filter(word => """[^0-9]*""".r.pattern.matcher(word).matches).
  map(w => (w, 1)).reduceByKey(_+_).
  filter { case (k, v) => v < 2 }.map { case (k, v) => k }.
  collect.
  toSet

// Define tokenizer function
def token(line: String): Seq[String] = {
  //get rid of nonWords such as puncutation as opposed to splitting by just " "
  line.split("""\W+""")
    .map(_.toLowerCase)

    //remove mix of words+numbers
    .filter(word => """[^0-9]*""".r.pattern.matcher(word).matches)

    //remove stopwords defined above (you can add to this list if you want)
    .filterNot(word => stopwords.contains(word))

    //leave only words greater than 1 characters.
    //this deletes A LOT of words but useful to reduce our feature-set
    .filter(word => word.size >= 2)

    //remove rare occurences of words
    .filterNot(word => rareWords.contains(word))
}

val XXXwords = data.map(d => (d(0), token(d(1)).toSeq)).filter(s => s._2.length > 0)
val words = XXXwords.map(v => v._2)
val XXXlabels = XXXwords.map(v => v._1)

// Sanity Check
println(jobTitles.flatMap(lines => token(lines)).distinct.count)

// Make some helper functions
def sumArray (m: Array[Double], n: Array[Double]): Array[Double] = {
  for (i <- 0 until m.length) {m(i) += n(i)}
  return m
}

def divArray (m: Array[Double], divisor: Double) : Array[Double] = {
  for (i <- 0 until m.length) {m(i) /= divisor}
  return m
}

def wordToVector (w:String, m: Word2VecModel): Vector = {
  try {
    return m.transform(w)
  } catch {
    case e: Exception => return Vectors.zeros(100)
  }
}

//
// Word2Vec Model
//

val word2vec = new Word2Vec()
val model = word2vec.fit(words)

// Sanity Check
model.findSynonyms("teacher", 5).foreach(println)

val title_vectors = words.map(x => new DenseVector(
  divArray(x.map(m => wordToVector(m, model).toArray).
    reduceLeft(sumArray),x.length)).asInstanceOf[Vector])

//val title_pairs = words.map(x => (x,new DenseVector(
//    divArray(x.map(m => wordToVector(m, model).toArray).
//            reduceLeft(sumArray),x.length)).asInstanceOf[Vector]))

// Create H2OFrame
import org.apache.spark.mllib
case class CRAIGSLIST(target: String, a: mllib.linalg.Vector)

import org.apache.spark.h2o._
val h2oContext = H2OContext.getOrCreate(sc)
import h2oContext._
import h2oContext.implicits._

val resultRDD: DataFrame = XXXlabels.zip(title_vectors).map(v => CRAIGSLIST(v._1, v._2)).toDF

val table:H2OFrame = resultRDD

// OPEN FLOW UI
openFlow