/**
  * Launch following commands:
  *    export MASTER="local[*]"
  *   bin/sparkling-shell -i examples/scripts/hamSpam.script.script.scala
  *
  * When running using spark shell or using scala rest API:
  *    SQLContext is available as sqlContext
  *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
  *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
  *    SparkContext is available as sc
  */

import org.apache.spark.SparkFiles
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.h2o.features.ColumnPruner
import org.apache.spark.ml.h2o.algos.H2OGBM
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import water.support.SparkContextSupport
import water.fvec.H2OFrame


val smsDataFileName = "smsData.txt"
val smsDataFilePath = "examples/smalldata/" + smsDataFileName

// Register files to SparkContext
SparkContextSupport.addFiles(sc, smsDataFilePath)

// This method loads the data, perform some basic filtering and create Spark's dataframe
def load(dataFile: String)(implicit sqlContext: SQLContext): DataFrame = {
  val smsSchema = StructType(Array(
    StructField("label", StringType, nullable = false),
    StructField("text", StringType, nullable = false)))
  val rowRDD = sc.textFile(SparkFiles.get(dataFile)).map(_.split("\t")).filter(r => !r(0).isEmpty).map(p => Row(p(0),p(1)))
  sqlContext.createDataFrame(rowRDD, smsSchema)
}

import org.apache.spark.h2o._
implicit val h2oContext = H2OContext.getOrCreate(spark)

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
  setInputCol(tokenizer.getOutputCol).
  setOutputCol("wordToIndex")

// Create inverse document frequencies model
val idf = new IDF().
  setMinDocFreq(4).
  setInputCol(hashingTF.getOutputCol).
  setOutputCol("tf_idf")

// Remove specified columns
val colPruner = new ColumnPruner().
  setKeep(true).
  setColumns(Array[String]("label", "tf_idf"))

// Create H2ODeepLearning model
// If the key specified the training set is specified using setTrainKey, then frame with this key is used as the training
// frame, otherwise it uses the frame from the previous stage as the training frame
//val dl = new H2OGBM().
//  setTrainRatio(0.8).
//  setFeaturesCol("tf_idf").
//  setPredictionsCol("label")

import org.apache.spark.ml.h2o.algos.H2ODeepLearning
val dl = new H2ODeepLearning().
  setEpochs(10).
  setL1(0.001).
  setL2(0.0).
  setHidden(Array[Int](200, 200)).
  setResponseColumn("label")

import org.apache.spark.ml.Pipeline
// Create the pipeline by defining all the stages
val pipeline = new Pipeline().
  setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, colPruner, dl))

// Train the pipeline model
val data = load("smsData.txt")
val model = pipeline.fit(data)


/*
 * Make predictions on unlabeled data
 * Spam detector
 */
def isSpam(smsText: String,
           model: PipelineModel,
           h2oContext: H2OContext,
           hamThreshold: Double = 0.5) = {
  import h2oContext.implicits._
  val smsTextSchema = StructType(Array(StructField("text", StringType, nullable = false)))
  val smsTextRowRDD = sc.parallelize(Seq(smsText)).map(Row(_))
  val smsTextDF = sqlContext.createDataFrame(smsTextRowRDD, smsTextSchema)
  val prediction: H2OFrame = model.transform(smsTextDF)
  prediction.vecs()(1).at(0) < hamThreshold
}

val f = isSpam("Michal, h2oworld party tonight in MV?", model, h2oContext)

println(isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", model, h2oContext))
