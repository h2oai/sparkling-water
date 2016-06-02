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
import org.apache.spark.ml.h2o.H2OPipeline
import org.apache.spark.ml.h2o.features.{ColRemover, DatasetSplitter}
import org.apache.spark.ml.h2o.models.H2ODeepLearning
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

// Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)
// Start H2O services
import org.apache.spark.h2o._
implicit val h2oContext = H2OContext.getOrCreate(sc)

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
// This is OneTimeTransformer which is executed only during fitting stage
val colRemover = new ColRemover().
  setKeep(true).
  setColumns(Array[String]("label", "tf_idf"))

// Split the dataset and store the splits with the specified keys into DKV
// This is OneTimeTransformer which is executed only during fitting stage
// It determines the frame which is passed on the output in the following order:
//    1) If the train key is specified using setTrainKey method and the key is also specified in the list of keys, then frame with this key is passed on the output
//    2) Otherwise, if the default key - "train.hex" is specified in the list of keys, then frame with this key is passed on the output
//    3) Otherwise the first frame specified in the list of keys is passed on the output
val splitter = new DatasetSplitter().
  setKeys(Array[String]("train.hex", "valid.hex")).
  setRatios(Array[Double](0.8)).
  setTrainKey("train.hex")

// Create H2ODeepLearning model
// If the key specified the training set is specified using setTrainKey, then frame with this key is used as the training
// frame, otherwise it uses the frame from the previous stage as the training frame
val dl = new H2ODeepLearning().
  setEpochs(10).
  setL1(0.001).
  setL2(0.0).
  setHidden(Array[Int](200, 200)).
  setValidKey(splitter.getKeys(1)).
  setResponseColumn("label")

// Create the pipeline by defining all the stages
val pipeline = new H2OPipeline().
  setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, colRemover, splitter, dl))

// Train the pipeline model
val data = load("smsData.txt")
val model = pipeline.fit(data)

// now we can optionally save the fitted pipeline to disk
model.write.overwrite().save("/tmp/hamOrSpamPipeline")
// load the fitted model
val loadedModel = PipelineModel.load("/tmp/hamOrSpamPipeline")

// we can also save this unfit pipeline to disk
pipeline.write.overwrite().save("/tmp/unfit-hamOrSpamPipeline")
// load unfitted pipeline
val loadedPipeline = H2OPipeline.load("/tmp/unfit-hamOrSpamPipeline")
// Train the pipeline model
val modelOfLoadedPipeline = pipeline.fit(data)

/*
 * Make predictions on unlabeled data
 * Spam detector
 */
def isSpam(smsText: String,
           model: PipelineModel,
           h2oContext: H2OContext,
           hamThreshold: Double = 0.5):Boolean = {
  import h2oContext.implicits._
  val smsTextSchema = StructType(Array(StructField("text", StringType, nullable = false)))
  val smsTextRowRDD = sc.parallelize(Seq(smsText)).map(Row(_))
  val smsTextDF = sqlContext.createDataFrame(smsTextRowRDD, smsTextSchema)
  val prediction: H2OFrame = model.transform(smsTextDF)
  prediction.vecs()(1).at(0) < hamThreshold
}

println(isSpam("Michal, h2oworld party tonight in MV?", modelOfLoadedPipeline, h2oContext))
println(isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", loadedModel, h2oContext))
