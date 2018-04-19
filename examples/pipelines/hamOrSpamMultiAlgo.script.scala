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
import org.apache.spark.ml.h2o.algos.{H2OAutoML, H2ODeepLearning, H2OGBM, H2OGridSearch}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import water.support.SparkContextSupport
import water.fvec.H2OFrame
import org.apache.spark.ml.Pipeline

import scala.collection.mutable.HashMap


val smsDataFileName = "smsData.txt"
val smsDataFilePath = "examples/smalldata/" + smsDataFileName

// Register files to SparkContext
SparkContextSupport.addFiles(sc, smsDataFilePath)

// This method loads the data, perform some basic filtering and create Spark's dataframe
def load(dataFile: String)(implicit sqlContext: SQLContext): DataFrame = {
  val smsSchema = StructType(Array(
    StructField("label", StringType, nullable = false),
    StructField("text", StringType, nullable = false)))
  val rowRDD = sc.textFile(SparkFiles.get(dataFile)).map(_.split("\t", 2)).filter(r => !r(0).isEmpty).map(p => Row(p(0),p(1)))
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
  setInputCol(stopWordsRemover.getOutputCol).
  setOutputCol("wordToIndex")

// Create inverse document frequencies model
val idf = new IDF().
  setMinDocFreq(4).
  setInputCol(hashingTF.getOutputCol).
  setOutputCol("tf_idf")

val algoStage = algo match {
  case "gbm" =>
    // Create GBM model
    new H2OGBM().
      setTrainRatio(0.8).
      setSeed(1).
      setFeaturesCols("tf_idf").
      setPredictionsCol("label")
  case "dl" =>
    // Create H2ODeepLearning model
    new H2ODeepLearning().
      setEpochs(10).
      setL1(0.001).
      setL2(0.0).
      setSeed(1).
      setHidden(Array[Int](200, 200)).
      setFeaturesCols("tf_idf").
      setPredictionsCol("label")
  case "automl" =>
    // Create H2OAutoML model
    new H2OAutoML().
      setPredictionsCol("label").
      setSeed(1).
      setMaxRuntimeSecs(60). // 1 minutes
      setConvertUnknownCategoricalLevelsToNa(true)
  case "grid_gbm" =>
    // Create Grid GBM Model
    import scala.collection.mutable.HashMap
    val hyperParams: HashMap[String, Array[AnyRef]] = HashMap()
    hyperParams += ("_ntrees" -> Array(1, 30).map(_.asInstanceOf[AnyRef]))
    new H2OGridSearch().
      setPredictionsCol("label").
      setHyperParameters(hyperParams).
      setParameters(new H2OGBM().setMaxDepth(30).setSeed(1))
}


// Remove all intermediate columns
val colPruner = new ColumnPruner().
  setColumns(Array[String](idf.getOutputCol, hashingTF.getOutputCol, stopWordsRemover.getOutputCol, tokenizer.getOutputCol))

// Create the pipeline by defining all the stages
val pipeline = new Pipeline().
  setStages(Array(tokenizer, stopWordsRemover, hashingTF, idf, algoStage, colPruner))

// Test exporting and importing the pipeline. On Systems where HDFS & Hadoop is not available, this call store the pipeline
// to local file in the current directory. In case HDFS & Hadoop is available, this call stores the pipeline to HDFS home
// directory for the current user. Absolute paths can be used as wells. The same holds for the model import/export bellow.
pipeline.write.overwrite.save("examples/build/pipeline")
val loadedPipeline = Pipeline.load("examples/build/pipeline")
// Train the pipeline model
val data = load("smsData.txt")
val model = loadedPipeline.fit(data)

model.write.overwrite.save("examples/build/model")
val loadedModel = PipelineModel.load("examples/build/model")

/*
 * Make predictions on unlabeled data
 * Spam detector
 */
def isSpam(smsText: String,
           model: PipelineModel,
           hamThreshold: Double = 0.5) = {
  val smsTextSchema = StructType(Array(StructField("text", StringType, nullable = false)))
  val smsTextRowRDD = sc.parallelize(Seq(smsText)).map(Row(_))
  val smsTextDF = sqlContext.createDataFrame(smsTextRowRDD, smsTextSchema)
  val prediction = model.transform(smsTextDF)
  prediction.select("prediction_output.p1").first.getDouble(0) > hamThreshold
}

println(isSpam("Michal, h2oworld party tonight in MV?", loadedModel))

println(isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", loadedModel))
