/**
 * Launch following commands:
 *    export MASTER="local-cluster[3,2,4096]"
 *   bin/sparkling-shell -i examples/scripts/mlconf_2015_hamSpam.script.script.scala
 *
 * When running using spark shell or using scala rest API:
 *    SQLContext is available as sqlContext
 *    SparkContext is available as sc
 */
import hex.deeplearning.{DeepLearningModel}
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.h2o._
import org.apache.spark.{SparkFiles, mllib}
import org.apache.spark.mllib.feature.{IDFModel, IDF, HashingTF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import water.Key
import water.app.ModelMetricsSupport

// Register files to SparkContext
addFiles(sc,"examples/smalldata/smsData.txt")
// One training message
case class SMS(target: String, fv: mllib.linalg.Vector)

// Data loader
def load(dataFile: String): RDD[Array[String]] = {
  sc.textFile(SparkFiles.get(dataFile)).map(l => l.split("\t")).filter(r => !r(0).isEmpty)
}

// Tokenizer
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

def buildDLModel(train: Frame, valid: Frame,
                 epochs: Int = 10, l1: Double = 0.001, l2: Double = 0.0,
                 hidden: Array[Int] = Array[Int](200, 200))
                (implicit h2oContext: H2OContext): DeepLearningModel = {
  import h2oContext._
  // Build a model
  import hex.deeplearning.DeepLearning
  import hex.deeplearning.DeepLearningParameters
  val dlParams = new DeepLearningParameters()
  dlParams._model_id = Key.make("dlModel.hex")
  dlParams._train = train
  dlParams._valid = valid
  dlParams._response_column = 'target
  dlParams._epochs = epochs
  dlParams._l1 = l1
  dlParams._hidden = hidden

  // Create a job
  val dl = new DeepLearning(dlParams)
  val dlModel = dl.trainModel.get

  // Compute metrics on both datasets
  dlModel.score(train).delete()
  dlModel.score(valid).delete()

  dlModel
}

// Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)
import sqlContext.implicits._

// Start H2O services
import org.apache.spark.h2o._
implicit val h2oContext = new H2OContext(sc).start()
import h2oContext._


// Data load
val data = load("smsData.txt")
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

// Split table
val keys = Array[String]("train.hex", "valid.hex")
val ratios = Array[Double](0.8)
val frs = split(table, keys, ratios)
val (train, valid) = (frs(0), frs(1))
table.delete()

// Build a model
val dlModel = buildDLModel(train, valid)

/*
 * The following code is appended life during presentation.
 */
// Collect model metrics and evaluate model quality
val trainMetrics = ModelMetricsSupport.binomialMM(dlModel, train)
val validMetrics = ModelMetricsSupport.binomialMM(dlModel, valid)
println(trainMetrics.auc._auc)
println(validMetrics.auc._auc)

// Spam detector
def isSpam(msg: String,
           dlModel: DeepLearningModel,
           hashingTF: HashingTF,
           idfModel: IDFModel,
           hamThreshold: Double = 0.5):Boolean = {
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

println(isSpam("Michal, beer tonight in MV?", dlModel, hashingTF, idfModel))
println(isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", dlModel, hashingTF, idfModel))

