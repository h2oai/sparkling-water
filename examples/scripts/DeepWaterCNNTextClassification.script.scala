/**
  * This script requires deepwater-all.jar to be added through the "--jars" parameter in bin/sparkling-shell script
  *
  * Launch following commands:
  *   export MASTER="local-cluster[3,2,4096]"
  *   bin/sparkling-shell -i examples/scripts/DeepWaterCNNTextClassification.script.scala
  *
  * When running using spark shell or using scala rest API:
  *    SQLContext is available as sqlContext
  *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
  *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
  *    SparkContext is available as sc
  */
import _root_.hex.deepwater.{DeepWater, DeepWaterParameters}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.mllib.feature.HashingTF
import water.support.SparkContextSupport

import scala.io.Source
import scala.io.Source

def cleanString(string: String): String =
  string.replaceAll("[^A-Za-z0-9(),!?\\'\\`]", " ").
    replaceAll("\\'s", " \'s").
    replaceAll("\'ve", " \'ve").
    replaceAll("n\'t", " n\'t").
    replaceAll("\'re", " \'re").
    replaceAll("\'d", " \'d").
    replaceAll("\'ll", " \'ll").
    replaceAll(",", " , ").
    replaceAll("!", " ! ").
    replaceAll("\\(", " \\( ").
    replaceAll("\\)", " \\) ").
    replaceAll("\\?", " \\? ").
    replaceAll("\\s{2,}", " ").
    trim.toLowerCase

def loadData(): RDD[(Int, Array[String])] = {
  val pos_file = Source.fromURL("https://raw.githubusercontent.com/yoonkim/CNN_sentence/master/rt-polarity.pos", "latin1")
  val neg_file = Source.fromURL("https://raw.githubusercontent.com/yoonkim/CNN_sentence/master/rt-polarity.neg", "latin1")

  val positive_examples = sc.parallelize(pos_file.getLines().toSeq).map(s => (1, cleanString(s.trim).split(" ")) )
  val negative_examples = sc.parallelize(neg_file.getLines().toSeq).map(s => (0, cleanString(s.trim).split(" ")) )

  positive_examples.union(negative_examples)
}

def padSentences(sentences: RDD[(Int, Array[String])], paddingWord: String = "</s>"): RDD[(Int, Array[String])] = {
  val sequenceLength = sentences.values.map(_.length).max()
  sentences.map{ case (l, s) => ( l, s.padTo(sequenceLength, paddingWord) ) }
}

def vectorize(sentences: RDD[(Int, Array[String])], vocabSize: Int) =
  sentences.map{ case (l, s) => (l, new HashingTF(vocabSize).transform(s))}

val vocabSize = 65536

val labeledSentences = loadData()
val paddedSenteces = padSentences(labeledSentences)

val hc = H2OContext.getOrCreate(sc)
import hc.implicits._
import spark.sqlContext.implicits._

val vectorizedSentences = vectorize(paddedSenteces, vocabSize).toDF

val frame : H2OFrame = vectorizedSentences

val p = new DeepWaterParameters
p._backend = DeepWaterParameters.Backend.tensorflow
p._train = frame._key
p._response_column = "_1"
p._learning_rate = 1e-4
p._mini_batch_size = 8
p._train_samples_per_iteration = 8
p._epochs = 1e-3
//p._network = DeepWaterParameters.Network.user
p._network_definition_file = "examples/networks/cnn_text_tensorflow.meta"
p._image_shape = Array(16247, 1)
p._channels = 1
val model = new DeepWater(p).trainModel.get

// TODO add predictions