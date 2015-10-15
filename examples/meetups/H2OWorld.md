# H2OWorld - Building Machine Learning Applications with Sparkling Water

## Requirements
 
### For Sparkling Water
 - Oracle Java 7+
 - [Spark 1.5.0+](http://spark.apache.org/downloads.html)
 - [Sparkling Water 1.5.4 - FIXME](http://h2o-release.s3.amazonaws.com/sparkling-water/master/93/index.html)
 
## Download
TODO: Point to USB and S3.

## Slides
TODO: Point to USB and WEB

## Script
TODO: Point to USB and WEB

## Dataset
TODO: Point to USB and WEB

## ML Workflow

> Create a model which will predict spam messages.

### Prepare data and build GBM model

1. Run Sparkling shell with an embedded Spark cluster:
  ```
  export SPARK_HOME="/path/to/spark/installation"
  export MASTER="local-cluster[3,2,4096]"
  bin/sparkling-shell --conf spark.executor.memory=3G 
  ```
  > Note: I would recommend to edit your `$SPARK_HOME/conf/log4j.properties` and configure log level to `WARN` to avoid flooding output with Spark INFO messages.

2. Open Spark UI: You can go to [http://localhost:4040/](http://localhost:4040/) to see the Spark status.

3. Prepare environment
  ```scala
  // Input data
  val DATAFILE="examples/smalldata/smsData.txt"
  // Common imports
  import _root_.hex.deeplearning.{DeepLearningModel, DeepLearning}
  import _root_.hex.deeplearning.DeepLearningParameters
  import org.apache.spark.examples.h2o.DemoUtils._
  import org.apache.spark.h2o._
  import org.apache.spark.mllib
  import org.apache.spark.mllib.feature.{IDFModel, IDF, HashingTF}
  import org.apache.spark.rdd.RDD
  import water.Key
  ```
  
4. Define representation of training message
   ```scala
   // Representation of a training message
   case class SMS(target: String, fv: mllib.linalg.Vector)
   ```

5. Define data loader
  ```scala
  def load(dataFile: String): RDD[Array[String]] = {
    sc.textFile(dataFile).map(l => l.split("\t")).filter(r => !r(0).isEmpty)
  }
  ```
  
6. Tokenize messages
  ```scala
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
  ```

6. Tf-IDF model builder 
  ```scala
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
  ```
  
7. DeepLearning model builder
  ```scala
  def buildDLModel(train: Frame, valid: Frame,
                 epochs: Int = 10, l1: Double = 0.001, l2: Double = 0.0,
                 hidden: Array[Int] = Array[Int](200, 200))
                (implicit h2oContext: H2OContext): DeepLearningModel = {
  import h2oContext._
  // Build a model
  import _root_.hex.deeplearning.DeepLearning
  import _root_.hex.deeplearning.DeepLearningParameters
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
    ```
8. Start H2O services on top of the Spark
  ```scala
   // Create SQL support
   import org.apache.spark.sql._
   implicit val sqlContext = SQLContext.getOrCreate(sc)
   import sqlContext.implicits._

   // Start H2O services
   import org.apache.spark.h2o._
   val h2oContext = new H2OContext(sc).start()
  ```

9. Open H2O UI: 
  ```scala
  h2oContext.openFlow
  ```
  > At this point, you can go use H2O UI and see status of H2O cloud by typing `getCloud`.
  
  
10. Build the application
  ```scala
  // Data load
  val data = load("examples/smalldata/smsData.txt")
  // Extract response spam or ham
  val hamSpam = data.map( r => r(0))
  val message = data.map( r => r(1))
  // Tokenize message content
  val tokens = tokenize(message)

  // Build IDF model
  var (hashingTF, idfModel, tfidf) = buildIDFModel(tokens)

  // Merge response with extracted vectors
  val resultRDD: DataFrame = hamSpam.zip(tfidf).map(v => SMS(v._1, v._2)).toDF

  // Publish Spark DataFrame as H2OFrame
  val table = h2oContext.asH2OFrame(resultRDD)
  // Transform target column into categorical
  table.replace(table.find("target"), table.vec("target").toCategoricalVec()).remove()
  table.update(null)

  // Split table
  val keys = Array[String]("train.hex", "valid.hex")
  val ratios = Array[Double](0.8)
  val frs = split(table, keys, ratios)
  val (train, valid) = (frs(0), frs(1))
  table.delete()

  // Build a model
  val dlModel = buildDLModel(train, valid)(h2oContext)
  ```
  
11. Evaluate model quality
   ```scala
   // Collect model metrics and evaluate model quality
   import water.app.ModelMetricsSupport
   val trainMetrics = ModelMetricsSupport.binomialMM(dlModel, train)
   val validMetrics = ModelMetricsSupport.binomialMM(dlModel, valid)
   println(trainMetrics.auc._auc)
   println(validMetrics.auc._auc)
   ```
   > You can also open H2O UI and type `getPredictions` to visualize model performance or `getModels` to see model output.
   
12. Create a spam detector
   ```scala
   // Spam detector
   def isSpam(msg: String,
           dlModel: DeepLearningModel,
           hashingTF: HashingTF,
           idfModel: IDFModel,
           h2oContext: H2OContext,
           hamThreshold: Double = 0.5):String = {
  val msgRdd = sc.parallelize(Seq(msg))
  val msgVector: DataFrame = idfModel.transform(
                              hashingTF.transform (
                                tokenize (msgRdd))).map(v => SMS("?", v)).toDF
  val msgTable: H2OFrame = h2oContext.asH2OFrame(msgVector)
  msgTable.remove(0) // remove first column
  val prediction = dlModel.score(msgTable)
  //println(prediction)
  if (prediction.vecs()(1).at(0) < hamThreshold) "SPAM DETECTED!" else "HAM"
  }   
  ```
  
13. Try to detect spam:
   ```scala
   isSpam("Michal, h2oworld party tonight in MV?", dlModel, hashingTF, idfModel, h2oContext)
   isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", dlModel, hashingTF, idfModel, h2oContext)
   ```

14. Use model from R.
   ```R
   library(h2o)
   h2o.init()
   # Get model 
   dl_model = h2o.getModel("dlModel.hex")
   # Generate a random vector representing a message
   validation.df = as.data.frame(t(runif(n = 1024, min = 0, max = 4)))
   names(validation.df) <- paste0("fv",0:1023)
   # Upload vector to a cluster
   validation.hex <- as.h2o(validation.df)
   # Make a prediction
   pred.hex = predict(dl_model, validation.hex)
   pred.hex
   ```
   > This requires installation of H2O R plugin - please follow installation instructions lister [here](http://h2o-release.s3.amazonaws.com/h2o-dev/master/1109/index.html#R).
   
