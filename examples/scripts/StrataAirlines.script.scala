/**
 * Launch following commands:
 *   export MASTER="local-cluster[3,2,4096]"
 *   bin/sparkling-shell -i examples/scripts/StrataAirlines.script.scala
 *
  * When running using spark shell or using scala rest API:
  *    SQLContext is available as sqlContext
  *     - if you want to use sqlContext implicitly, you have to redefine it like: implicit val sqlContext = sqlContext,
  *      but better is to use it like this: implicit val sqlContext = SQLContext.getOrCreate(sc)
  *    SparkContext is available as sc
  */
// Common imports
import org.apache.spark.SparkFiles
import org.apache.spark.examples.h2o.DemoUtils._
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
import org.apache.spark.sql.SQLContext
import water.Key
import java.io.File

// Create SQL support
implicit val sqlContext = SQLContext.getOrCreate(sc)
import sqlContext.implicits._

// Start H2O services
val h2oContext = H2OContext.getOrCreate(sc)
import h2oContext._
import h2oContext.implicits._

// Register files to SparkContext
addFiles(sc,
  "examples/smalldata/year2005.csv.gz",
  "examples/smalldata/Chicago_Ohare_International_Airport.csv"
)

// Import all year airlines data into H2O
val airlinesData = new H2OFrame(new File(SparkFiles.get("year2005.csv.gz")))

// Import weather data into Spark
val wrawdata = sc.textFile(SparkFiles.get("Chicago_Ohare_International_Airport.csv"),8).cache()
val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())

// Transfer data from H2O to Spark RDD
val airlinesTable : RDD[Airlines] = asRDD[Airlines](airlinesData)
val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

// Use Spark SQL to join flight and weather data in spark
flightsToORD.toDF.registerTempTable("FlightsToORD")
weatherTable.toDF.registerTempTable("WeatherORD")

// Perform SQL Join on both tables
val bigTable = sqlContext.sql(
  """SELECT
          |f.Year,f.Month,f.DayofMonth,
          |f.CRSDepTime,f.CRSArrTime,f.CRSElapsedTime,
          |f.UniqueCarrier,f.FlightNum,f.TailNum,
          |f.Origin,f.Distance,
          |w.TmaxF,w.TminF,w.TmeanF,w.PrcpIn,w.SnowIn,w.CDD,w.HDD,w.GDD,
          |f.IsDepDelayed
          |FROM FlightsToORD f
          |JOIN WeatherORD w
          |ON f.Year=w.Year AND f.Month=w.Month AND f.DayofMonth=w.Day""".stripMargin)

val trainFrame:H2OFrame = bigTable
trainFrame.replace(19, trainFrame.vec("IsDepDelayed").toCategoricalVec)
trainFrame.update()

// Run deep learning to produce model estimating arrival delay
import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
val dlParams = new DeepLearningParameters()
dlParams._epochs = 100
dlParams._train = trainFrame
dlParams._response_column = 'IsDepDelayed
dlParams._variable_importances = true
// Create a job
val dl = new DeepLearning(dlParams, Key.make("dlModel.hex"))
val dlModel = dl.trainModel.get

// Use model to estimate delay on training data
val predictionH2OFrame = dlModel.score(bigTable)('predict)
val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

// Run GLM to produce model estimating arrival delay
import hex.glm.GLMModel.GLMParameters.Family
import hex.glm.GLM
import hex.glm.GLMModel.GLMParameters
val glmParams = new GLMParameters(Family.binomial)
glmParams._train = bigTable
glmParams._response_column = 'IsDepDelayed
glmParams._alpha = Array[Double](0.5)
val glm = new GLM(glmParams, Key.make("glmModel.hex"))
val glmModel = glm.trainModel().get()

// Use model to estimate delay on training data
val predGLMH2OFrame = glmModel.score(bigTable)('predict)
val predGLMFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

