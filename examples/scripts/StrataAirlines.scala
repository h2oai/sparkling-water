// Start H2O
import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
val h2oContext = new H2OContext(sc).start()
import h2oContext._


// Import all year airlines into H2O
import java.io.File
val dataFile = "examples/smalldata/year2005.csv.gz"
val airlinesData = new DataFrame(new File(dataFile))


// Import weather data into Spark
val weatherDataFile = "examples/smalldata/Chicago_Ohare_International_Airport.csv"
val wrawdata = sc.textFile(weatherDataFile,8).cache()
val weatherTable = wrawdata.map(_.split(",")).map(row => WeatherParse(row)).filter(!_.isWrongRow())


// Transfer data from H2O to Spark RDD
val airlinesTable : RDD[Airlines] = toRDD[Airlines](airlinesData)
val flightsToORD = airlinesTable.filter(f => f.Dest==Some("ORD"))

// Use Spark SQL to join flight and weather data in spark
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
flightsToORD.registerTempTable("FlightsToORD")
weatherTable.registerTempTable("WeatherORD")


// Perform SQL Join on both tables
val bigTable = sql(
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

val trainFrame:DataFrame = bigTable
trainFrame.replace(19, trainFrame.vec("IsDepDelayed").toEnum)
trainFrame.update(null)

// Run deep learning to produce model estimating arrival delay
import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
val dlParams = new DeepLearningParameters()
dlParams._epochs = 100
dlParams._train = trainFrame
dlParams._response_column = 'IsDepDelayed
dlParams._variable_importances = true
// Create a job
val dl = new DeepLearning(dlParams)
val dlModel = dl.trainModel.get

// Use model to estimate delay on training data
val predictionH2OFrame = dlModel.score(bigTable)('predict)
val predictionsFromModel = toRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

// Run GLM to produce model estimating arrival delay
import hex.glm.GLMModel.GLMParameters.Family
import hex.glm.GLM
import hex.glm.GLMModel.GLMParameters
val glmParams = new GLMParameters(Family.binomial)
glmParams._train = bigTable
glmParams._response_column = 'IsDepDelayed
glmParams._alpha = Array[Double](0.5)
val glm = new GLM(glmParams)
val glmModel = glm.trainModel().get()

// Use model to estimate delay on training data
val predGLMH2OFrame = glmModel.score(bigTable)('predict)
val predGLMFromModel = toRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

// End of test
//exit
