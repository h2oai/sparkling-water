package org.apache.spark.examples.h2o

import java.net.URI

import hex.Model
import hex.Model.Output
import org.apache.spark.SparkContext
import org.apache.spark.examples.h2o.CraigslistJobTitlesApp._
import org.apache.spark.h2o.H2OContext
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import water.app.{ModelSerializationSupport, SparkContextSupport}

/**
  * Streaming app using saved models.
  */
object CraigslistJobTitlesStreamingOnlyApp extends SparkContextSupport with ModelSerializationSupport {

  def main(args: Array[String]) {
    // Prepare environment
    val sc = new SparkContext(configure("CraigslistJobTitlesStreamingOnlyApp"))
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new SQLContext(sc)
    // Start H2O services
    val h2oContext = new H2OContext(sc).start()
    val staticApp = new CraigslistJobTitlesApp()(sc, sqlContext, h2oContext)

    try {
      val h2oModel: Model[_, _, _] = loadH2OModel(URI.create("file:///tmp/h2omodel"))
      val modelId = h2oModel._key.toString
      val classNames = h2oModel._output.asInstanceOf[Output].classNames()
      val sparkModel = loadSparkModel[Word2VecModel](URI.create("file:///tmp/sparkmodel"))

      // Start streaming context
      val jobTitlesStream = ssc.socketTextStream("localhost", 9999)

      // Classify incoming messages
      jobTitlesStream.filter(!_.isEmpty)
        .map(jobTitle => (jobTitle, staticApp.classify(jobTitle, modelId, sparkModel)))
        .map(pred => "\"" + pred._1 + "\" = " + show(pred._2, classNames))
        .print()

      println("Please start the event producer at port 9999, for example: nc -lk 9999")
      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      ssc.stop()
      staticApp.shutdown()
    }
  }
}
