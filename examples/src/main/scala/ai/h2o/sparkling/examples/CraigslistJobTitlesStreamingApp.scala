package ai.h2o.sparkling.examples

import ai.h2o.sparkling.examples.CraigslistJobTitlesApp._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

/**
  * Variant of Craigslist App with streaming support to classify incoming events.
  *
  * Launch: nc -lk 9999 and send events from your console
  *
  * Send "poison pill" to kill the application.
  *
  */
object CraigslistJobTitlesStreamingApp {

  val POISON_PILL_MSG = "poison pill"

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Craigslist Job Titles Streaming App")
      .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val titlesTable = loadTitlesTable(spark)
    val model = fitModelPipeline(titlesTable)

    // Start streaming context
    val jobTitlesStream = ssc.socketTextStream("localhost", 9999)

    // Classify incoming messages
    jobTitlesStream
      .filter(!_.isEmpty)
      .map(jobTitle => (jobTitle, predict(spark, jobTitle, model)))
      .print()

    // Shutdown app if poison pill is passed as a message
    jobTitlesStream
      .filter(msg => POISON_PILL_MSG == msg)
      .foreachRDD(rdd =>
        if (!rdd.isEmpty()) {
          println("Poison pill received! Application is going to shut down...")
          ssc.stop(stopSparkContext = true, stopGracefully = true)
        })

    println("Please start the event producer at port 9999, for example: nc -lk 9999")
    ssc.start()
    ssc.awaitTermination()
  }
}
