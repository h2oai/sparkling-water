package ai.h2o

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.h2o._


object PipelineDemo {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PipelineDemo <port>")
      System.exit(1)
    }
    val port = args(0).toInt

    val sparkConf = new SparkConf().setAppName("PipelineDemo")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val hc = H2OContext.getOrCreate(ssc.sparkContext)
    val sqlContext = new org.apache.spark.sql.SQLContext(ssc.sparkContext)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    import hc._
    import hc.implicits._

    val lines = ssc.socketTextStream("localhost", port)
    lines.print() // useful to see some of the data stream for debugging
    val events = lines.map(_.split(",")).map(
      e=> RandomEvent(e(0), e(1).toDouble, e(2).toDouble)
    )
    var hf:H2OFrame = null
    events.window(Seconds(300), Seconds(10)).foreachRDD(rdd =>
      {
        if (!rdd.isEmpty ) {
          try {
            hf.delete()
          } catch { case e: Exception => println("Initialized frame") }
          hf = hc.asH2OFrame(rdd.toDF(), "demoFrame10s") //the name of the frame
          // perform your munging, score your events with a pretrained model or
          // mini-batch training on checkpointed models, etc
          // make sure your execution finishes within the batch cycle (the
          // second arg in the window)
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}
