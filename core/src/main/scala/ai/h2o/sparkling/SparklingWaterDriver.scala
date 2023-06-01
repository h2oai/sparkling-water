package ai.h2o.sparkling

import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.SparkConf

/**
  * A simple wrapper to allow launching H2O itself on the
  * top of Spark.
  */
object SparklingWaterDriver {

  /** Entry point */
  def main(args: Array[String]) {
    // Configure this application
    val conf: SparkConf = H2OConf.checkSparkConf(
      new SparkConf()
        .setAppName("Sparkling Water Driver")
        .setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local[*]"))
        .set("spark.ext.h2o.repl.enabled", "true"))

    SparkSessionUtils.createSparkSession(conf)
    // Start H2O cluster only
    val hc = H2OContext.getOrCreate()

    println(hc)

    // Infinite wait
    this.synchronized(while (true) {
      wait()
    })
  }
}
