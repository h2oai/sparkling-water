package water

import org.apache.spark.h2o.H2OContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * A simple wrapper to allow launching H2O itself on the
 * top of Spark.
 */
object SparklingWaterDriver {

  /** Entry point */
  def main(args: Array[String]) {
    // Configure this application
    val conf: SparkConf = new SparkConf().setAppName("Sparkling Water")
    conf.setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
    val localMode = conf.get("spark.master").equals("local") || conf.get("spark.master").startsWith("local[")
    conf.setIfMissing("spark.ext.h2o.cluster.size",
      if (localMode) "1" else sys.env.getOrElse("spark.h2o.workers", "-1"))

    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    // Start H2O cluster only
    new H2OContext(sc).start()

    // Infinite wait
    this.synchronized( while(true) wait )
  }
}
