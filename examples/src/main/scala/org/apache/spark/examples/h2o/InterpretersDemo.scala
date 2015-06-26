package org.apache.spark.examples.h2o

import org.apache.log4j.{Level, Logger}
import org.apache.spark.h2o.{H2OContext, SparklingConf}
import org.apache.spark.{SparkConf, SparkContext}
import water.app.SparkContextSupport


object InterpretersDemo extends SparkContextSupport with org.apache.spark.Logging {

  def main(args: Array[String]): Unit = {
    // Configure this application
    val conf: SparkConf = new SparklingConf().sparkConf
    
    // Create SparkContext to execute application on Spark cluster
    val sc = new SparkContext(conf)
    val h2oContext = new H2OContext(sc).start()
    // Infinite wait
    this.synchronized(while (true) wait())
  }
}