package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.config.CATALOG_IMPLEMENTATION

/**
  * Spark session utils which can access Spark private API.
  */
object SparkSessionUtils extends Logging {

  /**
    * Initialize SparkSession with respect to environment.
    * Enable Hive support if environment supports Hive.
    *
    * @param conf  Spark configuration
    * @return a new SparkSession
    */
  def createSparkSession(conf: SparkConf, forceHive: Boolean = false): SparkSession = {
    val builder = SparkSession.builder.config(conf)
    if (conf.get(CATALOG_IMPLEMENTATION.key, "hive").toLowerCase == "hive" || forceHive) {
      if (SparkSession.hiveClassesArePresent || forceHive) {
        builder.enableHiveSupport()
        logInfo("Enabling Hive support for Spark session")
      } else {
        builder.config(CATALOG_IMPLEMENTATION.key, "in-memory")
      }
    } else {
      // Nop
    }
    val sparkSession = builder.getOrCreate()
    logInfo("Created Spark session")
    sparkSession
  }
}
