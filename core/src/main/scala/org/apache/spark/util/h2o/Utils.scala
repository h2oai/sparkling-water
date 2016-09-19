package org.apache.spark.util.h2o

/**
  * Expose some utilities method already defined in Spark
  */
object Utils {

  def createTempDir() = org.apache.spark.util.Utils.createTempDir()
}
