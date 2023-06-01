package org.apache.spark.h2o.ui

trait SparklingWaterInfoProvider {

  def localIpPort: String

  def sparklingWaterProperties: Seq[(String, String, String)]

  def H2OClusterInfo: H2OClusterInfo

  def isSparklingWaterStarted: Boolean

  def H2OBuildInfo: H2OBuildInfo

  def memoryInfo: Array[(String, String)]

  def timeInMillis: Long

  def isCloudHealthy: Boolean
}
