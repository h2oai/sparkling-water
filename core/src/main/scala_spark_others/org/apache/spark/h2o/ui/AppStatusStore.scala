package org.apache.spark.h2o.ui

import org.apache.spark.status.KVUtils.KVIndexParam
import org.apache.spark.util.kvstore.KVStore

/**
  * Sparkling Water accessors into general Spark KVStore
  */
class AppStatusStore(store: KVStore, val listener: Option[AppStatusListener] = None)
  extends SparklingWaterInfoProvider {

  private def getStartedInfo(): SparklingWaterStartedInfo = {
    val klass = classOf[SparklingWaterStartedInfo]
    store.read(klass, klass.getName)
  }

  private def getUpdateInfo(): SparklingWaterUpdateInfo = {
    val klass = classOf[SparklingWaterUpdateInfo]
    store.read(klass, klass.getName)
  }

  override def isSparklingWaterStarted(): Boolean = {
    val klass = classOf[SparklingWaterStartedInfo]
    store.count(klass) != 0
  }

  override def localIpPort: String = getStartedInfo().h2oClusterInfo.localClientIpPort

  override def sparklingWaterProperties: Seq[(String, String, String)] = getStartedInfo().swProperties

  override def H2OClusterInfo: H2OClusterInfo = getStartedInfo().h2oClusterInfo

  override def H2OBuildInfo: H2OBuildInfo = getStartedInfo().h2oBuildInfo

  override def memoryInfo: Array[(String, String)] = getUpdateInfo().memoryInfo

  override def timeInMillis: Long = getUpdateInfo().timeInMillis

  override def isCloudHealthy: Boolean = getUpdateInfo().cloudHealthy
}

/**
  * Object encapsulating information produced when Sparkling Water is started
  */
class SparklingWaterStartedInfo(
    val h2oClusterInfo: H2OClusterInfo,
    val h2oBuildInfo: H2OBuildInfo,
    val swProperties: Array[(String, String, String)]) {
  // Use lass name ad key since there is always just a single instance of this object in KVStore
  @KVIndexParam val id: String = getClass.getName
}

/**
  * Object encapsulating information about Sparkling Water Heartbeat
  */
class SparklingWaterUpdateInfo(
    val cloudHealthy: Boolean,
    val timeInMillis: Long,
    val memoryInfo: Array[(String, String)]) {
  // Use lass name ad key since there is always just a single instance of this object in KVStore
  @KVIndexParam val id: String = getClass.getName
}
