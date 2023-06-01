package org.apache.spark.h2o.ui

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.ui.SparkUI

/**
  * Required at the history server in order to create the [[SparklingWaterHistoryListener]]
  */
class SparklingWaterHistoryListenerFactory extends SparkHistoryListenerFactory {

  override def createListeners(conf: SparkConf, sparkUI: SparkUI): Seq[SparkListener] = {
    List(new SparklingWaterHistoryListener(conf, sparkUI))
  }
}

/**
  * Listener processing related sparkling water spark events
  */
class SparklingWaterListener(conf: SparkConf) extends SparkListener with Logging with SparklingWaterInfoProvider {
  var uiReady = false
  var h2oClusterInfo: Option[H2OClusterInfo] = None
  var h2oBuildInfo: Option[H2OBuildInfo] = None
  var swProperties: Option[Array[(String, String, String)]] = None
  var cloudHealthy = true
  var lastTimeHeadFromH2O: Long = 0
  var memoryInfo = Array.empty[(String, String)]

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case H2OContextStartedEvent(h2oClusterInfo, h2oBuildInfo, swProperties) => {
      this.h2oClusterInfo = Some(h2oClusterInfo)
      this.h2oBuildInfo = Some(h2oBuildInfo)
      this.swProperties = Some(swProperties)
      cloudHealthy = h2oClusterInfo.cloudHealthy
      lastTimeHeadFromH2O = h2oClusterInfo.h2oStartTime
      uiReady = true
    }
    case SparklingWaterHeartbeatEvent(cloudHealthy, timeInMillis, memoryInfo) => {
      this.cloudHealthy = cloudHealthy
      this.lastTimeHeadFromH2O = timeInMillis
      this.memoryInfo = memoryInfo
    }
    case _ => // Ignore
  }

  override def localIpPort: String = h2oClusterInfo.get.localClientIpPort

  override def sparklingWaterProperties: Seq[(String, String, String)] = swProperties.get

  override def H2OClusterInfo: H2OClusterInfo = h2oClusterInfo.get

  override def isSparklingWaterStarted: Boolean = uiReady

  override def H2OBuildInfo: H2OBuildInfo = h2oBuildInfo.get

  override def timeInMillis: Long = lastTimeHeadFromH2O

  override def isCloudHealthy: Boolean = cloudHealthy
}

/**
  * A [[SparklingWaterListener]] for rendering the Sparkling Water UI in the history server.
  */
class SparklingWaterHistoryListener(conf: SparkConf, sparkUI: SparkUI) extends SparklingWaterListener(conf) {

  private var sparklingWaterTabAttached = false

  override def onExecutorMetricsUpdate(u: SparkListenerExecutorMetricsUpdate): Unit = {
    // Do nothing; these events are not logged
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case _: H2OContextStartedEvent =>
      if (!sparklingWaterTabAttached) {
        new SparklingWaterUITab(this, sparkUI)
        sparklingWaterTabAttached = true
      }
      super.onOtherEvent(event)
    case _ => super.onOtherEvent(event)
  }
}
