package org.apache.spark.h2o.ui

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.status.{ElementTrackingStore, LiveEntity}

/**
  * Listener processing Sparkling Water events
  */
class AppStatusListener(conf: SparkConf, store: ElementTrackingStore, live: Boolean)
  extends SparkListener
  with Logging {

  private def onSparklingWaterStart(event: H2OContextStartedEvent): Unit = {
    val H2OContextStartedEvent(h2oClusterInfo, h2oBuildInfo, swProperties) = event
    val now = System.nanoTime()
    new SparklingWaterInfo(h2oClusterInfo, h2oBuildInfo, swProperties).write(store, now)
  }

  private def onSparklingWaterUpdate(event: SparklingWaterHeartbeatEvent): Unit = {
    val SparklingWaterHeartbeatEvent(cloudHealthy, timeInMillis, memoryInfo) = event
    val now = System.nanoTime()
    new SparklingWaterUpdate(cloudHealthy, timeInMillis, memoryInfo).write(store, now)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: H2OContextStartedEvent => onSparklingWaterStart(e)
    case e: SparklingWaterHeartbeatEvent => onSparklingWaterUpdate(e)
    case _ => // ignore
  }

  private class SparklingWaterInfo(
      h2oClusterInfo: H2OClusterInfo,
      h2oBuildInfo: H2OBuildInfo,
      swProperties: Array[(String, String, String)])
    extends LiveEntity {
    override protected def doUpdate(): Any = {
      new SparklingWaterStartedInfo(h2oClusterInfo, h2oBuildInfo, swProperties)
    }
  }

  private class SparklingWaterUpdate(cloudHealthy: Boolean, timeInMillis: Long, val memoryInfo: Array[(String, String)])
    extends LiveEntity {
    override protected def doUpdate(): Any = {
      new SparklingWaterUpdateInfo(cloudHealthy, timeInMillis, memoryInfo)
    }
  }

}
