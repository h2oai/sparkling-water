package org.apache.spark.h2o.ui

import org.apache.spark.scheduler.SparkListenerEvent

/**
  * Event representing update of H2O status at run-time
  */
case class SparklingWaterHeartbeatEvent(cloudHealthy: Boolean, timeInMillis: Long, memoryInfo: Array[(String, String)])
  extends SparkListenerEvent
