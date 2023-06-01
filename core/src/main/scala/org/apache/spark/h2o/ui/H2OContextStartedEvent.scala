package org.apache.spark.h2o.ui

import org.apache.spark.scheduler.SparkListenerEvent

/**
  * Event representing start of H2OContext
  */
case class H2OContextStartedEvent(
    h2oClusterInfo: H2OClusterInfo,
    h2oBuildInfo: H2OBuildInfo,
    swProperties: Array[(String, String, String)])
  extends SparkListenerEvent
