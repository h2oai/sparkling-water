package org.apache.spark.h2o.ui

import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.ui.SparkUI

/**
  * History server plugin to enable Sparkling Water UI on history server
  */
class AppStatusPlugin extends AppHistoryServerPlugin {
  override def createListeners(conf: SparkConf, store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new AppStatusListener(conf, store, live = false))
  }

  override def setupUI(ui: SparkUI): Unit = {
    val sparklingWaterAppStatusStore = new AppStatusStore(ui.store.store)
    if (sparklingWaterAppStatusStore.isSparklingWaterStarted()) {
      new SparklingWaterUITab(sparklingWaterAppStatusStore, ui)
    }
  }
}
