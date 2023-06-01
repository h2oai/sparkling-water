package org.apache.spark.h2o

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkContext
import org.apache.spark.h2o.ui.{AppStatusListener, AppStatusStore, CrossSparkUtils, SparklingWaterUITab}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.ui.{SparkUITab, UIUtils}

import scala.xml.Node

object SparkSpecificUtils extends CrossSparkUtils {
  override def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      helpText: String): Seq[Node] = {
    UIUtils.headerSparkPage("Sparkling Water", content, activeTab, helpText = Some(helpText))
  }

  override def addSparklingWaterTab(sc: SparkContext): Unit = {
    val kvStore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new AppStatusListener(sc.getConf, kvStore, live = true)
    sc.addSparkListener(listener)
    val statusStore = new AppStatusStore(kvStore, Some(listener))
    new SparklingWaterUITab(statusStore, sc.ui.get)
  }
}
