package org.apache.spark.h2o

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkContext
import org.apache.spark.h2o.ui.{CrossSparkUtils, SparklingWaterListener, SparklingWaterUITab}
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
    val sparklingWaterListener = new SparklingWaterListener(sc.conf)
    sc.addSparkListener(sparklingWaterListener)
    new SparklingWaterUITab(sparklingWaterListener, sc.ui.get)
  }
}
