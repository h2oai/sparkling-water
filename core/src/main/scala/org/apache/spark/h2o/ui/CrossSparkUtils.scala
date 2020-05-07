package org.apache.spark.h2o.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkContext
import org.apache.spark.ui.SparkUITab

import scala.xml.Node

trait CrossSparkUtils {
  def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      helpText: String): Seq[Node]

  def addSparklingWaterTab(sc: SparkContext): Unit
}
