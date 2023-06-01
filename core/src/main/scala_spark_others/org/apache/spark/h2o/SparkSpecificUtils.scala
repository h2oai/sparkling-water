package org.apache.spark.h2o

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkContext
import org.apache.spark.h2o.ui.{AppStatusListener, AppStatusStore, CrossSparkUtils, SparklingWaterUITab}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.ui.{SparkUITab, UIUtils}
import java.lang.Boolean._

import scala.xml.Node

object SparkSpecificUtils extends CrossSparkUtils {
  override def headerSparkPage(
      request: HttpServletRequest,
      title: String,
      content: => Seq[Node],
      activeTab: SparkUITab,
      helpText: String): Seq[Node] = {
    val method = UIUtils.getClass.getMethods.find(m => m.getName == "headerSparkPage").get
    val arguments = Seq[AnyRef](request, title, () => content, activeTab, Some(helpText), FALSE, FALSE)
    val result = if (arguments.length == method.getParameterCount) {
      method.invoke(UIUtils, arguments: _*)
    } else if (arguments.length + 1 == method.getParameterCount) {
      method.invoke(UIUtils, (arguments ++ Seq[AnyRef](FALSE)): _*)
    } else {
      throw new RuntimeException(
        s"UIUtils.headerSparkPage has ${method.getParameterCount} parameters which is unexpected!")
    }
    result.asInstanceOf[Seq[Node]]
  }

  override def addSparklingWaterTab(sc: SparkContext): Unit = {
    val kvStore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new AppStatusListener(sc.getConf, kvStore, live = true)
    sc.addSparkListener(listener)
    val statusStore = new AppStatusStore(kvStore, Some(listener))
    new SparklingWaterUITab(statusStore, sc.ui.get)
  }
}
