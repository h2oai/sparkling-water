package org.apache.spark.h2o.ui

import org.apache.spark.SparkException
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.ui.SparklingWaterUITab.getSparkUI
import org.apache.spark.ui.{SparkUI, SparkUITab}


/**
  * Enrich Spark UI by a Sparkling Water specific tab.
  */
private[h2o] class SparklingWaterUITab(val hc: H2OContext)
  extends SparkUITab(getSparkUI(hc), "sparkling-water") {

  override val name = "Sparkling Water \u2728"

  val parent = getSparkUI(hc)

  attachPage(new SparklingWaterInfoPage(this))
  
  def attach() {
    getSparkUI(hc).attachTab(this)
  }
  
  def detach(): Unit = {
    getSparkUI(hc).detachTab(this)
  }

}

private object SparklingWaterUITab {
  def getSparkUI(hc: H2OContext): SparkUI = {
    hc.sparkContext.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
