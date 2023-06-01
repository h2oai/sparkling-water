package org.apache.spark.h2o.ui

import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
  * Enrich Spark UI by a Sparkling Water specific tab.
  */
private[h2o] class SparklingWaterUITab(val provider: SparklingWaterInfoProvider, val parent: SparkUI)
  extends SparkUITab(parent, "sparkling-water") {

  override val name = "Sparkling Water \u2728"

  attachPage(SparklingWaterInfoPage(this))
  parent.attachTab(this)

  def getSparkUser: String = parent.getSparkUser
}
