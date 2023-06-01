package ai.h2o.sparkling

import java.util.TimeZone

import ai.h2o.sparkling.utils.SparkSessionUtils

object SparkTimeZone {
  def current(): TimeZone = TimeZone.getTimeZone(SparkSessionUtils.active.sessionState.conf.sessionLocalTimeZone)
}
