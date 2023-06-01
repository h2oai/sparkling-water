package ai.h2o.sparkling

import java.util.TimeZone

import org.apache.spark.sql.catalyst.util.DateTimeUtils

object SparkTimeZone {
  def current(): TimeZone = DateTimeUtils.defaultTimeZone
}
