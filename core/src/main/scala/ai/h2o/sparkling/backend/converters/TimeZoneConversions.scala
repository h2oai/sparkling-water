package ai.h2o.sparkling.backend.converters

import java.sql.{Date, Timestamp}
import java.time.ZoneId
import java.util.TimeZone

import org.apache.spark.sql.catalyst.util.DateTimeUtils

trait TimeZoneConversions {
  protected def sparkTimeZone: TimeZone

  def fromSparkTimeZoneToUTC(timestamp: Long): Long = DateTimeUtils.fromUTCTime(timestamp, sparkTimeZone.getID)

  def fromSparkTimeZoneToUTC(timestamp: Timestamp): Long = fromSparkTimeZoneToUTC(timestamp.getTime * 1000) / 1000

  def fromSparkTimeZoneToUTC(date: Date): Long = {
    DateTimeUtils.fromUTCTime(date.getTime * 1000, ZoneId.systemDefault().getId) / 1000
  }

  def fromUTCToSparkTimeZone(timestamp: Long): Long = DateTimeUtils.toUTCTime(timestamp, sparkTimeZone.getID)
}

class TimeZoneConverter(val sparkTimeZone: TimeZone) extends TimeZoneConversions
