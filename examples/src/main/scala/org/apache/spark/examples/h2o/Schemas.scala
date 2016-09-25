/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.examples.h2o

import org.apache.spark.sql.Row
import org.joda.time.{DateTimeZone, MutableDateTime}

/** Prostate schema definition. */
case class Prostate(ID      :Option[Long]  ,
                    CAPSULE :Option[Int]  ,
                    AGE     :Option[Int]  ,
                    RACE    :Option[Int]  ,
                    DPROS   :Option[Int]  ,
                    DCAPS   :Option[Int]  ,
                    PSA     :Option[Float],
                    VOL     :Option[Float],
                    GLEASON :Option[Int]  ) {
  def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
}

/** A dummy csv parser for prostate dataset. */
object ProstateParse extends Serializable {
  val EMPTY = Prostate(None, None, None, None, None, None, None, None, None)
  def apply(row: Array[String]): Prostate = {
    import water.support.ParseSupport._
    if (row.length < 9) EMPTY
    else Prostate(long(row(0)), int(row(1)), int(row(2)), int(row(3)), int(row(4)), int(row(5)), float(row(6)), float(row(7)), int(row(8)) )
  }
}

/** Airlines schema definition. */
class Airlines (val Year              :Option[Int],
                val Month             :Option[Int],
                val DayofMonth        :Option[Int],
                val DayOfWeek         :Option[Int],
                val DepTime           :Option[Int],
                val CRSDepTime        :Option[Int],
                val ArrTime           :Option[Int],
                val CRSArrTime        :Option[Int],
                val UniqueCarrier     :Option[String],
                val FlightNum         :Option[Int],
                val TailNum           :Option[Int],
                val ActualElapsedTime :Option[Int],
                val CRSElapsedTime    :Option[Int],
                val AirTime           :Option[Int],
                val ArrDelay          :Option[Int],
                val DepDelay          :Option[Int],
                val Origin            :Option[String],
                val Dest              :Option[String],
                val Distance          :Option[Int],
                val TaxiIn            :Option[Int],
                val TaxiOut           :Option[Int],
                val Cancelled         :Option[Int],
                val CancellationCode  :Option[Int],
                val Diverted          :Option[Int],
                val CarrierDelay      :Option[Int],
                val WeatherDelay      :Option[Int],
                val NASDelay          :Option[Int],
                val SecurityDelay     :Option[Int],
                val LateAircraftDelay :Option[Int],
                val IsArrDelayed      :Option[Boolean],
                val IsDepDelayed      :Option[Boolean]) extends Product with Serializable {

  /*
  def this() = this(None,None,None,None,None,None,None,None,None,
    None,None,None,None,None,None,None,None,None,
    None,None,None,None,None,None,None,None,None,
    None,None,None,None) */
  override def canEqual(that: Any):Boolean = that.isInstanceOf[Airlines]
  override def productArity: Int = 31
  override def productElement(n: Int) = n match {
    case  0 => Year
    case  1 => Month
    case  2 => DayofMonth
    case  3 => DayOfWeek
    case  4 => DepTime
    case  5 => CRSDepTime
    case  6 => ArrTime
    case  7 => CRSArrTime
    case  8 => UniqueCarrier
    case  9 => FlightNum
    case 10 => TailNum
    case 11 => ActualElapsedTime
    case 12 => CRSElapsedTime
    case 13 => AirTime
    case 14 => ArrDelay
    case 15 => DepDelay
    case 16 => Origin
    case 17 => Dest
    case 18 => Distance
    case 19 => TaxiIn
    case 20 => TaxiOut
    case 21 => Cancelled
    case 22 => CancellationCode
    case 23 => Diverted
    case 24 => CarrierDelay
    case 25 => WeatherDelay
    case 26 => NASDelay
    case 27 => SecurityDelay
    case 28 => LateAircraftDelay
    case 29 => IsArrDelayed
    case 30 => IsDepDelayed
    case  _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def toString: String = {
    val sb = new StringBuffer
    for( i <- 0 until productArity )
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
}

/** A dummy csv parser for airlines dataset. */
object AirlinesParse extends Serializable {

  def apply(row: Row): Airlines = {
    apply(row.mkString(",").split(","))
  }

  def apply(row: Array[String]): Airlines = {
    import water.support.ParseSupport._
    new Airlines(int (row( 0)), // Year
      int (row( 1)), // Month
      int (row( 2)), // DayofMonth
      int (row( 3)), // DayOfWeek
      int (row( 4)), // DepTime
      int (row( 5)), // CRSDepTime
      int (row( 6)), // ArrTime
      int (row( 7)), // CRSArrTime
      str (row( 8)), // UniqueCarrier
      int (row( 9)), // FlightNum
      int (row(10)), // TailNum
      int (row(11)), // ActualElapsedTime
      int (row(12)), // CRSElapsedTime
      int (row(13)), // AirTime
      int (row(14)), // ArrDelay
      int (row(15)), // DepDelay
      str (row(16)), // Origin
      str (row(17)), // Dest
      int (row(18)), // Distance
      int (row(19)), // TaxiIn
      int (row(20)), // TaxiOut
      int (row(21)), // Cancelled
      int (row(22)), // CancellationCode
      int (row(23)), // Diverted
      int (row(24)), // CarrierDelay
      int (row(25)), // WeatherDelay
      int (row(26)), // NASDelay
      int (row(27)), // SecurityDelay
      int (row(28)), // LateAircraftDelay
      bool(row(29)), // IsArrDelayed
      bool(row(30))) // IsDepDelayed
  }
}

case class Weather( val Year   : Option[Int],
                    val Month  : Option[Int],
                    val Day    : Option[Int],
                    val TmaxF  : Option[Int],   // Max temperatur in F
                    val TminF  : Option[Int],   // Min temperatur in F
                    val TmeanF : Option[Float], // Mean temperatur in F
                    val PrcpIn : Option[Float], // Precipitation (inches)
                    val SnowIn : Option[Float], // Snow (inches)
                    val CDD    : Option[Float], // Cooling Degree Day
                    val HDD    : Option[Float], // Heating Degree Day
                    val GDD    : Option[Float]) // Growing Degree Day
{
  def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
}

object WeatherParse extends Serializable {
  import water.support.ParseSupport._
  type DATE = (Option[Int], Option[Int], Option[Int]) // Year, Month, Day

  val datePattern1 = """(\d\d\d\d)-(\d\d)-(\d\d)""".r("year", "month", "day")
  val datePattern2 = """(\d+)/(\d+)/(\d\d\d\d)""".r("month", "day", "year")

  def apply(row: Array[String]): Weather = {
    val b = if (row.length==9) 0 else 1 // base index
    val d = parseDate(row(b)).getOrElse( (None, None, None) )
    Weather(d._1, d._2, d._3,
            int  (row(b + 1)),
            int  (row(b + 2)),
            float(row(b + 3)),
            float(row(b + 4)),
            float(row(b + 5)),
            float(row(b + 6)),
            float(row(b + 7)),
            float(row(b + 8))
    )
  }

  private def parseDate(s: String): Option[DATE] =
    s match {
      case datePattern1(y,m,d) => Some( (int(y),int(m),int(d)) )
      case datePattern2(m,d,y) => Some( (int(y),int(m),int(d)) )
      case _ => None
    }
}

case class NYWeather(val Days: Option[Long],
                     val HourLocal   : Option[Int],
                     val DewPoint    : Option[Float],
                     val HumidityFraction  : Option[Float],
                     val Prcp1Hour   : Option[Float],
                     val Temperature : Option[Float],
                     val WeatherCode1: Option[String])
{
  def isWrongRow():Boolean = (0 until productArity).map( idx => productElement(idx)).forall(e => e==None)
}

object NYWeatherParse extends Serializable {
  import water.support.ParseSupport._

  def apply(row: Array[String]): NYWeather = {
    val yearLocal =  float(row(0))
    val monthLocal = float(row(1))
    val dayLocal =   float(row(2))
    val hourLocal =  float(row(3))
    val msec:Option[Long] = if (yearLocal.isDefined && monthLocal.isDefined && dayLocal.isDefined && hourLocal.isDefined) {
      Some(new MutableDateTime(yearLocal.get.toInt,
        monthLocal.get.toInt, dayLocal.get.toInt, hourLocal.get.toInt, 0, 0, 0, DateTimeZone.UTC).getMillis)
      } else {
        None
      }
    // Compute days since epoch
    val days = if (msec.isDefined) Some(msec.get / (1000*60*60*24)) else None

    NYWeather(
      days,
      if (hourLocal.isDefined) Some(hourLocal.get.toInt) else None,
      float(row(23)),
      float(row(24)),
      float(row(25)),
      float(row(30)),
      str(row(33))
    )
  }
}
