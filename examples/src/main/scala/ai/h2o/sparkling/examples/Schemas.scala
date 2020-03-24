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

package ai.h2o.sparkling.examples

import org.joda.time.{DateTimeZone, MutableDateTime}

case class NYWeather(val Days: Option[Long],
                     val HourLocal: Option[Int],
                     val DewPoint: Option[Float],
                     val HumidityFraction: Option[Float],
                     val Prcp1Hour: Option[Float],
                     val Temperature: Option[Float],
                     val WeatherCode1: Option[String]) {
  def isWrongRow(): Boolean = (0 until productArity).map(idx => productElement(idx)).forall(e => e == None)
}

object NYWeatherParse extends Serializable {

  import water.support.ParseSupport._

  def apply(row: Array[String]): NYWeather = {
    val yearLocal = float(row(0))
    val monthLocal = float(row(1))
    val dayLocal = float(row(2))
    val hourLocal = float(row(3))
    val msec: Option[Long] = if (yearLocal.isDefined && monthLocal.isDefined && dayLocal.isDefined && hourLocal.isDefined) {
      Some(new MutableDateTime(yearLocal.get.toInt,
        monthLocal.get.toInt, dayLocal.get.toInt, hourLocal.get.toInt, 0, 0, 0, DateTimeZone.UTC).getMillis)
    } else {
      None
    }
    // Compute days since epoch
    val days = if (msec.isDefined) Some(msec.get / (1000 * 60 * 60 * 24)) else None

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
