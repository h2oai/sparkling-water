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
package water.support

/**
 * Simple support for parsing data.
 */
trait ParseSupport {

  /**
   * Parse string as an integer
   *
   * @param s string to parse
   * @return option with parsed int or empty option in case the parse failed
   */
  def int(s: String): Option[Int] = if (isValid(s)) parseInt(s) else None

  /**
   * Parse string as a long
   *
   * @param s string to parse
   * @return option with parsed long or empty option in case the parse failed
   */
  def long(s: String): Option[Long] = if (isValid(s)) parseLong(s) else None

  /**
   * Parse string as a float
   *
   * @param s string to parse
   * @return option with parsed float or empty option in case the parse failed
   */
  def float(s: String): Option[Float] = if (isValid(s)) parseFloat(s) else None

  /**
   * Parse string as a string
   *
   * @param s string to parse
   * @return option with parsed string or empty option in case the parse failed
   */
  def str(s: String): Option[String] = if (isValid(s)) Option(s) else None

  /**
   * Parse string as a boolean
   *
   * @param s string to parse
   * @return option with parsed boolean or empty option in case the parse failed
   */
  def bool(s: String): Option[Boolean] = if (isValid(s)) parseBool(s) else None

  /**
   * Parse string as an integer
   *
   * @param s string to parse
   * @return option with parsed int or empty option in case the parse failed
   */
  def parseInt(s: String): Option[Int] =
    try {
      Option(s.trim().toInt)
    } catch {
      case _: NumberFormatException => None
    }

  /**
   * Parse string as a long
   *
   * @param s string to parse
   * @return option with parsed long or empty option in case the parse failed
   */
  def parseLong(s: String): Option[Long] =
    try {
      Option(s.trim().toLong)
    } catch {
      case _: NumberFormatException => None
    }

  /**
   * Parse string as a float
   *
   * @param s string to parse
   * @return option with parsed float or empty option in case the parse failed
   */
  def parseFloat(s: String): Option[Float] =
    try {
      Option(s.trim().toFloat)
    } catch {
      case _: NumberFormatException => None
    }

  /**
   * Parse string as a boolean
   *
   * @param s string to parse
   * @return option with parsed boolean or empty option in case the parse failed
   */
  def parseBool(s: String): Option[Boolean] = s.trim().toLowerCase match {
    case "true" | "yes" => Option(true)
    case "false" | "no" => Option(false)
    case _ => None
  }

  /**
   * Check if the value is NA
   *
   * @param s string to check if it represents NA
   * @return true or false whether the string represents NA
   */
  def isNA(s: String): Boolean = s == null || s.isEmpty || (s.trim.toLowerCase match {
    case "na" => true
    case "n/a" => true
    case _ => false
  })

  /** Check if the value is valid, such as if it's not NA */
  def isValid(s: String): Boolean = !isNA(s)
}

object ParseSupport extends ParseSupport
