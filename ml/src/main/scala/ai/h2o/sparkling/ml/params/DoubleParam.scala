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
package ai.h2o.sparkling.ml.params

import org.json4s.{JDouble, JString, JValue}

object DoubleParam {
  /** Encodes a param value into JValue. */
  def jValueEncode(value: Double): JValue = {
    value match {
      case _ if value.isNaN =>
        JString("NaN")
      case Double.NegativeInfinity =>
        JString("-Inf")
      case Double.PositiveInfinity =>
        JString("Inf")
      case _ =>
        JDouble(value)
    }
  }

  /** Decodes a param value from JValue. */
  def jValueDecode(jValue: JValue): Double = {
    jValue match {
      case JString("NaN") =>
        Double.NaN
      case JString("-Inf") =>
        Double.NegativeInfinity
      case JString("Inf") =>
        Double.PositiveInfinity
      case JDouble(x) =>
        x
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Double.")
    }
  }
}
