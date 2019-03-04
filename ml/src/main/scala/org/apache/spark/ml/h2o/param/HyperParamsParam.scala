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
package org.apache.spark.ml.h2o.param

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.{JNull, JValue}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import water.AutoBuffer

class HyperParamsParam(parent: Params, name: String, doc: String, isValid: Map[String, Array[AnyRef]] => Boolean)
  extends Param[Map[String, Array[AnyRef]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: Map[String, Array[AnyRef]]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      val ab = new AutoBuffer()
      ab.put1(value.size)
      value.foreach{ case (k, v) =>
        ab.putStr(k)
        ab.putASer(v)
      }
      val bytes = ab.buf()
      JArray(bytes.toSeq.map(JInt(_)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Map[String, Array[AnyRef]] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        val bytes = values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray
        val ab = new AutoBuffer(bytes)
        val numParams = ab.get1()
        (0 until numParams).map{ _ => (ab.getStr, ab.getASer[AnyRef](classOf[AnyRef]))}.toMap
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Map[String, Array[AnyRef]].")
    }
  }
}
