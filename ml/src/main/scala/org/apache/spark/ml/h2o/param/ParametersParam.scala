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

import hex.Model
import org.apache.spark.ml.param.{Param, Params}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer

import scala.reflect.ClassTag


class ParametersParam[T <: Model.Parameters](parent: Params, name: String, doc: String, isValid: T => Boolean)(implicit tag: ClassTag[T])
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String)(implicit tag: ClassTag[T]) =
    this(parent, name, doc, _ => true)(tag)

  override def jsonEncode(value: T): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      val ab = new AutoBuffer()
      value.write(ab)
      val bytes = ab.buf()
      JArray(bytes.toSeq.map(JInt(_)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): T = {
    parse(json) match {
      case JNull =>
        null.asInstanceOf[T]
      case JArray(values) =>
        val bytes = values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray

        val params = tag.runtimeClass.newInstance().asInstanceOf[T]
        params.read(new AutoBuffer(bytes))
        params
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to ${tag.runtimeClass}.")
    }
  }
}