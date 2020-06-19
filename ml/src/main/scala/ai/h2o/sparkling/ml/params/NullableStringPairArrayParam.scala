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

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer

import scala.collection.JavaConverters._

class NullableStringPairArrayParam(
    parent: Params,
    name: String,
    doc: String,
    isValid: Array[(String, String)] => Boolean)
  extends Param[Array[(String, String)]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[Array[Object]]): ParamPair[Array[(String, String)]] = {
    w(value.asScala.map(array => (array(0).toString, array(1).toString)).toArray)
  }

  override def jsonEncode(value: Array[(String, String)]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      val ab = new AutoBuffer()
      ab.putASer(value.asInstanceOf[Array[AnyRef]])
      val bytes = ab.buf()
      JArray(bytes.toSeq.map(JInt(_)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Array[(String, String)] = {
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
        ab.getASer[(String, String)](classOf[(String, String)])
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[(String, String)].")
    }
  }
}
