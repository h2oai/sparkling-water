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
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString}

import scala.collection.JavaConverters._

class NullableStringArrayParam(parent: Params, name: String, doc: String, isValid: Array[String] => Boolean)
  extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.String]): ParamPair[Array[String]] =
    w(value.asScala.map(_.asInstanceOf[String]).toArray)

  override def jsonEncode(value: Array[String]): String = {
    if (value == null) {
      compact(render(JNull))
    } else {
      import org.json4s.JsonDSL._
      compact(render(value.toSeq.map {
        JString(_)
      }))
    }
  }

  override def jsonDecode(json: String): Array[String] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JString(s) =>
            s
          case jValue =>
            throw new IllegalArgumentException(s"Cannot decode $jValue to String.")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[String].")
    }
  }
}
