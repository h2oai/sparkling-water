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
import org.json4s.JNull
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableIntArrayParam(parent: Params, name: String, doc: String, isValid: Array[Int] => Boolean)
  extends Param[Array[Int]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Integer]): ParamPair[Array[Int]] =
    w(value.asScala.map(_.asInstanceOf[Int]).toArray)

  override def jsonEncode(value: Array[Int]): String = {
    if (value == null) {
      compact(render(JNull))
    } else {
      import org.json4s.JsonDSL._
      compact(render(value.toSeq.map(JInt(_))))
    }
  }

  override def jsonDecode(json: String): Array[Int] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JInt(x) => x.toInt
          case jValue => throw new IllegalArgumentException(s"Cannot decode $jValue to Int.")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Int].")
    }
  }
}
