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

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString, JValue}

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag, classTag}

abstract class EnumArrayParam[T: ClassTag](parent: Params, name: String, doc: String, isValid: Array[T] => Boolean)
  extends Param[Array[T]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[T]): ParamPair[Array[T]] = w(value.asScala.toArray)

  override def jsonEncode(value: Array[T]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JArray(value.map(algo => JString(algo.toString)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Array[T] = {
    parse(json) match {
      case JArray(values) =>
        values.map {
          case JString(x) =>
            import scala.reflect._
            val method = classTag[T].runtimeClass.getMethod("valueOf", classOf[String])
            method.invoke(null, x).asInstanceOf[T]
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to ${classTag[T].runtimeClass.getName}.")
        }.toArray.asInstanceOf[Array[T]]
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[${classTag[T].runtimeClass.getName}].")
    }
  }
}
