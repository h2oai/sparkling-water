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

package ai.h2o.sparkling.backend.utils

import java.net.URLEncoder

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.immutable.Map

private[sparkling] trait RestEncodingUtils {
  protected def stringifyPrimitiveParam(value: Any): String = {
    val charset = "UTF-8"
    value match {
      case v: Boolean => v.toString
      case v: Byte => v.toString
      case v: Int => v.toString
      case v: Long => v.toString
      case v: Float => v.toString
      case v: Double => v.toString
      case v: String => URLEncoder.encode(v, charset)
      case unknown => throw new RuntimeException(s"Unsupported parameter '$unknown' of type ${unknown.getClass}")
    }
  }

  protected def isPrimitiveType(value: Any): Boolean = {
    value match {
      case _: Boolean => true
      case _: Byte => true
      case _: Int => true
      case _: Long => true
      case _: Float => true
      case _: Double => true
      case _: String => true
      case _ => false
    }
  }

  protected def stringifyArray(arr: Array[_]): String = {
    arr.map(stringify).mkString("[", ",", "]")
  }

  protected def stringifyMap(map: Map[_, _]): String = {
    val items = for ((key, value) <- map if value != null) yield s"{'key': $key, 'value': ${stringify(value)}}"
    stringifyArray(items.toArray)
  }

  protected def stringifyPair(pair: (_, _)): String = {
    s"""{"a": ${stringify(pair._1)}, "b": ${stringify(pair._2)}}"""
  }

  protected def stringify(value: Any): String = {
    import scala.collection.JavaConverters._
    value match {
      case map: java.util.AbstractMap[_, _] => stringifyMap(map.asScala.toMap)
      case map: Map[_, _] => stringifyMap(map)
      case arr: Array[_] => stringifyArray(arr)
      case pair: (_, _) => stringifyPair(pair)
      case primitive if isPrimitiveType(primitive) => stringifyPrimitiveParam(primitive)
      case unknown => throw new RuntimeException(s"Unsupported parameter '$unknown' of type ${unknown.getClass}")
    }
  }

  protected def stringifyParams(params: Map[String, Any] = Map.empty, encodeParamsAsJson: Boolean = false): String = {
    if (encodeParamsAsJson) {
      new ObjectMapper().registerModule(DefaultScalaModule).writeValueAsString(params)
    } else {
      val stringifiedMap = for ((key, value) <- params if value != null) yield s"$key=${stringify(value)}"
      stringifiedMap.mkString("&")
    }
  }
}
