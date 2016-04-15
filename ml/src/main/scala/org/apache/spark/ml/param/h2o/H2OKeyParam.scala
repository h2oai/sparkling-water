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

package org.apache.spark.ml.param.h2o

import org.apache.spark.ml.param._
import water.{Keyed, Key}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * H2O specific parameter for Spark ML transformers.
  *
  * It represents H2O key.
  */
class H2OKeyParam[T<:Keyed[T]](parent: Params, name: String, doc: String, isValid: Key[T] => Boolean)
  extends Param[Key[T]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, ParamValidators.alwaysTrue)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: Key[T]): ParamPair[Key[T]] = super.w(value)

  override def jsonEncode(value: Key[T]): String = {
    compact(render(H2OKeyParam.jValueEncode[T](value)))
  }

  override def jsonDecode(json: String): Key[T] = {
    H2OKeyParam.jValueDecode[T](parse(json))
  }
}

private object H2OKeyParam{

  /** Encodes a param value into JValue. */
  def jValueEncode[T<:Keyed[T]](value: Key[T]): JValue = {
    if (value == null){
      JNull
    }else{
      JString(value.toString)
    }
  }

  /** Decodes a param value from JValue. */
  def jValueDecode[T<:Keyed[T]](jValue: JValue): Key[T]= {
    jValue match {
      case JString(x) =>
        Key.make[T](x)
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $jValue to Key[T].")
    }

  }
}
