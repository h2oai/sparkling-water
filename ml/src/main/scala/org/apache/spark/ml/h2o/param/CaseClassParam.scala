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
import org.json4s._
import org.json4s.jackson.Serialization.{read, write}

class CaseClassParam[T <: AnyRef with Product : Manifest](parent: Params, name: String, doc: String, isValid: T => Boolean)
  extends Param[T](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  @transient private implicit val formats = DefaultFormats

  override def jsonEncode(value: T): String = write[T](value)

  override def jsonDecode(json: String): T = {
    if (json == null) null.asInstanceOf[T] else read[T](json)
  }
}
