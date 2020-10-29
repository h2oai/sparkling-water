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

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

import scala.collection.JavaConverters._

class DictionaryParam(parent: Params, name: String, doc: String, isValid: java.util.Map[String, Double] => Boolean)
  extends Param[java.util.Map[String, Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  implicit def formats = DefaultFormats

  override def jsonEncode(dictionary: java.util.Map[String, Double]): String = write(dictionary.asScala)

  override def jsonDecode(json: String): java.util.Map[String, Double] = read[Map[String, Double]](json).asJava
}
