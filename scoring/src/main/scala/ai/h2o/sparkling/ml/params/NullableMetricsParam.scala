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

import ai.h2o.sparkling.ml.metrics.H2OMetrics
import ai.h2o.sparkling.ml.utils.H2OParamsReader
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.apache.spark.ml.util.expose.DefaultParamsReader.Metadata
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNull
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class NullableMetricsParam(parent: Params, name: String, doc: String, isValid: H2OMetrics => Boolean)
  extends Param[H2OMetrics](parent, name, doc, isValid)
  with H2OParamsReader[H2OMetrics] {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, (_: H2OMetrics) => true)

  override def jsonEncode(metrics: H2OMetrics): String = {
    val ast = if (metrics == null) {
      JNull
    } else {
      val params = metrics.getParamMap().toSeq
      val jsonParams = render(params.map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }.toList)
      val metadataAst = ("class" -> metrics.getClass.getName) ~ ("uid" -> metrics.uid) ~ ("paramMap" -> jsonParams)
      metadataAst
    }
    compact(render(ast))
  }

  override def jsonDecode(json: String): H2OMetrics = {
    parse(json) match {
      case JNull =>
        null
      case ast =>
        implicit val format = DefaultFormats
        val className = (ast \ "class").extract[String]
        val uid = (ast \ "uid").extract[String]
        val params = ast \ "paramMap"
        val metadata = Metadata(className, uid, params, json)
        load(metadata)
    }
  }
}
