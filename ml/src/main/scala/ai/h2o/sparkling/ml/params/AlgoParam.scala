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

import ai.h2o.sparkling.ml.algos._
import hex.Model
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.JNull
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class AlgoParam(parent: Params, name: String, doc: String, isValid: H2OAlgorithm[_ <: Model.Parameters] => Boolean)
  extends Param[H2OAlgorithm[_ <: Model.Parameters]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: H2OAlgorithm[_ <: Model.Parameters]): String = {
    val encoded = if (value == null) {
      JNull
    } else {
      val algoClassName = value.getClass.getName
      val uid = value.uid
      val params = value.extractParamMap().toSeq
      val jsonParams = render(params.map {
        case ParamPair(p, v) =>
          p.name -> parse(p.jsonEncode(v))
      }.toList)

      ("class" -> algoClassName) ~ ("uid" -> uid) ~ ("paramMap" -> jsonParams)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): H2OAlgorithm[_ <: Model.Parameters] = {
    val parsed = parse(json)

    if (parsed == JNull) {
      null
    } else {
      implicit val format = DefaultFormats
      val className = (parsed \ "class").extract[String]
      val uid = (parsed \ "uid").extract[String]
      val jsonParams = parsed \ "paramMap"

      val algo = createH2OAlgoInstance(className, uid)
      jsonParams match {
        case JObject(pairs) =>
          pairs.foreach {
            case (paramName, jsonValue) =>
              val param = algo.getParam(paramName)
              val value = param.jsonDecode(compact(render(jsonValue)))
              algo.set(param, value)
          }
        case _ => throw new RuntimeException("Invalid JSON parameters")
      }
      algo
    }
  }

  private def createH2OAlgoInstance(algoName: String, uid: String): H2OAlgorithm[_ <: Model.Parameters] = {
    val cls = Class.forName(algoName)
    cls.getConstructor(classOf[String]).newInstance(uid).asInstanceOf[H2OAlgorithm[_ <: Model.Parameters]]
  }
}
