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
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableAlgoArrayParam(parent: Params, name: String, doc: String, isValid: Array[H2OAlgorithm[_ <: Model.Parameters]] => Boolean)
  extends Param[Array[H2OAlgorithm[_ <: Model.Parameters]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[H2OAlgorithm[_ <: Model.Parameters]]): ParamPair[Array[H2OAlgorithm[_ <: Model.Parameters]]] = {
    w(value.asScala.toArray)
  }

  override def jsonEncode(algos: Array[H2OAlgorithm[_ <: Model.Parameters]]): String = {
    val encoded = if (algos == null) {
      JNull
    } else {
      JArray(algos.toList.map(AlgoParam.jsonEncode(_)))
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Array[H2OAlgorithm[_ <: Model.Parameters]] = {
    val parsed = parse(json)

    parsed match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          AlgoParam.jsonDecode(_)
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[H2OAlgorithm[...]].")
    }
  }
}
