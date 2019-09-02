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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import ai.h2o.sparkling.ml.algos._
import hex.Model
import org.apache.spark.ml.param.{Param, Params}
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}


class AlgoParam(parent: Params, name: String, doc: String, isValid: H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters] => Boolean)
  extends Param[H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters]): String = {
    val stream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    val bytes = stream.toByteArray
    val encoded: JValue = JArray(bytes.toSeq.map(JInt(_)).toList)
    compact(render(encoded))
  }

  override def jsonDecode(json: String): H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters] = {
    parse(json) match {
      case JArray(values) =>
        val bytes = values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray

        val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
        val value = ois.readObject
        ois.close()
        value.asInstanceOf[H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters]]
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to a H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters].")
    }
  }
}
