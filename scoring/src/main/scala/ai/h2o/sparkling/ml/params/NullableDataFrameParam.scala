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

import java.io.ByteArrayInputStream
import java.util.Base64

import ai.h2o.sparkling.utils.ScalaUtils.withResource
import ai.h2o.sparkling.utils.{CompatibilityObjectInputStream, DataFrameSerializer, SparkSessionUtils}
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.JsonAST.{JField, JNull, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class NullableDataFrameParam(parent: HasDataFrameSerializer, name: String, doc: String, isValid: DataFrame => Boolean)
  extends Param[DataFrame](parent, name, doc, isValid) {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) =
    this(parent, name, doc, (_: DataFrame) => true)

  override def jsonEncode(dataFrame: DataFrame): String = {
    val ast = if (dataFrame == null) {
      JNull
    } else {
      val serializerClassName = parent.getDataFrameSerializer()
      val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
      val serializedValue = serializer.serialize(dataFrame)
      JObject(JField("serializer", JString(serializerClassName)), JField("value", serializedValue))
    }
    compact(render(ast))
  }

  override def jsonDecode(json: String): DataFrame = {
    parse(json) match {
      case JNull =>
        null
      case JObject(fields) =>
        val fieldsMap = fields.toMap[String, JValue]
        val serializerClassName = fieldsMap("serializer").asInstanceOf[JString].values
        val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
        val serializedValue = fieldsMap("value")
        serializer.deserialize(serializedValue)
      case JString(data) =>
        val bytes = Base64.getDecoder().decode(data)
        withResource(new ByteArrayInputStream(bytes)) { byteStream =>
          withResource(new CompatibilityObjectInputStream(byteStream)) { objectStream =>
            val schema = objectStream.readObject().asInstanceOf[StructType]
            val rows = objectStream.readObject().asInstanceOf[java.util.List[Row]]
            SparkSessionUtils.active.createDataFrame(rows, schema)
          }
        }
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to DataFrame.")
    }
  }
}
