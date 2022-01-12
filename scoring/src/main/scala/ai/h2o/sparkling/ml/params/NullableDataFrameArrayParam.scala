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

import ai.h2o.sparkling.utils.DataFrameSerializer
import org.apache.spark.ml.param.{Param, ParamPair}
import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.{JArray, JField, JNull, JObject, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableDataFrameArrayParam(
    parent: HasDataFrameSerializer,
    name: String,
    doc: String,
    isValid: Array[DataFrame] => Boolean)
  extends Param[Array[DataFrame]](parent, name, doc, isValid) {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[DataFrame]): ParamPair[Array[DataFrame]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[DataFrame]): String = {
    val encoded = if (value == null) {
      JNull
    } else {
      val serializerClassName = parent.getDataFrameSerializer()
      val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]

      JObject(
        JField("serializer", JString(serializerClassName)),
        JField("dataframes", JArray(value.toList.map(jsonEncodeDF(_, serializer)))))
    }

    compact(render(encoded))
  }

  def jsonEncodeDF(dataFrame: DataFrame, serializer: DataFrameSerializer): JValue = {
    if (dataFrame == null) {
      JNull
    } else {
      serializer.serialize(dataFrame)
    }
  }

  override def jsonDecode(json: String): Array[DataFrame] = {
    parse(json) match {
      case JNull =>
        null
      case JObject(fields) =>
        val fieldsMap = fields.toMap[String, JValue]

        val serializerClassName = fieldsMap("serializer").asInstanceOf[JString].values
        val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]

        val serializedDataFrames = fieldsMap("dataframes")
        serializedDataFrames.children.map(jsonDecodeDF(_, serializer)).toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[DataFrame].")
    }
  }

  def jsonDecodeDF(json: JValue, serializer: DataFrameSerializer): DataFrame = {
    json match {
      case JNull =>
        null
      case _ =>
        serializer.deserialize(json)
    }
  }
}
