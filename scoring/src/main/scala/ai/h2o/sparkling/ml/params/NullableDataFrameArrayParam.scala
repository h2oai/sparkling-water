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

  private lazy val serializerClassName = parent.getDataFrameSerializer()
  private lazy val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[DataFrame]): ParamPair[Array[DataFrame]] =
    w(value.asScala.toArray)

  override def jsonEncode(value: Array[DataFrame]): String = {
    val encoded = if (value == null) {
      JNull
    } else {
      JArray(value.toList.map(jsonEncodeDF))
    }

    compact(render(encoded))
  }

  def jsonEncodeDF(dataFrame: DataFrame): JValue = {
    if (dataFrame == null) {
      JNull
    } else {
      val serializedValue = serializer.serialize(dataFrame)
      JObject(JField("serializer", JString(serializerClassName)), JField("value", serializedValue))
    }
  }

  override def jsonDecode(json: String): Array[DataFrame] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map(jsonDecodeDF).toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Double].")
    }
  }

  def jsonDecodeDF(json: JValue): DataFrame = {
    json match {
      case JNull =>
        null
      case JObject(fields) =>
        val fieldsMap = fields.toMap[String, JValue]
        val deserializerClassName = fieldsMap("serializer").asInstanceOf[JString].values
        val deserializer = Class.forName(deserializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
        val serializedValue = fieldsMap("value")
        deserializer.deserialize(serializedValue)
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to DataFrame.")
    }
  }
}
