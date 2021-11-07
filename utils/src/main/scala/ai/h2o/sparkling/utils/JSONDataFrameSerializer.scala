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

package ai.h2o.sparkling.utils

import org.apache.spark.sql.{DataFrame, DataTypeExtensions, Encoders}
import org.apache.spark.sql.DataTypeExtensions._
import org.apache.spark.sql.types.StructType
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class JSONDataFrameSerializer extends DataFrameSerializer {
  def serialize(df: DataFrame): JValue = {
    val schemaJsonValue = df.schema.jsonValue
    val rows = df.toJSON.collect().map(parse(_)).toList
    val rowsJsonArray = JArray(rows)
    JObject(JField("schema", schemaJsonValue), JField("rows", rowsJsonArray))
  }

  def deserialize(input: JValue): DataFrame = {
    val objMap = input.asInstanceOf[JObject].obj.toMap
    val schema = DataTypeExtensions.jsonToDateType(objMap("schema")).asInstanceOf[StructType]
    val rows = objMap("rows").asInstanceOf[JArray].arr.map(v => compact(render(v)))
    val spark = SparkSessionUtils.active
    val jsonDataset = spark.createDataset(rows)(Encoders.STRING)
    SparkSessionUtils.active.read.schema(schema).json(jsonDataset)
  }
}
