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

package org.apache.spark.sql

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.Base64

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JBool, JDecimal, JDouble, JField, JInt, JNull, JObject, JString, JValue}

import scala.collection.mutable

object JSONRowConverter {

  private def iteratorToJsonArray(iterator: Iterator[_], elementType: DataType): JArray = {
    JArray(iterator.map(valueToJson(_, elementType)).toList)
  }

  private def valueToJson(value: Any, dataType: DataType): JValue = (value, dataType) match {
    case (null, _) => JNull
    case (b: Boolean, _) => JBool(b)
    case (b: Byte, _) => JInt(BigInt(b))
    case (s: Short, _) => JInt(BigInt(s))
    case (i: Int, _) => JInt(i)
    case (l: Long, _) => JInt(l)
    case (f: Float, _) => JDouble(f)
    case (d: Double, _) => JDouble(d)
    case (d: BigDecimal, _) => JDecimal(d)
    case (d: java.math.BigDecimal, _) => JDecimal(d)
    case (d: Decimal, _) => JDecimal(d.toBigDecimal)
    case (s: String, _) => JString(s)
    case (b: Array[Byte], BinaryType) => JString(Base64.getEncoder.encodeToString(b))
    case (d: LocalDate, _) => JInt(d.toEpochDay)
    case (d: Date, _) => JInt(d.toLocalDate.toEpochDay)
    case (i: Instant, _) => JInt(i.toEpochMilli)
    case (t: Timestamp, _) => JInt(t.toInstant.toEpochMilli)
    case (a: Array[_], ArrayType(elementType, _)) =>
      iteratorToJsonArray(a.iterator, elementType)
    case (s: Seq[_], ArrayType(elementType, _)) =>
      iteratorToJsonArray(s.iterator, elementType)
    case (m: Map[String @unchecked, _], MapType(StringType, valueType, _)) =>
      new JObject(m.toList.sortBy(_._1).map {
        case (k, v) => k -> valueToJson(v, valueType)
      })
    case (m: Map[_, _], MapType(keyType, valueType, _)) =>
      new JArray(m.iterator.map {
        case (k, v) =>
          new JObject("key" -> valueToJson(k, keyType) :: "value" -> valueToJson(v, valueType) :: Nil)
      }.toList)
    case (r: Row, s: StructType) => rowToJsonValue(r, s)
    case _ =>
      throw new IllegalArgumentException(
        s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType to JSON.")
  }

  private def jsonToValue(value: Any, dataType: DataType): Any = (value, dataType) match {
    case (JNull, _) => null
    case (JBool(b), _) => b
    case (JInt(b), ByteType) => b.toByte
    case (JInt(s), ShortType) => s.toShort
    case (JInt(i), IntegerType) => i.toInt
    case (JInt(l), LongType) => l.toLong
    case (JDouble(f), FloatType) => f.toFloat
    case (JString(s), FloatType) => s.toFloat
    case (JDouble(d), DoubleType) => d
    case (JString(s), DoubleType) => s.toDouble
    case (JDecimal(d), _: DecimalType) => d
    case (JString(s), _: DecimalType) => BigDecimal(s)
    case (JString(s), StringType) => s
    case (JString(b), BinaryType) => Base64.getDecoder.decode(b)
    case (JInt(d), DateType) => Date.valueOf(LocalDate.ofEpochDay(d.toLong))
    case (JInt(i), TimestampType) => Timestamp.from(Instant.ofEpochMilli(i.toLong))
    case (JArray(a), ArrayType(elementType, _)) => a.map(jsonToValue(_, elementType)).toArray
    case (o: JObject, MapType(StringType, valueType, _)) => o.obj.toMap.mapValues(jsonToValue(_, valueType))
    case (JArray(a), MapType(keyType, valueType, _)) =>
      a.map {
        case o: JObject =>
          val objMap = o.obj.toMap
          jsonToValue(objMap("key"), keyType) -> jsonToValue(objMap("value"), valueType)
      }.toMap
    case (r: Row, s: StructType) => rowToJsonValue(r, s)
    case _ =>
      throw new IllegalArgumentException(
        s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType from JSON.")
  }

  def rowToJsonValue(row: Row, schema: StructType): JValue = {
    var i = 0
    var elements = new mutable.ListBuffer[JField]
    val len = row.length
    while (i < len) {
      val field = schema(i)
      elements += (field.name -> valueToJson(row(i), field.dataType))
      i += 1
    }
    new JObject(elements.toList)
  }

  def jsonValueToRow(value: JValue, schema: StructType): Row = {
    val fields = value.asInstanceOf[JObject].obj.toMap
    var i = 0
    val len = schema.length
    val values = new Array[Any](schema.length)

    while (i < len) {
      val field = schema(i)
      values(i) = jsonToValue(fields(field.name), field.dataType)
      i += 1
    }
    new GenericRowWithSchema(values, schema)
  }
}
