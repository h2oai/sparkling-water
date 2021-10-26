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

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.json4s.JsonAST.{JArray, JBool, JDecimal, JDouble, JField, JLong, JNull, JObject, JString, JValue}

import scala.collection.mutable

object JSONRowConverter {
  lazy val zoneId = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
  lazy val dateFormatter = DateFormatter.apply(zoneId)
  lazy val timestampFormatter = TimestampFormatter(zoneId)

  private def iteratorToJsonArray(iterator: Iterator[_], elementType: DataType): JArray = {
    JArray(iterator.map(valueToJson(_, elementType)).toList)
  }

  private def valueToJson(value: Any, dataType: DataType): JValue = (value, dataType) match {
    case (null, _) => JNull
    case (b: Boolean, _) => JBool(b)
    case (b: Byte, _) => JLong(b)
    case (s: Short, _) => JLong(s)
    case (i: Int, _) => JLong(i)
    case (l: Long, _) => JLong(l)
    case (f: Float, _) => JDouble(f)
    case (d: Double, _) => JDouble(d)
    case (d: BigDecimal, _) => JDecimal(d)
    case (d: java.math.BigDecimal, _) => JDecimal(d)
    case (d: Decimal, _) => JDecimal(d.toBigDecimal)
    case (s: String, _) => JString(s)
    case (b: Array[Byte], BinaryType) => JString(Base64.getEncoder.encodeToString(b))
    case (d: LocalDate, _) => JLong(d.toEpochDay)
    case (d: Date, _) => JLong(d.toLocalDate.toEpochDay)
    case (i: Instant, _) => JLong(i.toEpochMilli)
    case (t: Timestamp, _) => JLong(t.toInstant.toEpochMilli)
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
    case (v: Any, udt: UserDefinedType[Any @unchecked]) =>
      val dataType = udt.sqlType
      valueToJson(CatalystTypeConverters.convertToScala(udt.serialize(v), dataType), dataType)
    case _ =>
      throw new IllegalArgumentException(
        s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType to JSON.")
  }

  private def jsonToValue(value: Any, dataType: DataType): Any = (value, dataType) match {
    case (JNull, _) => null
    case (JBool(b), _) => b
    case (JLong(b), ByteType) => b.toByte
    case (JLong(s), ShortType) => s.toShort
    case (JLong(i), IntegerType) => i.toInt
    case (JLong(l), LongType) => l
    case (JDouble(f), FloatType) => f.toFloat
    case (JDouble(d), DoubleType) => d
    case (JDecimal(d), _) => d
    case (JString(s), StringType) => s
    case (JString(b), BinaryType) => Base64.getDecoder.decode(b)
    case (JLong(d), DateType) => LocalDate.ofEpochDay(d)
    case (JLong(i), TimestampType) => Instant.ofEpochMilli(i)
    case (JArray(a), ArrayType(elementType, _)) => a.map(jsonToValue(_, elementType)).toArray
    case (o: JObject, MapType(StringType, valueType, _)) => o.values.mapValues(jsonToValue(_, valueType))
    case (JArray(a), MapType(keyType, valueType, _)) =>
      a.map {
        case o: JObject =>
          val objMap = o.values
          jsonToValue(objMap("key"), keyType) -> jsonToValue(objMap("value"), valueType)
      }.toMap
    case (r: Row, s: StructType) => rowToJsonValue(r, s)
    case (v: Any, udt: UserDefinedType[Any @unchecked]) =>
      val dataType = udt.sqlType
      udt.deserialize(CatalystTypeConverters.convertToCatalyst(jsonToValue(v, dataType)))
    case _ =>
      throw new IllegalArgumentException(
        s"Failed to convert value $value " +
          s"(class of ${value.getClass}}) with the type of $dataType to JSON.")
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
    val fields = value.asInstanceOf[JObject].values
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
