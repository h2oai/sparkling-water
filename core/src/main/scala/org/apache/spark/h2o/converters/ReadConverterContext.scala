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

package org.apache.spark.h2o.converters

import org.apache.spark.h2o.utils.ReflectionUtils._
import org.apache.spark.h2o.utils.SupportedTypes
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.language.postfixOps

/**
  * Methods which each ReadConverterContext has to implement.
  *
  * Read Converter Context is a class which holds the state of connection/chunks and allows us to read/download data from those chunks
  * via unified API
  */
trait ReadConverterContext {
  /** Key pointing to underlying H2OFrame */
  val keyName: String

  /** Chunk Idx/Partition index */
  val chunkIdx: Int

  /** Current row index */
  var rowIdx: Int = 0

  def getByte(columnNum: Int): Option[Byte] = getLong(columnNum) map (_.toByte)
  def getShort(columnNum: Int): Option[Short] = getLong(columnNum) map (_.toShort)
  def getInt(columnNum: Int): Option[Int] = getLong(columnNum) map (_.toInt)

  def getFloat(columnNum: Int): Option[Float] = getDouble(columnNum) map (_.toFloat)
  def getBoolean(columnNum: Int): Option[Boolean] = getLong(columnNum) map (1==)
  def getTimestamp(columnNum: Int): Option[Long]  = getLong(columnNum) map (1000L*)
  def getUTF8String(columnNum: Int): Option[UTF8String] = getString(columnNum) map UTF8String.fromString

  def isNA(columnNum: Int): Boolean
  def getLong(columnNum: Int): Option[Long]
  def getDouble(columnNum: Int): Option[Double]
  def getString(columnNum: Int): Option[String]

  def numRows: Int
  def increaseRowIdx() = rowIdx += 1

  def hasNext = rowIdx < numRows

  case class OptionReader(name: Any, apply: Int => Option[Any]) {
  }

//  type OptionReader = Int => Option[Any]

  import SupportedTypes._

  val ReaderPerType = Map[SupportedType, Int => Option[Any]](
    Byte -> getByte,
    Short -> getShort,
    Integer -> getInt,
    Long -> getLong,
    Float -> getFloat,
    Double -> getDouble,
    Boolean -> getBoolean,
    String -> getUTF8String,
    UTF8 -> getUTF8String,
    Timestamp -> getTimestamp
  ) withDefault (t => throw new scala.IllegalArgumentException(s"Type $t not supported for conversion from H2OFrame to Spark's Dataframe"))

  def readerFor(dt: DataType): OptionReader = OptionReader(bySparkType(dt), ReaderPerType(bySparkType(dt)))

  case class Reader(name: Any, apply: Int => Any)

  val readerMapByName: Map[NameOfType, Reader]

  /**
    * For a given array of source column indexes and required data types,
    * produces an array of value providers.
    *
    * @param columnIndexesWithTypes lists which columns we need, and what are the required types
    * @return an array of value providers. Each provider gives the current column value
    */
  def columnValueProviders(columnIndexesWithTypes: Array[(Int, DataType)]): Array[() => Option[Any]]
}
