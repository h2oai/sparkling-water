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

  private type Getter = Int => Option[Any]

  val get: DataType => Getter = Map[DataType, Getter](
    ByteType -> getByte,
    ShortType -> getShort,
    IntegerType -> getInt,
    LongType -> getLong,
    FloatType -> getFloat,
    DoubleType -> getDouble,
    BooleanType -> getBoolean,
    StringType -> getUTF8String,
    TimestampType -> getTimestamp
  ) withDefault (t => throw new scala.IllegalArgumentException(s"Type $t not supported for conversion from H2OFrame to Spark's Dataframe"))

  def getNullable(dataType: DataType): (Int => Any) = (i: Int) => get(dataType)(i) orNull

  def numRows: Int
  def increaseRowIdx() = rowIdx += 1
  def hasNext = rowIdx < numRows
}
