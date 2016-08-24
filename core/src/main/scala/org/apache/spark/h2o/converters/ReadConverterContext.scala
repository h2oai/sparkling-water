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

import org.apache.spark.unsafe.types.UTF8String


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

  def getByte(columnNum: Int): Byte = getLong(columnNum).toByte
  def getShort(columnNum: Int): Short = getLong(columnNum).toShort
  def getInt(columnNum: Int): Int = getLong(columnNum).toInt

  def getFloat(columnNum: Int): Float = getDouble(columnNum).toFloat
  def getBoolean(columnNum: Int): Boolean = getLong(columnNum) == 1
  def getTimestamp(columnNum: Int): Long  = getLong(columnNum) * 1000L
  def getUTF8String(columnNum: Int): UTF8String = {
    val str = getString(columnNum)
    if(str == null){
      null
    }else{
      UTF8String.fromString(str)
    }
  }

  def isNA(columnNum: Int): Boolean
  def getLong(columnNum: Int): Long
  def getDouble(columnNum: Int): Double
  def getString(columnNum: Int): String

  def numRows: Int
  def increaseRowIdx() = rowIdx += 1
  def hasNext = rowIdx < numRows
}
