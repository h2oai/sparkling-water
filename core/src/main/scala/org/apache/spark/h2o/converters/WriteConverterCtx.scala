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

/**
  * Methods which each WriteConverterCtx has to implement.
  *
  * Write Converter Context is a class which holds the state of connection/chunks and allows us to write/upload to those chunks
  * via unified API
  */
trait WriteConverterCtx {
  def createChunks(keyName: String, vecTypes: Array[Byte], chunkId: Int)

  def closeChunks()

  def put(colIdx: Int, data: Boolean)

  def put(colIdx: Int, data: Byte)

  def put(colIdx: Int, data: Char)

  def put(colIdx: Int, data: Short)

  def put(colIdx: Int, data: Int)

  def put(colIdx: Int, data: Long)

  def put(colIdx: Int, data: Float)

  def put(colIdx: Int, data: Double)

  def put(colIdx: Int, data: java.sql.Timestamp)

  def put(colIdx: Int, data: java.sql.Date)

  def put(colIdx: Int, data: String)

  def putNA(colIdx: Int)

  def putAnySupportedType[T](colIdx: Int, data: T): Unit = {
    data match {
      case n: Boolean => put(colIdx, n)
      case n: Byte => put(colIdx, n)
      case n: Char => put(colIdx, n)
      case n: Short => put(colIdx, n)
      case n: Int => put(colIdx, n)
      case n: Long => put(colIdx, n)
      case n: Float => put(colIdx, n)
      case n: Double => put(colIdx, n)
      case n: String => put(colIdx, n)
      case n: java.sql.Timestamp => put(colIdx, n)
      case n: java.sql.Date => put(colIdx, n)
      case _ => putNA(colIdx)
    }
  }

  def numOfRows(): Int
}
