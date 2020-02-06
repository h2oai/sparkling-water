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

package ai.h2o.sparkling.extensions.serde

import java.io.{Closeable, InputStream}
import java.sql.Timestamp

import water.AutoBuffer
import SerializationUtils._

final class ChunkAutoBufferReader(val inputStream: InputStream) extends Closeable {
  private val buffer = new AutoBuffer(inputStream)
  private val numRows = readInt
  private var isLastNAVar: Boolean = false

  def getNumRows: Int = numRows

  def readBoolean(): Boolean = {
    val data = buffer.getZ
    isLastNAVar = isNA(buffer, data)
    data
  }

  def readByte(): Byte = {
    val data = buffer.get1
    isLastNAVar= isNA(buffer, data)
    data
  }

  def readChar(): Char = {
    val data = buffer.get2
    isLastNAVar = isNA(buffer, data)
    data
  }

  def readShort(): Short = {
    val data = buffer.get2s
    isLastNAVar = isNA(buffer, data)
    data
  }

  def readInt(): Int = {
    val data = buffer.getInt
    isLastNAVar= isNA(buffer, data)
    data
  }

  def readLong(): Long = {
    val data = buffer.get8
    isLastNAVar = isNA(buffer, data)
    data
  }

  def readFloat(): Float = {
    val data = buffer.get4f
    isLastNAVar = isNA(data)
    data
  }

  def readDouble(): Double = {
    val data = buffer.get8d
    isLastNAVar = isNA(data)
    data
  }

  def readString(): String = {
    val data = buffer.getStr
    isLastNAVar = isNA(buffer, data)
    data
  }

  def readTimestamp(): Timestamp = {
    val data = new Timestamp(buffer.get8)
    isLastNAVar = isNA(buffer, data)
    data
  }

  /**
    * This method is used to check if the last received value was marked as NA by H2O backend
    */
  def isLastNA: Boolean = isLastNAVar

  override def close(): Unit = {
    buffer.close
  }
}
