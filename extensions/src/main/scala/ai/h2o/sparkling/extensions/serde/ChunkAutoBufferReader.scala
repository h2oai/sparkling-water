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
import water.fvec.{ChunkUtils, NewChunk}

final class ChunkAutoBufferReader(val inputStream: InputStream) extends Closeable with ChunkSerdeConstants {

  private val buffer = new AutoBuffer(inputStream)
  private var isLastNAVar: Boolean = false

  def readChunk(frameName: String, numRows: Int, chunkId: Int, expectedTypes: Array[Byte], maxVecSizes: Array[Int]): Unit = {
    val vecTypes = SerdeUtils.expectedTypesToVecTypes(expectedTypes, maxVecSizes)
    val elementSizes = getElementSizes(expectedTypes, maxVecSizes)
    val startPositions = getStartPositions(elementSizes)
    val chunks = ChunkUtils.createNewChunks(frameName, vecTypes, chunkId)
    var rowIdx = 0
    while (rowIdx < numRows) {
      var typeIdx = 0
      while (typeIdx < expectedTypes.length) {
        expectedTypes(typeIdx) match {
          case EXPECTED_BOOL | EXPECTED_BYTE => addToChunk(chunks(startPositions(typeIdx)), readByte())
          case EXPECTED_CHAR => addToChunk(chunks(startPositions(typeIdx)), readChar())
          case EXPECTED_SHORT => addToChunk(chunks(startPositions(typeIdx)), readShort())
          case EXPECTED_INT => addToChunk(chunks(startPositions(typeIdx)), readInt());
          case EXPECTED_LONG | EXPECTED_TIMESTAMP => addToChunk(chunks(startPositions(typeIdx)), readLong())
          case EXPECTED_FLOAT => addToChunk(chunks(startPositions(typeIdx)), readFloat())
          case EXPECTED_DOUBLE => addToChunk(chunks(startPositions(typeIdx)), readDouble())
          case EXPECTED_STRING => addToChunk(chunks(startPositions(typeIdx)), readString())
          case EXPECTED_VECTOR =>
            isLastNAVar = false
            val isSparse = buffer.getZ
            if (isSparse) addSparseVectorToChunk(chunks, elementSizes(typeIdx), startPositions(typeIdx))
            else addDenseVectorToChunk(chunks, elementSizes(typeIdx), startPositions(typeIdx))
        }
        typeIdx += 1
      }
      rowIdx += 1
    }
    ChunkUtils.closeNewChunks(chunks)
  }

  private def addSparseVectorToChunk(chunks: Array[NewChunk], maxVecSize: Int, startPosition: Int): Unit = {
    val indices = buffer.getA4
    val values = buffer.getA8d
    if (values == null) throw new RuntimeException("Values of sparse Vector can't be null!")
    if (indices == null) throw new RuntimeException("Indices of sparse Vector can't be null!")
    // store values
    var zeroSectionStart = 0
    var i = 0
    while (i < indices.length) {
      var zeroIdx = zeroSectionStart
      while (zeroIdx < indices(i)) {
        addToChunk(chunks(startPosition + zeroIdx), 0)
        zeroIdx += 1
      }
      addToChunk(chunks(startPosition + indices(i)), values(i))
      zeroSectionStart = indices(i) + 1
      i += 1
    }
    // fill remaining zeros
    var j = zeroSectionStart
    while (j < maxVecSize) {
      addToChunk(chunks(startPosition + j), 0)
      j += 1
    }
  }

  private def addDenseVectorToChunk(chunks: Array[NewChunk], maxVecSize: Int, startPosition: Int): Unit = {
    val values = buffer.getA8d
    if (values == null) throw new RuntimeException("Values of dense Vector can't be null!")
    // fill values
    var j = 0
    while (j < values.length) {
      addToChunk(chunks(startPosition + j), values(j))
      j += 1
    }
    while (j < maxVecSize) {
      addToChunk(chunks(startPosition + j), 0)
      j += 1
    }
  }

  private def addToChunk(chunk: NewChunk, data: Long): Unit = {
    if (isLastNA) chunk.addNA()
    else chunk.addNum(data)
  }

  private def addToChunk(chunk: NewChunk, data: Double): Unit = {
    if (isLastNA) chunk.addNA()
    else chunk.addNum(data)
  }

  private def addToChunk(chunk: NewChunk, data: String): Unit = {
    if (isLastNA) chunk.addNA()
    else chunk.addStr(data)
  }

  private def getStartPositions(elemSizes: Array[Int]): Array[Int] = {
    val startPositions = new Array[Int](elemSizes.length)
    var i = 1
    while (i < elemSizes.length) {
      startPositions(i) = startPositions(i - 1) + elemSizes(i - 1)
      i += 1
    }
    startPositions
  }

  private def getElementSizes(expectedTypes: Array[Byte], vecElemSizes: Array[Int]): Array[Int] = {
    var vecCount = 0
    expectedTypes.map {
      case EXPECTED_BOOL | EXPECTED_BYTE | EXPECTED_CHAR | EXPECTED_SHORT | EXPECTED_INT | EXPECTED_LONG |
           EXPECTED_FLOAT | EXPECTED_DOUBLE | EXPECTED_STRING | EXPECTED_TIMESTAMP => 1
      case EXPECTED_VECTOR =>
        val result = vecElemSizes(vecCount)
        vecCount += 1
        result
    }
  }

  def readBoolean(): Boolean = {
    val data = buffer.getZ
    isLastNAVar = isNA(data)
    data
  }

  def readByte(): Byte = {
    val data = buffer.get1
    isLastNAVar = isNA(data)
    data
  }

  def readChar(): Char = {
    val data = buffer.get2
    isLastNAVar = isNA(data)
    data
  }

  def readShort(): Short = {
    val data = buffer.get2s
    isLastNAVar = isNA(data)
    data
  }

  def readInt(): Int = {
    val data = buffer.getInt
    isLastNAVar = isNA(data)
    data
  }

  def readLong(): Long = {
    val data = buffer.get8
    isLastNAVar = isNA(data)
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
    isLastNAVar = isNA(data)
    data
  }

  def readTimestamp(): Timestamp = {
    val data = new Timestamp(buffer.get8)
    isLastNAVar = isNA(data)
    data
  }

  private def isNA(data: Boolean): Boolean = isNA(if (data) 1L else 0L)

  private def isNA(data: Long): Boolean = data == NUM_MARKER_NEXT_BYTE_FOLLOWS && buffer.get1 == MARKER_NA

  private def isNA(data: Double): Boolean = data.isNaN()

  private def isNA(data: Timestamp): Boolean = isNA(data.getTime)

  private def isNA(data: String): Boolean = {
    data != null && data == STR_MARKER_NEXT_BYTE_FOLLOWS && buffer.get1 == MARKER_NA
  }

  /**
   * This method is used to check if the last received value was marked as NA by H2O backend
   */
  def isLastNA: Boolean = isLastNAVar

  override def close(): Unit = buffer.close()
}
