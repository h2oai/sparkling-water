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

import java.io.{Closeable, OutputStream}
import java.sql.Timestamp
import java.util.UUID

import water.fvec.{ChunkUtils, Frame}
import water.parser.BufferedString
import water.{AutoBuffer, DKV}

final class ChunkAutoBufferWriter(val outputStream: OutputStream) extends Closeable with ChunkSerdeConstants {

  private val buffer = new AutoBuffer(outputStream, false)

  def writeChunk(
      frameName: String,
      numRows: Int,
      chunkId: Int,
      expectedTypes: Array[Byte],
      selectedColumnIndices: Array[Int]): Unit = {
    val frame = DKV.getGet[Frame](frameName)
    val chunks = ChunkUtils.getChunks(frame, chunkId)
    // buffered string to be reused for strings to avoid multiple allocation in the loop
    val valStr = new BufferedString
    var rowIdx = 0
    while (rowIdx < numRows) {
      var i = 0
      while (i < selectedColumnIndices.length) {
        if (chunks(selectedColumnIndices(i)).isNA(rowIdx)) {
          writeNA(expectedTypes(i))
        } else {
          val chunk = chunks(selectedColumnIndices(i))
          expectedTypes(i) match {
            case EXPECTED_BOOL | EXPECTED_BYTE => writeByte(chunk.at8(rowIdx).toByte)
            case EXPECTED_CHAR => writeChar(chunk.at8(rowIdx).toChar)
            case EXPECTED_SHORT => writeShort(chunk.at8(rowIdx).toShort)
            case EXPECTED_INT => writeInt(chunk.at8(rowIdx).toInt)
            case EXPECTED_FLOAT => writeFloat(chunk.atd(rowIdx).toFloat)
            case EXPECTED_LONG | EXPECTED_TIMESTAMP => writeLong(chunk.at8(rowIdx))
            case EXPECTED_DOUBLE => writeDouble(chunk.atd(rowIdx))
            case EXPECTED_STRING =>
              var str: String = null
              if (chunk.vec.isCategorical) str = chunk.vec().domain()(chunk.at8(rowIdx).toInt)
              else if (chunk.vec.isString) str = chunk.atStr(valStr, rowIdx).toString
              else if (chunk.vec.isUUID) str = new UUID(chunk.at16h(rowIdx), chunk.at16l(rowIdx)).toString
              else throw new RuntimeException("Unreachable code!")
              writeString(str)
          }
        }
        i += 1
      }
      rowIdx = rowIdx + 1
    }
  }

  def writeBoolean(data: Boolean): Unit = {
    val value = if (data) 1.toByte else 0.toByte
    buffer.put1(value)
    putMarker(value)
  }

  def writeByte(data: Byte): Unit = {
    buffer.put1(data)
    putMarker(data)
  }

  def writeChar(data: Char): Unit = {
    buffer.put2(data)
    putMarker(data)
  }

  def writeShort(data: Short): Unit = {
    buffer.put2s(data)
    putMarker(data)
  }

  def writeInt(data: Int): Unit = {
    buffer.putInt(data)
    putMarker(data)
  }

  def writeLong(data: Long): Unit = {
    buffer.put8(data)
    putMarker(data)
  }

  def writeFloat(data: Float): Unit = buffer.put4f(data)

  def writeDouble(data: Double): Unit = buffer.put8d(data)

  def writeString(data: String): Unit = {
    buffer.putStr(data)
    if (data != null && data == STR_MARKER_NEXT_BYTE_FOLLOWS) {
      buffer.put1(MARKER_ORIGINAL_VALUE)
    }
  }

  def writeTimestamp(timestamp: Timestamp): Unit = writeLong(timestamp.getTime)

  def writeNA(expectedType: Byte): Unit = expectedType match {
    case EXPECTED_BOOL | EXPECTED_BYTE =>
      buffer.put1(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      buffer.put1(MARKER_NA)
    case EXPECTED_CHAR =>
      buffer.put2(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      buffer.put1(MARKER_NA)
    case EXPECTED_SHORT =>
      buffer.put2s(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      buffer.put1(MARKER_NA)
    case EXPECTED_INT =>
      buffer.putInt(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      buffer.put1(MARKER_NA)
    case EXPECTED_TIMESTAMP | EXPECTED_LONG =>
      buffer.put8(NUM_MARKER_NEXT_BYTE_FOLLOWS)
      buffer.put1(MARKER_NA)
    case EXPECTED_FLOAT =>
      buffer.put4f(Float.NaN)
    case EXPECTED_DOUBLE =>
      buffer.put8d(Double.NaN)
    case EXPECTED_STRING =>
      buffer.putStr(STR_MARKER_NEXT_BYTE_FOLLOWS)
      buffer.put1(MARKER_NA)
    case _ =>
      throw new IllegalArgumentException("Unknown expected type " + expectedType)
  }

  def writeIntArray(data: Array[Int]): Unit = buffer.putA4(data)

  def writeDoubleArray(data: Array[Double]): Unit = buffer.putA8d(data)

  def writeSparseVector(indices: Array[Int], values: Array[Double]): Unit = {
    writeBoolean(VECTOR_IS_SPARSE)
    writeIntArray(indices)
    writeDoubleArray(values)
  }

  def writeDenseVector(values: Array[Double]): Unit = {
    writeBoolean(VECTOR_IS_DENSE)
    writeDoubleArray(values)
  }

  /**
    * Sends another byte as a marker if it's needed and send the data
    */
  private def putMarker(data: Long): Unit = {
    if (data == NUM_MARKER_NEXT_BYTE_FOLLOWS) { // we need to send another byte because zero is represented as 00 ( 2 bytes )
      buffer.put1(MARKER_ORIGINAL_VALUE)
    }
  }

  override def close(): Unit = buffer.close()
}
