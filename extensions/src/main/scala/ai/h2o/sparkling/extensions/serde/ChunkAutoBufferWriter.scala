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

import SerializationUtils._
import java.io.{Closeable, OutputStream}
import java.util.UUID

import water.fvec.{ChunkUtils, Frame}
import water.parser.BufferedString
import water.{AutoBuffer, DKV}

final class ChunkAutoBufferWriter(val outputStream: OutputStream) extends Closeable {

  private val buffer = new AutoBuffer(outputStream, false)

  def writeChunk(frameName: String, chunkId: Int, expectedTypes: Array[Byte], selectedColumnIndices: Array[Int]): Unit = {
    val frame = DKV.getGet[Frame](frameName)
    val chunks = ChunkUtils.getChunks(frame, chunkId)
    val numberOfRows = chunks(0)._len
    sendInt(buffer, numberOfRows)
    // buffered string to be reused for strings to avoid multiple allocation in the loop
    val valStr = new BufferedString
    var rowIdx = 0
    while (rowIdx < numberOfRows) {
      var i = 0
      while (i < selectedColumnIndices.length) {
        if (chunks(selectedColumnIndices(i)).isNA(rowIdx)) {
          sendNA(buffer, expectedTypes(i))
        } else {
          val chunk = chunks(selectedColumnIndices(i))
          expectedTypes(i) match {
            case EXPECTED_BOOL => sendBoolean(buffer, chunk.at8(rowIdx).toByte)
            case EXPECTED_BYTE => sendByte(buffer, chunk.at8(rowIdx).toByte)
            case EXPECTED_CHAR => sendChar(buffer, chunk.at8(rowIdx).toChar)
            case EXPECTED_SHORT => sendShort(buffer, chunk.at8(rowIdx).toShort)
            case EXPECTED_INT => sendInt(buffer, chunk.at8(rowIdx).toInt)
            case EXPECTED_FLOAT => sendFloat(buffer, chunk.atd(rowIdx).toFloat)
            case EXPECTED_LONG => sendLong(buffer, chunk.at8(rowIdx))
            case EXPECTED_DOUBLE => sendDouble(buffer, chunk.atd(rowIdx))
            case EXPECTED_TIMESTAMP => sendTimestamp(buffer, chunk.at8(rowIdx))
            case EXPECTED_STRING =>
              var str: String = null
              if (chunk.vec.isCategorical) str = chunk.vec().domain()(chunk.at8(rowIdx).toInt)
              else if (chunk.vec.isString) str = chunk.atStr(valStr, rowIdx).toString
              else if (chunk.vec.isUUID) str = new UUID(chunk.at16h(rowIdx), chunk.at16l(rowIdx)).toString
              else assert(false, "Can never be here")
              sendString(buffer, str)
          }
        }
        i += 1
      }
      rowIdx = rowIdx + 1
    }
  }

  override def close(): Unit = buffer.close()
}
