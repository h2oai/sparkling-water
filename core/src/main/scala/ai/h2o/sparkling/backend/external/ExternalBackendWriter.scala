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

package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.backend.shared.Writer
import ai.h2o.sparkling.extensions.serde.ChunkAutoBufferWriter
import ai.h2o.sparkling.frame.H2OChunk
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}

private[sparkling] class ExternalBackendWriter(conf: H2OConf,
                                               nodeDesc: NodeDesc,
                                               frameName: String,
                                               numRows: Int,
                                               expectedTypes: Array[Byte],
                                               chunkId: Int,
                                               maxVecSizes: Array[Int],
                                               sparse: Array[Boolean]) extends Writer {

  private val outputStream = H2OChunk.putChunk(nodeDesc, conf, frameName, numRows, chunkId, expectedTypes, maxVecSizes)
  private val chunkWriter: ChunkAutoBufferWriter = new ChunkAutoBufferWriter(outputStream)

  override def put(colIdx: Int, data: Boolean): Unit = chunkWriter.writeBoolean(data)

  override def put(colIdx: Int, data: Byte): Unit = chunkWriter.writeByte(data)

  override def put(colIdx: Int, data: Char): Unit = chunkWriter.writeChar(data)

  override def put(colIdx: Int, data: Short): Unit = chunkWriter.writeShort(data)

  override def put(colIdx: Int, data: Int): Unit = chunkWriter.writeInt(data)

  override def put(colIdx: Int, data: Long): Unit = chunkWriter.writeLong(data)

  override def put(colIdx: Int, data: Float): Unit = chunkWriter.writeFloat(data)

  override def put(colIdx: Int, data: Double): Unit = chunkWriter.writeDouble(data)

  override def put(colIdx: Int, data: String): Unit = chunkWriter.writeString(data)

  override def putNA(columnIdx: Int, sparkIdx: Int): Unit = chunkWriter.writeNA(expectedTypes(sparkIdx))

  override def putSparseVector(startIdx: Int, vector: SparseVector, maxVecSize: Int): Unit = {
    chunkWriter.writeSparseVector(vector.indices, vector.values)
  }

  override def putDenseVector(startIdx: Int, vector: DenseVector, maxVecSize: Int): Unit = {
    chunkWriter.writeDenseVector(vector.values)
  }

  override def startRow(rowIdx: Int): Unit = {}

  override def close(): Unit = {
    chunkWriter.close()
  }
}
